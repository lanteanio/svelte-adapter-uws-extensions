/**
 * Match keys that look like auth/session credentials. Excludes the bare
 * substring "key" because legitimate id-like fields often contain it.
 * Used by stripInternal() to redact userData before it leaves the process.
 */
export const SENSITIVE_STRIP_RE = /token|secret|password|auth|session|cookie|jwt|credential/i;

/**
 * Same as STRIP, plus "key". Used only for warnings; legitimate id-like
 * fields often contain "key" so we tell the developer instead of silently
 * dropping the field.
 */
export const SENSITIVE_WARN_RE = /token|secret|password|key|auth|session|cookie|jwt|credential/i;

/**
 * Recursively strip internal/sensitive keys from a user-supplied object.
 * Drops keys starting with "__" (internal adapter state - catches
 * "__proto__" so the result is safe to spread / Object.assign without
 * triggering prototype pollution), keys matching SENSITIVE_STRIP_RE
 * (token/secret/password/auth/session/cookie/jwt/credential), and the
 * "constructor" and "prototype" keys (defense-in-depth against
 * shadow-via-Object.assign on consumer targets).
 *
 * Binary views (Buffer, TypedArray, DataView, ArrayBuffer) are
 * substituted with a "[bytes: <len>]" placeholder. Default Object.keys
 * iteration over a binary view yields stringified numeric indices, so
 * naively walking them would JSON.stringify the raw bytes into log
 * lines - a credential-in-logs hazard for any caller that pipes auth
 * tokens through as Buffers.
 *
 * Cycle-safe via a per-call WeakSet, and bounded on BOTH axes that a
 * hostile shape can grow. Past `STRIP_MAX_DEPTH` levels a subtree becomes
 * a `"[deep]"` placeholder, and past `STRIP_MAX_NODES` visited nodes the
 * rest of the walk becomes `"[truncated]"`.
 *
 * The node budget is not redundant with the depth cap. The WeakSet tracks
 * the ancestor PATH (entries are removed on the way back up) so that a
 * legitimately repeated sibling is not silently dropped as a cycle - which
 * means a shared subtree is re-walked once per reference, and
 * `let n = {x:1}; for (let i=0;i<26;i++) n = {a:n, b:n}` expands to 2^26
 * visits at depth 26, comfortably inside the depth cap and fatal to the
 * process (a V8 heap OOM, which no caller can catch). This is reachable
 * from app-constructed presence/cursor userData and from assert context.
 *
 * What the budget bounds, stated exactly: the OUTPUT, and the walk's own
 * per-value work. It does not bound `Object.keys` on a single container.
 * JavaScript has no O(1) own-key count, so learning that an object is too
 * wide to walk costs one key-array materialization of that width - and the
 * ancestor set is a path set, so a wide object behind several references
 * pays it once per reference. One 3M-key object behind three references
 * takes ~0.9 s and ~45 MB to produce 55 bytes of output. Arrays do not have
 * this term: `obj.length` is O(1) and is charged before the walk. The bound
 * is therefore O(`STRIP_MAX_NODES` + widest-object x references-to-it), not
 * O(`STRIP_MAX_NODES`), and a caller placing untrusted data here should
 * bound the width upstream.
 *
 * @param {unknown} obj
 * @param {WeakSet<object>} [ancestors]
 * @param {number} [depth]
 * @param {{ n: number }} [budget] - Shared visit counter for one top-level call.
 * @param {(key: string) => boolean} [dropKey] - Internal projection-policy override.
 */
export const STRIP_MAX_DEPTH = 32;
export const STRIP_MAX_NODES = 100_000;
export function stripInternal(obj, ancestors, depth, budget, dropKey) {
	if (depth === undefined) depth = 0;
	if (!budget) budget = { n: 0 };
	if (!obj || typeof obj !== 'object') return obj;
	if (ArrayBuffer.isView(obj) || obj instanceof ArrayBuffer) {
		const len = /** @type {{ byteLength: number }} */ (obj).byteLength;
		return '[bytes: ' + len + ']';
	}
	if (depth >= STRIP_MAX_DEPTH) return '[deep]';
	// Entry guard, BEFORE any per-container work. Charging a container its
	// width bounds the OUTPUT, but `Object.keys` is itself O(width) and
	// allocating, and returning '[truncated]' does not stop the parent's
	// sibling loop - so without this, every remaining slot pointing at the
	// same wide object still pays a full key materialization after the
	// budget is already blown. A 100k-element array of references to one
	// 100k-key object bounded the output to 280KB and still blocked the
	// event loop for over three minutes. Placed above `ancestors.add`, so
	// there is no path entry to undo.
	if (budget.n > STRIP_MAX_NODES) return '[truncated]';
	if (!ancestors) ancestors = new WeakSet();
	if (ancestors.has(obj)) return undefined;
	ancestors.add(obj);
	let result;
	// Each container charges its OWN WIDTH once, before walking it. Every
	// value the walk can emit occupies a slot in some container, so the sum
	// of widths bounds the output just as counting each emitted value did -
	// without paying a counter increment per primitive on the hot path. The
	// leaf FAN-OUT stays bounded, so the shared-subtree expansion the budget
	// exists to stop still lands inside it: in
	// `let n = [1..1000]; for (let i=0;i<26;i++) n = [n, n]` each re-walk of
	// the 1000-element leaf charges 1000, rather than being free because its
	// elements are primitives.
	// The WeakSet is a PATH set, so EVERY exit below has to leave it as it
	// found it - including the exceptional one. `obj[k]` invokes a getter,
	// which is free to throw; unwinding without the delete leaves this object
	// permanently in the caller's set, and the next walk that reaches it
	// drops it as a phantom cycle.
	//
	// catch-and-rethrow rather than `finally`: identical semantics here, but
	// `finally` also has to model the RETURN path, and it measured with
	// occasional double-digit excursions on this per-connection primitive
	// where catch stayed at or below the unguarded baseline on every shape.
	// `catch` does not run on a RETURN the way `finally` does, so each early
	// exit below carries its own unwind. That is the one thing the
	// catch-and-rethrow form does not get for free, and missing it leaves the
	// truncated object in the caller's path set - where the next walk reads it
	// as a phantom cycle and silently drops the whole subtree.
	try {
		if (Array.isArray(obj)) {
			const width = obj.length;
			budget.n += width;
			if (budget.n > STRIP_MAX_NODES) { ancestors.delete(obj); return '[truncated]'; }
			// Indexed loop rather than `.map`: the callback captures three
			// variables and allocates one closure per array, which measured
			// +6-7% on an array-of-objects shape - and this runs per connection
			// on `select()` output. Inlining the primitive check here also
			// matches the object branch below and skips a call per element.
			// Deliberately NOT re-checking the budget per slot the way the
			// object branch below does, and the asymmetry is the measurement,
			// not an oversight. `obj.length` above is an O(1) count charged
			// BEFORE the walk, so a wide array is already refused at entry -
			// the object branch has no O(1) key count and that is the whole
			// gap the re-check exists to close. What is left here is an array
			// carrying index ACCESSORS, which takes a deliberate
			// `Object.defineProperty` on an index and does not arise from
			// app-constructed presence or cursor data. Adding the check cost
			// +8.7 to +13.1% on a 200-element primitive array across three
			// interleaved runs - this loop body is tight enough that one
			// integer compare per element is a tenth of it.
			const out = new Array(width);
			for (let i = 0; i < width; i++) {
				const v = obj[i];
				out[i] = (v && typeof v === 'object') ? stripInternal(v, ancestors, depth + 1, budget, dropKey) : v;
			}
			result = out;
		} else {
			const keys = Object.keys(obj);
			budget.n += keys.length;
			if (budget.n > STRIP_MAX_NODES) { ancestors.delete(obj); return '[truncated]'; }
			result = {};
			for (const k of keys) {
				if (
					k.startsWith('__') || k === 'constructor' || k === 'prototype' ||
					(dropKey ? dropKey(k) : SENSITIVE_STRIP_RE.test(k))
				) continue;
				// Re-checked per key, after the skip filters so a dropped key
				// pays nothing for it. `budget.n` moves only inside this
				// function, so between two iterations of this loop it can only
				// have changed by a nested walk - but once it has, reading
				// `obj[k]` is pure waste, and reading it is not free: the
				// property is free to be a getter that builds another wide
				// object before handing back a value this walk then discards.
				// A container of 5000 such getters invoked all 5000 and cost
				// 3.1 s; it now invokes the ones the budget actually paid for.
				// The emitted shape is unchanged for the object-valued keys
				// this bounds - they truncate either way - and for primitives
				// it now matches the documented contract, which already said
				// the REST OF THE WALK becomes '[truncated]'.
				if (budget.n > STRIP_MAX_NODES) { result[k] = '[truncated]'; continue; }
				const v = obj[k];
				result[k] = (v && typeof v === 'object') ? stripInternal(v, ancestors, depth + 1, budget, dropKey) : v;
			}
		}
	} catch (err) {
		ancestors.delete(obj);
		throw err;
	}
	ancestors.delete(obj);
	return result;
}

/**
 * Redact the password segment of a connection URL so the URL is safe to
 * embed in log lines and error messages. Substitutes the userinfo password
 * with `***` and any `password=` / `pass=` / `pwd=` query-string value
 * with `***`. Other URL bytes (scheme, user, host, port, path, fragment,
 * non-password query params) pass through unchanged. Defensive against
 * non-string input so callers can pipe through arbitrary error context
 * without a type guard.
 *
 * Implementation is a byte-level scan (not URL parsing) so it correctly
 * redacts:
 *
 * - **Passwords containing `@`** (`redis://user:p@ssword@host`). A naive
 *   first-`@` regex stops too early and leaks the tail of the password.
 *   The scan walks to the LAST `@` in the authority region.
 * - **IPv6 hosts** (`redis://:secret@[::1]:6379`). Bracket characters
 *   suspend authority-terminator detection so `/`, `:`, `@` inside `[...]`
 *   are not mistaken for authority boundaries.
 * - **Query-string passwords** (`postgres://host/db?password=hunter2`).
 *   pg accepts `password` as a connection parameter; redaction is
 *   case-insensitive and matches `password` / `pass` / `pwd`.
 * - **Every URL in the string, not just the first.** Callers pass whole
 *   error messages and whole stack traces, which routinely carry the DSN
 *   more than once; redacting only the first occurrence leaks the rest.
 *   Each URL is bounded at the next whitespace byte so free text after it is
 *   neither scanned nor rewritten - the query-value match would otherwise
 *   run past a newline and swallow the remaining stack frames.
 *
 * @param {unknown} url
 * @returns {string}
 */
export function redactConnectionUrl(url) {
	if (typeof url !== 'string' || url.length === 0) return String(url);
	if (url.indexOf('://') === -1) return url;

	let out = '';
	let cursor = 0;
	let from = 0;
	for (;;) {
		const at = url.indexOf('://', from);
		if (at === -1) break;
		// Where this URL stops: the first whitespace, or the start of the NEXT
		// URL's scheme, whichever comes first. Both bounds are load-bearing.
		// Without the whitespace bound the query-value match runs past a
		// newline and swallows the rest of a stack trace. Without the
		// next-scheme bound, two DSNs with no whitespace between them (a
		// compact JSON config line is the ordinary carrier) become ONE token:
		// only one authority is found, and the scan resumes past both, so
		// every later credential in that run is re-emitted verbatim.
		const ws = whitespaceAt(url, at + 3);
		const nextScheme = url.indexOf('://', at + 3);
		const end = nextScheme === -1 ? ws : Math.min(ws, schemeStart(url, nextScheme));
		out += url.slice(cursor, at) + redactUrlRegion(url.slice(at, end));
		cursor = end;
		from = end;
	}
	return out + url.slice(cursor);
}

/**
 * Index of the first whitespace byte at or after `from`, else `s.length`.
 *
 * Whitespace is the ONLY generic terminator. Quotes, backticks and angle
 * brackets look like sensible delimiters and are not safe ones here: `'` and
 * the other RFC 3986 sub-delims (`!$&'()*+,;=`) are legal unescaped in a
 * userinfo password, so treating one as a terminator cuts the token before
 * the authority `@` and the whole credential is re-emitted. `]` is likewise
 * not a terminator - it closes an IPv6 host.
 *
 * @param {string} s
 * @param {number} from
 * @returns {number}
 */
function whitespaceAt(s, from) {
	for (let i = from; i < s.length; i++) {
		const c = s.charCodeAt(i);
		if (c === 32 || c === 9 || c === 10 || c === 13 || c === 12 || c === 11) return i;
	}
	return s.length;
}

/**
 * Walk back from a `://` over the scheme characters (ALPHA / DIGIT / `+`
 * `-` `.`) to where that URL actually begins, so the PRECEDING url can be
 * cut there rather than swallowing it.
 *
 * @param {string} s
 * @param {number} colonIdx - index of the `://`
 * @returns {number}
 */
function schemeStart(s, colonIdx) {
	let i = colonIdx;
	while (i > 0) {
		const c = s.charCodeAt(i - 1);
		const isSchemeChar =
			(c >= 65 && c <= 90) || (c >= 97 && c <= 122) ||
			(c >= 48 && c <= 57) || c === 43 || c === 45 || c === 46;
		if (!isSchemeChar) break;
		i--;
	}
	return i;
}

/**
 * Redact ONE url token. `url` begins at its `://`.
 *
 * @param {string} url
 * @returns {string}
 */
function redactUrlRegion(url) {
	const protoEnd = 0;
	const userinfoStart = protoEnd + 3;

	// Scan the authority region (from `://` to the first unbracketed
	// `/`, `?`, `#`, or end-of-string). Track the LAST `@` in the
	// region so a password containing `@` is redacted in full. Skip
	// bytes inside `[...]` so IPv6 host colons / `@` are not parsed
	// as userinfo terminators.
	let atIdx = -1;
	let authorityEnd = url.length;
	let inBracket = false;
	for (let i = userinfoStart; i < url.length; i++) {
		const c = url.charCodeAt(i);
		if (c === 91 /* [ */) inBracket = true;
		else if (c === 93 /* ] */) inBracket = false;
		else if (!inBracket) {
			if (c === 64 /* @ */) atIdx = i;
			else if (c === 47 /* / */ || c === 63 /* ? */ || c === 35 /* # */) {
				authorityEnd = i;
				break;
			}
		}
	}

	let result = url;

	if (atIdx !== -1) {
		// Find the first `:` between userinfo-start and the authority `@`.
		// That opens the password segment. (Subsequent `:` chars inside
		// the password are part of the password and stay.)
		let colonIdx = -1;
		for (let i = userinfoStart; i < atIdx; i++) {
			if (url.charCodeAt(i) === 58 /* : */) { colonIdx = i; break; }
		}
		// Only redact a non-empty password segment. `user:@host` (empty
		// password) and `user@host` (no password) pass through untouched.
		if (colonIdx !== -1 && atIdx > colonIdx + 1) {
			result = result.slice(0, colonIdx + 1) + '***' + result.slice(atIdx);
		}
	}

	// Query-string password redaction. Match `(^|&)pass(word)?=` and `&pwd=`
	// case-insensitively, replacing the value up to the next `&` or end of
	// the query region with `***`. The query region is between the first
	// `?` after the authority and the first `#` after that `?` (or end of
	// string). A `?` that appears AFTER `#` is part of the fragment and
	// not a query separator. Indices are located on the POST-redaction
	// string: userinfo redaction above changes the string length whenever
	// the password is not exactly 3 chars, so an offset captured from the
	// original url would silently miss the real `?` and leak the query
	// password.
	const redactedAuthorityEnd = authorityEnd - (url.length - result.length);
	const hashStart = result.indexOf('#', Math.max(redactedAuthorityEnd, 0));
	const queryHorizon = hashStart === -1 ? result.length : hashStart;
	const queryStart = result.indexOf('?', Math.max(redactedAuthorityEnd, 0));
	if (queryStart !== -1 && queryStart < queryHorizon) {
		const queryEnd = queryHorizon;
		const query = result.slice(queryStart + 1, queryEnd);
		// The value runs to the next `&` - or to the first character that
		// cannot appear in a query at all. RFC 3986 query = pchar / "/" / "?",
		// so `"` backtick `< > \ { } | ^ [ ]` are all invalid there, while the
		// sub-delims `'()!,;=$*+` ARE valid and must stay inside the match.
		// The userinfo scan above cannot use the same set (sub-delims are
		// legal in a password), which is why the two positions differ. Without
		// this, a value with no trailing `&` ran to the whitespace bound and
		// ate the rest of the line - `?password=x","user":"bob"` collapsed to
		// `?password=***`, destroying real data to hide a credential.
		const newQuery = query.replace(
			/(^|&)(pass(?:word)?|pwd)=[^&"`<>\\{}|^[\]\s]+/gi,
			'$1$2=***'
		);
		if (newQuery !== query) {
			result = result.slice(0, queryStart + 1) + newQuery + result.slice(queryEnd);
		}
	}

	return result;
}

/**
 * Create a one-shot warner for sensitive userData keys. The returned
 * function recursively scans up to depth 3, calls console.warn on the
 * first match it finds, and latches so subsequent calls are no-ops.
 *
 * @param {string} prefix - Module label, e.g. "redis/cursor".
 */
export function createSensitiveWarner(prefix) {
	let warned = false;
	function warn(data, depth) {
		if (warned || !data || typeof data !== 'object') return;
		if (depth === undefined) depth = 0;
		if (depth > 3) return;
		if (Array.isArray(data)) {
			for (let i = 0; i < data.length; i++) {
				warn(data[i], depth + 1);
				if (warned) return;
			}
			return;
		}
		for (const k of Object.keys(data)) {
			if (SENSITIVE_WARN_RE.test(k)) {
				console.warn(
					`[${prefix}] userData key "${k}" looks sensitive; ` +
					'use the select option to strip it before broadcasting\n' +
					'  See: https://svti.me/userdata-sensitive'
				);
				warned = true;
				return;
			}
			if (typeof data[k] === 'object' && data[k] !== null) {
				warn(data[k], depth + 1);
				if (warned) return;
			}
		}
	}
	return warn;
}
