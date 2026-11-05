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
 * non-password query params) pass through unchanged where boundaries are
 * unambiguous. Query passwords can consume trailing free text because drivers
 * accept whitespace inside them. Defensive against
 * non-string input so callers can pipe through arbitrary error context
 * without a type guard.
 *
 * Implementation is a byte-level scan (not URL parsing) so it correctly
 * redacts:
 *
 * - **Passwords containing `@`** (`redis://user:p@ssword@host`). A naive
 *   first-`@` regex stops too early and leaks the tail of the password.
 *   The scan walks to the LAST `@` in the authority region.
 * - **IPv6 hosts** (`redis://:secret@[::1]:6379`). Host brackets are
 *   preserved, while raw brackets in userinfo remain credential bytes.
 * - **Query-string passwords** (`postgres://host/db?password=hunter2`).
 *   pg accepts `password` as a connection parameter; redaction is
 *   case-insensitive and matches `password` / `pass` / `pwd`.
 * - **Every URL in the string, not just the first.** Callers pass whole
 *   error messages and whole stack traces, which routinely carry the DSN
 *   more than once; redacting only the first occurrence leaks the rest.
 *   The scan does not stop at whitespace: a userinfo password runs to the
 *   last `@` before the next `/`, `?` or `#`, and a query password to the
 *   next `&` or `#` or the end of the string. JSON containers are decoded
 *   per string so unrelated fields and formatting are retained.
 *
 * @param {unknown} url
 * @returns {string}
 */
export function redactConnectionUrl(url) {
	if (typeof url !== 'string' || url.length === 0) return String(url);
	// Decode valid JSON strings before looking for schemes, including schemes
	// whose slashes are escaped. Preserve all other fields and formatting.
	if (/^\s*[\[{\"]/.test(url)) {
		try {
			JSON.parse(url);
			return url.replace(/"(?:[^"\\]|\\.)*"/g, token => {
				const value = JSON.parse(token);
				const safe = redactConnectionUrl(value);
				return safe === value ? token : JSON.stringify(safe);
			});
		} catch { /* free text: use conservative credential boundaries */ }
	}

	// WHATWG parsing removes these controls even inside schemes and keys.
	// Keep an offset map only when needed, so the output retains original
	// non-credential text rather than normalizing a whole diagnostic.
	let scan = url;
	let offsets = null;
	if (/[\t\n\r]/.test(url)) {
		offsets = [];
		const chars = [];
		for (let i = 0; i < url.length; i++) {
			const code = url.charCodeAt(i);
			if (code === 9 || code === 10 || code === 13) continue;
			chars.push(url[i]);
			offsets.push(i);
		}
		scan = chars.join('');
	}
	if (!scan.includes('://')) return url;

	// Collect ranges on the original scan before removing anything. A query
	// password can contain another apparent URL, whose own password must also
	// be found even when its scheme falls inside the first redacted range.
	const boundaries = [];
	for (let i = 0; i < scan.length; i++) {
		if (scan[i] === '&' || scan[i] === '#') boundaries.push(i);
	}
	boundaries.push(scan.length);
	let boundary = 0;
	const ranges = [];
	function hide(start, end) {
		if (start >= end) return;
		const previous = ranges[ranges.length - 1];
		if (previous && start <= previous[1]) previous[1] = Math.max(previous[1], end);
		else ranges.push([start, end]);
	}
	let active = false, query = false, fragment = false, pendingQuery = false, field = -1;
	for (let i = 0; i < scan.length; i++) {
		while (boundaries[boundary] <= i) boundary++;
		const char = scan[i];
		if (char === ':' && scan[i + 1] === '/' && scan[i + 2] === '/') {
			active = true;
			// A scheme inside a query value cannot end the enclosing query.
			// Its own first ? can also introduce an embedded URL's query.
			pendingQuery = true;
			fragment = false;
			field = -1;
			let colon = -1, at = -1;
			for (let j = i + 3; j < scan.length; j++) {
				const part = scan[j];
				if (part === '/' || part === '?' || part === '#') break;
				if (part === ':' && colon === -1) colon = j;
				if (part === '@') at = j;
			}
			if (colon !== -1 && at > colon + 1) hide(colon + 1, at);
			continue;
		}
		if (!active) continue;
		if (char === '#') { fragment = true; query = false; pendingQuery = false; field = -1; }
		else if (char === '?' && (!query || pendingQuery) && !fragment) { query = true; pendingQuery = false; field = i + 1; }
		else if (char === '&' && query) field = i + 1;
		else if (char === '=' && field !== -1) {
			if (isPasswordParameter(scan.slice(field, i))) hide(i + 1, boundaries[boundary]);
			field = -1;
		}
	}
	let result = '', cursor = 0;
	for (const [start, end] of ranges) {
		const originalStart = offsets ? offsets[start] : start;
		const originalEnd = offsets ? (offsets[end] ?? url.length) : end;
		result += url.slice(cursor, originalStart) + '***';
		cursor = originalEnd;
	}
	return result + url.slice(cursor);
}

/** @param {string} key */
function isPasswordParameter(key) {
	try { return /^(?:password|pass|pwd)$/i.test(decodeURIComponent(key.replace(/\+/g, ' '))); }
	catch { return false; }
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
					`[extensions] [${prefix}] userData key "${k}" looks sensitive; ` +
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
