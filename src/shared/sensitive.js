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
 * Cycle-safe via a per-call WeakSet.
 *
 * @param {unknown} obj
 * @param {WeakSet<object>} [ancestors]
 */
export function stripInternal(obj, ancestors) {
	if (!obj || typeof obj !== 'object') return obj;
	if (ArrayBuffer.isView(obj) || obj instanceof ArrayBuffer) {
		const len = /** @type {{ byteLength: number }} */ (obj).byteLength;
		return '[bytes: ' + len + ']';
	}
	if (!ancestors) ancestors = new WeakSet();
	if (ancestors.has(obj)) return undefined;
	ancestors.add(obj);
	let result;
	if (Array.isArray(obj)) {
		result = obj.map((v) => stripInternal(v, ancestors));
	} else {
		result = {};
		for (const k of Object.keys(obj)) {
			if (k.startsWith('__') || k === 'constructor' || k === 'prototype' || SENSITIVE_STRIP_RE.test(k)) continue;
			const v = obj[k];
			result[k] = (v && typeof v === 'object') ? stripInternal(v, ancestors) : v;
		}
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
 *
 * @param {unknown} url
 * @returns {string}
 */
export function redactConnectionUrl(url) {
	if (typeof url !== 'string' || url.length === 0) return String(url);

	const protoEnd = url.indexOf('://');
	if (protoEnd === -1) return url;
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
	// not a query separator.
	const hashStart = result.indexOf('#', authorityEnd);
	const queryHorizon = hashStart === -1 ? result.length : hashStart;
	const queryStart = result.indexOf('?', authorityEnd);
	if (queryStart !== -1 && queryStart < queryHorizon) {
		const queryEnd = queryHorizon;
		const query = result.slice(queryStart + 1, queryEnd);
		const newQuery = query.replace(/(^|&)(pass(?:word)?|pwd)=[^&]+/gi, '$1$2=***');
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
