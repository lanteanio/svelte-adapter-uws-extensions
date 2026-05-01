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
 * Drops keys starting with "__" (internal adapter state) or matching
 * SENSITIVE_STRIP_RE. Cycle-safe via a per-call WeakSet.
 *
 * @param {unknown} obj
 * @param {WeakSet<object>} [ancestors]
 */
export function stripInternal(obj, ancestors) {
	if (!obj || typeof obj !== 'object') return obj;
	if (!ancestors) ancestors = new WeakSet();
	if (ancestors.has(obj)) return undefined;
	ancestors.add(obj);
	let result;
	if (Array.isArray(obj)) {
		result = obj.map((v) => stripInternal(v, ancestors));
	} else {
		result = {};
		for (const k of Object.keys(obj)) {
			if (k.startsWith('__') || SENSITIVE_STRIP_RE.test(k)) continue;
			const v = obj[k];
			result[k] = (v && typeof v === 'object') ? stripInternal(v, ancestors) : v;
		}
	}
	ancestors.delete(obj);
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
					'use the select option to strip it before broadcasting'
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
