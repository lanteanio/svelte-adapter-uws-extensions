/**
 * Sensitive-key helpers. Used framework-side to keep credential-shaped
 * keys (`token`, `secret`, `password`, etc.) out of broadcast payloads
 * and log lines, and exposed publicly so app-level code can apply the
 * same redaction rules to its own log surfaces.
 */

/**
 * Match keys that look like auth/session credentials. Excludes the bare
 * substring "key" because legitimate id-like fields often contain it.
 * Used by `stripInternal()` to redact userData before it leaves the
 * process.
 */
export const SENSITIVE_STRIP_RE: RegExp;

/**
 * Same as `SENSITIVE_STRIP_RE`, plus "key". Used only for warnings;
 * legitimate id-like fields often contain "key" so we tell the developer
 * instead of silently dropping the field.
 */
export const SENSITIVE_WARN_RE: RegExp;

/**
 * Recursively strip internal/sensitive keys from a user-supplied object.
 * Drops keys starting with `__` (internal adapter state) or matching
 * `SENSITIVE_STRIP_RE`. Cycle-safe via a per-call `WeakSet`.
 *
 * Returns `obj` unchanged when it is not a non-null object. Returns
 * `undefined` when called on an object already in the ancestor chain
 * (cycle break).
 */
export function stripInternal(obj: unknown, ancestors?: WeakSet<object>): unknown;

/**
 * Redact the password segment of a connection URL so the URL is safe to
 * embed in log lines and error messages. Matches the standard
 * `scheme://user:password@host[:port][/path]` shape and substitutes the
 * password for `***`. Other URL forms (no userinfo, no password, query
 * string with `@`, path-segment colons) pass through untouched. Defensive
 * against non-string input so callers can pipe through arbitrary error
 * context without a type guard.
 *
 * @example
 * redactConnectionUrl('redis://:s3cret@redis.internal:6379');
 * // => 'redis://:***@redis.internal:6379'
 */
export function redactConnectionUrl(url: unknown): string;

/**
 * Create a one-shot warner for sensitive userData keys. The returned
 * function recursively scans up to depth 3, calls `console.warn` on the
 * first match it finds, and latches so subsequent calls are no-ops.
 *
 * @param prefix - Module label, e.g. `"redis/cursor"`.
 */
export function createSensitiveWarner(
	prefix: string
): (data: unknown, depth?: number) => void;
