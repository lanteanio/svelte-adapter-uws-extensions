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
 * Drops keys starting with `__` (internal adapter state - catches
 * `__proto__` so the result is safe to spread / `Object.assign` without
 * triggering prototype pollution), keys matching `SENSITIVE_STRIP_RE`
 * (token/secret/password/auth/session/cookie/jwt/credential), and the
 * `constructor` / `prototype` keys (defense-in-depth against shadow-via-
 * `Object.assign` on consumer targets).
 *
 * Binary views (`Buffer`, `TypedArray`, `DataView`, `ArrayBuffer`) are
 * substituted with a `"[bytes: <byteLength>]"` placeholder string rather
 * than walked as objects: a naive walk would JSON.stringify each byte by
 * its stringified index, leaking raw bytes (which may include auth
 * tokens) into log lines.
 *
 * Cycle-safe via a per-call `WeakSet`.
 *
 * Returns `obj` unchanged when it is not a non-null object. Returns
 * `undefined` when called on an object already in the ancestor chain
 * (cycle break). Returns a `string` for binary-view input.
 */
export function stripInternal(obj: unknown, ancestors?: WeakSet<object>): unknown;

/**
 * Redact the password segment of a connection URL so the URL is safe to
 * embed in log lines and error messages. Substitutes the userinfo
 * password with `***` and any `password=` / `pass=` / `pwd=` query-string
 * value with `***`. Other URL bytes (scheme, user, host, port, path,
 * fragment, non-password query params) pass through unchanged. Defensive
 * against non-string input so callers can pipe through arbitrary error
 * context without a type guard.
 *
 * Implementation is a byte-level scan (not URL parsing) so it correctly
 * redacts:
 *
 * - **Passwords containing `@`** (`redis://user:p@ssword@host`). Walks
 *   to the last `@` in the authority region.
 * - **IPv6 hosts** (`redis://:secret@[::1]:6379`). Suspends
 *   authority-terminator detection inside `[...]`.
 * - **Query-string passwords** (`postgres://host/db?password=hunter2`).
 *   Case-insensitive match on `password` / `pass` / `pwd` keys.
 *
 * @example
 * redactConnectionUrl('redis://:s3cret@redis.internal:6379');
 * // => 'redis://:***@redis.internal:6379'
 *
 * redactConnectionUrl('redis://user:p@ssword@[::1]:6379');
 * // => 'redis://user:***@[::1]:6379'
 *
 * redactConnectionUrl('postgres://host/db?sslmode=require&password=hunter2');
 * // => 'postgres://host/db?sslmode=require&password=***'
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
