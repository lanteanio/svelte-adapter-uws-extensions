/**
 * Inbound bus envelope validation. Bus subscribers (Redis pub/sub,
 * Postgres LISTEN/NOTIFY, etc.) accept messages from a shared transport
 * and forward them to the local platform via `activePlatform.publish(...)`.
 * Without validation, any actor with bus write access (compromised app,
 * shared Redis ACL, accidental public binding, hostile DBA) can inject a
 * giant envelope, send a non-string topic/event that crashes downstream
 * `esc(topic)` paths, or fan an attacker-controlled topic to every
 * cluster node from a single PUBLISH.
 *
 * The wire-level subscribe gate in the adapter blocks client-initiated
 * subscribes to `__`-prefixed system topics by default; this helper
 * hardens the bus side as defense in depth.
 *
 * Each bus subscriber holds one validator instance and calls its methods
 * in the message handler before forwarding to the local platform.
 */

export interface BusValidatorOptions {
	/**
	 * Reject envelopes whose raw bytes exceed this length. Applied BEFORE
	 * `JSON.parse` so the parser never sees attacker-stuffed payloads.
	 * @default 1048576 (1 MB)
	 */
	maxBytes?: number;

	/**
	 * When `false` (default), reject any topic starting with `__` apart
	 * from the explicit allowlist. Foreign actors with bus write access
	 * cannot inject forged `__signal:*` / `__rpc` / plugin-internal
	 * frames into the local platform. Apps that legitimately bus-relay
	 * user-defined `__`-prefixed topics (rare) can opt back in via
	 * `allowSystemTopics: true`.
	 * @default false
	 */
	allowSystemTopics?: boolean;

	/**
	 * Exact-match topic names that bypass the system-topic denylist. The
	 * bus owner adds its own `systemChannel` here (e.g. `__realtime`) so
	 * the bus can relay its own degraded / recovered events even when
	 * external `__` topics are denied.
	 */
	allowedSystemTopics?: string[];
}

export interface BusValidator {
	/**
	 * Pre-parse size guard. Pass the byte length of the raw message (not
	 * the parsed object). Returns `true` to proceed, `false` to drop the
	 * message (the caller should bump a parse-error metric and skip).
	 *
	 * @deprecated Prefer `acceptRaw(message)` - it computes the byte
	 * length internally and removes the off-by-encoding foot-gun where a
	 * caller passes `.length` (character count) on a UTF-8 string with
	 * multi-byte characters.
	 */
	acceptSize(bytes: number): boolean;

	/**
	 * Pre-parse size guard that computes byte length internally from the
	 * raw message. Accepts `string` (encoded as UTF-8 byte length),
	 * `Buffer`, `Uint8Array`, or any other `ArrayBuffer` view. Returns
	 * `false` for any other input shape (drop the message).
	 *
	 * Preferred over `acceptSize(bytes)` for all new bus subscribers - no
	 * caller-side `Buffer.byteLength` boilerplate, no risk of passing
	 * `.length` on a UTF-8 string with multi-byte characters and under-
	 * counting the actual byte length.
	 */
	acceptRaw(message: string | Buffer | Uint8Array | ArrayBufferView | unknown): boolean;

	/**
	 * Post-parse envelope check. Returns `true` if the envelope is safe
	 * to republish on the local platform. Validates topic shape (string,
	 * 1-256 chars, no control chars), event shape (string, 1-256 chars),
	 * and the `__`-prefix policy.
	 *
	 * The `data` field is not inspected: any JSON-encodable shape is
	 * acceptable. Apps that need stricter data validation should wrap
	 * their own check around the platform.publish path.
	 */
	acceptEnvelope(topic: unknown, event: unknown): boolean;
}

/**
 * Mirror of the adapter's `isValidWireTopic` for its unconditional
 * rejections. Returns `true` for valid topic strings: non-empty, at
 * most 256 characters, no control bytes (charCode < 32), no `"` (34)
 * or `\\` (92). The quote / backslash rejection matches the adapter's
 * `esc(topic)` rejection set, so a topic that survives this validator
 * is also safe to embed in a JSON envelope.
 *
 * Non-ASCII rejection is opt-in on the adapter side and is NOT mirrored
 * here, since server-side code legitimately produces topics with
 * localized labels (e.g. `__signal:José`) that the bus relays.
 */
export function isValidBusTopic(topic: unknown): boolean;

/**
 * Build a per-bus validator. Each subscriber holds one instance and
 * calls its methods in the message handler before forwarding to the
 * local platform.
 */
export function createBusValidator(options?: BusValidatorOptions): BusValidator;
