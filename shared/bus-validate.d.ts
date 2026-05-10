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
	 * When `false`, reject any topic starting with `__` (apart from the
	 * explicit allowlist). Apps that publish only over user-space topics
	 * may set this for defense in depth on top of the wire-level
	 * subscribe gate.
	 * @default true
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
	 */
	acceptSize(bytes: number): boolean;

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
 * Mirror of the adapter's `isValidWireTopic` (kept inline here to avoid
 * importing deep into the adapter package). Returns `true` for valid
 * topic strings: non-empty, at most 256 characters, no control bytes
 * (charCode < 32).
 */
export function isValidBusTopic(topic: unknown): boolean;

/**
 * Build a per-bus validator. Each subscriber holds one instance and
 * calls its methods in the message handler before forwarding to the
 * local platform.
 */
export function createBusValidator(options?: BusValidatorOptions): BusValidator;
