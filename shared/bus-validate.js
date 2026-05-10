/**
 * Inbound bus envelope validation.
 *
 * Bus subscribers (Redis pub/sub, Postgres LISTEN/NOTIFY, etc.) accept
 * messages from a shared transport and forward them to the local
 * platform via `activePlatform.publish(...)`. Without validation, any
 * actor with bus write access (compromised app, shared Redis ACL,
 * accidental public binding, hostile DBA) can:
 *
 *   - Inject a giant envelope that ties up the worker on JSON.parse +
 *     republish (DoS).
 *   - Send a non-string `topic` / `event` that crashes downstream
 *     `esc(topic)` / `JSON.stringify` paths.
 *   - Send a topic with control characters that survives the bus and
 *     surprises log dashboards / admin tools.
 *   - Inject an attacker-controlled topic on every cluster node from a
 *     single PUBLISH.
 *
 * The wire-level subscribe gate in adapter handler.js blocks
 * client-initiated subscribes to `__`-prefixed system topics by
 * default, so the cross-cluster fan-out chain is already broken at the
 * subscribe side. This helper hardens the bus side as defense in
 * depth: size cap, topic/event shape validation, optional system-topic
 * denylist with an explicit allowlist for the bus's own internal
 * channels (e.g. pubsub's `__realtime` degraded/recovered events).
 */

const DEFAULT_MAX_ENVELOPE_BYTES = 1024 * 1024; // 1 MB
const MAX_TOPIC_LEN = 256;
const MAX_EVENT_LEN = 256;

/**
 * Mirror of svelte-adapter-uws/files/utils.js#isValidWireTopic. Kept
 * inline here so the extensions package does not import deep into the
 * adapter (peer dependency). The two are tested for parity.
 *
 * @param {unknown} topic
 * @returns {boolean}
 */
export function isValidBusTopic(topic) {
	if (typeof topic !== 'string' || topic.length === 0 || topic.length > MAX_TOPIC_LEN) return false;
	for (let i = 0; i < topic.length; i++) {
		if (topic.charCodeAt(i) < 32) return false;
	}
	return true;
}

/**
 * @typedef {Object} BusValidatorOptions
 * @property {number} [maxBytes=1048576] - Reject envelopes whose raw
 *   bytes exceed this length. Applied BEFORE JSON.parse so the parser
 *   never sees attacker-stuffed payloads.
 * @property {boolean} [allowSystemTopics=true] - When false, reject
 *   any topic starting with `__` (apart from the explicit allowlist).
 *   Apps that publish only over user-space topics may set this for
 *   defense in depth on top of the wire-level subscribe gate.
 * @property {string[]} [allowedSystemTopics=[]] - Exact-match topic
 *   names that bypass the system-topic denylist. The bus owner adds
 *   its own `systemChannel` here (e.g. `__realtime`) so the bus can
 *   relay its own degraded/recovered events even when external `__`
 *   topics are denied.
 */

/**
 * Build a per-bus validator. Each subscriber holds one instance and
 * calls its methods in the message handler before forwarding to the
 * local platform.
 *
 * @param {BusValidatorOptions} [options]
 */
export function createBusValidator(options = {}) {
	const maxBytes = typeof options.maxBytes === 'number' && options.maxBytes > 0
		? options.maxBytes
		: DEFAULT_MAX_ENVELOPE_BYTES;
	const allowSystemTopics = options.allowSystemTopics !== false;
	/** @type {Set<string>} */
	const explicitAllow = new Set(Array.isArray(options.allowedSystemTopics) ? options.allowedSystemTopics : []);

	return {
		/**
		 * Pre-parse size guard. Pass the byte length of the raw message
		 * (not the parsed object). Returns true to proceed, false to
		 * drop the message (the caller should bump a parse-error metric
		 * and skip).
		 *
		 * @param {number} bytes
		 */
		acceptSize(bytes) {
			return typeof bytes === 'number' && bytes >= 0 && bytes <= maxBytes;
		},

		/**
		 * Post-parse envelope check. Returns true if the envelope is
		 * safe to republish on the local platform. Validates:
		 *  - topic shape (string, 1-256 chars, no control chars)
		 *  - event shape (string, 1-256 chars)
		 *  - `__`-prefix policy (off when allowSystemTopics=false unless
		 *    the topic is in the explicit allowlist)
		 *
		 * The `data` field is not inspected: any JSON-encodable shape
		 * is acceptable. Apps that need stricter data validation should
		 * wrap their own check around the platform.publish path.
		 *
		 * @param {unknown} topic
		 * @param {unknown} event
		 */
		acceptEnvelope(topic, event) {
			if (!isValidBusTopic(topic)) return false;
			if (typeof event !== 'string' || event.length === 0 || event.length > MAX_EVENT_LEN) return false;
			if (!allowSystemTopics &&
				/** @type {string} */ (topic).charCodeAt(0) === 95 &&
				/** @type {string} */ (topic).charCodeAt(1) === 95 &&
				!explicitAllow.has(/** @type {string} */ (topic))) {
				return false;
			}
			return true;
		}
	};
}
