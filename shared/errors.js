/**
 * Thrown when a connection to an external service (Redis, Postgres) fails.
 */
export class ConnectionError extends Error {
	/**
	 * @param {string} service - e.g. 'redis', 'postgres'
	 * @param {string} detail
	 * @param {Error} [cause]
	 */
	constructor(service, detail, cause) {
		super(`${service}: ${detail}`);
		this.name = 'ConnectionError';
		this.service = service;
		if (cause) this.cause = cause;
	}
}

/**
 * Thrown when an operation times out.
 */
export class TimeoutError extends Error {
	/**
	 * @param {string} service
	 * @param {string} operation
	 * @param {number} ms
	 */
	constructor(service, operation, ms) {
		super(`${service}: ${operation} timed out after ${ms}ms`);
		this.name = 'TimeoutError';
		this.service = service;
		this.ms = ms;
	}
}

/**
 * Thrown when an idempotency-store `commit(result)` is called with a
 * payload whose JSON-encoded byte length exceeds the store's
 * `maxResultBytes` cap. Cross-backend (Redis + Postgres) so consumer
 * code can pattern-match on `err.code === 'IDEMPOTENCY_RESULT_TOO_LARGE'`
 * regardless of which backend they wired.
 *
 * The cap defaults to 256 KB - the operational shape Redis pubsub /
 * Postgres NOTIFY and the cluster bus can absorb without becoming
 * meta-stable. Operators with legitimately larger payloads opt up via
 * the `maxResultBytes` option at store construction.
 */
export class IdempotencyResultTooLargeError extends Error {
	/**
	 * @param {number} bytes - Actual JSON-encoded byte length of the result.
	 * @param {number} maxBytes - Configured cap.
	 */
	constructor(bytes, maxBytes) {
		super(`idempotency: result payload ${bytes} bytes exceeds maxResultBytes ${maxBytes}`);
		this.name = 'IdempotencyResultTooLargeError';
		this.code = 'IDEMPOTENCY_RESULT_TOO_LARGE';
		this.bytes = bytes;
		this.maxBytes = maxBytes;
	}
}

/**
 * Thrown by RPC-shaped operations (`presence.join`, `cursor.attach`) when the
 * caller's websocket closes during an async gap before the operation could
 * commit, OR the websocket was already gone by the time the operation
 * resumed from one of its awaits. Server-side state is fully rolled back
 * before the throw so the caller does not need to compensate.
 *
 * Stable contract: `err.code === 'WS_CLOSED'`. Catch on the code, not the
 * class - future RPC-shaped operations that hit the same pattern throw the
 * same code. The `operation` field carries the dotted path (e.g.
 * `'presence.join'`) for operators that want to bucket by feature without
 * parsing the message.
 *
 * Pattern in callers:
 *
 * ```js
 * try {
 *   await presence.join(ws, topic, platform);
 * } catch (err) {
 *   if (err.code === 'WS_CLOSED') return; // ws already gone, no compensation needed
 *   throw err;
 * }
 * ```
 */
export class WsClosedError extends Error {
	/**
	 * @param {string} operation - Dotted operation path, e.g. `'presence.join'`.
	 * @param {string} topic
	 */
	constructor(operation, topic) {
		super(`${operation}: websocket closed during async gap (topic="${topic}"); rolled back`);
		this.name = 'WsClosedError';
		this.code = 'WS_CLOSED';
		this.operation = operation;
		this.topic = topic;
	}
}
