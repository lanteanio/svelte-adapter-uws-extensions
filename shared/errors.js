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
