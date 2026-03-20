/**
 * Circuit breaker for svelte-adapter-uws-extensions.
 *
 * Prevents thundering herd when a backend (Redis, Postgres) goes down.
 * Extensions opt in via a `breaker` option. When the circuit is broken,
 * awaited operations fail fast instead of timing out, and fire-and-forget
 * operations are skipped entirely.
 *
 * Three states:
 *   - healthy:  everything works, requests go through
 *   - broken:   too many failures, requests fail fast
 *   - probing:  one request is allowed through to test if the backend is back
 *
 * @module svelte-adapter-uws-extensions/breaker
 */

export class CircuitBrokenError extends Error {
	constructor() {
		super('circuit breaker is open — backend unavailable');
		this.name = 'CircuitBrokenError';
	}
}

/**
 * @typedef {Object} CircuitBreakerOptions
 * @property {number} [failureThreshold=5] - Consecutive failures before breaking
 * @property {number} [resetTimeout=30000] - Ms before transitioning from broken to probing
 * @property {(from: string, to: string) => void} [onStateChange] - Called on state transitions
 */

/**
 * @typedef {Object} CircuitBreaker
 * @property {'healthy' | 'broken' | 'probing'} state
 * @property {boolean} isHealthy - True only when state === 'healthy'
 * @property {number} failures - Current consecutive failure count
 * @property {() => void} guard - Throws CircuitBrokenError if broken
 * @property {() => void} success - Record a successful operation
 * @property {(err?: any) => void} failure - Record a failed operation
 * @property {() => void} reset - Force back to healthy
 * @property {() => void} destroy - Clear internal timers
 */

/**
 * Create a circuit breaker.
 *
 * @param {CircuitBreakerOptions} [options]
 * @returns {CircuitBreaker}
 *
 * @example
 * ```js
 * import { createCircuitBreaker } from 'svelte-adapter-uws-extensions/breaker';
 *
 * const breaker = createCircuitBreaker({ failureThreshold: 5, resetTimeout: 30000 });
 *
 * // Pass to extensions:
 * const presence = createPresence(redis, { breaker, key: 'id' });
 * const replay = createReplay(redis, { breaker });
 * ```
 */
/**
 * Run an async operation through a breaker. Guards before, records
 * success/failure after. Pass null/undefined breaker to skip.
 */
export async function withBreaker(b, fn) {
	if (!b) return fn();
	b.guard();
	try {
		const result = await fn();
		b.success();
		return result;
	} catch (err) {
		b.failure(err);
		throw err;
	}
}

export function createCircuitBreaker(options = {}) {
	const failureThreshold = options.failureThreshold ?? 5;
	const resetTimeout = options.resetTimeout ?? 30000;
	const onStateChange = options.onStateChange ?? null;

	if (!Number.isInteger(failureThreshold) || failureThreshold < 1) {
		throw new Error('circuit breaker: failureThreshold must be a positive integer');
	}
	if (typeof resetTimeout !== 'number' || !Number.isFinite(resetTimeout) || resetTimeout < 0) {
		throw new Error('circuit breaker: resetTimeout must be a non-negative number');
	}

	let state = 'healthy';
	let failures = 0;
	let probeAllowed = false;
	let resetTimer = null;

	function transition(to) {
		const from = state;
		if (from === to) return;
		state = to;
		if (onStateChange) onStateChange(from, to);
	}

	function scheduleProbe() {
		clearTimeout(resetTimer);
		resetTimer = setTimeout(() => {
			resetTimer = null;
			probeAllowed = true;
			transition('probing');
		}, resetTimeout);
		if (resetTimer.unref) resetTimer.unref();
	}

	return {
		get state() { return state; },
		get isHealthy() { return state === 'healthy'; },
		get failures() { return failures; },

		guard() {
			if (state === 'healthy') return;
			if (state === 'probing' && probeAllowed) {
				probeAllowed = false;
				return;
			}
			throw new CircuitBrokenError();
		},

		success() {
			if (state === 'probing') {
				clearTimeout(resetTimer);
				resetTimer = null;
				failures = 0;
				transition('healthy');
			} else if (state === 'healthy') {
				failures = 0;
			}
		},

		failure() {
			failures++;
			if (state === 'probing') {
				transition('broken');
				scheduleProbe();
			} else if (state === 'healthy' && failures >= failureThreshold) {
				transition('broken');
				scheduleProbe();
			}
		},

		reset() {
			clearTimeout(resetTimer);
			resetTimer = null;
			failures = 0;
			probeAllowed = false;
			transition('healthy');
		},

		destroy() {
			clearTimeout(resetTimer);
			resetTimer = null;
		}
	};
}
