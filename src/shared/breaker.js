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

import { MAX_BREAKER_LISTENERS, MAX_BREAKER_KEYS } from './caps.js';
import { setTimer, clearTimer } from './runtime.js';

export class CircuitBrokenError extends Error {
	constructor() {
		super('circuit breaker is open - backend unavailable');
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
 * Per-KEY circuit breaker. State is partitioned by an optional string key, so one
 * key (e.g. a tenant) can break without tripping the others. The default key `''`
 * is the single global breaker - every method is byte-identical for callers that
 * pass no key, so existing shared-infra call sites are unchanged. A caller
 * protecting a tenant-specific resource passes the tenant id to isolate its state.
 *
 * @typedef {Object} CircuitBreaker
 * @property {'healthy' | 'broken' | 'probing'} state - State of the default (`''`) key
 * @property {boolean} isHealthy - True only when the default key's state === 'healthy'
 * @property {number} failures - Default key's consecutive failure count
 * @property {(key?: string) => ('healthy' | 'broken' | 'probing')} stateOf - State for a given key
 * @property {(key?: string) => number} failuresOf - Failure count for a given key
 * @property {(key?: string) => void} guard - Throws CircuitBrokenError if that key is broken
 * @property {(key?: string) => void} success - Record a successful operation for a key
 * @property {(err?: any, key?: string) => void} failure - Record a failed operation for a key
 * @property {(key?: string) => void} reset - Force a key back to healthy
 * @property {(handler: (from: string, to: string) => void) => () => void} subscribe - Register a state-transition listener; returns an unsubscribe function
 * @property {() => void} destroy - Clear internal timers (all keys)
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
 * success/failure after. Pass null/undefined breaker to skip. The optional `key`
 * partitions the breaker state (default `''` = the global breaker) - shared-infra
 * call sites pass no key; a caller protecting a tenant-specific resource passes the
 * tenant id so its failures cannot trip another tenant's breaker.
 */
export async function withBreaker(b, fn, key) {
	if (!b) return fn();
	b.guard(key);
	try {
		const result = await fn();
		b.success(key);
		return result;
	} catch (err) {
		b.failure(err, key);
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

	// Per-key state. The default key '' is the single global breaker - every existing
	// (no-key) caller hits exactly this one slot, so behavior is byte-identical.
	/** @type {Map<string, { state: string, failures: number, probeAllowed: boolean, resetTimer: any }>} */
	const states = new Map();
	function stateFor(key) {
		const k = key || '';
		let s = states.get(k);
		if (!s) {
			// Backstop against unbounded key growth: at the cap, evict the oldest
			// non-default keyed state (the '' global key is never evicted). A healthy
			// evicted key simply recreates on next access; an evicted broken key is
			// treated as healthy until it re-breaks - bounded graceful degradation,
			// the same trade-off as the rate-limiter's maxBuckets.
			if (states.size >= MAX_BREAKER_KEYS) {
				for (const existing of states.keys()) {
					if (existing !== '') {
						const old = states.get(existing);
						if (old && old.resetTimer) clearTimer(old.resetTimer);
						states.delete(existing);
						break;
					}
				}
			}
			s = { state: 'healthy', failures: 0, probeAllowed: false, resetTimer: null };
			states.set(k, s);
		}
		return s;
	}

	const listeners = new Set();
	if (onStateChange) listeners.add(onStateChange);

	function transition(s, to) {
		const from = s.state;
		if (from === to) return;
		s.state = to;
		for (const listener of listeners) {
			try { listener(from, to); } catch { /* don't let one listener break the others */ }
		}
	}

	function scheduleProbe(s) {
		clearTimer(s.resetTimer);
		s.resetTimer = setTimer(() => {
			s.resetTimer = null;
			s.probeAllowed = true;
			transition(s, 'probing');
		}, resetTimeout);
		if (s.resetTimer.unref) s.resetTimer.unref();
	}

	return {
		get state() { return stateFor('').state; },
		get isHealthy() { return stateFor('').state === 'healthy'; },
		get failures() { return stateFor('').failures; },
		stateOf(key) { return stateFor(key).state; },
		failuresOf(key) { return stateFor(key).failures; },

		guard(key) {
			const s = stateFor(key);
			if (s.state === 'healthy') return;
			if (s.state === 'probing' && s.probeAllowed) {
				s.probeAllowed = false;
				return;
			}
			throw new CircuitBrokenError();
		},

		success(key) {
			const s = stateFor(key);
			if (s.state === 'probing') {
				clearTimer(s.resetTimer);
				s.resetTimer = null;
				s.failures = 0;
				transition(s, 'healthy');
			} else if (s.state === 'healthy') {
				s.failures = 0;
			}
		},

		failure(err, key) {
			const s = stateFor(key);
			if (s.failures < failureThreshold) s.failures++;
			if (s.state === 'probing') {
				transition(s, 'broken');
				scheduleProbe(s);
			} else if (s.state === 'healthy' && s.failures >= failureThreshold) {
				transition(s, 'broken');
				scheduleProbe(s);
			}
		},

		reset(key) {
			const s = stateFor(key);
			clearTimer(s.resetTimer);
			s.resetTimer = null;
			s.failures = 0;
			s.probeAllowed = false;
			transition(s, 'healthy');
		},

		subscribe(handler) {
			if (typeof handler !== 'function') {
				throw new Error('circuit breaker: subscribe handler must be a function');
			}
			if (listeners.size >= MAX_BREAKER_LISTENERS) {
				throw new Error(
					'circuit breaker: listener count exceeded ' + MAX_BREAKER_LISTENERS +
					' on this breaker - a leak signal, since each module wires at most one'
				);
			}
			listeners.add(handler);
			return () => listeners.delete(handler);
		},

		destroy() {
			for (const s of states.values()) {
				clearTimer(s.resetTimer);
				s.resetTimer = null;
			}
		}
	};
}
