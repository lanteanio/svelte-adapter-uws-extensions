/**
 * Cluster-wide COOPERATIVE locking primitive for svelte-adapter-uws.
 *
 * `withLock(key, fn)` serializes access per key across every instance while
 * every holder stays responsive to its lease. It is a TTL lease with
 * cooperative release, NOT a hard fencing mutex: a holder that stalls past
 * `ttlMs` (GC pause, blocked loop) loses the lease while still running, and a
 * successor can acquire and overlap it. The `AbortSignal` tells such a holder
 * to bail, but is advisory. For strict single-writer correctness across a
 * stall, make the protected resource reject stale writes, or use the
 * task runner (Postgres fence). The internal fence token guards release and
 * heartbeat only; it is not surfaced to `fn`.
 *
 * Distinct from `redis/fence`: fence is task-runner-specific
 * (one fence per `taskId`, paired with the Postgres state machine);
 * this lock is a general-purpose primitive any user code can grab.
 *
 * Backing primitive: `SET <key> <fenceToken> NX PX <ttlMs>` to acquire,
 * Lua-atomic `if get == fenceToken then del end` to release. While the
 * holder is running, a heartbeat tick refreshes the TTL via Lua-atomic
 * `if get == fenceToken then pexpire end`. If the heartbeat reports
 * "no longer ours" (operator force-takeover, TTL elapsed before our
 * heartbeat could refresh, etc.), the supplied `AbortSignal` fires so
 * the user code can bail.
 *
 * @module svelte-adapter-uws-extensions/redis/lock
 */

import { assert } from '../shared/assert.js';
import { evalCached } from '../shared/eval-cached.js';
import {
	randomBytes,
	randomFloat,
	monotonicNow,
	setTimer,
	clearTimer
} from '../shared/runtime.js';
import {
	LEASE_RENEW_SCRIPT as HEARTBEAT_SCRIPT,
	LEASE_RELEASE_SCRIPT as RELEASE_SCRIPT
} from '../shared/lease-scripts.js';

/**
 * @typedef {Object} DistributedLockOptions
 * @property {string} [keyPrefix='lock:'] - Prefix prepended (after the client keyPrefix) to every lock key.
 * @property {number} [defaultTtlMs=30000] - Default TTL on a held lock in milliseconds. Heartbeat refreshes this back to the original value before it elapses. Override per call via `withLock(key, fn, { ttlMs })`.
 * @property {number} [retryDelayMs=50] - Sleep between acquire retries when the key is held by another instance.
 * @property {number} [maxWaitMs=5000] - Total time to wait for the key to free up before throwing a `LockAcquireTimeoutError`. Override per call via `withLock(key, fn, { maxWaitMs })`.
 * @property {number} [heartbeatMs] - Interval at which the heartbeat refreshes the TTL. Defaults to `defaultTtlMs / 3` so a single missed beat still leaves margin before the TTL elapses.
 * @property {(key: string) => string} [mapKey] - Map lock key names to bounded label values for cardinality control on the `lock_acquired_total` / `lock_lost_total` / `lock_acquire_timeouts_total` counters. Default: the identifier-free class "all".
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

const DEFAULT_KEY_PREFIX = 'lock:';
const DEFAULT_TTL_MS = 30_000;
const DEFAULT_RETRY_DELAY_MS = 50;
const DEFAULT_MAX_WAIT_MS = 5000;

export class LockAcquireTimeoutError extends Error {
	constructor(key, waitedMs) {
		super(`lock: failed to acquire "${key}" within ${waitedMs}ms`);
		this.name = 'LockAcquireTimeoutError';
		this.code = 'LOCK_ACQUIRE_TIMEOUT';
		this.key = key;
		this.waitedMs = waitedMs;
	}
}

export class LockLostError extends Error {
	constructor(key) {
		super(`lock: lost ownership of "${key}" (lease ownership changed or TTL elapsed)`);
		this.name = 'LockLostError';
		this.code = 'LOCK_LOST';
		this.key = key;
	}
}

/**
 * Create a cluster-wide distributed lock factory.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {DistributedLockOptions} [options]
 *
 * @example
 * ```js
 * import { createDistributedLock } from 'svelte-adapter-uws-extensions/redis/lock';
 *
 * const lock = createDistributedLock(redis, {
 *   defaultTtlMs: 30_000,
 *   maxWaitMs: 5000
 * });
 *
 * await lock.withLock('order-42', async (signal) => {
 *   // serialized cluster-wide. Bail when `signal.aborted` if the heartbeat
 *   // detects we lost ownership mid-flight.
 * });
 * ```
 */
export function createDistributedLock(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('lock: client (from createRedisClient) is required');
	}
	const keyPrefix = options.keyPrefix !== undefined ? String(options.keyPrefix) : DEFAULT_KEY_PREFIX;
	const defaultTtlMs = options.defaultTtlMs ?? DEFAULT_TTL_MS;
	if (!Number.isFinite(defaultTtlMs) || defaultTtlMs < 1) {
		throw new Error('lock: defaultTtlMs must be a positive number (ms)');
	}
	const retryDelayMs = options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
	if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
		throw new Error('lock: retryDelayMs must be a non-negative number (ms)');
	}
	const maxWaitMs = options.maxWaitMs ?? DEFAULT_MAX_WAIT_MS;
	if (!Number.isFinite(maxWaitMs) || maxWaitMs < 0) {
		throw new Error('lock: maxWaitMs must be a non-negative number (ms)');
	}
	const heartbeatMs = options.heartbeatMs ?? Math.max(1, Math.floor(defaultTtlMs / 3));
	if (!Number.isFinite(heartbeatMs) || heartbeatMs < 1) {
		throw new Error('lock: heartbeatMs must be a positive number (ms)');
	}
	if (options.mapKey !== undefined && typeof options.mapKey !== 'function') {
		throw new Error('lock: mapKey must be a function');
	}
	const mapKey = options.mapKey || (() => 'all');
	const breaker = options.breaker;
	const redis = client.redis;

	const m = options.metrics;
	const mAcquired = m?.counter('lock_acquired_total', 'Locks acquired by key class', ['key_class']);
	const mWait = m?.histogram('lock_acquire_wait_seconds', 'Time waited from withLock call to successful acquire (seconds)', [],
		[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]);
	const mTimeouts = m?.counter('lock_acquire_timeouts_total', 'withLock calls that exceeded maxWaitMs without acquiring', ['key_class']);
	const mLost = m?.counter('lock_lost_total', 'Locks whose lease ownership was lost', ['key_class']);

	function fullKey(key) {
		return client.key(keyPrefix + key);
	}

	async function withLock(key, fn, callOpts = {}) {
		if (typeof key !== 'string' || key.length === 0) {
			throw new Error('lock.withLock: key must be a non-empty string');
		}
		if (typeof fn !== 'function') {
			throw new Error('lock.withLock: fn must be a function');
		}
		const ttlMs = callOpts.ttlMs ?? defaultTtlMs;
		if (!Number.isFinite(ttlMs) || ttlMs < 1) {
			throw new Error('lock.withLock: ttlMs must be a positive number (ms)');
		}
		const callMaxWaitMs = callOpts.maxWaitMs ?? maxWaitMs;
		if (!Number.isFinite(callMaxWaitMs) || callMaxWaitMs < 0) {
			throw new Error('lock.withLock: maxWaitMs must be a non-negative number (ms)');
		}
		const externalSignal = callOpts.signal;
		if (externalSignal && externalSignal.aborted) {
			throw externalSignal.reason || new Error('lock.withLock: aborted before acquire');
		}

		const fullK = fullKey(key);
		const keyClass = mapKey(key);
		const fenceToken = randomBytes(16).toString('hex');

		// Acquire loop. Timing uses `monotonicNow()` (a `performance.now()`-backed
		// counter) so a backward NTP step between successive reads cannot make
		// elapsed appear negative (extending the timeout indefinitely) or
		// produce a histogram observation that underflows the bucket boundaries.
		const start = monotonicNow();
		let leaseDeadline = start + ttlMs, acknowledgedAt;
		while (true) {
			let result;
			try {
				result = await redis.set(fullK, fenceToken, 'NX', 'PX', ttlMs);
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
			if (result === 'OK') {
				acknowledgedAt = monotonicNow();
				mWait?.observe((acknowledgedAt - start) / 1000);
				mAcquired?.inc({ key_class: keyClass });
				if (acknowledgedAt >= leaseDeadline) {
					mLost?.inc({ key_class: keyClass });
					try { await evalCached(redis, RELEASE_SCRIPT, 1, fullK, fenceToken); } catch (error) { breaker?.failure(error); }
					throw new LockLostError(key);
				}
				break;
			}
			const elapsed = monotonicNow() - start;
			if (elapsed >= callMaxWaitMs) {
				mTimeouts?.inc({ key_class: keyClass });
				throw new LockAcquireTimeoutError(key, elapsed);
			}
			// Jittered backoff: spread N contending callers across the retry
			// window so a single key-handoff does not produce an N-way SET-NX
			// burst on the Redis socket. A uniform float is the right primitive
			// (thundering-herd avoidance, not security-relevant). +/-25% of
			// retryDelayMs preserves the expected wait while smearing actual
			// delays over [0.75x, 1.25x] of the configured base.
			const jitter = (randomFloat() - 0.5) * 0.5 * retryDelayMs;
			const delay = Math.max(0, Math.min(retryDelayMs + jitter, callMaxWaitMs - elapsed));
			await sleepWithAbort(delay, externalSignal);
			leaseDeadline = monotonicNow() + ttlMs;
		}

		// Held: start heartbeat, run user fn under combined signal, release.
		const controller = new AbortController();
		const onExternalAbort = externalSignal ? () => controller.abort(externalSignal.reason) : null;
		if (externalSignal) {
			if (externalSignal.aborted) controller.abort(externalSignal.reason);
			else externalSignal.addEventListener('abort', onExternalAbort, { once: true });
		}

		let lost = false;
		let stopped = false;
		let settledAt;
		let lostError;
		// One timer caps its next wake-up at the last confirmed lease deadline,
		// including while a Redis reply is pending. Refreshes never queue up.
		let inFlight = null, finishWaiter;
		async function heartbeatTick() {
			try {
				if (lost || stopped) return;
				const sentAt = monotonicNow();
				const r = await evalCached(redis, HEARTBEAT_SCRIPT, 1, fullK, fenceToken, ttlMs);
				breaker?.success();
				if (lost || (stopped && !finishWaiter)) return;
				if (Number(r) !== 1 || (settledAt ?? monotonicNow()) >= leaseDeadline) {
					lost = true;
					lostError = new LockLostError(key);
					mLost?.inc({ key_class: keyClass });
					controller.abort(lostError);
				} else leaseDeadline = sentAt + ttlMs;
			} catch (err) {
				breaker?.failure(err);
				// An error does not extend the deadline. A later refresh may still
				// recover within it; the deadline-capped timer detects expiry.
			} finally {
				inFlight = null;
				finishWaiter?.();
				finishWaiter = undefined;
			}
		}
		let heartbeatTimer;
		function heartbeatDue() {
			if (lost) return;
			const remaining = leaseDeadline - monotonicNow();
			if (remaining <= 0) {
				if (stopped && settledAt < leaseDeadline) {
					finishWaiter?.();
					finishWaiter = undefined;
					return;
				}
				lost = true;
				lostError = new LockLostError(key);
				mLost?.inc({ key_class: keyClass });
				controller.abort(lostError);
				finishWaiter?.();
				finishWaiter = undefined;
			} else {
				if (!stopped && !inFlight) inFlight = Promise.resolve().then(heartbeatTick).catch(() => {});
				heartbeatTimer = setTimer(heartbeatDue, Math.min(heartbeatMs, remaining));
				if (heartbeatTimer.unref) heartbeatTimer.unref();
			}
		}
		heartbeatTimer = setTimer(heartbeatDue, Math.min(heartbeatMs, leaseDeadline - acknowledgedAt));
		if (heartbeatTimer.unref) heartbeatTimer.unref();

		let result;
		try {
			result = await fn(controller.signal);
		} finally {
			settledAt = monotonicNow();
			stopped = true;
			// Keep deadline observation alive while an admitted refresh settles.
			// Expiry releases this wait even if the transport never replies.
			if (inFlight && !lost) {
				if (settledAt >= leaseDeadline) {
					lost = true;
					lostError = new LockLostError(key);
					mLost?.inc({ key_class: keyClass });
					controller.abort(lostError);
				} else await new Promise(resolve => { finishWaiter = resolve; });
			}
			clearTimer(heartbeatTimer);
			if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
			// A loss verdict always calls controller.abort(...). The signal must
			// always be aborted by the time we observe `lost`. The reverse
			// is not true: an external signal may have aborted the
			// controller without lostLock firing. No context: the lock key is the
			// application's, and it can embed a user or an order id.
			assert(!lost || controller.signal.aborted, 'lock.heartbeat.signal-aborted-iff-lost');
			if (!lost && inFlight) {
				// The callback completed inside its lease, but its refresh never
				// replied. Release best-effort without delaying completed work.
				void evalCached(redis, RELEASE_SCRIPT, 1, fullK, fenceToken).catch(err => breaker?.failure(err));
			} else if (!lost) {
				try {
					const released = await evalCached(redis, RELEASE_SCRIPT, 1, fullK, fenceToken);
					if (Number(released) !== 1) {
						lost = true;
						lostError = new LockLostError(key);
						mLost?.inc({ key_class: keyClass });
						controller.abort(lostError);
					}
					breaker?.success();
				} catch (err) {
					breaker?.failure(err);
					// Cleanup latency cannot invalidate work completed inside the lease.
					if (settledAt >= leaseDeadline) {
						lost = true;
						lostError = new LockLostError(key);
						mLost?.inc({ key_class: keyClass });
						controller.abort(lostError);
					}
				}
			}
		}
		if (lost) throw lostError;
		return result;
	}

	return { withLock };
}

function sleepWithAbort(ms, signal) {
	if (ms <= 0) return Promise.resolve();
	return new Promise((resolve, reject) => {
		const t = setTimer(() => {
			if (signal) signal.removeEventListener('abort', onAbort);
			resolve();
		}, ms);
		if (t.unref) t.unref();
		const onAbort = () => {
			clearTimer(t);
			reject(signal.reason || new Error('lock: aborted while waiting'));
		};
		if (signal) {
			if (signal.aborted) {
				clearTimer(t);
				reject(signal.reason || new Error('lock: aborted while waiting'));
				return;
			}
			signal.addEventListener('abort', onAbort, { once: true });
		}
	});
}
