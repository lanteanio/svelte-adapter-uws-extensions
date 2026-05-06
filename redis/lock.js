/**
 * Cluster-wide mutual-exclusion primitive for svelte-adapter-uws.
 *
 * `withLock(key, fn)` serializes access per key across every instance.
 * Distinct from `redis/fence` (B2c): fence is task-runner-specific
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

import { randomBytes } from 'node:crypto';
import { assert } from '../shared/assert.js';
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
 * @property {(key: string) => string} [mapKey] - Map lock key names to bounded label values for cardinality control on the `lock_acquired_total` / `lock_lost_total` / `lock_acquire_timeouts_total` counters. Default: identity.
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
		this.key = key;
		this.waitedMs = waitedMs;
	}
}

export class LockLostError extends Error {
	constructor(key) {
		super(`lock: lost ownership of "${key}" (heartbeat detected force-takeover or TTL elapsed)`);
		this.name = 'LockLostError';
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
	const mapKey = options.mapKey || ((k) => k);
	const breaker = options.breaker;
	const redis = client.redis;

	const m = options.metrics;
	const mAcquired = m?.counter('lock_acquired_total', 'Locks acquired by key class', ['key_class']);
	const mWait = m?.histogram('lock_acquire_wait_ms', 'Time waited from withLock call to successful acquire (ms)');
	const mTimeouts = m?.counter('lock_acquire_timeouts_total', 'withLock calls that exceeded maxWaitMs without acquiring', ['key_class']);
	const mLost = m?.counter('lock_lost_total', 'Locks lost mid-flight via heartbeat detection', ['key_class']);

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

		// Acquire loop
		const start = Date.now();
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
				mWait?.observe(Date.now() - start);
				mAcquired?.inc({ key_class: keyClass });
				break;
			}
			const elapsed = Date.now() - start;
			if (elapsed >= callMaxWaitMs) {
				mTimeouts?.inc({ key_class: keyClass });
				throw new LockAcquireTimeoutError(key, elapsed);
			}
			await sleepWithAbort(Math.min(retryDelayMs, callMaxWaitMs - elapsed), externalSignal);
		}

		// Held: start heartbeat, run user fn under combined signal, release.
		const controller = new AbortController();
		const onExternalAbort = () => controller.abort(externalSignal?.reason);
		if (externalSignal) {
			if (externalSignal.aborted) controller.abort(externalSignal.reason);
			else externalSignal.addEventListener('abort', onExternalAbort, { once: true });
		}

		let lost = false;
		const heartbeatTimer = setInterval(async () => {
			try {
				const r = await redis.eval(HEARTBEAT_SCRIPT, 1, fullK, fenceToken, ttlMs);
				breaker?.success();
				if (Number(r) !== 1) {
					lost = true;
					mLost?.inc({ key_class: keyClass });
					clearInterval(heartbeatTimer);
					controller.abort(new LockLostError(key));
				}
			} catch (err) {
				breaker?.failure(err);
				// A flaky heartbeat does not abort by itself -- the next tick
				// retries. The TTL is still on track unless heartbeats stay
				// failing past `ttlMs`, at which point the next tick will
				// observe the absent key and abort cleanly.
			}
		}, heartbeatMs);
		if (heartbeatTimer.unref) heartbeatTimer.unref();

		try {
			return await fn(controller.signal);
		} finally {
			clearInterval(heartbeatTimer);
			if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
			// `lost === true` is set only inside the heartbeat callback,
			// which then calls controller.abort(...). The signal must
			// always be aborted by the time we observe `lost`. The reverse
			// is not true: an external signal may have aborted the
			// controller without lostLock firing.
			assert(
				!lost || controller.signal.aborted,
				'lock.heartbeat.signal-aborted-iff-lost',
				{ key }
			);
			if (!lost) {
				try {
					await redis.eval(RELEASE_SCRIPT, 1, fullK, fenceToken);
					breaker?.success();
				} catch (err) {
					breaker?.failure(err);
					// Best-effort: a release failure leaves the key in place
					// until its TTL elapses. The compare-and-delete shape means
					// we never accidentally release someone else's lock.
				}
			}
		}
	}

	return { withLock };
}

function sleepWithAbort(ms, signal) {
	if (ms <= 0) return Promise.resolve();
	return new Promise((resolve, reject) => {
		const t = setTimeout(() => {
			if (signal) signal.removeEventListener('abort', onAbort);
			resolve();
		}, ms);
		if (t.unref) t.unref();
		const onAbort = () => {
			clearTimeout(t);
			reject(signal.reason || new Error('lock: aborted while waiting'));
		};
		if (signal) {
			if (signal.aborted) {
				clearTimeout(t);
				reject(signal.reason || new Error('lock: aborted while waiting'));
				return;
			}
			signal.addEventListener('abort', onAbort, { once: true });
		}
	});
}
