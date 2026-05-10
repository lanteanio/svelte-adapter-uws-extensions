/**
 * Cluster-wide leader election via Redis lease.
 *
 * One worker across the cluster holds the lease at any moment; the
 * synchronous `isLeader()` getter is microsecond-cost and cached. Use
 * for cluster-wide singletons (cron schedulers, periodic cleanup,
 * health probes that should run from one place) where firing N times
 * across N workers would be wrong.
 *
 * Backing primitive: `SET <key> <instanceId> NX PX <leaseMs>` to acquire,
 * Lua-atomic compare-and-pexpire to renew, Lua-atomic compare-and-delete
 * to release. The compare-on-mutate guard means a stale renewal from a
 * worker that already lost leadership cannot extend somebody else's
 * lease, and a release call from one worker cannot delete somebody
 * else's lease.
 *
 * Failure model: fail-closed. A renewal that throws (Redis disconnect,
 * breaker open, network partition) drops `_isLeader` to false and surfaces
 * via `onError`; the renewal interval keeps ticking so leadership can
 * recover when Redis recovers. Errors never escape the interval - a
 * losing-Redis event must not crash the worker. Across the cluster, a
 * partitioned Redis means the lease expires server-side and no worker
 * holds leadership until the partition heals - jobs miss ticks rather
 * than double-fire.
 *
 * GC pause caveat: a long stop-the-world pause on the leader can cause
 * brief overlap with a freshly-elected successor. Recommend job
 * idempotency at the consumer; this primitive does not provide fencing
 * tokens (consumer sinks for cron-style work rarely have the
 * machinery to consume them anyway).
 *
 * @module svelte-adapter-uws-extensions/redis/leader
 */

import { randomBytes } from 'node:crypto';
import { LEASE_RENEW_SCRIPT, LEASE_RELEASE_SCRIPT } from '../shared/lease-scripts.js';

const DEFAULT_KEY = 'leader';
const DEFAULT_LEASE_MS = 30_000;

/**
 * @typedef {Object} LeaderOptions
 * @property {string} [key='leader'] - Redis key for the lease (prefixed by the client's keyPrefix).
 * @property {string} [instanceId] - This worker's identity. Default: `randomBytes(8).toString('hex')`. Override only if you want a stable identity for diagnostics; correctness does not depend on it.
 * @property {number} [leaseMs=30000] - TTL on the lease in milliseconds. Worst-case window between leader death and successor takeover.
 * @property {number} [renewMs] - Renewal interval in milliseconds. Must be `< leaseMs`. Default: `leaseMs / 3`. Also the interval at which non-leaders attempt fresh acquire.
 * @property {(err: Error) => void} [onError] - Called on every Redis failure (renewal, fresh acquire, release). Use for structured logging. Errors never escape the renewal interval regardless.
 * @property {(key: string) => string} [mapKey] - Map the lease key to a bounded label value for cardinality control on the four leader counters. Default: identity.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} Leader
 * @property {() => boolean} isLeader - Synchronous cached check. Microsecond-cost. Call at the top of every cron tick / scheduled job.
 * @property {() => Promise<string | null>} currentLeader - Single-GET diagnostic read of the current owner's instanceId. Returns null if unowned or on Redis failure.
 * @property {() => Promise<void>} stop - Stop the renewal interval and best-effort release the lease via compare-and-delete so a sibling can take over within `renewMs` instead of `leaseMs`. Idempotent. Never throws.
 * @property {string} instanceId - This worker's identity (provided or generated).
 * @property {string} key - The fully-prefixed lease key (useful for diagnostics).
 */

/**
 * Create a cluster-wide leader-election primitive.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {LeaderOptions} [options]
 * @returns {Leader}
 *
 * @example
 * ```js
 * import { createLeader } from 'svelte-adapter-uws-extensions/redis/leader';
 * import { configureCron } from 'svelte-realtime/server';
 *
 * const leader = createLeader(redis);
 * configureCron({ leader: leader.isLeader });
 *
 * // On clean shutdown, release the lease so a sibling takes over fast.
 * export async function shutdown() {
 *   await leader.stop();
 * }
 * ```
 */
export function createLeader(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('leader: client (from createRedisClient) is required');
	}
	const key = options.key !== undefined ? String(options.key) : DEFAULT_KEY;
	if (key.length === 0) {
		throw new Error('leader: key must be a non-empty string');
	}
	const instanceId = options.instanceId !== undefined
		? String(options.instanceId)
		: randomBytes(8).toString('hex');
	if (instanceId.length === 0) {
		throw new Error('leader: instanceId must be a non-empty string');
	}
	const leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
	if (!Number.isFinite(leaseMs) || leaseMs < 1) {
		throw new Error('leader: leaseMs must be a positive number (ms)');
	}
	const renewMs = options.renewMs ?? Math.max(1, Math.floor(leaseMs / 3));
	if (!Number.isFinite(renewMs) || renewMs < 1) {
		throw new Error('leader: renewMs must be a positive number (ms)');
	}
	if (renewMs >= leaseMs) {
		throw new Error('leader: renewMs must be < leaseMs (otherwise the lease can expire between renewals)');
	}
	if (options.onError !== undefined && typeof options.onError !== 'function') {
		throw new Error('leader: onError must be a function');
	}
	if (options.mapKey !== undefined && typeof options.mapKey !== 'function') {
		throw new Error('leader: mapKey must be a function');
	}

	const onError = options.onError;
	const mapKey = options.mapKey || ((k) => k);
	const breaker = options.breaker;
	const redis = client.redis;
	const fullKey = client.key(key);
	const keyClass = mapKey(key);

	const m = options.metrics;
	const mAcquired = m?.counter('leader_acquired_total', 'Leader acquisitions by key class', ['key_class']);
	const mLost = m?.counter('leader_lost_total', 'Leader losses by key class', ['key_class']);
	const mRenewals = m?.counter('leader_renewals_total', 'Successful renewals by key class', ['key_class']);
	const mRenewalFailures = m?.counter('leader_renewal_failures_total', 'Renewal calls that threw or returned 0 by key class', ['key_class']);

	let _isLeader = false;
	let stopped = false;
	let timer = null;
	let inFlight = null;

	function reportError(err) {
		mRenewalFailures?.inc({ key_class: keyClass });
		if (onError) {
			try { onError(err); } catch { /* swallow listener errors */ }
		}
	}

	async function tick() {
		if (stopped) return;
		try {
			if (_isLeader) {
				const r = await redis.eval(LEASE_RENEW_SCRIPT, 1, fullKey, instanceId, leaseMs);
				breaker?.success();
				if (Number(r) === 1) {
					mRenewals?.inc({ key_class: keyClass });
				} else {
					// Lease vanished or was taken over by another worker.
					_isLeader = false;
					mLost?.inc({ key_class: keyClass });
				}
			} else {
				const r = await redis.set(fullKey, instanceId, 'NX', 'PX', leaseMs);
				breaker?.success();
				if (r === 'OK') {
					_isLeader = true;
					mAcquired?.inc({ key_class: keyClass });
				}
			}
		} catch (err) {
			breaker?.failure(err);
			if (_isLeader) {
				_isLeader = false;
				mLost?.inc({ key_class: keyClass });
			}
			reportError(err);
		}
	}

	// Fire the first attempt immediately so leadership can be claimed
	// within one Redis round-trip of construction. Errors are caught
	// inside tick(); the unhandled-rejection guard here is belt-and-suspenders.
	inFlight = tick().catch(() => {});

	timer = setInterval(() => {
		// Chain so a slow tick never overlaps itself.
		inFlight = inFlight.then(tick).catch(() => {});
	}, renewMs);
	if (timer.unref) timer.unref();

	async function currentLeader() {
		try {
			const v = await redis.get(fullKey);
			breaker?.success();
			return v;
		} catch (err) {
			breaker?.failure(err);
			if (onError) {
				try { onError(err); } catch { /* swallow */ }
			}
			return null;
		}
	}

	async function stop() {
		if (stopped) return;
		stopped = true;
		if (timer) {
			clearInterval(timer);
			timer = null;
		}
		// Wait for any in-flight tick so we don't race with our own
		// renewal landing right as we try to release.
		try { await inFlight; } catch { /* swallow */ }

		if (_isLeader) {
			try {
				await redis.eval(LEASE_RELEASE_SCRIPT, 1, fullKey, instanceId);
				breaker?.success();
			} catch (err) {
				// Best-effort: a release failure leaves the key in place
				// until its TTL elapses. The compare-and-delete shape means
				// we never accidentally release someone else's lease.
				breaker?.failure(err);
			}
			_isLeader = false;
		}
	}

	return {
		isLeader: () => _isLeader,
		currentLeader,
		stop,
		instanceId,
		key: fullKey
	};
}
