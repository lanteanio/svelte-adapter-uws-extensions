/**
 * Cluster clock: multi-source fused time with a non-decreasing clamp, a
 * cluster-consistent reading anchored on the shared Redis server clock, and an
 * optional leader-stamped timestamp authority for clustered event ordering.
 *
 * Builds on the same measurement the clock-skew sampler takes (round-trip
 * compensated Redis `TIME`, median over several reads to reject jitter) and
 * extends it three ways:
 *
 * - `now()` - the robust local wall estimate: the median of the available
 *   clock sources {local wall, Redis-corrected, NTP-corrected}, clamped so a
 *   returned value never regresses. One wrong source (an NTP step on the local
 *   clock, a drifting Redis host) cannot swing the median when three vote.
 * - `consistent()` - the cluster-consistent reading: the local wall clock
 *   corrected onto the shared Redis server clock, clamped non-decreasing.
 *   Every instance's `consistent()` converges to the same reference within
 *   round-trip error, which is the property cross-instance lease/expiry/
 *   ordering math actually needs (a per-instance `now()` cannot give it).
 * - `stamp()` - the event-timestamp authority. In leader mode only the leader
 *   assigns timestamps from its own clock; followers reproduce the leader's
 *   clock through the shared Redis reference (leader publishes its offset,
 *   followers apply it to their own Redis-corrected reading - "ask the leader
 *   for its time, compensated for network skew" without a per-stamp round
 *   trip), cached with a TTL and refreshed in the background sampling round.
 *   Without a leader, `stamp()` is `consistent()`.
 *
 * The first sample runs at construction by default, which IS the automatic
 * startup drift check: a local clock more than `warnMs` (default 100 ms) off
 * the Redis reference fires `onWarn` immediately, `tripMs` (default 500 ms)
 * fires `onTrip`, and `tripped()` flips true - wire that into admission
 * control with the `{clockTripped}` rule (`shared/admission.js`) so a machine
 * whose clock is about to corrupt leases stops admitting work.
 *
 * Determinism: every wall read goes through this package's runtime seam
 * (`wallEpoch`) and the mock Redis serves `TIME` from the same seam clock, so
 * a simulation reproduces (or deliberately injects) skew; the NTP source is
 * injectable the same way. The sampling interval unrefs its timer, so the
 * clock never holds a process (or a simulated scheduler) open.
 *
 * Relationship to `redis/clock-skew`: the sampler stays the minimal
 * observability tool (gauge + warn/trip callbacks + the W118 fence
 * hysteresis). This module is the time SOURCE built on the same measurement.
 * They can run side by side; the shared `platform_clock_skew_ms` gauge is
 * registry-deduplicated.
 *
 * @module svelte-adapter-uws-extensions/redis/clock
 */

import { wallEpoch, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';

const DEFAULT_INTERVAL_MS = 30_000;
const DEFAULT_SAMPLES = 5;
const DEFAULT_WARN_MS = 100;
const DEFAULT_TRIP_MS = 500;
const DEFAULT_DRIFT_THRESHOLD_MS = 100;
const DEFAULT_LEADER_KEY = 'clock:leader-offset';

/**
 * Read the Redis server clock once as epoch milliseconds.
 * `TIME` returns `[seconds, microseconds]`; ioredis surfaces them as strings.
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<number>}
 */
async function readRedisEpochMs(redis) {
	const res = await redis.time();
	return Number(res[0]) * 1000 + Math.floor(Number(res[1]) / 1000);
}

/**
 * Median of a numeric array. Does not mutate the input.
 * @param {number[]} values - Non-empty.
 * @returns {number}
 */
function median(values) {
	const sorted = values.slice().sort((a, b) => a - b);
	const mid = sorted.length >> 1;
	return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

/**
 * A per-surface non-decreasing clamp: returns the input unless it would
 * regress relative to this surface's previously returned value, in which case
 * the previous value holds until real time catches up.
 * @returns {(v: number) => number}
 */
function monotoneClamp() {
	let last = -Infinity;
	return (v) => {
		if (v < last) return last;
		last = v;
		return v;
	};
}

/**
 * @typedef {Object} ClusterClockOptions
 * @property {number} [intervalMs=30000] - Background sampling cadence.
 * @property {number} [samples=5] - Redis `TIME` reads per round (median-reduced). Must be >= 1.
 * @property {number} [warnMs=100] - Absolute Redis skew (ms) at or above which `onWarn` fires (below `tripMs`).
 * @property {number} [tripMs=500] - Absolute Redis skew (ms) at or above which `onTrip` fires and `tripped()` reads true.
 * @property {number} [driftThresholdMs=100] - Max pairwise source disagreement (ms) at or above which `onDrift` fires.
 * @property {boolean} [immediate=true] - Take the first sample at construction (the automatic startup drift check).
 * @property {import('./ntp-source.js').NtpSource} [ntp] - Optional third clock source (one read per round). Off by default; a failed read drops the source from the median until it reads again.
 * @property {{ isLeader: () => boolean }} [leader] - Leader election handle (`createLeader(...)`) enabling leader-stamped `stamp()`.
 * @property {string} [leaderKey='clock:leader-offset'] - Key the leader publishes its clock offset under.
 * @property {number} [leaderTtlMs=3*intervalMs] - Freshness bound for both the published offset (PX) and a follower's cached copy; a staler cache falls back to `consistent()`.
 * @property {(skewMs: number) => void} [onSample] - After every successful round, with the signed median Redis skew (positive = local ahead).
 * @property {(skewMs: number) => void} [onWarn]
 * @property {(skewMs: number) => void} [onTrip]
 * @property {(drift: { driftMs: number, sources: { redis: number, ntp: number | null } }) => void} [onDrift] - When two sources disagree by `driftThresholdMs` or more.
 * @property {(err: Error) => void} [onError] - Redis/NTP/leader read-write failures. The last good state is retained.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} ClusterClock
 * @property {() => number} now - Fused multi-source wall clock, non-decreasing. Before the first sample: the seam wall clock.
 * @property {() => number} consistent - Cluster-consistent reading (Redis-anchored), non-decreasing. Before the first sample: falls back to `now()`.
 * @property {() => number} stamp - Leader-stamped event timestamp (leader mode), else `consistent()`. Non-decreasing.
 * @property {() => boolean} ready - True once the first successful sample landed.
 * @property {() => number | null} skew - Most recent signed Redis skew (ms), or null before the first sample.
 * @property {() => boolean} tripped - True while the last sampled |skew| >= tripMs. For the admission `{clockTripped}` rule.
 * @property {() => number | null} drift - Most recent max pairwise source disagreement (ms), or null before the first sample.
 * @property {() => Promise<number | null>} sample - Run one round now; resolves with the signed Redis skew (null on failure).
 * @property {() => Promise<void>} stop - Stop the interval and await any in-flight round. Idempotent.
 */

/**
 * Create a cluster clock. Starts sampling immediately (the startup drift
 * check); call `stop()` on shutdown.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {ClusterClockOptions} [options]
 * @returns {ClusterClock}
 *
 * @example
 * ```js
 * import { createClusterClock, attachClusterClock } from 'svelte-adapter-uws-extensions/redis/clock';
 * import { createLeader } from 'svelte-adapter-uws-extensions/redis/leader';
 *
 * const leader = createLeader(redis, { key: 'clock-leader' });
 * const clock = createClusterClock(redis, { leader, onTrip: (ms) => console.error(`clock ${ms}ms off - check NTP`) });
 * attachClusterClock(platform, clock);
 * // ...
 * const ts = platform.clusterClock.stamp(); // leader-authoritative event time
 * ```
 */
export function createClusterClock(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('cluster clock: client (from createRedisClient) is required');
	}
	const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
	if (!Number.isFinite(intervalMs) || intervalMs < 1) {
		throw new Error('cluster clock: intervalMs must be a positive number (ms)');
	}
	const samples = options.samples ?? DEFAULT_SAMPLES;
	if (!Number.isInteger(samples) || samples < 1) {
		throw new Error('cluster clock: samples must be a positive integer');
	}
	const warnMs = options.warnMs ?? DEFAULT_WARN_MS;
	const tripMs = options.tripMs ?? DEFAULT_TRIP_MS;
	if (!Number.isFinite(warnMs) || warnMs < 0 || !Number.isFinite(tripMs) || tripMs < 0) {
		throw new Error('cluster clock: warnMs and tripMs must be non-negative numbers (ms)');
	}
	const driftThresholdMs = options.driftThresholdMs ?? DEFAULT_DRIFT_THRESHOLD_MS;
	if (!Number.isFinite(driftThresholdMs) || driftThresholdMs < 0) {
		throw new Error('cluster clock: driftThresholdMs must be a non-negative number (ms)');
	}
	for (const name of ['onSample', 'onWarn', 'onTrip', 'onDrift', 'onError']) {
		if (options[name] !== undefined && typeof options[name] !== 'function') {
			throw new Error(`cluster clock: ${name} must be a function`);
		}
	}
	const ntp = options.ntp;
	if (ntp !== undefined && (typeof ntp !== 'object' || ntp === null || typeof ntp.read !== 'function')) {
		throw new Error('cluster clock: ntp must be a source with read() => Promise<epochMs> (see redis/ntp-source)');
	}
	const leader = options.leader;
	if (leader !== undefined && (typeof leader !== 'object' || leader === null || typeof leader.isLeader !== 'function')) {
		throw new Error('cluster clock: leader must expose isLeader() (see redis/leader createLeader)');
	}
	const leaderKey = options.leaderKey !== undefined ? String(options.leaderKey) : DEFAULT_LEADER_KEY;
	if (leaderKey.length === 0) {
		throw new Error('cluster clock: leaderKey must be a non-empty string');
	}
	const leaderTtlMs = options.leaderTtlMs ?? intervalMs * 3;
	if (!Number.isFinite(leaderTtlMs) || leaderTtlMs < 1) {
		throw new Error('cluster clock: leaderTtlMs must be a positive number (ms)');
	}

	const redis = client.redis;
	const breaker = options.breaker;
	const { onSample, onWarn, onTrip, onDrift, onError } = options;

	const m = options.metrics;
	const mSkew = m?.gauge(
		'platform_clock_skew_ms',
		'Signed clock skew of this instance wall clock relative to the Redis server clock, in milliseconds (positive = local clock ahead of Redis). The median of several round-trip-compensated Redis TIME reads.'
	);
	const mDrift = m?.gauge(
		'platform_clock_drift_ms',
		'Maximum pairwise disagreement between this instance clock sources (local wall, Redis TIME, NTP if configured), in milliseconds. High drift means at least one source is wrong.'
	);

	/** @type {number | null} signed ms, positive = local ahead of Redis */
	let redisSkew = null;
	/** @type {number | null} signed ms, positive = local ahead of NTP */
	let ntpSkew = null;
	/** @type {number | null} */
	let lastDrift = null;
	/** @type {{ o: number, atMs: number } | null} follower cache of the leader's published offset */
	let leaderOffset = null;
	let stopped = false;
	let timer = null;
	let inFlight = null;

	const clampNow = monotoneClamp();
	const clampConsistent = monotoneClamp();
	const clampStamp = monotoneClamp();

	/** Fused multi-source wall estimate, unclamped. */
	function fusedRaw() {
		const local = wallEpoch();
		const estimates = [local];
		if (redisSkew !== null) estimates.push(local - redisSkew);
		if (ntpSkew !== null) estimates.push(local - ntpSkew);
		return median(estimates);
	}

	/** Redis-anchored reading, unclamped; falls back to the fused estimate. */
	function consistentRaw() {
		return redisSkew === null ? fusedRaw() : wallEpoch() - redisSkew;
	}

	/** Run one sampling round. Resolves to the signed Redis skew, or null on failure. */
	async function sample() {
		if (stopped) return redisSkew;
		try {
			const estimates = [];
			for (let i = 0; i < samples; i++) {
				// Round-trip compensation: the server timestamp corresponds to an
				// instant between the local reads bracketing the call, so the local
				// clock at the read instant is best estimated by their midpoint
				// (assumes symmetric RTT, the standard NTP first-order estimate).
				const t0 = wallEpoch();
				const redisMs = await readRedisEpochMs(redis);
				const t1 = wallEpoch();
				estimates.push(t0 + (t1 - t0) / 2 - redisMs);
			}
			breaker?.success();
			redisSkew = median(estimates);
		} catch (err) {
			breaker?.failure(err);
			// Keep the last good state rather than fabricating a zero.
			if (onError) { try { onError(err); } catch { /* swallow */ } }
			return null;
		}
		mSkew?.set(redisSkew);

		// The NTP source is auxiliary: a failed read drops it from the median
		// (stale offsets must not keep voting) but never fails the round.
		if (ntp) {
			try {
				const t0 = wallEpoch();
				const ntpMs = await ntp.read();
				const t1 = wallEpoch();
				ntpSkew = t0 + (t1 - t0) / 2 - ntpMs;
			} catch (err) {
				ntpSkew = null;
				if (onError) { try { onError(err); } catch { /* swallow */ } }
			}
		}

		// Pairwise disagreement across the available sources. The local source
		// is the zero point, so local-vs-X is |skew_X| and redis-vs-ntp is the
		// skew difference.
		let drift = Math.abs(redisSkew);
		if (ntpSkew !== null) {
			drift = Math.max(drift, Math.abs(ntpSkew), Math.abs(redisSkew - ntpSkew));
		}
		lastDrift = drift;
		mDrift?.set(drift);

		if (onSample) { try { onSample(redisSkew); } catch { /* swallow */ } }
		const abs = Math.abs(redisSkew);
		if (abs >= tripMs) {
			if (onTrip) { try { onTrip(redisSkew); } catch { /* swallow */ } }
		} else if (abs >= warnMs) {
			if (onWarn) { try { onWarn(redisSkew); } catch { /* swallow */ } }
		}
		if (drift >= driftThresholdMs && onDrift) {
			try { onDrift({ driftMs: drift, sources: { redis: redisSkew, ntp: ntpSkew } }); } catch { /* swallow */ }
		}

		// Leader half of the round (best-effort; a failure keeps the last
		// cached state). The leader publishes its clock as an offset RELATIVE
		// TO THE SHARED REDIS REFERENCE, so a follower reproduces the leader's
		// clock by applying that offset to its OWN Redis-corrected reading -
		// both sides compensate their network skew independently and meet at
		// the reference.
		if (leader) {
			try {
				if (leader.isLeader()) {
					const o = fusedRaw() - (wallEpoch() - redisSkew);
					await redis.set(leaderKey, JSON.stringify({ o }), 'PX', Math.ceil(leaderTtlMs));
					leaderOffset = null; // a (re)elected leader stamps from its own clock
				} else {
					const raw = await redis.get(leaderKey);
					if (raw != null) {
						const parsed = JSON.parse(raw);
						if (parsed && typeof parsed.o === 'number' && Number.isFinite(parsed.o)) {
							leaderOffset = { o: parsed.o, atMs: wallEpoch() };
						}
					}
				}
			} catch (err) {
				if (onError) { try { onError(err); } catch { /* swallow */ } }
			}
		}
		return redisSkew;
	}

	if (options.immediate !== false) {
		// The automatic startup drift check: the gauge, warn/trip callbacks and
		// tripped() are live within one Redis round-trip of construction.
		inFlight = sample().catch(() => {});
	} else {
		inFlight = Promise.resolve();
	}

	timer = setIntervalTimer(() => {
		// Chain so a slow round never overlaps itself.
		inFlight = inFlight.then(sample).catch(() => {});
	}, intervalMs);
	if (timer.unref) timer.unref();

	async function stop() {
		if (stopped) return;
		stopped = true;
		if (timer) {
			clearIntervalTimer(timer);
			timer = null;
		}
		try { await inFlight; } catch { /* swallow */ }
	}

	return {
		now: () => clampNow(fusedRaw()),
		consistent: () => clampConsistent(consistentRaw()),
		stamp() {
			if (leader) {
				if (leader.isLeader()) return clampStamp(fusedRaw());
				if (leaderOffset !== null && wallEpoch() - leaderOffset.atMs <= leaderTtlMs) {
					return clampStamp(consistentRaw() + leaderOffset.o);
				}
			}
			return clampStamp(consistentRaw());
		},
		ready: () => redisSkew !== null,
		skew: () => redisSkew,
		tripped: () => redisSkew !== null && Math.abs(redisSkew) >= tripMs,
		drift: () => lastDrift,
		sample,
		stop
	};
}

/**
 * Attach a cluster clock to the platform as the `clusterClock` convention, so
 * framework layers (and app code holding a wrapped platform) can read
 * cluster-consistent / leader-stamped time without importing this package.
 * `bus.wrap` forwards it, mirroring `clockFence`.
 *
 * @param {any} platform
 * @param {ClusterClock} clock
 */
export function attachClusterClock(platform, clock) {
	if (!platform || typeof platform !== 'object') {
		throw new Error('cluster clock: attachClusterClock requires a platform object');
	}
	if (!clock || typeof clock.now !== 'function' || typeof clock.stamp !== 'function') {
		throw new Error('cluster clock: attachClusterClock requires a clock from createClusterClock');
	}
	platform.clusterClock = {
		now: clock.now,
		consistent: clock.consistent,
		stamp: clock.stamp,
		ready: clock.ready,
		tripped: clock.tripped
	};
	return platform;
}
