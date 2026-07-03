/**
 * Clock-skew sampler: measures this instance's wall clock against the Redis
 * server clock and exposes it as a gauge.
 *
 * A timer-driven background task periodically issues Redis `TIME`, compares it
 * to this instance's wall clock with round-trip compensation, takes the median
 * of several reads to reject network jitter, and publishes the signed skew as
 * `platform_clock_skew_ms` (positive = local clock ahead of Redis). Skew that
 * crosses the warn / trip thresholds fires optional callbacks for alerting.
 *
 * Why this matters: features that order or expire events across workers (HLC
 * stamps, lease TTLs, replay windows) assume the fleet's wall clocks agree to
 * within a small bound. A drifting clock is silent until it corrupts ordering;
 * this sampler makes the drift observable before it does.
 *
 * Determinism: the wall clock is read through this package's runtime seam
 * (`wallEpoch`), so a simulation harness that drives the seam clock - and whose
 * Redis double returns the same virtual clock from `TIME` - reproduces (or
 * deliberately injects) skew without a real Redis. The sampler reads the EXACT
 * wall clock (`wallEpoch`), not the coarse ~1 Hz cached `now()`, because the
 * cache lag alone would otherwise register as hundreds of milliseconds of
 * phantom skew on a perfectly synchronized clock.
 *
 * Cluster note: on an ioredis Cluster client, `TIME` carries no key and is
 * routed to an arbitrary node, so successive reads may sample different nodes.
 * For a coarse drift gauge that is acceptable (the median still reflects the
 * fleet); pin to one node upstream if you need per-node attribution.
 *
 * @module svelte-adapter-uws-extensions/redis/clock-skew
 */

import { wallEpoch, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';

const DEFAULT_INTERVAL_MS = 30_000;
const DEFAULT_SAMPLES = 5;
const DEFAULT_WARN_MS = 100;
const DEFAULT_TRIP_MS = 500;

/**
 * @typedef {Object} ClockSkewSamplerOptions
 * @property {number} [intervalMs=30000] - How often to sample, in milliseconds.
 * @property {number} [samples=5] - Redis `TIME` reads per sample; the per-read skew estimates are reduced by median to reject jitter. Must be >= 1.
 * @property {number} [warnMs=100] - Absolute skew (ms) at or above which `onWarn` fires (and below `tripMs`).
 * @property {number} [tripMs=500] - Absolute skew (ms) at or above which `onTrip` fires.
 * @property {boolean} [immediate=true] - Take the first sample at construction instead of waiting one interval.
 * @property {(skewMs: number) => void} [onSkew] - Called after every successful sample with the signed median skew.
 * @property {(skewMs: number) => void} [onWarn] - Called when `warnMs <= |skew| < tripMs`.
 * @property {(skewMs: number) => void} [onTrip] - Called when `|skew| >= tripMs`.
 * @property {(err: Error) => void} [onError] - Called when a sample fails (Redis error). The last good gauge value is retained.
 * @property {boolean | { tripSamples?: number, releaseSamples?: number }} [fence=false] -
 *   Opt-in self-fencing: turn the trip threshold from an alert into a state.
 *   The sampler enters the FENCED state after `tripSamples` (default 2)
 *   consecutive trip-level samples - one bad sample must never shed authority
 *   - and releases after `releaseSamples` (default 3) consecutive below-warn
 *   samples (warn-level samples hold the current state; the hysteresis gap is
 *   deliberate). While fenced, `fenced()` reads true; consumers that stamp or
 *   order by this instance's clock should stand down and let a healthy
 *   instance take over (`attachClockFence` + the smooth authority gate consume
 *   it that way). A failed sample never changes fence state - losing the
 *   measurement is not evidence of drift.
 * @property {(fenced: boolean) => void} [onFence] - Called on every fence-state transition.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

/**
 * @typedef {Object} ClockSkewSampler
 * @property {() => number | null} current - The most recent signed skew in milliseconds, or `null` before the first successful sample.
 * @property {() => boolean} fenced - Whether the sampler is in the FENCED state (always false when the `fence` option is off).
 * @property {() => Promise<number | null>} sample - Run one sample immediately and return the signed median skew (or `null` on failure). Also updates the gauge and fires callbacks.
 * @property {() => Promise<void>} stop - Stop the interval and await any in-flight sample. Idempotent. Never throws.
 */

/**
 * Read the Redis server clock once as epoch milliseconds.
 *
 * `TIME` returns `[seconds, microseconds]`; ioredis surfaces them as strings.
 * @param {import('ioredis').Redis} redis
 * @returns {Promise<number>}
 */
async function readRedisEpochMs(redis) {
	const res = await redis.time();
	const seconds = Number(res[0]);
	const micros = Number(res[1]);
	return seconds * 1000 + Math.floor(micros / 1000);
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
 * Create a clock-skew sampler. Starts sampling immediately; call `stop()` on
 * shutdown.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {ClockSkewSamplerOptions} [options]
 * @returns {ClockSkewSampler}
 *
 * @example
 * ```js
 * import { createClockSkewSampler } from 'svelte-adapter-uws-extensions/redis/clock-skew';
 * import { createMetrics } from 'svelte-adapter-uws-extensions/prometheus';
 *
 * const metrics = createMetrics();
 * const skew = createClockSkewSampler(redis, {
 *   metrics,
 *   onTrip: (ms) => console.error(`[clock] skew ${ms}ms exceeds 500ms - check NTP`)
 * });
 *
 * export async function shutdown() {
 *   await skew.stop();
 * }
 * ```
 */
export function createClockSkewSampler(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('clock-skew: client (from createRedisClient) is required');
	}
	const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
	if (!Number.isFinite(intervalMs) || intervalMs < 1) {
		throw new Error('clock-skew: intervalMs must be a positive number (ms)');
	}
	const samples = options.samples ?? DEFAULT_SAMPLES;
	if (!Number.isInteger(samples) || samples < 1) {
		throw new Error('clock-skew: samples must be a positive integer');
	}
	const warnMs = options.warnMs ?? DEFAULT_WARN_MS;
	const tripMs = options.tripMs ?? DEFAULT_TRIP_MS;
	if (!Number.isFinite(warnMs) || warnMs < 0 || !Number.isFinite(tripMs) || tripMs < 0) {
		throw new Error('clock-skew: warnMs and tripMs must be non-negative numbers (ms)');
	}
	for (const name of ['onSkew', 'onWarn', 'onTrip', 'onError', 'onFence']) {
		if (options[name] !== undefined && typeof options[name] !== 'function') {
			throw new Error(`clock-skew: ${name} must be a function`);
		}
	}
	const fenceOpt = options.fence;
	let fenceEnabled = false;
	let fenceTripSamples = 2;
	let fenceReleaseSamples = 3;
	if (fenceOpt !== undefined && fenceOpt !== false) {
		if (fenceOpt !== true && (typeof fenceOpt !== 'object' || fenceOpt === null)) {
			throw new Error('clock-skew: fence must be a boolean or { tripSamples?, releaseSamples? }');
		}
		fenceEnabled = true;
		if (fenceOpt !== true) {
			fenceTripSamples = fenceOpt.tripSamples ?? 2;
			fenceReleaseSamples = fenceOpt.releaseSamples ?? 3;
			if (!Number.isInteger(fenceTripSamples) || fenceTripSamples < 1
				|| !Number.isInteger(fenceReleaseSamples) || fenceReleaseSamples < 1) {
				throw new Error('clock-skew: fence tripSamples and releaseSamples must be positive integers');
			}
		}
	}

	const redis = client.redis;
	const breaker = options.breaker;
	const { onSkew, onWarn, onTrip, onError, onFence } = options;

	const m = options.metrics;
	const mSkew = m?.gauge(
		'platform_clock_skew_ms',
		'Signed clock skew of this instance wall clock relative to the Redis server clock, in milliseconds (positive = local clock ahead of Redis). The median of several round-trip-compensated Redis TIME reads.'
	);
	const mFenced = fenceEnabled ? m?.gauge(
		'platform_clock_fenced',
		'Whether this instance has self-fenced on clock skew (1 = fenced: stamping/ordering authority stood down until the skew clears)'
	) : undefined;
	mFenced?.set(0);

	let lastSkew = null;
	let stopped = false;
	let timer = null;
	let inFlight = null;
	let fenced = false;
	let tripStreak = 0;
	let calmStreak = 0;

	/** @param {boolean} next */
	function setFenced(next) {
		if (fenced === next) return;
		fenced = next;
		mFenced?.set(next ? 1 : 0);
		if (onFence) { try { onFence(next); } catch { /* swallow */ } }
	}

	/** Run one sample. Resolves to the signed median skew, or null on failure. */
	async function sample() {
		if (stopped) return lastSkew;
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
				const localMid = t0 + (t1 - t0) / 2;
				estimates.push(localMid - redisMs);
			}
			breaker?.success();

			const skew = median(estimates);
			lastSkew = skew;
			mSkew?.set(skew);

			if (onSkew) {
				try { onSkew(skew); } catch { /* swallow listener errors */ }
			}
			const abs = Math.abs(skew);
			if (abs >= tripMs) {
				if (onTrip) { try { onTrip(skew); } catch { /* swallow */ } }
			} else if (abs >= warnMs) {
				if (onWarn) { try { onWarn(skew); } catch { /* swallow */ } }
			}
			if (fenceEnabled) {
				// Enter on consecutive trips, release on consecutive calm
				// (below-warn) samples; a warn-level sample holds the current
				// state, so the warn..trip band is the hysteresis gap.
				if (abs >= tripMs) {
					tripStreak++;
					calmStreak = 0;
					if (tripStreak >= fenceTripSamples) setFenced(true);
				} else {
					tripStreak = 0;
					if (abs < warnMs) {
						calmStreak++;
						if (calmStreak >= fenceReleaseSamples) setFenced(false);
					} else {
						calmStreak = 0;
					}
				}
			}
			return skew;
		} catch (err) {
			breaker?.failure(err);
			// Keep the last good gauge value rather than emitting a misleading 0.
			if (onError) {
				try { onError(err); } catch { /* swallow */ }
			}
			return null;
		}
	}

	if (options.immediate !== false) {
		// Fire the first sample immediately so the gauge is populated within one
		// Redis round-trip of construction. Errors are caught inside sample().
		inFlight = sample().catch(() => {});
	} else {
		inFlight = Promise.resolve();
	}

	timer = setIntervalTimer(() => {
		// Chain so a slow sample never overlaps itself.
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
		current: () => lastSkew,
		fenced: () => fenced,
		sample,
		stop
	};
}

/**
 * Attach a sampler's fence surface to the platform as the `clockFence`
 * convention, so framework layers that stamp or order by this instance's
 * clock (the smooth authority renewal gate today) can stand down while the
 * clock is untrustworthy. `bus.wrap` forwards it, so the wrapped platform a
 * clustered app hands to the framework carries it too.
 *
 * @param {any} platform
 * @param {ClockSkewSampler} sampler
 */
export function attachClockFence(platform, sampler) {
	if (!platform || typeof platform !== 'object') {
		throw new Error('clock-skew: attachClockFence requires a platform object');
	}
	if (!sampler || typeof sampler.fenced !== 'function') {
		throw new Error('clock-skew: attachClockFence requires a sampler with a fenced() surface (set the fence option)');
	}
	platform.clockFence = { fenced: sampler.fenced };
	return platform;
}
