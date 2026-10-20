/**
 * Cluster-wide emergency scale for every Redis-backed limiter.
 *
 * One shared Redis key holds a single scaling factor applied to every
 * limiter's budget at check time: `0.2` tightens every limit to 20% of its
 * configured points, `2` doubles them, absent/`1` is a no-op. Flipping the
 * key during an incident retunes the whole fleet's admission within the
 * reader's refresh window (~1s) - no config rollout, no process restart.
 *
 * Why a cached GET instead of a key read inside the consume script: the
 * bucket keys and one global key do not share a Redis Cluster hash slot, so
 * a multi-key script would fail with CROSSSLOT on clustered deployments.
 * A per-instance cached read costs at most one extra GET per refresh window
 * (not per check), passes the factor to the script as a plain argument
 * (slot-agnostic), and hands the SAME cached value to the in-process floor
 * paths - so an incident clamp keeps applying even while the store is
 * unreachable, exactly when it matters most.
 *
 * Failure philosophy: fail STICKY. A blip on the refresh read keeps the
 * last-known factor rather than snapping back to 1 (an incident clamp must
 * not silently vanish on a transient error). The setter applies a TTL by
 * default so a forgotten clamp expires on its own.
 *
 * Determinism: time comes from the injectable runtime clock; no RNG, no
 * timers (the refresh is lazy, piggybacked on check traffic).
 *
 * @module svelte-adapter-uws-extensions/redis/emergency-scale
 */

import { wallEpoch } from '../shared/runtime.js';

// A namespace no limiter bucket key can produce: bucket keys live under
// `v1:ratelimit:`, `v1:ratelimits:`, `v1:ratelimitg:` and `v1:ratelimitc:`, so
// neither a bucket named `emergency` nor a limiter clear() can reach it.
const EMERGENCY_KEY = 'v1:ratelimit-control:emergency';
// Earlier versions stored the factor inside the fixed-window bucket keyspace.
// A string there is a factor written by such a version and is still honored
// while the current key is absent; a hash there is an ordinary bucket named
// `emergency` (GET on it fails with WRONGTYPE) and is ignored.
const LEGACY_EMERGENCY_KEY = 'v1:ratelimit:emergency';

// The factor is clamped into a sane band on read AND write so a corrupt or
// hostile value can neither zero out all traffic (0) nor disable limiting
// entirely through overflow (Infinity).
const MIN_SCALE = 0.001;
const MAX_SCALE = 1000;

const DEFAULT_REFRESH_MS = 1000;
const DEFAULT_SET_TTL_MS = 3600000;

/**
 * Parse a stored factor. Anything non-numeric or out of band reads as the
 * neutral 1 (a corrupt key must never deny all traffic).
 * @param {unknown} raw
 * @returns {number}
 */
export function _parseScale(raw) {
	if (raw == null || raw === '') return 1;
	const n = typeof raw === 'number' ? raw : parseFloat(String(raw));
	if (!Number.isFinite(n)) return 1;
	if (n < MIN_SCALE) return MIN_SCALE;
	if (n > MAX_SCALE) return MAX_SCALE;
	return n;
}

/**
 * The factor an earlier version stored, or null when there is none. A bucket
 * hash at the same key is not a factor: its WRONGTYPE reply reads as null.
 * @param {import('ioredis').Redis} redis
 * @param {string} legacyKey
 * @returns {Promise<string | null>}
 */
async function readLegacy(redis, legacyKey) {
	try { return await redis.get(legacyKey); } catch (error) {
		if (/WRONGTYPE/.test(String(error?.message))) return null;
		throw error;
	}
}

/**
 * Remove a factor an earlier version stored, leaving a bucket at that key alone.
 * @param {import('ioredis').Redis} redis
 * @param {string} legacyKey
 */
async function clearLegacy(redis, legacyKey) {
	if (await readLegacy(redis, legacyKey) !== null) await redis.del(legacyKey);
}

/**
 * Lazily-refreshed reader for the shared factor. `current()` is synchronous
 * and never blocks a check: it returns the cached factor and, at most once
 * per refresh window, fires a background GET to refresh it. The first call
 * returns 1 while the first read is in flight (a cold instance briefly at
 * neutral beats a check stalled on Redis).
 *
 * @param {import('./index.js').RedisClient} client
 * @param {{ refreshMs?: number }} [opts]
 * @returns {{ current: () => number, _key: string }}
 */
export function createEmergencyScaleReader(client, opts) {
	const refreshMs = opts?.refreshMs ?? DEFAULT_REFRESH_MS;
	if (typeof refreshMs !== 'number' || !Number.isFinite(refreshMs) || refreshMs < 1) {
		throw new Error('redis ratelimit: emergency refreshMs must be a positive number');
	}
	const key = client.key(EMERGENCY_KEY);
	const legacyKey = client.key(LEGACY_EMERGENCY_KEY);
	const redis = client.redis;
	let scale = 1;
	let lastFetch = 0;
	let inflight = false;
	return {
		_key: key,
		current() {
			const now = wallEpoch();
			if (!inflight && now - lastFetch >= refreshMs) {
				inflight = true;
				lastFetch = now;
				Promise.resolve()
					.then(() => redis.get(key))
					.then(async (raw) => { scale = _parseScale(raw ?? await readLegacy(redis, legacyKey)); })
					.catch(() => { /* fail sticky: keep the last-known factor */ })
					.then(() => { inflight = false; });
			}
			return scale;
		}
	};
}

/**
 * Operator surface for the shared factor. Attached to every limiter
 * instance as `.emergency` and exported standalone for ops scripts that
 * have a Redis client but no limiter.
 *
 * @param {import('./index.js').RedisClient} client
 * @returns {{
 *   set: (scale: number, opts?: { ttlMs?: number }) => Promise<void>,
 *   clear: () => Promise<void>,
 *   get: () => Promise<number>
 * }}
 */
export function createEmergencyScaleOps(client) {
	const key = client.key(EMERGENCY_KEY);
	const legacyKey = client.key(LEGACY_EMERGENCY_KEY);
	const redis = client.redis;
	return {
		/**
		 * Apply a factor fleet-wide. Defaults to a one-hour TTL so a clamp set
		 * during an incident and then forgotten expires on its own; pass
		 * `{ ttlMs: 0 }` for a persistent factor (must then be cleared
		 * explicitly).
		 */
		async set(scale, opts) {
			if (typeof scale !== 'number' || !Number.isFinite(scale) || scale < MIN_SCALE || scale > MAX_SCALE) {
				throw new Error(`redis ratelimit: emergency scale must be a number in [${MIN_SCALE}, ${MAX_SCALE}]`);
			}
			const ttlMs = opts?.ttlMs ?? DEFAULT_SET_TTL_MS;
			if (typeof ttlMs !== 'number' || !Number.isFinite(ttlMs) || ttlMs < 0) {
				throw new Error('redis ratelimit: emergency ttlMs must be a non-negative number (0 = no expiry)');
			}
			if (ttlMs > 0) {
				await redis.set(key, String(scale), 'PX', ttlMs);
			} else {
				await redis.set(key, String(scale));
			}
			// A factor from an earlier version would resurface once this one expires.
			await clearLegacy(redis, legacyKey);
		},
		async clear() {
			await redis.del(key);
			await clearLegacy(redis, legacyKey);
		},
		async get() {
			return _parseScale((await redis.get(key)) ?? await readLegacy(redis, legacyKey));
		}
	};
}
