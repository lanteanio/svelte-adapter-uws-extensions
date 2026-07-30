/**
 * Redis-backed rate limiter for svelte-adapter-uws.
 *
 * Same API as the core createRateLimit plugin, but stores bucket state
 * in Redis so rate limits are enforced across all server instances.
 *
 * Uses a Lua script for atomic token consumption to avoid race conditions.
 * The Lua script runs entirely on the Redis server, so there is exactly
 * one roundtrip per consume() call.
 *
 * @module svelte-adapter-uws-extensions/redis/ratelimit
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { evalCached } from '../shared/eval-cached.js';
import { withBreaker } from '../shared/breaker.js';
import { isPrivateOrLoopbackAddress, isAddressHeaderConfigured } from '../shared/client-ip.js';
import { wallEpoch } from '../shared/runtime.js';
import {
	CONSUME_SCRIPT,
	PEEK_SCRIPT,
	SLIDING_SCRIPT,
	SLIDING_PEEK_SCRIPT,
	GCRA_SCRIPT,
	GCRA_PEEK_SCRIPT
} from './token-bucket-script.js';
import { createEmergencyScaleReader, createEmergencyScaleOps } from './emergency-scale.js';

export { createEmergencyScaleOps as createRateLimitEmergency } from './emergency-scale.js';
export { createCompositeRateLimit } from './composite-ratelimit.js';

// Upper bound on in-process floor buckets so a keyed flood during an outage
// cannot exhaust memory; least-recently-touched entries are evicted first.
const FLOOR_MAX_ENTRIES = 10000;

/**
 * @typedef {Object} FloorBucket
 * @property {(key: string, cost: number) => ConsumeResult} consume - Spend on
 *   the in-process bucket (mutates state).
 * @property {(key: string, cost: number) => ConsumeResult} peek - Report the
 *   verdict a consume would return without mutating, initializing, or touching
 *   any bucket (the read-only floor path for `peek()` during a store outage).
 */

/**
 * In-process fixed-window bucket mirroring CONSUME_SCRIPT's semantics (init at
 * full, ban check, interval refill, consume, ban-on-exhaust) so a verdict
 * decided on the floor matches what Redis would have said for a single
 * instance. State is per process: while degraded, N instances allow up to N
 * times the configured budget in the worst case, which is the deliberate trade
 * against denying everything (fail closed) or counting nothing (fail open).
 *
 * @param {number} maxPoints
 * @param {number} interval
 * @param {number} blockDuration
 * @param {(() => number) | null} [getScale] - emergency scale source (the
 *   limiter's cached reader), so an incident clamp keeps applying on the
 *   floor path while the store is unreachable. Same effective-budget math as
 *   the Lua: max(1, floor(maxPoints * scale)) with a mid-window clamp.
 * @returns {FloorBucket}
 */
export function createWindowFloor(maxPoints, interval, blockDuration, getScale) {
	/** @type {Map<string, { points: number, resetAt: number, bannedUntil: number }>} */
	const buckets = new Map();
	function effMaxNow() {
		const scale = getScale ? getScale() : 1;
		return scale > 0 && scale !== 1 ? Math.max(1, Math.floor(maxPoints * scale)) : maxPoints;
	}
	return {
		consume(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			let entry = buckets.get(key);
			if (entry !== undefined) {
				// Reinsert on touch: insertion order then doubles as LRU order.
				buckets.delete(key);
			} else {
				entry = { points: effMax, resetAt: nowMs + interval, bannedUntil: 0 };
			}
			buckets.set(key, entry);
			if (buckets.size > FLOOR_MAX_ENTRIES) {
				buckets.delete(buckets.keys().next().value);
			}
			if (entry.bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: entry.bannedUntil - nowMs };
			}
			if (entry.resetAt <= nowMs) {
				entry.points = effMax;
				entry.resetAt = nowMs + interval;
			}
			if (entry.points > effMax) {
				entry.points = effMax;
			}
			if (entry.points >= cost) {
				entry.points -= cost;
				return { allowed: true, remaining: entry.points, resetMs: entry.resetAt - nowMs };
			}
			if (blockDuration > 0) {
				entry.bannedUntil = nowMs + blockDuration;
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			return { allowed: false, remaining: Math.max(0, entry.points), resetMs: entry.resetAt - nowMs };
		},
		peek(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			const stored = buckets.get(key);
			let points, resetAt, bannedUntil;
			if (stored === undefined) {
				points = effMax; resetAt = nowMs + interval; bannedUntil = 0;
			} else {
				points = stored.points; resetAt = stored.resetAt; bannedUntil = stored.bannedUntil;
			}
			if (bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: bannedUntil - nowMs };
			}
			if (resetAt <= nowMs) {
				points = effMax; resetAt = nowMs + interval;
			}
			if (points > effMax) points = effMax;
			if (points >= cost) {
				return { allowed: true, remaining: points - cost, resetMs: resetAt - nowMs };
			}
			if (blockDuration > 0) {
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			return { allowed: false, remaining: Math.max(0, points), resetMs: resetAt - nowMs };
		}
	};
}

/**
 * Backward-compatible alias for the fixed-window floor's consume function (the
 * shape callers imported before the peek-capable floor landed).
 * @param {number} maxPoints
 * @param {number} interval
 * @param {number} blockDuration
 * @param {(() => number) | null} [getScale]
 * @returns {(key: string, cost: number) => ConsumeResult}
 */
export function createLocalFloorBucket(maxPoints, interval, blockDuration, getScale) {
	return createWindowFloor(maxPoints, interval, blockDuration, getScale).consume;
}

/**
 * In-process sliding-window-counter floor mirroring SLIDING_SCRIPT. Same
 * effective-budget math and window roll as the Lua, evaluated on the exact
 * wall-clock seam.
 *
 * @param {number} maxPoints
 * @param {number} interval
 * @param {number} blockDuration
 * @param {(() => number) | null} [getScale]
 * @returns {FloorBucket}
 */
export function createSlidingFloor(maxPoints, interval, blockDuration, getScale) {
	/** @type {Map<string, { curr: number, prev: number, windowStart: number, bannedUntil: number }>} */
	const buckets = new Map();
	function effMaxNow() {
		const scale = getScale ? getScale() : 1;
		return scale > 0 && scale !== 1 ? Math.max(1, Math.floor(maxPoints * scale)) : maxPoints;
	}
	function evict() {
		if (buckets.size > FLOOR_MAX_ENTRIES) buckets.delete(buckets.keys().next().value);
	}
	return {
		consume(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			const stored = buckets.get(key);
			if (stored !== undefined) buckets.delete(key);
			let curr, prev, windowStart, bannedUntil;
			if (stored === undefined) {
				curr = 0; prev = 0; windowStart = nowMs - (nowMs % interval); bannedUntil = 0;
			} else {
				curr = stored.curr; prev = stored.prev; windowStart = stored.windowStart; bannedUntil = stored.bannedUntil;
			}
			const entry = { curr, prev, windowStart, bannedUntil };
			buckets.set(key, entry);
			evict();
			if (bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: bannedUntil - nowMs };
			}
			const winStartNow = nowMs - (nowMs % interval);
			const elapsed = winStartNow - windowStart;
			if (elapsed >= interval * 2) { prev = 0; curr = 0; windowStart = winStartNow; }
			else if (elapsed >= interval) { prev = curr; curr = 0; windowStart = winStartNow; }
			if (curr > effMax) curr = effMax;
			const into = nowMs - windowStart;
			let prevWeight = (interval - into) / interval;
			if (prevWeight < 0) prevWeight = 0;
			if (prevWeight > 1) prevWeight = 1;
			const weighted = curr + prev * prevWeight;
			const resetMs = windowStart + interval - nowMs;
			if (weighted + cost <= effMax) {
				curr = curr + cost;
				entry.curr = curr; entry.prev = prev; entry.windowStart = windowStart; entry.bannedUntil = bannedUntil;
				return { allowed: true, remaining: Math.max(0, Math.floor(effMax - weighted - cost)), resetMs };
			}
			if (blockDuration > 0) {
				bannedUntil = nowMs + blockDuration;
				entry.curr = curr; entry.prev = prev; entry.windowStart = windowStart; entry.bannedUntil = bannedUntil;
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			entry.curr = curr; entry.prev = prev; entry.windowStart = windowStart; entry.bannedUntil = bannedUntil;
			return { allowed: false, remaining: Math.max(0, Math.floor(effMax - weighted)), resetMs };
		},
		peek(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			const stored = buckets.get(key);
			let curr, prev, windowStart, bannedUntil;
			if (stored === undefined) {
				curr = 0; prev = 0; windowStart = nowMs - (nowMs % interval); bannedUntil = 0;
			} else {
				curr = stored.curr; prev = stored.prev; windowStart = stored.windowStart; bannedUntil = stored.bannedUntil;
			}
			if (bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: bannedUntil - nowMs };
			}
			const winStartNow = nowMs - (nowMs % interval);
			const elapsed = winStartNow - windowStart;
			if (elapsed >= interval * 2) { prev = 0; curr = 0; windowStart = winStartNow; }
			else if (elapsed >= interval) { prev = curr; curr = 0; windowStart = winStartNow; }
			if (curr > effMax) curr = effMax;
			const into = nowMs - windowStart;
			let prevWeight = (interval - into) / interval;
			if (prevWeight < 0) prevWeight = 0;
			if (prevWeight > 1) prevWeight = 1;
			const weighted = curr + prev * prevWeight;
			const resetMs = windowStart + interval - nowMs;
			if (weighted + cost <= effMax) {
				return { allowed: true, remaining: Math.max(0, Math.floor(effMax - weighted - cost)), resetMs };
			}
			if (blockDuration > 0) {
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			return { allowed: false, remaining: Math.max(0, Math.floor(effMax - weighted)), resetMs };
		}
	};
}

/**
 * In-process GCRA / leaky-bucket floor mirroring GCRA_SCRIPT. The emission
 * interval is recomputed from the scaled budget each check, so a tightened
 * emergency clamp applies immediately with no stored-points clamp.
 *
 * @param {number} maxPoints
 * @param {number} interval
 * @param {number} blockDuration
 * @param {(() => number) | null} [getScale]
 * @returns {FloorBucket}
 */
export function createGcraFloor(maxPoints, interval, blockDuration, getScale) {
	/** @type {Map<string, { tat: number, bannedUntil: number }>} */
	const buckets = new Map();
	function effMaxNow() {
		const scale = getScale ? getScale() : 1;
		return scale > 0 && scale !== 1 ? Math.max(1, Math.floor(maxPoints * scale)) : maxPoints;
	}
	function evict() {
		if (buckets.size > FLOOR_MAX_ENTRIES) buckets.delete(buckets.keys().next().value);
	}
	return {
		consume(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			const emission = interval / effMax;
			const burst = interval;
			const stored = buckets.get(key);
			if (stored !== undefined) buckets.delete(key);
			const tat = stored === undefined ? null : stored.tat;
			let bannedUntil = stored === undefined ? 0 : stored.bannedUntil;
			const entry = { tat: tat === null ? nowMs : tat, bannedUntil };
			buckets.set(key, entry);
			evict();
			if (bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: bannedUntil - nowMs };
			}
			const tatEff = (tat === null || tat <= nowMs) ? nowMs : tat;
			const newTat = tatEff + emission * cost;
			const allowAt = newTat - burst;
			if (nowMs >= allowAt) {
				entry.tat = newTat; entry.bannedUntil = bannedUntil;
				return { allowed: true, remaining: Math.max(0, Math.floor((nowMs - allowAt) / emission)), resetMs: Math.floor(newTat - nowMs) };
			}
			if (blockDuration > 0) {
				bannedUntil = nowMs + blockDuration;
				entry.tat = tatEff; entry.bannedUntil = bannedUntil;
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			entry.tat = tatEff; entry.bannedUntil = bannedUntil;
			return { allowed: false, remaining: Math.max(0, Math.floor((nowMs - (tatEff - burst)) / emission)), resetMs: Math.ceil(allowAt - nowMs) };
		},
		peek(key, cost) {
			const nowMs = wallEpoch();
			const effMax = effMaxNow();
			const emission = interval / effMax;
			const burst = interval;
			const stored = buckets.get(key);
			const tat = stored === undefined ? null : stored.tat;
			const bannedUntil = stored === undefined ? 0 : stored.bannedUntil;
			if (bannedUntil > nowMs) {
				return { allowed: false, remaining: 0, resetMs: bannedUntil - nowMs };
			}
			const tatEff = (tat === null || tat <= nowMs) ? nowMs : tat;
			const newTat = tatEff + emission * cost;
			const allowAt = newTat - burst;
			if (nowMs >= allowAt) {
				return { allowed: true, remaining: Math.max(0, Math.floor((nowMs - allowAt) / emission)), resetMs: Math.floor(newTat - nowMs) };
			}
			if (blockDuration > 0) {
				return { allowed: false, remaining: 0, resetMs: blockDuration };
			}
			return { allowed: false, remaining: Math.max(0, Math.floor((nowMs - (tatEff - burst)) / emission)), resetMs: Math.ceil(allowAt - nowMs) };
		}
	};
}

// Per refill mode: the consume + peek Lua and the in-process floor factory. The
// key infix keeps each mode in its own key space so switching modes never reads
// a foreign-layout hash. `window` keeps the original 'ratelimit:' infix
// (byte-identical to prior releases); the two smooth modes get their own.
const REFILL_MODES = {
	window: { consume: CONSUME_SCRIPT, peek: PEEK_SCRIPT, infix: 'ratelimit:', floor: createWindowFloor },
	sliding: { consume: SLIDING_SCRIPT, peek: SLIDING_PEEK_SCRIPT, infix: 'ratelimits:', floor: createSlidingFloor },
	gcra: { consume: GCRA_SCRIPT, peek: GCRA_PEEK_SCRIPT, infix: 'ratelimitg:', floor: createGcraFloor }
};

const BAN_SCRIPT = `
local key = KEYS[1]
local duration = tonumber(ARGV[1])
local defaultPoints = tonumber(ARGV[2])
local defaultInterval = tonumber(ARGV[3])
if duration == nil or defaultPoints == nil or defaultInterval == nil then
  return redis.error_reply('BAN: duration/defaultPoints/defaultInterval must be numeric')
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local vals = redis.call('hmget', key, 'points', 'resetAt')
local pts = vals[1] or defaultPoints
local rst = vals[2] or (now + defaultInterval)

redis.call('hset', key, 'points', pts, 'resetAt', rst, 'bannedUntil', now + duration)
redis.call('pexpire', key, duration + 60000)
return 1
`;

/**
 * @typedef {Object} RedisRateLimitOptions
 * @property {number} points - Tokens available per interval. Must be a positive integer.
 * @property {number} interval - Refill interval in milliseconds. Must be positive.
 * @property {number} [blockDuration=0] - Auto-ban duration in ms when exhausted. 0 = no ban.
 * @property {'ip' | 'connection' | ((ws: any) => string)} [keyBy='ip'] - Key extraction mode.
 *   In 'ip' mode (the default) the bucket key is `userData.remoteAddress`, which the adapter
 *   resolves from ADDRESS_HEADER / XFF_DEPTH. Behind an address-rewriting proxy (docker
 *   userland-proxy, an L4 load balancer, a non-XFF proxy) with ADDRESS_HEADER unset, every
 *   client arrives as the same gateway address and the per-IP bucket collapses into one shared
 *   global bucket. Set ADDRESS_HEADER (and XFF_DEPTH) so the real client IP is resolved, or pass
 *   an explicit keyBy. The limiter logs a one-shot warning the first time it denies on a
 *   loopback/private key while ADDRESS_HEADER is unset (the signature of that collapse).
 * @property {(ws: any) => (string | null | undefined)} [tenant] - Optional per-connection
 *   tenant resolver. When set, the bucket key is scoped by the returned tenant id, so two
 *   tenants sharing an IP / connection / custom key get independent buckets and a tenant's
 *   admin ops (`reset` / `ban` / `unban` / `clear`) touch only that tenant. Return
 *   null/undefined for an unscoped connection. Omit for a single-tenant deploy (byte-identical
 *   to before). The id should be a delimiter-safe slug - it is joined to the key with a NUL,
 *   so it stays unambiguous even when the key is an IPv6 address.
 * @property {boolean | { points?: number, interval?: number }} [localFloorOnStorageFailure=false] -
 *   Opt-in degraded mode for `consume()`: when Redis is unreachable (or the breaker is open),
 *   decide on an in-process token bucket with the same semantics instead of rejecting the
 *   promise. `true` reuses the configured points/interval; the object form sets a tighter
 *   per-instance budget (e.g. `points / instanceCount` keeps the fleet-wide allowance
 *   roughly constant while degraded). Floor state is per process and per instance, so N
 *   instances allow up to N times the floor budget in the worst case; it never leaks back
 *   into Redis. Admin ops (`reset` / `ban` / `unban` / `clear` / `purgeUser`) are operator
 *   actions and still reject while the store is down - only the request-path verdict
 *   degrades. Off (today's reject-to-caller behavior) unless set.
 * @property {'window' | 'sliding' | 'gcra'} [refill='window'] - Refill algorithm.
 *   'window' (default) is the fixed-window counter, byte-identical to prior releases;
 *   it admits up to ~2x points across a window edge (the whole bucket refills at once).
 *   'sliding' is a sliding-window-counter and 'gcra' is a leaky-bucket/GCRA - both remove
 *   that boundary burst. Each mode uses its own key space, so switching modes never reads a
 *   foreign-layout bucket.
 */

/**
 * @typedef {Object} ConsumeResult
 * @property {boolean} allowed
 * @property {number} remaining
 * @property {number} resetMs
 */

/**
 * @typedef {Object} RedisRateLimiter
 * @property {(ws: any, cost?: number) => Promise<ConsumeResult>} consume
 * @property {(ws: any, cost?: number) => Promise<ConsumeResult>} peek - Read-only: the verdict
 *   a consume(ws, cost) WOULD return, without spending, initializing the bucket, or moving the
 *   allowed/denied counters.
 * @property {(key: string, tenant?: string | null) => Promise<void>} reset
 * @property {(key: string, duration?: number, tenant?: string | null) => Promise<void>} ban
 * @property {(key: string, tenant?: string | null) => Promise<void>} unban
 * @property {(tenant?: string | null) => Promise<void>} clear
 */

/**
 * Create a Redis-backed rate limiter.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisRateLimitOptions} options
 * @returns {RedisRateLimiter}
 */
export function createRateLimit(client, options) {
	if (!options || typeof options !== 'object') {
		throw new Error('redis ratelimit: options object is required');
	}

	const { points, interval, blockDuration = 0, keyBy = 'ip', tenant } = options;

	if (!Number.isInteger(points) || points <= 0) {
		throw new Error('redis ratelimit: points must be a positive integer');
	}
	if (typeof interval !== 'number' || !Number.isFinite(interval) || interval <= 0) {
		throw new Error('redis ratelimit: interval must be a positive number');
	}
	if (typeof blockDuration !== 'number' || !Number.isFinite(blockDuration) || blockDuration < 0) {
		throw new Error('redis ratelimit: blockDuration must be a non-negative number');
	}
	if (keyBy !== 'ip' && keyBy !== 'connection' && typeof keyBy !== 'function') {
		throw new Error("redis ratelimit: keyBy must be 'ip', 'connection', or a function");
	}
	if (tenant !== undefined && typeof tenant !== 'function') {
		throw new Error('redis ratelimit: tenant must be a function (ws) => id | null');
	}

	const refill = options.refill ?? 'window';
	if (!Object.prototype.hasOwnProperty.call(REFILL_MODES, refill)) {
		throw new Error("redis ratelimit: refill must be 'window', 'sliding', or 'gcra'");
	}
	const mode = REFILL_MODES[refill];

	const floorOpt = options.localFloorOnStorageFailure;
	let floor = null;
	if (floorOpt !== undefined && floorOpt !== false) {
		if (floorOpt !== true && (typeof floorOpt !== 'object' || floorOpt === null)) {
			throw new Error('redis ratelimit: localFloorOnStorageFailure must be a boolean or { points?, interval? }');
		}
		const floorPoints = floorOpt === true ? points : (floorOpt.points ?? points);
		const floorInterval = floorOpt === true ? interval : (floorOpt.interval ?? interval);
		if (!Number.isInteger(floorPoints) || floorPoints <= 0) {
			throw new Error('redis ratelimit: localFloorOnStorageFailure.points must be a positive integer');
		}
		if (typeof floorInterval !== 'number' || !Number.isFinite(floorInterval) || floorInterval <= 0) {
			throw new Error('redis ratelimit: localFloorOnStorageFailure.interval must be a positive number');
		}
		// The floor mirrors the selected refill algorithm so a degraded verdict
		// matches what Redis would have said in the same mode.
		floor = mode.floor(floorPoints, floorInterval, blockDuration, () => emergencyScale.current());
	}
	let warnedStorageFloor = false;

	const redis = client.redis;

	// The fleet-wide emergency factor: read through a lazily-refreshed cache
	// (at most one background GET per refresh window, never per check) and
	// applied inside the consume script as an effective-budget argument. The
	// same cached value feeds the floor path above, so a clamp survives a
	// store outage. Neutral (1) when the key is absent - zero-config.
	const emergencyScale = createEmergencyScaleReader(client, options.emergency);

	// Version prefix for Redis keys. Different script versions use different
	// key spaces so rolling deployments with algorithm changes don't produce
	// inconsistent rate limiting. Old-version keys expire naturally via TTL.
	const SCRIPT_VERSION = 'v1';

	const b = options.breaker;
	const m = options.metrics;
	// When a tenant resolver is set, label the rate-limit counters by tenant so an
	// operator can see per-tenant allow/deny/ban rates. Opt-in (no resolver -> no
	// label, byte-identical series); the label is bounded by the metric's default
	// max-series cardinality cap, so a tenant burst cannot blow up the registry.
	const labelTenants = typeof tenant === 'function';
	const tlabels = labelTenants ? ['tenant_id'] : undefined;
	const mAllowed = m?.counter('ratelimit_allowed_total', 'Requests allowed', tlabels);
	const mDenied = m?.counter('ratelimit_denied_total', 'Requests denied', tlabels);
	const mBans = m?.counter('ratelimit_bans_total', 'Bans applied', tlabels);
	const mFloor = floor !== null
		? m?.counter('ratelimit_storage_fallbacks_total', 'Rate-limit verdicts decided by the in-process floor while the store was unreachable')
		: undefined;

	// Per-connection keying uses a WeakMap to avoid leaks
	const wsKeys = new WeakMap();
	let connCounter = 0;

	// One-shot proxy-collapse diagnostic. In the default 'ip' mode the bucket key
	// is the resolved remote address; behind an address-rewriting proxy with no
	// ADDRESS_HEADER configured, every client collapses onto the gateway address
	// and this per-IP limiter quietly becomes one shared global bucket. The first
	// time we deny on a loopback/private key with no proxy header set (the
	// signature of that collapse) we warn once. Read the env once here - it is
	// fixed before the server starts. Purely diagnostic; never changes a verdict.
	const ipMode = keyBy === 'ip';
	const addressHeaderSet = isAddressHeaderConfigured();
	let warnedProxyCollapse = false;

	function maybeWarnProxyCollapse(key) {
		if (warnedProxyCollapse || !ipMode || addressHeaderSet) return;
		if (!isPrivateOrLoopbackAddress(key)) return;
		warnedProxyCollapse = true;
		console.warn(
			`redis ratelimit: denied a request keyed on the private/loopback address "${key}" ` +
			"while keyBy:'ip' and ADDRESS_HEADER is unset. If this server sits behind an " +
			'address-rewriting proxy (docker userland-proxy, an L4 load balancer, a non-XFF proxy), ' +
			'every client arrives as the same gateway address and this per-IP rate limiter collapses ' +
			'into one shared global bucket. Set ADDRESS_HEADER (and XFF_DEPTH) so the adapter resolves ' +
			'the real client IP, or pass an explicit keyBy. This warning fires once.'
		);
	}

	/**
	 * Resolve the rate limit key for a WebSocket connection.
	 *
	 * In 'ip' mode (default), uses `userData.remoteAddress` which the core
	 * adapter v0.4.0+ resolves via ADDRESS_HEADER/XFF_DEPTH - so this is
	 * the real client IP when behind a proxy, not the raw socket address.
	 * Falls back to `ip`, `address`, then 'unknown'.
	 */
	function resolveKey(ws) {
		if (typeof keyBy === 'function') return keyBy(ws);
		if (keyBy === 'connection') {
			let k = wsKeys.get(ws);
			if (!k) {
				k = '__conn:' + (++connCounter);
				wsKeys.set(ws, k);
			}
			return k;
		}
		const ud = typeof ws.getUserData === 'function' ? ws.getUserData() : null;
		if (ud) {
			return String(ud.remoteAddress || ud.ip || ud.address || 'unknown');
		}
		return 'unknown';
	}

	// The tenant segment (when a `tenant` resolver is set) is FIRST and NUL-delimited,
	// so a validated id stays unambiguous even when the key is an IPv6 address (colons).
	// Null tenant -> no segment, byte-identical to the single-tenant key space. The id is
	// rejected if it contains the NUL delimiter (the one char that would let two distinct
	// tenants collide on one bucket); this is the injection-safety the realtime tier
	// validates at its own boundary, enforced here too so the property does not silently
	// depend on the caller's resolver. The check short-circuits on the null (default) path.
	function bucketKey(key, tenantId) {
		if (tenantId && tenantId.indexOf('\0') !== -1) {
			throw new Error('redis ratelimit: tenant id must not contain a NUL byte (it is the bucket-key delimiter)');
		}
		return client.key(SCRIPT_VERSION + ':' + mode.infix + (tenantId ? tenantId + '\0' : '') + key);
	}

	return {
		async consume(ws, cost = 1) {
			if (typeof cost !== 'number' || !Number.isInteger(cost) || cost < 1) {
				throw new Error('redis ratelimit: cost must be a positive integer');
			}
			const key = resolveKey(ws);
			const tenantId = tenant ? tenant(ws) : null;
			// Resolved before the try so a tenant-id validation throw surfaces
			// to the caller and can never be mistaken for a storage failure.
			const bk = bucketKey(key, tenantId);

			let verdict;
			try {
				const result = await withBreaker(b, () =>
					evalCached(redis, mode.consume, 1, bk, points, interval, cost, blockDuration, emergencyScale.current())
				);
				verdict = { allowed: result[0] === 1, remaining: result[1], resetMs: result[2] };
			} catch (err) {
				if (floor === null) throw err;
				mFloor?.inc();
				if (!warnedStorageFloor) {
					warnedStorageFloor = true;
					console.warn(
						'redis ratelimit: store unreachable; deciding on the in-process floor. ' +
						'Limits hold per instance while degraded (N instances allow up to N times ' +
						'the floor budget); cross-instance state resumes when the store recovers. ' +
						'This warning fires once.'
					);
				}
				verdict = floor.consume(bk, cost);
			}

			const labels = labelTenants ? { tenant_id: tenantId || '' } : undefined;
			if (verdict.allowed) {
				mAllowed?.inc(labels);
			} else {
				mDenied?.inc(labels);
				maybeWarnProxyCollapse(key);
			}

			return verdict;
		},

		/**
		 * Read-only verdict: what a consume(ws, cost) WOULD return right now,
		 * without spending, initializing the bucket, or moving the allowed/denied
		 * counters. Enables a "spend only on failure" gate (peek to admit every
		 * request; consume to charge a point only when the request fails). During a
		 * store outage with the local floor enabled, decides on the in-process
		 * floor read-only (no floor mutation); without a floor it rejects like
		 * consume.
		 * @param {any} ws
		 * @param {number} [cost=1]
		 * @returns {Promise<ConsumeResult>}
		 */
		async peek(ws, cost = 1) {
			if (typeof cost !== 'number' || !Number.isInteger(cost) || cost < 1) {
				throw new Error('redis ratelimit: cost must be a positive integer');
			}
			const key = resolveKey(ws);
			const tenantId = tenant ? tenant(ws) : null;
			const bk = bucketKey(key, tenantId);
			try {
				const result = await withBreaker(b, () =>
					evalCached(redis, mode.peek, 1, bk, points, interval, cost, blockDuration, emergencyScale.current())
				);
				return { allowed: result[0] === 1, remaining: result[1], resetMs: result[2] };
			} catch (err) {
				if (floor === null) throw err;
				return floor.peek(bk, cost);
			}
		},

		async reset(key, tenantId) {
			await withBreaker(b, () => redis.del(bucketKey(key, tenantId)));
		},

		/**
		 * Right-to-erasure (`live.forget`): clear a user's rate-limit bucket. Only
		 * meaningful when the configured `keyBy` resolves to the userId (then the
		 * bucket key IS the userId); a harmless no-op (DEL of an absent key) when
		 * buckets are keyed by ip/connection. Buckets are counters-only and
		 * short-lived, so this never exposes PII - it is completeness, not erasure
		 * of stored data.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} buckets removed (0 or 1)
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			const removed = await withBreaker(b, () => redis.del(bucketKey(userId, tenantId)));
			return typeof removed === 'number' && removed > 0 ? removed : 0;
		},

		async ban(key, duration, tenantId) {
			const dur = duration ?? (blockDuration || 60000);
			if (dur <= 0) throw new Error('redis ratelimit: ban duration must be positive');
			const bk = bucketKey(key, tenantId);
			await withBreaker(b, () => evalCached(redis, BAN_SCRIPT, 1, bk, dur, points, interval));
			mBans?.inc(labelTenants ? { tenant_id: tenantId || '' } : undefined);
		},

		async unban(key, tenantId) {
			await withBreaker(b, () => redis.hset(bucketKey(key, tenantId), 'bannedUntil', 0));
		},

		// No tenant -> clears the whole key space (every tenant's buckets too, since the
		// glob `*` spans the NUL-delimited tenant segments). Pass a tenant id to clear
		// only that tenant's buckets. Scoped to this limiter's refill-mode key space.
		async clear(tenantId) {
			// The tenant id is interpolated into a SCAN MATCH glob, so it needs
			// the same delimiter guard as bucketKey PLUS a glob-metachar guard:
			// clear('*') would otherwise wipe every tenant's buckets through a
			// nominally tenant-scoped call. Coerce first - the id is
			// interpolated as a string a line below, so validating the raw
			// argument would miss a number and throw a TypeError on anything
			// without .indexOf.
			if (tenantId != null && typeof tenantId !== 'string') {
				// Rejected rather than coerced. The falsy-tenant branch below
				// is deliberate and matches `bucketKey`, which puts a falsy
				// tenant's buckets in the UNTENANTED segment - so widening the
				// test to `!= null` would desync the pair and make `clear(0)`
				// scan `0\0*` and match nothing. But `clear(0)` reading as
				// "clear everything" is a trap either way, and a numeric tenant
				// does not work anywhere else in this module (`bucketKey` calls
				// `tenantId.indexOf`), so the honest answer is to refuse the type.
				throw new Error(`redis ratelimit: clear tenant id must be a string or null/undefined, got ${typeof tenantId}`);
			}
			if (tenantId != null && /[\0*?[\]\\]/.test(tenantId)) {
				throw new Error('redis ratelimit: clear tenant id must not contain NUL or glob metacharacters (* ? [ ] \\)');
			}
			// '' passes the type and glob guards and is then FALSY, so it falls
			// into the global branch below and a nominally tenant-scoped call
			// wipes every tenant. It cannot be honoured as written either: an
			// empty tenant lands in bucketKey's untenanted segment, whose keys
			// are exactly the ones a glob cannot separate from the tenanted
			// ones. Refused for the same reason clear(0) is - the two real
			// calls are clear() for everything and clear(id) for one tenant.
			if (tenantId === '') {
				throw new Error('redis ratelimit: clear tenant id must not be empty - call clear() with no argument to clear every tenant');
			}
			const suffix = tenantId ? tenantId + '\0*' : '*';
			await withBreaker(b, () => scanAndUnlink(redis, client.key(SCRIPT_VERSION + ':' + mode.infix + suffix)));
		},

		// Operator surface for the fleet-wide emergency factor. `set(0.2)`
		// tightens every limiter sharing this Redis to 20% of its configured
		// budget within the readers' refresh window (~1s); `set` applies a
		// one-hour TTL by default so a forgotten incident clamp expires on its
		// own. Shared across every limiter instance on the same client - any
		// instance (or the standalone createRateLimitEmergency export) can
		// flip it.
		emergency: createEmergencyScaleOps(client)
	};
}
