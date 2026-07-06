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
import { CONSUME_SCRIPT } from './token-bucket-script.js';
import { createEmergencyScaleReader, createEmergencyScaleOps } from './emergency-scale.js';

export { createEmergencyScaleOps as createRateLimitEmergency } from './emergency-scale.js';
export { createCompositeRateLimit } from './composite-ratelimit.js';

// Upper bound on in-process floor buckets so a keyed flood during an outage
// cannot exhaust memory; least-recently-touched entries are evicted first.
const FLOOR_MAX_ENTRIES = 10000;

/**
 * In-process token bucket mirroring CONSUME_SCRIPT's semantics (init at full,
 * ban check, interval refill, consume, ban-on-exhaust) so a verdict decided
 * on the floor matches what Redis would have said for a single instance.
 * State is per process: while degraded, N instances allow up to N times the
 * configured budget in the worst case, which is the deliberate trade against
 * denying everything (fail closed) or counting nothing (fail open).
 *
 * @param {number} maxPoints
 * @param {number} interval
 * @param {number} blockDuration
 * @param {(() => number) | null} [getScale] - emergency scale source (the
 *   limiter's cached reader), so an incident clamp keeps applying on the
 *   floor path while the store is unreachable. Same effective-budget math as
 *   the Lua: max(1, floor(maxPoints * scale)) with a mid-window clamp.
 * @returns {(key: string, cost: number) => ConsumeResult}
 */
export function createLocalFloorBucket(maxPoints, interval, blockDuration, getScale) {
	/** @type {Map<string, { points: number, resetAt: number, bannedUntil: number }>} */
	const buckets = new Map();
	return function consumeLocal(key, cost) {
		const nowMs = wallEpoch();
		const scale = getScale ? getScale() : 1;
		const effMax = scale > 0 && scale !== 1 ? Math.max(1, Math.floor(maxPoints * scale)) : maxPoints;
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
	};
}

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

	const floorOpt = options.localFloorOnStorageFailure;
	let consumeFloor = null;
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
		consumeFloor = createLocalFloorBucket(floorPoints, floorInterval, blockDuration, () => emergencyScale.current());
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
	const mFloor = consumeFloor !== null
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
		return client.key(SCRIPT_VERSION + ':ratelimit:' + (tenantId ? tenantId + '\0' : '') + key);
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
					evalCached(redis, CONSUME_SCRIPT, 1, bk, points, interval, cost, blockDuration, emergencyScale.current())
				);
				verdict = { allowed: result[0] === 1, remaining: result[1], resetMs: result[2] };
			} catch (err) {
				if (consumeFloor === null) throw err;
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
				verdict = consumeFloor(bk, cost);
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
		// only that tenant's buckets.
		async clear(tenantId) {
			const suffix = tenantId ? tenantId + '\0*' : '*';
			await withBreaker(b, () => scanAndUnlink(redis, client.key(SCRIPT_VERSION + ':ratelimit:' + suffix)));
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
