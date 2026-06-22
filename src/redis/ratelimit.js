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
import { withBreaker } from '../shared/breaker.js';
import { CONSUME_SCRIPT } from './token-bucket-script.js';

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
 * @property {(ws: any) => (string | null | undefined)} [tenant] - Optional per-connection
 *   tenant resolver. When set, the bucket key is scoped by the returned tenant id, so two
 *   tenants sharing an IP / connection / custom key get independent buckets and a tenant's
 *   admin ops (`reset` / `ban` / `unban` / `clear`) touch only that tenant. Return
 *   null/undefined for an unscoped connection. Omit for a single-tenant deploy (byte-identical
 *   to before). The id should be a delimiter-safe slug - it is joined to the key with a NUL,
 *   so it stays unambiguous even when the key is an IPv6 address.
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

	const redis = client.redis;

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

	// Per-connection keying uses a WeakMap to avoid leaks
	const wsKeys = new WeakMap();
	let connCounter = 0;

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

			const result = await withBreaker(b, () =>
				redis.eval(CONSUME_SCRIPT, 1, bucketKey(key, tenantId), points, interval, cost, blockDuration)
			);

			const allowed = result[0] === 1;
			const labels = labelTenants ? { tenant_id: tenantId || '' } : undefined;
			if (allowed) {
				mAllowed?.inc(labels);
			} else {
				mDenied?.inc(labels);
			}

			return {
				allowed,
				remaining: result[1],
				resetMs: result[2]
			};
		},

		async reset(key, tenantId) {
			await withBreaker(b, () => redis.del(bucketKey(key, tenantId)));
		},

		async ban(key, duration, tenantId) {
			const dur = duration ?? (blockDuration || 60000);
			if (dur <= 0) throw new Error('redis ratelimit: ban duration must be positive');
			const bk = bucketKey(key, tenantId);
			await withBreaker(b, () => redis.eval(BAN_SCRIPT, 1, bk, dur, points, interval));
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
		}
	};
}
