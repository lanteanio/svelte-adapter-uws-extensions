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

/**
 * Lua script for atomic token bucket consumption.
 *
 * KEYS[1] = bucket key (hash with fields: points, resetAt, bannedUntil)
 * ARGV[1] = max points
 * ARGV[2] = interval (ms)
 * ARGV[3] = cost
 * ARGV[4] = blockDuration (ms)
 *
 * Uses Redis TIME internally for clock-skew-safe timestamps.
 *
 * Returns: [allowed (0/1), remaining, resetMs]
 */
const CONSUME_SCRIPT = `
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])

-- Use Redis server time to avoid clock skew between app server and Redis
local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local vals = redis.call('hmget', key, 'points', 'resetAt', 'bannedUntil')
local points = tonumber(vals[1])
local resetAt = tonumber(vals[2])
local bannedUntil = tonumber(vals[3])

-- Initialize if missing
if points == nil then
  points = maxPoints
  resetAt = now + interval
  bannedUntil = 0
end

-- Check ban
if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

-- Refill if interval elapsed
if resetAt <= now then
  points = maxPoints
  resetAt = now + interval
end

-- Try to consume
if points >= cost then
  points = points - cost
  redis.call('hset', key, 'points', points, 'resetAt', resetAt, 'bannedUntil', bannedUntil)
  -- Set TTL to avoid stale keys: interval + blockDuration + buffer
  local ttlMs = interval + blockDuration + 60000
  redis.call('pexpire', key, ttlMs)
  return {1, points, resetAt - now}
end

-- Exhausted
if blockDuration > 0 then
  bannedUntil = now + blockDuration
  redis.call('hset', key, 'points', points, 'resetAt', resetAt, 'bannedUntil', bannedUntil)
  local ttlMs = blockDuration + 60000
  redis.call('pexpire', key, ttlMs)
  return {0, 0, blockDuration}
end

redis.call('hset', key, 'points', points, 'resetAt', resetAt, 'bannedUntil', bannedUntil)
local ttlMs = interval + 60000
redis.call('pexpire', key, ttlMs)
return {0, math.max(0, points), resetAt - now}
`;

const BAN_SCRIPT = `
local key = KEYS[1]
local duration = tonumber(ARGV[1])
local defaultPoints = tonumber(ARGV[2])
local defaultInterval = tonumber(ARGV[3])

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
 * @property {(key: string) => Promise<void>} reset
 * @property {(key: string, duration?: number) => Promise<void>} ban
 * @property {(key: string) => Promise<void>} unban
 * @property {() => Promise<void>} clear
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

	const { points, interval, blockDuration = 0, keyBy = 'ip' } = options;

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

	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mAllowed = m?.counter('ratelimit_allowed_total', 'Requests allowed');
	const mDenied = m?.counter('ratelimit_denied_total', 'Requests denied');
	const mBans = m?.counter('ratelimit_bans_total', 'Bans applied');

	// Per-connection keying uses a WeakMap to avoid leaks
	const wsKeys = new WeakMap();
	let connCounter = 0;

	/**
	 * Resolve the rate limit key for a WebSocket connection.
	 *
	 * In 'ip' mode (default), uses `userData.remoteAddress` which the core
	 * adapter v0.4.0+ resolves via ADDRESS_HEADER/XFF_DEPTH — so this is
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

	function bucketKey(key) {
		return client.key('ratelimit:' + key);
	}

	return {
		async consume(ws, cost = 1) {
			if (typeof cost !== 'number' || !Number.isInteger(cost) || cost < 1) {
				throw new Error('redis ratelimit: cost must be a positive integer');
			}
			const key = resolveKey(ws);

			b?.guard();
			let result;
			try {
				result = await redis.eval(
					CONSUME_SCRIPT,
					1,
					bucketKey(key),
					points,
					interval,
					cost,
					blockDuration
				);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			const allowed = result[0] === 1;
			if (allowed) {
				mAllowed?.inc();
			} else {
				mDenied?.inc();
			}

			return {
				allowed,
				remaining: result[1],
				resetMs: result[2]
			};
		},

		async reset(key) {
			if (b) b.guard();
			try {
				await redis.del(bucketKey(key));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async ban(key, duration) {
			const dur = duration ?? (blockDuration || 60000);
			if (dur <= 0) throw new Error('redis ratelimit: ban duration must be positive');
			const bk = bucketKey(key);
			b?.guard();
			try {
				await redis.eval(BAN_SCRIPT, 1, bk, dur, points, interval);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			mBans?.inc();
		},

		async unban(key) {
			if (b) b.guard();
			try {
				await redis.hset(bucketKey(key), 'bannedUntil', 0);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async clear() {
			if (b) b.guard();
			try {
				const pattern = client.key('ratelimit:*');
				let cursor = '0';
				do {
					const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
					cursor = nextCursor;
					if (keys.length > 0) {
						await redis.unlink(...keys);
					}
				} while (cursor !== '0');
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		}
	};
}
