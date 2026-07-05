/**
 * Atomic token-bucket Lua, shared by the Redis-backed application rate limiter
 * and the per-IP upgrade-admission bucket.
 *
 * Internal module: not in the package `exports` map, so the raw Lua never
 * appears on a public subpath. Both callers import `CONSUME_SCRIPT` from here
 * rather than minting a second copy of the audited script.
 *
 * KEYS[1] = bucket key (hash with fields: points, resetAt, bannedUntil)
 * ARGV[1] = max points
 * ARGV[2] = interval (ms)
 * ARGV[3] = cost
 * ARGV[4] = blockDuration (ms)
 * ARGV[5] = emergency scale factor (optional; default 1). The caller reads
 *           the shared factor via its cached reader and passes it as a plain
 *           argument - an ARGV, not a KEYS entry, so the script stays
 *           single-key and Redis Cluster slot-safe. The effective budget is
 *           max(1, floor(maxPoints * scale)); a bucket already holding more
 *           points than the scaled budget is clamped down immediately, so a
 *           mid-window tighten applies now, not at the next refill.
 *
 * Uses Redis TIME internally for clock-skew-safe timestamps.
 *
 * Returns: [allowed (0/1), remaining, resetMs]
 *
 * @module svelte-adapter-uws-extensions/redis/token-bucket-script
 */

export const CONSUME_SCRIPT = `
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('CONSUME: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

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

-- A tightened emergency budget applies mid-window: never hold more points
-- than the (scaled) budget allows right now.
if points > maxPoints then
  points = maxPoints
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
