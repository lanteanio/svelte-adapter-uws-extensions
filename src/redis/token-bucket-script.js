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
 * This module also exports the read-only and alternate-refill companions, all
 * sharing the same single key, the same ARGV[1..5] contract, and the same
 * emergency-scale clamp:
 *   - PEEK_SCRIPT           read-only fixed window (no write, no init)
 *   - SLIDING_SCRIPT        sliding-window-counter consume
 *   - SLIDING_PEEK_SCRIPT   read-only sliding-window-counter
 *   - GCRA_SCRIPT           leaky-bucket / GCRA consume (no boundary burst)
 *   - GCRA_PEEK_SCRIPT      read-only GCRA
 * Each carries a unique marker comment so the in-memory test double routes it
 * to the matching read-only or refill evaluator instead of the spending one.
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

// Read-only fixed window: reports the verdict a consume(cost) WOULD return
// without writing, initializing, or moving any counter. Mirrors CONSUME_SCRIPT's
// return values exactly (including the ban window it would apply) but never calls
// hset/pexpire, so a peek that gates a request can never spend a point.
export const PEEK_SCRIPT = `
-- PEEK_WINDOW
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('PEEK_WINDOW: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local vals = redis.call('hmget', key, 'points', 'resetAt', 'bannedUntil')
local points = tonumber(vals[1])
local resetAt = tonumber(vals[2])
local bannedUntil = tonumber(vals[3])

if points == nil then
  points = maxPoints
  resetAt = now + interval
  bannedUntil = 0
end

if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

if resetAt <= now then
  points = maxPoints
  resetAt = now + interval
end

if points > maxPoints then
  points = maxPoints
end

if points >= cost then
  return {1, points - cost, resetAt - now}
end

if blockDuration > 0 then
  return {0, 0, blockDuration}
end

return {0, math.max(0, points), resetAt - now}
`;

// Sliding-window-counter: weights the previous fixed window by the fraction of
// it still overlapping the current position, so admission does not jump at a
// window edge - it removes the classic ~2x fixed-window boundary burst while
// keeping one atomic single-key roundtrip. State fields: curr (current window
// count), prev (previous window count), windowStart (aligned to the interval),
// bannedUntil. Same ARGV[1..5] and emergency-scale clamp as CONSUME_SCRIPT.
export const SLIDING_SCRIPT = `
-- SLIDING_CONSUME
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('SLIDING_CONSUME: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)
local winStartNow = now - (now % interval)

local vals = redis.call('hmget', key, 'curr', 'prev', 'windowStart', 'bannedUntil')
local curr = tonumber(vals[1])
local prev = tonumber(vals[2])
local windowStart = tonumber(vals[3])
local bannedUntil = tonumber(vals[4])

-- Default bannedUntil independently of the counter fields so an admin ban
-- (BAN_SCRIPT writes bannedUntil but not curr) survives the fresh-counter init.
if bannedUntil == nil then bannedUntil = 0 end
if curr == nil then
  curr = 0
  prev = 0
  windowStart = winStartNow
end

if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

local elapsed = winStartNow - windowStart
if elapsed >= interval * 2 then
  prev = 0
  curr = 0
  windowStart = winStartNow
elseif elapsed >= interval then
  prev = curr
  curr = 0
  windowStart = winStartNow
end

if curr > maxPoints then
  curr = maxPoints
end

local into = now - windowStart
local prevWeight = (interval - into) / interval
if prevWeight < 0 then prevWeight = 0 end
if prevWeight > 1 then prevWeight = 1 end
local weighted = curr + prev * prevWeight
local resetMs = windowStart + interval - now

if weighted + cost <= maxPoints then
  curr = curr + cost
  redis.call('hset', key, 'curr', curr, 'prev', prev, 'windowStart', windowStart, 'bannedUntil', bannedUntil)
  redis.call('pexpire', key, interval * 2 + blockDuration + 60000)
  local remaining = maxPoints - weighted - cost
  if remaining < 0 then remaining = 0 end
  return {1, math.floor(remaining), resetMs}
end

if blockDuration > 0 then
  bannedUntil = now + blockDuration
  redis.call('hset', key, 'curr', curr, 'prev', prev, 'windowStart', windowStart, 'bannedUntil', bannedUntil)
  redis.call('pexpire', key, blockDuration + 60000)
  return {0, 0, blockDuration}
end

redis.call('hset', key, 'curr', curr, 'prev', prev, 'windowStart', windowStart, 'bannedUntil', bannedUntil)
redis.call('pexpire', key, interval * 2 + 60000)
local remaining = maxPoints - weighted
if remaining < 0 then remaining = 0 end
return {0, math.floor(remaining), resetMs}
`;

// Read-only sliding-window-counter: the verdict SLIDING_SCRIPT would return with
// no write, no init, no ban applied.
export const SLIDING_PEEK_SCRIPT = `
-- SLIDING_PEEK
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('SLIDING_PEEK: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)
local winStartNow = now - (now % interval)

local vals = redis.call('hmget', key, 'curr', 'prev', 'windowStart', 'bannedUntil')
local curr = tonumber(vals[1])
local prev = tonumber(vals[2])
local windowStart = tonumber(vals[3])
local bannedUntil = tonumber(vals[4])

if bannedUntil == nil then bannedUntil = 0 end
if curr == nil then
  curr = 0
  prev = 0
  windowStart = winStartNow
end

if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

local elapsed = winStartNow - windowStart
if elapsed >= interval * 2 then
  prev = 0
  curr = 0
  windowStart = winStartNow
elseif elapsed >= interval then
  prev = curr
  curr = 0
  windowStart = winStartNow
end

if curr > maxPoints then
  curr = maxPoints
end

local into = now - windowStart
local prevWeight = (interval - into) / interval
if prevWeight < 0 then prevWeight = 0 end
if prevWeight > 1 then prevWeight = 1 end
local weighted = curr + prev * prevWeight
local resetMs = windowStart + interval - now

if weighted + cost <= maxPoints then
  local remaining = maxPoints - weighted - cost
  if remaining < 0 then remaining = 0 end
  return {1, math.floor(remaining), resetMs}
end

if blockDuration > 0 then
  return {0, 0, blockDuration}
end

local remaining = maxPoints - weighted
if remaining < 0 then remaining = 0 end
return {0, math.floor(remaining), resetMs}
`;

// Leaky-bucket / GCRA: one theoretical-arrival-time (tat) meters a smooth
// emission of one token every interval/points ms, tolerating a burst of up to
// points tokens. Because admission is continuous there is no window edge and no
// boundary burst. A tightened emergency budget takes effect immediately: the
// emission interval is recomputed from the scaled budget every check, so no
// stored-points clamp is needed. State fields: tat, bannedUntil.
export const GCRA_SCRIPT = `
-- GCRA_CONSUME
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('GCRA_CONSUME: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local emission = interval / maxPoints
local burst = interval

local vals = redis.call('hmget', key, 'tat', 'bannedUntil')
local tat = tonumber(vals[1])
local bannedUntil = tonumber(vals[2])
if bannedUntil == nil then bannedUntil = 0 end

if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

local tatEff = now
if tat ~= nil and tat > now then
  tatEff = tat
end

local increment = emission * cost
local newTat = tatEff + increment
local allowAt = newTat - burst

if now >= allowAt then
  local remaining = math.floor((now - allowAt) / emission)
  if remaining < 0 then remaining = 0 end
  redis.call('hset', key, 'tat', newTat, 'bannedUntil', bannedUntil)
  redis.call('pexpire', key, math.floor(burst) + blockDuration + 60000)
  return {1, remaining, math.floor(newTat - now)}
end

if blockDuration > 0 then
  bannedUntil = now + blockDuration
  redis.call('hset', key, 'tat', tatEff, 'bannedUntil', bannedUntil)
  redis.call('pexpire', key, blockDuration + 60000)
  return {0, 0, blockDuration}
end

local remaining = math.floor((now - (tatEff - burst)) / emission)
if remaining < 0 then remaining = 0 end
return {0, remaining, math.ceil(allowAt - now)}
`;

// Read-only GCRA: the verdict GCRA_SCRIPT would return with no write, no ban.
export const GCRA_PEEK_SCRIPT = `
-- GCRA_PEEK
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('GCRA_PEEK: maxPoints/interval/cost/blockDuration must be numeric')
end
local scale = tonumber(ARGV[5])
if scale ~= nil and scale > 0 and scale ~= 1 then
  maxPoints = math.floor(maxPoints * scale)
  if maxPoints < 1 then maxPoints = 1 end
end

local rtime = redis.call('TIME')
local now = tonumber(rtime[1]) * 1000 + math.floor(tonumber(rtime[2]) / 1000)

local emission = interval / maxPoints
local burst = interval

local vals = redis.call('hmget', key, 'tat', 'bannedUntil')
local tat = tonumber(vals[1])
local bannedUntil = tonumber(vals[2])
if bannedUntil == nil then bannedUntil = 0 end

if bannedUntil > now then
  return {0, 0, bannedUntil - now}
end

local tatEff = now
if tat ~= nil and tat > now then
  tatEff = tat
end

local increment = emission * cost
local newTat = tatEff + increment
local allowAt = newTat - burst

if now >= allowAt then
  local remaining = math.floor((now - allowAt) / emission)
  if remaining < 0 then remaining = 0 end
  return {1, remaining, math.floor(newTat - now)}
end

if blockDuration > 0 then
  return {0, 0, blockDuration}
end

local remaining = math.floor((now - (tatEff - burst)) / emission)
if remaining < 0 then remaining = 0 end
return {0, remaining, math.ceil(allowAt - now)}
`;
