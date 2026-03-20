/**
 * Lua script for server-side stale field cleanup.
 * Runs HGETALL + timestamp check + batch HDEL entirely on Redis,
 * avoiding transferring the full hash to the client.
 *
 * KEYS[1] = hash key
 * ARGV[1] = now (ms)
 * ARGV[2] = ttlMs
 *
 * Returns number of removed fields.
 */
export const CLEANUP_SCRIPT = `-- CLEANUP_STALE
local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local all = redis.call('hgetall', key)
local stale = {}
for i = 1, #all, 2 do
  local ok, parsed = pcall(cjson.decode, all[i+1])
  if not ok or not parsed.ts or (now - parsed.ts) > ttlMs then
    stale[#stale + 1] = all[i]
  end
end
if #stale > 0 then
  redis.call('hdel', key, unpack(stale))
end
return #stale
`;

/**
 * Lua script for server-side count of live entries in a hash.
 * Runs HGETALL + timestamp check entirely on Redis,
 * returning just the integer count.
 *
 * KEYS[1] = hash key
 * ARGV[1] = now (ms)
 * ARGV[2] = ttlMs
 *
 * Returns number of live entries.
 */
export const COUNT_SCRIPT = `
local all = redis.call('hgetall', KEYS[1])
local now = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local count = 0
for i = 1, #all, 2 do
  local ok, parsed = pcall(cjson.decode, all[i+1])
  if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
    count = count + 1
  end
end
return count
`;

/**
 * Lua script for server-side count of live entries with deduplication by userKey.
 * Used by presence where compound fields like `instanceId|userKey` mean one user
 * can have multiple hash fields across instances.
 *
 * KEYS[1] = hash key
 * ARGV[1] = now (ms)
 * ARGV[2] = ttlMs
 *
 * Returns number of unique live users.
 */
export const COUNT_DEDUP_SCRIPT = `
local all = redis.call('hgetall', KEYS[1])
local now = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local seen = {}
for i = 1, #all, 2 do
  local ok, parsed = pcall(cjson.decode, all[i+1])
  if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
    local sep = string.find(all[i], '|', 1, true)
    local userKey = sep and string.sub(all[i], sep + 1) or all[i]
    seen[userKey] = true
  end
end
local count = 0
for _ in pairs(seen) do
  count = count + 1
end
return count
`;

/**
 * Lua script for server-side presence list with deduplication.
 * Runs HGETALL + timestamp check + per-user dedup entirely on Redis,
 * returning flat [userKey, json, ...] pairs.
 *
 * KEYS[1] = hash key
 * ARGV[1] = now (ms)
 * ARGV[2] = ttlMs
 *
 * Returns array of [userKey, json, userKey, json, ...].
 */
export const LIST_SCRIPT = `
local all = redis.call('hgetall', KEYS[1])
local now = tonumber(ARGV[1])
local ttlMs = tonumber(ARGV[2])
local seen = {}
local best = {}
for i = 1, #all, 2 do
  local ok, parsed = pcall(cjson.decode, all[i+1])
  if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
    local sep = string.find(all[i], '|', 1, true)
    local userKey = sep and string.sub(all[i], sep + 1) or all[i]
    if not seen[userKey] or parsed.ts > seen[userKey] then
      seen[userKey] = parsed.ts
      best[userKey] = all[i+1]
    end
  end
end
local out = {}
for k, v in pairs(best) do
  out[#out + 1] = k
  out[#out + 1] = v
end
return out
`;
