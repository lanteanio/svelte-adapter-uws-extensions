/**
 * Atomic Redis Lua programs + the cross-instance pub/sub envelope event names
 * for the Redis-backed presence tracker. Immutable; no dependencies. The KEYS
 * arity and the per-user/per-topic hash shapes documented here are load-bearing
 * for cluster correctness (the {topic} hash tag colocates a topic's two keys on
 * one slot) - keep them in lockstep with the key builders in presence.js.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/lua
 */

/**
 * Lua script for atomic JOIN. Sets this instance's field on the per-user
 * hash (the "is this user here?" index), refreshes both hashes' per-field
 * TTLs via HPEXPIRE, and writes the user's data to the per-topic hash with
 * a newer-ts-wins conditional set.
 *
 * KEYS[1] = userHashKey  (presence:user:{topic}:{userKey})
 * KEYS[2] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = instanceId
 * ARGV[2] = userKey (also the field on topicHashKey)
 * ARGV[3] = topicHashValue (JSON {data, ts} pre-stringified)
 * ARGV[4] = ts (numeric, for the conditional set)
 * ARGV[5] = ttlMs (numeric, for HPEXPIRE)
 *
 * Returns 1 if this was the FIRST instance to present this user on the
 * topic (HLEN was 0 before our HSET) -> caller broadcasts a join. Returns
 * 0 if the user was already present from another instance or another
 * tab on this instance via an idempotent re-run.
 *
 * The conditional set on the topic hash preserves the "newer data wins"
 * property the previous LIST_SCRIPT enforced at read time. Two instances
 * with the same userKey but different `select()` output land deterministic
 * data on the topic hash regardless of arrival order: the higher-ts write
 * wins. Concurrent instances with the same ts (rare) tie-break on write
 * order, which is acceptable; subsequent heartbeats do not re-write data
 * so the tie-break sticks.
 *
 * Note: HSET on an existing field clears its per-field TTL, so every
 * HSET in this script is paired with HPEXPIRE in the same atomic block.
 */
export const JOIN_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]
local topicHashValue = ARGV[3]
local newTs = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])
if newTs == nil or ttlMs == nil then
  return redis.error_reply('PRESENCE_JOIN: newTs/ttlMs must be numeric')
end

local wasEmpty = (redis.call('HLEN', userKey) == 0)

redis.call('HSET', userKey, instanceId, newTs)
redis.call('HPEXPIRE', userKey, ttlMs, 'FIELDS', 1, instanceId)

local existing = redis.call('HGET', topicKey, userKeyStr)
local valueToWrite = topicHashValue
local shouldWrite = true
if existing then
  local ok, parsed = pcall(cjson.decode, existing)
  if ok and type(parsed) == 'table' then
    local existingTs = tonumber(parsed.ts) or 0
    if newTs < existingTs then
      shouldWrite = false
    elseif type(parsed.fields) == 'table' then
      -- Preserve dynamic fields (set via update()) across a newer-data
      -- overwrite. The join value carries identity data only, but the
      -- durable fields must survive an avatar/name change or a
      -- cross-instance new-tab join; without this they would be wiped.
      -- cjson round-trips nested objects faithfully (verified on Redis 7.4).
      local okIncoming, incoming = pcall(cjson.decode, valueToWrite)
      if okIncoming and type(incoming) == 'table' then
        incoming.fields = parsed.fields
        valueToWrite = cjson.encode(incoming)
      end
    end
  end
end
if shouldWrite then
  redis.call('HSET', topicKey, userKeyStr, valueToWrite)
end
redis.call('HPEXPIRE', topicKey, ttlMs, 'FIELDS', 1, userKeyStr)

return wasEmpty and 1 or 0
`;

/**
 * Lua script for atomic LEAVE. Removes this instance's field from the
 * per-user hash and broadcasts only when no other instance still has the
 * user (HLEN == 0 after our HDEL).
 *
 * KEYS[1] = userHashKey  (presence:user:{topic}:{userKey})
 * KEYS[2] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = instanceId
 * ARGV[2] = userKey
 *
 * Returns 1 if this instance was the last one presenting this user on the
 * topic -> caller broadcasts a leave AND the per-topic hash entry is
 * removed. Returns 0 if another instance still has the user (no broadcast,
 * per-topic hash unchanged).
 *
 * The O(1) HLEN check replaces the previous O(M_topic) suffix-scan loop.
 * Mass-disconnect of N users is now O(N) Redis-blocked Lua time rather
 * than O(N x M_topic).
 */
export const LEAVE_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]

redis.call('HDEL', userKey, instanceId)
if redis.call('HLEN', userKey) == 0 then
  redis.call('HDEL', topicKey, userKeyStr)
  return 1
end
return 0
`;

/**
 * Lua script for atomic field-level UPDATE. Merges a user's changed DURABLE
 * dynamic fields (set via `update()`, transient fields excluded by the caller)
 * into the per-topic hash value's `fields` object so a cross-instance `state`
 * read (HGETALL) reconstructs them. Transient fields are never passed here -
 * they are relay-only and must never persist, so a (re)joining client never
 * inherits a stale transient value.
 *
 * KEYS[1] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = userKey (the field on the topic hash)
 * ARGV[2] = durableFieldsJson (a non-empty JSON object of changed durable fields)
 * ARGV[3] = ts (numeric, refreshes the stored timestamp so a stale concurrent
 *           join cannot overwrite the merged value)
 * ARGV[4] = ttlMs (numeric, for HPEXPIRE - HSET clears the field TTL so it is
 *           re-armed in the same atomic block, matching JOIN_SCRIPT)
 *
 * Returns 1 if the merge was applied, 0 if the user is not present on the
 * topic (already left, or never joined) - the durable value is dropped, which
 * is correct: there is no user to carry it.
 *
 * Single-key (the topic hash only), so an instance's own subscribers' durable
 * fields persist without touching the per-user ownership hash. cjson round-trips
 * the existing value faithfully (an empty `data` object stays `{}`, verified on
 * Redis 7.4), so the identity payload is preserved byte-for-byte.
 */
export const UPDATE_SCRIPT = `
local topicKey = KEYS[1]
local userKey = ARGV[1]
local newTs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
if newTs == nil or ttlMs == nil then
  return redis.error_reply('PRESENCE_UPDATE: newTs/ttlMs must be numeric')
end
local existing = redis.call('HGET', topicKey, userKey)
if not existing then return 0 end
local ok, parsed = pcall(cjson.decode, existing)
if not ok or type(parsed) ~= 'table' then return 0 end
local okd, durable = pcall(cjson.decode, ARGV[2])
if not okd or type(durable) ~= 'table' then return 0 end
if type(parsed.fields) ~= 'table' then parsed.fields = {} end
for k, v in pairs(durable) do parsed.fields[k] = v end
parsed.ts = newTs
redis.call('HSET', topicKey, userKey, cjson.encode(parsed))
redis.call('HPEXPIRE', topicKey, ttlMs, 'FIELDS', 1, userKey)
return 1
`;

/**
 * Internal cross-instance Redis pub/sub envelope event names. NOT the
 * client wire shape - clients see `state` / `diff` /
 * `heartbeat`. These names live on the `presence:events:{topic}` channel
 * between instances and are routed into the local diff buffer on receive.
 */
export const INTERNAL_EVENTS = Object.freeze({
	JOIN: 'join',
	LEAVE: 'leave',
	UPDATED: 'updated',
	FIELDS: 'fields'
});
