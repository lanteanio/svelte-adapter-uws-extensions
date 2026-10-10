/**
 * Atomic Redis Lua programs + the cross-instance pub/sub envelope event names
 * for the Redis-backed presence tracker. Immutable; no dependencies. The KEYS
 * arity and the per-user/per-topic hash shapes documented here are load-bearing
 * for cluster correctness (the {topic} hash tag colocates a topic's keys on
 * one slot) - keep them in lockstep with the key builders in keys.js.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/lua
 */

// Cold-path identity comparison ignores object insertion order. Durable fields
// and timestamps deliberately do not participate in the legacy-writer fence.
const IDENTITY_HELPERS = `
local function decode(raw)
  if not raw then return nil end
  local ok, value = pcall(cjson.decode, raw)
  if ok and type(value) == 'table' then return value end
  return nil
end
local function sameIdentity(a, b)
  local stack = {{a, b}}
  while #stack > 0 do
    local pair = table.remove(stack)
    local left, right = pair[1], pair[2]
    if type(left) ~= type(right) then return false end
    if type(left) == 'table' then
      for key, value in pairs(left) do
        if right[key] == nil then return false end
        if type(value) == 'table' then
          table.insert(stack, {value, right[key]})
        elseif value ~= right[key] then return false end
      end
      for key in pairs(right) do if left[key] == nil then return false end end
    elseif left ~= right then return false end
  end
  return true
end
`;

/**
 * Reserve membership and an identity generation. The public roster contains
 * committed identities only; private metadata never enters the UPDATE path.
 * KEYS: membership hash, topic hash, private pending hash, all on {topic}.
 * ARGV: instance, user, selected entry JSON, timestamp, field TTL, optional token.
 * Tokenless callers retain the legacy immediate-write behavior.
 * Returns {firstInstance, previousEntry, reserved, generation, incarnation, fallback}.
 */
export const JOIN_SCRIPT = `
${IDENTITY_HELPERS}
local userKey, topicKey, pendingKey = KEYS[1], KEYS[2], KEYS[3]
local instance, user, token = ARGV[1], ARGV[2], ARGV[6]
local ts, ttl = tonumber(ARGV[4]), tonumber(ARGV[5])
if not ts or not ttl then return redis.error_reply('PRESENCE_JOIN: newTs/ttlMs must be numeric') end
local first = redis.call('HLEN', userKey) == 0
redis.call('HSET', userKey, instance, token or ts)
redis.call('HPEXPIRE', userKey, ttl, 'FIELDS', 1, instance)
local raw = redis.call('HGET', topicKey, user)
local value = decode(raw)
if not token then
  local wrote = not value or ts >= (tonumber(value.ts) or 0)
  if wrote then
    local incoming = cjson.decode(ARGV[3])
    if value and type(value.fields) == 'table' then incoming.fields = value.fields end
    redis.call('HSET', topicKey, user, cjson.encode(incoming))
    if pendingKey then redis.call('HDEL', pendingKey, user) end
  end
  redis.call('HPEXPIRE', topicKey, ttl, 'FIELDS', 1, user)
  return {first and 1 or 0, raw or false, wrote and 1 or 0, 0, '', 0}
end
local metadata = decode(redis.call('HGET', pendingKey, user))
if first or not metadata or not sameIdentity(metadata.data, value and value.data) then
  metadata = {epoch=token, version=0, committedVersion=0, ts=value and value.ts or 0, data=value and value.data}
end
local reserved = ts >= math.max(tonumber(metadata.ts) or 0, value and tonumber(value.ts) or 0)
local version = 0
if reserved then
  version = (tonumber(metadata.version) or 0) + 1
  metadata.version, metadata.ts, metadata.token = version, ts, token
end
redis.call('HSET', pendingKey, user, cjson.encode(metadata))
redis.call('HPEXPIRE', pendingKey, ttl, 'FIELDS', 1, user)
redis.call('HPEXPIRE', topicKey, ttl, 'FIELDS', 1, user)
return {first and 1 or 0, raw or false, reserved and 1 or 0, version, metadata.epoch, not value and 1 or 0}
`;

/**
 * Promote a successfully admitted generation using current durable fields.
 * The private incarnation fences expired/recreated membership. A legacy writer
 * that changes public identity remains authoritative. Returns {entry, created};
 * the created token makes a lost-reply retry report the same first promotion.
 * KEYS: membership, topic, pending hashes. ARGV: instance, user, token,
 * incarnation, generation, selected JSON, timestamp, field TTL.
 */
export const FINALIZE_JOIN_SCRIPT = `
-- PRESENCE_FINALIZE
${IDENTITY_HELPERS}
local userKey, topicKey, pendingKey = KEYS[1], KEYS[2], KEYS[3]
local user, token, epoch = ARGV[2], ARGV[3], ARGV[4]
local version, ts, ttl = tonumber(ARGV[5]), tonumber(ARGV[7]), tonumber(ARGV[8])
if not redis.call('HGET', userKey, ARGV[1]) then return false end
local raw = redis.call('HGET', topicKey, user)
local value = decode(raw)
local metadata = decode(redis.call('HGET', pendingKey, user))
if not metadata then return false end
if metadata.epoch ~= epoch or (tonumber(metadata.version) or 0) < version then return false end
if not sameIdentity(metadata.data, value and value.data) then return raw and {raw, 0} or false end
local fallback = version == 0 and not value
if fallback or version > (tonumber(metadata.committedVersion) or 0) then
  local incoming = {data=cjson.decode(ARGV[6]), ts=math.max(value and tonumber(value.ts) or 0, ts)}
  if value and type(value.fields) == 'table' then incoming.fields = value.fields end
  if not value then metadata.createdToken = token end
  metadata.data, metadata.committedVersion, metadata.committedToken = incoming.data, version, token
  raw = cjson.encode(incoming)
  redis.call('HSET', topicKey, user, raw)
  redis.call('HPEXPIRE', topicKey, ttl, 'FIELDS', 1, user)
  redis.call('HSET', pendingKey, user, cjson.encode(metadata))
  redis.call('HPEXPIRE', pendingKey, ttl, 'FIELDS', 1, user)
end
return raw and {raw, metadata.createdToken == token and 1 or 0} or false
`;

/**
 * Remove only the failed operation's membership. Identity is already committed
 * or has never been visible, so compensation never rewrites it. Private state
 * expires per user even when unrelated users keep the topic active.
 * Returns {currentOrRemovedEntry, ownCommittedToken, removedVisibleEntry}.
 * KEYS: membership, topic, pending hashes. ARGV: instance, user, token, remove, TTL.
 */
export const ROLLBACK_JOIN_SCRIPT = `
-- PRESENCE_ROLLBACK
local userKey, topicKey, pendingKey = KEYS[1], KEYS[2], KEYS[3]
local instance, user, token = ARGV[1], ARGV[2], ARGV[3]
if ARGV[4] == '1' and redis.call('HGET', userKey, instance) == token then redis.call('HDEL', userKey, instance) end
if redis.call('HLEN', userKey) == 0 then
  local removed = redis.call('HGET', topicKey, user)
  redis.call('HDEL', topicKey, user)
  redis.call('HDEL', pendingKey, user)
  return {removed or false, 0, removed and 1 or 0}
end
local raw = redis.call('HGET', topicKey, user)
local private = redis.call('HGET', pendingKey, user)
local committed = false
if private then
  local ok, value = pcall(cjson.decode, private)
  committed = ok and type(value) == 'table' and value.committedToken == token
end
return {raw or false, committed and 1 or 0, 0}
`;

/**
 * Lua script for atomic LEAVE. Removes this instance's field from the
 * per-user hash and broadcasts only when no other instance still has the
 * user (HLEN == 0 after our HDEL).
 *
 * KEYS[1] = userHashKey  (presence:user:{topic}:{userKey})
 * KEYS[2] = topicHashKey (presence:topic:{topic})
 * KEYS[3] = pendingHashKey (presence:pending:{topic})
 * ARGV[1] = instanceId
 * ARGV[2] = userKey
 *
 * Returns 1 if this instance was the last one presenting this user on the
 * topic -> caller broadcasts a leave AND the per-topic hash entry is
 * removed. Returns 0 if another instance still has the user (no broadcast,
 * per-topic hash unchanged), or if no committed public row existed.
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
  local removed = redis.call('HDEL', topicKey, userKeyStr)
  if KEYS[3] then redis.call('HDEL', KEYS[3], userKeyStr) end
  return removed
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
