// A result keeps its owner so erasure never deletes a successor's value.
// The prefix cannot occur at the start of a legacy JSON-encoded result.
export const RESULT_PREFIX = '__idem_result_v2__:';

// Every invocation touches only the user's index slot. Unacknowledged primary
// acquisitions remain unarmed and keep the hash persistent. After SET returns,
// acquireTtl + resultTtl bounds the latest possible primary expiry: commit must
// still match the original pending token and cannot extend its pending lease.
export const OWNERSHIP_SCRIPT = `
-- IDEM_OWNERSHIP
local key, op, owner, primary = KEYS[1], ARGV[1], ARGV[2], ARGV[3]
local metadata = redis.call('HMGET', key, '@unarmed', '@expires')
local unarmed = tonumber(metadata[1] or '0')
local expires = tonumber(metadata[2] or '0')
if ((not metadata[1] or not metadata[2]) and redis.call('EXISTS', key) ~= 0)
  or not unarmed or unarmed < 0 or unarmed > 9007199254740991 or unarmed ~= math.floor(unarmed)
  or not expires or expires < 0 or expires > 9007199254740991 or expires ~= math.floor(expires) then
  return redis.error_reply('invalid idempotency ownership metadata')
end
local function expireIndex()
  if redis.call('HLEN', key) <= 2 then
    redis.call('DEL', key)
  elseif unarmed == 0 then
    redis.call('PEXPIREAT', key, string.format('%.0f', expires))
  else
    redis.call('PERSIST', key)
  end
end
if op == 'register' then
  local current = redis.call('HGET', key, owner)
  if current then return current == 'u' .. primary and 1 or 0 end
  if unarmed == 9007199254740991 then return redis.error_reply('invalid idempotency ownership count') end
  redis.call('PERSIST', key)
  redis.call('HSET', key, owner, 'u' .. primary, '@unarmed', unarmed + 1, '@expires', expires)
  return 1
elseif op == 'arm' then
  local current = redis.call('HGET', key, owner)
  if current == 'a' .. primary then return 1 end
  if current ~= 'u' .. primary then return 0 end
  if unarmed < 1 then return redis.error_reply('invalid idempotency ownership count') end
  local ttl = tonumber(ARGV[4])
  local time = redis.call('TIME')
  local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
  if not ttl or ttl < 1 or ttl > 9007199254740991 - now then
    return redis.error_reply('invalid idempotency ownership lifetime')
  end
  expires = math.max(expires, now + ttl)
  unarmed = unarmed - 1
  redis.call('HSET', key, owner, 'a' .. primary, '@unarmed', unarmed, '@expires', string.format('%.0f', expires))
  -- Establish the safe hash deadline before optional field expiry. Lua errors
  -- do not roll back HSET; an ACL-denied HPEXPIRE must not leave it persistent.
  expireIndex()
  if ARGV[5] == '1' then redis.call('HPEXPIRE', key, ttl, 'FIELDS', 1, owner) end
  return 1
elseif op == 'drop' then
  local current = redis.call('HGET', key, owner)
  if not current then return 0 end
  local state = string.sub(current, 1, 1)
  if state == 'u' or state == 'U' then
    if unarmed < 1 then return redis.error_reply('invalid idempotency ownership count') end
    unarmed = unarmed - 1
  end
  redis.call('HDEL', key, owner)
  redis.call('HSET', key, '@unarmed', unarmed)
  expireIndex()
  return 1
elseif op == 'revoke' then
  local fields = redis.call('HGETALL', key)
  local result = {}
  for i = 1, #fields, 2 do
    if fields[i] ~= '@unarmed' and fields[i] ~= '@expires' then
      local state = string.sub(fields[i + 1], 1, 1)
      if state ~= 'u' and state ~= 'a' and state ~= 'U' and state ~= 'A' then
        return redis.error_reply('invalid idempotency ownership record')
      end
    end
  end
  for i = 1, #fields, 2 do
    if fields[i] ~= '@unarmed' and fields[i] ~= '@expires' then
      local value = fields[i + 1]
      redis.call('HSET', key, fields[i], string.upper(string.sub(value, 1, 1)) .. string.sub(value, 2))
      result[#result + 1] = fields[i]
      result[#result + 1] = string.sub(value, 2)
    end
  end
  if #result == 0 then redis.call('DEL', key) end
  return result
end
return redis.error_reply('invalid idempotency ownership operation')
`;

export const ERASE_SCRIPT = `
-- IDEM_ERASE_OWNER
local value = redis.call('GET', KEYS[1])
if value == ARGV[1] then return redis.call('DEL', KEYS[1]) end
if value and string.sub(value, 1, string.len(ARGV[2])) == ARGV[2] then
  local ok, envelope = pcall(cjson.decode, string.sub(value, string.len(ARGV[2]) + 1))
  if not ok or type(envelope) ~= 'table' or type(envelope[1]) ~= 'string' then
    return redis.error_reply('invalid idempotency result envelope')
  end
  if envelope[1] == ARGV[1] then return redis.call('DEL', KEYS[1]) end
end
return 0
`;

// Old writers must be drained before the format transition. Their index has no
// owner tokens; remove only the exact legacy value observed, never a v2 result
// or pending owner that has since replaced it.
export const LEGACY_ERASE_SCRIPT = `
-- IDEM_ERASE_LEGACY
local value = redis.call('GET', KEYS[1])
if value == ARGV[1]
  and string.sub(value, 1, string.len(ARGV[2])) ~= ARGV[2]
  and string.sub(value, 1, string.len(ARGV[3])) ~= ARGV[3] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

/**
 * Lua script for atomic acquire.
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token (`__idem_pending__:<uuid>`) written into the pending slot
 * ARGV[2] = acquireTtl (seconds)
 * ARGV[3] = pending prefix (`__idem_pending__`)
 *
 * Pending is detected by the ARGV[3] PREFIX (not equality): every owner writes
 * a distinct token, so an equality check against one owner's token would never
 * recognise another owner's pending slot.
 *
 * Returns one of:
 *   { 1, '',    0 }  acquired (caller runs work)
 *   { 0, '',    1 }  pending  (another caller is mid-flight)
 *   { 0, value, 0 }  result   (cached, value is JSON)
 */
export const ACQUIRE_SCRIPT = `
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if ok then
  return {1, '', 0}
end
local v = redis.call('GET', KEYS[1])
if v ~= false and string.sub(v, 1, string.len(ARGV[3])) == ARGV[3] then
  return {0, '', 1}
end
return {0, v, 0}
`;

/**
 * Lua compare-and-set for commit. Writes the committed result under the long
 * TTL only if this owner still holds the pending slot (its token is still
 * stored). Returns 1 on success, 0 if the lease was lost (a successor
 * re-acquired) - the caller then throws IdempotencyLeaseLostError.
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token
 * ARGV[2] = committed value (JSON)
 * ARGV[3] = ttl (seconds)
 */
export const COMMIT_SCRIPT = `
-- IDEM_COMMIT
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0
`;

/**
 * Lua compare-and-delete for abort. Releases the pending slot only if this
 * owner still holds it. Returns 1 if released, 0 if the lease was already
 * lost - a no-op the caller ignores (nothing of ours to release).
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token
 */
export const ABORT_SCRIPT = `
-- IDEM_ABORT
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;
