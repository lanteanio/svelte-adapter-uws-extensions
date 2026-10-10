import { MAX_ALARM_GENERATION } from '../shared/alarm-generation.js';

// All five keys share one slot. Keep generations as strings: Lua numbers cannot
// exactly represent every signed BIGINT, including the result of HINCRBY.
export const ALARM_GENERATION_SCRIPT = `-- ORDERED_ALARM_GENERATIONS_V1
local maximum = '${MAX_ALARM_GENERATION}'
local function less(a, b)
  return #a < #b or (#a == #b and a < b)
end
local function valid(value, zero)
  if zero and value == '0' then return true end
  return type(value) == 'string' and string.match(value, '^[1-9][0-9]*$')
    and not less(maximum, value)
end
local function corrupt()
  error('redis alarm store: inconsistent generation authority')
end
local function validate(topic, score, payload, reserved, committed, current)
  local legacy = false
  if not reserved then
    if committed or current then corrupt() end
    reserved, committed = '0', '0'
    if score then
      reserved, committed, current, legacy = '1', '1', '1', true
    end
  elseif not valid(reserved, false) or not valid(committed, true)
    or less(reserved, committed) then
    corrupt()
  end
  if score then
    if not valid(current, false) or current ~= committed then corrupt() end
  elseif current then
    corrupt()
  end
  return {topic=topic, score=score, payload=payload, reserved=reserved,
    committed=committed, current=current, legacy=legacy}
end
local function read(topic)
  return validate(topic, redis.call('ZSCORE', KEYS[1], topic),
    redis.call('HGET', KEYS[2], topic), redis.call('HGET', KEYS[3], topic),
    redis.call('HGET', KEYS[4], topic), redis.call('HGET', KEYS[5], topic))
end
local function adopt(row)
  if row.legacy then
    redis.call('HSET', KEYS[3], row.topic, '1')
    redis.call('HSET', KEYS[4], row.topic, '1')
    redis.call('HSET', KEYS[5], row.topic, '1')
  end
end
local function result(row)
  if not row.score then return {} end
  return {row.topic, row.score, row.payload or '', row.current}
end
local operation = ARGV[1]
if operation == 'clear' then
  local topics, seen = {}, {}
  local function include(topic)
    if not seen[topic] then seen[topic] = true; table.insert(topics, topic) end
  end
  for _, topic in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do include(topic) end
  -- Read every hash before any writes, including metadata, so WRONGTYPE cannot
  -- leave a partially cleared store. Redis scripts do not roll back errors.
  for key = 2, 5 do
    for _, topic in ipairs(redis.call('HKEYS', KEYS[key])) do include(topic) end
  end
  local rows = {}
  for _, topic in ipairs(topics) do table.insert(rows, read(topic)) end
  for _, row in ipairs(rows) do
    adopt(row)
    if row.reserved ~= '0' then redis.call('HSET', KEYS[4], row.topic, row.reserved) end
  end
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[5])
  return 1
end
if operation == 'due' then
  local entries = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'WITHSCORES', 'LIMIT', 0, ARGV[3])
  local rows, out = {}, {}
  -- Bound unpack's argument count even when dueBatch exceeds Lua's stack limit.
  -- Read and validate every chunk before legacy adoption: scripts do not roll
  -- back writes if a later chunk has corrupt authority or a wrong key type.
  for first = 1, #entries, 512 do
    local topics = {}
    for index = first, math.min(first + 511, #entries), 2 do
      topics[#topics + 1] = entries[index]
    end
    local payloads = redis.call('HMGET', KEYS[2], unpack(topics))
    local reserved = redis.call('HMGET', KEYS[3], unpack(topics))
    local committed = redis.call('HMGET', KEYS[4], unpack(topics))
    local current = redis.call('HMGET', KEYS[5], unpack(topics))
    for index, topic in ipairs(topics) do
      rows[#rows + 1] = validate(topic, entries[first + (index - 1) * 2 + 1],
        payloads[index], reserved[index], committed[index], current[index])
    end
  end
  for _, row in ipairs(rows) do
    adopt(row)
    table.insert(out, result(row))
  end
  return out
end
local row = read(ARGV[2])
if operation == 'next' then
  if row.reserved == maximum then error('redis alarm store: generation allocator exhausted') end
  adopt(row)
  if row.reserved == '0' then redis.call('HSET', KEYS[4], row.topic, '0') end
  redis.call('HINCRBY', KEYS[3], row.topic, 1)
  return redis.call('HGET', KEYS[3], row.topic)
end
if operation == 'get' then
  adopt(row)
  return result(row)
end
if operation == 'set' then
  local generation = ARGV[3]
  if not valid(generation, false) then error('redis alarm store: invalid generation') end
  if generation ~= row.reserved or not less(row.committed, generation) then return 0 end
  local at = tonumber(ARGV[4])
  if not at or at ~= at or at == math.huge or at == -math.huge then
    error('redis alarm store: at must be finite')
  end
  redis.call('HSET', KEYS[2], row.topic, ARGV[5])
  redis.call('ZADD', KEYS[1], ARGV[4], row.topic)
  redis.call('HSET', KEYS[4], row.topic, generation)
  redis.call('HSET', KEYS[5], row.topic, generation)
  return 1
end
if operation == 'delete' then
  if row.current ~= ARGV[3] then return 0 end
  adopt(row)
  redis.call('ZREM', KEYS[1], row.topic)
  redis.call('HDEL', KEYS[2], row.topic)
  redis.call('HDEL', KEYS[5], row.topic)
  return 1
end
error('redis alarm store: unknown operation')
`;
