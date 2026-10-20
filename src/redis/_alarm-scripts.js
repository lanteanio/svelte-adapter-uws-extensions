import { MAX_ALARM_GENERATION } from '../shared/alarm-generation.js';

// All eight keys share one slot. Keep generations and attempts as strings: Lua
// numbers cannot exactly represent every signed BIGINT, including the result of
// HINCRBY, and tostring() rounds large integers.
//
// KEYS: 1 due ZSET, 2 meta HASH, 3 reserved HASH, 4 committed HASH, 5 current
// HASH, 6 attempt HASH (topic -> "generation:attempt" of a requeued schedule),
// 7 executions HASH (execution id -> JSON record), 8 execution order ZSET (all
// scores 0, members are execution ids so ZRANGEBYLEX pages them in order).
//
// An execution id is the topic with every NUL byte written as NUL 0x01, then
// NUL NUL, then the generation. Byte order of ids is the (topic, generation)
// tuple order of the unescaped values.
export const ALARM_GENERATION_SCRIPT = `-- ORDERED_ALARM_GENERATIONS_V2
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
local function id(topic, generation)
  return (string.gsub(topic, '%z', '\\0\\1')) .. '\\0\\0' .. generation
end
local function execution(topic, generation)
  local raw = redis.call('HGET', KEYS[7], id(topic, generation))
  if not raw then return nil end
  local ok, record = pcall(cjson.decode, raw)
  if not ok or type(record) ~= 'table' or record.topic ~= topic
    or record.generation ~= generation or type(record.attempt) ~= 'string' then
    error('redis alarm store: inconsistent execution record')
  end
  return record
end
-- The attempt the next claim of the current schedule receives. Only an entry
-- written by requeue for this exact generation counts.
local function attempt(row)
  local value = redis.call('HGET', KEYS[6], row.topic)
  if not value then return '1' end
  local generation, count = string.match(value, '^([1-9][0-9]*):([1-9][0-9]*)$')
  if not generation then error('redis alarm store: inconsistent execution record') end
  if generation ~= row.current then return '1' end
  return count
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
  redis.call('HLEN', KEYS[6])
  local rows = {}
  for _, topic in ipairs(topics) do table.insert(rows, read(topic)) end
  for _, row in ipairs(rows) do
    adopt(row)
    if row.reserved ~= '0' then redis.call('HSET', KEYS[4], row.topic, row.reserved) end
  end
  -- Schedules only: retained executions survive and stay inspectable.
  redis.call('DEL', KEYS[1], KEYS[2], KEYS[5], KEYS[6])
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
if operation == 'executions' then
  local start = '-'
  if ARGV[3] then start = '(' .. id(ARGV[3], ARGV[4]) end
  local ids = redis.call('ZRANGEBYLEX', KEYS[8], start, '+', 'LIMIT', 0, tonumber(ARGV[2]) + 1)
  if #ids == 0 then return {} end
  local records = redis.call('HMGET', KEYS[7], unpack(ids))
  for index = 1, #ids do
    if not records[index] then error('redis alarm store: inconsistent execution record') end
  end
  return records
end
if operation == 'execution' then
  return redis.call('HGET', KEYS[7], id(ARGV[2], ARGV[3]))
end
if operation == 'complete' then
  local record = execution(ARGV[2], ARGV[3])
  if not record or record.attempt ~= ARGV[4] then return 0 end
  local key = id(ARGV[2], ARGV[3])
  redis.call('HDEL', KEYS[7], key)
  redis.call('ZREM', KEYS[8], key)
  return 1
end
if operation == 'fail' then
  local record = execution(ARGV[2], ARGV[3])
  if not record or record.attempt ~= ARGV[4] then return 0 end
  -- A repeated outcome for the same attempt keeps the first failure time.
  if record.status ~= 'failed' then
    record.status, record.failedAt, record.errorClass = 'failed', ARGV[5], 'handler'
    redis.call('HSET', KEYS[7], id(ARGV[2], ARGV[3]), cjson.encode(record))
  end
  return 1
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
  redis.call('HDEL', KEYS[6], row.topic)
  return 1
end
if operation == 'delete' then
  if row.current ~= ARGV[3] then return 0 end
  adopt(row)
  redis.call('ZREM', KEYS[1], row.topic)
  redis.call('HDEL', KEYS[2], row.topic)
  redis.call('HDEL', KEYS[5], row.topic)
  redis.call('HDEL', KEYS[6], row.topic)
  return 1
end
if operation == 'claim' then
  local generation = ARGV[3]
  if not row.score or row.current ~= generation then return false end
  local key = id(row.topic, generation)
  if redis.call('HEXISTS', KEYS[7], key) == 1 then
    error('redis alarm store: inconsistent execution record')
  end
  -- Never evict retained work: refuse the claim and keep the schedule.
  if redis.call('ZCARD', KEYS[8]) >= tonumber(ARGV[5]) then return -1 end
  local count = attempt(row)
  adopt(row)
  redis.call('HSET', KEYS[7], key, cjson.encode({topic=row.topic, generation=generation,
    score=row.score, payload=row.payload or '', attempt=count, status='claimed', claimedAt=ARGV[4]}))
  redis.call('ZADD', KEYS[8], 0, key)
  redis.call('ZREM', KEYS[1], row.topic)
  redis.call('HDEL', KEYS[2], row.topic)
  redis.call('HDEL', KEYS[5], row.topic)
  redis.call('HDEL', KEYS[6], row.topic)
  return count
end
if operation == 'requeue' then
  local generation = ARGV[3]
  local record = execution(row.topic, generation)
  if not record or record.attempt ~= ARGV[4] then return 0 end
  if record.status ~= 'failed' and ARGV[7] ~= '1' then return 0 end
  -- A newer reservation or an existing schedule wins over the retry.
  if row.score or row.reserved ~= generation or row.committed ~= generation then return 0 end
  local at = tonumber(ARGV[5])
  if not at or at ~= at or at == math.huge or at == -math.huge then
    error('redis alarm store: at must be finite')
  end
  local payload = record.payload
  if payload ~= '' then
    local ok, decoded = pcall(cjson.decode, payload)
    if ok and type(decoded) == 'table' then
      local meta = decoded.meta
      if meta == nil then meta = cjson.null end
      payload = '{"at":' .. ARGV[5] .. ',"meta":' .. cjson.encode(meta) .. '}'
    end
    redis.call('HSET', KEYS[2], row.topic, payload)
  end
  redis.call('ZADD', KEYS[1], ARGV[5], row.topic)
  redis.call('HSET', KEYS[5], row.topic, generation)
  redis.call('HSET', KEYS[6], row.topic, generation .. ':' .. ARGV[6])
  local key = id(row.topic, generation)
  redis.call('HDEL', KEYS[7], key)
  redis.call('ZREM', KEYS[8], key)
  return 1
end
error('redis alarm store: unknown operation')
`;
