/**
 * Topic bookkeeping shared by the sorted-set and stream Redis replay
 * backends: the generation counter that hands out per-topic epochs, and the
 * `maxTopics` registry that bounds how many topics keep replay state.
 *
 * Generations. Every epoch a topic is ever given comes from one counter, the
 * `next` field of `replay:generation:{replay-topics}`, which only grows and
 * never falls below the Redis server's clock in milliseconds. A value is
 * therefore never handed out twice, even after the counter itself is lost to
 * a FLUSH or to memory eviction: the next one starts above every value issued
 * before at no more than one per millisecond. That is what lets a topic's keys
 * be deleted - by eviction, by `ttl` expiry or by `clear()` - without a
 * recreated topic coming back under an epoch a client still holds for its
 * predecessor.
 *
 * Seeding. Topics stored before the registry existed are found once per
 * deployment by a scan of the replay keys. One instance holds a lease and
 * scans; the cursor and the highest epoch seen are kept in the counter hash, so
 * a pass that fails resumes where it stopped, and other instances do not scan
 * at all. A topic created while the scan is still running waits for it at most
 * SEED_WAIT_MS and then takes a generation anyway, from the clock floor above.
 * The scan enrolls each existing topic with its last access as Redis tracks it
 * (OBJECT IDLETIME), because neither buffer stores a time per entry, and raises
 * the counter above the highest stored epoch when it completes.
 *
 * Registry. `replay:topics:{replay-topics}` is a sorted set of topics scored
 * by last use. It is written only when a topic is created or cleared and when
 * eviction settles, never by a steady publish or by a read, so it adds no
 * traffic to the hot path and no shared slot to it. Publishes and reads stamp
 * `replay:used:{topic}` in the topic's own slot instead (reads at most once a
 * minute per topic per process); when the registry is over `maxTopics`, its
 * lowest-scored entries are checked against that stamp, and an entry used
 * since it was scored is moved up rather than evicted. An entry is evicted only
 * when its stamp is no newer than its score, which makes it the least recently
 * used topic. Every time is the Redis server's (TIME), so instance clock skew
 * does not reorder topics.
 *
 * @module svelte-adapter-uws-extensions/redis/_replay-topics
 */

import { evalCached, evalCachedName } from '../shared/eval-cached.js';
import { execMultiSlot } from '../shared/cluster.js';
import { scanNodes } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { createLruMap } from '../shared/lru-map.js';
import { MAX_REPLAY_EPOCH_CACHE_TOPICS, REPLAY_READ_USE_INTERVAL_MS } from '../shared/caps.js';
import { now, randomUuid, setTimer, clearTimer } from '../shared/runtime.js';

/** Registry entries checked per eviction round. */
const EVICTION_BATCH = 16;

/**
 * Eviction rounds per reservation. A round ends early when every candidate
 * was evicted; candidates used since they were scored are moved up and the
 * next lowest are checked.
 */
const EVICTION_ROUNDS = 4;

/** How long a topic creation waits for a running seed scan before going on. */
const SEED_WAIT_MS = 2_000;

/** Seed lease; extended on every scanned batch. */
const SEED_LEASE_MS = 30_000;

/** A process that lost the lease, or failed, tries again at most this often. */
const SEED_RETRY_MS = 10_000;

/** Server time in ms, read by every script that stamps a use. */
const NOW_MS = `
local serverTime = redis.call('time')
local nowMs = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
`;

/**
 * Reserve a generation and record the topic as used now. Returns `{0, 0}`
 * when the counter is missing and ARGV[4] does not allow creating it, else
 * `{generation, seeded, topic1, score1, ...}` with the lowest-scored registry
 * entries when the registry holds more than `maxTopics` topics. The reserving
 * topic is never a candidate: its score can tie the lowest one, and skipping
 * it then would leave the store over the cap.
 *
 * KEYS[1] = generation hash, KEYS[2] = registry
 * ARGV[1] = topic, ARGV[2] = maxTopics, ARGV[3] = batch, ARGV[4] = '1' to create the counter
 */
const RESERVE_SCRIPT = `
-- REPLAY_TOPIC_RESERVE
local generationKey = KEYS[1]
local registryKey = KEYS[2]
local topic = ARGV[1]
local maxTopics = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])
if maxTopics == nil or batch == nil then
  return redis.error_reply('REPLAY_TOPIC_RESERVE: maxTopics/batch must be numeric')
end
local last = tonumber(redis.call('hget', generationKey, 'next') or '')
if last == nil and ARGV[4] ~= '1' then
  return {0, 0}
end
${NOW_MS}
local generation = nowMs
if last ~= nil and last >= nowMs then
  generation = last + 1
end
redis.call('hset', generationKey, 'next', generation)
local seeded = 0
if redis.call('hget', generationKey, 'seeded') == '1' then
  seeded = 1
end
redis.call('zadd', registryKey, nowMs, topic)
local over = redis.call('zcard', registryKey) - maxTopics
local reply = {generation, seeded}
if over > 0 then
  if over > batch then
    over = batch
  end
  local candidates = redis.call('zrange', registryKey, 0, over, 'WITHSCORES')
  for i = 1, #candidates, 2 do
    if #reply >= 2 + over * 2 then
      break
    end
    if candidates[i] ~= topic then
      reply[#reply + 1] = candidates[i]
      reply[#reply + 1] = candidates[i + 1]
    end
  end
end
return reply
`;

/**
 * The lowest-scored registry entries while the registry is over the cap,
 * leaving out the topic being created (see RESERVE_SCRIPT).
 *
 * KEYS[1] = registry
 * ARGV[1] = maxTopics, ARGV[2] = batch, ARGV[3] = topic being created
 */
const CANDIDATES_SCRIPT = `
-- REPLAY_TOPIC_CANDIDATES
local registryKey = KEYS[1]
local maxTopics = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local topic = ARGV[3]
if maxTopics == nil or batch == nil then
  return redis.error_reply('REPLAY_TOPIC_CANDIDATES: maxTopics/batch must be numeric')
end
local over = redis.call('zcard', registryKey) - maxTopics
if over <= 0 then
  return {}
end
if over > batch then
  over = batch
end
local candidates = redis.call('zrange', registryKey, 0, over, 'WITHSCORES')
local reply = {}
for i = 1, #candidates, 2 do
  if #reply >= over * 2 then
    break
  end
  if candidates[i] ~= topic then
    reply[#reply + 1] = candidates[i]
    reply[#reply + 1] = candidates[i + 1]
  end
end
return reply
`;

/**
 * Evict one topic unless it was used since its registry score. Runs in the
 * topic's own slot, so the check and the delete are atomic against a publish
 * or a read stamping the topic. Returns 0 when the keys were deleted, else the
 * newer use stamp.
 *
 * KEYS[1] = seq, KEYS[2] = sorted-set buffer, KEYS[3] = stream buffer,
 * KEYS[4] = epoch, KEYS[5] = use stamp
 * ARGV[1] = registry score the candidate was read with
 */
const EVICT_SCRIPT = `
-- REPLAY_TOPIC_EVICT
local seen = tonumber(ARGV[1])
if seen == nil then
  return redis.error_reply('REPLAY_TOPIC_EVICT: score must be numeric')
end
local used = tonumber(redis.call('get', KEYS[5]) or '0') or 0
if used > seen then
  return used
end
redis.call('unlink', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5])
return 0
`;

/**
 * Apply eviction outcomes to the registry. Each entry is a triple (topic,
 * score read, new score); an empty new score removes the entry. An entry whose
 * score changed since it was read belongs to a newer use of the topic (it was
 * recreated or cleared meanwhile) and is left alone. Returns entries removed.
 *
 * KEYS[1] = registry
 */
const SETTLE_SCRIPT = `
-- REPLAY_TOPIC_SETTLE
local registryKey = KEYS[1]
local removed = 0
for i = 1, #ARGV, 3 do
  local topic = ARGV[i]
  local current = redis.call('zscore', registryKey, topic)
  if current and tonumber(current) == tonumber(ARGV[i + 1]) then
    if ARGV[i + 2] == '' then
      redis.call('zrem', registryKey, topic)
      removed = removed + 1
    else
      redis.call('zadd', registryKey, 'XX', ARGV[i + 2], topic)
    end
  end
end
return removed
`;

/**
 * Stamp a read as a use of an existing topic, keeping the topic's expiry.
 * Returns 0 for a topic with no seq space (nothing is created).
 *
 * KEYS[1] = seq, KEYS[2] = use stamp
 */
export const TOUCH_SCRIPT = `
-- REPLAY_TOPIC_TOUCH
local pttl = redis.call('pttl', KEYS[1])
if pttl == -2 then
  return 0
end
${NOW_MS}
redis.call('set', KEYS[2], nowMs)
if pttl > 0 then
  redis.call('pexpire', KEYS[2], pttl)
end
return 1
`;

/**
 * Take or extend the seed lease. Returns 2 when the deployment is already
 * seeded, 1 when this token holds the lease, 0 when another live one does.
 *
 * KEYS[1] = generation hash
 * ARGV[1] = token, ARGV[2] = lease ms
 */
const SEED_LEASE_SCRIPT = `
-- REPLAY_SEED_LEASE
local generationKey = KEYS[1]
if redis.call('hget', generationKey, 'seeded') == '1' then
  return 2
end
${NOW_MS}
local owner = redis.call('hget', generationKey, 'lease')
local untilMs = tonumber(redis.call('hget', generationKey, 'leaseUntil') or '')
if owner and owner ~= ARGV[1] and untilMs ~= nil and untilMs > nowMs then
  return 0
end
redis.call('hset', generationKey, 'lease', ARGV[1], 'leaseUntil', nowMs + tonumber(ARGV[2]))
return 1
`;

/**
 * Record one scanned batch: enroll its topics (an already registered topic
 * keeps its score), raise the epoch floor, store the node's next cursor and
 * extend the lease. Returns 0 when the lease is no longer this token's.
 *
 * KEYS[1] = generation hash, KEYS[2] = registry
 * ARGV[1] = token, ARGV[2] = lease ms, ARGV[3] = node, ARGV[4] = next cursor
 * ('done' when the node is finished), ARGV[5] = highest epoch in the batch,
 * then pairs of topic and idle seconds (-1 = unknown)
 */
const SEED_PROGRESS_SCRIPT = `
-- REPLAY_SEED_PROGRESS
local generationKey = KEYS[1]
local registryKey = KEYS[2]
if redis.call('hget', generationKey, 'lease') ~= ARGV[1] then
  return 0
end
${NOW_MS}
for i = 6, #ARGV, 2 do
  local idle = tonumber(ARGV[i + 1])
  local score = 0
  if idle ~= nil and idle >= 0 then
    score = nowMs - idle * 1000
  end
  redis.call('zadd', registryKey, 'NX', score, ARGV[i])
end
local floor = tonumber(ARGV[5]) or 0
local current = tonumber(redis.call('hget', generationKey, 'floor') or '0') or 0
if floor > current then
  redis.call('hset', generationKey, 'floor', ARGV[5])
end
redis.call('hset', generationKey, 'cursor:' .. ARGV[3], ARGV[4])
redis.call('hset', generationKey, 'leaseUntil', nowMs + tonumber(ARGV[2]))
return 1
`;

/**
 * Finish seeding: raise the counter above every stored epoch and the clock,
 * mark the deployment seeded and drop the scan state. Returns the registry
 * size, or -1 when the lease is no longer this token's.
 *
 * KEYS[1] = generation hash, KEYS[2] = registry
 * ARGV[1] = token
 */
const SEED_FINISH_SCRIPT = `
-- REPLAY_SEED_FINISH
local generationKey = KEYS[1]
if redis.call('hget', generationKey, 'lease') ~= ARGV[1] then
  return -1
end
${NOW_MS}
local next = math.max(tonumber(redis.call('hget', generationKey, 'next') or '0') or 0, tonumber(redis.call('hget', generationKey, 'floor') or '0') or 0, nowMs)
redis.call('hset', generationKey, 'next', next, 'seeded', '1')
local fields = redis.call('hkeys', generationKey)
for i = 1, #fields do
  if fields[i] == 'lease' or fields[i] == 'leaseUntil' or fields[i] == 'floor' or string.sub(fields[i], 1, 7) == 'cursor:' then
    redis.call('hdel', generationKey, fields[i])
  end
end
return redis.call('zcard', KEYS[2])
`;

/**
 * Reset a topic's seq space under a new generation: the buffer is dropped,
 * the counter restarts at 0 (kept, so the next publish continues under this
 * generation instead of taking another) and the epoch and use stamp are set.
 *
 * KEYS[1] = seq, KEYS[2] = buffer, KEYS[3] = epoch, KEYS[4] = use stamp
 * ARGV[1] = generation, ARGV[2] = ttl seconds (0 = none)
 */
const CLEAR_TOPIC_SCRIPT = `
-- REPLAY_CLEAR_TOPIC
local ttl = tonumber(ARGV[2])
if ttl == nil or tonumber(ARGV[1]) == nil then
  return redis.error_reply('REPLAY_CLEAR_TOPIC: generation/ttl must be numeric')
end
${NOW_MS}
redis.call('unlink', KEYS[2])
redis.call('set', KEYS[1], '0')
redis.call('set', KEYS[3], ARGV[1])
redis.call('set', KEYS[4], nowMs)
if ttl > 0 then
  redis.call('expire', KEYS[1], ttl)
  redis.call('expire', KEYS[3], ttl)
  redis.call('expire', KEYS[4], ttl)
end
return 1
`;

/**
 * Validate and default the `maxTopics` option the same way the in-memory
 * replay plugin does.
 *
 * @param {string} prefix
 * @param {unknown} value
 * @returns {number}
 */
export function parseMaxTopics(prefix, value) {
	if (value !== undefined && (typeof value !== 'number' || !Number.isInteger(value) || value < 1)) {
		throw new Error(`${prefix}: maxTopics must be a positive integer, got ${value}`);
	}
	return value || 100;
}

// A scan cursor is meaningful only on the node that issued it, so stored
// cursors are keyed by node address (by position where none is known).
function nodeName(redis, node, index) {
	if (node === redis) return 'standalone';
	const options = /** @type {any} */ (node).options || {};
	return options.host !== undefined && options.port !== undefined ? String(options.host) + ':' + String(options.port) : 'node' + index;
}

/**
 * @param {import('./index.js').RedisClient} client
 * @param {{
 *   maxTopics: number,
 *   breaker?: import('../shared/breaker.js').CircuitBreaker,
 *   onEvicted?: (topic: string) => void,
 *   onSeeded?: (topics: number) => void
 * }} options
 */
export function createReplayTopics(client, { maxTopics, breaker: b, onEvicted, onSeeded }) {
	const redis = client.redis;
	const generationKey = client.key('replay:generation:{replay-topics}');
	const registryKey = client.key('replay:topics:{replay-topics}');
	const seqPrefix = client.key('replay:seq:{');
	const epochPrefix = client.key('replay:epoch:{');

	const keys = {
		seq: (topic) => client.key('replay:seq:{' + topic + '}'),
		buf: (topic) => client.key('replay:buf:{' + topic + '}'),
		streambuf: (topic) => client.key('replay:streambuf:{' + topic + '}'),
		epoch: (topic) => client.key('replay:epoch:{' + topic + '}'),
		used: (topic) => client.key('replay:used:{' + topic + '}')
	};

	// Last read stamp this process sent per topic, bounded like the epoch cache.
	const readStamps = createLruMap(MAX_REPLAY_EPOCH_CACHE_TOPICS);

	let seedRun = null;
	let seedTriedAt = -Infinity;
	function kickSeed() {
		if (seedRun) return seedRun;
		if (now() - seedTriedAt < SEED_RETRY_MS) return Promise.resolve();
		seedTriedAt = now();
		seedRun = withBreaker(b, runSeed).catch(() => {}).finally(() => { seedRun = null; });
		return seedRun;
	}

	async function runSeed() {
		const token = randomUuid();
		if (Number(await evalCached(redis, SEED_LEASE_SCRIPT, 1, generationKey, token, SEED_LEASE_MS)) !== 1) return;
		const pattern = client.key('replay:*');
		const nodes = scanNodes(redis);
		for (let index = 0; index < nodes.length; index++) {
			const node = nodes[index];
			const name = nodeName(redis, node, index);
			let cursor = (await redis.hget(generationKey, 'cursor:' + name)) ?? '0';
			while (cursor !== 'done') {
				const [next, found] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				const topics = new Set();
				for (const key of found) {
					if (!key.endsWith('}')) continue;
					if (key.startsWith(seqPrefix)) topics.add(key.slice(seqPrefix.length, -1));
					else if (key.startsWith(epochPrefix)) topics.add(key.slice(epochPrefix.length, -1));
				}
				const list = [...topics];
				const pairs = [];
				let floor = 0;
				if (list.length > 0) {
					// Asked of the node that returned the keys, which owns their
					// slots. OBJECT IDLETIME is the last access Redis tracks; it
					// fails under an LFU memory policy (or an ACL without it), and
					// such a topic then ranks as oldest (-1).
					const pipe = node.pipeline();
					for (const t of list) {
						pipe.object('IDLETIME', keys.seq(t));
						pipe.get(keys.epoch(t));
					}
					const results = await pipe.exec();
					for (let i = 0; i < list.length; i++) {
						const [idleErr, idle] = results[2 * i];
						const [epochErr, epoch] = results[2 * i + 1];
						if (epochErr) throw epochErr;
						const value = parseInt(epoch, 10);
						if (Number.isSafeInteger(value) && value > floor) floor = value;
						pairs.push(list[i], !idleErr && Number.isInteger(idle) ? idle : -1);
					}
				}
				const nextCursor = String(next) === '0' ? 'done' : String(next);
				const kept = await evalCached(redis, SEED_PROGRESS_SCRIPT, 2, generationKey, registryKey,
					token, SEED_LEASE_MS, name, nextCursor, floor, ...pairs);
				if (Number(kept) !== 1) return;
				cursor = nextCursor;
			}
		}
		const enrolled = Number(await evalCached(redis, SEED_FINISH_SCRIPT, 2, generationKey, registryKey, token));
		if (enrolled >= 0) onSeeded?.(enrolled);
	}

	async function evict(flat, topic) {
		for (let round = 0; round < EVICTION_ROUNDS; round++) {
			const candidates = [];
			for (let i = 0; i + 1 < flat.length; i += 2) {
				if (flat[i] !== topic) candidates.push([String(flat[i]), String(flat[i + 1])]);
			}
			if (candidates.length === 0) return;
			const name = evalCachedName(redis, EVICT_SCRIPT);
			const results = await execMultiSlot(redis, candidates.map(([t, score]) =>
				[name, 5, keys.seq(t), keys.buf(t), keys.streambuf(t), keys.epoch(t), keys.used(t), score]));
			const settle = [];
			let moved = false;
			for (let i = 0; i < candidates.length; i++) {
				const [err, value] = results[i];
				if (err) continue;
				const [t, score] = candidates[i];
				if (Number(value) === 0) {
					settle.push(t, score, '');
					onEvicted?.(t);
				} else {
					settle.push(t, score, String(value));
					moved = true;
				}
			}
			if (settle.length > 0) await evalCached(redis, SETTLE_SCRIPT, 1, registryKey, ...settle);
			if (!moved) return;
			flat = await evalCached(redis, CANDIDATES_SCRIPT, 1, registryKey, maxTopics, EVICTION_BATCH, topic);
		}
	}

	function boundedWait(promise, ms) {
		let timer;
		const timeout = new Promise((resolve) => { timer = setTimer(resolve, ms); timer?.unref?.(); });
		return Promise.race([promise, timeout]).finally(() => clearTimer(timer));
	}

	/**
	 * Take a fresh generation for `topic`, record the topic as used and, when
	 * the registry is over `maxTopics`, evict the least recently used topics.
	 * Eviction failures do not fail the caller: the topic's own write still
	 * happens and the next creation retries the eviction.
	 *
	 * @param {string} topic
	 * @returns {Promise<number>}
	 */
	async function reserve(topic) {
		const call = (create) => withBreaker(b, () =>
			evalCached(redis, RESERVE_SCRIPT, 2, generationKey, registryKey, topic, maxTopics, EVICTION_BATCH, create));
		let reply = await call('0');
		if (Number(reply[0]) === 0) {
			// No counter: an unseeded deployment, or one whose counter was lost.
			// Give the seed scan a bounded head start so the counter can start
			// above the stored epochs, then create it from the clock floor.
			await boundedWait(kickSeed(), SEED_WAIT_MS);
			reply = await call('1');
		}
		if (Number(reply[1]) !== 1) kickSeed();
		if (reply.length > 2) {
			try {
				await withBreaker(b, () => evict(reply.slice(2), topic));
			} catch {
				// Over the cap until the next creation evicts; the reserved
				// generation is still valid.
			}
		}
		return Number(reply[0]);
	}

	/**
	 * Whether a read of `topic` should stamp it as used: at most once per
	 * REPLAY_READ_USE_INTERVAL_MS per topic in this process. Records the stamp.
	 *
	 * @param {string} topic
	 * @returns {boolean}
	 */
	function readStampDue(topic) {
		const at = now();
		const last = readStamps.get(topic);
		if (last !== undefined && at - last < REPLAY_READ_USE_INTERVAL_MS) return false;
		readStamps.set(topic, at);
		return true;
	}

	/**
	 * Reset a topic's seq space under a fresh generation.
	 *
	 * @param {string} topic
	 * @param {string} bufKey - the calling backend's buffer key
	 * @param {number} ttl
	 * @returns {Promise<number>} the new epoch
	 */
	async function clearTopic(topic, bufKey, ttl) {
		const generation = await reserve(topic);
		await withBreaker(b, () => evalCached(redis, CLEAR_TOPIC_SCRIPT, 4,
			keys.seq(topic), bufKey, keys.epoch(topic), keys.used(topic), generation, ttl));
		return generation;
	}

	return {
		keys, generationKey, registryKey, reserve, clearTopic, readStampDue,
		touchCommand: () => evalCachedName(redis, TOUCH_SCRIPT)
	};
}
