/**
 * Topic bookkeeping shared by the sorted-set and stream Redis replay
 * backends: the `maxTopics` registry that bounds how many topics keep replay
 * state, and the epoch every new seq space is given.
 *
 * Epochs. As in the adapter, an epoch is an opaque random 32-bit integer,
 * compared only for equality: it is drawn from the runtime RNG when a topic's
 * seq space is created or recreated and stored with it, and carries no time
 * and no count. A recreated topic draws again, and the creating script makes
 * sure the draw differs from the epoch still stored for the topic, if any.
 * The accepted failure is a 2^-32 collision with an epoch the topic had before
 * and no longer stores: one missed reset, never corruption.
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
 * Seeding. Topics stored before the registry existed are enrolled once per
 * deployment by a scan of the replay keys, at score 0: neither buffer stores a
 * time per entry, so they rank as least recently used until a publish or read
 * after the upgrade stamps them. One instance holds a lease and scans; the
 * cursor is kept in `replay:seed:{replay-topics}`, so a pass that fails resumes
 * where it stopped, and other instances do not scan. A topic created before
 * the deployment is seeded waits for the scan at most SEED_WAIT_MS.
 *
 * @module svelte-adapter-uws-extensions/redis/_replay-topics
 */

import { evalCached, evalCachedName } from '../shared/eval-cached.js';
import { execMultiSlot } from '../shared/cluster.js';
import { scanNodes } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { createLruMap } from '../shared/lru-map.js';
import { MAX_REPLAY_EPOCH_CACHE_TOPICS, REPLAY_READ_USE_INTERVAL_MS } from '../shared/caps.js';
import { now, randomUuid, randomU32, setTimer, clearTimer } from '../shared/runtime.js';

/** Registry entries checked per eviction round. */
const EVICTION_BATCH = 16;

/**
 * Eviction rounds per reservation. A round ends early when every candidate
 * was evicted; candidates used since they were scored are moved up and the
 * next lowest are checked.
 */
const EVICTION_ROUNDS = 4;

/**
 * How long a topic creation in an unseeded deployment waits for the seed scan,
 * so the cap counts the existing topics; it then goes on without them.
 */
const SEED_WAIT_MS = 2_000;

/** Seed lease; extended on every scanned batch. */
const SEED_LEASE_MS = 30_000;

/** A process that lost the lease, or failed, tries again at most this often. */
const SEED_RETRY_MS = 10_000;

/**
 * The epoch a new seq space takes: the drawn value, unless that equals the
 * epoch still stored for the topic, in which case the next value (wrapping
 * within 1 .. 2^32 - 1). Returned as text, as stored.
 */
export const FRESH_EPOCH = `
local function freshEpoch(epochKey, drawn)
  if redis.call('get', epochKey) == drawn then
    return string.format('%.0f', tonumber(drawn) % 4294967295 + 1)
  end
  return drawn
end
`;

/** Server time in ms, read by every script that stamps a use. */
const NOW_MS = `
local serverTime = redis.call('time')
local nowMs = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
`;

/**
 * Record the topic as used now. Returns `{seeded, topic1, score1, ...}` with
 * the lowest-scored registry entries when the registry holds more than
 * `maxTopics` topics. The reserving topic is never a candidate: its score can
 * tie the lowest one, and skipping it then would leave the store over the cap.
 *
 * KEYS[1] = seed state hash, KEYS[2] = registry
 * ARGV[1] = topic, ARGV[2] = maxTopics, ARGV[3] = batch
 */
const RESERVE_SCRIPT = `
-- REPLAY_TOPIC_RESERVE
local seedKey = KEYS[1]
local registryKey = KEYS[2]
local topic = ARGV[1]
local maxTopics = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])
if maxTopics == nil or batch == nil then
  return redis.error_reply('REPLAY_TOPIC_RESERVE: maxTopics/batch must be numeric')
end
${NOW_MS}
local seeded = 0
if redis.call('hget', seedKey, 'seeded') == '1' then
  seeded = 1
end
redis.call('zadd', registryKey, nowMs, topic)
local over = redis.call('zcard', registryKey) - maxTopics
local reply = {seeded}
if over > 0 then
  if over > batch then
    over = batch
  end
  local candidates = redis.call('zrange', registryKey, 0, over, 'WITHSCORES')
  for i = 1, #candidates, 2 do
    if #reply >= 1 + over * 2 then
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
 * KEYS[1] = seed state hash
 * ARGV[1] = token, ARGV[2] = lease ms
 */
const SEED_LEASE_SCRIPT = `
-- REPLAY_SEED_LEASE
local seedKey = KEYS[1]
if redis.call('hget', seedKey, 'seeded') == '1' then
  return 2
end
${NOW_MS}
local owner = redis.call('hget', seedKey, 'lease')
local untilMs = tonumber(redis.call('hget', seedKey, 'leaseUntil') or '')
if owner and owner ~= ARGV[1] and untilMs ~= nil and untilMs > nowMs then
  return 0
end
redis.call('hset', seedKey, 'lease', ARGV[1], 'leaseUntil', nowMs + tonumber(ARGV[2]))
return 1
`;

/**
 * Record one scanned batch: enroll its topics at score 0 (an already
 * registered topic keeps its score), store the node's next cursor and extend
 * the lease. Returns 0 when the lease is no longer this token's.
 *
 * KEYS[1] = seed state hash, KEYS[2] = registry
 * ARGV[1] = token, ARGV[2] = lease ms, ARGV[3] = node, ARGV[4] = next cursor
 * ('done' when the node is finished), then the topics
 */
const SEED_PROGRESS_SCRIPT = `
-- REPLAY_SEED_PROGRESS
local seedKey = KEYS[1]
local registryKey = KEYS[2]
if redis.call('hget', seedKey, 'lease') ~= ARGV[1] then
  return 0
end
${NOW_MS}
for i = 5, #ARGV do
  redis.call('zadd', registryKey, 'NX', 0, ARGV[i])
end
redis.call('hset', seedKey, 'cursor:' .. ARGV[3], ARGV[4])
redis.call('hset', seedKey, 'leaseUntil', nowMs + tonumber(ARGV[2]))
return 1
`;

/**
 * Finish seeding: mark the deployment seeded and drop the scan state. Returns
 * the registry size, or -1 when the lease is no longer this token's.
 *
 * KEYS[1] = seed state hash, KEYS[2] = registry
 * ARGV[1] = token
 */
const SEED_FINISH_SCRIPT = `
-- REPLAY_SEED_FINISH
local seedKey = KEYS[1]
if redis.call('hget', seedKey, 'lease') ~= ARGV[1] then
  return -1
end
redis.call('del', seedKey)
redis.call('hset', seedKey, 'seeded', '1')
return redis.call('zcard', KEYS[2])
`;

/**
 * Reset a topic's seq space under a new epoch: the buffer is dropped, the
 * counter restarts at 0 (kept, so the next publish continues under this epoch
 * instead of drawing another) and the epoch and use stamp are set. Returns the
 * epoch stored, which differs from the one it replaces.
 *
 * KEYS[1] = seq, KEYS[2] = buffer, KEYS[3] = epoch, KEYS[4] = use stamp
 * ARGV[1] = drawn epoch (1 .. 2^32 - 1), ARGV[2] = ttl seconds (0 = none)
 */
const CLEAR_TOPIC_SCRIPT = `
-- REPLAY_CLEAR_TOPIC
local ttl = tonumber(ARGV[2])
if ttl == nil or tonumber(ARGV[1]) == nil then
  return redis.error_reply('REPLAY_CLEAR_TOPIC: epoch/ttl must be numeric')
end
${NOW_MS}
${FRESH_EPOCH}
local epoch = freshEpoch(KEYS[3], ARGV[1])
redis.call('unlink', KEYS[2])
redis.call('set', KEYS[1], '0')
redis.call('set', KEYS[3], epoch)
redis.call('set', KEYS[4], nowMs)
if ttl > 0 then
  redis.call('expire', KEYS[1], ttl)
  redis.call('expire', KEYS[3], ttl)
  redis.call('expire', KEYS[4], ttl)
end
return tonumber(epoch)
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
	const seedKey = client.key('replay:seed:{replay-topics}');
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

	// An epoch for a new seq space: random, 1 .. 2^32 - 1 (0 is the baseline
	// that means "no seq space").
	function drawEpoch() {
		return (randomU32() % 4294967295) + 1;
	}

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
		if (Number(await evalCached(redis, SEED_LEASE_SCRIPT, 1, seedKey, token, SEED_LEASE_MS)) !== 1) return;
		const pattern = client.key('replay:*');
		const nodes = scanNodes(redis);
		for (let index = 0; index < nodes.length; index++) {
			const node = nodes[index];
			const name = nodeName(redis, node, index);
			let cursor = (await redis.hget(seedKey, 'cursor:' + name)) ?? '0';
			while (cursor !== 'done') {
				const [next, found] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				const topics = new Set();
				for (const key of found) {
					if (!key.endsWith('}')) continue;
					if (key.startsWith(seqPrefix)) topics.add(key.slice(seqPrefix.length, -1));
					else if (key.startsWith(epochPrefix)) topics.add(key.slice(epochPrefix.length, -1));
				}
				const nextCursor = String(next) === '0' ? 'done' : String(next);
				const kept = await evalCached(redis, SEED_PROGRESS_SCRIPT, 2, seedKey, registryKey,
					token, SEED_LEASE_MS, name, nextCursor, ...topics);
				if (Number(kept) !== 1) return;
				cursor = nextCursor;
			}
		}
		const enrolled = Number(await evalCached(redis, SEED_FINISH_SCRIPT, 2, seedKey, registryKey, token));
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
	 * Record `topic` as used and, when the registry is over `maxTopics`, evict
	 * the least recently used topics. Eviction failures do not fail the caller:
	 * the topic's own write still happens and the next creation retries.
	 *
	 * @param {string} topic
	 * @returns {Promise<void>}
	 */
	async function reserve(topic) {
		const reply = await withBreaker(b, () =>
			evalCached(redis, RESERVE_SCRIPT, 2, seedKey, registryKey, topic, maxTopics, EVICTION_BATCH));
		let candidates = reply.slice(1);
		if (Number(reply[0]) !== 1) {
			// Not seeded: give the scan a bounded head start so the existing
			// topics count toward the cap, then read the candidates again.
			await boundedWait(kickSeed(), SEED_WAIT_MS);
			candidates = await withBreaker(b, () =>
				evalCached(redis, CANDIDATES_SCRIPT, 1, registryKey, maxTopics, EVICTION_BATCH, topic));
		}
		if (candidates.length > 0) {
			try {
				await withBreaker(b, () => evict(candidates, topic));
			} catch {
				// Over the cap until the next creation evicts.
			}
		}
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
	 * Reset a topic's seq space under a new epoch.
	 *
	 * @param {string} topic
	 * @param {string} bufKey - the calling backend's buffer key
	 * @param {number} ttl
	 * @returns {Promise<number>} the new epoch
	 */
	async function clearTopic(topic, bufKey, ttl) {
		await reserve(topic);
		return Number(await withBreaker(b, () => evalCached(redis, CLEAR_TOPIC_SCRIPT, 4,
			keys.seq(topic), bufKey, keys.epoch(topic), keys.used(topic), drawEpoch(), ttl)));
	}

	return {
		keys, seedKey, registryKey, reserve, clearTopic, readStampDue, drawEpoch,
		touchCommand: () => evalCachedName(redis, TOUCH_SCRIPT)
	};
}
