/**
 * Topic bookkeeping shared by the sorted-set and stream Redis replay
 * backends: the generation counter that hands out per-topic epochs, and the
 * `maxTopics` registry that bounds how many topics keep replay state.
 *
 * Generations. Every epoch a topic is ever given comes from one counter
 * (`replay:generation:{replay-topics}`), so a value is never handed out twice.
 * That is what lets a topic's per-topic keys be deleted - by eviction, by
 * `ttl` expiry or by `clear()` - without a recreated topic coming back under
 * an epoch a client still holds for its predecessor. Epochs written before the
 * counter existed are raised into it once per deployment: the first
 * reservation that finds the counter missing scans the replay keys, sets the
 * counter to the highest stored epoch and enrolls every existing topic.
 *
 * Registry. `replay:topics:{replay-topics}` is a sorted set of topics scored
 * by last use. It is written only when a topic is created or cleared and when
 * eviction settles, never by a steady publish or by a read, so it adds no
 * traffic to the hot path and no shared slot to it. A steady publish stamps
 * `replay:used:{topic}` in its own slot instead; when the registry is over
 * `maxTopics`, its lowest-scored entries are checked against that stamp, and
 * an entry that was used since it was scored is moved up rather than evicted.
 * An entry is evicted only when its stamp is no newer than its score, which
 * makes it the least recently published topic.
 *
 * @module svelte-adapter-uws-extensions/redis/_replay-topics
 */

import { evalCached, evalCachedName } from '../shared/eval-cached.js';
import { execMultiSlot, firstCommandError } from '../shared/cluster.js';
import { scanEach } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { now } from '../shared/runtime.js';

/** Registry entries checked per eviction round. */
const EVICTION_BATCH = 16;

/**
 * Eviction rounds per reservation. A round ends early when every candidate
 * was evicted; candidates used since they were scored are moved up and the
 * next lowest are checked.
 */
const EVICTION_ROUNDS = 4;

/**
 * Reserve a generation and record the topic as used now. Returns `{0}` when
 * the counter is missing (not seeded yet), else `{generation, topic1, score1,
 * ...}` with the lowest-scored registry entries when the registry holds more
 * than `maxTopics` topics.
 *
 * KEYS[1] = generation counter, KEYS[2] = registry
 * ARGV[1] = topic, ARGV[2] = now (ms), ARGV[3] = maxTopics, ARGV[4] = batch
 */
const RESERVE_SCRIPT = `
-- REPLAY_TOPIC_RESERVE
local generationKey = KEYS[1]
local registryKey = KEYS[2]
local topic = ARGV[1]
local usedAt = tonumber(ARGV[2])
local maxTopics = tonumber(ARGV[3])
local batch = tonumber(ARGV[4])
if usedAt == nil or maxTopics == nil or batch == nil then
  return redis.error_reply('REPLAY_TOPIC_RESERVE: now/maxTopics/batch must be numeric')
end
if redis.call('exists', generationKey) == 0 then
  return {0}
end
local generation = redis.call('incr', generationKey)
redis.call('zadd', registryKey, usedAt, topic)
local over = redis.call('zcard', registryKey) - maxTopics
local reply = {generation}
if over > 0 then
  if over > batch then
    over = batch
  end
  local candidates = redis.call('zrange', registryKey, 0, over - 1, 'WITHSCORES')
  for i = 1, #candidates do
    reply[i + 1] = candidates[i]
  end
end
return reply
`;

/**
 * The lowest-scored registry entries while the registry is over the cap.
 *
 * KEYS[1] = registry
 * ARGV[1] = maxTopics, ARGV[2] = batch
 */
const CANDIDATES_SCRIPT = `
-- REPLAY_TOPIC_CANDIDATES
local registryKey = KEYS[1]
local maxTopics = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
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
return redis.call('zrange', registryKey, 0, over - 1, 'WITHSCORES')
`;

/**
 * Evict one topic unless it was published since its registry score. Runs in
 * the topic's own slot, so the check and the delete are atomic against a
 * publish. Returns 0 when the keys were deleted, else the newer use stamp.
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
 * Enroll topics that existed before the registry, at score 0 so they are the
 * first eviction candidates. An already registered topic keeps its score.
 *
 * KEYS[1] = registry
 * ARGV = topics
 */
const ENROLL_SCRIPT = `
-- REPLAY_TOPIC_ENROLL
local registryKey = KEYS[1]
for i = 1, #ARGV do
  redis.call('zadd', registryKey, 'NX', 0, ARGV[i])
end
return #ARGV
`;

/**
 * Create the generation counter at no less than the highest stored epoch.
 * Never lowers an existing counter.
 *
 * KEYS[1] = generation counter
 * ARGV[1] = highest stored epoch
 */
const SEED_SCRIPT = `
-- REPLAY_GENERATION_SEED
local generationKey = KEYS[1]
local floor = tonumber(ARGV[1])
if floor == nil then
  return redis.error_reply('REPLAY_GENERATION_SEED: floor must be numeric')
end
local current = tonumber(redis.call('get', generationKey) or '-1')
if current == nil then
  return redis.error_reply('REPLAY_GENERATION_SEED: counter is not an integer')
end
if current < floor then
  redis.call('set', generationKey, ARGV[1])
end
return redis.call('get', generationKey)
`;

/**
 * Reset a topic's seq space under a new generation: the buffer is dropped,
 * the counter restarts at 0 (kept, so the next publish continues under this
 * generation instead of taking another) and the epoch and use stamp are set.
 *
 * KEYS[1] = seq, KEYS[2] = buffer, KEYS[3] = epoch, KEYS[4] = use stamp
 * ARGV[1] = generation, ARGV[2] = now (ms), ARGV[3] = ttl seconds (0 = none)
 */
const CLEAR_TOPIC_SCRIPT = `
-- REPLAY_CLEAR_TOPIC
local ttl = tonumber(ARGV[3])
if ttl == nil or tonumber(ARGV[1]) == nil then
  return redis.error_reply('REPLAY_CLEAR_TOPIC: generation/ttl must be numeric')
end
redis.call('unlink', KEYS[2])
redis.call('set', KEYS[1], '0')
redis.call('set', KEYS[3], ARGV[1])
redis.call('set', KEYS[4], ARGV[2])
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

/**
 * @param {import('./index.js').RedisClient} client
 * @param {{ maxTopics: number, breaker?: import('../shared/breaker.js').CircuitBreaker, onEvicted?: (topic: string) => void }} options
 */
export function createReplayTopics(client, { maxTopics, breaker: b, onEvicted }) {
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

	// One seeding pass per process at a time; concurrent reservations that
	// find the counter missing wait on the same pass.
	let seeding = null;
	function ensureSeeded() {
		if (!seeding) seeding = withBreaker(b, seed).finally(() => { seeding = null; });
		return seeding;
	}

	async function seed() {
		let floor = 0;
		await scanEach(redis, client.key('replay:*'), async (found) => {
			const topics = new Set();
			const epochKeys = [];
			for (const key of found) {
				if (!key.endsWith('}')) continue;
				if (key.startsWith(seqPrefix)) {
					topics.add(key.slice(seqPrefix.length, -1));
				} else if (key.startsWith(epochPrefix)) {
					topics.add(key.slice(epochPrefix.length, -1));
					epochKeys.push(key);
				}
			}
			if (epochKeys.length > 0) {
				const results = await execMultiSlot(redis, epochKeys.map((key) => ['get', key]));
				const error = firstCommandError(results);
				if (error) throw error;
				for (const [, value] of results) {
					const epoch = parseInt(value, 10);
					if (Number.isSafeInteger(epoch) && epoch > floor) floor = epoch;
				}
			}
			if (topics.size > 0) await evalCached(redis, ENROLL_SCRIPT, 1, registryKey, ...topics);
		});
		// Written last: the counter's existence is what marks the deployment
		// seeded, so an interrupted pass is simply run again.
		await evalCached(redis, SEED_SCRIPT, 1, generationKey, floor);
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
			flat = await evalCached(redis, CANDIDATES_SCRIPT, 1, registryKey, maxTopics, EVICTION_BATCH);
		}
	}

	/**
	 * Take a fresh generation for `topic`, record the topic as used and, when
	 * the registry is over `maxTopics`, evict the least recently published
	 * topics. Eviction failures do not fail the caller: the topic's own write
	 * still happens and the next creation retries the eviction.
	 *
	 * @param {string} topic
	 * @returns {Promise<number>}
	 */
	async function reserve(topic) {
		for (let attempt = 0; attempt < 2; attempt++) {
			const reply = await withBreaker(b, () =>
				evalCached(redis, RESERVE_SCRIPT, 2, generationKey, registryKey, topic, now(), maxTopics, EVICTION_BATCH));
			const generation = Number(reply[0]);
			if (generation > 0) {
				if (reply.length > 1) {
					try {
						await withBreaker(b, () => evict(reply.slice(1), topic));
					} catch {
						// Over the cap until the next creation evicts; the reserved
						// generation is still valid.
					}
				}
				return generation;
			}
			await ensureSeeded();
		}
		throw new Error('redis replay: the topic generation counter could not be seeded');
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
			keys.seq(topic), bufKey, keys.epoch(topic), keys.used(topic), generation, now(), ttl));
		return generation;
	}

	return { keys, generationKey, registryKey, reserve, clearTopic };
}
