/**
 * Redis-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in Redis
 * sorted sets so they survive restarts and are shared across instances.
 *
 * Pass `storage: 'stream'` to dispatch to the Redis Streams backend
 * (XADD/XRANGE) instead. Both backends share the seq counter shape
 * but use different buf-key prefixes so they can coexist.
 *
 * Storage layout per topic (default sorted-set backend):
 *   - Key `{prefix}replay:seq:{topic}` - INCR counter for sequence numbers
 *   - Key `{prefix}replay:buf:{topic}` - sorted set (score = seq, member = JSON payload)
 *
 * @module svelte-adapter-uws-extensions/redis/replay
 */

import { createStreamReplay } from './replay-stream.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { ReplicationTimeoutError, parseReplayOptions, awaitReplication } from '../shared/replay-helpers.js';
import { withBreaker } from '../shared/breaker.js';
export { ReplicationTimeoutError };

/**
 * @typedef {Object} RedisReplayOptions
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds for replay keys (0 = no expiry)
 * @property {'replicated'} [durability] - Opt into per-publish replication signalling. After the write, runs `WAIT minReplicas replicationTimeoutMs`; throws `ReplicationTimeoutError` and skips the local broadcast when fewer than `minReplicas` replicas ack.
 * @property {number} [minReplicas=1] - Minimum replicas that must ack before publish is considered durable. Required when `durability: 'replicated'`.
 * @property {number} [replicationTimeoutMs=1000] - Per-publish replication timeout in milliseconds. `0` blocks indefinitely (Redis WAIT semantics).
 */

/**
 * @typedef {Object} RedisReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
 * @property {(topic: string, lastSeenSeq: number) => Promise<{truncated: boolean, missingFrom: number | null}>} gap
 * @property {(topic: string, since: number) => Promise<Array<{seq: number, topic: string, event: string, data: unknown}>>} since
 * @property {(ws: any, topic: string, sinceSeq: number, platform: import('svelte-adapter-uws').Platform) => Promise<void>} replay
 * @property {() => Promise<void>} clear
 * @property {(topic: string) => Promise<void>} clearTopic
 */

/**
 * Lua script for atomic publish: increment seq, store message, trim buffer.
 *
 * KEYS[1] = seq key
 * KEYS[2] = buf key (sorted set)
 * ARGV[1] = topic
 * ARGV[2] = event
 * ARGV[3] = data (JSON-encoded)
 * ARGV[4] = maxSize
 * ARGV[5] = ttl (seconds, 0 = no expiry)
 *
 * Returns the new sequence number.
 */
const PUBLISH_SCRIPT = `
local seqKey = KEYS[1]
local bufKey = KEYS[2]
local topic = ARGV[1]
local event = ARGV[2]
local data = ARGV[3]
local maxSize = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local seq = redis.call('incr', seqKey)
local envelope = cjson.encode({seq = seq, topic = topic, event = event})
local payload = string.sub(envelope, 1, -2) .. ',"data":' .. data .. '}'
redis.call('zadd', bufKey, seq, payload)

local count = redis.call('zcard', bufKey)
if count > maxSize then
  redis.call('zremrangebyrank', bufKey, 0, count - maxSize - 1)
end

if ttl > 0 then
  redis.call('expire', seqKey, ttl)
  redis.call('expire', bufKey, ttl)
end

return seq
`;

/**
 * Create a Redis-backed replay buffer.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisReplayOptions} [options]
 * @returns {RedisReplayBuffer}
 */
export function createReplay(client, options = {}) {
	if (options.storage !== undefined && options.storage !== 'sortedset' && options.storage !== 'stream') {
		throw new Error(`redis replay: storage must be 'sortedset' or 'stream', got ${options.storage}`);
	}
	if (options.storage === 'stream') {
		return createStreamReplay(client, options);
	}

	const { maxSize, ttl, replicated, minReplicas, replicationTimeoutMs } =
		parseReplayOptions('redis replay', options);

	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mReplications = replicated ? m?.counter('replay_replications_total', 'Publishes confirmed replicated within timeout') : null;
	const mReplicationTimeouts = replicated ? m?.counter('replay_replication_timeouts_total', 'Publishes that did not reach minReplicas within timeout') : null;

	function seqKey(topic) {
		return client.key('replay:seq:' + topic);
	}

	function bufKey(topic) {
		return client.key('replay:buf:' + topic);
	}

	return {
		async publish(platform, topic, event, data) {
			const sk = seqKey(topic);
			const bk = bufKey(topic);

			await withBreaker(b, () =>
				redis.eval(PUBLISH_SCRIPT, 2, sk, bk, topic, event, JSON.stringify(data ?? null), maxSize, ttl)
			);
			mPublishes?.inc({ topic: mt(topic) });

			if (replicated) {
				await awaitReplication(redis, minReplicas, replicationTimeoutMs, b, mReplications, mReplicationTimeouts);
			}

			return platform.publish(topic, event, data);
		},

		async seq(topic) {
			const val = await withBreaker(b, () => redis.get(seqKey(topic)));
			return val ? parseInt(val, 10) : 0;
		},

		async gap(topic, lastSeenSeq) {
			if (!Number.isInteger(lastSeenSeq) || lastSeenSeq < 0) {
				throw new Error(`redis replay: lastSeenSeq must be a non-negative integer, got ${lastSeenSeq}`);
			}
			if (lastSeenSeq === 0) return { truncated: false, missingFrom: null };

			const target = lastSeenSeq + 1;
			if (b) b.guard();

			let raw;
			try {
				raw = await redis.zrangebyscore(bufKey(topic), target, '+inf');
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			for (let i = 0; i < raw.length; i++) {
				try {
					const parsed = JSON.parse(raw[i]);
					if (parsed.seq != null) {
						b?.success();
						if (parsed.seq > target) {
							return { truncated: true, missingFrom: target };
						}
						return { truncated: false, missingFrom: null };
					}
				} catch { /* skip corrupt */ }
			}

			let val;
			try {
				val = await redis.get(seqKey(topic));
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			b?.success();
			const currentSeq = val ? parseInt(val, 10) : 0;
			if (currentSeq > lastSeenSeq) {
				return { truncated: true, missingFrom: target };
			}
			return { truncated: false, missingFrom: null };
		},

		async since(topic, since) {
			const raw = await withBreaker(b, () => redis.zrangebyscore(bufKey(topic), since + 1, '+inf'));
			const result = [];
			for (let i = 0; i < raw.length; i++) {
				try {
					result.push(JSON.parse(raw[i]));
				} catch { /* skip corrupted entry */ }
			}
			return result;
		},

		async replay(ws, topic, sinceSeq, platform, reqId) {
			if (b) b.guard();
			const replayTopic = '__replay:' + topic;
			const bk = bufKey(topic);

			// Pipeline the oldest-entry probe (for truncation detection)
			// alongside the seq>sinceSeq fetch so this is one RTT and the
			// returned data covers only what the client actually needs.
			// Fetch a small slice for the oldest probe so corrupt entries
			// at the head don't hide the actual oldest valid seq.
			let oldestRaw, missedRaw;
			try {
				const pipe = redis.pipeline();
				pipe.zrange(bk, 0, 9);
				pipe.zrangebyscore(bk, sinceSeq + 1, '+inf');
				const results = await pipe.exec();
				if (results[0][0]) throw results[0][0];
				if (results[1][0]) throw results[1][0];
				oldestRaw = results[0][1];
				missedRaw = results[1][1];
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			let oldestSeq = null;
			if (oldestRaw) {
				for (let i = 0; i < oldestRaw.length; i++) {
					try {
						const parsed = JSON.parse(oldestRaw[i]);
						if (parsed.seq != null) {
							oldestSeq = parsed.seq;
							break;
						}
					} catch { /* skip corrupt */ }
				}
			}
			if (oldestSeq !== null && sinceSeq > 0 && oldestSeq > sinceSeq + 1) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}

			const missed = [];
			for (let i = 0; i < missedRaw.length; i++) {
				try {
					missed.push(JSON.parse(missedRaw[i]));
				} catch { /* skip corrupted */ }
			}

			if (oldestSeq === null && sinceSeq > 0 && missed.length === 0) {
				try {
					const val = await redis.get(seqKey(topic));
					const currentSeq = val ? parseInt(val, 10) : 0;
					if (currentSeq > sinceSeq) {
						mTruncations?.inc({ topic: mt(topic) });
						platform.send(ws, replayTopic, 'truncated', null);
					}
				} catch (err) {
					b?.failure(err);
					throw err;
				}
			}
			b?.success();

			for (let i = 0; i < missed.length; i++) {
				const msg = missed[i];
				platform.send(ws, replayTopic, 'msg', {
					seq: msg.seq,
					event: msg.event,
					data: msg.data
				});
			}
			if (missed.length > 0) mReplayed?.inc({ topic: mt(topic) }, missed.length);
			platform.send(ws, replayTopic, 'end', { reqId: reqId || undefined });
		},

		async clear() {
			await withBreaker(b, () => scanAndUnlink(redis, client.key('replay:*')));
		},

		async clearTopic(topic) {
			await withBreaker(b, () => redis.unlink(seqKey(topic), bufKey(topic)));
		}
	};
}
