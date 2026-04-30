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

/**
 * @typedef {Object} RedisReplayOptions
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds for replay keys (0 = no expiry)
 * @property {'replicated'} [durability] - Opt into per-publish replication signalling. After the write, runs `WAIT minReplicas replicationTimeoutMs`; throws `ReplicationTimeoutError` and skips the local broadcast when fewer than `minReplicas` replicas ack.
 * @property {number} [minReplicas=1] - Minimum replicas that must ack before publish is considered durable. Required when `durability: 'replicated'`.
 * @property {number} [replicationTimeoutMs=1000] - Per-publish replication timeout in milliseconds. `0` blocks indefinitely (Redis WAIT semantics).
 */

/**
 * Thrown by `publish()` when `durability: 'replicated'` is set and the
 * Redis WAIT command reports fewer replicas than `minReplicas` within
 * `replicationTimeoutMs`. The data is in the master; the local broadcast
 * was skipped to avoid committing live consumers to state that could be
 * lost if the master fails before replicas catch up.
 */
export class ReplicationTimeoutError extends Error {
	constructor(ack, minReplicas, timeoutMs) {
		super(
			`replication timed out: ${ack}/${minReplicas} replicas acked within ${timeoutMs}ms`
		);
		this.name = 'ReplicationTimeoutError';
		this.ack = ack;
		this.minReplicas = minReplicas;
		this.timeoutMs = timeoutMs;
	}
}

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
	if (options.size !== undefined) {
		if (typeof options.size !== 'number' || options.size < 1 || !Number.isInteger(options.size)) {
			throw new Error(`redis replay: size must be a positive integer, got ${options.size}`);
		}
	}
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 0 || !Number.isInteger(options.ttl)) {
			throw new Error(`redis replay: ttl must be a non-negative integer, got ${options.ttl}`);
		}
	}

	const replicated = options.durability === 'replicated';
	if (options.durability !== undefined && options.durability !== 'replicated') {
		throw new Error(`redis replay: durability must be 'replicated' or undefined, got ${options.durability}`);
	}
	let minReplicas = 1;
	let replicationTimeoutMs = 1000;
	if (replicated) {
		if (options.minReplicas !== undefined) {
			if (typeof options.minReplicas !== 'number' || !Number.isInteger(options.minReplicas) || options.minReplicas < 1) {
				throw new Error(`redis replay: minReplicas must be a positive integer, got ${options.minReplicas}`);
			}
			minReplicas = options.minReplicas;
		}
		if (options.replicationTimeoutMs !== undefined) {
			if (typeof options.replicationTimeoutMs !== 'number' || !Number.isInteger(options.replicationTimeoutMs) || options.replicationTimeoutMs < 0) {
				throw new Error(`redis replay: replicationTimeoutMs must be a non-negative integer, got ${options.replicationTimeoutMs}`);
			}
			replicationTimeoutMs = options.replicationTimeoutMs;
		}
	}

	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
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

			b?.guard();
			try {
				await redis.eval(
					PUBLISH_SCRIPT, 2, sk, bk,
					topic, event, JSON.stringify(data ?? null), maxSize, ttl
				);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			mPublishes?.inc({ topic: mt(topic) });

			if (replicated) {
				let ack;
				try {
					ack = await redis.wait(minReplicas, replicationTimeoutMs);
				} catch (err) {
					// WAIT itself failed (network/protocol). Treat as a
					// real backend failure for breaker accounting.
					b?.failure(err);
					throw err;
				}
				if (ack < minReplicas) {
					mReplicationTimeouts?.inc();
					throw new ReplicationTimeoutError(ack, minReplicas, replicationTimeoutMs);
				}
				mReplications?.inc();
			}

			return platform.publish(topic, event, data);
		},

		async seq(topic) {
			if (b) b.guard();
			try {
				const val = await redis.get(seqKey(topic));
				b?.success();
				return val ? parseInt(val, 10) : 0;
			} catch (err) {
				b?.failure(err);
				throw err;
			}
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
			if (b) b.guard();
			let raw;
			try {
				raw = await redis.zrangebyscore(bufKey(topic), since + 1, '+inf');
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
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

			let rawAll;
			try {
				rawAll = await redis.zrangebyscore(bufKey(topic), '-inf', '+inf');
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			let oldestSeq = null;
			for (let i = 0; i < rawAll.length; i++) {
				try {
					const parsed = JSON.parse(rawAll[i]);
					if (parsed.seq != null) {
						oldestSeq = parsed.seq;
						break;
					}
				} catch { /* skip corrupt */ }
			}
			if (oldestSeq !== null && sinceSeq > 0 && oldestSeq > sinceSeq + 1) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}

			const missed = [];
			for (let i = 0; i < rawAll.length; i++) {
				try {
					const parsed = JSON.parse(rawAll[i]);
					if (parsed.seq > sinceSeq) missed.push(parsed);
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
			if (b) b.guard();
			try {
				const pattern = client.key('replay:*');
				let cursor = '0';
				do {
					const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
					cursor = nextCursor;
					if (keys.length > 0) {
						await redis.unlink(...keys);
					}
				} while (cursor !== '0');
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async clearTopic(topic) {
			if (b) b.guard();
			try {
				await redis.unlink(seqKey(topic), bufKey(topic));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		}
	};
}
