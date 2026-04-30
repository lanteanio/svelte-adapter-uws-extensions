/**
 * Redis Streams-backed replay buffer for svelte-adapter-uws.
 *
 * Same external contract as the sorted-set replay (`createReplay`)
 * but stores entries in a Redis Stream (`XADD`/`XRANGE`) instead of
 * a sorted set (`ZADD`/`ZRANGEBYSCORE`). Listpack encoding is more
 * compact than sorted-set encoding for the typical message shape,
 * and `XRANGE` against `<seq>-0` IDs lets queries filter natively
 * by sequence number with no app-side scan.
 *
 * Stream IDs are `<seq>-0` where seq is the same INCR counter the
 * sorted-set backend uses. Both backends can coexist on the same
 * Redis (different buf-key prefix) but a single topic should pick
 * one backend and stay there.
 *
 * Requires Redis 7+ for the listpack encoding wins; works on Redis
 * 5+ functionally.
 *
 * @module svelte-adapter-uws-extensions/redis/replay-stream
 */

import { ReplicationTimeoutError } from './replay.js';

/**
 * Lua script for atomic idempotent publish.
 *
 * KEYS[1] = idmp cache key (hash; field = requestId, value = seq)
 * KEYS[2] = seq key
 * KEYS[3] = stream key
 * ARGV[1] = requestId
 * ARGV[2] = maxSize
 * ARGV[3] = ttl seconds (0 = no expiry; applies to seq + stream keys)
 * ARGV[4] = idmpTtl seconds (0 = no expiry on the dedup cache)
 * ARGV[5] = topic
 * ARGV[6] = event
 * ARGV[7] = data (JSON-encoded)
 *
 * Returns {isDuplicate (1|0), seq}.
 */
const IDMP_PUBLISH_SCRIPT = `
local idmpKey = KEYS[1]
local seqKey = KEYS[2]
local bufKey = KEYS[3]
local requestId = ARGV[1]
local maxSize = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local idmpTtl = tonumber(ARGV[4])
local topic = ARGV[5]
local event = ARGV[6]
local data = ARGV[7]

local cached = redis.call('hget', idmpKey, requestId)
if cached then
  return {1, tonumber(cached)}
end

local seq = redis.call('incr', seqKey)
local id = seq .. '-0'
redis.call('xadd', bufKey, 'MAXLEN', '~', maxSize, id, 'topic', topic, 'event', event, 'data', data)

redis.call('hset', idmpKey, requestId, seq)
if idmpTtl > 0 then
  redis.call('expire', idmpKey, idmpTtl)
end

if ttl > 0 then
  redis.call('expire', seqKey, ttl)
  redis.call('expire', bufKey, ttl)
end

return {0, seq}
`;

/**
 * Lua script for atomic stream publish: increment seq counter, XADD
 * the entry with id `<seq>-0`, optionally apply TTL.
 *
 * KEYS[1] = seq key
 * KEYS[2] = stream key
 * ARGV[1] = maxSize (XADD MAXLEN ~)
 * ARGV[2] = ttl seconds (0 = no expiry)
 * ARGV[3] = topic
 * ARGV[4] = event
 * ARGV[5] = data (JSON-encoded)
 *
 * Returns the new sequence number.
 */
const PUBLISH_SCRIPT = `
local seqKey = KEYS[1]
local bufKey = KEYS[2]
local maxSize = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local topic = ARGV[3]
local event = ARGV[4]
local data = ARGV[5]

local seq = redis.call('incr', seqKey)
local id = seq .. '-0'
redis.call('xadd', bufKey, 'MAXLEN', '~', maxSize, id, 'topic', topic, 'event', event, 'data', data)

if ttl > 0 then
  redis.call('expire', seqKey, ttl)
  redis.call('expire', bufKey, ttl)
end

return seq
`;

function fieldsToObject(arr) {
	const obj = {};
	for (let i = 0; i < arr.length; i += 2) obj[arr[i]] = arr[i + 1];
	return obj;
}

function seqFromId(id) {
	const dash = id.indexOf('-');
	return dash === -1 ? parseInt(id, 10) : parseInt(id.slice(0, dash), 10);
}

/**
 * Create a Streams-backed Redis replay buffer.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {import('./replay.js').RedisReplayOptions} [options]
 * @returns {import('./replay.js').RedisReplayBuffer}
 */
export function createStreamReplay(client, options = {}) {
	if (options.size !== undefined) {
		if (typeof options.size !== 'number' || options.size < 1 || !Number.isInteger(options.size)) {
			throw new Error(`redis stream replay: size must be a positive integer, got ${options.size}`);
		}
	}
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 0 || !Number.isInteger(options.ttl)) {
			throw new Error(`redis stream replay: ttl must be a non-negative integer, got ${options.ttl}`);
		}
	}

	const replicated = options.durability === 'replicated';
	if (options.durability !== undefined && options.durability !== 'replicated') {
		throw new Error(`redis stream replay: durability must be 'replicated' or undefined, got ${options.durability}`);
	}
	let minReplicas = 1;
	let replicationTimeoutMs = 1000;
	if (replicated) {
		if (options.minReplicas !== undefined) {
			if (typeof options.minReplicas !== 'number' || !Number.isInteger(options.minReplicas) || options.minReplicas < 1) {
				throw new Error(`redis stream replay: minReplicas must be a positive integer, got ${options.minReplicas}`);
			}
			minReplicas = options.minReplicas;
		}
		if (options.replicationTimeoutMs !== undefined) {
			if (typeof options.replicationTimeoutMs !== 'number' || !Number.isInteger(options.replicationTimeoutMs) || options.replicationTimeoutMs < 0) {
				throw new Error(`redis stream replay: replicationTimeoutMs must be a non-negative integer, got ${options.replicationTimeoutMs}`);
			}
			replicationTimeoutMs = options.replicationTimeoutMs;
		}
	}

	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	const defaultIdempotencyTtl = options.idempotencyTtl !== undefined
		? options.idempotencyTtl
		: 48 * 60 * 60;
	if (typeof defaultIdempotencyTtl !== 'number' || !Number.isInteger(defaultIdempotencyTtl) || defaultIdempotencyTtl < 0) {
		throw new Error(`redis stream replay: idempotencyTtl must be a non-negative integer, got ${defaultIdempotencyTtl}`);
	}
	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mReplications = replicated ? m?.counter('replay_replications_total', 'Publishes confirmed replicated within timeout') : null;
	const mReplicationTimeouts = replicated ? m?.counter('replay_replication_timeouts_total', 'Publishes that did not reach minReplicas within timeout') : null;
	const mIdmpHits = m?.counter('replay_idmp_hits_total', 'publishIdempotent calls served from the dedup cache (no XADD)', ['topic']);
	const mIdmpWrites = m?.counter('replay_idmp_writes_total', 'publishIdempotent calls that produced a new entry', ['topic']);

	function idmpKey(producerId, topic) {
		return client.key('replay:idmp:' + producerId + ':' + topic);
	}

	function seqKey(topic) {
		return client.key('replay:seq:' + topic);
	}

	function bufKey(topic) {
		return client.key('replay:streambuf:' + topic);
	}

	return {
		async publishIdempotent(platform, topic, event, data, opts) {
			if (!opts || typeof opts !== 'object') {
				throw new Error('redis stream replay: publishIdempotent requires { producerId, requestId } options');
			}
			const { producerId, requestId } = opts;
			if (typeof producerId !== 'string' || producerId.length === 0) {
				throw new Error('redis stream replay: producerId must be a non-empty string');
			}
			if (typeof requestId !== 'string' || requestId.length === 0) {
				throw new Error('redis stream replay: requestId must be a non-empty string');
			}
			const idmpTtl = opts.idempotencyTtl !== undefined ? opts.idempotencyTtl : defaultIdempotencyTtl;
			if (typeof idmpTtl !== 'number' || !Number.isInteger(idmpTtl) || idmpTtl < 0) {
				throw new Error(`redis stream replay: idempotencyTtl must be a non-negative integer, got ${idmpTtl}`);
			}

			const ik = idmpKey(producerId, topic);
			const sk = seqKey(topic);
			const bk = bufKey(topic);

			b?.guard();
			let result;
			try {
				result = await redis.eval(
					IDMP_PUBLISH_SCRIPT, 3, ik, sk, bk,
					requestId, maxSize, ttl, idmpTtl, topic, event, JSON.stringify(data ?? null)
				);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			const isDuplicate = Number(result[0]) === 1;
			const seq = Number(result[1]);

			if (isDuplicate) {
				mIdmpHits?.inc({ topic: mt(topic) });
				return { seq, isDuplicate: true };
			}

			mIdmpWrites?.inc({ topic: mt(topic) });
			mPublishes?.inc({ topic: mt(topic) });

			if (replicated) {
				let ack;
				try {
					ack = await redis.wait(minReplicas, replicationTimeoutMs);
				} catch (err) {
					b?.failure(err);
					throw err;
				}
				if (ack < minReplicas) {
					mReplicationTimeouts?.inc();
					throw new ReplicationTimeoutError(ack, minReplicas, replicationTimeoutMs);
				}
				mReplications?.inc();
			}

			await platform.publish(topic, event, data);
			return { seq, isDuplicate: false };
		},

		async publish(platform, topic, event, data) {
			const sk = seqKey(topic);
			const bk = bufKey(topic);

			b?.guard();
			try {
				await redis.eval(
					PUBLISH_SCRIPT, 2, sk, bk,
					maxSize, ttl, topic, event, JSON.stringify(data ?? null)
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
				throw new Error(`redis stream replay: lastSeenSeq must be a non-negative integer, got ${lastSeenSeq}`);
			}
			if (lastSeenSeq === 0) return { truncated: false, missingFrom: null };

			const target = lastSeenSeq + 1;
			if (b) b.guard();

			let entries;
			try {
				entries = await redis.xrange(bufKey(topic), `${target}-0`, '+', 'COUNT', 1);
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			if (entries.length > 0) {
				const seq = seqFromId(entries[0][0]);
				b?.success();
				if (seq > target) {
					return { truncated: true, missingFrom: target };
				}
				return { truncated: false, missingFrom: null };
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
			let entries;
			try {
				const startId = since >= 0 ? `(${since}-0` : '-';
				entries = await redis.xrange(bufKey(topic), startId, '+');
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const result = [];
			for (const [id, flat] of entries) {
				const fields = fieldsToObject(flat);
				try {
					result.push({
						seq: seqFromId(id),
						topic: fields.topic,
						event: fields.event,
						data: JSON.parse(fields.data)
					});
				} catch { /* skip corrupted */ }
			}
			return result;
		},

		async replay(ws, topic, sinceSeq, platform, reqId) {
			if (b) b.guard();
			const replayTopic = '__replay:' + topic;

			let entries;
			try {
				const startId = sinceSeq >= 0 ? `(${sinceSeq}-0` : '-';
				entries = await redis.xrange(bufKey(topic), startId, '+');
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			const missed = [];
			for (const [id, flat] of entries) {
				const fields = fieldsToObject(flat);
				try {
					missed.push({
						seq: seqFromId(id),
						event: fields.event,
						data: JSON.parse(fields.data)
					});
				} catch { /* skip corrupted */ }
			}

			let truncated = false;
			if (sinceSeq > 0) {
				if (missed.length > 0 && missed[0].seq > sinceSeq + 1) {
					truncated = true;
				} else if (missed.length === 0) {
					try {
						const val = await redis.get(seqKey(topic));
						const currentSeq = val ? parseInt(val, 10) : 0;
						if (currentSeq > sinceSeq) truncated = true;
					} catch (err) {
						b?.failure(err);
						throw err;
					}
				}
			}
			b?.success();

			if (truncated) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}

			for (const msg of missed) {
				platform.send(ws, replayTopic, 'msg', { seq: msg.seq, event: msg.event, data: msg.data });
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
					if (keys.length > 0) await redis.unlink(...keys);
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
