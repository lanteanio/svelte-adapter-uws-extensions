/**
 * Redis-backed replay buffer for svelte-adapter-uws.
 *
 * Same API as the core createReplay plugin, but stores messages in Redis
 * sorted sets so they survive restarts and are shared across instances.
 *
 * Storage layout per topic:
 *   - Key `{prefix}replay:seq:{topic}` - INCR counter for sequence numbers
 *   - Key `{prefix}replay:buf:{topic}` - sorted set (score = seq, member = JSON payload)
 *
 * @module svelte-adapter-uws-extensions/redis/replay
 */

/**
 * @typedef {Object} RedisReplayOptions
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds for replay keys (0 = no expiry)
 */

/**
 * @typedef {Object} RedisReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
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

	const maxSize = options.size || 1000;
	const ttl = options.ttl || 0;
	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);

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
				b?.success();
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
