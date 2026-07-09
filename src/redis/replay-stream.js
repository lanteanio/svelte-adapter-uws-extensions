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

import { scanAndUnlink, scanKeys } from '../shared/redis-scan.js';
import { evalCached } from '../shared/eval-cached.js';
import { parseReplayOptions, awaitReplicationGrouped, ReplayStorageError, ReplaySerializationError, createResumeHook } from '../shared/replay-helpers.js';
import { execMultiSlot } from '../shared/cluster.js';
import { withBreaker } from '../shared/breaker.js';
import { checkReplayAccess } from '../shared/replay-gate.js';

/**
 * Lua script for atomic idempotent publish.
 *
 * KEYS[1] = idmp cache key (hash; field = requestId, value = seq)
 * KEYS[2] = seq key
 * KEYS[3] = stream key
 * KEYS[4] = epoch key
 * ARGV[1] = requestId
 * ARGV[2] = maxSize
 * ARGV[3] = ttl seconds (0 = no expiry; applies to seq + stream keys)
 * ARGV[4] = idmpTtl seconds (0 = no expiry on the dedup cache)
 * ARGV[5] = event
 * ARGV[6] = data (JSON-encoded)
 *
 * Returns {isDuplicate (1|0), seq}.
 *
 * The entry stores only the `event` and `data` fields. The topic is NOT
 * stored: it is already encoded in the per-topic stream key, so writing it
 * into every entry is a redundant per-entry value (Redis stream SAMEFIELDS
 * dedups the field NAMES but never the VALUES). The read path recovers the
 * topic from the key. Legacy entries written before this change still carry
 * a `topic` field; the reader falls back to it when present.
 *
 * The `seq == 1` reset edge bumps the epoch in the same atomic script (see
 * PUBLISH_SCRIPT below for the rationale). The epoch key is given NO ttl, so
 * it survives the seq key being reaped and the bump sticks across the reset.
 */
const IDMP_PUBLISH_SCRIPT = `
local idmpKey = KEYS[1]
local seqKey = KEYS[2]
local bufKey = KEYS[3]
local epochKey = KEYS[4]
local requestId = ARGV[1]
local maxSize = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local idmpTtl = tonumber(ARGV[4])
local event = ARGV[5]
local data = ARGV[6]
if maxSize == nil or ttl == nil or idmpTtl == nil then
  return redis.error_reply('REPLAY_IDMP_PUBLISH: maxSize/ttl/idmpTtl must be numeric')
end

local cached = redis.call('hget', idmpKey, requestId)
if cached then
  return {1, tonumber(cached)}
end

local seq = redis.call('incr', seqKey)
if seq == 1 then
  redis.call('incr', epochKey)
end
local id = seq .. '-0'
redis.call('xadd', bufKey, 'MAXLEN', '~', maxSize, id, 'event', event, 'data', data)

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
 * KEYS[3] = epoch key
 * ARGV[1] = maxSize (XADD MAXLEN ~)
 * ARGV[2] = ttl seconds (0 = no expiry)
 * ARGV[3] = event
 * ARGV[4] = data (JSON-encoded)
 *
 * Returns the new sequence number.
 *
 * The entry stores only `event` and `data`; the topic lives in the per-topic
 * stream key, so storing it per entry is a redundant value the reader recovers
 * from the key. Legacy entries still carry a `topic` field; the reader falls
 * back to it when present.
 *
 * When the seq counter reads 1 the seq space is fresh (brand-new topic or one
 * whose seq key was reaped by TTL), so the numbering restarted: bump the epoch
 * in the same atomic script. A resuming client holding a pre-reset epoch then
 * mismatches and re-reads instead of trusting an offset into the restarted
 * numbering. The epoch key is given NO ttl, so the bump survives the reaping.
 */
const PUBLISH_SCRIPT = `
local seqKey = KEYS[1]
local bufKey = KEYS[2]
local epochKey = KEYS[3]
local maxSize = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local event = ARGV[3]
local data = ARGV[4]
if maxSize == nil or ttl == nil then
  return redis.error_reply('REPLAY_PUBLISH: maxSize/ttl must be numeric')
end

local seq = redis.call('incr', seqKey)
if seq == 1 then
  redis.call('incr', epochKey)
end
local id = seq .. '-0'
redis.call('xadd', bufKey, 'MAXLEN', '~', maxSize, id, 'event', event, 'data', data)

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
	const { maxSize, ttl, replicated, minReplicas, replicationTimeoutMs, localFanoutOnStorageFailure } =
		parseReplayOptions('redis stream replay', options);

	const defaultIdempotencyTtl = options.idempotencyTtl !== undefined
		? options.idempotencyTtl
		: 48 * 60 * 60;
	if (typeof defaultIdempotencyTtl !== 'number' || !Number.isInteger(defaultIdempotencyTtl) || defaultIdempotencyTtl < 0) {
		throw new Error(`redis stream replay: idempotencyTtl must be a non-negative integer, got ${defaultIdempotencyTtl}`);
	}
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('redis stream replay: forgetUserId must be a function ({ topic, event, data }) => userId');
	}
	// Right-to-erasure: maps a buffered event to its authoring userId so
	// `live.forget` can XDEL the user's stream entries. Unset => not purgeable.
	const forgetUserId = options.forgetUserId;
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
	const mStorageFallbacks = localFanoutOnStorageFailure
		? m?.counter('replay_storage_fallbacks_total', 'Publishes that fell back to local fanout when storage failed', ['topic'])
		: null;

	function idmpKey(producerId, topic) {
		return client.key('replay:idmp:' + producerId + ':{' + topic + '}');
	}

	function seqKey(topic) {
		return client.key('replay:seq:{' + topic + '}');
	}

	function bufKey(topic) {
		return client.key('replay:streambuf:{' + topic + '}');
	}

	// Per-topic seq-space generation. Same hash tag as seq:/buf: so all keys
	// for one topic co-locate on one slot (the publish eval touches all of
	// them). Given NO ttl: a tiny monotonic integer that survives the seq key
	// being reaped, so a reset the resume hook must catch is never hidden by an
	// expired epoch.
	function epochKey(topic) {
		return client.key('replay:epoch:{' + topic + '}');
	}

	// Last epoch this process observed for a topic, so the synchronous
	// subscribe-ack carrier can read a recent value without awaiting Redis.
	/** @type {Map<string, number>} */
	const epochCache = new Map();

	async function currentEpoch(topic) {
		const val = await withBreaker(b, () => redis.get(epochKey(topic)));
		const epoch = val ? parseInt(val, 10) : 0;
		epochCache.set(topic, epoch);
		return epoch;
	}

	// Batched epoch read for the resume hook: one pipeline round trip on
	// standalone, per-node fan-out on Cluster (each epoch key is single-slot;
	// the BATCH may cross slots, which is exactly what execMultiSlot handles).
	async function currentEpochs(topics) {
		const res = await withBreaker(b, () => execMultiSlot(redis, topics.map((t) => ['get', epochKey(t)])));
		const out = new Map();
		for (let i = 0; i < topics.length; i++) {
			const [err, val] = res[i];
			if (err) throw err;
			const epoch = val ? parseInt(val, 10) : 0;
			epochCache.set(topics[i], epoch);
			out.set(topics[i], epoch);
		}
		return out;
	}

	async function bumpEpoch(topic) {
		const next = await withBreaker(b, () => redis.incr(epochKey(topic)));
		const epoch = typeof next === 'number' ? next : parseInt(next, 10);
		epochCache.set(topic, epoch);
		return epoch;
	}

	// Latch so the storage-fallback degradation warns ONCE per tracker, not per
	// event (the per-publish volume under a sustained outage is the
	// replay_storage_fallbacks_total metric). The one warn carries the first
	// degraded publish's requestId as a correlation anchor; the raw topic is
	// omitted (it can embed user ids - the metric carries the sanitized label).
	let warnedStorageFallback = false;

	const tracker = {
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
			const ek = epochKey(topic);

			// Serialize BEFORE entering the storage try-block so a malformed
			// payload (BigInt, circular reference, etc.) does not surface as
			// a misleading ReplayStorageError or trigger any fallback path.
			let payload;
			try {
				payload = JSON.stringify(data ?? null);
			} catch (err) {
				throw new ReplaySerializationError('publishIdempotent', err);
			}

			let result;
			try {
				result = await withBreaker(b, () =>
					evalCached(redis, IDMP_PUBLISH_SCRIPT, 4, ik, sk, bk, ek,
						requestId, maxSize, ttl, idmpTtl, event, payload)
				);
			} catch (err) {
				throw new ReplayStorageError('publishIdempotent', err);
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
				await awaitReplicationGrouped(redis, minReplicas, replicationTimeoutMs, b, mReplications, mReplicationTimeouts);
			}

			// Thread the authoritative stream seq onto the live frame (see the
			// sortedset replay for the full rationale) so a resuming client dedups
			// against the same seq space the buffer stores. seq comes from the Lua
			// INCR, so it is a valid positive integer; guarded for safety.
			await platform.publish(topic, event, data, Number.isInteger(seq) && seq >= 1 ? { seq } : undefined);
			return { seq, isDuplicate: false };
		},

		async publish(platform, topic, event, data) {
			const sk = seqKey(topic);
			const bk = bufKey(topic);
			const ek = epochKey(topic);

			// Serialize BEFORE entering the storage try-block. See publishIdempotent
			// above for the rationale: a malformed payload is a caller-input bug
			// that must bypass localFanoutOnStorageFailure.
			let payload;
			try {
				payload = JSON.stringify(data ?? null);
			} catch (err) {
				throw new ReplaySerializationError('publish', err);
			}

			let seq;
			try {
				seq = Number(await withBreaker(b, () =>
					evalCached(redis, PUBLISH_SCRIPT, 3, sk, bk, ek, maxSize, ttl, event, payload)
				));
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
					if (!warnedStorageFallback) {
						warnedStorageFallback = true;
						console.warn(
							'[redis stream replay] storage failed; falling back to local publish, durability degraded' +
							(platform?.requestId ? ' (requestId=' + platform.requestId + ')' : '') +
							'. Further occurrences are suppressed; see the replay_storage_fallbacks_total metric. Cause: ' +
							(err?.message ?? err)
						);
					}
					return platform.publish(topic, event, data);
				}
				throw new ReplayStorageError('publish', err);
			}
			mPublishes?.inc({ topic: mt(topic) });

			if (replicated) {
				await awaitReplicationGrouped(redis, minReplicas, replicationTimeoutMs, b, mReplications, mReplicationTimeouts);
			}

			// Thread the authoritative stream seq onto the live frame; the degraded
			// local-fanout fallback above stays counter-stamped (no authoritative seq).
			return Number.isInteger(seq) && seq >= 1
				? platform.publish(topic, event, data, { seq })
				: platform.publish(topic, event, data);
		},

		async seq(topic) {
			const val = await withBreaker(b, () => redis.get(seqKey(topic)));
			return val ? parseInt(val, 10) : 0;
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
			// Reject malformed since values defensively. Negative since
			// fell through to `'-'` (XRANGE from start of stream) and
			// returned the entire buffer - a data-leak vector for buggy
			// host code that forwards client input unchecked. Authorization is handled upstream in
			// replay() via checkSubscribe; since() is a direct caller
			// API and needs its own gate.
			if (!Number.isInteger(since) || since < 0) return [];
			const startId = `(${since}-0`;
			const entries = await withBreaker(b, () => redis.xrange(bufKey(topic), startId, '+'));
			const result = [];
			for (const [id, flat] of entries) {
				const fields = fieldsToObject(flat);
				try {
					result.push({
						seq: seqFromId(id),
						// Versioned read: new entries omit the topic field (it is
						// the per-topic key), so recover it from the `topic` param;
						// legacy entries written before that change still carry it.
						topic: fields.topic ?? topic,
						event: fields.event,
						data: JSON.parse(fields.data)
					});
				} catch { /* skip corrupted */ }
			}
			return result;
		},

		async replay(ws, topic, sinceSeq, platform, reqId) {
			if (!await checkReplayAccess(ws, topic, platform, reqId)) return;
			const replayTopic = '__replay:' + topic;
			// Same input gate as since(): malformed sinceSeq would fall
			// through to `'-'` (entire stream) via the prior ternary.
			// Emit a bare `end` marker so the wire protocol shape is
			// preserved.
			if (!Number.isInteger(sinceSeq) || sinceSeq < 0) {
				platform.send(ws, replayTopic, 'end', { reqId: reqId || undefined });
				return;
			}
			if (b) b.guard();

			let entries;
			try {
				const startId = `(${sinceSeq}-0`;
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
			await withBreaker(b, () => scanAndUnlink(redis, client.key('replay:*')));
		},

		async clearTopic(topic) {
			// clearTopic restarts the seq counter at 1 on the next publish, so
			// it IS a seq-space reset and must bump the epoch. Bump BEFORE the
			// unlink so there is never a window where seq:/buf: are gone but the
			// epoch still reads the pre-reset value.
			await bumpEpoch(topic);
			await withBreaker(b, () => redis.unlink(seqKey(topic), bufKey(topic)));
		},

		/**
		 * Right-to-erasure (`live.forget`): XDEL a user's buffered events from
		 * every topic stream. Scans `replay:streambuf:{*}`, XRANGEs each, maps each
		 * entry through `forgetUserId` (the topic is recovered from the stream key),
		 * and XDELs the matches. Leaves the seq space intact (the holes read as
		 * truncation on resume -> a full rehydrate, the safe outcome). A no-op
		 * without a `forgetUserId` extractor.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} buffered events removed
		 */
		async purgeUser(tenantId, userId) {
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			const prefix = client.key('replay:streambuf:{');
			let keys;
			try { keys = await scanKeys(redis, bufKey('*')); } catch { return 0; }
			let n = 0;
			for (const bk of keys) {
				const topic = bk.startsWith(prefix) && bk.endsWith('}') ? bk.slice(prefix.length, bk.length - 1) : undefined;
				let entries;
				try { entries = await redis.xrange(bk, '-', '+'); } catch { continue; }
				const toDel = [];
				for (const [id, flat] of entries) {
					const fields = fieldsToObject(flat);
					let data;
					try { data = JSON.parse(fields.data); } catch { continue; }
					let uid;
					try { uid = forgetUserId({ topic: fields.topic ?? topic, event: fields.event, data }); } catch { continue; }
					if (uid === userId) toDel.push(id);
				}
				if (toDel.length > 0) {
					try { await redis.xdel(bk, ...toDel); n += toDel.length; } catch { /* best-effort */ }
				}
			}
			return n;
		},

		/**
		 * Current stored generation of a topic's seq space (baseline 0 when it
		 * has never reset). The resume hook compares this to the client's
		 * presented epoch.
		 * @param {string} topic
		 * @returns {Promise<number>}
		 */
		currentEpoch(topic) {
			return currentEpoch(topic);
		},

		/**
		 * Synchronous best-effort read of a topic's epoch from the in-process
		 * cache, for the subscribe-ack carrier that cannot await Redis. Wire it
		 * to `platform.topicEpoch`. Baseline 0 for a topic not yet observed.
		 * @param {string} topic
		 * @returns {number}
		 */
		cachedEpoch(topic) {
			return epochCache.get(topic) ?? 0;
		},

		// Returns a hook function for `hooks.ws.resume`. Shared body (epoch
		// match/rehydrate semantics + batched epoch reads + concurrent
		// per-topic gap-fills): `createResumeHook` in shared/replay-helpers.js.
		resumeHook() {
			return createResumeHook({
				currentEpochs,
				replay: (ws, topic, seq, platform) => tracker.replay(ws, topic, seq, platform)
			});
		}
	};
	return tracker;
}
