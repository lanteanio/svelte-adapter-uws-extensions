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
 * The topic is wrapped in a Redis hash tag (the literal braces around the
 * topic) so both keys for one topic share a hash slot. That keeps the
 * multi-key publish eval and the per-topic UNLINK on a single slot under
 * Redis Cluster while leaving the key string unchanged in behavior on a
 * standalone server.
 *
 * @module svelte-adapter-uws-extensions/redis/replay
 */

import { createStreamReplay } from './replay-stream.js';
import { MAX_REPLAY_EPOCH_CACHE_TOPICS } from '../shared/caps.js';
import { createLruMap } from '../shared/lru-map.js';
import { evalCached } from '../shared/eval-cached.js';
import { scanUnlinkExcept, scanKeys } from '../shared/redis-scan.js';
import { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError, parseReplayOptions, awaitReplicationGrouped, createResumeHook } from '../shared/replay-helpers.js';
import { execMultiSlot } from '../shared/cluster.js';
import { withBreaker } from '../shared/breaker.js';
import { checkReplayAccess, RESUME_PREAUTHORIZED } from '../shared/replay-gate.js';
import { decodeSortedSetMember } from '../shared/replay-envelope.js';
import { topicInTenant } from '../shared/tenant-topic.js';
export { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError };
export { migrateReplayToStream } from './replay-migrate.js';

/**
 * @typedef {Object} RedisReplayOptions
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds for replay keys (0 = no expiry)
 * @property {'replicated'} [durability] - Opt into per-publish replication signalling. After the write, runs `WAIT minReplicas replicationTimeoutMs`; throws `ReplicationTimeoutError` and skips the local broadcast when fewer than `minReplicas` replicas ack.
 * @property {number} [minReplicas=1] - Minimum replicas that must ack before publish is considered durable. Required when `durability: 'replicated'`.
 * @property {number} [replicationTimeoutMs=1000] - Per-publish replication timeout in milliseconds. `0` blocks indefinitely (Redis WAIT semantics).
 * @property {number} [maxDataBytes=262144] - Positive-integer byte cap on one JSON-encoded payload, shared by sorted-set and stream storage.
 */

/**
 * @typedef {Object} RedisReplayBuffer
 * @property {(platform: import('svelte-adapter-uws').Platform, topic: string, event: string, data?: unknown) => Promise<boolean>} publish
 * @property {(topic: string) => Promise<number>} seq
 * @property {(topic: string, lastSeenSeq: number) => Promise<{truncated: boolean, missingFrom: number | null}>} gap
 * @property {(topic: string, since: number) => Promise<Array<{seq: number, topic: string, event: string, data: unknown}>>} since
 * @property {(ws: any, topic: string, sinceSeq: number, platform: import('svelte-adapter-uws').Platform) => Promise<number | undefined>} replay
 * @property {() => Promise<void>} clear
 * @property {(topic: string) => Promise<void>} clearTopic
 */

/**
 * Lua script for atomic publish: increment seq, store message, trim buffer.
 *
 * KEYS[1] = seq key
 * KEYS[2] = buf key (sorted set)
 * KEYS[3] = epoch key
 * ARGV[1] = event
 * ARGV[2] = data (JSON-encoded)
 * ARGV[3] = maxSize
 * ARGV[4] = ttl (seconds, 0 = no expiry)
 *
 * Returns the new sequence number.
 *
 * The stored member is a VERSIONED envelope `{"v":1,"seq":N,"event":..,"data":..}`.
 * The topic is NOT stored: it is the per-topic buffer key, so writing it into
 * every member is a redundant value the reader recovers from the key. The `seq`
 * stays in the member (it is the read authority) and equals the ZSET score by
 * construction. `v` lets a future format change be a self-describing migration;
 * an absent `v` on a legacy member reads as v1.
 *
 * When the seq counter reads 1 the seq space is fresh: either a brand-new
 * topic or one whose seq key was reaped (TTL expiry) since the last publish.
 * Both mean the seq numbering restarted, so bump the epoch in the same atomic
 * script - a client holding a pre-reset epoch then mismatches on resume and
 * re-reads instead of trusting an offset into the restarted numbering. The
 * epoch key is deliberately given NO ttl below, so it survives the seq-key
 * reaping and the bump sticks across the reset edge.
 */
const PUBLISH_SCRIPT = `
local seqKey = KEYS[1]
local bufKey = KEYS[2]
local epochKey = KEYS[3]
local event = ARGV[1]
local data = ARGV[2]
local maxSize = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
if maxSize == nil or ttl == nil then
  return redis.error_reply('REPLAY_PUBLISH: maxSize/ttl must be numeric')
end

local seq = redis.call('incr', seqKey)
if seq == 1 then
  redis.call('incr', epochKey)
end
local envelope = cjson.encode({v = 1, seq = seq, event = event})
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
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('redis replay: forgetUserId must be a function ({ topic, event, data }) => userId');
	}
	if (options.storage === 'stream') {
		return createStreamReplay(client, options);
	}
	// Right-to-erasure: a buffered event payload is app-defined; when set this maps
	// an event to its authoring userId so `live.forget` drops the user's buffered
	// events at purge time (per-topic buffers are bounded, so a scan is cheap).
	const forgetUserId = options.forgetUserId;

	const { maxSize, ttl, replicated, minReplicas, replicationTimeoutMs, maxDataBytes, localFanoutOnStorageFailure } =
		parseReplayOptions('redis replay', options);

	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mResumeTopicOverflows = m?.counter(
		'replay_resume_topic_overflows_total',
		'Resume frames whose topic set exceeded MAX_RESUME_TOPICS'
	);
	const mCorruptions = m?.counter('replay_corruptions_total', 'Stored replay entries dropped as corrupt or an unknown envelope version', ['topic']);
	const mReplications = replicated ? m?.counter('replay_replications_total', 'Publishes confirmed replicated within timeout') : null;
	const mReplicationTimeouts = replicated ? m?.counter('replay_replication_timeouts_total', 'Publishes that did not reach minReplicas within timeout') : null;
	const mStorageFallbacks = localFanoutOnStorageFailure
		? m?.counter('replay_storage_fallbacks_total', 'Publishes that fell back to local fanout when storage failed', ['topic'])
		: null;

	function seqKey(topic) {
		return client.key('replay:seq:{' + topic + '}');
	}

	function bufKey(topic) {
		return client.key('replay:buf:{' + topic + '}');
	}

	// Recover a topic from its `replay:buf:{<topic>}` key for the erasure scan,
	// where the loop holds the key rather than the topic (the key is the topic's
	// canonical home now that the envelope no longer stores it).
	const bufPrefix = client.key('replay:buf:{');
	function topicFromBufKey(key) {
		return key.startsWith(bufPrefix) && key.endsWith('}') ? key.slice(bufPrefix.length, key.length - 1) : '';
	}

	// Per-topic seq-space generation. Wrapped in the SAME hash tag as seq:/buf:
	// so all three keys for one topic co-locate on one slot (the publish eval
	// touches all three; cluster requires a shared slot). Given NO ttl: it is a
	// tiny monotonic integer that survives the seq key being reaped, so a reset
	// the resume hook must catch is never hidden by an expired epoch.
	function epochKey(topic) {
		return client.key('replay:epoch:{' + topic + '}');
	}

	// Last epoch this process observed for a topic, so a synchronous caller (the
	// subscribe-ack carrier, which cannot await Redis) can read a recent value.
	// Read-through populated by currentEpoch and refreshed by bumpEpoch; a topic
	// not yet seen reads as the baseline 0.
	/** @type {Map<string, number>} */
	const epochCache = createLruMap(MAX_REPLAY_EPOCH_CACHE_TOPICS);
	function epochCacheSet(topic, epoch) {
		epochCache.set(topic, epoch);
	}


	// Read the stored epoch for a topic. A topic whose seq space has never reset
	// has no epoch key yet - that is the baseline epoch and reads as 0. An old
	// client that presents no epoch SKIPS the comparison entirely (an
	// unconditional match): a literal compare against 0 would be wrong, because
	// the first publish on a fresh topic bumps it 0 -> 1, so want=0 vs have=1
	// would spuriously rehydrate every old-client resume.
	async function currentEpoch(topic) {
		const val = await withBreaker(b, () => redis.get(epochKey(topic)));
		const epoch = val ? parseInt(val, 10) : 0;
		epochCacheSet(topic, epoch);
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
			epochCacheSet(topics[i], epoch);
			out.set(topics[i], epoch);
		}
		return out;
	}

	// Atomic single-key INCR of a topic's epoch. Same slot as seq:/buf: via the
	// shared hash tag, so it resolves identically on standalone and cluster (one
	// slot, no cross-slot fan-out). Called at every point that resets the seq
	// space outside the publish Lua (clearTopic); the in-Lua `seq == 1` edge
	// handles the TTL-reap and lossy-reshard cases.
	async function bumpEpoch(topic) {
		const next = await withBreaker(b, () => redis.incr(epochKey(topic)));
		const epoch = typeof next === 'number' ? next : parseInt(next, 10);
		epochCacheSet(topic, epoch);
		return epoch;
	}

	// Latch so the storage-fallback degradation warns ONCE per tracker, not per
	// event: under a sustained outage the per-publish volume would spam the log,
	// and that volume is already the replay_storage_fallbacks_total metric. The
	// one warn carries the first degraded publish's requestId as a correlation
	// anchor; the raw topic is deliberately omitted (it can embed user ids - the
	// metric carries the sanitized topic label).
	let warnedStorageFallback = false;

	const tracker = {
		async publish(platform, topic, event, data) {
			const sk = seqKey(topic);
			const bk = bufKey(topic);
			const ek = epochKey(topic);

			// Serialize BEFORE entering the storage try-block. A JSON.stringify
			// throw (BigInt, circular reference, etc.) is a caller-input bug,
			// not a transient storage failure, and must not trigger the
			// localFanoutOnStorageFailure fallback - that would silently
			// degrade the durability contract on payloads the user thought
			// were being persisted.
			let payload;
			try {
				payload = JSON.stringify(data ?? null);
			} catch (err) {
				throw new ReplaySerializationError('publish', err);
			}
			// Same bound the Postgres buffer applies. An unbounded entry feeds
			// memory growth in the sorted set and the read-back amplification
			// of replay(); a cap on one backend and not the other is not a
			// contract, it is a difference in which deployment falls over.
			if (Buffer.byteLength(payload) > maxDataBytes) {
				throw new ReplaySerializationError('publish', new Error(`redis replay: data exceeds maxDataBytes (${maxDataBytes} bytes)`));
			}

			let seq;
			try {
				seq = Number(await withBreaker(b, () =>
					evalCached(redis, PUBLISH_SCRIPT, 3, sk, bk, ek, event, payload, maxSize, ttl)
				));
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
					if (!warnedStorageFallback) {
						warnedStorageFallback = true;
						console.warn(
							'[redis replay] storage failed; falling back to local publish, durability degraded' +
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

			// Thread the authoritative buffer seq (the Lua INCR result) onto the live
			// frame so a resuming client gap-fills against the SAME seq space the
			// buffer stores - across a restart, or across instances via the shared
			// Redis buffer - instead of the adapter's per-worker counter, which would
			// diverge and cause duplicate/dropped events on resume. Guard on a valid
			// positive integer (the adapter poisons its convergence tracker on 0/NaN);
			// the degraded local-fanout fallback above stays counter-stamped since it
			// has no authoritative seq.
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
				const decoded = decodeSortedSetMember(raw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); continue; }
				b?.success();
				if (decoded.seq > target) {
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
			// would expand `since + 1` to <= 0 and ZRANGEBYSCORE would
			// return every entry in the buffer. Authorization is handled
			// upstream (checkSubscribe gate) for the replay() entrypoint
			// but since() is a direct caller API; this gate protects
			// against buggy host code that forwards client input
			// unchecked.
			if (!Number.isInteger(since) || since < 0) return [];
			const raw = await withBreaker(b, () => redis.zrangebyscore(bufKey(topic), since + 1, '+inf'));
			const result = [];
			for (let i = 0; i < raw.length; i++) {
				const decoded = decodeSortedSetMember(raw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); continue; }
				result.push(decoded);
			}
			return result;
		},

		async replay(ws, topic, sinceSeq, platform, reqId, preAuthorized) {
			// The resume hook has just run this exact gate for this exact
			// topic. Re-running it would invoke the APP's subscribe hook a
			// second time per resumed topic - doubling its authorization load
			// and any side effects it has (audit rows, rate-limit tokens, a
			// presence join). Identity-compared against the private token, not
			// truthiness: this is a public method, so a plain boolean here
			// would let any caller skip the gate by passing `true`.
			if (preAuthorized !== RESUME_PREAUTHORIZED && !await checkReplayAccess(ws, topic, platform, reqId)) return;
			// Same input gate as `since()`: malformed sinceSeq would
			// return the entire buffer via `sinceSeq + 1 <= 0`. Emit a
			// bare `end` marker so the wire protocol shape is preserved.
			if (!Number.isInteger(sinceSeq) || sinceSeq < 0) {
				platform.send(ws, '__replay:' + topic, 'end', { reqId: reqId || undefined });
				return;
			}
			const replayTopic = '__replay:' + topic;
			if (b) b.guard();
			const bk = bufKey(topic);

			// Pipeline the oldest-entry probe (for truncation detection)
			// alongside the seq>sinceSeq fetch so this is one RTT and the
			// returned data covers only what the client actually needs.
			// Fetch a small slice for the oldest probe so corrupt entries
			// at the head don't hide the actual oldest valid seq. The seq
			// counter rides the same RTT: the empty-gap-fill truncation check
			// and the watermark confirmation (see the return below) both need
			// it, and resume is the reconnect-storm hot path, so it must not
			// cost a second round trip. All three keys share the {topic} hash
			// tag, so the pipeline stays single-slot on a cluster.
			let oldestRaw, missedRaw, seqRaw;
			try {
				const pipe = redis.pipeline();
				pipe.zrange(bk, 0, 9);
				pipe.zrangebyscore(bk, sinceSeq + 1, '+inf');
				pipe.get(seqKey(topic));
				const results = await pipe.exec();
				if (results[0][0]) throw results[0][0];
				if (results[1][0]) throw results[1][0];
				if (results[2][0]) throw results[2][0];
				oldestRaw = results[0][1];
				missedRaw = results[1][1];
				seqRaw = results[2][1];
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			let oldestSeq = null;
			if (oldestRaw) {
				for (let i = 0; i < oldestRaw.length; i++) {
					// Probe for the oldest valid seq (truncation detection) only. A
					// corrupt member here is counted by the missed-range scan below when
					// it falls in the delivered range; counting it in both loops would
					// double-report the same entry in replay_corruptions_total.
					const decoded = decodeSortedSetMember(oldestRaw[i], topic);
					if (decoded === null) continue;
					oldestSeq = decoded.seq;
					break;
				}
			}
			if (oldestSeq !== null && sinceSeq > 0 && oldestSeq > sinceSeq + 1) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}

			const missed = [];
			for (let i = 0; i < missedRaw.length; i++) {
				const decoded = decodeSortedSetMember(missedRaw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); continue; }
				missed.push(decoded);
			}

			const currentSeq = seqRaw ? parseInt(seqRaw, 10) : 0;
			if (oldestSeq === null && sinceSeq > 0 && missed.length === 0 && currentSeq > sinceSeq) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
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
			// Covered watermark for the adapter's recovery barrier: the highest seq
			// this gap-fill actually delivered (ZRANGEBYSCORE is ascending, so the
			// last valid decode is the highest). Everything at or below it is
			// already on the wire, so the barrier can dedup its held live frames
			// exactly instead of falling back to the pre-window floor. A corrupt
			// newest entry lowers the watermark to the last seq actually SENT, so
			// a held live frame carrying the corrupt entry's seq flushes and fills
			// the hole rather than being wrongly skipped.
			if (missed.length > 0) return missed[missed.length - 1].seq;
			if (sinceSeq === 0) return 0;
			// Empty gap-fill: the client's claim of being current at sinceSeq is
			// wire input, trusted only when the seq counter confirms the store
			// ever issued that seq. An offset above the counter (corrupt client
			// state, or a failover to a lagging replica regressing the counter
			// with the epoch unchanged) resolves undefined instead, so the
			// adapter falls back to its conservative pre-window floor and an
			// in-window publish is delivered at-least-once rather than silently
			// skipped. Not clamped to the counter: an in-window publish can bump
			// the counter to the held frame's own seq before this read, so a
			// clamped watermark could still swallow that frame.
			return currentSeq >= sinceSeq ? sinceSeq : undefined;
		},

		async clear() {
			// A global clear resets every topic's seq space, so - exactly like
			// clearTopic - every topic's epoch must ROTATE, not vanish. Deleting the
			// epoch key (the old blanket `replay:*` unlink) let the next publish
			// recreate generation 1, so a client that straddled the clear matched on
			// resume and silently gap-filled against the restarted numbering. INCR
			// every epoch key before dropping any seq/buf state; the epoch key itself
			// is preserved.
			const epochPrefix = client.key('replay:epoch:{');
			await withBreaker(b, () => scanUnlinkExcept(
				redis,
				client.key('replay:*'),
				(k) => k.startsWith(epochPrefix),
				(node, k) => node.incr(k)
			));
			// The in-process epoch cache backs the synchronous ack carrier; drop it
			// so a read after the clear reflects the rotated generation.
			epochCache.clear();
		},

		async clearTopic(topic) {
			// clearTopic restarts the seq counter at 1 on the next publish, so
			// it IS a seq-space reset and must bump the epoch. Bump BEFORE the
			// unlink so there is never a window where seq:/buf: are gone but the
			// epoch still reads the pre-reset value (a resume landing in that
			// window would gap-fill against an empty buffer and wrongly see
			// contiguity).
			await bumpEpoch(topic);
			await withBreaker(b, () => redis.unlink(seqKey(topic), bufKey(topic)));
		},

		/**
		 * Right-to-erasure (`live.forget`): drop a user's buffered events from
		 * every topic. Scans `replay:buf:{*}`, parses each ZSET member ({seq, topic,
		 * event, data}), maps it through `forgetUserId`, and ZREMs the matches.
		 * Leaves the seq space intact - the resulting seq holes read as truncation
		 * on resume (a full rehydrate), which is the safe outcome. A no-op without a
		 * `forgetUserId` extractor.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} buffered events removed
		 */
		async purgeUser(tenantId, userId) {
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			let keys;
			try { keys = await scanKeys(redis, bufKey('*')); } catch { return 0; }
			let n = 0;
			for (const bk of keys) {
				const topic = topicFromBufKey(bk);
				// Same tenant rule as the room-owner and presence-roster legs of
				// the same purge. Without it one tenant's erasure reached every
				// other tenant's buffered events for the same user id.
				if (!topicInTenant(tenantId, topic)) continue;
				let members;
				try { members = await redis.zrange(bk, 0, -1); } catch { continue; }
				const toRemove = [];
				for (const member of members) {
					const decoded = decodeSortedSetMember(member, topic);
					if (decoded === null) continue;
					let uid;
					try { uid = forgetUserId({ topic: decoded.topic, event: decoded.event, data: decoded.data }); } catch { continue; }
					if (uid === userId) toRemove.push(member);
				}
				if (toRemove.length > 0) {
					try { await redis.zrem(bk, ...toRemove); n += toRemove.length; } catch { /* best-effort */ }
				}
			}
			return n;
		},

		/**
		 * Current stored generation of a topic's seq space. A topic whose seq
		 * space has never reset reads as the baseline 0. Used by the resume
		 * hook to compare against the client's presented epoch.
		 * @param {string} topic
		 * @returns {Promise<number>}
		 */
		currentEpoch(topic) {
			return currentEpoch(topic);
		},

		/**
		 * Synchronous best-effort read of a topic's epoch from the in-process
		 * cache (populated by currentEpoch / bumpEpoch). For the subscribe-ack
		 * carrier, which cannot await Redis; wire it to `platform.topicEpoch`
		 * so the ack carries the per-topic generation a resuming client then
		 * presents back. Returns the baseline 0 for a topic not yet observed.
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
				// Forwards `reqId` and `preAuthorized` too. A 4-arity forwarder
				// drops the flag, and `replay()` then re-runs the app's subscribe
				// hook for every topic the hook just authorized - once more per
				// topic, and from inside Promise.all, so with no concurrency
				// bound at all.
				replay: (ws, topic, seq, platform, reqId, preAuthorized) =>
					tracker.replay(ws, topic, seq, platform, reqId, preAuthorized),
				onTruncate: () => mResumeTopicOverflows?.inc()
			});
		}
	};
	return tracker;
}
