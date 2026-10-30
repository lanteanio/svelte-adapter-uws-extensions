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
 *   - Key `{prefix}replay:epoch:{topic}` - epoch of the current seq space (random)
 *   - Key `{prefix}replay:used:{topic}` - last publish time, for `maxTopics`
 *
 * The topic is wrapped in a Redis hash tag (the literal braces around the
 * topic) so every key for one topic shares a hash slot. That keeps the
 * multi-key publish eval and the per-topic UNLINK on a single slot under
 * Redis Cluster while leaving the key string unchanged in behavior on a
 * standalone server. The topic registry that enforces `maxTopics` has its
 * own slot and is written only when a topic is created, cleared or evicted
 * (see _replay-topics.js).
 *
 * @module svelte-adapter-uws-extensions/redis/replay
 */

import { createStreamReplay } from './replay-stream.js';
import { MAX_REPLAY_EPOCH_CACHE_TOPICS } from '../shared/caps.js';
import { createLruMap } from '../shared/lru-map.js';
import { evalCached } from '../shared/eval-cached.js';
import { scanUnlinkExcept, scanKeys } from '../shared/redis-scan.js';
import { createReplayTopics, parseMaxTopics, FRESH_EPOCH } from './_replay-topics.js';
import { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError, parseReplayOptions, awaitReplicationGrouped, createResumeHook } from '../shared/replay-helpers.js';
import { execMultiSlot, firstCommandError } from '../shared/cluster.js';
import { withBreaker } from '../shared/breaker.js';
import { checkReplayAccess, RESUME_PREAUTHORIZED } from '../shared/replay-gate.js';
import { decodeSortedSetMember, replayHasHole } from '../shared/replay-envelope.js';
import { topicInTenant } from '../shared/tenant-topic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure, firstReportInProcess } from '../shared/diagnostic.js';
import { createInputDropReporter } from '../shared/input-drops.js';
import { UNSEQUENCED_PUBLISH } from '../shared/sequence-authority.js';
import { defineOptions, option } from '../shared/options.js';
export { ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError };
export { migrateReplayToStream } from './replay-migrate.js';

/**
 * @typedef {Object} RedisReplayOptions
 * @property {number} [size=1000] - Max messages per topic
 * @property {number} [ttl=0] - TTL in seconds for replay keys (0 = no expiry)
 * @property {'replicated'} [durability] - Opt into per-publish replication signalling. After the write, runs `WAIT minReplicas replicationTimeoutMs`; throws `ReplicationTimeoutError` and skips the local broadcast when fewer than `minReplicas` replicas ack.
 * @property {number} [minReplicas=1] - Minimum replicas that must ack before publish is considered durable. Required when `durability: 'replicated'`.
 * @property {number} [replicationTimeoutMs=1000] - Per-publish replication timeout in milliseconds. `0` disables the Redis WAIT timeout; client command and socket deadlines still apply (5000 ms by default).
 * @property {number} [maxDataBytes=262144] - Positive-integer byte cap on one JSON-encoded payload, shared by sorted-set and stream storage.
 * @property {number} [maxTopics=100] - Max topics holding replay state in this key prefix, across every instance sharing it. Creating a topic past the cap evicts the least recently published topic's buffer, counter and epoch.
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
 * KEYS[4] = use stamp key
 * ARGV[1] = event
 * ARGV[2] = data (JSON-encoded)
 * ARGV[3] = maxSize
 * ARGV[4] = ttl (seconds, 0 = no expiry)
 * ARGV[5] = drawn epoch for a fresh seq space (0 = none yet)
 *
 * Returns the new sequence number, or -1 without writing anything when the
 * seq space is fresh and no epoch was passed.
 *
 * The stored member is a VERSIONED envelope `{"v":1,"seq":N,"event":..,"data":..}`.
 * The topic is NOT stored: it is the per-topic buffer key, so writing it into
 * every member is a redundant value the reader recovers from the key. The `seq`
 * stays in the member (it is the read authority) and equals the ZSET score by
 * construction. `v` lets a future format change be a self-describing migration;
 * an absent `v` on a legacy member reads as v1.
 *
 * A missing seq key means the seq space is fresh: a brand-new topic, or one
 * whose keys expired or were evicted. The numbering restarts at 1, so the
 * topic takes a new epoch in the same atomic script: an opaque random value,
 * different from the epoch still stored for the topic if any. A resuming
 * client holding an earlier epoch then mismatches and re-reads instead of
 * trusting an offset into the restarted numbering. A stale buffer left in the slot is
 * dropped so its entries cannot pass for the new numbering. The use stamp read
 * by `maxTopics` eviction is the server's time, so instance clocks do not
 * matter. With a `ttl`, all four keys share it, so a topic's metadata expires
 * with its entries.
 */
const PUBLISH_SCRIPT = `
local seqKey = KEYS[1]
local bufKey = KEYS[2]
local epochKey = KEYS[3]
local usedKey = KEYS[4]
local event = ARGV[1]
local data = ARGV[2]
local maxSize = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local drawn = tonumber(ARGV[5])
if maxSize == nil or ttl == nil or drawn == nil then
  return redis.error_reply('REPLAY_PUBLISH: maxSize/ttl/epoch must be numeric')
end
${FRESH_EPOCH}
if redis.call('exists', seqKey) == 0 then
  if drawn == 0 then
    return -1
  end
  redis.call('unlink', bufKey)
  redis.call('set', epochKey, freshEpoch(epochKey, ARGV[5]))
end
local seq = redis.call('incr', seqKey)
local envelope = cjson.encode({v = 1, seq = seq, event = event})
local payload = string.sub(envelope, 1, -2) .. ',"data":' .. data .. '}'
redis.call('zadd', bufKey, seq, payload)

local count = redis.call('zcard', bufKey)
if count > maxSize then
  redis.call('zremrangebyrank', bufKey, 0, count - maxSize - 1)
end

local serverTime = redis.call('time')
redis.call('set', usedKey, tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000))
if ttl > 0 then
  redis.call('expire', seqKey, ttl)
  redis.call('expire', bufKey, ttl)
  redis.call('expire', epochKey, ttl)
  redis.call('expire', usedKey, ttl)
end

return seq
`;

const validateOptions = defineOptions({
	factory: 'createReplay',
	label: 'redis replay',
	types: 'RedisReplayOptions in redis/replay.d.ts'
}, {
	storage: option.known,
	size: option.known,
	ttl: option.known,
	durability: option.known,
	minReplicas: option.known,
	replicationTimeoutMs: option.known,
	idempotencyTtl: option.known,
	localFanoutOnStorageFailure: option.known,
	maxDataBytes: option.known,
	forgetUserId: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

/**
 * Create a Redis-backed replay buffer.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisReplayOptions} [options]
 * @returns {RedisReplayBuffer}
 */
export function createReplay(client, options = {}) {
	options = validateOptions(options);
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
	const maxTopics = parseMaxTopics('redis replay', options.maxTopics);
	if (ttl === 0 && !forgetUserId && firstReportInProcess(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_RETENTION_UNBOUNDED)) {
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_RETENTION_UNBOUNDED);
	}

	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic || (() => 'all');
	const mPublishes = m?.counter('replay_publishes_total', 'Messages published to replay buffer', ['topic']);
	const mEvictions = m?.counter('replay_topic_evictions_total', 'Topics evicted past maxTopics with their replay history');
	const mReplayed = m?.counter('replay_messages_replayed_total', 'Messages replayed to clients', ['topic']);
	const mTruncations = m?.counter('replay_truncations_total', 'Truncation events detected', ['topic']);
	const mResumeTopicOverflows = m?.counter(
		'replay_resume_topic_overflows_total',
		'Resume frames whose topic set exceeded MAX_RESUME_TOPICS'
	);
	const mCorruptions = m?.counter('replay_corruptions_total', 'Stored replay entries dropped as corrupt or an unknown envelope version', ['topic']);
	const inputDropped = createInputDropReporter(m, 'replay', EXTENSIONS_ERROR_IDS.REDIS_REPLAY_INPUT_DROPPED);
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

	// Per-topic seq-space epoch. Wrapped in the SAME hash tag as seq:/buf:
	// so every key for one topic co-locates on one slot (the publish eval
	// touches all of them; cluster requires a shared slot). Its value is an
	// opaque random draw made when the seq space is created, carrying no time
	// and no count, so the key may expire or be evicted with the rest of the
	// topic: a recreated topic draws again.
	function epochKey(topic) {
		return client.key('replay:epoch:{' + topic + '}');
	}

	// Last publish time, read by `maxTopics` eviction (see _replay-topics.js).
	function usedKey(topic) {
		return client.key('replay:used:{' + topic + '}');
	}

	// Last epoch this process observed for a topic, so a synchronous caller (the
	// subscribe-ack carrier, which cannot await Redis) can read a recent value.
	// Read-through populated by currentEpoch and refreshed by clearTopic; a topic
	// not yet seen reads as the baseline 0.
	/** @type {Map<string, number>} */
	const epochCache = createLruMap(MAX_REPLAY_EPOCH_CACHE_TOPICS);
	function epochCacheSet(topic, epoch) {
		epochCache.set(topic, epoch);
	}

	const topics = createReplayTopics(client, {
		maxTopics,
		breaker: b,
		onEvicted: (topic) => {
			epochCache.delete(topic);
			mEvictions?.inc();
		},
		onSeeded: (count) => {
			if (count > maxTopics && firstReportInProcess(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_TOPICS_OVER_CAP)) {
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_TOPICS_OVER_CAP, { attributes: { topics: count, maxTopics } });
			}
		}
	});

	// One read command, with the topic's use stamp added to the same round
	// trip when this process has not stamped it within
	// REPLAY_READ_USE_INTERVAL_MS.
	// Reads count as use for maxTopics, as in the core plugin; the stamp shares
	// the topic's hash slot, so the pipeline stays single-slot on a cluster. A
	// failed stamp does not fail the read.
	async function readStamped(topic, method, ...args) {
		if (!topics.readStampDue(topic)) return redis[method](...args);
		// A pipeline carries only the scripts defined before it was created.
		const touch = topics.touchCommand();
		const results = await redis.pipeline()[method](...args)[touch](2, seqKey(topic), usedKey(topic)).exec();
		if (results[0][0]) throw results[0][0];
		return results[0][1];
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
		const res = await withBreaker(b, async () => {
			const results = await execMultiSlot(redis, topics.map((t) => ['get', epochKey(t)]));
			const error = firstCommandError(results);
			if (error) throw error;
			return results;
		});
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
			const uk = usedKey(topic);

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
				// One round trip for a topic that already has a seq space. A
				// fresh one answers -1 without writing; the topic is then
				// registered (and may evict another topic past maxTopics) and
				// the write is retried with a drawn epoch.
				seq = Number(await withBreaker(b, () =>
					evalCached(redis, PUBLISH_SCRIPT, 4, sk, bk, ek, uk, event, payload, maxSize, ttl, 0)
				));
				if (seq === -1) {
					await topics.reserve(topic);
					const epoch = topics.drawEpoch();
					seq = Number(await withBreaker(b, () =>
						evalCached(redis, PUBLISH_SCRIPT, 4, sk, bk, ek, uk, event, payload, maxSize, ttl, epoch)
					));
				}
			} catch (err) {
				if (localFanoutOnStorageFailure) {
					mStorageFallbacks?.inc({ topic: mt(topic) });
					if (!warnedStorageFallback) {
						warnedStorageFallback = true;
						emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_REPLAY_STORAGE_FALLBACK, {
							error: err,
							// Each key embeds the topic; cut them should the error repeat one.
							redact: [sk, bk, ek, uk, topic],
							attributes: platform?.requestId ? { requestId: platform.requestId } : null
						});
					}
					return platform.publish(topic, event, data, UNSEQUENCED_PUBLISH);
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
			// positive integer (the adapter poisons its convergence tracker on 0/NaN).
			// A frame without one, like the degraded local-fanout fallback above, goes
			// out with seq: false: a local counter value in this topic's seq space
			// could move a client's watermark past entries the buffer still holds.
			return Number.isInteger(seq) && seq >= 1
				? platform.publish(topic, event, data, { seq })
				: platform.publish(topic, event, data, UNSEQUENCED_PUBLISH);
		},

		async seq(topic) {
			const val = await withBreaker(b, () => readStamped(topic, 'get', seqKey(topic)));
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
				raw = await readStamped(topic, 'zrangebyscore', bufKey(topic), target, '+inf');
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			for (let i = 0; i < raw.length; i++) {
				const decoded = decodeSortedSetMember(raw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); inputDropped('corrupt_entry', 'decode'); continue; }
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
			// A counter of 0 means the topic has no seq space (never published,
			// cleared, expired or evicted): a nonzero position belongs to a
			// numbering the store no longer holds.
			if (currentSeq > lastSeenSeq || currentSeq === 0) {
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
			const raw = await withBreaker(b, () => readStamped(topic, 'zrangebyscore', bufKey(topic), since + 1, '+inf'));
			const result = [];
			for (let i = 0; i < raw.length; i++) {
				const decoded = decodeSortedSetMember(raw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); inputDropped('corrupt_entry', 'decode'); continue; }
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
				platform.send(ws, '__replay:' + topic, 'end', { reqId: reqId ?? undefined });
				return;
			}
			const replayTopic = '__replay:' + topic;
			if (b) b.guard();
			const bk = bufKey(topic);

			// The seq counter rides the same RTT as the range read: hole
			// detection, the empty-gap-fill truncation check and the watermark
			// confirmation (see the return below) all need it, and resume is the
			// reconnect-storm hot path, so it must not cost a second round trip.
			// Both keys share the {topic} hash tag, so the pipeline stays
			// single-slot on a cluster. The counter is read FIRST: a publish
			// increments it and adds its entry in one script, so every seq the
			// counter reports is already visible to the range read that follows,
			// and an in-flight publish cannot pass for a missing entry.
			let missedRaw, seqRaw;
			try {
				// A pipeline carries only the scripts defined before it was created.
				const touch = topics.readStampDue(topic) ? topics.touchCommand() : null;
				const pipe = redis.pipeline();
				pipe.get(seqKey(topic));
				pipe.zrangebyscore(bk, sinceSeq + 1, '+inf');
				// The use stamp rides the same round trip (see readStamped).
				if (touch) pipe[touch](2, seqKey(topic), usedKey(topic));
				const results = await pipe.exec();
				if (results[0][0]) throw results[0][0];
				if (results[1][0]) throw results[1][0];
				seqRaw = results[0][1];
				missedRaw = results[1][1];
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			const missed = [];
			for (let i = 0; i < missedRaw.length; i++) {
				const decoded = decodeSortedSetMember(missedRaw[i], topic);
				if (decoded === null) { mCorruptions?.inc({ topic: mt(topic) }); inputDropped('corrupt_entry', 'decode'); continue; }
				missed.push(decoded);
			}

			const currentSeq = seqRaw ? parseInt(seqRaw, 10) : 0;
			const truncated = replayHasHole(missed, sinceSeq, currentSeq);
			if (truncated) {
				mTruncations?.inc({ topic: mt(topic) });
				platform.send(ws, replayTopic, 'truncated', null);
			}
			b?.success();

			for (let i = 0; i < missed.length; i++) {
				const msg = missed[i];
				platform.send(ws, replayTopic, 'msg', {
					...(reqId == null ? {} : { reqId }),
					seq: msg.seq,
					event: msg.event,
					data: msg.data
				});
			}
			if (missed.length > 0) mReplayed?.inc({ topic: mt(topic) }, missed.length);
			platform.send(ws, replayTopic, 'end', { reqId: reqId ?? undefined, ...(truncated ? { truncated: true } : {}) });
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
			// Drop every topic's keys, the registry included; the seed state
			// stays, so the deployment is not scanned again. A topic published
			// after the clear draws a new random epoch, so a client that straddled
			// the clear mismatches on resume and rehydrates.
			await withBreaker(b, () => scanUnlinkExcept(
				redis,
				client.key('replay:*'),
				(k) => k === topics.seedKey,
				async () => {}
			));
			// The in-process epoch cache backs the synchronous ack carrier; drop it
			// so a read after the clear reflects the dropped epochs.
			epochCache.clear();
		},

		async clearTopic(topic) {
			// One script resets the counter to 0, drops the buffer and sets a new
			// random epoch, so no read sees the buffer gone under the old epoch.
			// The counter is kept at 0 rather than deleted, so the next publish
			// continues under this epoch: a client that subscribed after the
			// clear resumes by gap-fill, not by rehydrate.
			epochCacheSet(topic, await topics.clearTopic(topic, bufKey(topic), ttl));
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
			try { keys = await scanKeys(redis, bufKey('*')); } catch { inputDropped('scan_failed', 'erase'); return 0; }
			let n = 0;
			for (const bk of keys) {
				const topic = topicFromBufKey(bk);
				// Same tenant rule as the room-owner and presence-roster legs of
				// the same purge. Without it one tenant's erasure reached every
				// other tenant's buffered events for the same user id.
				if (!topicInTenant(tenantId, topic)) continue;
				let members;
				try { members = await redis.zrange(bk, 0, -1); } catch { inputDropped('read_failed', 'erase'); continue; }
				const toRemove = [];
				for (const member of members) {
					const decoded = decodeSortedSetMember(member, topic);
					if (decoded === null) { inputDropped('corrupt_entry', 'erase'); continue; }
					let uid;
					try { uid = forgetUserId({ topic: decoded.topic, event: decoded.event, data: decoded.data }); } catch { inputDropped('extractor_failed', 'erase'); continue; }
					if (uid === userId) toRemove.push(member);
				}
				if (toRemove.length > 0) {
					try { await redis.zrem(bk, ...toRemove); n += toRemove.length; } catch { inputDropped('delete_failed', 'erase'); }
				}
			}
			return n;
		},

		/**
		 * Current stored epoch of a topic's seq space. A topic whose seq
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
		 * cache (populated by currentEpoch / clearTopic). For the subscribe-ack
		 * carrier, which cannot await Redis; wire it to `platform.topicEpoch`
		 * so the ack carries the per-topic epoch a resuming client then
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
