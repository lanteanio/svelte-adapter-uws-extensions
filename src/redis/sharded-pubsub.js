import { collapseByCoalesceKey } from '../shared/_coalesce-batch.js';
import { guardBreaker } from '../shared/breaker-admission.js';
/**
 * Sharded Redis pub/sub bus for svelte-adapter-uws.
 *
 * SPUBLISH / SSUBSCRIBE variant of `createPubSubBus`. In Redis Cluster
 * deployments, sharded pub/sub keeps messages on the shard that owns
 * the channel rather than fanning out across every node, which reduces
 * cluster bus bandwidth proportionally to the number of subscribers
 * actually interested in each topic.
 *
 * Differs from `createPubSubBus` in three ways:
 *
 * - Per-topic channels (`channelPrefix + shardKey(topic)`), not one
 *   shared channel for everything. SSUBSCRIBE has no wildcard, so
 *   every subscriber must SSUBSCRIBE to each channel it cares about.
 * - Dynamic subscription: `follow(topic)` and `unfollow(topic)`
 *   maintain a refcount; `SSUBSCRIBE` on first follower per channel,
 *   `SUNSUBSCRIBE` on the last one out. Wire via `bus.hooks` for
 *   automatic management against WebSocket subscribe/unsubscribe.
 * - Requires Redis 7+. `activate()` runs `INFO server` and throws on
 *   older servers; users on Redis 6 should keep using `createPubSubBus`.
 *
 * @module svelte-adapter-uws-extensions/redis/sharded-pubsub
 */

import { randomBytes, now, monotonicNow, setTimer, clearTimer } from '../shared/runtime.js';
import { fallbackRandom, fallbackHlc } from '../shared/platform-fallback.js';
import { parseRedisVersion } from '../shared/redis-version.js';
import { MAX_SHARDED_BUS_TOPICS } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { createRelayMetrics } from './_relay-metrics.js';
import { createRelayQueue } from './_relay-queue.js';
import { execMultiSlot, firstCommandError, isCluster, keySlot } from '../shared/cluster.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';
import {
	REDIS_TRACE_ATTRIBUTES,
	injectTraceContext,
	carriedTraceContext,
	liveTraceContext,
	runTraceOperation,
	traceEnabled
} from '../shared/trace-context.js';
import { openSubscriber, watchLiveness, closeConnection, isReplyError } from '../shared/redis-subscriber.js';
import { MULTI_WORKER_RUNTIME, UNSEQUENCED_DELIVERY, carriedSeq, receivedOptions, relayedLocally, relayedBatchLocally, relayEntry } from '../shared/sequence-authority.js';

// A node.ssubscribe() / sunsubscribe() aimed at a master that does not own the
// channel's slot never resolves - the command hangs instead of returning a MOVED
// redirect. So every shard (un)subscribe is bounded by this timeout; a timeout
// means the resolved owner was stale, and the call is retried on the true owner
// after a topology refresh.
const SHARD_SUBSCRIBE_TIMEOUT_MS = 5000;

// A reconciliation that leaves a wanted channel unsubscribed (its new owner
// refused it, timed out, or the slot map could not be read) is retried on its
// own: on a stable cluster no further subscriber error would ever trigger one.
// The delay doubles from the first to the second bound; a subscriber error
// still reconciles at once.
const RECONCILE_RETRY_FIRST_MS = 1000;
const RECONCILE_RETRY_MAX_MS = 30000;
// ioredis updates its subscription set on server SUNSUBSCRIBE even when an
// outstanding command absorbs the reply. Inspect that local state off the
// message path; no additional Redis commands or public options are needed.
const SHARD_STATE_CHECK_MS = 1000;

/**
 * @typedef {Object} ShardedBusOptions
 * @property {string} [channelPrefix='uws:sharded:'] - Prefix for sharded pub/sub channels.
 * @property {(topic: string) => string} [shardKey] - Map a topic to a shard label. The channel name is `channelPrefix + shardKey(topic)`. Default: identity (one channel per topic).
 * @property {{ subscribersOf(topic: string): number }} [subscribersAggregator] - Optional aggregator (typically from `redis/publish-rate`) wired with this bus's `localSubjects` as its `subjects` source. When present, `bus.subscribers(topic)` returns the cluster-wide count. When absent, `bus.subscribers(topic)` returns the local count only.
 */

/**
 * @typedef {Object} ShardedBus
 * @property {(platform: import('svelte-adapter-uws').Platform) => import('svelte-adapter-uws').Platform} wrap
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate
 * @property {() => Promise<void>} deactivate
 * @property {(topic: string) => Promise<void>} follow
 * @property {(topic: string) => Promise<void>} unfollow
 * @property {{ subscribe: Function, unsubscribe: Function, close: Function }} hooks
 */

/**
 * Create a sharded Redis pub/sub bus.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {ShardedBusOptions} [options]
 * @returns {ShardedBus}
 */
export function createShardedBus(client, options = {}) {
	const channelPrefix = options.channelPrefix ?? 'uws:sharded:';
	if (typeof channelPrefix !== 'string') {
		throw new Error('sharded bus: channelPrefix must be a string');
	}
	if (options.shardKey !== undefined && typeof options.shardKey !== 'function') {
		throw new Error('sharded bus: shardKey must be a function');
	}
	const shardKey = options.shardKey || ((topic) => topic);
	const subscribersAggregator = options.subscribersAggregator;
	if (subscribersAggregator !== undefined && (
		!subscribersAggregator || typeof subscribersAggregator.subscribersOf !== 'function'
	)) {
		throw new Error('sharded bus: subscribersAggregator must expose subscribersOf(topic)');
	}
	const instanceId = randomBytes(8).toString('hex');

	const b = options.breaker;
	const m = options.metrics;
	const relayMetrics = createRelayMetrics(m, 'sharded_pubsub');
	const mt = m?.mapTopic || (() => 'all');
	const mRelayed = m?.counter('sharded_pubsub_messages_relayed_total', 'Messages SPUBLISHed', ['topic']);
	const mReceived = m?.counter('sharded_pubsub_messages_received_total', 'Messages received via SSUBSCRIBE', ['topic']);
	const mEchoSuppressed = m?.counter('sharded_pubsub_echo_suppressed_total', 'Messages dropped by echo suppression');
	const mBatchRejected = m?.counter('sharded_pubsub_batch_rejected_total', 'Batched publishes refused before local or remote delivery', ['reason']);
	const mParseErrors = m?.counter('sharded_pubsub_parse_errors_total', 'Malformed envelopes dropped on receive');
	const mFollows = m?.counter('sharded_pubsub_ssubscribes_total', 'SSUBSCRIBE calls');
	const mUnfollows = m?.counter('sharded_pubsub_sunsubscribes_total', 'SUNSUBSCRIBE calls');
	const mResubscribes = m?.counter('sharded_pubsub_resubscribes_total', 'Channels re-subscribed on a new owner after a cluster reshard');

	function channelFor(topic) {
		return channelPrefix + shardKey(topic);
	}

	const validator = createBusValidator({
		label: 'sharded bus',
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: options.allowSystemTopics === true,
		allowedSystemTopics: []
	});

	// Whether this bus carries a frame to the other workers of this process at
	// all: their receivers apply this same envelope rule, so a frame it refuses
	// (a `__` topic it does not admit, a malformed name) can only reach them
	// through the adapter's relay. Only read on a multi-worker runtime (see
	// relayedLocally).
	const reachesSiblings = (topic, event) => validator.acceptEnvelope(topic, event);
	const entryReachesSiblings = (m) => reachesSiblings(m.topic, m.event);

	// Wire codecs registered on THIS instance, keyed by capability. A
	// `shared: true` stateless codec relays its logical event over the shard
	// channel; a receiving instance re-derives the codec from this map and
	// re-runs its own native cohort split locally (only the capability token
	// crosses the bus, never the codec). Populated by the wrap's registerWireCodec.
	/** @type {Map<string, any>} */
	const wireCodecs = new Map();

	// Sharded subscriber connections. Standalone: one duplicate. Cluster: one
	// subscriber connection per master, with each channel ssubscribed on the
	// master owning its slot. ioredis `Cluster.ssubscribe()` does not resolve
	// against a cluster (the call hangs), but a direct `node.ssubscribe()` on the
	// owning master subscribes and delivers `smessage`. The owner is resolved
	// natMap-agnostically by node id (CLUSTER SLOTS owner id -> CLUSTER MYID).
	/** @type {import('ioredis').Redis | null} */
	let subStandalone = null;
	/** @type {Map<string, import('ioredis').Redis>} nodeId -> subscriber connection */
	const subByNodeId = new Map();
	// Asks each of those for a PING while the bus is open on a cluster; one that
	// stopped answering is closed, and its end hands its channels to the
	// reconciliation (see masterEnded).
	/** @type {{ stop(): void } | null} */
	let subLiveness = null;
	/** @type {WeakMap<object, string>} subscriber connection -> its master's node id */
	const subIdOf = new WeakMap();
	// Why a master the last topology refresh read did not give its id, if one did not.
	/** @type {unknown} */
	let masterUnread = null;
	// When the slot map was last read on the bus's behalf (see refreshSlotMap).
	let slotMapReadAt = -Infinity;
	/** @type {Array<{start: number, end: number, nodeId: string}> | null} */
	let slotRanges = null;
	let subReady = false;
	let shardStateTimer = null;
	let active = false;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	// Whether activePlatform has a tracing provider, read once at activate.
	let activeTracing = false;

	/** @type {Map<string, number>} topic -> follow refcount */
	const followCounts = new Map();
	/** @type {Map<string, number>} channel -> # of topics resolving to it */
	const channelRefcounts = new Map();
	/** @type {Map<string, Promise<void>>} channel -> shared admission */
	const pendingChannels = new Map();
	/** @type {Map<string, number>} pending callers keep failures off the fast path until rollback */
	const pendingTopics = new Map();
	/** @type {Set<string>} */
	const subscribedChannels = new Set();
	/** @type {Map<string, string>} channel -> node id it is currently ssubscribed on (cluster) */
	const channelNode = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} pending reconcile: the debounce after a subscriber error, or a retry */
	let reconcileTimer = null;
	let reconcileDueAt = 0;
	// Bumped by subClose. A topology refresh or a subscribe still awaiting Redis
	// when the bus closes sees a different generation and stops before it opens
	// a connection or records state that nothing would clean up.
	let subGeneration = 0;
	// Set by subEnsure, cleared by subClose: a refresh started while the bus is
	// closed (by work that outlived the close) opens nothing.
	let subOpen = false;
	// Delay of the next retry; 0 while the last reconciliation completed.
	let reconcileRetryMs = 0;
	// One report per failure streak for each kind of failure, re-armed by a
	// reconciliation that completes: a channel its new owner refuses fails
	// again on every later reconciliation.
	let refreshFailing = false;
	let channelsFailing = false;
	// Set when a subscriber error signals a possible reshard, so the next
	// subscribe refreshes the slot map before resolving an owner (rather than
	// blind-subscribing on a stale - and therefore hanging - old owner).
	let topologyDirty = false;
	/** @type {Promise<void> | null} in-flight topology refresh, shared by concurrent callers */
	let topologyRefresh = null;
	/** @type {WeakMap<any, Set<string>>} ws -> set of followed topics */
	const wsFollows = new WeakMap();


	// Bounded FIFO tick batches: coalesce SPUBLISHes across channels
	// within one event-loop iteration into bounded pipelined calls. Each
	// entry tracks the underlying topic list so the relayed-messages
	// counter stays accurate when batch envelopes carry many messages.
	//
	// Why `setTimeout(0)` and not `queueMicrotask`: uWS dispatches each WS
	// message as its own JS task, and N-API drains microtasks at the C++/JS
	// boundary between tasks. A microtask-deferred flush fires BEFORE the
	// next socket's handler runs, so cross-socket coalescing is impossible
	// at the microtask level. `setTimeout(0)` lands in libuv's timers
	// phase, which fires only after the poll phase has dispatched every
	// ready socket message in the current iteration. Same structural
	// choice the 0.5.7 cursor always-tick rewrite locked in.
	const relayQueue = createRelayQueue({
		submit: flushRelay, metrics: m, prefix: 'sharded_pubsub',
		dropped: (reason, envelopes, messages) => relayMetrics?.dropped(reason, envelopes, messages),
		overflow: attributes => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_RELAY_BURST, { attributes })
	});
	// UTF-8 uses at most three bytes per UTF-16 code unit. Short strings
	// fit without another scan of the common small-message path.
	const safeEnvelopeChars = Math.floor(validator.maxBytes / 3);
	let envelopeOversized = false;
	function scheduleRelay(channel, msg, topics, checked = false) {
		if (!checked && msg.length > safeEnvelopeChars && !validator.acceptRaw(msg)) {
			relayMetrics?.dropped('envelope_oversized', 1, topics.length);
			if (!envelopeOversized) {
				envelopeOversized = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_ENVELOPE_OVERSIZED, {
					attributes: { maxEnvelopeBytes: validator.maxBytes }
				});
			}
			return;
		}
		envelopeOversized = false;
		relayQueue.push(msg, topics.length, channel, topics);
	}

	// Relay one envelope on its shard channel from inside a producer operation,
	// carrying its trace context when the encoded result still fits under
	// maxEnvelopeBytes. Every receiving instance enforces that cap before
	// parsing, so an envelope the trace fields push over it would be dropped by
	// every peer; the message goes without them instead. An envelope over the
	// cap on its own is reported and dropped by scheduleRelay.
	let traceOmissionReported = false;
	function relayTraced(channel, env, topics, traceContext) {
		if (traceContext === null) {
			scheduleRelay(channel, JSON.stringify(env), topics);
			return;
		}
		injectTraceContext(env, traceContext);
		const traced = JSON.stringify(env);
		if (validator.acceptRaw(traced)) {
			scheduleRelay(channel, traced, topics);
			return;
		}
		delete env.traceparent;
		delete env.tracestate;
		const plain = JSON.stringify(env);
		if (!traceOmissionReported && validator.acceptRaw(plain)) {
			traceOmissionReported = true;
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_TRACE_OMITTED, {
				attributes: { maxEnvelopeBytes: validator.maxBytes }
			});
		}
		scheduleRelay(channel, plain, topics);
	}

	// Local delivery of one envelope relayed by another instance. The untraced
	// receive path calls it directly; on a Platform with a tracing provider it
	// runs inside a consumer operation parented on the producer's context.
	function deliverEnvelope(parsed, receivedShard) {
		// Coalesced relay: re-coalesce onto local subscribers (latest-value-
		// wins per ws+key), mirroring the publishing instance's sendCoalesced.
		// Precedes the batch/single branches so a coalesced frame is never
		// broadcast.
		if (parsed.coalesced === true) {
			if (!validator.acceptEnvelope(parsed.topic, parsed.event) || String(shardKey(parsed.topic)) !== receivedShard) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc({ topic: mt(parsed.topic) });
			// A platform without sendCoalesced cannot coalesce per key at all, so
			// the value goes out as a plain publish: every subscriber still gets
			// it, as the wire branch below falls back to JSON. Unsequenced, like the
			// coalesced send it stands in for.
			if (typeof activePlatform.sendCoalesced !== 'function' || typeof activePlatform.forEachSubscriber !== 'function') {
				activePlatform.publish(parsed.topic, parsed.event, parsed.data, UNSEQUENCED_DELIVERY);
				return;
			}
			const fullKey = parsed.topic + '\0' + (parsed.coalesceKey == null ? '' : parsed.coalesceKey);
			activePlatform.forEachSubscriber(parsed.topic, (ws) => {
				activePlatform.sendCoalesced(ws, { key: fullKey, topic: parsed.topic, event: parsed.event, data: parsed.data });
			});
			return;
		}
		// Shared binary fan-out relay: a stateless `shared: true` codec
		// published on another instance. Re-derive the codec by capability and
		// re-run publishWire locally so THIS instance does its own native cohort
		// split (its own server-wide wire-id, its own subscribers); relay: false
		// stops a re-relay loop. Falls back to a plain JSON publish when the
		// codec is not registered here (a pure-subscriber instance) - still a
		// correct delivery. Precedes the batch/single branches.
		if (parsed.wire === true) {
			if (typeof parsed.capability !== 'string' || !validator.acceptEnvelope(parsed.topic, parsed.event) || String(shardKey(parsed.topic)) !== receivedShard) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc({ topic: mt(parsed.topic) });
			const codec = wireCodecs.get(parsed.capability);
			if (codec && typeof activePlatform.publishWire === 'function') {
				activePlatform.publishWire(parsed.topic, parsed.event, parsed.data, codec,
					receivedOptions(parsed.s, undefined, parsed.compress === true ? true : undefined));
			} else {
				activePlatform.publish(parsed.topic, parsed.event, parsed.data, receivedOptions(parsed.s, undefined, parsed.compress === true));
			}
			return;
		}
		if (Array.isArray(parsed.batch)) {
			const local = [];
			for (let i = 0; i < parsed.batch.length; i++) {
				const m = parsed.batch[i];
				if (!m || !validator.acceptEnvelope(m.topic, m.event) || String(shardKey(m.topic)) !== receivedShard) {
					mParseErrors?.inc();
					continue;
				}
				mReceived?.inc({ topic: mt(m.topic) });
				local.push({
					topic: m.topic,
					event: m.event,
					data: m.data,
					options: receivedOptions(m.s, m.j, parsed.compress === true ? true : undefined)
				});
			}
			if (local.length === 0) return;
			// A peer's platform batched these; this one may not have the batched
			// lane, and then each message goes out on its own.
			if (typeof activePlatform.publishBatched === 'function') {
				activePlatform.publishBatched(local, parsed.compress === true ? { compress: true } : undefined);
			} else {
				for (let i = 0; i < local.length; i++) {
					activePlatform.publish(local[i].topic, local[i].event, local[i].data, { ...local[i].options, compress: parsed.compress === true });
				}
			}
		} else {
			if (!validator.acceptEnvelope(parsed.topic, parsed.event) || String(shardKey(parsed.topic)) !== receivedShard) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc({ topic: mt(parsed.topic) });
			// Re-apply the de-herd window (`j`) and the origin's seq choice (`s`);
			// `relay: false` stops the re-relay.
			activePlatform.publish(parsed.topic, parsed.event, parsed.data, receivedOptions(parsed.s, parsed.j, parsed.compress));
		}
	}

	function deliverTraced(parsed, receivedShard) {
		const parent = carriedTraceContext(parsed);
		// A null parent means current context to the adapter. Delivery without
		// a carrier must not inherit the subscriber's activation request span.
		if (parent === null) return deliverEnvelope(parsed, receivedShard);
		runTraceOperation(activePlatform, 'redis.sharded-pubsub.receive', 'consumer', parent, REDIS_TRACE_ATTRIBUTES,
			() => deliverEnvelope(parsed, receivedShard));
	}

	// One publish as the bus relays it. The de-herd window (`j`) and the seq
	// choice (`s`: an authoritative number, or `false` for a publish without a
	// seq) ride along when set; the common publish's envelope stays byte-identical.
	function singleEnvelope(topic, event, data, opts) {
		const env = { instanceId, topic, event, data };
		if (opts && typeof opts.jitterMs === 'number' && opts.jitterMs > 0) {
			/** @type {any} */ (env).j = opts.jitterMs;
		}
		const s = opts ? carriedSeq(opts.seq) : undefined;
		if (s !== undefined) /** @type {any} */ (env).s = s;
		if (typeof opts?.compress === 'boolean') /** @type {any} */ (env).compress = opts.compress;
		return env;
	}

	function batchEnvelope(batch, options) {
		const env = { instanceId, batch };
		if (options?.compress === true) /** @type {any} */ (env).compress = true;
		return env;
	}

	const encodeBatch = mBatchRejected ? (env) => {
		try { return JSON.stringify(env); } catch (err) {
			mBatchRejected.inc({ reason: 'serialize' });
			throw err;
		}
	} : JSON.stringify;

	function oversizedBatch(env, traceContext) {
		if (traceContext !== null) {
			delete env.traceparent;
			delete env.tracestate;
			const plain = encodeBatch(env);
			if (plain.length <= safeEnvelopeChars || validator.acceptRaw(plain)) {
				if (!traceOmissionReported) {
					traceOmissionReported = true;
					emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_TRACE_OMITTED, {
						attributes: { maxEnvelopeBytes: validator.maxBytes }
					});
				}
				return plain;
			}
		}
		mBatchRejected?.inc({ reason: 'oversized' });
		throw new RangeError('sharded bus: batched relay envelope exceeds maxEnvelopeBytes');
	}

	function prepareBatch(batch, options, traceContext = null) {
		const env = batchEnvelope(batch, options);
		if (traceContext !== null) injectTraceContext(env, traceContext);
		const msg = encodeBatch(env);
		return msg.length <= safeEnvelopeChars || validator.acceptRaw(msg) ? msg : oversizedBatch(env, traceContext);
	}

	function wireEnvelope(topic, event, data, wire, opts) {
		const env = { instanceId, wire: true, capability: wire.capability, topic, event, data };
		if (opts && opts.compress === true) /** @type {any} */ (env).compress = true;
		const s = opts ? carriedSeq(opts.seq) : undefined;
		if (s !== undefined) /** @type {any} */ (env).s = s;
		return env;
	}

	/** @returns {Map<string, Array<{topic: string, event: string, data: unknown}>>} */
	function batchesByChannel(messages) {
		const byChannel = new Map();
		for (let i = 0; i < messages.length; i++) {
			const mes = messages[i];
			if (mes.options && mes.options.relay === false) continue;
			const ch = channelFor(mes.topic);
			let arr = byChannel.get(ch);
			if (!arr) {
				arr = [];
				byChannel.set(ch, arr);
			}
			arr.push(relayEntry(mes));
		}
		return byChannel;
	}

	function batchTopics(batch) {
		const topics = new Array(batch.length);
		for (let i = 0; i < batch.length; i++) topics[i] = batch[i].topic;
		return topics;
	}

	function flushRelay(batch) {
		if (b) {
			try { b.guard(); } catch {
				if (relayMetrics) relayMetrics.dropped('breaker_open', batch.length, batch.reduce((sum, entry) => sum + entry.count, 0));
				return;
			}
		}
		// Submit one bounded FIFO chunk; the next waits for its actual outcome.
		const commands = new Array(batch.length);
		const commandTopics = new Array(batch.length);
		for (let i = 0; i < batch.length; i++) {
			commands[i] = ['spublish', batch[i].channel, batch[i].msg];
			commandTopics[i] = batch[i].topics;
		}
		const started = relayMetrics?.start();
		const failed = (err) => {
			if (relayMetrics) {
				let messages = 0;
				for (const topics of commandTopics) messages += topics.length;
				relayMetrics.settled(started, commands.length, commands.length, messages);
			}
			b?.failure(err);
		};
		try {
			return execMultiSlot(client.redis, commands).then((results) => {
				let failedEnvelopes = 0, failedMessages = 0;
				for (let c = 0; c < commandTopics.length; c++) {
					const topics = commandTopics[c];
					if (results[c] && results[c][0]) {
						failedEnvelopes++; failedMessages += topics.length;
						continue;
					}
					for (let i = 0; i < topics.length; i++) mRelayed?.inc({ topic: mt(topics[i]) });
				}
				relayMetrics?.settled(started, commands.length, failedEnvelopes, failedMessages);
				const err = firstCommandError(results);
				if (err) b?.failure(err);
				else b?.success();
			}).catch(failed);
		} catch (err) { failed(err); }
	}
	function handleSMessage(ch, message) {
			if (!ch.startsWith(channelPrefix) || !validator.acceptRaw(message)) {
				mParseErrors?.inc();
				return;
			}
			try {
				const parsed = JSON.parse(message);
				// An envelope without a string instanceId cannot be echo-checked or
				// delivered. It is input from a shared transport, not a broken
				// invariant of this package, so it is counted and dropped like any
				// other malformed envelope, as the pubsub bus does.
				if (!(typeof parsed === 'object' && parsed !== null && typeof parsed.instanceId === 'string')) {
					mParseErrors?.inc();
					return;
				}
				// One echo check per envelope; batched envelopes carry
				// one instanceId for the whole batch.
				if (parsed.instanceId === instanceId) {
					mEchoSuppressed?.inc();
					return;
				}
				if (!activePlatform) return;
				// Local delivery runs no application code, so a carried trace
				// context only matters to a tracing provider: with one, delivery is a
				// consumer operation parented on the producer's context; without one
				// the trace fields are ignored and nothing is allocated for them.
				const shard = ch.slice(channelPrefix.length);
				if (activeTracing) deliverTraced(parsed, shard);
				else deliverEnvelope(parsed, shard);
			} catch {
				// Malformed envelope; counted so a stream of bad messages
				// is observable instead of silently swallowed.
				mParseErrors?.inc();
			}
	}

	function subOnError(err) {
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_SUBSCRIBER, { error: err });
		// A slot migration makes the source master drop that slot's shard
		// subscriptions; ioredis surfaces the server-initiated unsubscribe here
		// rather than as a clean event. Mark the slot map stale so the next
		// subscribe refreshes before resolving, and re-establish the dropped
		// subscriptions on their new owner.
		if (isCluster(client.redis)) {
			topologyDirty = true;
			scheduleReconcile();
		}
	}

	// Build (or extend) the per-master subscriber connections and the slot ->
	// owner-node-id map. Refreshed lazily and when a channel's owner is unknown.
	// Single-flight: concurrent callers (a batch subscribe in a dirty window, many
	// unknown-owner resolutions, a retry storm) share one CLUSTER SLOTS refresh
	// instead of each issuing its own.
	function subRefreshTopology() {
		if (!topologyRefresh) {
			// Clear only this refresh: subClose drops a stale one, and a newer
			// refresh may already be in the slot when this one settles.
			const refresh = refreshTopologyNow().finally(() => {
				if (topologyRefresh === refresh) topologyRefresh = null;
			});
			topologyRefresh = refresh;
		}
		return topologyRefresh;
	}

	async function refreshTopologyNow() {
		if (!subOpen) return;
		const generation = subGeneration;
		const masters = client.redis.nodes('master');
		/** @type {unknown} why the first master that did not give its id did not */
		let unread = null;
		// A master that answered, which the slot map is read from: one sent to
		// any node could land on the one that does not.
		/** @type {any} */
		let reader = null;
		await Promise.all(masters.map(async (node) => {
			let id;
			try {
				id = await withSubTimeout(node.cluster('MYID'), 'CLUSTER MYID', 'the master did not answer');
				reader ??= node;
			} catch (err) {
				// The channels on the other masters need nothing from this one;
				// those on its slots find no connection and fail with this error
				// until it answers or the slot map moves them.
				unread ??= err;
				// Not answering, unlike refusing the command, may mean the master
				// moved or left.
				if (!isReplyError(err)) refreshSlotMap();
				return;
			}
			if (generation !== subGeneration) return;
			if (!subByNodeId.has(id)) {
				const s = node.duplicate({ enableReadyCheck: false });
				s.on('error', subOnError);
				s.on('smessage', handleSMessage);
				// ioredis gives cluster node connections no retry strategy and a
				// duplicate inherits it, so this connection never reconnects: one
				// the master drops ends for good.
				s.on('end', () => masterEnded(id, s));
				subByNodeId.set(id, s);
				subIdOf.set(s, id);
			}
		}));
		// No master gave its id: nothing could be subscribed anywhere.
		if (reader === null && unread !== null) throw unread;
		const readSlots = (/** @type {any} */ from) => withSubTimeout(from.cluster('SLOTS'), 'CLUSTER SLOTS', 'the node did not answer');
		let slots;
		try {
			slots = await readSlots(reader ?? client.redis);
		} catch (err) {
			// That node's connection may have closed since it answered; the client
			// sends it on to another.
			if (reader === null) throw err;
			slots = await readSlots(client.redis);
		}
		if (generation !== subGeneration) return;
		// Each entry: [startSlot, endSlot, [ip, port, nodeId], ...replicas].
		slotRanges = slots.map((e) => ({ start: e[0], end: e[1], nodeId: e[2][2] }));
		masterUnread = unread;
		topologyDirty = false;
	}

	// A master connection that ended took its shard channels with it. It is
	// dropped, so the next topology refresh opens a fresh one for the master,
	// and its channels go to the reconciliation, which subscribes each on its
	// slot's current owner with a timeout, a slot-map refresh and a retry (an
	// SSUBSCRIBE for a slot the master no longer owns is never answered, so the
	// old owner is not assumed).
	function masterEnded(id, conn) {
		if (subByNodeId.get(id) !== conn) return;
		subByNodeId.delete(id);
		// The master may have restarted, moved or failed over.
		refreshSlotMap();
		let lost = false;
		for (const [channel, nodeId] of channelNode) {
			if (nodeId !== id) continue;
			channelNode.delete(channel);
			lost = true;
		}
		if (lost) scheduleReconcile();
	}

	// The node id owning a slot, from the cached slot map (null if unknown). The
	// slot is computed client-side (CRC16 of the channel's hash-tag) - identical
	// to CLUSTER KEYSLOT but with no per-channel round trip.
	function ownerIdForSlot(slot) {
		if (!slotRanges) return null;
		for (const r of slotRanges) {
			if (slot >= r.start && slot <= r.end) return r.nodeId;
		}
		return null;
	}

	// ioredis reads the slot map again after MOVED or a failed command, and the
	// bus sends its masters nothing else, so a master that failed over, restarted
	// or moved can stay off ioredis's node list, which the topology refresh reads.
	// Each read opens a connection, so they are at least this far apart.
	function refreshSlotMap() {
		const now = monotonicNow();
		if (now - slotMapReadAt < SHARD_SUBSCRIBE_TIMEOUT_MS) return;
		slotMapReadAt = now;
		const redis = /** @type {any} */ (client.redis);
		if (typeof redis.refreshSlotsCache === 'function') redis.refreshSlotsCache();
	}

	/**
	 * The error for a slot whose owner has no connection here: ioredis does not
	 * list it as a master yet, which a slot map read fixes, or it is a master
	 * that did not give its id, which the error mentions without telling which.
	 */
	function noOwner(slot) {
		refreshSlotMap();
		const unread = /** @type {any} */ (masterUnread);
		return new Error('sharded bus: no owning node for slot ' + slot +
			(unread === null ? '' : ' (a master did not give its id: ' + (unread?.message ?? String(unread)) + ')'));
	}

	// Bound a shard (un)subscribe so a stale-owner hang cannot wedge the caller
	// forever (see SHARD_SUBSCRIBE_TIMEOUT_MS), and CLUSTER MYID so a master that
	// stopped answering cannot hold every topology refresh.
	function withSubTimeout(promise, label, reason = 'stale slot owner') {
		let timer;
		const timeout = new Promise((_, reject) => {
			timer = setTimer(
				() => reject(new Error('sharded bus: ' + label + ' timed out (' + reason + ')')),
				SHARD_SUBSCRIBE_TIMEOUT_MS
			);
			if (timer.unref) timer.unref();
		});
		return Promise.race([promise, timeout]).finally(() => clearTimer(timer));
	}

	// Subscribe one channel on the master that currently owns its slot, recording
	// the owner so a later reshard can be detected and reconciled. A stale cache
	// would resolve the old owner, where ssubscribe hangs, so a timeout triggers
	// one topology refresh + retry on the true owner.
	async function subscribeChannelOnOwner(channel, admissionOwnsCleanup = false) {
		const generation = subGeneration;
		const slot = keySlot(channel);
		// If a reshard was signaled, the cached owner map may be stale and an
		// ssubscribe on the old owner would hang, so refresh before resolving.
		// Each wait below can outlive a close; stop there rather than resolve an
		// owner from a cleared slot map.
		if (topologyDirty) await subRefreshTopology();
		if (generation !== subGeneration) return;
		let ownerId = ownerIdForSlot(slot);
		// No owner in the map, or an owner whose connection ended since the map
		// was read: a refresh resolves the owner and opens its connection. One
		// that was already past this master when its connection ended does not
		// reopen it, so a second one runs.
		for (let attempt = 0; attempt < 2 && (ownerId === null || !subByNodeId.has(ownerId)); attempt++) {
			await subRefreshTopology();
			if (generation !== subGeneration) return;
			ownerId = ownerIdForSlot(slot);
		}
		let conn = ownerId !== null ? subByNodeId.get(ownerId) : null;
		if (!conn) throw noOwner(slot);
		try {
			await withSubTimeout(conn.ssubscribe(channel), 'ssubscribe on slot ' + slot);
		} catch {
			if (generation !== subGeneration) return;
			await subRefreshTopology();
			if (generation !== subGeneration) return;
			ownerId = ownerIdForSlot(slot);
			conn = ownerId !== null ? subByNodeId.get(ownerId) : null;
			if (!conn) throw noOwner(slot);
			await withSubTimeout(conn.ssubscribe(channel), 'ssubscribe on slot ' + slot + ' (retry)');
		}
		// The bus closed while the subscribe was in flight: its connections are
		// already quit.
		if (generation !== subGeneration) return;
		// A reconcile retry can outlive the last unfollow. Shared admissions
		// instead retain every physical subscription until all channels settle:
		// another follower can arrive while a different channel is still pending.
		if (!admissionOwnsCleanup && !channelRefcounts.has(channel)) {
			withSubTimeout(conn.sunsubscribe(channel), 'sunsubscribe on slot ' + slot).catch(() => {});
			return;
		}
		channelNode.set(channel, ownerId);
	}

	// Re-establish every wanted channel on its current owner. A slot migration
	// makes the source master drop that slot's shard subscriptions, so on a
	// subscriber error this refreshes the topology and re-subscribes the channels
	// whose owner moved. Debounced (a reshard fans out errors across connections)
	// and refreshes first, so resolution is never stale. Resolves false when a
	// wanted channel is left unsubscribed, which schedules a retry.
	async function subReconcile() {
		if (!isCluster(client.redis) || !subReady) return true;
		const generation = subGeneration;
		try {
			await subRefreshTopology();
		} catch (err) {
			if (generation !== subGeneration) return true;
			if (!refreshFailing) {
				refreshFailing = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_RECONCILE, { error: err });
			}
			return false;
		}
		const moved = [];
		for (const channel of subscribedChannels) {
			const ownerId = ownerIdForSlot(keySlot(channel));
			// A slot with no owner in the map is retried like a refused one.
			if (ownerId === null || channelNode.get(channel) !== ownerId) moved.push(channel);
		}
		// Re-subscribe the moved channels in parallel for fast recovery; the
		// topology was refreshed above, so none resolves a stale owner. One
		// failure does not abort the rest.
		let failed = 0;
		let first = null;
		await Promise.all(moved.map((channel) =>
			subscribeChannelOnOwner(channel).then(
				() => mResubscribes?.inc(),
				(err) => {
					failed++;
					first ??= { err, channel };
				}
			)
		));
		// A close during the reconciliation ends it; nothing is left to report,
		// and the report latches belong to whatever bus is active now.
		if (generation !== subGeneration) return true;
		if (first === null) {
			refreshFailing = false;
			channelsFailing = false;
			return true;
		}
		if (channelsFailing) return false;
		channelsFailing = true;
		// The channel is built from a topic; cut it should the error repeat it.
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_CHANNEL_RECONCILE, {
			error: first.err,
			redact: [first.channel],
			attributes: { slot: keySlot(first.channel), failed }
		});
		return false;
	}

	function scheduleReconcile(delayMs = 0) {
		const dueAt = monotonicNow() + delayMs;
		if (reconcileTimer !== null) {
			// A subscriber error does not wait out a pending retry's backoff.
			if (dueAt >= reconcileDueAt) return;
			clearTimer(reconcileTimer);
		}
		reconcileDueAt = dueAt;
		reconcileTimer = setTimer(() => {
			reconcileTimer = null;
			const generation = subGeneration;
			subReconcile().then((complete) => {
				// One that outlived a close leaves the retry state alone.
				if (generation !== subGeneration) return;
				if (complete) {
					reconcileRetryMs = 0;
					return;
				}
				if (!subReady) return;
				reconcileRetryMs = reconcileRetryMs === 0
					? RECONCILE_RETRY_FIRST_MS
					: Math.min(reconcileRetryMs * 2, RECONCILE_RETRY_MAX_MS);
				scheduleReconcile(reconcileRetryMs);
			}, (err) => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.SHARDED_RECONCILE, { error: err }));
		}, delayMs);
		if (reconcileTimer.unref) reconcileTimer.unref();
	}

	function watchShardState(generation) {
		if (!subOpen || generation !== subGeneration || shardStateTimer !== null) return;
		function tick() {
			if (!subOpen || generation !== subGeneration) return;
			shardStateTimer = null;
			// Each confirmed channel is visited once, bounded by the topic cap.
			// Snapshot each node's local set once rather than per channel.
			const reported = new Map();
			for (const [id, conn] of subByNodeId) {
				if (conn.status !== 'ready' || !conn.condition) continue;
				const state = conn.condition.subscriber;
				if (state === false) reported.set(id, new Set());
				else if (typeof state?.channels === 'function') reported.set(id, new Set(state.channels('ssubscribe')));
			}
			let lost = false;
			for (const [channel, id] of channelNode) {
				if (!subscribedChannels.has(channel) || !reported.has(id) || reported.get(id).has(channel)) continue;
				channelNode.delete(channel);
				lost = true;
			}
			if (lost) {
				refreshSlotMap();
				scheduleReconcile();
			}
			shardStateTimer = setTimer(tick, SHARD_STATE_CHECK_MS);
			shardStateTimer.unref?.();
		}
		shardStateTimer = setTimer(tick, SHARD_STATE_CHECK_MS);
		shardStateTimer.unref?.();
	}

	async function subEnsure() {
		if (subReady) return;
		subOpen = true;
		const generation = subGeneration;
		if (isCluster(client.redis)) {
			if (subLiveness === null) subLiveness = watchLiveness('sharded-pubsub', () => subByNodeId.values(), closeConnection, { identify: (s) => subIdOf.get(s) });
			await subRefreshTopology();
			watchShardState(generation);
		} else {
			subStandalone = openSubscriber(client, 'sharded-pubsub');
			subStandalone.on('error', subOnError);
			subStandalone.on('smessage', handleSMessage);
		}
		subReady = true;
	}

	async function subSubscribe(channels) {
		if (channels.length === 0) return;
		if (!isCluster(client.redis)) {
			await subStandalone.ssubscribe(...channels);
			return;
		}
		// On a cluster, SSUBSCRIBE requires every channel in one call to share a
		// slot (same owning node is not enough), and node.ssubscribe only resolves
		// on the slot's owning master (Cluster.ssubscribe hangs), so subscribe each
		// channel individually on its owner.
		const results = await Promise.allSettled(channels.map((ch) => subscribeChannelOnOwner(ch, true)));
		const failed = results.find((result) => result.status === 'rejected');
		if (failed) throw failed.reason;
	}

	async function subUnsubscribe(channel) {
		if (!isCluster(client.redis)) {
			if (subStandalone) await subStandalone.sunsubscribe(channel);
			return;
		}
		const ownerId = channelNode.get(channel);
		channelNode.delete(channel);
		const conn = ownerId !== undefined ? subByNodeId.get(ownerId) : null;
		if (!conn) return;
		// Best-effort: a channel whose slot already migrated is gone from this
		// node, and sunsubscribe on a non-owner would hang, so cap and ignore.
		await withSubTimeout(conn.sunsubscribe(channel), 'sunsubscribe on slot ' + keySlot(channel)).catch(() => {});
	}

	async function subClose() {
		subOpen = false;
		subGeneration++;
		if (shardStateTimer !== null) { clearTimer(shardStateTimer); shardStateTimer = null; }
		topologyRefresh = null;
		if (reconcileTimer !== null) {
			clearTimer(reconcileTimer);
			reconcileTimer = null;
		}
		if (subLiveness !== null) {
			subLiveness.stop();
			subLiveness = null;
		}
		refreshFailing = false;
		channelsFailing = false;
		reconcileRetryMs = 0;
		const all = isCluster(client.redis)
			? [...subByNodeId.values()]
			: (subStandalone ? [subStandalone] : []);
		subByNodeId.clear();
		channelNode.clear();
		masterUnread = null;
		subStandalone = null;
		slotRanges = null;
		subReady = false;
		// Closed at once: a QUIT waits forever on a master that stopped answering.
		for (const s of all) closeConnection(s);
	}

	async function activate(platform) {
		activePlatform = platform;
		activeTracing = traceEnabled(platform);
		if (active) return;

		b?.guard();
		let info;
		try {
			info = await client.redis.info('server');
		} catch (err) {
			b?.failure(err);
			throw err;
		}
		const major = parseRedisVersion(info);
		if (major === null) {
			b?.failure(new Error('could not parse Redis version'));
			throw new Error('sharded bus: could not parse Redis version from INFO server');
		}
		if (major < 7) {
			b?.failure(new Error(`Redis ${major} too old`));
			throw new Error(`sharded bus: requires Redis 7 or newer (server reports ${major}). Use createPubSubBus for older servers.`);
		}
		b?.success();

		try {
			await subEnsure();
		} catch (err) {
			// On a cluster subEnsure opens a subscriber per master before it reads
			// the slot map, so a CLUSTER SLOTS failure leaves those connections
			// open. deactivate() does nothing for a bus that never became active,
			// so close them here.
			b?.failure(err);
			activePlatform = null;
			activeTracing = false;
			await subClose();
			throw err;
		}
		active = true;
	}

	async function deactivate() {
		relayQueue.clear();
		if (!active) return;
		active = false;
		activePlatform = null;
		subscribedChannels.clear();
		await subClose();
		followCounts.clear();
		channelRefcounts.clear();
		pendingChannels.clear();
		pendingTopics.clear();
	}

	// Reserve the shared promise before starting I/O, so every caller waits for
	// the same admission even when it follows an already-reserved topic.
	function admitChannels(channels) {
		const waits = new Set();
		const fresh = [];
		for (const channel of new Set(channels)) {
			const pending = pendingChannels.get(channel);
			if (pending) waits.add(pending);
			else if (!subscribedChannels.has(channel)) fresh.push(channel);
		}
		if (fresh.length) {
			let resolve, reject;
			const admission = new Promise((yes, no) => { resolve = yes; reject = no; });
			for (const channel of fresh) pendingChannels.set(channel, admission);
			waits.add(admission);
			const generation = subGeneration;
			(async () => {
				let cancel;
				try {
					cancel = guardBreaker(b);
					await subSubscribe(fresh);
					if (generation !== subGeneration) throw new Error('sharded bus: closed during follow');
					let needsCleanup = false;
					for (const channel of fresh) {
						if (channelRefcounts.has(channel)) subscribedChannels.add(channel);
						else needsCleanup = true;
					}
					if (needsCleanup) {
						// Ownership can change during cleanup I/O. Keep all these
						// channels pending until physical subscriptions and reservations
						// agree, then publish flags and release admission without yielding.
						for (const channel of fresh) subscribedChannels.delete(channel);
						const active = new Set(fresh);
						for (;;) {
							const release = fresh.filter(channel => active.has(channel) && !channelRefcounts.has(channel));
							const restore = fresh.filter(channel => !active.has(channel) && channelRefcounts.has(channel));
							if (release.length === 0 && restore.length === 0) break;
							if (release.length) {
								for (const channel of release) {
									await subUnsubscribe(channel);
									active.delete(channel);
									if (generation !== subGeneration) throw new Error('sharded bus: closed during follow');
								}
							}
							if (generation !== subGeneration) throw new Error('sharded bus: closed during follow');
							if (restore.length) {
								await subSubscribe(restore);
								for (const channel of restore) active.add(channel);
							}
							if (generation !== subGeneration) throw new Error('sharded bus: closed during follow');
						}
						for (const channel of active) subscribedChannels.add(channel);
					}
					b?.success();
					mFollows?.inc(fresh.length);
				} catch (err) {
					if (generation !== subGeneration) cancel?.();
					else {
						if (cancel) b?.failure(err);
						for (const channel of fresh) subscribedChannels.delete(channel);
						// A multi-channel admission can succeed on some cluster nodes
						// before another refuses it. Release those partial subscriptions.
						await Promise.allSettled(fresh.map(subUnsubscribe));
					}
					throw err;
				} finally {
					for (const channel of fresh) {
						if (pendingChannels.get(channel) === admission) pendingChannels.delete(channel);
					}
				}
			})().then(resolve, reject);
		}
		return waits.size ? Promise.all(waits) : null;
	}

	async function follow(topic) {
		if (!subReady) throw new Error('sharded bus: activate() must be called before follow()');
		const count = followCounts.get(topic) || 0;
		if (count === 0 && followCounts.size >= MAX_SHARDED_BUS_TOPICS) {
			throw new Error('sharded bus: distinct topic count exceeded ' + MAX_SHARDED_BUS_TOPICS + ' on this instance');
		}
		followCounts.set(topic, count + 1);
		if (count > 0 && (pendingTopics.size === 0 || !pendingTopics.has(topic))) return;
		const channel = channelFor(topic);
		if (count === 0) channelRefcounts.set(channel, (channelRefcounts.get(channel) || 0) + 1);
		if (subscribedChannels.has(channel) && !pendingChannels.has(channel)) return;
		pendingTopics.set(topic, (pendingTopics.get(topic) || 0) + 1);
		const generation = subGeneration;
		try {
			await admitChannels([channel]);
		} catch (err) {
			if (generation === subGeneration) await unfollow(topic);
			throw err;
		} finally {
			if (generation === subGeneration) releasePendingTopic(topic);
		}
	}

	function releasePendingTopic(topic) {
		const count = pendingTopics.get(topic) || 0;
		if (count <= 1) pendingTopics.delete(topic);
		else pendingTopics.set(topic, count - 1);
	}

	/**
	 * Bulk follow. Groups input topics by shard channel, runs one
	 * SSUBSCRIBE round trip for any channel transitioning from refcount 0
	 * to active. Refcount semantics for individual topics match `follow`:
	 * each call to `followBatch` bumps every input topic's refcount by 1,
	 * and only the channel transitions trigger Redis traffic.
	 *
	 * Pairs with the adapter's `subscribeBatch` hook so an N-topic
	 * subscribe batch turns into one round-trip-per-channel rather than
	 * one round-trip-per-topic. With the adapter's next.7 client-side
	 * coalescing, the win covers initial-mount subscribes too, not just
	 * reconnect resubscribes.
	 *
	 * Empty arrays no-op. Duplicate topics in the input collapse to one
	 * refcount bump.
	 *
	 * @param {string[]} topics
	 */
	async function followBatch(topics) {
		if (!subReady) {
			throw new Error('sharded bus: activate() must be called before followBatch()');
		}
		if (!Array.isArray(topics)) {
			throw new TypeError('sharded bus: followBatch requires an array of topics');
		}
		if (topics.length === 0) return;

		const touchedTopics = new Set();
		const waitingTopics = [];
		const channels = new Set();
		for (const topic of topics) {
			if (typeof topic !== 'string' || topic.length === 0 || touchedTopics.has(topic)) continue;
			const count = followCounts.get(topic) || 0;
			if (count === 0 && followCounts.size >= MAX_SHARDED_BUS_TOPICS) continue;
			touchedTopics.add(topic);
			followCounts.set(topic, count + 1);
			if (count > 0 && (pendingTopics.size === 0 || !pendingTopics.has(topic))) continue;
			waitingTopics.push(topic);
			pendingTopics.set(topic, (pendingTopics.get(topic) || 0) + 1);
			const channel = channelFor(topic);
			channels.add(channel);
			if (count === 0) channelRefcounts.set(channel, (channelRefcounts.get(channel) || 0) + 1);
		}
		if (channels.size === 0) return;
		const generation = subGeneration;
		try {
			const admission = admitChannels(channels);
			if (admission) await admission;
		} catch (err) {
			// Each caller releases exactly its own reservation. A channel remains
			// owned until the final topic's final reservation is released.
			if (generation === subGeneration) await Promise.all([...touchedTopics].map(unfollow));
			throw err;
		} finally {
			if (generation === subGeneration) for (const topic of waitingTopics) releasePendingTopic(topic);
		}
	}

	async function unfollow(topic) {
		const count = followCounts.get(topic) || 0;
		if (count === 0) return;
		const newCount = count - 1;
		if (newCount > 0) {
			followCounts.set(topic, newCount);
			return;
		}
		followCounts.delete(topic);

		const channel = channelFor(topic);
		const chCount = channelRefcounts.get(channel) || 0;
		const newChCount = chCount - 1;
		if (newChCount > 0) {
			channelRefcounts.set(channel, newChCount);
			return;
		}
		channelRefcounts.delete(channel);

		if (subReady && subscribedChannels.has(channel)) {
			subscribedChannels.delete(channel);
			try {
				await subUnsubscribe(channel);
				mUnfollows?.inc();
			} catch {
				// SUNSUBSCRIBE failure is non-fatal - the connection
				// will be torn down on deactivate anyway.
			}
		}
	}

	/**
	 * Snapshot every topic this bus is currently following with its
	 * local subscriber count from the supplied platform. Use as the
	 * `subjects` callback on `createPublishRateAggregator` so the
	 * aggregator can broadcast subscriber counts cluster-wide:
	 *
	 *     const bus = createShardedBus(client);
	 *     const aggregator = createPublishRateAggregator(client, {
	 *       subjects: () => bus.localSubjects(platform)
	 *     });
	 *
	 * Topics with 0 local subscribers are omitted - they have nothing
	 * to contribute. Topics subscribed outside the bus's hooks (raw
	 * `ws.subscribe` bypass) are not enumerated; they will not propagate
	 * cluster-wide via this path.
	 *
	 * @param {import('svelte-adapter-uws').Platform} platform
	 * @returns {Array<{topic: string, count: number}>}
	 */
	function localSubjects(platform) {
		if (!platform || typeof platform.subscribers !== 'function') return [];
		const out = [];
		for (const topic of followCounts.keys()) {
			const count = platform.subscribers(topic) | 0;
			if (count > 0) out.push({ topic, count });
		}
		return out;
	}

	const bus = {
		wrap(platform) {
			// The relaying methods below are the untraced relay: they read no
			// trace context and allocate nothing for tracing. When the Platform
			// has a tracing provider, the traced versions defined here replace
			// them on the returned object (see the end of wrap), and each call
			// becomes one producer operation, parented on the Platform's live
			// context, around the local delivery and the relay; the envelope
			// carries the operation's own context. The adapter configures its
			// provider at build time, so whether a Platform traces is fixed for
			// its lifetime and the choice is made once, here.
			const traced = (fn) => runTraceOperation(platform, 'redis.sharded-pubsub.publish', 'producer',
				liveTraceContext(platform), REDIS_TRACE_ATTRIBUTES, fn);
			const publishTraced = (topic, event, data, opts) => {
				if (opts && opts.relay === false) return platform.publish(topic, event, data, opts);
				return traced((traceContext) => {
					const result = platform.publish(topic, event, data, relayedLocally(opts, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					relayTraced(channelFor(topic), singleEnvelope(topic, event, data, opts), [topic], traceContext);
					return result;
				});
			};
			// A batch with nothing to relay opens no operation, like a publish
			// with relay: false. A non-array or a null entry counts as relaying, so
			// it takes the traced path and the platform rejects it there exactly
			// as it does untraced.
			const relaysAny = (messages) => {
				if (!Array.isArray(messages)) return true;
				for (let i = 0; i < messages.length; i++) {
					const m = messages[i];
					if (!m || !m.options || m.options.relay !== false) return true;
				}
				return false;
			};
			const batchTraced = (messages) => (!relaysAny(messages) ? platform.batch(messages) : traced((traceContext) => {
				const results = platform.batch(relayedBatchLocally(messages, entryReachesSiblings));
				for (let i = 0; i < messages.length; i++) {
					const mes = messages[i];
					if (!mes.options || mes.options.relay !== false) {
						relayTraced(channelFor(mes.topic), singleEnvelope(mes.topic, mes.event, mes.data, mes.options), [mes.topic], traceContext);
					}
				}
				return results;
			}));
			const publishBatchedTraced = (messages, options) => {
				if (!Array.isArray(messages)) return;
				if (messages.length === 0) return;
				messages = collapseByCoalesceKey(messages);
				if (!relaysAny(messages)) { platform.publishBatched(messages, options); return; }
				traced((traceContext) => {
					// Preserve the platform's malformed-entry error without touching shard routing.
					for (let i = 0; i < messages.length; i++) {
						if (!messages[i]) return platform.publishBatched(messages, options);
					}
					const byChannel = batchesByChannel(messages);
					// Keep channel, checked bytes and topics together until every channel passes.
					const prepared = new Array(byChannel.size * 3);
					let index = 0;
					for (const [channel, batch] of byChannel) {
						prepared[index++] = channel;
						prepared[index++] = prepareBatch(batch, options, traceContext);
						prepared[index++] = batchTopics(batch);
					}
					platform.publishBatched(relayedBatchLocally(messages, entryReachesSiblings), options);
					for (let i = 0; i < prepared.length; i += 3) scheduleRelay(prepared[i], prepared[i + 1], prepared[i + 2], true);
				});
			};
			const relayCoalescedTraced = (topic, event, data, coalesceKey) => traced((traceContext) =>
				relayTraced(channelFor(topic), { instanceId, coalesced: true, topic, event, data, coalesceKey }, [topic], traceContext));
			const publishWireTraced = (topic, event, data, wire, opts) => {
				if (!(wire && wire.shared === true && (!opts || opts.relay !== false))) {
					return platform.publishWire(topic, event, data, wire, opts);
				}
				return traced((traceContext) => {
					const result = platform.publishWire(topic, event, data, wire, relayedLocally(opts, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					relayTraced(channelFor(topic), wireEnvelope(topic, event, data, wire, opts), [topic], traceContext);
					return result;
				});
			};
			// Per-event loop. Mirrors `platform.batch` semantics: N submitted
			// messages produce N wire frames per subscriber. Use
			// `publishBatched` instead when you want one wire frame per
			// subscriber per call.
			const batchUntraced = (messages) => {
				const results = platform.batch(relayedBatchLocally(messages, entryReachesSiblings));
				for (let i = 0; i < messages.length; i++) {
					const mes = messages[i];
					if (!mes.options || mes.options.relay !== false) {
						const channel = channelFor(mes.topic);
						scheduleRelay(channel, JSON.stringify(singleEnvelope(mes.topic, mes.event, mes.data, mes.options)), [mes.topic]);
					}
				}
				return results;
			};
			// Wire-batched publish. One SPUBLISH envelope per shard channel
			// per call. Receivers fan out via local `platform.publishBatched`
			// for one wire frame per subscriber. Empty arrays no-op.
			const publishBatchedUntraced = (messages, options) => {
				if (!Array.isArray(messages)) return;
				if (messages.length === 0) return;
				messages = collapseByCoalesceKey(messages);
				for (let i = 0; i < messages.length; i++) {
					if (!messages[i]) return platform.publishBatched(messages, options);
				}
				const byChannel = batchesByChannel(messages);
				// Keep channel, checked bytes and topics together until every channel passes.
				const prepared = new Array(byChannel.size * 3);
				let index = 0;
				for (const [channel, batch] of byChannel) {
					prepared[index++] = channel;
					prepared[index++] = prepareBatch(batch, options);
					prepared[index++] = batchTopics(batch);
				}
				platform.publishBatched(relayedBatchLocally(messages, entryReachesSiblings), options);
				for (let i = 0; i < prepared.length; i += 3) scheduleRelay(prepared[i], prepared[i + 1], prepared[i + 2], true);
			};
			const tracing = traceEnabled(platform);
			const batch = tracing ? batchTraced : batchUntraced;
			const publishBatched = tracing ? publishBatchedTraced : publishBatchedUntraced;
			// The subscribe-ack epoch must carry the SAME generation domain
			// the resume hook later compares against. When a replay tracker
			// is wired on the source platform it owns the per-topic seq space,
			// so its cached per-topic generation is the authority - route the
			// ack to it. Without this bridge the ack would carry the source
			// platform's single-worker process generation while the resume
			// hook reads the tracker's small-integer generation, mismatching
			// on every resume and forcing a cold re-read where gap-fill should
			// apply. A deployment may still override the source platform's
			// topicEpoch directly; that path is honoured when no cachedEpoch
			// tracker is present. Read live so post-wrap wiring propagates.
			const topicEpoch = (t) => {
				const r = platform.replay;
				if (r && typeof r.cachedEpoch === 'function') return r.cachedEpoch(t);
				return platform.topicEpoch(t);
			};
			// A method the platform does not have is undefined on the wrap too, so a
			// caller's feature check (`if (!platform.batch)`) gives the same answer
			// through it. Which methods are there is decided here, when wrap() runs;
			// batch, publishBatched and topicEpoch read the platform's member (and
			// topicEpoch its replay tracker) when called.
			const wrapped = {
				publish(topic, event, data, opts) {
					if (opts && opts.relay === false) return platform.publish(topic, event, data, opts);
					// The bus is this publish's fan-out to every other worker and
					// instance, so a multi-worker adapter's in-process relay stays off.
					const result = platform.publish(topic, event, data, relayedLocally(opts, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					scheduleRelay(channelFor(topic), JSON.stringify(singleEnvelope(topic, event, data, opts)), [topic]);
					return result;
				},
				batch: typeof platform.batch === 'function' ? batch : undefined,
				publishBatched: typeof platform.publishBatched === 'function' ? publishBatched : undefined,
				send: typeof platform.send === 'function' ? platform.send.bind(platform) : undefined,
				sendTo: typeof platform.sendTo === 'function' ? platform.sendTo.bind(platform) : undefined,
				// Relay-only coalesce (mirrors the pubsub bus): the local sendCoalesced
				// fan-out already ran on the publishing instance; this carries the
				// latest value to OTHER instances on the topic's shard channel, where
				// each re-coalesces onto its own subscribers. Echo-suppressed by
				// instanceId.
				relayCoalesced(topic, event, data, coalesceKey) {
					const channel = channelFor(topic);
					scheduleRelay(channel, JSON.stringify({ instanceId, coalesced: true, topic, event, data, coalesceKey }), [topic]);
				},
				sendCoalesced: typeof platform.sendCoalesced === 'function' ? platform.sendCoalesced.bind(platform) : undefined,
				request: typeof platform.request === 'function' ? platform.request.bind(platform) : undefined,
				// Single-instance topic broadcast-with-reply, forwarded as-is so the
				// realtime layer can serve THIS instance's subscribers; the cluster
				// fan-out rides the `topicBroadcast` coordinator (getter below).
				requestTopic: typeof platform.requestTopic === 'function' ? platform.requestTopic.bind(platform) : undefined,
				// Binary wire methods. Forwarded for local fan-out. A STATELESS
				// `shared: true` codec additionally relays its logical event across the
				// cluster on the topic's shard channel: the byte-identical 0x03 frame's
				// only per-connection-varying byte is a per-process server-wide wire-id,
				// so the frame and id never cross the bus - each receiving instance
				// re-derives the codec by capability and runs its OWN native cohort split
				// with its own id and subscribers (mirrors the adapter's worker-thread
				// relay). Stateful codecs (cursor/presence) carry `shared` falsy and drive
				// their own per-plugin relay, so they never double-relay here. `undefined`
				// when the underlying platform predates per-frame binary wire, so the
				// plugins' typeof guard degrades gracefully.
				publishWire: typeof platform.publishWire === 'function' ? (topic, event, data, wire, opts) => {
					if (!(wire && wire.shared === true && (!opts || opts.relay !== false))) {
						return platform.publishWire(topic, event, data, wire, opts);
					}
					const result = platform.publishWire(topic, event, data, wire, relayedLocally(opts, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					// excludeWs is NOT carried: the excluded socket is local to this
					// origin instance; other instances have no such socket and fan out
					// fully (the adapter relays on its excludeWs walk path too).
					scheduleRelay(channelFor(topic), JSON.stringify(wireEnvelope(topic, event, data, wire, opts)), [topic]);
					return result;
				} : undefined,
				sendWire: typeof platform.sendWire === 'function' ? platform.sendWire.bind(platform) : undefined,
				// Register a shared/stateful codec by capability on THIS instance.
				// Stores the codec locally (so an inbound shared relay can re-encode
				// binary without the codec crossing the bus) AND forwards to the
				// underlying platform so the adapter's intra-process worker-thread relay
				// registry is populated. A shared-codec author must register at activate
				// on every instance for a pure-subscriber instance to re-encode binary
				// (an unregistered instance degrades a shared relay to JSON, which is
				// still a correct delivery). Version-gated like publishWire.
				registerWireCodec: typeof platform.registerWireCodec === 'function' ? (wire) => {
					if (wire && typeof wire.capability === 'string') wireCodecs.set(wire.capability, wire);
					platform.registerWireCodec(wire);
				} : undefined,
				get connections() { return platform.connections; },
				get requestId() { return platform.requestId; },
				get traceContext() { return platform.traceContext ?? null; },
				get trace() { return platform.trace; },
				get pressure() { return platform.pressure; },
				// Forward the live protection posture through the wrapped seam so
				// cluster-side admission code (per-IP upgrade buckets, capability
				// cookies) reads the same level as the source platform. Live getter:
				// a posture transition after wrap propagates. The `?? 'normal'`
				// fallback degrades gracefully when the underlying platform predates
				// the posture property, mirroring the closedWsAborts `?? 0` guard.
				get protection() { return platform.protection ?? 'normal'; },
				// Forward the injectable clock and RNG the adapter projects onto its
				// Platform from the runtime module. Cluster-side per-message handlers
				// read these off the wrapped seam (the caller's reference IS the wrap
				// result), so a seeded harness clock / RNG must propagate through.
				// Forward the live reference - the projected `now` / `monotonic`
				// functions and the `random` object - rather than rebinding, so a
				// wrapped platform exposes the same source the adapter installed. The
				// `?? <native>` fallback degrades gracefully when the underlying
				// platform predates the projection (the peer-dependency floor admits
				// such adapters): a cluster handler still gets a working clock / RNG
				// from this package's own runtime seam instead of reading `undefined`,
				// and that seam stays seedable in a simulation harness. Same
				// live-getter + nullish-fallback contract the protection forwarder uses.
				get now() { return platform.now ?? now; },
				get monotonic() { return platform.monotonic ?? monotonicNow; },
				get random() { return platform.random ?? fallbackRandom; },
				// Forward the hybrid logical clock the adapter projects onto its
				// Platform. Cluster-side handlers that causally stamp an event read
				// it off the wrapped platform, so the reference must propagate live;
				// the fallback is a process-local HLC of the same shape for adapters
				// that predate the projection.
				get hlc() { return platform.hlc ?? fallbackHlc; },
				onPressure: typeof platform.onPressure === 'function' ? platform.onPressure.bind(platform) : undefined,
				onPublishRate: typeof platform.onPublishRate === 'function' ? platform.onPublishRate.bind(platform) : undefined,
				subscribers: typeof platform.subscribers === 'function' ? platform.subscribers.bind(platform) : undefined,
				forEachSubscriber: typeof platform.forEachSubscriber === 'function' ? platform.forEachSubscriber.bind(platform) : undefined,
				subscribe: typeof platform.subscribe === 'function' ? platform.subscribe.bind(platform) : undefined,
				unsubscribe: typeof platform.unsubscribe === 'function' ? platform.unsubscribe.bind(platform) : undefined,
				checkSubscribe: typeof platform.checkSubscribe === 'function' ? platform.checkSubscribe.bind(platform) : undefined,
				// Forwarded so a framework arming wire-subscribe authorization at init
				// reaches the real adapter flag through the wrapped platform. Version-
				// gated: an older adapter without it degrades to a no-op.
				authorizeWireSubscribe: typeof platform.authorizeWireSubscribe === 'function' ? platform.authorizeWireSubscribe.bind(platform) : undefined,
				get maxPayloadLength() { return platform.maxPayloadLength; },
				bufferedAmount: typeof platform.bufferedAmount === 'function' ? platform.bufferedAmount.bind(platform) : undefined,
				get closedWsAborts() { return platform.closedWsAborts ?? 0; },
				// There when a replay tracker or the platform can answer it.
				topicEpoch: (platform.replay && typeof platform.replay.cachedEpoch === 'function') || typeof platform.topicEpoch === 'function' ? topicEpoch : undefined,
				// Framework conventions stashed on the source platform by
				// app init code (e.g. `platform.replay = createReplay(...)`,
				// `platform.redis = ioredisClient`) must survive the wrap so
				// downstream framework auto-routing can discover them on the
				// wrapped seam too. Live getters so post-wrap reassignment
				// propagates.
				get replay() { return platform.replay; },
				get redis() { return platform.redis; },
				get presence() { return platform.presence; },
				get crdt() { return platform.crdt; },
				get smooth() { return platform.smooth; },
				// Clock-fence convention (attachClockFence); live getter so
				// post-wrap attachment propagates.
				get clockFence() { return platform.clockFence; },
				// Cluster-clock convention (attachClusterClock); live getter so
				// post-wrap attachment propagates.
				get clusterClock() { return platform.clusterClock; },
				// The topic-broadcast cluster coordinator; live getter so realtime
				// detects it on the wrapped seam to fan live.push({ topic }) cluster-wide.
				get topicBroadcast() { return platform.topicBroadcast; },
				// The adapter's scoped helpers, published through this wrap so they
				// relay: each forwards its options, the counters default to 1, and
				// each returns the publish result.
				topic(t) {
					return {
						publish: (event, data, opts) => wrapped.publish(t, event, data, opts),
						created: (data, opts) => wrapped.publish(t, 'created', data, opts),
						updated: (data, opts) => wrapped.publish(t, 'updated', data, opts),
						deleted: (data, opts) => wrapped.publish(t, 'deleted', data, opts),
						set: (value, opts) => wrapped.publish(t, 'set', value, opts),
						increment: (amount = 1, opts) => wrapped.publish(t, 'increment', amount, opts),
						decrement: (amount = 1, opts) => wrapped.publish(t, 'decrement', amount, opts)
					};
				}
			};
			if (tracing) {
				wrapped.publish = publishTraced;
				wrapped.relayCoalesced = relayCoalescedTraced;
				if (wrapped.publishWire) wrapped.publishWire = publishWireTraced;
			}
			return wrapped;
		},
		activate,
		deactivate,
		follow,
		followBatch,
		unfollow,
		localSubjects,
		/**
		 * Cluster-wide subscriber count for a topic. Returns the local
		 * count (`platform.subscribers(topic)`) plus the remote sum from
		 * the wired aggregator's `subscribersOf(topic)`. When no
		 * aggregator is wired, returns the local count only.
		 *
		 * Eventually-consistent within the aggregator's `publishInterval`
		 * for the remote contribution; the local read is always live.
		 *
		 * @param {string} topic
		 * @returns {number}
		 */
		subscribers(topic) {
			const local = activePlatform ? activePlatform.subscribers(topic) | 0 : 0;
			if (!subscribersAggregator) return local;
			// Aggregator's subscribersOf already includes self (via its
			// own `subjects()` callback wired to bus.localSubjects).
			// Trust it as the cluster total.
			return subscribersAggregator.subscribersOf(topic);
		},
		hooks: {
			async subscribe(ws, topic) {
				if (topic.startsWith('__')) return;
				let topics = wsFollows.get(ws);
				if (!topics) {
					topics = new Set();
					wsFollows.set(ws, topics);
				}
				if (topics.has(topic)) return;
				topics.add(topic);
				try {
					await follow(topic);
				} catch (err) {
					topics.delete(topic);
					if (topics.size === 0 && wsFollows.get(ws) === topics) wsFollows.delete(ws);
					throw err;
				}
			},
			async subscribeBatch(ws, topics) {
				if (!Array.isArray(topics) || topics.length === 0) return;
				let wsTopics = wsFollows.get(ws);
				if (!wsTopics) {
					wsTopics = new Set();
					wsFollows.set(ws, wsTopics);
				}
				const fresh = [];
				for (const topic of topics) {
					if (typeof topic !== 'string' || topic.startsWith('__')) continue;
					if (wsTopics.has(topic)) continue;
					wsTopics.add(topic);
					fresh.push(topic);
				}
				if (fresh.length === 0) return;
				try {
					await followBatch(fresh);
				} catch (err) {
					for (const topic of fresh) wsTopics.delete(topic);
					if (wsTopics.size === 0 && wsFollows.get(ws) === wsTopics) wsFollows.delete(ws);
					throw err;
				}
			},
			async unsubscribe(ws, topic) {
				const topics = wsFollows.get(ws);
				if (!topics || !topics.has(topic)) return;
				topics.delete(topic);
				if (topics.size === 0) wsFollows.delete(ws);
				await unfollow(topic);
			},
			async close(ws) {
				const topics = wsFollows.get(ws);
				if (!topics) return;
				wsFollows.delete(ws);
				const pending = [];
				for (const t of topics) pending.push(unfollow(t));
				await Promise.all(pending);
			}
		}
	};
	return bus;
}
