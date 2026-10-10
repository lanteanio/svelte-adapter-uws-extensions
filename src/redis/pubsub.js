/**
 * Redis pub/sub bus for svelte-adapter-uws.
 *
 * Distributes WebSocket publishes across multiple server instances via Redis.
 * Each instance publishes to Redis AND locally. Incoming Redis messages are
 * forwarded to the local platform.publish() with a flag to prevent re-publishing
 * back to Redis (relay loop prevention).
 *
 * @module svelte-adapter-uws-extensions/redis/pubsub
 */

import { assert } from '../shared/assert.js';
import { randomBytes, now, monotonicNow } from '../shared/runtime.js';
import { fallbackRandom, fallbackHlc } from '../shared/platform-fallback.js';
import { createRelayQueue } from './_relay-queue.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { createRelayMetrics } from './_relay-metrics.js';
import { firstCommandError, isCluster } from '../shared/cluster.js';
import {
	REDIS_TRACE_ATTRIBUTES,
	injectTraceContext,
	carriedTraceContext,
	liveTraceContext,
	runTraceOperation,
	traceEnabled
} from '../shared/trace-context.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';
import { openSubscriber } from '../shared/redis-subscriber.js';
import { MULTI_WORKER_RUNTIME, UNSEQUENCED_DELIVERY, carriedSeq, receivedOptions, relayedLocally, relayedBatchLocally, relayEntry } from '../shared/sequence-authority.js';

// The degraded / recovered frames report this worker's own breaker, so they go
// to this worker's subscribers only, and they make no monotonic promise.
const SYSTEM_FRAME = Object.freeze({ relay: false, seq: false });

// The subscribe denial reasons the adapter declares (its SubscribeDenialReason
// type) and INTERNAL_ERROR, its answer when a subscribe hook throws. Any
// other reason is text the application's hook wrote and is not logged.
const ADAPTER_DENIAL_CODES = new Set(['UNAUTHENTICATED', 'FORBIDDEN', 'INVALID_TOPIC', 'RATE_LIMITED', 'INTERNAL_ERROR']);

/**
 * @typedef {Object} PubSubBusOptions
 * @property {string} [channel='uws:pubsub'] - Redis channel name for pub/sub messages
 * @property {string | null | false} [systemChannel='__realtime'] - Topic used for auto-emitted `degraded` / `recovered` events on the local platform. Set to `null` or `false` to disable auto-emission.
 * @property {() => void} [onDegraded] - Called once when the breaker leaves the healthy state. Requires a `breaker` to be passed.
 * @property {() => void} [onRecovered] - Called once when the breaker returns to the healthy state. Requires a `breaker` to be passed.
 * @property {number} [maxEnvelopeBytes=1048576] - Bound inbound JSON and outbound relay bytes. Oversized publishBatched calls throw RangeError before delivery; other oversized relays are withheld and reported while local delivery remains intact.
 * @property {boolean} [allowSystemTopics=false] - Default false: drop inbound envelopes whose topic starts with `__` apart from this bus's own `systemChannel`. Defense against bus-side topic injection on shared-Redis deployments where a foreign publisher could otherwise inject forged `__signal:*` / `__rpc` / plugin-internal frames into the local platform. Apps that legitimately bus-relay user-defined `__`-prefixed topics (rare) can opt back in via `allowSystemTopics: true`.
 */

/**
 * @typedef {Object} PubSubBusHooks
 * @property {(ws: import('svelte-adapter-uws').WS, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void>} open -
 *   Drop-in `hooks.ws.open` that subscribes `ws` to the bus's `systemChannel`
 *   (so `degraded` / `recovered` events reach this connection) and activates
 *   the Redis subscriber. Both calls are idempotent: per-connection subscribe
 *   is a no-op on repeat; activate early-outs after the first call. When
 *   `systemChannel` is `null` or `false`, only the activate step runs.
 */

/**
 * @typedef {Object} PubSubBus
 * @property {(platform: import('svelte-adapter-uws').Platform) => import('svelte-adapter-uws').Platform} wrap -
 *   Returns a new Platform whose publish() sends to Redis + local.
 *   Use this wrapped platform everywhere you call publish().
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate -
 *   Start the Redis subscriber. Incoming messages are forwarded to the
 *   original platform.publish(). Call this once at startup (e.g. in your open hook).
 * @property {() => Promise<void>} deactivate -
 *   Stop the Redis subscriber and clean up.
 * @property {PubSubBusHooks} hooks -
 *   Ready-made hooks for `hooks.ws.js`. Destructure `const { open } = bus.hooks`
 *   for a one-line wiring that activates the bus AND puts every connection in
 *   `systemChannel`'s subscriber set so `degraded` / `recovered` events are
 *   delivered.
 */

/**
 * Create a Redis-backed pub/sub bus.
 *
 * @param {import('./index.js').RedisClient} client - Redis client from createRedisClient
 * @param {PubSubBusOptions} [options]
 * @returns {PubSubBus}
 *
 * @example
 * ```js
 * import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
 * import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
 *
 * const redis = createRedisClient({ url: 'redis://localhost:6379' });
 * const bus = createPubSubBus(redis);
 *
 * // Drop-in open hook: activates the bus AND subscribes every connection
 * // to the systemChannel so degraded / recovered events are delivered.
 * export const { open } = bus.hooks;
 *
 * // Use the wrapped platform for publishing:
 * const distributed = bus.wrap(platform);
 * distributed.publish('chat', 'message', { text: 'hello' });
 * ```
 */
export function createPubSubBus(client, options = {}) {
	const channel = options.channel || 'uws:pubsub';
	const instanceId = randomBytes(8).toString('hex');

	const b = options.breaker;
	const m = options.metrics;
	const relayMetrics = createRelayMetrics(m, 'pubsub');
	const mRelayed = m?.counter('pubsub_messages_relayed_total', 'Messages relayed to Redis');
	const mReceived = m?.counter('pubsub_messages_received_total', 'Messages received from Redis');
	const mEchoSuppressed = m?.counter('pubsub_echo_suppressed_total', 'Messages dropped by echo suppression');
	const mBatchRejected = m?.counter('pubsub_batch_rejected_total', 'Batched publishes refused before local or remote delivery', ['reason']);
	const mParseErrors = m?.counter('pubsub_parse_errors_total', 'Malformed envelopes dropped on receive');
	const mBatchSize = m?.histogram('pubsub_relay_batch_size', 'Relay batch size per flush', [],
		[1, 5, 10, 25, 50, 100, 250, 500, 1000]);
	const mDegraded = m?.counter('pubsub_degraded_total', 'Auto-emitted degraded events');
	const mRecovered = m?.counter('pubsub_recovered_total', 'Auto-emitted recovered events');

	const systemChannel = options.systemChannel === undefined ? '__realtime' : options.systemChannel;
	const onDegraded = options.onDegraded;
	const onRecovered = options.onRecovered;
	if (onDegraded !== undefined && typeof onDegraded !== 'function') {
		throw new Error('pubsub bus: onDegraded must be a function');
	}
	if (onRecovered !== undefined && typeof onRecovered !== 'function') {
		throw new Error('pubsub bus: onRecovered must be a function');
	}
	const degradationPolicy = options.degradationPolicy;
	if (degradationPolicy !== undefined && (degradationPolicy === null || typeof degradationPolicy.onDegraded !== 'function' || typeof degradationPolicy.onRecovered !== 'function')) {
		throw new Error('pubsub bus: degradationPolicy must be a createDegradationPolicy() result');
	}
	if (systemChannel && typeof systemChannel !== 'string') {
		throw new Error('pubsub bus: systemChannel must be a string, null, or false');
	}

	const validator = createBusValidator({
		label: 'pubsub bus',
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: options.allowSystemTopics === true,
		allowedSystemTopics: systemChannel ? [systemChannel] : []
	});

	// Whether this bus carries a frame to the other workers of this process at
	// all: their receivers apply this same envelope rule, so a frame it refuses
	// (a `__` topic it does not admit, a malformed name) can only reach them
	// through the adapter's relay. Only read on a multi-worker runtime (see
	// relayedLocally).
	const reachesSiblings = (topic, event) => validator.acceptEnvelope(topic, event);
	const entryReachesSiblings = (m) => reachesSiblings(m.topic, m.event);

	// Wire codecs registered on THIS instance, keyed by capability. A
	// `shared: true` stateless codec relays its logical event over the bus; a
	// receiving instance re-derives the codec from this map and re-runs its own
	// native cohort split locally (the codec object never crosses the bus, only
	// the capability token). Populated by the wrap's registerWireCodec.
	/** @type {Map<string, any>} */
	const wireCodecs = new Map();

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;

	/** @type {boolean} */
	let active = false;

	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	// Whether activePlatform has a tracing provider, read once at activate.
	let activeTracing = false;

	/** @type {(() => void) | null} */
	let unsubscribeBreaker = null;

	// Bounded FIFO relay batching: coalesce Redis publishes within a single
	// event-loop iteration into bounded pipelined round trips. Each envelope
	// tracks its underlying message count so the relayed-messages counter
	// stays accurate when batch envelopes carry many messages each.
	//
	// Why `setTimeout(0)` and not `queueMicrotask`: uWS dispatches each WS
	// message as its own JS task, and N-API drains microtasks at the C++/JS
	// boundary between tasks. A microtask-deferred flush fires BEFORE the
	// next socket's handler runs, so cross-socket coalescing is impossible
	// at the microtask level - N publishes from N socket handlers in the
	// same iteration produce N Redis round-trips instead of one pipelined
	// call. `setTimeout(0)` lands in libuv's timers phase, which fires only
	// after the poll phase has dispatched every ready socket message in the
	// current iteration. Same structural choice the 0.5.7 cursor always-tick
	// rewrite locked in.
	const relayQueue = createRelayQueue({
		submit: flushRelay, metrics: m, prefix: 'pubsub',
		dropped: (reason, envelopes, messages) => relayMetrics?.dropped(reason, envelopes, messages),
		overflow: attributes => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_RELAY_BURST, { attributes })
	});
	// UTF-8 uses at most three bytes per UTF-16 code unit. Short strings
	// fit without another scan of the common small-message path.
	const safeEnvelopeChars = Math.floor(validator.maxBytes / 3);
	let envelopeOversized = false;
	function scheduleRelay(msg, count, checked = false) {
		if (!checked && msg.length > safeEnvelopeChars && !validator.acceptRaw(msg)) {
			relayMetrics?.dropped('envelope_oversized', 1, count);
			if (!envelopeOversized) {
				envelopeOversized = true;
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_ENVELOPE_OVERSIZED, {
					attributes: { maxEnvelopeBytes: validator.maxBytes }
				});
			}
			return;
		}
		envelopeOversized = false;
		assert(count >= 1, 'pubsub.relay-batch.count-positive', { count });
		relayQueue.push(msg, count);
	}

	// Relay one envelope from inside a producer operation, carrying its trace
	// context when the encoded result still fits under maxEnvelopeBytes. Every
	// receiving instance enforces that cap before parsing, so an envelope the
	// trace fields push over it would be dropped by every peer; the message
	// goes without them instead. An envelope over the cap on its own is
	// reported and dropped by scheduleRelay.
	let traceOmissionReported = false;
	function relayTraced(env, count, traceContext) {
		if (traceContext === null) {
			scheduleRelay(JSON.stringify(env), count);
			return;
		}
		injectTraceContext(env, traceContext);
		const traced = JSON.stringify(env);
		if (validator.acceptRaw(traced)) {
			scheduleRelay(traced, count);
			return;
		}
		delete env.traceparent;
		delete env.tracestate;
		const plain = JSON.stringify(env);
		if (!traceOmissionReported && validator.acceptRaw(plain)) {
			traceOmissionReported = true;
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_TRACE_OMITTED, {
				attributes: { maxEnvelopeBytes: validator.maxBytes }
			});
		}
		scheduleRelay(plain, count);
	}

	// A platform.subscribe(ws, systemChannel) result from the open hook: null
	// when subscribed, a reason string when refused. Only a bus with a breaker
	// publishes degraded / recovered, so only there does a refusal lose any.
	const announcesHealth = !!(b && typeof b.subscribe === 'function');
	let systemChannelRefusalReported = false;
	function reportSystemChannelRefusal(denial) {
		if (typeof denial !== 'string' || !announcesHealth || systemChannelRefusalReported) return;
		systemChannelRefusalReported = true;
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_SYSTEM_CHANNEL_REFUSED, {
			attributes: { reason: ADAPTER_DENIAL_CODES.has(denial) ? denial : 'APP_DEFINED' }
		});
	}

	// Local delivery of one envelope relayed by another instance. The untraced
	// receive path calls it directly; on a Platform with a tracing provider it
	// runs inside a consumer operation parented on the producer's context.
	function deliverEnvelope(parsed) {
		// Coalesced relay: re-coalesce the latest value onto this
		// instance's local subscribers (latest-value-wins per ws+key),
		// mirroring the publishing instance's sendCoalesced fan-out. Must
		// precede the batch/single branches - a plain publish would
		// broadcast and defeat the per-key coalesce.
		if (parsed.coalesced === true) {
			if (!validator.acceptEnvelope(parsed.topic, parsed.event)) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc();
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
		// relay: false prevents the adapter from IPC-relaying to
		// sibling workers, since each worker has its own Redis
		// subscriber already receiving this envelope.
		// Shared binary fan-out relay: a stateless `shared: true` codec
		// published on another instance. Re-derive the codec by capability
		// and re-run publishWire locally so THIS instance does its own
		// native cohort split (its own server-wide wire-id, its own
		// subscribers); relay: false stops a re-relay loop. Falls back to a
		// plain JSON publish when the codec is not registered here (a
		// pure-subscriber instance that never registered it) - still a
		// correct delivery. Precedes the batch/single branches.
		if (parsed.wire === true) {
			if (typeof parsed.capability !== 'string' || !validator.acceptEnvelope(parsed.topic, parsed.event)) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc();
			const codec = wireCodecs.get(parsed.capability);
			if (codec && typeof activePlatform.publishWire === 'function') {
				activePlatform.publishWire(parsed.topic, parsed.event, parsed.data, codec,
					receivedOptions(parsed.s, undefined, parsed.compress === true));
			} else {
				activePlatform.publish(parsed.topic, parsed.event, parsed.data, receivedOptions(parsed.s));
			}
			return;
		}
		if (Array.isArray(parsed.batch)) {
			const local = [];
			for (let i = 0; i < parsed.batch.length; i++) {
				const m = parsed.batch[i];
				if (!m || !validator.acceptEnvelope(m.topic, m.event)) {
					mParseErrors?.inc();
					continue;
				}
				local.push({
					topic: m.topic,
					event: m.event,
					data: m.data,
					options: receivedOptions(m.s, m.j, parsed.compress === true)
				});
			}
			if (local.length === 0) return;
			mReceived?.inc(local.length);
			// A peer's platform batched these; this one may not have the batched
			// lane, and then each message goes out on its own.
			if (typeof activePlatform.publishBatched === 'function') {
				activePlatform.publishBatched(local, parsed.compress === true ? { compress: true } : undefined);
			} else {
				for (let i = 0; i < local.length; i++) {
					activePlatform.publish(local[i].topic, local[i].event, local[i].data, local[i].options);
				}
			}
		} else {
			if (!validator.acceptEnvelope(parsed.topic, parsed.event)) {
				mParseErrors?.inc();
				return;
			}
			mReceived?.inc();
			// Re-apply the de-herd window (carried as `j`) and the origin's seq
			// choice (carried as `s`) so this node's subscribers stagger AND dedup
			// against the origin's seq space; `relay: false` stops the re-relay.
			activePlatform.publish(parsed.topic, parsed.event, parsed.data, receivedOptions(parsed.s, parsed.j));
		}
	}

	function deliverTraced(parsed) {
		const parent = carriedTraceContext(parsed);
		// A null parent means current context to the adapter. Delivery without
		// a carrier must not inherit the subscriber's activation request span.
		if (parent === null) return deliverEnvelope(parsed);
		runTraceOperation(activePlatform, 'redis.pubsub.receive', 'consumer', parent, REDIS_TRACE_ATTRIBUTES,
			() => deliverEnvelope(parsed));
	}

	// The traced publish's envelope; the untraced publish builds the same
	// fields inline (see wrap().publish for why each optional one is there).
	function singleEnvelope(topic, event, data, options) {
		const env = { instanceId, topic, event, data };
		if (options && typeof options.jitterMs === 'number' && options.jitterMs > 0) {
			/** @type {any} */ (env).j = options.jitterMs;
		}
		const s = options ? carriedSeq(options.seq) : undefined;
		if (s !== undefined) /** @type {any} */ (env).s = s;
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
					emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_TRACE_OMITTED, {
						attributes: { maxEnvelopeBytes: validator.maxBytes }
					});
				}
				return plain;
			}
		}
		mBatchRejected?.inc({ reason: 'oversized' });
		throw new RangeError('pubsub bus: batched relay envelope exceeds maxEnvelopeBytes');
	}

	function prepareBatch(batch, options, traceContext = null) {
		const env = batchEnvelope(batch, options);
		if (traceContext !== null) injectTraceContext(env, traceContext);
		const msg = encodeBatch(env);
		return msg.length <= safeEnvelopeChars || validator.acceptRaw(msg) ? msg : oversizedBatch(env, traceContext);
	}

	function wireEnvelope(topic, event, data, wire, options) {
		const env = { instanceId, wire: true, capability: wire.capability, topic, event, data };
		if (options && options.compress === true) /** @type {any} */ (env).compress = true;
		const s = options ? carriedSeq(options.seq) : undefined;
		if (s !== undefined) /** @type {any} */ (env).s = s;
		return env;
	}

	function relayableBatch(messages) {
		const relayable = [];
		for (let i = 0; i < messages.length; i++) {
			const m = messages[i];
			if (!m) return null;
			if (!m.options || m.options.relay !== false) {
				relayable.push(relayEntry(m));
			}
		}
		return relayable;
	}

	// A keyless Cluster pipeline may choose a different origin for each chunk.
	// Its reply does not fence gossip from that origin, so keep a ready master
	// for successive chunks. Borrow the existing node; its deadlines and
	// lifecycle remain owned by the caller's Cluster.
	const clusteredPublisher = isCluster(client.redis);
	let publisher = null;
	function relayPublisher() {
		if (!clusteredPublisher) return client.redis;
		const masters = /** @type {any} */ (client.redis).nodes('master');
		if (publisher?.status === 'ready' && masters.includes(publisher)) return publisher;
		publisher = masters.find(node => node.status === 'ready') || null;
		if (!publisher) throw new Error('No ready Redis Cluster publisher');
		return publisher;
	}

	function flushRelay(batch) {
		let totalMessages = 0;
		for (let i = 0; i < batch.length; i++) totalMessages += batch[i].count;
		if (b) {
			try { b.guard(); } catch { relayMetrics?.dropped('breaker_open', batch.length, totalMessages); return; }
		}
		const started = relayMetrics?.start();
		const failed = (err) => {
			relayMetrics?.settled(started, batch.length, batch.length, totalMessages);
			b?.failure(err);
		};
		try {
			const redis = relayPublisher();
			if (batch.length === 1) {
				return redis.publish(channel, batch[0].msg).then(() => {
					mBatchSize?.observe(1);
					mRelayed?.inc(totalMessages);
					relayMetrics?.settled(started, 1, 0, 0);
					b?.success();
				}).catch(failed);
			}
			const pipe = redis.pipeline();
			for (let i = 0; i < batch.length; i++) pipe.publish(channel, batch[i].msg);
			return pipe.exec().then((results) => {
				mBatchSize?.observe(batch.length);
				const err = firstCommandError(results);
				if (err === null) {
					mRelayed?.inc(totalMessages);
					relayMetrics?.settled(started, batch.length, 0, 0);
					b?.success();
					return;
				}
				let relayed = 0, failedEnvelopes = 0;
				for (let i = 0; i < batch.length; i++) {
					if (!results[i][0]) relayed += batch[i].count;
					else failedEnvelopes++;
				}
				if (relayed > 0) mRelayed?.inc(relayed);
				relayMetrics?.settled(started, batch.length, failedEnvelopes, totalMessages - relayed);
				b?.failure(err);
			}).catch(failed);
		} catch (err) { failed(err); }
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
			const traced = (fn) => runTraceOperation(platform, 'redis.pubsub.publish', 'producer',
				liveTraceContext(platform), REDIS_TRACE_ATTRIBUTES, fn);
			const publishTraced = (topic, event, data, options) => {
				if (options && options.relay === false) return platform.publish(topic, event, data, options);
				return traced((traceContext) => {
					const result = platform.publish(topic, event, data, relayedLocally(options, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					relayTraced(singleEnvelope(topic, event, data, options), 1, traceContext);
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
					const m = messages[i];
					if (!m.options || m.options.relay !== false) {
						relayTraced(singleEnvelope(m.topic, m.event, m.data, m.options), 1, traceContext);
					}
				}
				return results;
			}));
			const publishBatchedTraced = (messages, options) => {
				if (!Array.isArray(messages)) throw new TypeError('pubsub bus: publishBatched requires an array of messages');
				if (messages.length === 0) return;
				if (!relaysAny(messages)) { platform.publishBatched(messages, options); return; }
				traced((traceContext) => {
					const relayable = relayableBatch(messages);
					if (relayable === null) return platform.publishBatched(messages, options);
					const msg = relayable.length ? prepareBatch(relayable, options, traceContext) : null;
					platform.publishBatched(relayedBatchLocally(messages, entryReachesSiblings), options);
					if (msg !== null) scheduleRelay(msg, relayable.length, true);
				});
			};
			const relayCoalescedTraced = (topic, event, data, coalesceKey) => traced((traceContext) =>
				relayTraced({ instanceId, coalesced: true, topic, event, data, coalesceKey }, 1, traceContext));
			const publishWireTraced = (topic, event, data, wire, options) => {
				if (!(wire && wire.shared === true && (!options || options.relay !== false))) {
					return platform.publishWire(topic, event, data, wire, options);
				}
				return traced((traceContext) => {
					const result = platform.publishWire(topic, event, data, wire, relayedLocally(options, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					relayTraced(wireEnvelope(topic, event, data, wire, options), 1, traceContext);
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
					const m = messages[i];
					if (!m.options || m.options.relay !== false) {
						scheduleRelay(JSON.stringify(singleEnvelope(m.topic, m.event, m.data, m.options)), 1);
					}
				}
				return results;
			};
			// Wire-batched publish. One Redis envelope per call carries the
			// whole list; receivers fan out via local `platform.publishBatched`
			// for one wire frame per subscriber. Empty arrays no-op.
			const publishBatchedUntraced = (messages, options) => {
				if (!Array.isArray(messages)) throw new TypeError('pubsub bus: publishBatched requires an array of messages');
				if (messages.length === 0) return;
				const relayable = new Array(messages.length);
				let count = 0;
				for (let i = 0; i < messages.length; i++) {
					const m = messages[i];
					if (!m) return platform.publishBatched(messages, options);
					if (!m.options || m.options.relay !== false) relayable[count++] = relayEntry(m);
				}
				if (count === 0) return platform.publishBatched(messages, options);
				if (count !== messages.length) relayable.length = count;
				const env = batchEnvelope(relayable, options);
				const msg = encodeBatch(env);
				if (msg.length > safeEnvelopeChars && !validator.acceptRaw(msg)) oversizedBatch(env, null);
				platform.publishBatched(relayedBatchLocally(messages, entryReachesSiblings), options);
				scheduleRelay(msg, relayable.length, true);
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
				publish(topic, event, data, options) {
					if (options && options.relay === false) return platform.publish(topic, event, data, options);
					// The bus is this publish's fan-out to every other worker and
					// instance, so a multi-worker adapter's in-process relay stays off.
					const result = platform.publish(topic, event, data, relayedLocally(options, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));

					// Carry the de-herd window across the cluster so subscribers on
					// OTHER nodes also stagger; the receiving worker re-stamps `j` on
					// its local frame. Omitted when absent so the relay wire is
					// byte-identical for the common (non-jittered) publish.
					const env = { instanceId, topic, event, data };
					if (options && typeof options.jitterMs === 'number' && options.jitterMs > 0) {
						/** @type {any} */ (env).j = options.jitterMs;
					}
					// Carry the seq choice across the cluster: an authoritative number
					// so a subscriber on ANOTHER node dedups against the SAME seq space
					// (the replay buffer's) instead of a fresh local counter, which would
					// read as cross-instance non-monotonic on resume; `false` so a frame
					// published without a seq goes out without one there too. Omitted for
					// the common counter-stamped publish (byte-identical relay wire).
					const s = options ? carriedSeq(options.seq) : undefined;
					if (s !== undefined) /** @type {any} */ (env).s = s;
					scheduleRelay(JSON.stringify(env), 1);

					return result;
				},
				batch: typeof platform.batch === 'function' ? batch : undefined,
				publishBatched: typeof platform.publishBatched === 'function' ? publishBatched : undefined,
				send: typeof platform.send === 'function' ? platform.send.bind(platform) : undefined,
				sendTo: typeof platform.sendTo === 'function' ? platform.sendTo.bind(platform) : undefined,
				// Relay-only coalesce: realtime's coalesceBy branch has already fanned
				// out to THIS instance's sockets via sendCoalesced; this carries the
				// latest value to OTHER instances, which re-coalesce it onto their own
				// subscribers. Echo-suppressed by instanceId, so the publishing
				// instance never double-delivers.
				relayCoalesced(topic, event, data, coalesceKey) {
					scheduleRelay(JSON.stringify({ instanceId, coalesced: true, topic, event, data, coalesceKey }), 1);
				},
				sendCoalesced: typeof platform.sendCoalesced === 'function' ? platform.sendCoalesced.bind(platform) : undefined,
				request: typeof platform.request === 'function' ? platform.request.bind(platform) : undefined,
				// Single-instance topic broadcast-with-reply. Forwarded as-is (no
				// cross-instance fan-out here): the realtime layer uses this to serve
				// THIS instance's subscribers, and routes the cluster fan-out through
				// the `topicBroadcast` coordinator below. `undefined` when the
				// underlying platform predates requestTopic, so realtime's typeof guard
				// degrades to single-instance.
				requestTopic: typeof platform.requestTopic === 'function' ? platform.requestTopic.bind(platform) : undefined,
				// Binary wire methods. Forwarded for local fan-out. A STATELESS
				// `shared: true` codec additionally relays its logical event across the
				// cluster: the byte-identical 0x03 frame's only per-connection-varying
				// byte is a per-process server-wide wire-id, so the frame and id never
				// cross the bus - each receiving instance re-derives the codec by
				// capability and runs its OWN native cohort split with its own id and
				// subscribers (mirrors the adapter's worker-thread relay). Stateful
				// codecs (cursor/presence) carry `shared` falsy and drive their own
				// per-plugin relay, so they never double-relay here. `undefined` when the
				// underlying platform predates per-frame binary wire, so the plugins'
				// typeof guard degrades gracefully.
				publishWire: typeof platform.publishWire === 'function' ? (topic, event, data, wire, options) => {
					if (!(wire && wire.shared === true && (!options || options.relay !== false))) {
						return platform.publishWire(topic, event, data, wire, options);
					}
					const result = platform.publishWire(topic, event, data, wire, relayedLocally(options, MULTI_WORKER_RUNTIME && reachesSiblings(topic, event)));
					// excludeWs is NOT carried: the excluded socket is local to this
					// origin instance; other instances have no such socket and fan out
					// fully (the adapter relays on its excludeWs walk path too).
					scheduleRelay(JSON.stringify(wireEnvelope(topic, event, data, wire, options)), 1);
					return result;
				} : undefined,
				sendWire: typeof platform.sendWire === 'function' ? platform.sendWire.bind(platform) : undefined,
				// Register a shared/stateful codec by capability on THIS instance.
				// Stores the codec object locally (so an inbound shared relay can
				// re-encode binary without the codec crossing the bus) AND forwards to
				// the underlying platform so the adapter's intra-process worker-thread
				// relay registry is populated. A shared-codec author must register at
				// activate on every instance for a pure-subscriber instance to re-encode
				// binary (an unregistered instance degrades a shared relay to JSON,
				// which is still a correct delivery). Version-gated like publishWire.
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
				// Forwarded so a framework that arms wire-subscribe authorization at
				// init reaches the real adapter flag through the wrapped platform in a
				// cluster deployment. Version-gated: an older adapter without it
				// degrades to a no-op (the framework optional-chains the call).
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
				// Clock-fence convention (attachClockFence): layers that stamp
				// or order by this instance's clock read it to stand down while
				// the clock is fenced. Live getter so post-wrap attachment
				// propagates.
				get clockFence() { return platform.clockFence; },
				// Cluster-clock convention (attachClusterClock): cluster-consistent
				// / leader-stamped time for layers that order or expire across
				// instances. Live getter so post-wrap attachment propagates.
				get clusterClock() { return platform.clusterClock; },
				// The topic-broadcast cluster coordinator (createTopicBroadcast),
				// attached by app init like the other plugins. Live getter so
				// post-wrap assignment propagates; realtime detects it on the wrapped
				// seam to fan `live.push({ topic })` across instances.
				get topicBroadcast() { return platform.topicBroadcast; },
				// The adapter's scoped helpers, published through this wrap so they
				// relay: each forwards its options, the counters default to 1, and
				// each returns the publish result.
				topic(t) {
					return {
						publish: (event, data, options) => wrapped.publish(t, event, data, options),
						created: (data, options) => wrapped.publish(t, 'created', data, options),
						updated: (data, options) => wrapped.publish(t, 'updated', data, options),
						deleted: (data, options) => wrapped.publish(t, 'deleted', data, options),
						set: (value, options) => wrapped.publish(t, 'set', value, options),
						increment: (amount = 1, options) => wrapped.publish(t, 'increment', amount, options),
						decrement: (amount = 1, options) => wrapped.publish(t, 'decrement', amount, options)
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

		async activate(platform) {
			// Always update the platform reference so remote messages
			// are forwarded through the latest platform, even if a
			// previous activate() already started the subscriber.
			activePlatform = platform;
			activeTracing = traceEnabled(platform);
			if (active) return;
			b?.guard();

			if (b && typeof b.subscribe === 'function' && !unsubscribeBreaker) {
				unsubscribeBreaker = b.subscribe((from, to) => {
					if (from === 'healthy' && to !== 'healthy') {
						if (onDegraded) {
							try { onDegraded(); } catch { /* don't propagate user errors */ }
						}
						if (systemChannel && activePlatform) {
							// A degradation policy ships the precomputed client mitigation
							// alongside the event + de-herds the push (jitterMs), so the
							// resulting client retries/fallbacks spread across the cooldown.
							let out = null;
							if (degradationPolicy) { try { out = degradationPolicy.onDegraded({ from, to }); } catch { /* a policy must never break the emit */ } }
							const data = out && out.mitigation ? { at: now(), mitigation: out.mitigation } : { at: now() };
							activePlatform.publish(systemChannel, 'degraded', data, out && out.jitterMs ? { ...SYSTEM_FRAME, jitterMs: out.jitterMs } : SYSTEM_FRAME);
							mDegraded?.inc();
						}
					} else if (from !== 'healthy' && to === 'healthy') {
						if (onRecovered) {
							try { onRecovered(); } catch { /* don't propagate user errors */ }
						}
						if (systemChannel && activePlatform) {
							let out = null;
							if (degradationPolicy) { try { out = degradationPolicy.onRecovered({ from, to }); } catch { /* a policy must never break the emit */ } }
							const data = out && out.recovery ? { at: now(), recovery: out.recovery } : { at: now() };
							activePlatform.publish(systemChannel, 'recovered', data, out && out.jitterMs ? { ...SYSTEM_FRAME, jitterMs: out.jitterMs } : SYSTEM_FRAME);
							mRecovered?.inc();
						}
					}
				});
			}

			const sub = openSubscriber(client, 'pubsub');
			subscriber = sub;
			sub.on('error', (err) => emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PUBSUB_SUBSCRIBER, { error: err }));

			sub.on('message', (ch, message) => {
				if (ch !== channel) return;
				// Pre-parse size guard. A bus subscriber that JSON.parses an
				// arbitrarily large attacker payload pays parsing CPU + V8
				// heap before any further validation can fire.
				if (!validator.acceptRaw(message)) {
					mParseErrors?.inc();
					return;
				}
				try {
					const parsed = JSON.parse(message);
					// An envelope without a string instanceId cannot be echo-checked
					// or delivered. It is input from a shared transport, not a broken
					// invariant of this package, so it is counted and dropped like any
					// other malformed envelope: a line per message would let anyone who
					// can publish on the channel flood the log.
					if (!(typeof parsed === 'object' && parsed !== null && typeof parsed.instanceId === 'string')) {
						mParseErrors?.inc();
						return;
					}
					// Skip messages from this instance (echo suppression).
					// One check per envelope; batched envelopes carry one
					// instanceId for the whole batch.
					if (parsed.instanceId === instanceId) {
						mEchoSuppressed?.inc();
						return;
					}
					// A message can still arrive on the subscriber socket during the
					// deactivate window (activePlatform is nulled before the UNSUBSCRIBE
					// round-trip resolves). Drop it cleanly rather than dereferencing a
					// null platform in a branch below (which the outer catch would
					// swallow as a spurious parse error). Mirrors the sharded handler.
					if (!activePlatform) return;
					// Local delivery runs no application code, so a carried trace
					// context only matters to a tracing provider: with one, delivery is
					// a consumer operation parented on the producer's context; without
					// one the trace fields are ignored and nothing is allocated for them.
					if (activeTracing) deliverTraced(parsed);
					else deliverEnvelope(parsed);
				} catch {
					// Malformed envelope; counted so a stream of bad messages
					// is observable instead of silently swallowed.
					mParseErrors?.inc();
				}
			});

			try {
				await sub.subscribe(channel);
				b?.success();
				active = true;
			} catch (err) {
				b?.failure(err);
				activePlatform = null;
				// quit() is the likely one to reject here, since Redis is what just
				// failed, and its handler runs after the slot is cleared, so it uses
				// this connection rather than reading the slot.
				if (subscriber === sub) subscriber = null;
				sub.quit().catch(() => sub.disconnect());
				if (unsubscribeBreaker) {
					unsubscribeBreaker();
					unsubscribeBreaker = null;
				}
				throw err;
			}
		},

		async deactivate() {
			if (unsubscribeBreaker) {
				unsubscribeBreaker();
				unsubscribeBreaker = null;
			}
			relayQueue.clear();
			if (!active || !subscriber) return;
			active = false;
			activePlatform = null;
			const sub = subscriber;
			subscriber = null;
			await sub.unsubscribe(channel).catch(() => {});
			await sub.quit().catch(() => sub.disconnect());
		},

		hooks: {
			async open(ws, ctx) {
				const platform = ctx && ctx.platform;
				if (!platform) return;
				// systemChannel uses the platform-trust path (`platform.subscribe`),
				// not the wire path, so it bypasses the adapter's `__`-prefix
				// gate. Subscribing here is what restores `degraded` / `recovered`
				// delivery after the wire path was closed in adapter 0.5.0-next.21.
				// The platform still runs the application's subscribe /
				// subscribeBatch hook, which can refuse the topic. A refusal is
				// not awaited; on a bus with a breaker it is reported.
				if (systemChannel) {
					const denial = platform.subscribe(ws, systemChannel);
					if (denial && typeof denial.then === 'function') denial.then(reportSystemChannelRefusal);
					else reportSystemChannelRefusal(denial);
				}
				await bus.activate(platform);
			}
		}
	};
	return bus;
}
