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
import { randomBytes, now, monotonicNow, setTimer, clearTimer } from '../shared/runtime.js';
import { fallbackRandom, fallbackHlc } from '../shared/platform-fallback.js';
import { MAX_PUBSUB_RELAY_BATCH_PER_TICK } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';

/**
 * @typedef {Object} PubSubBusOptions
 * @property {string} [channel='uws:pubsub'] - Redis channel name for pub/sub messages
 * @property {string | null | false} [systemChannel='__realtime'] - Topic used for auto-emitted `degraded` / `recovered` events on the local platform. Set to `null` or `false` to disable auto-emission.
 * @property {() => void} [onDegraded] - Called once when the breaker leaves the healthy state. Requires a `breaker` to be passed.
 * @property {() => void} [onRecovered] - Called once when the breaker returns to the healthy state. Requires a `breaker` to be passed.
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound envelopes larger than this many bytes BEFORE JSON.parse runs. Defends against bus-side DoS in shared-Redis deployments.
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
	const mRelayed = m?.counter('pubsub_messages_relayed_total', 'Messages relayed to Redis');
	const mReceived = m?.counter('pubsub_messages_received_total', 'Messages received from Redis');
	const mEchoSuppressed = m?.counter('pubsub_echo_suppressed_total', 'Messages dropped by echo suppression');
	const mParseErrors = m?.counter('pubsub_parse_errors_total', 'Malformed envelopes dropped on receive');
	const mBatchSize = m?.histogram('pubsub_relay_batch_size', 'Relay batch size per flush');
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
	if (systemChannel && typeof systemChannel !== 'string') {
		throw new Error('pubsub bus: systemChannel must be a string, null, or false');
	}

	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: options.allowSystemTopics === true,
		allowedSystemTopics: systemChannel ? [systemChannel] : []
	});

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;

	/** @type {boolean} */
	let active = false;

	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;

	/** @type {(() => void) | null} */
	let unsubscribeBreaker = null;

	// Tick relay batching: coalesce Redis publishes within a single
	// event-loop iteration into one pipelined round trip. Each envelope
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
	/** @type {Array<{msg: string, count: number}>} */
	let relayBatch = [];
	/** @type {ReturnType<typeof setTimeout> | null} */
	let relayTimer = null;
	let relayBatchWarnFired = false;

	function scheduleRelay(msg, count) {
		assert(count >= 1, 'pubsub.relay-batch.count-positive', { count });
		relayBatch.push({ msg, count });
		if (relayBatch.length >= MAX_PUBSUB_RELAY_BATCH_PER_TICK && !relayBatchWarnFired) {
			relayBatchWarnFired = true;
			console.warn(
				'[pubsub] tick relay batch reached ' + relayBatch.length +
				' entries in one iteration. The batch is drained every tick, so a ' +
				'caller emitted a million publishes in one synchronous burst - likely ' +
				'a publish-in-loop without yielding.\n' +
				'  See: https://svti.me/pubsub-burst'
			);
		}
		if (relayTimer === null) {
			relayTimer = setTimer(flushRelay, 0);
			if (relayTimer.unref) relayTimer.unref();
		}
	}

	function flushRelay() {
		const batch = relayBatch;
		relayBatch = [];
		if (relayTimer !== null) {
			clearTimer(relayTimer);
			relayTimer = null;
		}
		if (b) {
			try { b.guard(); } catch { return; }
		}
		let totalMessages = 0;
		for (let i = 0; i < batch.length; i++) totalMessages += batch[i].count;

		if (batch.length === 1) {
			client.redis.publish(channel, batch[0].msg).then(() => {
				mBatchSize?.observe(1);
				mRelayed?.inc(totalMessages);
				b?.success();
			}).catch((err) => { b?.failure(err); });
			return;
		}
		const pipe = client.redis.pipeline();
		for (let i = 0; i < batch.length; i++) {
			pipe.publish(channel, batch[i].msg);
		}
		pipe.exec().then(() => {
			mBatchSize?.observe(batch.length);
			mRelayed?.inc(totalMessages);
			b?.success();
		}).catch((err) => { b?.failure(err); });
	}

	const bus = {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, options) {
					const result = platform.publish(topic, event, data, options);

					if (!options || options.relay !== false) {
						// Carry the de-herd window across the cluster so subscribers on
						// OTHER nodes also stagger; the receiving worker re-stamps `j` on
						// its local frame. Omitted when absent so the relay wire is
						// byte-identical for the common (non-jittered) publish.
						const env = { instanceId, topic, event, data };
						if (options && typeof options.jitterMs === 'number' && options.jitterMs > 0) {
							/** @type {any} */ (env).j = options.jitterMs;
						}
						scheduleRelay(JSON.stringify(env), 1);
					}

					return result;
				},
				// Per-event loop. Mirrors `platform.batch` semantics: N submitted
				// messages produce N wire frames per subscriber. Use
				// `publishBatched` instead when you want one wire frame per
				// subscriber per call.
				batch(messages) {
					const results = platform.batch(messages);
					for (let i = 0; i < messages.length; i++) {
						const m = messages[i];
						if (!m.options || m.options.relay !== false) {
							scheduleRelay(JSON.stringify({
								instanceId, topic: m.topic, event: m.event, data: m.data
							}), 1);
						}
					}
					return results;
				},
				// Wire-batched publish. One Redis envelope per call carries the
				// whole list; receivers fan out via local `platform.publishBatched`
				// for one wire frame per subscriber. Empty arrays no-op.
				publishBatched(messages) {
					if (!Array.isArray(messages)) {
						throw new TypeError('pubsub bus: publishBatched requires an array of messages');
					}
					if (messages.length === 0) return;

					platform.publishBatched(messages);

					const relayable = [];
					for (let i = 0; i < messages.length; i++) {
						const m = messages[i];
						if (!m.options || m.options.relay !== false) {
							relayable.push({ topic: m.topic, event: m.event, data: m.data });
						}
					}
					if (relayable.length === 0) return;

					scheduleRelay(JSON.stringify({ instanceId, batch: relayable }), relayable.length);
				},
				send: platform.send.bind(platform),
				sendTo: platform.sendTo.bind(platform),
				// Relay-only coalesce: realtime's coalesceBy branch has already fanned
				// out to THIS instance's sockets via sendCoalesced; this carries the
				// latest value to OTHER instances, which re-coalesce it onto their own
				// subscribers. Echo-suppressed by instanceId, so the publishing
				// instance never double-delivers.
				relayCoalesced(topic, event, data, coalesceKey) {
					scheduleRelay(JSON.stringify({ instanceId, coalesced: true, topic, event, data, coalesceKey }), 1);
				},
				sendCoalesced: platform.sendCoalesced.bind(platform),
				request: platform.request.bind(platform),
				// Single-instance topic broadcast-with-reply. Forwarded as-is (no
				// cross-instance fan-out here): the realtime layer uses this to serve
				// THIS instance's subscribers, and routes the cluster fan-out through
				// the `topicBroadcast` coordinator below. `undefined` when the
				// underlying platform predates requestTopic, so realtime's typeof guard
				// degrades to single-instance.
				requestTopic: platform.requestTopic ? platform.requestTopic.bind(platform) : undefined,
				// Binary wire methods. Forwarded like send/sendTo (local fanout, no
				// cross-instance relay - the plugin's own relay() handles that). Without
				// these the wrapped seam hides the binary path and cluster-backed cursor /
				// presence silently fall back to JSON. `undefined` when the underlying
				// platform predates per-frame binary wire, so the plugins' typeof guard
				// degrades gracefully.
				publishWire: platform.publishWire ? platform.publishWire.bind(platform) : undefined,
				sendWire: platform.sendWire ? platform.sendWire.bind(platform) : undefined,
				get connections() { return platform.connections; },
				get requestId() { return platform.requestId; },
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
				onPressure: platform.onPressure.bind(platform),
				onPublishRate: platform.onPublishRate.bind(platform),
				subscribers: platform.subscribers.bind(platform),
				forEachSubscriber: platform.forEachSubscriber.bind(platform),
				subscribe: platform.subscribe.bind(platform),
				unsubscribe: platform.unsubscribe.bind(platform),
				checkSubscribe: platform.checkSubscribe.bind(platform),
				get maxPayloadLength() { return platform.maxPayloadLength; },
				bufferedAmount: platform.bufferedAmount.bind(platform),
				get closedWsAborts() { return platform.closedWsAborts ?? 0; },
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
				topicEpoch(t) {
					const r = platform.replay;
					if (r && typeof r.cachedEpoch === 'function') return r.cachedEpoch(t);
					return platform.topicEpoch(t);
				},
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
				// The topic-broadcast cluster coordinator (createTopicBroadcast),
				// attached by app init like the other plugins. Live getter so
				// post-wrap assignment propagates; realtime detects it on the wrapped
				// seam to fan `live.push({ topic })` across instances.
				get topicBroadcast() { return platform.topicBroadcast; },
				topic(t) {
					return {
						publish(event, data) { wrapped.publish(t, event, data); },
						created(data) { wrapped.publish(t, 'created', data); },
						updated(data) { wrapped.publish(t, 'updated', data); },
						deleted(data) { wrapped.publish(t, 'deleted', data); },
						set(value) { wrapped.publish(t, 'set', value); },
						increment(amount) { wrapped.publish(t, 'increment', amount); },
						decrement(amount) { wrapped.publish(t, 'decrement', amount); }
					};
				}
			};
			return wrapped;
		},

		async activate(platform) {
			// Always update the platform reference so remote messages
			// are forwarded through the latest platform, even if a
			// previous activate() already started the subscriber.
			activePlatform = platform;
			if (active) return;
			b?.guard();

			if (b && typeof b.subscribe === 'function' && !unsubscribeBreaker) {
				unsubscribeBreaker = b.subscribe((from, to) => {
					if (from === 'healthy' && to !== 'healthy') {
						if (onDegraded) {
							try { onDegraded(); } catch { /* don't propagate user errors */ }
						}
						if (systemChannel && activePlatform) {
							activePlatform.publish(systemChannel, 'degraded', { at: now() });
							mDegraded?.inc();
						}
					} else if (from !== 'healthy' && to === 'healthy') {
						if (onRecovered) {
							try { onRecovered(); } catch { /* don't propagate user errors */ }
						}
						if (systemChannel && activePlatform) {
							activePlatform.publish(systemChannel, 'recovered', { at: now() });
							mRecovered?.inc();
						}
					}
				});
			}

			subscriber = client.duplicate({ enableReadyCheck: false });

			subscriber.on('message', (ch, message) => {
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
					// A forged or byte-corrupted envelope with a non-string instanceId
					// must drop the same way in every run mode. `assert` throws in test
					// mode but only logs-and-counts in production (throwing in a pubsub
					// callback could corrupt state), so a bare assert would fall through
					// in production and deliver an unidentifiable frame. Drop explicitly,
					// keeping the production shape signal.
					if (!(typeof parsed === 'object' && parsed !== null && typeof parsed.instanceId === 'string')) {
						assert(false, 'pubsub.envelope.shape', { ch });
						return;
					}
					// Skip messages from this instance (echo suppression).
					// One check per envelope; batched envelopes carry one
					// instanceId for the whole batch.
					if (parsed.instanceId === instanceId) {
						mEchoSuppressed?.inc();
						return;
					}
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
						const fullKey = parsed.topic + '\0' + (parsed.coalesceKey == null ? '' : parsed.coalesceKey);
						mReceived?.inc();
						activePlatform.forEachSubscriber(parsed.topic, (ws) => {
							activePlatform.sendCoalesced(ws, { key: fullKey, topic: parsed.topic, event: parsed.event, data: parsed.data });
						});
						return;
					}
					// relay: false prevents the adapter from IPC-relaying to
					// sibling workers, since each worker has its own Redis
					// subscriber already receiving this envelope.
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
								options: { relay: false }
							});
						}
						if (local.length === 0) return;
						mReceived?.inc(local.length);
						activePlatform.publishBatched(local);
					} else {
						if (!validator.acceptEnvelope(parsed.topic, parsed.event)) {
							mParseErrors?.inc();
							return;
						}
						mReceived?.inc();
						// Re-apply the de-herd window (carried as `j`) so this node's
						// subscribers stagger too; `relay: false` stops the re-relay.
						activePlatform.publish(parsed.topic, parsed.event, parsed.data,
							parsed.j !== undefined ? { relay: false, jitterMs: parsed.j } : { relay: false });
					}
				} catch {
					// Malformed envelope; counted so a stream of bad messages
					// is observable instead of silently swallowed.
					mParseErrors?.inc();
				}
			});

			try {
				await subscriber.subscribe(channel);
				b?.success();
				active = true;
			} catch (err) {
				b?.failure(err);
				activePlatform = null;
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
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
			if (relayTimer !== null) {
				clearTimer(relayTimer);
				relayTimer = null;
			}
			relayBatch = [];
			if (!active || !subscriber) return;
			active = false;
			activePlatform = null;
			await subscriber.unsubscribe(channel).catch(() => {});
			await subscriber.quit().catch(() => subscriber.disconnect());
			subscriber = null;
		},

		hooks: {
			async open(ws, ctx) {
				const platform = ctx && ctx.platform;
				if (!platform) return;
				// systemChannel uses the platform-trust path (`platform.subscribe`),
				// not the wire path, so it bypasses the adapter's `__`-prefix
				// gate. Subscribing here is what restores `degraded` / `recovered`
				// delivery after the wire path was closed in adapter 0.5.0-next.21.
				if (systemChannel) {
					platform.subscribe(ws, systemChannel);
				}
				await bus.activate(platform);
			}
		}
	};
	return bus;
}
