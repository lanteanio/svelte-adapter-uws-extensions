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

import { randomBytes } from 'node:crypto';
import { assert } from '../shared/assert.js';
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
 * @typedef {Object} PubSubBus
 * @property {(platform: import('svelte-adapter-uws').Platform) => import('svelte-adapter-uws').Platform} wrap -
 *   Returns a new Platform whose publish() sends to Redis + local.
 *   Use this wrapped platform everywhere you call publish().
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} activate -
 *   Start the Redis subscriber. Incoming messages are forwarded to the
 *   original platform.publish(). Call this once at startup (e.g. in your open hook).
 * @property {() => Promise<void>} deactivate -
 *   Stop the Redis subscriber and clean up.
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
 * // In your open hook:
 * export function open(ws, { platform }) {
 *   bus.activate(platform); // idempotent, only subscribes once
 * }
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

	// Microtask relay batching: coalesce Redis publishes within a single
	// event-loop tick into one pipelined round trip. Each envelope tracks
	// its underlying message count so the relayed-messages counter stays
	// accurate when batch envelopes carry many messages each.
	/** @type {Array<{msg: string, count: number}>} */
	let relayBatch = [];
	let relayScheduled = false;
	let relayBatchWarnFired = false;

	function scheduleRelay(msg, count) {
		assert(count >= 1, 'pubsub.relay-batch.count-positive', { count });
		relayBatch.push({ msg, count });
		if (relayBatch.length >= MAX_PUBSUB_RELAY_BATCH_PER_TICK && !relayBatchWarnFired) {
			relayBatchWarnFired = true;
			console.warn(
				'[pubsub] microtask relay batch reached ' + relayBatch.length +
				' entries in one tick. The batch is drained every microtask, so a ' +
				'caller emitted a million publishes in one synchronous burst - likely ' +
				'a publish-in-loop without yielding.\n' +
				'  See: https://svti.me/pubsub-burst'
			);
		}
		if (!relayScheduled) {
			relayScheduled = true;
			queueMicrotask(flushRelay);
		}
	}

	function flushRelay() {
		const batch = relayBatch;
		relayBatch = [];
		relayScheduled = false;
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

	return {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, options) {
					const result = platform.publish(topic, event, data, options);

					if (!options || options.relay !== false) {
						scheduleRelay(JSON.stringify({ instanceId, topic, event, data }), 1);
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
				sendCoalesced: platform.sendCoalesced.bind(platform),
				request: platform.request.bind(platform),
				get connections() { return platform.connections; },
				get requestId() { return platform.requestId; },
				get pressure() { return platform.pressure; },
				onPressure: platform.onPressure.bind(platform),
				onPublishRate: platform.onPublishRate.bind(platform),
				subscribers: platform.subscribers.bind(platform),
				subscribe: platform.subscribe.bind(platform),
				unsubscribe: platform.unsubscribe.bind(platform),
				checkSubscribe: platform.checkSubscribe.bind(platform),
				get maxPayloadLength() { return platform.maxPayloadLength; },
				bufferedAmount: platform.bufferedAmount.bind(platform),
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
							activePlatform.publish(systemChannel, 'degraded', { at: Date.now() });
							mDegraded?.inc();
						}
					} else if (from !== 'healthy' && to === 'healthy') {
						if (onRecovered) {
							try { onRecovered(); } catch { /* don't propagate user errors */ }
						}
						if (systemChannel && activePlatform) {
							activePlatform.publish(systemChannel, 'recovered', { at: Date.now() });
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
				const rawBytes = typeof message === 'string'
					? Buffer.byteLength(message)
					: /** @type {Buffer} */ (message).length;
				if (!validator.acceptSize(rawBytes)) {
					mParseErrors?.inc();
					return;
				}
				try {
					const parsed = JSON.parse(message);
					assert(
						typeof parsed === 'object' && parsed !== null && typeof parsed.instanceId === 'string',
						'pubsub.envelope.shape',
						{ ch }
					);
					// Skip messages from this instance (echo suppression).
					// One check per envelope; batched envelopes carry one
					// instanceId for the whole batch.
					if (parsed.instanceId === instanceId) {
						mEchoSuppressed?.inc();
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
						activePlatform.publish(parsed.topic, parsed.event, parsed.data, { relay: false });
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
			if (!active || !subscriber) return;
			active = false;
			activePlatform = null;
			await subscriber.unsubscribe(channel).catch(() => {});
			await subscriber.quit().catch(() => subscriber.disconnect());
			subscriber = null;
		}
	};
}
