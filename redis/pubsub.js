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

/**
 * @typedef {Object} PubSubBusOptions
 * @property {string} [channel='uws:pubsub'] - Redis channel name for pub/sub messages
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
	const mBatchSize = m?.histogram('pubsub_relay_batch_size', 'Relay batch size per flush');

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;

	/** @type {boolean} */
	let active = false;

	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;

	// Microtask relay batching: coalesce Redis publishes within a single
	// event-loop tick into one pipelined round trip.
	/** @type {Array<string>} */
	let relayBatch = [];
	let relayScheduled = false;

	function scheduleRelay(msg) {
		relayBatch.push(msg);
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
		if (batch.length === 1) {
			client.redis.publish(channel, batch[0]).then(() => {
				mBatchSize?.observe(1);
				mRelayed?.inc(1);
				b?.success();
			}).catch((err) => { b?.failure(err); });
			return;
		}
		const pipe = client.redis.pipeline();
		for (let i = 0; i < batch.length; i++) {
			pipe.publish(channel, batch[i]);
		}
		pipe.exec().then(() => {
			mBatchSize?.observe(batch.length);
			mRelayed?.inc(batch.length);
			b?.success();
		}).catch((err) => { b?.failure(err); });
	}

	return {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, options) {
					const result = platform.publish(topic, event, data, options);

					if (!options || options.relay !== false) {
						scheduleRelay(JSON.stringify({ instanceId, topic, event, data }));
					}

					return result;
				},
				batch(messages) {
					const results = platform.batch(messages);
					for (let i = 0; i < messages.length; i++) {
						const m = messages[i];
						if (!m.options || m.options.relay !== false) {
							scheduleRelay(JSON.stringify({
								instanceId, topic: m.topic, event: m.event, data: m.data
							}));
						}
					}
					return results;
				},
				send: platform.send.bind(platform),
				sendTo: platform.sendTo.bind(platform),
				get connections() { return platform.connections; },
				subscribers: platform.subscribers.bind(platform),
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

			subscriber = client.duplicate({ enableReadyCheck: false });

			subscriber.on('message', (ch, message) => {
				if (ch !== channel) return;
				try {
					const parsed = JSON.parse(message);
					// Skip messages from this instance (echo suppression)
					if (parsed.instanceId === instanceId) {
						mEchoSuppressed?.inc();
						return;
					}
					mReceived?.inc();
					// Forward to local platform only -- relay: false prevents the
					// adapter from IPC-relaying to sibling workers, since each
					// worker has its own Redis subscriber already receiving this.
					activePlatform.publish(parsed.topic, parsed.event, parsed.data, { relay: false });
				} catch {
					// Malformed message, skip
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
				throw err;
			}
		},

		async deactivate() {
			if (!active || !subscriber) return;
			active = false;
			activePlatform = null;
			await subscriber.unsubscribe(channel).catch(() => {});
			await subscriber.quit().catch(() => subscriber.disconnect());
			subscriber = null;
		}
	};
}
