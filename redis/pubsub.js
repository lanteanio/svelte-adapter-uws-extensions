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

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;

	/** @type {boolean} */
	let active = false;

	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;

	return {
		wrap(platform) {
			const wrapped = {
				publish(topic, event, data, options) {
					// Publish locally, forwarding options as-is
					const result = platform.publish(topic, event, data, options);

					// Only relay to Redis if the caller did not suppress relay.
					// When relay is explicitly false the message is local-only
					// (e.g. it already came from Redis on another instance).
					if (!options || options.relay !== false) {
						const msg = JSON.stringify({ instanceId, topic, event, data });
						client.redis.publish(channel, msg).catch(() => {
							// Fire-and-forget: ioredis auto-reconnects.
							// Swallowing here prevents unhandled rejections on transient disconnects.
						});
					}

					return result;
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

			subscriber = client.duplicate({ enableReadyCheck: false });

			subscriber.on('message', (ch, message) => {
				if (ch !== channel) return;
				try {
					const parsed = JSON.parse(message);
					// Skip messages from this instance (echo suppression)
					if (parsed.instanceId === instanceId) return;
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
				active = true;
			} catch (err) {
				// Clean up so the next activate() call can retry
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
