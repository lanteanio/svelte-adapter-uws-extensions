/**
 * Redis-backed presence tracker for svelte-adapter-uws.
 *
 * Same API as the core createPresence plugin, but stores presence state
 * in Redis hashes so it is shared across instances. Uses Redis pub/sub
 * for cross-instance join/leave notifications.
 *
 * Storage layout per topic:
 *   - Key `{prefix}presence:{topic}` - hash (field = userKey, value = JSON userData)
 *   - Channel `{prefix}presence:events:{topic}` - pub/sub for join/leave events
 *
 * Each instance also maintains a local connection map so it knows when to
 * publish leave events (last connection for a user on this instance).
 *
 * @module svelte-adapter-uws-extensions/redis/presence
 */

import { randomBytes } from 'node:crypto';

/**
 * @typedef {Object} RedisPresenceOptions
 * @property {string} [key='id'] - Field in selected data for user dedup
 * @property {(userData: any) => Record<string, any>} [select] - Extract public fields from userData
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh expiry)
 * @property {number} [ttl=90] - TTL in seconds for presence hash entries (should be > heartbeat * 3)
 */

/**
 * @typedef {Object} RedisPresenceTracker
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} join
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform) => Promise<void>} leave
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} sync
 * @property {(topic: string) => Promise<Array<Record<string, any>>>} list
 * @property {(topic: string) => Promise<number>} count
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop heartbeat and subscriber
 */

/**
 * Create a Redis-backed presence tracker.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisPresenceOptions} [options]
 * @returns {RedisPresenceTracker}
 */
export function createPresence(client, options = {}) {
	const keyField = options.key || 'id';
	const select = options.select || ((userData) => userData);
	const heartbeatInterval = options.heartbeat || 30000;
	const presenceTtl = options.ttl || 90;

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;

	let connCounter = 0;

	/**
	 * Per-connection state: which topics they've joined and their key on each.
	 * @type {Map<any, Map<string, { key: string, data: Record<string, any> }>>}
	 */
	const wsTopics = new Map();

	/**
	 * Local per-topic reference count per user key.
	 * Used to know when the last local connection for a user leaves.
	 * @type {Map<string, Map<string, number>>}
	 */
	const localCounts = new Map();

	function hashKey(topic) {
		return client.key('presence:' + topic);
	}

	function eventChannel(topic) {
		return client.key('presence:events:' + topic);
	}

	function resolveKey(data) {
		if (data && keyField in data && data[keyField] != null) {
			return String(data[keyField]);
		}
		return '__conn:' + (++connCounter);
	}

	// Heartbeat: refresh TTL on all hash keys this instance has users in
	/** @type {Set<string>} */
	const activeTopics = new Set();
	const heartbeatTimer = setInterval(() => {
		for (const topic of activeTopics) {
			redis.expire(hashKey(topic), presenceTtl).catch(() => {});
		}
	}, heartbeatInterval);
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Redis subscriber for cross-instance join/leave events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	/** @type {Set<string>} - channels we have subscribed to */
	const subscribedChannels = new Set();

	async function ensureSubscriber(platform) {
		if (!subscriber) {
			subscriber = client.duplicate();
			activePlatform = platform;
			subscriber.on('message', (ch, message) => {
				try {
					const parsed = JSON.parse(message);
					if (parsed.instanceId === instanceId) return;
					// Forward to local platform
					if (activePlatform) {
						activePlatform.publish('__presence:' + parsed.topic, parsed.event, parsed.payload);
					}
				} catch {
					// Malformed, skip
				}
			});
		}
	}

	async function subscribeToTopic(topic, platform) {
		await ensureSubscriber(platform);
		const ch = eventChannel(topic);
		if (!subscribedChannels.has(ch)) {
			subscribedChannels.add(ch);
			await subscriber.subscribe(ch);
		}
	}

	async function publishEvent(topic, event, payload) {
		const ch = eventChannel(topic);
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		await redis.publish(ch, msg).catch(() => {});
	}

	return {
		async join(ws, topic, platform) {
			if (topic.startsWith('__')) return;

			let connTopics = wsTopics.get(ws);
			if (connTopics && connTopics.has(topic)) return;

			const data = select(ws.getUserData());
			const key = resolveKey(data);

			// Track per-connection
			if (!connTopics) {
				connTopics = new Map();
				wsTopics.set(ws, connTopics);
			}
			connTopics.set(topic, { key, data });

			// Track local reference count
			let counts = localCounts.get(topic);
			if (!counts) {
				counts = new Map();
				localCounts.set(topic, counts);
			}
			const prevCount = counts.get(key) || 0;
			counts.set(key, prevCount + 1);

			activeTopics.add(topic);

			// Subscribe to cross-instance events for this topic
			await subscribeToTopic(topic, platform);

			if (prevCount === 0) {
				// New user on this topic (at least from this instance)
				// Write to Redis hash
				await redis.hset(hashKey(topic), key, JSON.stringify(data));
				await redis.expire(hashKey(topic), presenceTtl);

				// Publish join locally and to other instances
				const payload = { key, data };
				platform.publish('__presence:' + topic, 'join', payload);
				await publishEvent(topic, 'join', payload);
			}

			// Subscribe ws to presence channel
			ws.subscribe('__presence:' + topic);

			// Send current list to this connection
			const all = await redis.hgetall(hashKey(topic));
			const list = [];
			for (const [k, v] of Object.entries(all)) {
				try {
					list.push({ key: k, data: JSON.parse(v) });
				} catch {
					// Corrupted entry, skip
				}
			}
			platform.send(ws, '__presence:' + topic, 'list', list);
		},

		async leave(ws, platform) {
			const connTopics = wsTopics.get(ws);
			if (!connTopics) return;

			for (const [topic, { key, data }] of connTopics) {
				const counts = localCounts.get(topic);
				if (!counts) continue;

				const current = counts.get(key) || 0;
				if (current <= 1) {
					counts.delete(key);
					if (counts.size === 0) {
						localCounts.delete(topic);
						activeTopics.delete(topic);
					}

					// Remove from Redis hash
					await redis.hdel(hashKey(topic), key);

					// Broadcast leave
					const payload = { key, data };
					platform.publish('__presence:' + topic, 'leave', payload);
					await publishEvent(topic, 'leave', payload);
				} else {
					counts.set(key, current - 1);
				}
			}

			wsTopics.delete(ws);
		},

		async sync(ws, topic, platform) {
			const all = await redis.hgetall(hashKey(topic));
			const presenceTopic = '__presence:' + topic;
			const list = [];
			for (const [k, v] of Object.entries(all)) {
				try {
					list.push({ key: k, data: JSON.parse(v) });
				} catch {
					// Skip corrupted entries
				}
			}
			ws.subscribe(presenceTopic);
			platform.send(ws, presenceTopic, 'list', list);
		},

		async list(topic) {
			const all = await redis.hgetall(hashKey(topic));
			const result = [];
			for (const v of Object.values(all)) {
				try {
					result.push(JSON.parse(v));
				} catch {
					// Skip corrupted entries
				}
			}
			return result;
		},

		async count(topic) {
			return redis.hlen(hashKey(topic));
		},

		async clear() {
			// Clear all presence keys
			const pattern = client.key('presence:*');
			let cursor = '0';
			do {
				const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				cursor = nextCursor;
				if (keys.length > 0) {
					await redis.del(...keys);
				}
			} while (cursor !== '0');

			wsTopics.clear();
			localCounts.clear();
			activeTopics.clear();
			connCounter = 0;
		},

		destroy() {
			clearInterval(heartbeatTimer);
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscribedChannels.clear();
			activePlatform = null;
		}
	};
}
