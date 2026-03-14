/**
 * Redis-backed cursor / ephemeral state plugin for svelte-adapter-uws.
 *
 * Same API as the core createCursor plugin, but cursor positions are shared
 * across instances via Redis. Each instance throttles locally (same
 * leading/trailing edge logic as the core), then relays broadcasts through
 * Redis pub/sub so subscribers on other instances see cursor updates.
 *
 * Storage layout:
 *   - Hash `{prefix}cursor:{topic}` - field = connectionKey, value = JSON { user, data }
 *   - Channel `{prefix}cursor:events` - pub/sub for update/remove relay
 *
 * Hash entries expire via TTL so stale cursors from crashed instances
 * get cleaned up automatically.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor
 */

import { randomBytes } from 'node:crypto';

/**
 * @typedef {Object} RedisCursorOptions
 * @property {number} [throttle=50] - Minimum ms between broadcasts per user per topic.
 *   Trailing-edge timer fires to ensure the final position is always sent.
 * @property {(userData: any) => any} [select] - Extract user-identifying data from userData.
 *   Defaults to the full userData.
 * @property {number} [ttl=30] - TTL in seconds for hash entries. Should be longer than
 *   the expected gap between updates. Entries are refreshed on every broadcast.
 */

/**
 * @typedef {Object} CursorEntry
 * @property {string} key - Unique connection key.
 * @property {any} user - Selected user data.
 * @property {any} data - Latest cursor/position data.
 */

/**
 * @typedef {Object} RedisCursorTracker
 * @property {(ws: any, topic: string, data: any, platform: import('svelte-adapter-uws').Platform) => void} update
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, topic?: string) => Promise<void>} remove
 * @property {(topic: string) => Promise<CursorEntry[]>} list
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop the Redis subscriber
 */

/**
 * Create a Redis-backed cursor tracker.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisCursorOptions} [options]
 * @returns {RedisCursorTracker}
 */
export function createCursor(client, options = {}) {
	const throttleMs = options.throttle ?? 50;
	const select = options.select || ((userData) => userData);
	const cursorTtl = options.ttl || 30;

	if (typeof throttleMs !== 'number' || !Number.isFinite(throttleMs) || throttleMs < 0) {
		throw new Error('redis cursor: throttle must be a non-negative number');
	}
	if (typeof select !== 'function') {
		throw new Error('redis cursor: select must be a function');
	}

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const channel = client.key('cursor:events');

	let connCounter = 0;

	/**
	 * Per-ws state: their key and which topics they have cursor state on.
	 * @type {Map<any, { key: string, user: any, topics: Set<string> }>}
	 */
	const wsState = new Map();

	/**
	 * Per-topic cursor positions (local throttle state).
	 * @type {Map<string, Map<string, { user: any, data: any, lastBroadcast: number, timer: any }>>}
	 */
	const topics = new Map();

	// Redis subscriber for cross-instance relay
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	let subscriberReady = false;

	function ensureSubscriber(platform) {
		activePlatform = platform;
		if (subscriber) return;
		const sub = client.duplicate();
		subscriber = sub;
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				// relay: false -- each worker has its own subscriber,
				// so no need to IPC-relay to sibling workers.
				if (activePlatform) {
					activePlatform.publish(
						'__cursor:' + parsed.topic,
						parsed.event,
						parsed.payload,
						{ relay: false }
					);
				}
			} catch {
				// Malformed, skip
			}
		});
		sub.subscribe(channel).then(() => {
			subscriberReady = true;
		}).catch(() => {
			// Subscribe failed -- clean up so the next call can retry
			sub.quit().catch(() => sub.disconnect());
			if (subscriber === sub) {
				subscriber = null;
				subscriberReady = false;
			}
		});
	}

	const cursorTtlMs = cursorTtl * 1000;

	// Track topics with local activity for periodic stale field cleanup
	/** @type {Set<string>} */
	const activeTopics = new Set();

	// Periodic cleanup: remove stale fields from dead instances
	const cleanupInterval = Math.max(cursorTtlMs, 10000);
	const cleanupTimer = setInterval(() => {
		const now = Date.now();
		for (const topic of activeTopics) {
			redis.hgetall(client.key('cursor:' + topic)).then((all) => {
				if (!all) return;
				for (const [field, v] of Object.entries(all)) {
					try {
						const parsed = JSON.parse(v);
						if (parsed.ts && (now - parsed.ts) > cursorTtlMs) {
							redis.hdel(client.key('cursor:' + topic), field).catch(() => {});
						}
					} catch {
						redis.hdel(client.key('cursor:' + topic), field).catch(() => {});
					}
				}
			}).catch(() => {});
		}
	}, cleanupInterval);
	if (cleanupTimer.unref) cleanupTimer.unref();

	function hashKey(topic) {
		return client.key('cursor:' + topic);
	}

	function getWsState(ws) {
		let state = wsState.get(ws);
		if (!state) {
			state = {
				key: instanceId + ':' + (++connCounter),
				user: select(typeof ws.getUserData === 'function' ? ws.getUserData() : {}),
				topics: new Set()
			};
			wsState.set(ws, state);
		}
		return state;
	}

	/**
	 * Broadcast locally + relay to other instances via Redis.
	 */
	function broadcast(topic, key, user, data, platform) {
		// Local broadcast
		platform.publish('__cursor:' + topic, 'update', { key, user, data });

		// Persist to Redis hash with timestamp for per-entry staleness detection
		const now = Date.now();
		redis.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: now })).catch(() => {});
		redis.expire(hashKey(topic), cursorTtl).catch(() => {});

		// Relay to other instances
		const msg = JSON.stringify({
			instanceId,
			topic,
			event: 'update',
			payload: { key, user, data }
		});
		redis.publish(channel, msg).catch(() => {});
	}

	function broadcastRemove(topic, key, platform) {
		platform.publish('__cursor:' + topic, 'remove', { key });

		redis.hdel(hashKey(topic), key).catch(() => {});

		const msg = JSON.stringify({
			instanceId,
			topic,
			event: 'remove',
			payload: { key }
		});
		redis.publish(channel, msg).catch(() => {});
	}

	return {
		update(ws, topic, data, platform) {
			ensureSubscriber(platform);

			const state = getWsState(ws);
			state.topics.add(topic);
			activeTopics.add(topic);

			let topicMap = topics.get(topic);
			if (!topicMap) {
				topicMap = new Map();
				topics.set(topic, topicMap);
			}

			let entry = topicMap.get(state.key);
			const now = Date.now();

			if (!entry) {
				entry = { user: state.user, data, lastBroadcast: 0, timer: null };
				topicMap.set(state.key, entry);
			}

			// Always store latest data
			entry.data = data;
			entry.user = state.user;

			// Leading edge: broadcast immediately if throttle window passed
			if (now - entry.lastBroadcast >= throttleMs) {
				if (entry.timer) {
					clearTimeout(entry.timer);
					entry.timer = null;
				}
				entry.lastBroadcast = now;
				broadcast(topic, state.key, state.user, data, platform);
				return;
			}

			// Trailing edge: schedule a broadcast for the end of the window
			if (!entry.timer) {
				const key = state.key;
				const user = state.user;
				entry.timer = setTimeout(() => {
					const e = topicMap.get(key);
					if (e) {
						e.lastBroadcast = Date.now();
						e.timer = null;
						broadcast(topic, key, user, e.data, platform);
					}
				}, throttleMs - (now - entry.lastBroadcast));
			}
		},

		async remove(ws, platform, topic) {
			const state = wsState.get(ws);
			if (!state) return;

			if (topic !== undefined) {
				// --- Per-topic remove ---
				if (!state.topics.has(topic)) return;
				state.topics.delete(topic);

				const topicMap = topics.get(topic);
				if (topicMap) {
					const entry = topicMap.get(state.key);
					if (entry) {
						if (entry.timer) clearTimeout(entry.timer);
						topicMap.delete(state.key);
						if (topicMap.size === 0) {
							topics.delete(topic);
							activeTopics.delete(topic);
						}
						broadcastRemove(topic, state.key, platform);
					}
				}

				if (state.topics.size === 0) wsState.delete(ws);
				return;
			}

			// --- Remove from all topics ---
			for (const topic of state.topics) {
				const topicMap = topics.get(topic);
				if (!topicMap) continue;

				const entry = topicMap.get(state.key);
				if (entry) {
					if (entry.timer) clearTimeout(entry.timer);
					topicMap.delete(state.key);
					if (topicMap.size === 0) {
						topics.delete(topic);
						activeTopics.delete(topic);
					}
					broadcastRemove(topic, state.key, platform);
				}
			}

			wsState.delete(ws);
		},

		async list(topic) {
			const all = await redis.hgetall(hashKey(topic));
			const result = [];
			const now = Date.now();
			const ttlMs = cursorTtl * 1000;
			for (const [key, v] of Object.entries(all)) {
				try {
					const parsed = JSON.parse(v);
					// Filter out stale entries from crashed instances
					if (parsed.ts && (now - parsed.ts) > ttlMs) continue;
					result.push({ key, user: parsed.user, data: parsed.data });
				} catch {
					// Corrupted entry, skip
				}
			}
			return result;
		},

		async clear() {
			// Clear all timers
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			topics.clear();
			wsState.clear();
			activeTopics.clear();
			connCounter = 0;

			// Clear Redis keys
			const pattern = client.key('cursor:*');
			let cursor = '0';
			do {
				const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				cursor = nextCursor;
				if (keys.length > 0) {
					await redis.del(...keys);
				}
			} while (cursor !== '0');
		},

		destroy() {
			// Clear all timers
			clearInterval(cleanupTimer);
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscriberReady = false;
			activePlatform = null;
		}
	};
}
