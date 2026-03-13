/**
 * Redis-backed presence tracker for svelte-adapter-uws.
 *
 * Same API as the core createPresence plugin, but stores presence state
 * in Redis hashes so it is shared across instances. Uses Redis pub/sub
 * for cross-instance join/leave notifications.
 *
 * Storage layout per topic:
 *   - Key `{prefix}presence:{topic}` - hash
 *       field = `{instanceId}|{userKey}`, value = JSON `{ data, ts }`
 *       Each instance owns its own fields so cross-instance leave is safe.
 *   - Channel `{prefix}presence:events:{topic}` - pub/sub for join/leave events
 *
 * Each instance also maintains a local connection map so it knows when to
 * publish leave events (last connection for a user on this instance).
 *
 * @module svelte-adapter-uws-extensions/redis/presence
 */

import { randomBytes } from 'node:crypto';

/**
 * Lua script for atomic join: set this instance's field and check if
 * the user already exists on another instance (non-stale).
 *
 * KEYS[1] = hash key
 * ARGV[1] = field to set (instanceId|userKey)
 * ARGV[2] = field value (JSON with data and ts)
 * ARGV[3] = "|userKey" suffix to match
 * ARGV[4] = now (ms)
 * ARGV[5] = presenceTtlMs
 *
 * Returns 1 if this is the first live instance for the user (broadcast join),
 * 0 if another live instance already has this user.
 */
const JOIN_SCRIPT = `
local key = KEYS[1]
local field = ARGV[1]
local value = ARGV[2]
local suffix = ARGV[3]
local now = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])

redis.call('hset', key, field, value)

local all = redis.call('hgetall', key)
for i = 1, #all, 2 do
  local f = all[i]
  if f ~= field and #f >= #suffix and string.sub(f, -#suffix) == suffix then
    local ok, parsed = pcall(cjson.decode, all[i+1])
    if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
      return 0
    end
  end
end
return 1
`;

/**
 * Lua script for atomic leave + check if user is still present on
 * another instance (non-stale). Removes this instance's field and scans
 * remaining fields for the same userKey, ignoring stale entries.
 *
 * KEYS[1] = hash key
 * ARGV[1] = field to remove (instanceId|userKey)
 * ARGV[2] = "|userKey" suffix to match
 * ARGV[3] = now (ms)
 * ARGV[4] = presenceTtlMs
 *
 * Returns 1 if user is completely gone (broadcast leave), 0 if still present.
 */
const LEAVE_SCRIPT = `
local key = KEYS[1]
local field = ARGV[1]
local suffix = ARGV[2]
local now = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])

redis.call('hdel', key, field)

local all = redis.call('hgetall', key)
for i = 1, #all, 2 do
  local f = all[i]
  if #f >= #suffix and string.sub(f, -#suffix) == suffix then
    local ok, parsed = pcall(cjson.decode, all[i+1])
    if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
      return 0
    end
  end
end
return 1
`;

/**
 * @typedef {Object} RedisPresenceOptions
 * @property {string} [key='id'] - Field in selected data for user dedup
 * @property {(userData: any) => Record<string, any>} [select] - Extract public fields from userData
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh expiry)
 * @property {number} [ttl=90] - TTL in seconds for presence entries (should be > heartbeat * 3)
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
	const presenceTtlMs = presenceTtl * 1000;

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

	/**
	 * Local per-topic data cache for heartbeat updates.
	 * @type {Map<string, Map<string, Record<string, any>>>}
	 */
	const localData = new Map();

	/**
	 * Track sync-only ws so leave() can clean up their Redis channel subscriptions.
	 * @type {Map<any, Set<string>>}
	 */
	const syncObservers = new Map();

	/**
	 * Per-topic refcount for sync-only observers.
	 * Used alongside localCounts to decide when to unsubscribe from Redis.
	 * @type {Map<string, number>}
	 */
	const syncCounts = new Map();

	function hashKey(topic) {
		return client.key('presence:' + topic);
	}

	function eventChannel(topic) {
		return client.key('presence:events:' + topic);
	}

	function compoundField(userKey) {
		return instanceId + '|' + userKey;
	}

	function resolveKey(data) {
		if (data && keyField in data && data[keyField] != null) {
			return String(data[keyField]);
		}
		return '__conn:' + (++connCounter);
	}

	/**
	 * Parse hash entries, deduplicate by userKey, filter stale entries.
	 * Returns an array of { key, data } objects.
	 */
	function parseEntries(all) {
		const now = Date.now();
		const seen = new Map(); // userKey -> { data, ts }
		for (const [field, v] of Object.entries(all)) {
			try {
				const parsed = JSON.parse(v);
				// Filter stale entries
				if (parsed.ts && (now - parsed.ts) > presenceTtlMs) continue;
				// Extract userKey from compound field
				const sep = field.indexOf('|');
				const userKey = sep !== -1 ? field.slice(sep + 1) : field;
				// Keep the most recent entry per userKey
				const existing = seen.get(userKey);
				if (!existing || (parsed.ts || 0) > (existing.ts || 0)) {
					seen.set(userKey, parsed);
				}
			} catch {
				// Corrupted entry, skip
			}
		}
		return seen;
	}

	// Heartbeat: refresh timestamps on local entries, TTL on hash keys,
	// and clean up stale fields from crashed instances
	/** @type {Set<string>} */
	const activeTopics = new Set();
	const heartbeatTimer = setInterval(() => {
		const now = Date.now();
		for (const topic of activeTopics) {
			const data = localData.get(topic);
			if (data) {
				for (const [userKey, userData] of data) {
					const field = compoundField(userKey);
					redis.hset(hashKey(topic), field, JSON.stringify({ data: userData, ts: now })).catch(() => {});
				}
			}
			redis.expire(hashKey(topic), presenceTtl).catch(() => {});
			// Clean up stale fields from dead instances
			redis.hgetall(hashKey(topic)).then((all) => {
				if (!all) return;
				for (const [field, v] of Object.entries(all)) {
					try {
						const parsed = JSON.parse(v);
						if (parsed.ts && (now - parsed.ts) > presenceTtlMs) {
							redis.hdel(hashKey(topic), field).catch(() => {});
						}
					} catch { /* corrupted, remove */
						redis.hdel(hashKey(topic), field).catch(() => {});
					}
				}
			}).catch(() => {});
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
		activePlatform = platform;
		if (!subscriber) {
			subscriber = client.duplicate();
			subscriber.on('message', (ch, message) => {
				try {
					const parsed = JSON.parse(message);
					if (parsed.instanceId === instanceId) return;
					// Forward to local platform only -- relay: false prevents
					// duplicate delivery since each worker has its own subscriber.
					if (activePlatform) {
						activePlatform.publish('__presence:' + parsed.topic, parsed.event, parsed.payload, { relay: false });
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

	async function unsubscribeFromTopic(topic) {
		if (!subscriber) return;
		const ch = eventChannel(topic);
		if (subscribedChannels.has(ch)) {
			subscribedChannels.delete(ch);
			await subscriber.unsubscribe(ch).catch(() => {});
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

			// Track local data for heartbeat
			let topicData = localData.get(topic);
			if (!topicData) {
				topicData = new Map();
				localData.set(topic, topicData);
			}
			topicData.set(key, data);

			activeTopics.add(topic);

			// Subscribe to cross-instance events for this topic
			await subscribeToTopic(topic, platform);

			if (prevCount === 0) {
				// New user on this instance -- check if globally new via atomic Lua
				const now = Date.now();
				const field = compoundField(key);
				const value = JSON.stringify({ data, ts: now });
				const suffix = '|' + key;
				const isFirstGlobally = await redis.eval(
					JOIN_SCRIPT, 1, hashKey(topic),
					field, value, suffix, now, presenceTtlMs
				);
				await redis.expire(hashKey(topic), presenceTtl);

				if (isFirstGlobally === 1) {
					// No other live instance has this user -- broadcast join
					const payload = { key, data };
					platform.publish('__presence:' + topic, 'join', payload);
					await publishEvent(topic, 'join', payload);
				}
			}

			// Subscribe ws to presence channel
			ws.subscribe('__presence:' + topic);

			// Send current list to this connection
			const all = await redis.hgetall(hashKey(topic));
			const entries = parseEntries(all);
			const list = [];
			for (const [userKey, entry] of entries) {
				list.push({ key: userKey, data: entry.data });
			}
			platform.send(ws, '__presence:' + topic, 'list', list);
		},

		async leave(ws, platform) {
			// Handle joined users
			const connTopics = wsTopics.get(ws);
			if (connTopics) {
				for (const [topic, { key, data }] of connTopics) {
					const counts = localCounts.get(topic);
					if (!counts) continue;

					const current = counts.get(key) || 0;
					if (current <= 1) {
						counts.delete(key);

						// Clean up local data for this user on this topic
						const topicData = localData.get(topic);
						if (topicData) {
							topicData.delete(key);
							if (topicData.size === 0) localData.delete(topic);
						}

						if (counts.size === 0) {
							localCounts.delete(topic);
							activeTopics.delete(topic);
							// Unsubscribe from Redis channel if no sync observers remain
							if (!syncCounts.has(topic)) {
								await unsubscribeFromTopic(topic);
							}
						}

						// Atomically remove this instance's field and check if user
						// is still present on another instance (ignoring stale entries)
						const field = compoundField(key);
						const suffix = '|' + key;
						const now = Date.now();
						const userGone = await redis.eval(
							LEAVE_SCRIPT, 1, hashKey(topic), field, suffix, now, presenceTtlMs
						);

						if (userGone === 1) {
							// No other instance has this user -- broadcast leave
							const payload = { key, data };
							platform.publish('__presence:' + topic, 'leave', payload);
							await publishEvent(topic, 'leave', payload);
						}
					} else {
						counts.set(key, current - 1);
					}
				}
				wsTopics.delete(ws);
			}

			// Handle sync-only observers
			const syncTopics = syncObservers.get(ws);
			if (syncTopics) {
				for (const topic of syncTopics) {
					const count = (syncCounts.get(topic) || 1) - 1;
					if (count <= 0) {
						syncCounts.delete(topic);
						// Unsubscribe from Redis channel if no joined users remain
						if (!localCounts.has(topic)) {
							await unsubscribeFromTopic(topic);
						}
					} else {
						syncCounts.set(topic, count);
					}
				}
				syncObservers.delete(ws);
			}
		},

		async sync(ws, topic, platform) {
			const all = await redis.hgetall(hashKey(topic));
			const presenceTopic = '__presence:' + topic;
			const entries = parseEntries(all);
			const list = [];
			for (const [userKey, entry] of entries) {
				list.push({ key: userKey, data: entry.data });
			}
			// Subscribe to Redis channel so remote join/leave events are received
			await subscribeToTopic(topic, platform);

			// Track this sync-only observer so leave() can clean up
			if (!wsTopics.has(ws)) {
				let topics = syncObservers.get(ws);
				if (!topics) {
					topics = new Set();
					syncObservers.set(ws, topics);
				}
				if (!topics.has(topic)) {
					topics.add(topic);
					syncCounts.set(topic, (syncCounts.get(topic) || 0) + 1);
				}
			}

			ws.subscribe(presenceTopic);
			platform.send(ws, presenceTopic, 'list', list);
		},

		async list(topic) {
			const all = await redis.hgetall(hashKey(topic));
			const entries = parseEntries(all);
			const result = [];
			for (const entry of entries.values()) {
				result.push(entry.data);
			}
			return result;
		},

		async count(topic) {
			const all = await redis.hgetall(hashKey(topic));
			const entries = parseEntries(all);
			return entries.size;
		},

		async clear() {
			// Unsubscribe all local ws from their presence topics
			for (const [ws, connTopics] of wsTopics) {
				for (const topic of connTopics.keys()) {
					ws.unsubscribe('__presence:' + topic);
				}
			}
			for (const [ws, topics] of syncObservers) {
				for (const topic of topics) {
					ws.unsubscribe('__presence:' + topic);
				}
			}

			// Unsubscribe the Redis subscriber from all event channels
			if (subscriber) {
				for (const ch of subscribedChannels) {
					await subscriber.unsubscribe(ch).catch(() => {});
				}
				subscribedChannels.clear();
			}

			// Clear all presence keys in Redis
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
			localData.clear();
			activeTopics.clear();
			syncObservers.clear();
			syncCounts.clear();
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
