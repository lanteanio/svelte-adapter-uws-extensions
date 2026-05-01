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
import { CLEANUP_SCRIPT } from '../shared/scripts.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';

/** Wire-protocol event names this module emits. */
const EVENTS = Object.freeze({
	UPDATE: 'update',
	REMOVE: 'remove',
	BULK: 'bulk'
});

/**
 * @typedef {Object} RedisCursorOptions
 * @property {number} [throttle=50] - Minimum ms between broadcasts per user per topic.
 *   Trailing-edge timer fires to ensure the final position is always sent.
 * @property {number} [topicThrottle=0] - Minimum ms between aggregate broadcasts per
 *   topic. Caps total Redis writes regardless of connection count. 0 = no limit.
 *   Set to ~16 (60/sec) to prevent Redis saturation under high concurrency.
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
	const topicThrottleMs = options.topicThrottle ?? 0;
	if (options.select != null && typeof options.select !== 'function') {
		throw new Error('redis cursor: select must be a function');
	}
	const select = options.select || stripInternal;
	const cursorTtl = options.ttl ?? 30;

	if (typeof throttleMs !== 'number' || !Number.isFinite(throttleMs) || throttleMs < 0) {
		throw new Error('redis cursor: throttle must be a non-negative number');
	}
	if (typeof topicThrottleMs !== 'number' || !Number.isFinite(topicThrottleMs) || topicThrottleMs < 0) {
		throw new Error('redis cursor: topicThrottle must be a non-negative number');
	}
	if (typeof cursorTtl !== 'number' || !Number.isFinite(cursorTtl) || cursorTtl < 1) {
		throw new Error('redis cursor: ttl must be a positive number (seconds)');
	}

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const channel = client.key('cursor:events');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mUpdates = m?.counter('cursor_updates_total', 'Cursor update calls', ['topic']);
	const mBroadcasts = m?.counter('cursor_broadcasts_total', 'Cursor broadcasts sent', ['topic']);
	const mThrottled = m?.counter('cursor_throttled_total', 'Cursor updates deferred by throttle', ['topic']);

	const warnSensitive = createSensitiveWarner('redis/cursor');

	let connCounter = 0;

	/**
	 * Extract safe user data, stripping internal adapter keys (__subscriptions, remoteAddress).
	 */
	function safeUserData(ws) {
		const raw = typeof ws.getUserData === 'function' ? ws.getUserData() : {};
		if (!raw || typeof raw !== 'object') return {};
		const { __subscriptions, remoteAddress, ...safeData } = raw;
		return safeData;
	}

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
	let subscriberReady = null;

	function ensureSubscriber(platform) {
		activePlatform = platform;
		if (subscriber) return;
		if (b && b.state === 'broken') return;
		const sub = client.duplicate({ enableReadyCheck: false });
		subscriber = sub;
		sub.on('error', (err) => {
			console.error('cursor subscriber error:', err.message);
		});
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
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
		subscriberReady = sub.subscribe(channel).then(() => {
			if (activePlatform && activeTopics.size > 0) {
				for (const topic of activeTopics) {
					redis.hgetall(hashKey(topic)).then((all) => {
						if (!all || !activePlatform) return;
						const now = Date.now();
						const entries = [];
						for (const key of Object.keys(all)) {
							if (key.startsWith(instanceId + ':')) continue;
							try {
								const parsed = JSON.parse(all[key]);
								if (parsed.ts && (now - parsed.ts) <= cursorTtlMs) {
									entries.push({ key, user: parsed.user, data: parsed.data });
								}
							} catch { /* skip */ }
						}
						if (entries.length > 0 && activePlatform) {
							activePlatform.publish('__cursor:' + topic, EVENTS.BULK, entries, { relay: false });
						}
					}).catch(() => {});
				}
			}
		}).catch(() => {
			sub.quit().catch(() => sub.disconnect());
			if (subscriber === sub) {
				subscriber = null;
			}
		}).finally(() => {
			subscriberReady = null;
		});
	}

	const cursorTtlMs = cursorTtl * 1000;

	// Track topics with local activity for periodic stale field cleanup
	/** @type {Set<string>} */
	const activeTopics = new Set();

	const cleanupInterval = Math.max(cursorTtlMs, 10000);
	let cleanupTimer = null;

	function startCleanupTimer() {
		if (cleanupTimer) return;
		cleanupTimer = setInterval(() => {
			const now = Date.now();
			for (const topic of activeTopics) {
				redis.eval(CLEANUP_SCRIPT, 1, hashKey(topic), now, cursorTtlMs).catch((err) => {
					console.warn('cursor cleanup: stale removal failed for topic "' + topic + '":', err.message);
				});
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	function stopCleanupTimer() {
		if (cleanupTimer && activeTopics.size === 0) {
			clearInterval(cleanupTimer);
			cleanupTimer = null;
		}
	}

	function hashKey(topic) {
		return client.key('cursor:' + topic);
	}

	function getWsState(ws) {
		let state = wsState.get(ws);
		if (!state) {
			const user = select(safeUserData(ws));
			try { JSON.stringify(user); } catch {
				throw new Error('redis cursor: select() must return JSON-serializable data');
			}
			warnSensitive(user);
			state = {
				key: instanceId + ':' + (++connCounter),
				user,
				topics: new Set()
			};
			wsState.set(ws, state);
		}
		return state;
	}

	/**
	 * Broadcast locally + relay to other instances via Redis.
	 */
	/**
	 * Per-topic aggregate throttle state.
	 * When topicThrottleMs > 0, excess broadcasts are coalesced and
	 * flushed on a trailing-edge timer so total Redis load per topic
	 * is capped regardless of connection count.
	 * @type {Map<string, { lastFlush: number, timer: any, dirty: Map<string, { user: any, data: any, platform: any }> }>}
	 */
	const topicFlush = new Map();

	function doBroadcast(topic, key, user, data, platform) {
		mBroadcasts?.inc({ topic: mt(topic) });
		const payload = { key, user, data };
		platform.publish('__cursor:' + topic, EVENTS.UPDATE, payload);

		if (b) { try { b.guard(); } catch { return; } }
		const now = Date.now();
		const pipe = redis.pipeline();
		pipe.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: now }));
		pipe.expire(hashKey(topic), cursorTtl);
		pipe.exec().then(() => {
			b?.success();
			const relayMsg = JSON.stringify({ instanceId, topic, event: EVENTS.UPDATE, payload });
			if (subscriberReady) {
				subscriberReady.then(() => redis.publish(channel, relayMsg).catch(() => {}));
			} else {
				redis.publish(channel, relayMsg).catch(() => {});
			}
		}).catch((err) => {
			b?.failure(err);
		});
	}

	/**
	 * Flush all coalesced entries for a topic as a single "bulk" event.
	 * The client receives one event with all cursor positions instead of
	 * N individual events landing in the same microtask. This turns N
	 * store updates per frame into one, and reduces Redis PUBLISH calls
	 * from N to 1 per flush window.
	 *
	 * Each entry is still persisted individually to the Redis hash so
	 * the per-key TTL and staleness detection work unchanged.
	 */
	function flushBulk(topic, dirty) {
		const entries = [];
		const now = Date.now();
		let flushPlatform = null;

		if (b) { try { b.guard(); } catch { return; } }
		const pipe = redis.pipeline();
		for (const [k, v] of dirty) {
			entries.push({ key: k, user: v.user, data: v.data });
			flushPlatform = v.platform;
			pipe.hset(hashKey(topic), k, JSON.stringify({ user: v.user, data: v.data, ts: now }));
		}
		pipe.expire(hashKey(topic), cursorTtl);

		if (flushPlatform) {
			flushPlatform.publish('__cursor:' + topic, EVENTS.BULK, entries);
		}

		pipe.exec().then(() => {
			b?.success();
			if (flushPlatform) {
				const relayMsg = JSON.stringify({ instanceId, topic, event: EVENTS.BULK, payload: entries });
				if (subscriberReady) {
					subscriberReady.then(() => redis.publish(channel, relayMsg).catch(() => {}));
				} else {
					redis.publish(channel, relayMsg).catch(() => {});
				}
			}
		}).catch((err) => { b?.failure(err); });
	}

	function broadcast(topic, key, user, data, platform) {
		if (topicThrottleMs <= 0) {
			doBroadcast(topic, key, user, data, platform);
			return;
		}

		// Per-topic aggregate throttle
		let state = topicFlush.get(topic);
		if (!state) {
			state = { lastFlush: 0, timer: null, dirty: new Map() };
			topicFlush.set(topic, state);
		}

		// Always store the latest data per key
		state.dirty.set(key, { user, data, platform });

		const now = Date.now();

		// Leading edge: flush immediately if window has passed
		if (now - state.lastFlush >= topicThrottleMs) {
			if (state.timer) { clearTimeout(state.timer); state.timer = null; }
			state.lastFlush = now;
			if (state.dirty.size === 1) {
				// Single entry: use normal event so the client does not
				// need to handle bulk for the common non-contended case.
				const [k, v] = state.dirty.entries().next().value;
				doBroadcast(topic, k, v.user, v.data, v.platform);
			} else {
				flushBulk(topic, state.dirty);
			}
			state.dirty.clear();
			return;
		}

		// Trailing edge: schedule flush at end of window
		if (!state.timer) {
			state.timer = setTimeout(() => {
				const s = topicFlush.get(topic);
				if (!s) return;
				s.timer = null;
				s.lastFlush = Date.now();
				if (s.dirty.size === 1) {
					const [k, v] = s.dirty.entries().next().value;
					doBroadcast(topic, k, v.user, v.data, v.platform);
				} else {
					flushBulk(topic, s.dirty);
				}
				s.dirty.clear();
			}, topicThrottleMs - (now - state.lastFlush));
		}
	}

	async function broadcastRemove(topic, key, platform) {
		if (b) { try { b.guard(); } catch { return false; } }

		try {
			await redis.hdel(hashKey(topic), key);
			b?.success();
		} catch (err) {
			b?.failure(err);
			return false;
		}

		platform.publish('__cursor:' + topic, EVENTS.REMOVE, { key });

		const msg = JSON.stringify({
			instanceId,
			topic,
			event: EVENTS.REMOVE,
			payload: { key }
		});
		if (subscriberReady) {
			subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
		} else {
			redis.publish(channel, msg).catch(() => {});
		}
		return true;
	}

	/** @type {RedisCursorTracker} */
	const tracker = {
		update(ws, topic, data, platform) {
			mUpdates?.inc({ topic: mt(topic) });
			ensureSubscriber(platform);

			const state = getWsState(ws);
			state.topics.add(topic);
			if (!activeTopics.has(topic) && activeTopics.size === 0) {
				activeTopics.add(topic);
				startCleanupTimer();
			} else {
				activeTopics.add(topic);
			}

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
			mThrottled?.inc({ topic: mt(topic) });
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

				const topicMap = topics.get(topic);
				if (topicMap) {
					const entry = topicMap.get(state.key);
					if (entry) {
						if (entry.timer) clearTimeout(entry.timer);
						const removed = await broadcastRemove(topic, state.key, platform);
						if (!removed) return;
						topicMap.delete(state.key);
						state.topics.delete(topic);
						if (topicMap.size === 0) {
							topics.delete(topic);
							activeTopics.delete(topic);
							topicFlush.delete(topic);
							stopCleanupTimer();
						}
						const flushState = topicFlush.get(topic);
						if (flushState) {
							flushState.dirty.delete(state.key);
						}
					} else {
						state.topics.delete(topic);
					}
				} else {
					state.topics.delete(topic);
				}

				if (state.topics.size === 0) wsState.delete(ws);
				return;
			}

			// --- Remove from all topics ---
			if (b) { try { b.guard(); } catch { return; } }

			// Clear pending timers before the async pipeline to prevent
			// stale broadcasts for a disconnected ws.
			const removedTopics = [];
			for (const t of state.topics) {
				const topicMap = topics.get(t);
				if (!topicMap) continue;
				const entry = topicMap.get(state.key);
				if (entry) {
					if (entry.timer) clearTimeout(entry.timer);
					entry.timer = null;
					removedTopics.push(t);
				}
			}

			const pipe = redis.pipeline();
			for (const t of removedTopics) {
				pipe.hdel(hashKey(t), state.key);
				pipe.publish(channel, JSON.stringify({
					instanceId, topic: t, event: EVENTS.REMOVE, payload: { key: state.key }
				}));
			}

			try {
				await pipe.exec();
				b?.success();
			} catch (err) {
				b?.failure(err);
				return;
			}

			for (const t of removedTopics) {
				platform.publish('__cursor:' + t, EVENTS.REMOVE, { key: state.key });
				const topicMap = topics.get(t);
				if (topicMap) {
					topicMap.delete(state.key);
					if (topicMap.size === 0) {
						topics.delete(t);
						activeTopics.delete(t);
						topicFlush.delete(t);
					}
				}
				const flushState = topicFlush.get(t);
				if (flushState) {
					flushState.dirty.delete(state.key);
				}
			}
			wsState.delete(ws);
			stopCleanupTimer();
		},

		async snapshot(ws, topic, platform) {
			const cursors = await this.list(topic);
			if (cursors.length > 0) {
				try {
					platform.send(ws, '__cursor:' + topic, EVENTS.BULK, cursors);
				} catch {
					// WebSocket closed before send
				}
			}
		},

		async list(topic) {
			if (b) b.guard();
			let all;
			try {
				all = await redis.hgetall(hashKey(topic));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const result = [];
			const now = Date.now();
			const ttlMs = cursorTtl * 1000;
			for (const key of Object.keys(all)) {
				try {
					const parsed = JSON.parse(all[key]);
					if (!parsed.ts || (now - parsed.ts) > ttlMs) continue;
					result.push({ key, user: parsed.user, data: parsed.data });
				} catch { /* corrupted entry */ }
			}
			return result;
		},

		async clear() {
			b?.guard();
			try {
				await scanAndUnlink(redis, client.key('cursor:*'));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			for (const [, state] of topicFlush) {
				if (state.timer) clearTimeout(state.timer);
			}
			topics.clear();
			topicFlush.clear();
			wsState.clear();
			activeTopics.clear();
			stopCleanupTimer();
			connCounter = 0;
		},

		destroy() {
			// Clear all timers
			if (cleanupTimer) clearInterval(cleanupTimer);
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			for (const [, state] of topicFlush) {
				if (state.timer) clearTimeout(state.timer);
			}
			topicFlush.clear();
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			activePlatform = null;
		},

		hooks: {
			subscribe(ws, topic, { platform }) {
				if (topic.startsWith('__cursor:')) {
					const realTopic = topic.slice('__cursor:'.length);
					return tracker.snapshot(ws, realTopic, platform);
				}
			},
			message(ws, { data, platform }) {
				if (data && data.type === 'cursor' && data.topic && data.data !== undefined) {
					tracker.update(ws, data.topic, data.data, platform);
				}
			},
			close(ws, { platform }) {
				return tracker.remove(ws, platform);
			}
		}
	};

	return tracker;
}
