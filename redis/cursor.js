/**
 * Redis-backed cursor / ephemeral state plugin for svelte-adapter-uws.
 *
 * Same API as the core createCursor plugin, but cursor positions are shared
 * across instances via Redis. Each instance throttles locally (same
 * leading/trailing edge logic as the core), then relays broadcasts through
 * Redis pub/sub so subscribers on other instances see cursor updates.
 *
 * Wire shape (channel `__cursor:{topic}`):
 *   - `catalog`  [{key, user}, ...]   - sent on attach + on subscriber-startup
 *                                       reconcile. Roster of users on this topic.
 *   - `join`     {key, user}          - emitted once per (ws, topic) the first time
 *                                       that ws updates on the topic. Cross-replica.
 *   - `update`   {key, data}          - single-mover position update.
 *   - `bulk`     [{key, data}, ...]   - coalesced multi-mover positions.
 *   - `remove`   {key}                - user is gone (catalog + positions cleared).
 *
 * Separating user metadata (catalog) from per-frame positions cuts the per-flush
 * wire payload from ~100 bytes per cursor to ~16 bytes per cursor, and cuts the
 * Redis pub/sub relay envelope by the same factor. Catalog churn is O(joins +
 * leaves), not O(active-cursor count x rate).
 *
 * Storage layout:
 *   - Hash `{prefix}cursor:{topic}` - field = connectionKey, value = JSON
 *     `{user, data, ts}`. Writes are coalesced onto a `snapshotIntervalMs`
 *     timer; the broadcast path does not write to Redis directly. New joiners
 *     reading the hash see at most `snapshotIntervalMs`-stale data, which is
 *     fine for cursor reconcile.
 *   - Channel `{prefix}cursor:events` - pub/sub for join/update/bulk/remove relay.
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
import { MAX_CURSOR_WS, MAX_CURSOR_TOPICS } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';

/** Wire-protocol event names this module emits. */
const EVENTS = Object.freeze({
	CATALOG: 'catalog',
	JOIN: 'join',
	UPDATE: 'update',
	REMOVE: 'remove',
	BULK: 'bulk'
});

/**
 * @typedef {Object} RedisCursorOptions
 * @property {number} [throttle=16] - Minimum ms between broadcasts per user per topic.
 *   Trailing-edge timer fires to ensure the final position is always sent.
 *   Default 16 (60Hz) matches the world-state tick rate so an individual cursor's
 *   motion stays smooth at the per-peer wire rate set by `topicThrottle`.
 * @property {number} [topicThrottle=16] - World-state tick rate, in ms.
 *   Per-topic aggregate cap on broadcasts: each topic emits at most one frame
 *   per window, carrying the latest position for every cursor that moved.
 *   Bandwidth per peer scales with active-mover count, not with mover-count
 *   times per-mover rate. Default 16 (60Hz) suits typical small-to-medium
 *   rooms; raise to 33 (30Hz) for high-density rooms where wire bytes matter.
 *   0 disables the tick; per-cursor `throttle` then governs broadcast rate.
 * @property {number} [snapshotIntervalMs=100] - How often to flush coalesced
 *   cursor positions to Redis HSET. The wire/relay path runs on the per-flush
 *   cadence (above) and does not wait for HSET; this timer only governs the
 *   Redis snapshot used for new-joiner reconcile and cross-instance startup
 *   reconcile. 100ms staleness on the reconcile path is fine for cursors.
 *   0 disables coalescing and reverts to per-flush HSET (legacy behavior).
 * @property {(userData: any) => any} [select] - Extract user-identifying data from userData.
 *   Defaults to the full userData.
 * @property {number} [ttl=30] - TTL in seconds for hash entries. Should be longer than
 *   the expected gap between updates. Entries are refreshed on every snapshot tick.
 */

/**
 * @typedef {Object} CursorEntry
 * @property {string} key - Unique connection key.
 * @property {any} user - Selected user data.
 * @property {any} data - Latest cursor/position data.
 */

/**
 * @typedef {Object} RedisCursorTracker
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} attach
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => void} detach
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
	const throttleMs = options.throttle ?? 16;
	const topicThrottleMs = options.topicThrottle ?? 16;
	const snapshotIntervalMs = options.snapshotIntervalMs ?? 100;
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
	if (typeof snapshotIntervalMs !== 'number' || !Number.isFinite(snapshotIntervalMs) || snapshotIntervalMs < 0) {
		throw new Error('redis cursor: snapshotIntervalMs must be a non-negative number');
	}
	if (typeof cursorTtl !== 'number' || !Number.isFinite(cursorTtl) || cursorTtl < 1) {
		throw new Error('redis cursor: ttl must be a positive number (seconds)');
	}

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const channel = client.key('cursor:events');

	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: false,
		allowedSystemTopics: []
	});

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mUpdates = m?.counter('cursor_updates_total', 'Cursor update calls', ['topic']);
	const mBroadcasts = m?.counter('cursor_broadcasts_total', 'Cursor broadcasts sent', ['topic']);
	const mThrottled = m?.counter('cursor_throttled_total', 'Cursor updates deferred by throttle', ['topic']);

	const warnSensitive = createSensitiveWarner('redis/cursor');

	let connCounter = 0;

	function safeUserData(ws) {
		const raw = typeof ws.getUserData === 'function' ? ws.getUserData() : {};
		if (!raw || typeof raw !== 'object') return {};
		const { __subscriptions, remoteAddress, ...safeData } = raw;
		return safeData;
	}

	/**
	 * Per-ws state: connection key, selected user data, and which topics this ws
	 * has already announced (`join` emitted). `topics` doubles as the
	 * already-announced set - presence in the set means a join has fired.
	 * @type {Map<any, { key: string, user: any, topics: Set<string> }>}
	 */
	const wsState = new Map();

	/**
	 * Per-topic local cursor state. Drives the per-(ws,topic) throttle and the
	 * post-disconnect timer cleanup. The Redis snapshot is the cross-replica
	 * source of truth; this map is the local-replica cache.
	 * @type {Map<string, Map<string, { user: any, data: any, lastBroadcast: number, timer: any }>>}
	 */
	const topics = new Map();

	/**
	 * Coalesced HSET writes. Latest-wins per (topic, key). Drained on the
	 * `snapshotIntervalMs` timer into a single `pipe.hset(topic, f1, v1, ...)`
	 * per topic per tick. The broadcast/relay path queues entries here but
	 * does not await the write.
	 * @type {Map<string, Map<string, { user: any, data: any, ts: number }>>}
	 */
	let redisPending = new Map();

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
			if (!validator.acceptRaw(message)) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (!validator.acceptEnvelope(parsed.topic, parsed.event)) return;
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
		subscriberReady = sub.subscribe(channel).then(async () => {
			if (!activePlatform || activeTopics.size === 0) return;
			const topicList = [...activeTopics];
			const pipe = redis.pipeline();
			for (const topic of topicList) pipe.hgetall(hashKey(topic));
			let results;
			try {
				results = await pipe.exec();
			} catch { return; }
			if (!activePlatform) return;
			const now = Date.now();
			for (let i = 0; i < topicList.length; i++) {
				const [, all] = results[i];
				if (!all) continue;
				const topic = topicList[i];
				const catalogEntries = [];
				const positionEntries = [];
				for (const key of Object.keys(all)) {
					if (key.startsWith(instanceId + ':')) continue;
					try {
						const parsed = JSON.parse(all[key]);
						if (parsed.ts && (now - parsed.ts) <= cursorTtlMs) {
							catalogEntries.push({ key, user: parsed.user });
							positionEntries.push({ key, data: parsed.data });
						}
					} catch { /* skip */ }
				}
				if (catalogEntries.length > 0 && activePlatform) {
					activePlatform.publish('__cursor:' + topic, EVENTS.CATALOG, catalogEntries, { relay: false });
					activePlatform.publish('__cursor:' + topic, EVENTS.BULK, positionEntries, { relay: false });
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

	/** @type {Set<string>} */
	const activeTopics = new Set();

	const cleanupInterval = Math.max(cursorTtlMs, 10000);
	let cleanupTimer = null;
	let snapshotTimer = null;

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
		if (snapshotTimer && activeTopics.size === 0) {
			clearInterval(snapshotTimer);
			snapshotTimer = null;
		}
	}

	function startSnapshotTimer() {
		if (snapshotTimer || snapshotIntervalMs === 0) return;
		snapshotTimer = setInterval(flushSnapshot, snapshotIntervalMs);
		if (snapshotTimer.unref) snapshotTimer.unref();
	}

	function flushSnapshot() {
		if (redisPending.size === 0) return;
		if (b) { try { b.guard(); } catch { redisPending = new Map(); return; } }
		const pending = redisPending;
		redisPending = new Map();
		const pipe = redis.pipeline();
		let queued = 0;
		for (const [topic, entries] of pending) {
			if (entries.size === 0) continue;
			const args = [];
			for (const [key, entry] of entries) {
				args.push(key, JSON.stringify({ user: entry.user, data: entry.data, ts: entry.ts }));
			}
			pipe.hset(hashKey(topic), ...args);
			pipe.expire(hashKey(topic), cursorTtl);
			queued += entries.size;
		}
		if (queued === 0) return;
		pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
	}

	function queueSnapshot(topic, key, user, data) {
		if (snapshotIntervalMs === 0) {
			if (b) { try { b.guard(); } catch { return; } }
			const pipe = redis.pipeline();
			pipe.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: Date.now() }));
			pipe.expire(hashKey(topic), cursorTtl);
			pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
			return;
		}
		let topicPending = redisPending.get(topic);
		if (!topicPending) {
			topicPending = new Map();
			redisPending.set(topic, topicPending);
		}
		topicPending.set(key, { user, data, ts: Date.now() });
	}

	function hashKey(topic) {
		return client.key('cursor:' + topic);
	}

	function getWsState(ws) {
		let state = wsState.get(ws);
		if (!state) {
			if (wsState.size >= MAX_CURSOR_WS) {
				throw new Error(
					`redis cursor: local ws count exceeded ${MAX_CURSOR_WS} on this instance`
				);
			}
			const selected = select(safeUserData(ws));
			warnSensitive(selected);
			const user = stripInternal(selected);
			try { JSON.stringify(user); } catch {
				throw new Error('redis cursor: select() must return JSON-serializable data');
			}
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
	 * Per-topic aggregate throttle state.
	 * @type {Map<string, { lastFlush: number, timer: any, dirty: Map<string, { user: any, data: any, platform: any }> }>}
	 */
	const topicFlush = new Map();

	function relay(topic, event, payload) {
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		if (subscriberReady) {
			subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
		} else {
			redis.publish(channel, msg).catch(() => {});
		}
	}

	function emitJoin(topic, key, user, platform) {
		const payload = { key, user };
		platform.publish('__cursor:' + topic, EVENTS.JOIN, payload);
		relay(topic, EVENTS.JOIN, payload);
	}

	function doBroadcast(topic, key, user, data, platform) {
		mBroadcasts?.inc({ topic: mt(topic) });
		const payload = { key, data };
		platform.publish('__cursor:' + topic, EVENTS.UPDATE, payload);
		queueSnapshot(topic, key, user, data);
		relay(topic, EVENTS.UPDATE, payload);
	}

	/**
	 * Flush all coalesced entries for a topic as a single `bulk` event.
	 * Entries carry `{key, data}` only; `user` lives on the catalog channel.
	 * Per-entry Redis snapshot writes are coalesced through `queueSnapshot`
	 * onto the snapshot timer.
	 */
	function flushBulk(topic, dirty) {
		const entries = [];
		let flushPlatform = null;
		for (const [k, v] of dirty) {
			entries.push({ key: k, data: v.data });
			flushPlatform = v.platform;
			queueSnapshot(topic, k, v.user, v.data);
		}
		if (!flushPlatform || entries.length === 0) return;
		flushPlatform.publish('__cursor:' + topic, EVENTS.BULK, entries);
		relay(topic, EVENTS.BULK, entries);
	}

	function broadcast(topic, key, user, data, platform) {
		if (topicThrottleMs <= 0) {
			doBroadcast(topic, key, user, data, platform);
			return;
		}

		let state = topicFlush.get(topic);
		if (!state) {
			state = { lastFlush: 0, timer: null, dirty: new Map() };
			topicFlush.set(topic, state);
		}

		state.dirty.set(key, { user, data, platform });

		const now = Date.now();

		if (now - state.lastFlush >= topicThrottleMs) {
			if (state.timer) { clearTimeout(state.timer); state.timer = null; }
			state.lastFlush = now;
			if (state.dirty.size === 1) {
				const [k, v] = state.dirty.entries().next().value;
				doBroadcast(topic, k, v.user, v.data, v.platform);
			} else {
				flushBulk(topic, state.dirty);
			}
			state.dirty.clear();
			return;
		}

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

		// Drop any pending snapshot write for this key so we do not
		// resurrect a removed cursor on the next snapshot tick.
		const topicPending = redisPending.get(topic);
		if (topicPending) {
			topicPending.delete(key);
			if (topicPending.size === 0) redisPending.delete(topic);
		}

		platform.publish('__cursor:' + topic, EVENTS.REMOVE, { key });
		relay(topic, EVENTS.REMOVE, { key });
		return true;
	}

	/** @type {RedisCursorTracker} */
	const tracker = {
		async attach(ws, topic, platform) {
			try {
				platform.subscribe(ws, '__cursor:' + topic);
			} catch {
				return;
			}
			await tracker.snapshot(ws, topic, platform);
		},

		detach(ws, topic, platform) {
			try {
				platform.unsubscribe(ws, '__cursor:' + topic);
			} catch { /* closed */ }
		},

		update(ws, topic, data, platform) {
			mUpdates?.inc({ topic: mt(topic) });
			ensureSubscriber(platform);

			const state = getWsState(ws);
			const isFirstOnTopic = !state.topics.has(topic);
			state.topics.add(topic);
			if (!activeTopics.has(topic) && activeTopics.size === 0) {
				activeTopics.add(topic);
				startCleanupTimer();
				startSnapshotTimer();
			} else {
				activeTopics.add(topic);
				startSnapshotTimer();
			}

			let topicMap = topics.get(topic);
			if (!topicMap) {
				if (topics.size >= MAX_CURSOR_TOPICS) {
					throw new Error(
						`redis cursor: local topic count exceeded ${MAX_CURSOR_TOPICS} on this instance`
					);
				}
				topicMap = new Map();
				topics.set(topic, topicMap);
			}

			if (isFirstOnTopic) {
				emitJoin(topic, state.key, state.user, platform);
			}

			let entry = topicMap.get(state.key);
			const now = Date.now();

			if (!entry) {
				entry = { user: state.user, data, lastBroadcast: 0, timer: null };
				topicMap.set(state.key, entry);
			}

			entry.data = data;
			entry.user = state.user;

			if (now - entry.lastBroadcast >= throttleMs) {
				if (entry.timer) {
					clearTimeout(entry.timer);
					entry.timer = null;
				}
				entry.lastBroadcast = now;
				broadcast(topic, state.key, state.user, data, platform);
				return;
			}

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
							redisPending.delete(topic);
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

			if (b) { try { b.guard(); } catch { return; } }

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
						redisPending.delete(t);
					}
				}
				const flushState = topicFlush.get(t);
				if (flushState) {
					flushState.dirty.delete(state.key);
				}
				const topicPending = redisPending.get(t);
				if (topicPending) {
					topicPending.delete(state.key);
					if (topicPending.size === 0) redisPending.delete(t);
				}
			}
			wsState.delete(ws);
			stopCleanupTimer();
		},

		async snapshot(ws, topic, platform) {
			const cursors = await this.list(topic);
			if (cursors.length === 0) return;
			const catalog = cursors.map((c) => ({ key: c.key, user: c.user }));
			const positions = cursors.map((c) => ({ key: c.key, data: c.data }));
			try {
				platform.send(ws, '__cursor:' + topic, EVENTS.CATALOG, catalog);
				platform.send(ws, '__cursor:' + topic, EVENTS.BULK, positions);
			} catch {
				// WebSocket closed before send
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
			// Include locally-pending entries that have not yet been flushed
			// to Redis. Without this, a list() call between a broadcast and
			// the next snapshot tick under-reports the cursor we just saw.
			const topicPending = redisPending.get(topic);
			if (topicPending) {
				const seen = new Set(result.map((r) => r.key));
				for (const [key, entry] of topicPending) {
					if (seen.has(key)) continue;
					if (!entry.ts || (now - entry.ts) > ttlMs) continue;
					result.push({ key, user: entry.user, data: entry.data });
				}
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
			redisPending = new Map();
			stopCleanupTimer();
			connCounter = 0;
		},

		destroy() {
			if (cleanupTimer) clearInterval(cleanupTimer);
			cleanupTimer = null;
			if (snapshotTimer) clearInterval(snapshotTimer);
			snapshotTimer = null;
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
					return;
				}
				// Client-initiated reconnect-snapshot. The cursor plugin client
				// sends `{type:'cursor-snapshot', topic}` on every status==='open'
				// (initial connect + reconnect). Pre-fix, this text frame had no
				// server handler and was a dead wire frame; the snapshot path
				// only fired through `hooks.subscribe` -> `tracker.snapshot` when
				// the ws subscribed to the `__cursor:{topic}` channel. With this
				// branch, the snapshot also re-emits on the explicit frame so a
				// reconnecting tab that resubscribes via `subscribe-batch` (which
				// the adapter dedups when the topic is already in the user data
				// set) still gets a fresh catalog + bulk.
				if (data && data.type === 'cursor-snapshot' && typeof data.topic === 'string') {
					tracker.snapshot(ws, data.topic, platform);
					return;
				}
				_warnCursorHooksMessageShape(data);
			},
			close(ws, { platform }) {
				return tracker.remove(ws, platform);
			}
		}
	};

	return tracker;
}

/**
 * One-time dev-warn dedup for `cursor.hooks.message` shape misuse. The most
 * common cause is wiring the hook against `createMessage({ onUnhandled })`
 * which passes raw bytes, not a parsed envelope. The fix is to switch to
 * `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` (svelte-
 * realtime >= 0.5.9 + svelte-adapter-uws >= 0.5.3), which forwards the
 * parsed object directly.
 */
let _cursorHooksMessageBadShapeWarned = false;

/**
 * @param {any} data
 */
function _warnCursorHooksMessageShape(data) {
	if (_cursorHooksMessageBadShapeWarned) return;
	_cursorHooksMessageBadShapeWarned = true;
	const got = data instanceof ArrayBuffer
		? 'ArrayBuffer (raw bytes -- did you wire this from createMessage({onUnhandled}) ?)'
		: Array.isArray(data)
			? 'Array'
			: data === null
				? 'null'
				: typeof data === 'object'
					? 'object with data.type=' + String(data.type)
					: typeof data;
	console.warn(
		'[redis/cursor] hooks.message called with unexpected shape (' + got + '). ' +
		'Expected a parsed object {type:"cursor", topic, data} or ' +
		'{type:"cursor-snapshot", topic}. ' +
		'If you wired this from `createMessage({ onUnhandled })` and got raw bytes, ' +
		'switch to `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` ' +
		'which forwards the parsed JSON envelope. ' +
		'This warning fires once per process.\n' +
		'  See: https://svti.me/cursor-hooks-message'
	);
}
