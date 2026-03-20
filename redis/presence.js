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
import { now as cachedNow } from '../shared/time.js';
import { CLEANUP_SCRIPT, COUNT_DEDUP_SCRIPT, LIST_SCRIPT } from '../shared/scripts.js';

/**
 * Lua script for atomic join: set this instance's field and expire the key.
 *
 * KEYS[1] = hash key
 * ARGV[1] = field to set (instanceId|userKey)
 * ARGV[2] = field value (JSON with data and ts)
 * ARGV[3] = presenceTtl (seconds, for EXPIRE)
 *
 * Cross-instance dedup is intentionally omitted. The local localCounts
 * map already prevents duplicate joins from the same user on the same
 * instance (the script only runs when prevCount === 0). If the same
 * user joins on a second instance, both broadcast "join" -- the client
 * handles this as an idempotent Map.set on the same key.
 *
 * Removing the HGETALL + O(N) scan that was here before drops per-join
 * Redis work from O(N) to O(1), fixing the O(N^2) total cost that
 * killed the server at 2000+ concurrent joins.
 */
const JOIN_SCRIPT = `
local key = KEYS[1]
local field = ARGV[1]
local value = ARGV[2]
local ttlSec = tonumber(ARGV[3])

redis.call('hset', key, field, value)
redis.call('expire', key, ttlSec)
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
  if string.find(f, suffix, #f - #suffix + 1, true) then
    local ok, parsed = pcall(cjson.decode, all[i+1])
    if ok and parsed.ts and (now - parsed.ts) <= ttlMs then
      return 0
    end
  end
end
return 1
`;

const SENSITIVE_STRIP_RE = /token|secret|password|auth|session|cookie|jwt|credential/i;

function stripInternal(obj, ancestors) {
	if (!obj || typeof obj !== 'object') return obj;
	if (!ancestors) ancestors = new WeakSet();
	if (ancestors.has(obj)) return undefined;
	ancestors.add(obj);
	let result;
	if (Array.isArray(obj)) {
		result = obj.map((v) => stripInternal(v, ancestors));
	} else {
		result = {};
		for (const k of Object.keys(obj)) {
			if (k.startsWith('__') || SENSITIVE_STRIP_RE.test(k)) continue;
			const v = obj[k];
			result[k] = (v && typeof v === 'object') ? stripInternal(v, ancestors) : v;
		}
	}
	ancestors.delete(obj);
	return result;
}

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
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, topic?: string) => Promise<void>} leave
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} sync
 * @property {(topic: string) => Promise<Array<Record<string, any>>>} list
 * @property {(topic: string) => Promise<number>} count
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop heartbeat and subscriber
 * @property {{ subscribe: (ws: any, topic: string, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void>, unsubscribe: (ws: any, topic: string, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void>, close: (ws: any, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void> }} hooks
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
	if (options.select != null && typeof options.select !== 'function') {
		throw new Error('redis presence: select must be a function');
	}
	const select = options.select || stripInternal;
	const heartbeatInterval = options.heartbeat ?? 30000;
	const presenceTtl = options.ttl ?? 90;
	if (typeof heartbeatInterval !== 'number' || !Number.isFinite(heartbeatInterval) || heartbeatInterval < 1) {
		throw new Error('redis presence: heartbeat must be a positive number (ms)');
	}
	if (typeof presenceTtl !== 'number' || !Number.isFinite(presenceTtl) || presenceTtl < 1) {
		throw new Error('redis presence: ttl must be a positive number (seconds)');
	}
	const presenceTtlMs = presenceTtl * 1000;

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mJoins = m?.counter('presence_joins_total', 'Presence join events', ['topic']);
	const mLeaves = m?.counter('presence_leaves_total', 'Presence leave events', ['topic']);
	const mHeartbeats = m?.counter('presence_heartbeats_total', 'Heartbeat refresh cycles');
	const mStaleCleaned = m?.counter('presence_stale_cleaned_total', 'Stale entries removed by cleanup');

	const SENSITIVE_RE = /token|secret|password|key|auth|session|cookie|jwt|credential/i;
	let sensitiveWarned = false;

	function warnSensitive(data, depth) {
		if (sensitiveWarned || !data || typeof data !== 'object') return;
		if (depth === undefined) depth = 0;
		if (depth > 3) return;
		if (Array.isArray(data)) {
			for (let i = 0; i < data.length; i++) {
				warnSensitive(data[i], depth + 1);
				if (sensitiveWarned) return;
			}
			return;
		}
		for (const k of Object.keys(data)) {
			if (SENSITIVE_RE.test(k)) {
				console.warn(
					`[redis/presence] userData key "${k}" looks sensitive — ` +
					'use the select option to strip it before broadcasting'
				);
				sensitiveWarned = true;
				return;
			}
			if (typeof data[k] === 'object' && data[k] !== null) {
				warnSensitive(data[k], depth + 1);
				if (sensitiveWarned) return;
			}
		}
	}

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
	 * @type {Map<string, Map<string, { data: Record<string, any>, serialized: string, field: string }>>}
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

	/**
	 * Dedup in-flight HGETALL requests for the same topic. Multiple callers
	 * awaiting the same key share one Redis round trip.
	 * @type {Map<string, Promise<Record<string, string>>>}
	 */
	const hgetallInflight = new Map();

	function hashKey(topic) {
		return client.key('presence:' + topic);
	}

	function coalesceHgetall(topic) {
		const key = hashKey(topic);
		let pending = hgetallInflight.get(key);
		if (!pending) {
			pending = redis.hgetall(key).finally(() => hgetallInflight.delete(key));
			hgetallInflight.set(key, pending);
		}
		return pending;
	}

	function eventChannel(topic) {
		return client.key('presence:events:' + topic);
	}

	function compoundField(userKey) {
		return instanceId + '|' + userKey;
	}

	/**
	 * Shallow-then-deep equality check for presence data objects.
	 * Avoids redundant Redis writes and broadcasts when a user's
	 * data has not actually changed.
	 */
	function deepEqual(a, b) {
		if (a === b) return true;
		if (a == null || b == null) return a === b;
		if (typeof a !== 'object' || typeof b !== 'object') return false;
		const keysA = Object.keys(a);
		const keysB = Object.keys(b);
		if (keysA.length !== keysB.length) return false;
		for (let i = 0; i < keysA.length; i++) {
			const k = keysA[i];
			if (!deepEqual(a[k], b[k])) return false;
		}
		return true;
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
		const now = cachedNow();
		const seen = new Map(); // userKey -> { data, ts }
		for (const [field, v] of Object.entries(all)) {
			try {
				const parsed = JSON.parse(v);
				// Filter stale or timestamp-less entries
				if (!parsed.ts || (now - parsed.ts) > presenceTtlMs) continue;
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
		mHeartbeats?.inc();
		// Detect dead connections whose close handler never fired.
		// Under mass disconnect, the runtime may drop close events.
		// Probe each tracked ws; if the probe throws the socket is
		// dead and we synchronously purge it from local state so the
		// refresh loop below never touches it.
		if (activePlatform) {
			const dead = [];
			for (const [ws] of wsTopics) {
				try { ws.getBufferedAmount(); } catch { dead.push(ws); }
			}
			for (const ws of dead) {
				// Full leave (sync Phase 1 + async Phase 2 fire-and-forget)
				tracker.leave(ws, activePlatform).catch(() => {});
			}
		}

		if (b && !b.isHealthy) return;
		const now = cachedNow();
		const pipe = redis.pipeline();
		for (const topic of activeTopics) {
			const data = localData.get(topic);
			const hkey = hashKey(topic);
			if (data) {
				for (const [, entry] of data) {
					pipe.hset(hkey, entry.field, '{"data":' + entry.serialized + ',"ts":' + now + '}');
				}
			}
			pipe.expire(hkey, presenceTtl);

			if (mStaleCleaned) {
				redis.eval(CLEANUP_SCRIPT, 1, hkey, now, presenceTtlMs).then((cleaned) => {
					if (cleaned > 0) mStaleCleaned.inc(cleaned);
				}).catch(() => {});
			} else {
				pipe.eval(CLEANUP_SCRIPT, 1, hkey, now, presenceTtlMs);
			}

			if (activePlatform && data && data.size > 0) {
				const keys = [...data.keys()];
				activePlatform.publish('__presence:' + topic, 'heartbeat', keys);
			}
		}
		pipe.exec().catch(() => {});
	}, heartbeatInterval);
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Redis subscriber for cross-instance join/leave events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	/** @type {Set<string>} - channels we have subscribed to */
	const subscribedChannels = new Set();
	let idleTimer = null;

	async function ensureSubscriber(platform) {
		activePlatform = platform;
		if (!subscriber) {
			subscriber = client.duplicate({ enableReadyCheck: false });
			subscriber.on('error', (err) => {
				console.error('presence subscriber error:', err.message);
			});
			subscriber.on('message', (ch, message) => {
				try {
					const parsed = JSON.parse(message);
					if (parsed.instanceId === instanceId) return;
					const prefix = client.key('presence:events:');
					if (!ch.startsWith(prefix)) return;
					const topic = ch.slice(prefix.length);
					if (activePlatform) {
						activePlatform.publish('__presence:' + topic, parsed.event, parsed.payload, { relay: false });
					}
				} catch {
					// Malformed, skip
				}
			});
		}
	}

	async function subscribeToTopic(topic, platform) {
		if (idleTimer) {
			clearTimeout(idleTimer);
			idleTimer = null;
		}
		await ensureSubscriber(platform);
		if (!subscriber) return;
		const ch = eventChannel(topic);
		if (!subscribedChannels.has(ch)) {
			await subscriber.subscribe(ch);
			subscribedChannels.add(ch);
		}
	}

	async function unsubscribeFromTopic(topic) {
		if (!subscriber) return;
		const ch = eventChannel(topic);
		if (subscribedChannels.has(ch)) {
			subscribedChannels.delete(ch);
			await subscriber.unsubscribe(ch).catch(() => {});
		}
		if (subscribedChannels.size === 0 && subscriber) {
			if (!idleTimer) {
				idleTimer = setTimeout(() => {
					idleTimer = null;
					if (subscribedChannels.size === 0 && subscriber) {
						subscriber.quit().catch(() => subscriber.disconnect());
						subscriber = null;
					}
				}, 30000);
				if (idleTimer.unref) idleTimer.unref();
			}
		}
	}

	async function publishEvent(topic, event, payload) {
		const ch = eventChannel(topic);
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		await redis.publish(ch, msg).catch(() => {});
	}

	/**
	 * Full undo of a staged join. Rolls back local state, cleans up the
	 * Redis hash field if it was written, publishes a compensating leave
	 * event if a join was broadcast, and unsubscribes from the topic's
	 * Redis channel when no local observers remain.
	 */
	async function undoJoin(ws, topic, key, data, prevCount, prevData, didRedisWrite, didPublishJoin, platform) {
		const connTopics = wsTopics.get(ws);
		if (connTopics) {
			connTopics.delete(topic);
			if (connTopics.size === 0) wsTopics.delete(ws);
		}
		const counts = localCounts.get(topic);
		if (counts) {
			if (prevCount === 0) {
				counts.delete(key);
			} else {
				counts.set(key, prevCount);
			}
			if (counts.size === 0) {
				localCounts.delete(topic);
				activeTopics.delete(topic);
			}
		}
		const topicData = localData.get(topic);
		if (topicData) {
			if (prevData !== undefined) {
				topicData.set(key, { data: prevData, serialized: JSON.stringify(prevData), field: compoundField(key) });
			} else {
				topicData.delete(key);
			}
			if (topicData.size === 0) localData.delete(topic);
		}
		if (prevCount > 0 && prevData !== undefined) {
			const prevValue = JSON.stringify({ data: prevData, ts: Date.now() });
			await redis.hset(hashKey(topic), compoundField(key), prevValue);
		} else {
			await redis.hdel(hashKey(topic), compoundField(key));
		}
		if (didPublishJoin) {
			mLeaves?.inc({ topic: mt(topic) });
			const payload = { key, data };
			platform.publish('__presence:' + topic, 'leave', payload);
			await publishEvent(topic, 'leave', payload);
		}
		if (!localCounts.has(topic) && !syncCounts.has(topic)) {
			await unsubscribeFromTopic(topic);
		}
	}

	/** @type {RedisPresenceTracker} */
	const tracker = {
		async join(ws, topic, platform) {
			if (topic.startsWith('__')) return;

			let connTopics = wsTopics.get(ws);
			if (connTopics && connTopics.has(topic)) return;

			const raw = ws.getUserData();
			const { __subscriptions, remoteAddress, ...safeData } = raw || {};
			const key = resolveKey(safeData);
			const data = select(safeData);
			try { JSON.stringify(data); } catch {
				throw new Error('redis presence: select() must return JSON-serializable data');
			}
			warnSensitive(data);

			// Snapshot state for rollback
			const existingCounts = localCounts.get(topic);
			const prevCount = existingCounts ? (existingCounts.get(key) || 0) : 0;
			const existingTopicData = localData.get(topic);
			const prevEntry = existingTopicData ? existingTopicData.get(key) : undefined;
			const prevData = prevEntry ? prevEntry.data : undefined;

			// Stage local state for dedup and refcounting only.
			// localData and activeTopics are deferred until the join is
			// fully committed so the heartbeat cannot write a ghost entry
			// to Redis during any async gap.
			if (!connTopics) {
				connTopics = new Map();
				wsTopics.set(ws, connTopics);
			}
			connTopics.set(topic, { key, data });

			let counts = existingCounts;
			if (!counts) {
				counts = new Map();
				localCounts.set(topic, counts);
			}
			counts.set(key, prevCount + 1);

			try {
				b?.guard();
			} catch (err) {
				await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
				throw err;
			}

			try {
				await subscribeToTopic(topic, platform);
			} catch (err) {
				b?.failure(err);
				await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
				throw err;
			}

			if (!wsTopics.has(ws)) return;

			try { ws.getBufferedAmount(); } catch {
				await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
				return;
			}

			let didRedisWrite = false;
			let isNewUser = false;

			if (prevCount === 0) {
				const now = Date.now();
				const field = compoundField(key);
				const value = JSON.stringify({ data, ts: now });
				try {
					await redis.eval(
						JOIN_SCRIPT, 1, hashKey(topic),
						field, value, presenceTtl
					);
					didRedisWrite = true;
					isNewUser = true;
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
					throw err;
				}

				if (!wsTopics.has(ws)) {
					await redis.hdel(hashKey(topic), field).catch(() => {});
					return;
				}
			} else if (prevData !== undefined && !deepEqual(prevData, data)) {
				try {
					const now = Date.now();
					const field = compoundField(key);
					const value = JSON.stringify({ data, ts: now });
					await redis.hset(hashKey(topic), field, value);
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
					throw err;
				}

				const td = localData.get(topic);
				if (td) {
					td.set(key, { data, serialized: JSON.stringify(data), field: compoundField(key) });
				}

				const payload = { key, data };
				platform.publish('__presence:' + topic, 'updated', payload);
				await publishEvent(topic, 'updated', payload);
			}

			let all;
			try {
				all = await coalesceHgetall(topic);
				b?.success();
			} catch (err) {
				b?.failure(err);
				await undoJoin(ws, topic, key, data, prevCount, prevData, didRedisWrite, false, platform);
				throw err;
			}

			// Subscribe ws to presence channel (may have closed during async gap)
			try {
				ws.subscribe('__presence:' + topic);
			} catch {
				await undoJoin(ws, topic, key, data, prevCount, prevData, didRedisWrite, false, platform);
				return;
			}

			// If ws closed after subscribe, leave() already handled
			// local cleanup and leave events. Just clean the Redis field.
			if (!wsTopics.has(ws)) {
				if (didRedisWrite) {
					redis.hdel(hashKey(topic), compoundField(key)).catch(() => {});
				}
				return;
			}

			// Commit localData and activeTopics now that the join is
			// fully committed. The heartbeat reads from these, so they
			// must not be visible during any of the async gaps above.
			let topicData = localData.get(topic);
			if (!topicData) {
				topicData = new Map();
				localData.set(topic, topicData);
			}
			topicData.set(key, { data, serialized: JSON.stringify(data), field: compoundField(key) });
			activeTopics.add(topic);

			// Publish join event only after the operation is fully committed.
			// This prevents orphaned join events when the snapshot or subscribe
			// step fails, eliminating the need for compensating leave events.
			if (isNewUser) {
				mJoins?.inc({ topic: mt(topic) });
				const payload = { key, data };
				platform.publish('__presence:' + topic, 'join', payload);
				await publishEvent(topic, 'join', payload);
			}

			// Send current list to this connection
			const entries = parseEntries(all);
			const list = [];
			for (const [userKey, entry] of entries) {
				list.push({ key: userKey, data: entry.data });
			}
			try {
				platform.send(ws, '__presence:' + topic, 'list', list);
			} catch {
				// WebSocket closed before send
			}
		},

		async leave(ws, platform, topic) {
			if (topic !== undefined) {
				// --- Per-topic leave ---
				const connTopics = wsTopics.get(ws);
				if (connTopics && connTopics.has(topic)) {
					const { key, data } = connTopics.get(topic);
					connTopics.delete(topic);
					if (connTopics.size === 0) wsTopics.delete(ws);

					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }

					const counts = localCounts.get(topic);
					if (counts) {
						const current = counts.get(key) || 0;
						if (current <= 1) {
							counts.delete(key);

							const topicData = localData.get(topic);
							if (topicData) {
								topicData.delete(key);
								if (topicData.size === 0) localData.delete(topic);
							}

							if (counts.size === 0) {
								localCounts.delete(topic);
								activeTopics.delete(topic);
								if (!syncCounts.has(topic)) {
									await unsubscribeFromTopic(topic);
								}
							}

							const field = compoundField(key);
							const suffix = '|' + key;
							const now = Date.now();
							let userGone = -1;
							let skipLeaveRedis = false;
							if (b) { try { b.guard(); } catch { skipLeaveRedis = true; } }
							if (!skipLeaveRedis) {
								try {
									userGone = await redis.eval(
										LEAVE_SCRIPT, 1, hashKey(topic), field, suffix, now, presenceTtlMs
									);
									b?.success();
								} catch (err) {
									b?.failure(err);
								}
							}

							if (userGone === 1) {
								mLeaves?.inc({ topic: mt(topic) });
								const payload = { key, data };
								platform.publish('__presence:' + topic, 'leave', payload);
								await publishEvent(topic, 'leave', payload);
							}
						} else {
							counts.set(key, current - 1);
							const topicData = localData.get(topic);
							if (topicData) {
								let newest = null;
								for (const [otherWs, otherTopics] of wsTopics) {
									if (otherWs === ws) continue;
									const entry = otherTopics.get(topic);
									if (entry && entry.key === key) newest = entry.data;
								}
								const cached = topicData.get(key);
								if (newest && cached && !deepEqual(newest, cached.data)) {
									const ser = JSON.stringify(newest);
									topicData.set(key, { data: newest, serialized: ser, field: compoundField(key) });
									const now = Date.now();
									try {
										await redis.hset(hashKey(topic), compoundField(key), JSON.stringify({ data: newest, ts: now }));
										const payload = { key, data: newest };
										platform.publish('__presence:' + topic, 'updated', payload);
										publishEvent(topic, 'updated', payload);
									} catch {
										topicData.set(key, { data: cached.data, serialized: cached.serialized, field: cached.field });
									}
								}
							}
						}
					}
				}

				// Handle sync-only observer for this topic
				const syncTopics = syncObservers.get(ws);
				if (syncTopics && syncTopics.has(topic)) {
					syncTopics.delete(topic);
					if (syncTopics.size === 0) syncObservers.delete(ws);

					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }

					const count = (syncCounts.get(topic) || 1) - 1;
					if (count <= 0) {
						syncCounts.delete(topic);
						if (!localCounts.has(topic)) {
							await unsubscribeFromTopic(topic);
						}
					} else {
						syncCounts.set(topic, count);
					}
				}

				return;
			}

			// --- Leave all topics ---
			// Phase 1: Synchronous cleanup of ALL local state before any async
			// work. This prevents the heartbeat from refreshing dead entries and
			// lets concurrent join() calls detect the closed ws via wsTopics.
			const connTopics = wsTopics.get(ws);
			wsTopics.delete(ws);

			const syncTopics = syncObservers.get(ws);
			syncObservers.delete(ws);

			if (connTopics) {
				for (const topic of connTopics.keys()) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				}
			}
			if (syncTopics) {
				for (const topic of syncTopics) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				}
			}

			/** @type {Array<{ topic: string, key: string, data: Record<string, any>, needsUnsub: boolean }>} */
			const pendingLeaves = [];
			const pendingUpdatedRelays = [];
			const deferredRestores = [];

			if (connTopics) {
				for (const [topic, { key, data }] of connTopics) {
					const counts = localCounts.get(topic);
					if (!counts) continue;

					const current = counts.get(key) || 0;
					if (current <= 1) {
						counts.delete(key);

						const topicData = localData.get(topic);
						if (topicData) {
							topicData.delete(key);
							if (topicData.size === 0) localData.delete(topic);
						}

						let needsUnsub = false;
						if (counts.size === 0) {
							localCounts.delete(topic);
							activeTopics.delete(topic);
							if (!syncCounts.has(topic)) {
								needsUnsub = true;
							}
						}

						pendingLeaves.push({ topic, key, data, needsUnsub });
					} else {
						counts.set(key, current - 1);
						const topicData = localData.get(topic);
						if (topicData) {
							let newest = null;
							for (const [otherWs, otherTopics] of wsTopics) {
								const entry = otherTopics.get(topic);
								if (entry && entry.key === key) newest = entry.data;
							}
							const cached = topicData.get(key);
							if (newest && cached && !deepEqual(newest, cached.data)) {
								deferredRestores.push({ topic, key, newest, cached });
							}
						}
					}
				}
			}

			if (syncTopics) {
				for (const topic of syncTopics) {
					const count = (syncCounts.get(topic) || 1) - 1;
					if (count <= 0) {
						syncCounts.delete(topic);
					} else {
						syncCounts.set(topic, count);
					}
				}
			}

			for (const { topic, key, newest, cached } of deferredRestores) {
				const topicData = localData.get(topic);
				if (!topicData) continue;
				const ser = JSON.stringify(newest);
				topicData.set(key, { data: newest, serialized: ser, field: compoundField(key) });
				const now = Date.now();
				try {
					await redis.hset(hashKey(topic), compoundField(key), JSON.stringify({ data: newest, ts: now }));
					const payload = { key, data: newest };
					platform.publish('__presence:' + topic, 'updated', payload);
					pendingUpdatedRelays.push(publishEvent(topic, 'updated', payload));
				} catch {
					topicData.set(key, { data: cached.data, serialized: cached.serialized, field: cached.field });
				}
			}

			// Phase 2: Async Redis cleanup. Local state is already clean so
			// the heartbeat will not refresh any of these entries.
			// Use a pipeline to batch all LEAVE_SCRIPT EVALs into a single
			// Redis round trip. Under mass disconnect (1000+ connections)
			// this avoids overwhelming the Redis command queue.
			const unsubPromises = [];
			for (const { needsUnsub, topic } of pendingLeaves) {
				if (needsUnsub) {
					unsubPromises.push(unsubscribeFromTopic(topic));
				}
			}
			if (unsubPromises.length > 0) await Promise.all(unsubPromises);

			const now = Date.now();
			const pipe = redis.pipeline();
			for (const { topic, key } of pendingLeaves) {
				const field = compoundField(key);
				const suffix = '|' + key;
				pipe.eval(LEAVE_SCRIPT, 1, hashKey(topic), field, suffix, now, presenceTtlMs);
			}

			let results;
			let skipPipeline = false;
			if (b) { try { b.guard(); } catch { skipPipeline = true; } }
			if (!skipPipeline) {
				try {
					results = await pipe.exec();
					b?.success();
				} catch (err) {
					b?.failure(err);
				}
			}

			// Broadcast leave events only when Redis confirmed the user
			// is completely gone. If results is null (Redis unavailable
			// or pipeline failed), suppress all leave broadcasts to avoid
			// lying to other instances about a user that may still be
			// present elsewhere.
			const publishPromises = [];
			for (let i = 0; i < pendingLeaves.length; i++) {
				const userGone = results ? (!results[i][0] && results[i][1] === 1) : false;
				if (!userGone) continue;
				const { topic, key, data } = pendingLeaves[i];
				mLeaves?.inc({ topic: mt(topic) });
				const payload = { key, data };
				platform.publish('__presence:' + topic, 'leave', payload);
				publishPromises.push(publishEvent(topic, 'leave', payload));
			}
			if (publishPromises.length > 0) await Promise.all(publishPromises);
			if (pendingUpdatedRelays.length > 0) await Promise.all(pendingUpdatedRelays);

			// Async unsubscribe for sync-only observer topics
			if (syncTopics) {
				const syncUnsubPromises = [];
				for (const topic of syncTopics) {
					if (!syncCounts.has(topic) && !localCounts.has(topic)) {
						syncUnsubPromises.push(unsubscribeFromTopic(topic));
					}
				}
				if (syncUnsubPromises.length > 0) await Promise.all(syncUnsubPromises);
			}
		},

		async sync(ws, topic, platform) {
			b?.guard();
			try {
				await subscribeToTopic(topic, platform);
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			let all;
			try {
				all = await coalesceHgetall(topic);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const presenceTopic = '__presence:' + topic;
			const entries = parseEntries(all);
			const list = [];
			for (const [userKey, entry] of entries) {
				list.push({ key: userKey, data: entry.data });
			}

			let topics = syncObservers.get(ws);
			if (!topics) {
				topics = new Set();
				syncObservers.set(ws, topics);
			}
			if (!topics.has(topic)) {
				topics.add(topic);
				syncCounts.set(topic, (syncCounts.get(topic) || 0) + 1);
			}

			try {
				ws.subscribe(presenceTopic);
				platform.send(ws, presenceTopic, 'list', list);
			} catch {
				const topics = syncObservers.get(ws);
				if (topics && topics.has(topic)) {
					topics.delete(topic);
					if (topics.size === 0) syncObservers.delete(ws);
					const count = (syncCounts.get(topic) || 1) - 1;
					if (count <= 0) {
						syncCounts.delete(topic);
						if (!localCounts.has(topic)) {
							await unsubscribeFromTopic(topic);
						}
					} else {
						syncCounts.set(topic, count);
					}
				}
			}
		},

		async list(topic) {
			b?.guard();
			const now = cachedNow();
			let raw;
			try {
				raw = await redis.eval(LIST_SCRIPT, 1, hashKey(topic), now, presenceTtlMs);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const result = [];
			for (let i = 0; i < raw.length; i += 2) {
				try {
					const parsed = JSON.parse(raw[i + 1]);
					result.push(parsed.data);
				} catch { /* skip corrupted */ }
			}
			return result;
		},

		async count(topic) {
			b?.guard();
			const now = cachedNow();
			try {
				const result = await redis.eval(COUNT_DEDUP_SCRIPT, 1, hashKey(topic), now, presenceTtlMs);
				b?.success();
				return result;
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async clear() {
			b?.guard();
			try {
				const pattern = client.key('presence:*');
				let cursor = '0';
				do {
					const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
					cursor = nextCursor;
					if (keys.length > 0) {
						await redis.unlink(...keys);
					}
				} while (cursor !== '0');
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			for (const [ws, connTopics] of wsTopics) {
				for (const topic of connTopics.keys()) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				}
			}
			for (const [ws, topics] of syncObservers) {
				for (const topic of topics) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				}
			}

			if (subscriber) {
				for (const ch of subscribedChannels) {
					await subscriber.unsubscribe(ch).catch(() => {});
				}
				subscribedChannels.clear();
			}

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
			if (idleTimer) {
				clearTimeout(idleTimer);
				idleTimer = null;
			}
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscribedChannels.clear();
			activePlatform = null;
		},

		hooks: {
			async subscribe(ws, topic, { platform }) {
				if (topic.startsWith('__presence:')) {
					const realTopic = topic.slice('__presence:'.length);
					await tracker.sync(ws, realTopic, platform);
					return;
				}
				await tracker.join(ws, topic, platform);
			},
			async unsubscribe(ws, topic, { platform }) {
				if (topic.startsWith('__presence:')) {
					const realTopic = topic.slice('__presence:'.length);
					const syncTopics = syncObservers.get(ws);
					if (syncTopics && syncTopics.has(realTopic)) {
						syncTopics.delete(realTopic);
						if (syncTopics.size === 0) syncObservers.delete(ws);

						try { ws.unsubscribe(topic); } catch { /* closed */ }

						const count = (syncCounts.get(realTopic) || 1) - 1;
						if (count <= 0) {
							syncCounts.delete(realTopic);
							if (!localCounts.has(realTopic)) {
								await unsubscribeFromTopic(realTopic);
							}
						} else {
							syncCounts.set(realTopic, count);
						}
					}
					return;
				}
				if (topic.startsWith('__')) return;
				await tracker.leave(ws, platform, topic);
			},
			async close(ws, { platform }) {
				await tracker.leave(ws, platform);
			}
		}
	};

	return tracker;
}
