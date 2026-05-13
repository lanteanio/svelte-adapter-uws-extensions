/**
 * Redis-backed presence tracker for svelte-adapter-uws.
 *
 * Same API as the core createPresence plugin, but stores presence state
 * in Redis hashes so it is shared across instances. Uses Redis pub/sub
 * for cross-instance join/leave notifications.
 *
 * Wire shape clients see on `__presence:{topic}`:
 *   - `presence_state` (sent once on subscribe to a single connection)
 *       payload: `{[userKey]: data}` flat snapshot of current presence
 *   - `presence_diff` (broadcast to topic subscribers, microtask-batched)
 *       payload: `{joins: {[key]: data}, leaves: {[key]: data}}`
 *       Same-tick joins+leaves on the same key collapse: latest op wins.
 *   - `heartbeat` (broadcast to topic subscribers, per heartbeat interval)
 *       payload: array of currently-known user keys
 *
 * The adapter's bundled `createPresence` plugin emits the same wire
 * shape, so a single client decoder works for both single-instance and
 * cluster deployments.
 *
 * Storage layout per topic:
 *   - Key `{prefix}presence:{topic}` - hash
 *       field = `{instanceId}|{userKey}`, value = JSON `{ data, ts }`
 *       Each instance owns its own fields so cross-instance leave is safe.
 *   - Channel `{prefix}presence:events:{topic}` - cross-instance pub/sub.
 *       Internal envelope `{instanceId, topic, event, payload}` with
 *       event in {'join', 'leave', 'updated'}; receiving instances
 *       route those into their local diff buffer for client fan-out.
 *
 * Each instance also maintains a local connection map so it knows when to
 * publish leave events (last connection for a user on this instance).
 *
 * @module svelte-adapter-uws-extensions/redis/presence
 */

import { randomBytes } from 'node:crypto';
import { now as cachedNow } from '../shared/time.js';
import { CLEANUP_SCRIPT, COUNT_DEDUP_SCRIPT, LIST_SCRIPT } from '../shared/scripts.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_PRESENCE_WS, MAX_PRESENCE_TOPICS } from '../shared/caps.js';

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
 * user joins on a second instance, both broadcast "join" - the client
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
if ttlSec == nil then
  return redis.error_reply('PRESENCE_JOIN: ttlSec must be numeric')
end

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
if now == nil or ttlMs == nil then
  return redis.error_reply('PRESENCE_LEAVE: now/ttlMs must be numeric')
end

redis.call('hdel', key, field)

local all = redis.call('hgetall', key)
for i = 1, #all, 2 do
  local f = all[i]
  if string.find(f, suffix, #f - #suffix + 1, true) then
    local ok, parsed = pcall(cjson.decode, all[i+1])
    local ts = ok and parsed and tonumber(parsed.ts) or nil
    if ts and (now - ts) <= ttlMs then
      return 0
    end
  end
end
return 1
`;

/**
 * Internal cross-instance Redis pub/sub envelope event names. NOT the
 * client wire shape - clients see `presence_state` / `presence_diff` /
 * `heartbeat`. These names live on the `presence:events:{topic}` channel
 * between instances and are routed into the local diff buffer on receive.
 */
const INTERNAL_EVENTS = Object.freeze({
	JOIN: 'join',
	LEAVE: 'leave',
	UPDATED: 'updated'
});

/**
 * @typedef {Object} RedisPresenceOptions
 * @property {string} [key='id'] - Field in selected data for user dedup
 * @property {(userData: any) => Record<string, any>} [select] - Extract public fields from userData
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh expiry)
 * @property {number} [ttl=90] - TTL in seconds for presence entries (should be > heartbeat * 3)
 * @property {boolean} [keyspaceNotifications=false] - Subscribe to `__keyevent@*__:expired` so a topic's local subscribers receive an empty `list` event the moment its presence hash expires (instance-died scenario). Requires `CONFIG SET notify-keyspace-events Kx` (or any flagset including key-event + expired).
 */

/**
 * @typedef {Object} PresenceMetricsSnapshot
 * @property {number} totalOnline - Sum of unique-users-per-topic across all topics this instance is locally tracking. Same user in two topics counts as two; per-topic counts sum cleanly.
 * @property {number} heartbeatLatencyMs - Duration of the most recent heartbeat tick in milliseconds.
 * @property {number} staleCleanedTotal - Cumulative count of stale fields removed by the heartbeat-driven `CLEANUP_SCRIPT` since this instance started.
 */

/**
 * @typedef {Object} RedisPresenceTracker
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} join
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, topic?: string) => Promise<void>} leave
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} sync
 * @property {(topic: string) => Promise<Array<Record<string, any>>>} list
 * @property {(topic: string) => Promise<number>} count
 * @property {() => PresenceMetricsSnapshot} metrics
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

	const keyspaceNotifications = options.keyspaceNotifications === true;

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mJoins = m?.counter('presence_joins_total', 'Presence join events', ['topic']);
	const mLeaves = m?.counter('presence_leaves_total', 'Presence leave events', ['topic']);
	const mHeartbeats = m?.counter('presence_heartbeats_total', 'Heartbeat refresh cycles');
	const mStaleCleaned = m?.counter('presence_stale_cleaned_total', 'Stale entries removed by cleanup');
	const mTotalOnline = m?.gauge('presence_total_online', 'Unique users present per topic on this instance', ['topic']);
	const mHeartbeatLatency = m?.gauge('presence_heartbeat_latency_ms', 'Duration of the most recent heartbeat tick in milliseconds');
	const mKeyspaceCleanups = m?.counter('presence_keyspace_cleanups_total', 'Topics whose hash expiry triggered a local empty-list emit');
	const mDiffFrames = m?.counter('presence_diff_frames_total', 'presence_diff frames published to topic subscribers', ['topic']);
	const mDiffCoalesced = m?.counter('presence_diff_coalesced_total', 'Buffered diff entries overwritten by a later op in the same tick', ['topic']);

	let lastHeartbeatLatency = 0;
	let staleCleanedTotal = 0;
	let keyspaceSubscribed = false;

	const warnSensitive = createSensitiveWarner('redis/presence');

	let connCounter = 0;

	/**
	 * Per-connection state: which topics they've joined and their key on each.
	 * @type {Map<any, Map<string, { key: string, data: Record<string, any> }>>}
	 */
	const wsTopics = new Map();

	/**
	 * Reverse index from `topic + '|' + userKey` to the set of ws connections
	 * tracking that (topic, key) on this instance. Mirrors `wsTopics` so the
	 * leave path can find another live connection for the same user without
	 * scanning every ws on the instance.
	 * @type {Map<string, Set<any>>}
	 */
	const topicKeyToWs = new Map();

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

	/**
	 * Per-topic pending diff buffer: latest op per key wins. Joins and
	 * leaves on the same key in the same microtask collapse so the wire
	 * only sees the net change. Flushed once per microtask via
	 * `scheduleFlush`. Mirrors the buffer model the adapter's bundled
	 * presence plugin uses, so a single client decoder handles both.
	 * @type {Map<string, Map<string, { op: 'join' | 'leave', data: Record<string, any> }>>}
	 */
	const pendingDiffs = new Map();
	let diffFlushScheduled = false;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let diffFlushPlatform = null;

	function bufferDiff(topic, op, key, data, platform) {
		let entries = pendingDiffs.get(topic);
		if (!entries) {
			entries = new Map();
			pendingDiffs.set(topic, entries);
		}
		if (entries.has(key)) {
			mDiffCoalesced?.inc({ topic: mt(topic) });
		}
		entries.set(key, { op, data });
		diffFlushPlatform = platform;
		if (!diffFlushScheduled) {
			diffFlushScheduled = true;
			queueMicrotask(flushPendingDiffs);
		}
	}

	function flushPendingDiffs() {
		diffFlushScheduled = false;
		const platform = diffFlushPlatform;
		diffFlushPlatform = null;
		if (!platform) {
			pendingDiffs.clear();
			return;
		}
		for (const [topic, entries] of pendingDiffs) {
			/** @type {Record<string, Record<string, any>>} */
			const joins = {};
			/** @type {Record<string, Record<string, any>>} */
			const leaves = {};
			for (const [key, { op, data }] of entries) {
				if (op === 'join') joins[key] = data;
				else leaves[key] = data;
			}
			try {
				platform.publish('__presence:' + topic, 'presence_diff', { joins, leaves }, { relay: false });
				mDiffFrames?.inc({ topic: mt(topic) });
			} catch { /* platform unavailable mid-flight */ }
		}
		pendingDiffs.clear();
	}

	function hashKey(topic) {
		return client.key('presence:' + topic);
	}

	function indexAdd(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		let set = topicKeyToWs.get(k);
		if (!set) {
			set = new Set();
			topicKeyToWs.set(k, set);
		}
		set.add(ws);
	}

	function indexRemove(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		const set = topicKeyToWs.get(k);
		if (!set) return;
		set.delete(ws);
		if (set.size === 0) topicKeyToWs.delete(k);
	}

	function findOtherWsData(topic, userKey, exceptWs) {
		const set = topicKeyToWs.get(topic + '|' + userKey);
		if (!set) return null;
		let newest = null;
		for (const ws of set) {
			if (ws === exceptWs) continue;
			const entry = wsTopics.get(ws)?.get(topic);
			if (entry && entry.key === userKey) newest = entry.data;
		}
		return newest;
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
		const seen = new Map();
		for (const field of Object.keys(all)) {
			try {
				const parsed = JSON.parse(all[field]);
				if (!parsed.ts || (now - parsed.ts) > presenceTtlMs) continue;
				const sep = field.indexOf('|');
				const userKey = sep !== -1 ? field.slice(sep + 1) : field;
				const existing = seen.get(userKey);
				if (!existing || (parsed.ts || 0) > (existing.ts || 0)) {
					seen.set(userKey, parsed);
				}
			} catch { /* corrupted entry */ }
		}
		return seen;
	}

	// Heartbeat: refresh timestamps on local entries, TTL on hash keys,
	// and clean up stale fields from crashed instances
	/** @type {Set<string>} */
	const activeTopics = new Set();
	const heartbeatTimer = setInterval(() => {
		const tickStart = Date.now();
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
				// Full leave (sync Step 1 + async Step 2 fire-and-forget)
				tracker.leave(ws, activePlatform).catch(() => {});
			}
		}

		// totalOnline gauge tracks current state, which is meaningful
		// even when the breaker is broken and the rest of the tick bails.
		if (mTotalOnline) {
			for (const [topic, counts] of localCounts) {
				mTotalOnline.set({ topic: mt(topic) }, counts.size);
			}
		}

		if (b && !b.isHealthy) {
			lastHeartbeatLatency = Date.now() - tickStart;
			mHeartbeatLatency?.set(lastHeartbeatLatency);
			return;
		}
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

			redis.eval(CLEANUP_SCRIPT, 1, hkey, now, presenceTtlMs).then((cleaned) => {
				if (cleaned > 0) {
					staleCleanedTotal += cleaned;
					mStaleCleaned?.inc(cleaned);
				}
			}).catch(() => {});

			if (activePlatform && data && data.size > 0) {
				const keys = [...data.keys()];
				activePlatform.publish('__presence:' + topic, 'heartbeat', keys);
			}
		}
		pipe.exec().catch(() => {});
		lastHeartbeatLatency = Date.now() - tickStart;
		mHeartbeatLatency?.set(lastHeartbeatLatency);
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
					if (!activePlatform) return;
					const ev = parsed.event;
					const payload = parsed.payload;
					if (ev === INTERNAL_EVENTS.JOIN || ev === INTERNAL_EVENTS.UPDATED) {
						bufferDiff(topic, 'join', payload?.key, payload?.data, activePlatform);
					} else if (ev === INTERNAL_EVENTS.LEAVE) {
						bufferDiff(topic, 'leave', payload?.key, payload?.data, activePlatform);
					}
				} catch {
					// Malformed, skip
				}
			});
			if (keyspaceNotifications) {
				const presencePrefix = client.key('presence:');
				const eventsPrefix = client.key('presence:events:');
				subscriber.on('pmessage', (_pattern, _channel, expiredKey) => {
					if (typeof expiredKey !== 'string') return;
					if (!expiredKey.startsWith(presencePrefix)) return;
					if (expiredKey.startsWith(eventsPrefix)) return;
					const topic = expiredKey.slice(presencePrefix.length);
					if (activePlatform) {
						activePlatform.publish('__presence:' + topic, 'presence_state', {}, { relay: false });
						mKeyspaceCleanups?.inc();
					}
				});
				try {
					await subscriber.psubscribe('__keyevent@*__:expired');
					keyspaceSubscribed = true;
				} catch (err) {
					console.warn(
						'[redis/presence] keyspace notifications: psubscribe failed - ' +
						'enable on Redis with `CONFIG SET notify-keyspace-events Ex` (or any flagset including `K`/`E` and `x`): ' +
						err.message + '\n' +
						'  See: https://svti.me/redis-keyspace'
					);
				}
			}
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
		// Don't idle-shutdown when keyspace notifications are on - the
		// pattern subscription is the whole point of keeping the
		// subscriber alive.
		if (subscribedChannels.size === 0 && !keyspaceSubscribed && subscriber) {
			if (!idleTimer) {
				idleTimer = setTimeout(() => {
					idleTimer = null;
					if (subscribedChannels.size === 0 && !keyspaceSubscribed && subscriber) {
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
		indexRemove(topic, key, ws);
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
			bufferDiff(topic, 'leave', key, data, platform);
			await publishEvent(topic, INTERNAL_EVENTS.LEAVE, { key, data });
		}
		if (!localCounts.has(topic) && !syncCounts.has(topic)) {
			await unsubscribeFromTopic(topic);
		}
	}

	async function leaveTopic(ws, platform, topic) {
		const connTopics = wsTopics.get(ws);
		if (connTopics && connTopics.has(topic)) {
			const { key, data } = connTopics.get(topic);
			connTopics.delete(topic);
			if (connTopics.size === 0) wsTopics.delete(ws);
			indexRemove(topic, key, ws);

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
						bufferDiff(topic, 'leave', key, data, platform);
						await publishEvent(topic, INTERNAL_EVENTS.LEAVE, { key, data });
					}
				} else {
					counts.set(key, current - 1);
					const topicData = localData.get(topic);
					if (topicData) {
						const newest = findOtherWsData(topic, key, ws);
						const cached = topicData.get(key);
						if (newest && cached && !deepEqual(newest, cached.data)) {
							const ser = JSON.stringify(newest);
							topicData.set(key, { data: newest, serialized: ser, field: compoundField(key) });
							const now = Date.now();
							try {
								await redis.hset(hashKey(topic), compoundField(key), JSON.stringify({ data: newest, ts: now }));
								bufferDiff(topic, 'join', key, newest, platform);
								publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: newest });
							} catch {
								topicData.set(key, { data: cached.data, serialized: cached.serialized, field: cached.field });
							}
						}
					}
				}
			}
		}

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
	}

	async function leaveAll(ws, platform) {
		// Step 1: synchronous cleanup of all local state before any async
		// work. Prevents the heartbeat from refreshing dead entries and
		// lets concurrent join() calls detect the closed ws via wsTopics.
		const connTopics = wsTopics.get(ws);
		wsTopics.delete(ws);
		if (connTopics) {
			for (const [topic, { key }] of connTopics) {
				indexRemove(topic, key, ws);
			}
		}

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
						const newest = findOtherWsData(topic, key, ws);
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
				bufferDiff(topic, 'join', key, newest, platform);
				pendingUpdatedRelays.push(publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: newest }));
			} catch {
				topicData.set(key, { data: cached.data, serialized: cached.serialized, field: cached.field });
			}
		}

		// Step 2: async Redis cleanup. Local state is already clean so the
		// heartbeat will not refresh any of these entries. The pipeline
		// batches all LEAVE_SCRIPT evals into a single round-trip; under
		// mass disconnect (1000+ connections) this avoids saturating the
		// Redis command queue.
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

		// Broadcast leave events only when Redis confirmed the user is
		// completely gone. If results is null (Redis unavailable or
		// pipeline failed), suppress all leave broadcasts so we don't
		// lie to other instances about a user that may still be present
		// elsewhere.
		const publishPromises = [];
		for (let i = 0; i < pendingLeaves.length; i++) {
			const userGone = results ? (!results[i][0] && results[i][1] === 1) : false;
			if (!userGone) continue;
			const { topic, key, data } = pendingLeaves[i];
			mLeaves?.inc({ topic: mt(topic) });
			bufferDiff(topic, 'leave', key, data, platform);
			publishPromises.push(publishEvent(topic, INTERNAL_EVENTS.LEAVE, { key, data }));
		}
		if (publishPromises.length > 0) await Promise.all(publishPromises);
		if (pendingUpdatedRelays.length > 0) await Promise.all(pendingUpdatedRelays);

		if (syncTopics) {
			const syncUnsubPromises = [];
			for (const topic of syncTopics) {
				if (!syncCounts.has(topic) && !localCounts.has(topic)) {
					syncUnsubPromises.push(unsubscribeFromTopic(topic));
				}
			}
			if (syncUnsubPromises.length > 0) await Promise.all(syncUnsubPromises);
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
			let serializedData;
			try { serializedData = JSON.stringify(data); } catch {
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
				if (wsTopics.size >= MAX_PRESENCE_WS) {
					throw new Error(
						`presence: local ws count exceeded ${MAX_PRESENCE_WS} on this instance`
					);
				}
				connTopics = new Map();
				wsTopics.set(ws, connTopics);
			}
			connTopics.set(topic, { key, data });
			indexAdd(topic, key, ws);

			let counts = existingCounts;
			if (!counts) {
				if (localCounts.size >= MAX_PRESENCE_TOPICS) {
					throw new Error(
						`presence: local topic count exceeded ${MAX_PRESENCE_TOPICS} on this instance`
					);
				}
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
					td.set(key, { data, serialized: serializedData, field: compoundField(key) });
				}

				bufferDiff(topic, 'join', key, data, platform);
				await publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data });
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
			topicData.set(key, { data, serialized: serializedData, field: compoundField(key) });
			activeTopics.add(topic);

			// Buffer join only after the operation is fully committed.
			// Prevents orphaned diffs when snapshot or subscribe fails - the
			// compensating leave inside undoJoin would collapse with this
			// join in the buffer anyway, but skipping the buffer entirely
			// keeps the cross-instance pubsub clean.
			if (isNewUser) {
				mJoins?.inc({ topic: mt(topic) });
				bufferDiff(topic, 'join', key, data, platform);
				await publishEvent(topic, INTERNAL_EVENTS.JOIN, { key, data });
			}

			// Send current snapshot to this connection. Flat `{[key]: data}`
			// shape mirrors the adapter's bundled presence plugin so a single
			// client decoder handles both implementations.
			const entries = parseEntries(all);
			/** @type {Record<string, Record<string, any>>} */
			const state = {};
			for (const [userKey, entry] of entries) {
				state[userKey] = entry.data;
			}
			try {
				platform.send(ws, '__presence:' + topic, 'presence_state', state);
			} catch {
				// WebSocket closed before send
			}
		},

		async leave(ws, platform, topic) {
			if (topic !== undefined) return leaveTopic(ws, platform, topic);
			return leaveAll(ws, platform);
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
			/** @type {Record<string, Record<string, any>>} */
			const state = {};
			for (const [userKey, entry] of entries) {
				state[userKey] = entry.data;
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
				platform.send(ws, presenceTopic, 'presence_state', state);
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
			const now = cachedNow();
			const raw = await withBreaker(b, () =>
				redis.eval(LIST_SCRIPT, 1, hashKey(topic), now, presenceTtlMs)
			);
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
			const now = cachedNow();
			return withBreaker(b, () =>
				redis.eval(COUNT_DEDUP_SCRIPT, 1, hashKey(topic), now, presenceTtlMs)
			);
		},

		metrics() {
			let totalOnline = 0;
			for (const [, counts] of localCounts) {
				totalOnline += counts.size;
			}
			return {
				totalOnline,
				heartbeatLatencyMs: lastHeartbeatLatency,
				staleCleanedTotal
			};
		},

		flushDiffs() {
			flushPendingDiffs();
		},

		async clear() {
			await withBreaker(b, () => scanAndUnlink(redis, client.key('presence:*')));

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
			pendingDiffs.clear();
			diffFlushScheduled = false;
			diffFlushPlatform = null;
			connCounter = 0;
		},

		destroy() {
			clearInterval(heartbeatTimer);
			if (idleTimer) {
				clearTimeout(idleTimer);
				idleTimer = null;
			}
			if (subscriber) {
				const sub = subscriber;
				subscriber = null;
				sub.quit().catch(() => sub.disconnect());
			}
			subscribedChannels.clear();
			keyspaceSubscribed = false;
			activePlatform = null;
			pendingDiffs.clear();
			diffFlushScheduled = false;
			diffFlushPlatform = null;
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
