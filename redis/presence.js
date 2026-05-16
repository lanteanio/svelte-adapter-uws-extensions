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
 * Storage layout (two hashes per topic, Redis 7.4+ HEXPIRE for per-field TTL):
 *   - `{prefix}presence:topic:{topic}` - hash, field=userKey, value=JSON{data,ts}
 *       One entry per unique user on the topic. Backs `list()` / `count()`.
 *   - `{prefix}presence:user:{topic}:{userKey}` - hash, field=instanceId, value=ts
 *       One entry per instance currently presenting this user. Backs JOIN/LEAVE
 *       broadcast decision (HLEN check).
 *
 * Per-field TTLs via HPEXPIRE replace the previous timestamp-filter scan in
 * the Lua leave script: stale entries from a crashed instance auto-expire
 * field-by-field via Redis itself rather than via application-side filters.
 * Mass-disconnect is now O(N) Redis-blocked work (one HDEL+HLEN per leave)
 * rather than O(N x M_topic) (one HGETALL+linear-suffix-scan per leave).
 *
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
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_PRESENCE_WS, MAX_PRESENCE_TOPICS } from '../shared/caps.js';

/**
 * Lua script for atomic JOIN. Sets this instance's field on the per-user
 * hash (the "is this user here?" index), refreshes both hashes' per-field
 * TTLs via HPEXPIRE, and writes the user's data to the per-topic hash with
 * a newer-ts-wins conditional set.
 *
 * KEYS[1] = userHashKey  (presence:user:{topic}:{userKey})
 * KEYS[2] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = instanceId
 * ARGV[2] = userKey (also the field on topicHashKey)
 * ARGV[3] = topicHashValue (JSON {data, ts} pre-stringified)
 * ARGV[4] = ts (numeric, for the conditional set)
 * ARGV[5] = ttlMs (numeric, for HPEXPIRE)
 *
 * Returns 1 if this was the FIRST instance to present this user on the
 * topic (HLEN was 0 before our HSET) -> caller broadcasts a join. Returns
 * 0 if the user was already present from another instance or another
 * tab on this instance via an idempotent re-run.
 *
 * The conditional set on the topic hash preserves the "newer data wins"
 * property the previous LIST_SCRIPT enforced at read time. Two instances
 * with the same userKey but different `select()` output land deterministic
 * data on the topic hash regardless of arrival order: the higher-ts write
 * wins. Concurrent instances with the same ts (rare) tie-break on write
 * order, which is acceptable; subsequent heartbeats do not re-write data
 * so the tie-break sticks.
 *
 * Note: HSET on an existing field clears its per-field TTL, so every
 * HSET in this script is paired with HPEXPIRE in the same atomic block.
 */
const JOIN_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]
local topicHashValue = ARGV[3]
local newTs = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])
if newTs == nil or ttlMs == nil then
  return redis.error_reply('PRESENCE_JOIN: newTs/ttlMs must be numeric')
end

local wasEmpty = (redis.call('HLEN', userKey) == 0)

redis.call('HSET', userKey, instanceId, newTs)
redis.call('HPEXPIRE', userKey, ttlMs, 'FIELDS', 1, instanceId)

local existing = redis.call('HGET', topicKey, userKeyStr)
local shouldWrite = true
if existing then
  local ok, parsed = pcall(cjson.decode, existing)
  local existingTs = ok and parsed and tonumber(parsed.ts) or 0
  if newTs < existingTs then shouldWrite = false end
end
if shouldWrite then
  redis.call('HSET', topicKey, userKeyStr, topicHashValue)
end
redis.call('HPEXPIRE', topicKey, ttlMs, 'FIELDS', 1, userKeyStr)

return wasEmpty and 1 or 0
`;

/**
 * Lua script for atomic LEAVE. Removes this instance's field from the
 * per-user hash and broadcasts only when no other instance still has the
 * user (HLEN == 0 after our HDEL).
 *
 * KEYS[1] = userHashKey  (presence:user:{topic}:{userKey})
 * KEYS[2] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = instanceId
 * ARGV[2] = userKey
 *
 * Returns 1 if this instance was the last one presenting this user on the
 * topic -> caller broadcasts a leave AND the per-topic hash entry is
 * removed. Returns 0 if another instance still has the user (no broadcast,
 * per-topic hash unchanged).
 *
 * The O(1) HLEN check replaces the previous O(M_topic) suffix-scan loop.
 * Mass-disconnect of N users is now O(N) Redis-blocked Lua time rather
 * than O(N x M_topic).
 */
const LEAVE_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]

redis.call('HDEL', userKey, instanceId)
if redis.call('HLEN', userKey) == 0 then
  redis.call('HDEL', topicKey, userKeyStr)
  return 1
end
return 0
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
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh per-field TTLs)
 * @property {number} [ttl=90] - TTL in seconds for presence entries (should be > heartbeat * 3). Applied per-field via HPEXPIRE; fields auto-expire field-by-field rather than at whole-key granularity.
 * @property {boolean} [keyspaceNotifications=false] - Subscribe to `__keyevent@*__:expired` so a topic's local subscribers receive an empty `list` event the moment its per-topic presence hash key expires (instance-died scenario where every field of the hash has expired). Requires `CONFIG SET notify-keyspace-events Kx` (or any flagset including key-event + expired). With per-field TTLs, individual field expiry does NOT emit a key-expired notification; only whole-key expiry does, which happens when every field of the topic hash has expired (no live instances presenting any user on this topic).
 */

/**
 * @typedef {Object} PresenceMetricsSnapshot
 * @property {number} totalOnline - Sum of unique-users-per-topic across all topics this instance is locally tracking. Same user in two topics counts as two; per-topic counts sum cleanly.
 * @property {number} heartbeatLatencyMs - Duration of the most recent heartbeat tick in milliseconds.
 * @property {number} staleCleanedTotal - Reserved for backward compatibility. Always 0 in this build: staleness is enforced by Redis per-field HPEXPIRE rather than an application-side cleanup script, so there is nothing to count.
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

	// Per-field hash TTL (HPEXPIRE / HEXPIRE) requires Redis 7.4+. Defer the
	// version probe to first use so createPresence() can stay synchronous and
	// fast; the probe runs once, caches its result, and rejects any further
	// redis call with a clear error if the server is too old. Mirrors the
	// gating pattern createShardedBus uses for SPUBLISH / SSUBSCRIBE.
	let featureProbe = null;
	function ensureRedis74() {
		if (!featureProbe) {
			featureProbe = redis.info('server').then((info) => {
				const m = /redis_version:(\d+)\.(\d+)/.exec(info || '');
				if (!m) return; // can't parse - assume compatible
				const major = Number(m[1]);
				const minor = Number(m[2]);
				if (major < 7 || (major === 7 && minor < 4)) {
					throw new Error(
						'redis presence: requires Redis 7.4+ for per-field TTL (HEXPIRE); ' +
						'got ' + m[1] + '.' + m[2] + '. Upgrade Redis or use the in-memory ' +
						'createPresence plugin from svelte-adapter-uws/plugins/presence.'
					);
				}
			}).catch((err) => {
				// Reset on transient INFO failures so we re-probe on next call.
				// Hard errors (version mismatch) re-throw verbatim from the await.
				if (err && /requires Redis 7\.4\+/.test(err.message)) throw err;
				featureProbe = null;
				throw err;
			});
		}
		return featureProbe;
	}

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mJoins = m?.counter('presence_joins_total', 'Presence join events', ['topic']);
	const mLeaves = m?.counter('presence_leaves_total', 'Presence leave events', ['topic']);
	const mHeartbeats = m?.counter('presence_heartbeats_total', 'Heartbeat refresh cycles');
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

	// Per-topic hash: one field per unique user on the topic. Backs list() / count().
	function topicHashKey(topic) {
		return client.key('presence:topic:' + topic);
	}

	// Per-user hash for a topic: one field per instance currently presenting this
	// user. HLEN drives the JOIN/LEAVE broadcast decision.
	function userHashKey(topic, userKey) {
		return client.key('presence:user:' + topic + ':' + userKey);
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
		const key = topicHashKey(topic);
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
	 * Parse the per-topic hash HGETALL result into a Map<userKey, {data, ts}>.
	 * Staleness filtering and cross-instance deduplication are no longer
	 * needed: the new storage layout uses one field per userKey (Redis
	 * collapses cross-instance writes via HSET on the same field), and per-
	 * field HPEXPIRE removes stale entries before HGETALL sees them. So this
	 * is now just a JSON-parse + drop-corrupted loop.
	 */
	function parseEntries(all) {
		const seen = new Map();
		for (const userKey of Object.keys(all)) {
			try {
				seen.set(userKey, JSON.parse(all[userKey]));
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
		// HPEXPIRE refresh per locally-owned (topic, userKey). We do NOT
		// re-HSET the data here: HSET on an existing field clears its TTL
		// (Redis 7.4+ semantics) so an HSET-then-HPEXPIRE pair would be
		// required, doubling the heartbeat cost. Data only changes when a
		// user's select() output changes, which goes through the JOIN flow
		// where HSET + HPEXPIRE are paired inside the JOIN_SCRIPT.
		//
		// Staleness from crashed instances no longer needs an application-side
		// cleanup pass: per-field HPEXPIRE auto-removes fields whose owning
		// instance stopped heartbeating, exactly the behavior the previous
		// CLEANUP_SCRIPT simulated at every tick.
		const pipe = redis.pipeline();
		for (const topic of activeTopics) {
			const data = localData.get(topic);
			if (data && data.size > 0) {
				const topicHash = topicHashKey(topic);
				for (const userKey of data.keys()) {
					pipe.hpexpire(userHashKey(topic, userKey), presenceTtlMs, 'FIELDS', 1, instanceId);
					pipe.hpexpire(topicHash, presenceTtlMs, 'FIELDS', 1, userKey);
				}
				if (activePlatform) {
					const keys = [...data.keys()];
					activePlatform.publish('__presence:' + topic, 'heartbeat', keys);
				}
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
				// The per-topic hash key expires only when every field has
				// expired (no live instances presenting any user on this
				// topic). That is the "whole topic empty" signal we forward
				// as an empty presence_state to local subscribers. Per-user
				// hash keys (presence:user:{topic}:{userKey}) and the events
				// channel are filtered out.
				const topicPrefix = client.key('presence:topic:');
				subscriber.on('pmessage', (_pattern, _channel, expiredKey) => {
					if (typeof expiredKey !== 'string') return;
					if (!expiredKey.startsWith(topicPrefix)) return;
					const topic = expiredKey.slice(topicPrefix.length);
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
	 * Full undo of a staged join. Rolls back local state, reverts the Redis
	 * state to its pre-join shape (full leave if this was the first local
	 * connection, data-restore via JOIN_SCRIPT if there were other tabs
	 * already presenting this user), publishes a compensating leave event
	 * if a join was broadcast, and unsubscribes from the topic's Redis
	 * channel when no local observers remain.
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
				topicData.set(key, { data: prevData });
			} else {
				topicData.delete(key);
			}
			if (topicData.size === 0) localData.delete(topic);
		}
		if (prevCount > 0 && prevData !== undefined) {
			// Other local tabs still present this user. Restore the per-topic
			// hash data to prevData via JOIN_SCRIPT (which handles HSET +
			// HPEXPIRE atomically). Per-user hash field for this instance is
			// already present from the now-rolled-back join; the script's
			// idempotent HSET refreshes its TTL.
			const ts = Date.now();
			const value = JSON.stringify({ data: prevData, ts });
			await redis.eval(
				JOIN_SCRIPT, 2, userHashKey(topic, key), topicHashKey(topic),
				instanceId, key, value, ts, presenceTtlMs
			).catch(() => {});
		} else {
			// This was the first local presence for this user on this topic.
			// LEAVE_SCRIPT removes our instance's entry on the per-user hash
			// and clears the per-topic hash field if HLEN dropped to zero.
			await redis.eval(
				LEAVE_SCRIPT, 2, userHashKey(topic, key), topicHashKey(topic),
				instanceId, key
			).catch(() => {});
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

					let userGone = -1;
					let skipLeaveRedis = false;
					if (b) { try { b.guard(); } catch { skipLeaveRedis = true; } }
					if (!skipLeaveRedis) {
						try {
							userGone = await redis.eval(
								LEAVE_SCRIPT, 2,
								userHashKey(topic, key), topicHashKey(topic),
								instanceId, key
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
							topicData.set(key, { data: newest });
							const ts = Date.now();
							try {
								await redis.eval(
									JOIN_SCRIPT, 2,
									userHashKey(topic, key), topicHashKey(topic),
									instanceId, key, JSON.stringify({ data: newest, ts }), ts, presenceTtlMs
								);
								bufferDiff(topic, 'join', key, newest, platform);
								publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: newest });
							} catch {
								topicData.set(key, { data: cached.data });
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
			topicData.set(key, { data: newest });
			const ts = Date.now();
			try {
				await redis.eval(
					JOIN_SCRIPT, 2,
					userHashKey(topic, key), topicHashKey(topic),
					instanceId, key, JSON.stringify({ data: newest, ts }), ts, presenceTtlMs
				);
				bufferDiff(topic, 'join', key, newest, platform);
				pendingUpdatedRelays.push(publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: newest }));
			} catch {
				topicData.set(key, { data: cached.data });
			}
		}

		// Step 2: async Redis cleanup. Local state is already clean so the
		// heartbeat will not refresh any of these entries. The pipeline
		// batches all LEAVE_SCRIPT evals into a single round-trip; under
		// mass disconnect (1000+ connections) this avoids saturating the
		// Redis command queue. Each LEAVE_SCRIPT is now O(1) Redis-blocked
		// Lua time, so the total Redis-blocked time scales linearly with
		// N (the disconnect count), not with N x M (where M was the topic
		// hash size in the pre-Design-G layout).
		const unsubPromises = [];
		for (const { needsUnsub, topic } of pendingLeaves) {
			if (needsUnsub) {
				unsubPromises.push(unsubscribeFromTopic(topic));
			}
		}
		if (unsubPromises.length > 0) await Promise.all(unsubPromises);

		const pipe = redis.pipeline();
		for (const { topic, key } of pendingLeaves) {
			pipe.eval(
				LEAVE_SCRIPT, 2,
				userHashKey(topic, key), topicHashKey(topic),
				instanceId, key
			);
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
			// Warn first on the raw select output so developers see sensitive
			// keys their select forwarded (the warning fires once per process
			// and is the signal that they should tighten the select). Then
			// deep-strip for the wire: a user-supplied select might return
			// data with nested sensitive keys, and the wire output must not
			// carry them regardless of how select is wired. resolveKey runs
			// on the shallow safeData to keep id / name resolution unchanged.
			const selected = select(safeData);
			warnSensitive(selected);
			const data = stripInternal(selected);
			let serializedData;
			try { serializedData = JSON.stringify(data); } catch {
				throw new Error('redis presence: select() must return JSON-serializable data');
			}

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
				// Redis 7.4+ feature gate (HEXPIRE). Probe once, cache. Throwing
				// here treats version mismatch as a join failure - same shape
				// as any other startup-time misconfiguration.
				await ensureRedis74();
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
			// `serializedData` is computed above to surface non-JSON-serializable
			// data eagerly via the throw. The body below stringifies the full
			// {data, ts} envelope per call since ts is fresh.

			if (prevCount === 0) {
				const ts = Date.now();
				const value = JSON.stringify({ data, ts });
				try {
					const wasEmpty = await redis.eval(
						JOIN_SCRIPT, 2,
						userHashKey(topic, key), topicHashKey(topic),
						instanceId, key, value, ts, presenceTtlMs
					);
					didRedisWrite = true;
					// `wasEmpty === 1` means this instance was the FIRST to
					// present this user on the topic across the cluster; only
					// then do we broadcast a join. Same-user-already-on-another-
					// instance returns 0 and the script's HSET still updates
					// the per-topic data via newer-ts-wins.
					isNewUser = wasEmpty === 1;
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
					throw err;
				}

				if (!wsTopics.has(ws)) {
					// ws closed during the eval. Roll back our Redis write so
					// the per-user hash entry does not linger past TTL.
					await redis.eval(
						LEAVE_SCRIPT, 2,
						userHashKey(topic, key), topicHashKey(topic),
						instanceId, key
					).catch(() => {});
					return;
				}
			} else if (prevData !== undefined && !deepEqual(prevData, data)) {
				// Same instance, same user, different `select()` output.
				// JOIN_SCRIPT's newer-ts conditional set overwrites the per-
				// topic data, and refreshes the per-user-hash TTL so this
				// path counts as an implicit heartbeat for our entry.
				try {
					const ts = Date.now();
					const value = JSON.stringify({ data, ts });
					await redis.eval(
						JOIN_SCRIPT, 2,
						userHashKey(topic, key), topicHashKey(topic),
						instanceId, key, value, ts, presenceTtlMs
					);
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
					throw err;
				}

				const td = localData.get(topic);
				if (td) td.set(key, { data });

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
			// local cleanup and leave events. Just clean the Redis state.
			if (!wsTopics.has(ws)) {
				if (didRedisWrite) {
					redis.eval(
						LEAVE_SCRIPT, 2,
						userHashKey(topic, key), topicHashKey(topic),
						instanceId, key
					).catch(() => {});
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
			topicData.set(key, { data });
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
			// Direct HGETALL on the per-topic hash. Staleness is enforced by
			// Redis HPEXPIRE per field, so we no longer need a Lua-side
			// timestamp filter. The mock-redis prunes expired fields at read
			// time to mirror this; real Redis 7.4+ expires them by background
			// task and HGETALL never returns them.
			await ensureRedis74();
			const all = await withBreaker(b, () => redis.hgetall(topicHashKey(topic)));
			const result = [];
			for (const userKey of Object.keys(all)) {
				try {
					const parsed = JSON.parse(all[userKey]);
					result.push(parsed.data);
				} catch { /* skip corrupted */ }
			}
			return result;
		},

		async count(topic) {
			// HLEN on the per-topic hash. Per-field auto-expiry means HLEN
			// reflects the live count without needing a dedup or timestamp
			// scan; one userKey per live user is the storage invariant.
			await ensureRedis74();
			return withBreaker(b, () => redis.hlen(topicHashKey(topic)));
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
