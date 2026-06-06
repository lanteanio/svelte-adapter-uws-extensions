/**
 * Redis-backed presence tracker for svelte-adapter-uws.
 *
 * Same API as the core createPresence plugin, but stores presence state
 * in Redis hashes so it is shared across instances. Uses Redis pub/sub
 * for cross-instance join/leave notifications.
 *
 * Wire shape clients see on `__presence:{topic}`:
 *   - `state` (sent once on subscribe to a single connection)
 *       payload: `{[userKey]: data}` flat snapshot of current presence
 *   - `diff` (broadcast to topic subscribers, tick-batched)
 *       payload: `{joins: {[key]: data}, leaves: {[key]: data}}`
 *       Joins+leaves on the same key in one event-loop iteration
 *       collapse: latest op wins.
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

import {
	randomBytes,
	now,
	monotonicNow,
	setTimer,
	clearTimer,
	setIntervalTimer,
	clearIntervalTimer
} from '../shared/runtime.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { execMultiSlot } from '../shared/cluster.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_PRESENCE_WS, MAX_PRESENCE_TOPICS } from '../shared/caps.js';
import { WsClosedError } from '../shared/errors.js';
import { createPresenceWireCodec } from 'svelte-adapter-uws/plugins/presence';

export { WsClosedError };

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
local valueToWrite = topicHashValue
local shouldWrite = true
if existing then
  local ok, parsed = pcall(cjson.decode, existing)
  if ok and type(parsed) == 'table' then
    local existingTs = tonumber(parsed.ts) or 0
    if newTs < existingTs then
      shouldWrite = false
    elseif type(parsed.fields) == 'table' then
      -- Preserve dynamic fields (set via update()) across a newer-data
      -- overwrite. The join value carries identity data only, but the
      -- durable fields must survive an avatar/name change or a
      -- cross-instance new-tab join; without this they would be wiped.
      -- cjson round-trips nested objects faithfully (verified on Redis 7.4).
      local okIncoming, incoming = pcall(cjson.decode, valueToWrite)
      if okIncoming and type(incoming) == 'table' then
        incoming.fields = parsed.fields
        valueToWrite = cjson.encode(incoming)
      end
    end
  end
end
if shouldWrite then
  redis.call('HSET', topicKey, userKeyStr, valueToWrite)
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
 * Lua script for atomic field-level UPDATE. Merges a user's changed DURABLE
 * dynamic fields (set via `update()`, transient fields excluded by the caller)
 * into the per-topic hash value's `fields` object so a cross-instance `state`
 * read (HGETALL) reconstructs them. Transient fields are never passed here -
 * they are relay-only and must never persist, so a (re)joining client never
 * inherits a stale transient value.
 *
 * KEYS[1] = topicHashKey (presence:topic:{topic})
 * ARGV[1] = userKey (the field on the topic hash)
 * ARGV[2] = durableFieldsJson (a non-empty JSON object of changed durable fields)
 * ARGV[3] = ts (numeric, refreshes the stored timestamp so a stale concurrent
 *           join cannot overwrite the merged value)
 * ARGV[4] = ttlMs (numeric, for HPEXPIRE - HSET clears the field TTL so it is
 *           re-armed in the same atomic block, matching JOIN_SCRIPT)
 *
 * Returns 1 if the merge was applied, 0 if the user is not present on the
 * topic (already left, or never joined) - the durable value is dropped, which
 * is correct: there is no user to carry it.
 *
 * Single-key (the topic hash only), so an instance's own subscribers' durable
 * fields persist without touching the per-user ownership hash. cjson round-trips
 * the existing value faithfully (an empty `data` object stays `{}`, verified on
 * Redis 7.4), so the identity payload is preserved byte-for-byte.
 */
const UPDATE_SCRIPT = `
local topicKey = KEYS[1]
local userKey = ARGV[1]
local newTs = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])
if newTs == nil or ttlMs == nil then
  return redis.error_reply('PRESENCE_UPDATE: newTs/ttlMs must be numeric')
end
local existing = redis.call('HGET', topicKey, userKey)
if not existing then return 0 end
local ok, parsed = pcall(cjson.decode, existing)
if not ok or type(parsed) ~= 'table' then return 0 end
local okd, durable = pcall(cjson.decode, ARGV[2])
if not okd or type(durable) ~= 'table' then return 0 end
if type(parsed.fields) ~= 'table' then parsed.fields = {} end
for k, v in pairs(durable) do parsed.fields[k] = v end
parsed.ts = newTs
redis.call('HSET', topicKey, userKey, cjson.encode(parsed))
redis.call('HPEXPIRE', topicKey, ttlMs, 'FIELDS', 1, userKey)
return 1
`;

/**
 * Internal cross-instance Redis pub/sub envelope event names. NOT the
 * client wire shape - clients see `state` / `diff` /
 * `heartbeat`. These names live on the `presence:events:{topic}` channel
 * between instances and are routed into the local diff buffer on receive.
 */
const INTERNAL_EVENTS = Object.freeze({
	JOIN: 'join',
	LEAVE: 'leave',
	UPDATED: 'updated',
	FIELDS: 'fields'
});

/**
 * @typedef {Object} RedisPresenceOptions
 * @property {string} [key='id'] - Field in selected data for user dedup
 * @property {(userData: any) => Record<string, any>} [select] - Extract public fields from userData
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh per-field TTLs)
 * @property {number} [ttl=90] - TTL in seconds for presence entries (should be > heartbeat * 3). Applied per-field via HPEXPIRE; fields auto-expire field-by-field rather than at whole-key granularity.
 * @property {boolean} [keyspaceNotifications=false] - Subscribe to `__keyevent@*__:expired` so a topic's local subscribers receive an empty `list` event the moment its per-topic presence hash key expires (instance-died scenario where every field of the hash has expired). Requires `CONFIG SET notify-keyspace-events Kx` (or any flagset including key-event + expired). With per-field TTLs, individual field expiry does NOT emit a key-expired notification; only whole-key expiry does, which happens when every field of the topic hash has expired (no live instances presenting any user on this topic).
 * @property {string[]} [transient] - Dynamic field names (set via `update()`) that are broadcast live but NEVER persisted to Redis and NEVER included in the `state` snapshot or the heartbeat roster. A (re)joining or swept-then-readded client therefore never inherits a possibly-stale transient value - a disconnected typer leaves no stuck indicator across the cluster. Identity fields (from `select`) and durable `update()` fields not listed here persist and ride the snapshot normally. Default: none (every `update()` field is durable). Matches the bundled in-memory presence plugin.
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
 * @property {(ws: any, topic: string, fields: Record<string, any>, platform: import('svelte-adapter-uws').Platform) => Promise<void>} update
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

	// Fields tagged transient are broadcast live (in `update` diffs to whoever
	// is subscribed the moment they change) but are NEVER persisted to Redis and
	// EXCLUDED from the `state` snapshot and the heartbeat roster, so a
	// (re)joining or swept-then-readded client never inherits a possibly-stale
	// transient value. Identity fields (from `select`) are unaffected; durable
	// dynamic fields not tagged here persist to the per-topic hash and ride the
	// snapshot. Mirrors the bundled in-memory presence plugin exactly.
	const transientFields = new Set(
		Array.isArray(options.transient)
			? options.transient.filter((f) => typeof f === 'string')
			: []
	);

	// Binary wire codec (presence.protocol:1), built by the adapter's shared
	// factory so the cluster variant speaks the IDENTICAL wire to the bundled
	// in-memory presence plugin. null when `binary: false`. emit()/emitTo() prefer
	// the binary publishWire/sendWire (opt-in compression: presence is
	// low-frequency) and fall back to JSON publish/send when binary is off or the
	// platform lacks the wire methods (e.g. the unit-test mock) - the client
	// decodes either form transparently.
	const wireCodec = createPresenceWireCodec(options);

	/**
	 * Broadcast a presence wire event to local subscribers. `opts` carries the
	 * per-call `relay` flag - the cross-instance fan-out is this plugin's own
	 * Redis relay, so local frames pass `{ relay: false }`. `compress: true` opts
	 * into permessage-deflate (presence is low-frequency; the opposite of the
	 * cursor 60Hz hot path).
	 * @param {string} fullTopic
	 * @param {string} event
	 * @param {any} data
	 * @param {import('svelte-adapter-uws').Platform} platform
	 * @param {{ relay?: boolean }} [opts]
	 */
	function emit(fullTopic, event, data, platform, opts) {
		const wireOptions = opts ? { ...opts, compress: true } : { compress: true };
		if (wireCodec && typeof platform.publishWire === 'function') {
			platform.publishWire(fullTopic, event, data, wireCodec, wireOptions);
		} else {
			platform.publish(fullTopic, event, data, wireOptions);
		}
	}

	/**
	 * Single-target variant of {@link emit} (the `state` snapshot).
	 * @param {any} ws
	 * @param {string} fullTopic
	 * @param {string} event
	 * @param {any} data
	 * @param {import('svelte-adapter-uws').Platform} platform
	 */
	function emitTo(ws, fullTopic, event, data, platform) {
		if (wireCodec && typeof platform.sendWire === 'function') {
			platform.sendWire(ws, fullTopic, event, data, wireCodec, { compress: true });
		} else {
			platform.send(ws, fullTopic, event, data, { compress: true });
		}
	}
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
	const mJoinsAborted = m?.counter('presence_joins_aborted_total', 'Presence join calls that aborted before commit because the websocket closed during an async gap. Server state was rolled back before the throw. Distinct from `presence_joins_total` (commits) and from generic RPC error metrics (which bucket all throws together regardless of cause).', ['topic', 'reason']);
	const mLeaves = m?.counter('presence_leaves_total', 'Presence leave events', ['topic']);
	const mHeartbeats = m?.counter('presence_heartbeats_total', 'Heartbeat refresh cycles');
	const mTotalOnline = m?.gauge('presence_total_online', 'Unique users present per topic on this instance', ['topic']);
	const mHeartbeatLatency = m?.gauge('presence_heartbeat_latency_ms', 'Duration of the most recent heartbeat tick in milliseconds');
	const mKeyspaceCleanups = m?.counter('presence_keyspace_cleanups_total', 'Topics whose hash expiry triggered a local empty-list emit');
	const mDiffFrames = m?.counter('presence_diff_frames_total', 'diff frames published to topic subscribers', ['topic']);
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
	 * Local per-topic data cache for heartbeat updates. `data` is the identity
	 * (from `select`); `fields` (lazily allocated, `null` until the first
	 * `update()` touches this user on this instance) holds the dynamic fields
	 * set via `update()` (durable AND transient, kept for per-field change
	 * detection). `publicData()` merges identity + durable fields, stripping
	 * transient, for every snapshot-shaped path (heartbeat, the join roster at
	 * flush). Mirrors the adapter's `{ data, fields }` per-user entry.
	 * @type {Map<string, Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>>}
	 */
	const localData = new Map();

	/**
	 * The public presence value for a user: identity `data` merged with the
	 * user's durable dynamic `fields`, transient fields stripped. Used by every
	 * snapshot-shaped path (`state` reconstruction from Redis, the heartbeat
	 * roster, the join roster at flush) so a (re)joiner never sees a transient
	 * value. The no-`fields` user (the overwhelming common case) returns
	 * `entry.data` with zero copy - keeping a no-`update()` deployment's wire
	 * byte-identical to a deployment that never calls update(). Works on both a local cache entry
	 * (`{ data, fields }`) and a parsed Redis entry (`{ data, fields, ts }`);
	 * Redis only ever stores durable fields, so the transient strip is a no-op
	 * there but harmless.
	 * @param {{ data: Record<string, any>, fields?: Record<string, any> | null }} entry
	 * @returns {Record<string, any>}
	 */
	function publicData(entry) {
		if (!entry.fields) return entry.data;
		const out = { ...entry.data };
		for (const k of Object.keys(entry.fields)) {
			if (!transientFields.has(k)) out[k] = entry.fields[k];
		}
		return out;
	}

	/**
	 * Set a user's identity `data` on the local per-topic cache, PRESERVING any
	 * dynamic `fields` already tracked for the user. The identity-churn paths
	 * (join, data-change, leave-restore, rollback) re-set `data` repeatedly;
	 * dynamic fields are user-level and orthogonal, so they must ride across
	 * those re-sets rather than be clobbered.
	 * @param {Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>} topicData
	 * @param {string} key
	 * @param {Record<string, any>} data
	 */
	function setLocalData(topicData, key, data) {
		const existing = topicData.get(key);
		topicData.set(key, existing ? { data, fields: existing.fields } : { data, fields: null });
	}

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
	 * leaves on the same key in one event-loop iteration collapse so the
	 * wire only sees the net change. Flushed once per iteration via
	 * `setTimeout(flushPendingDiffs, 0)` armed when the first dirty entry
	 * lands. Mirrors the buffer model the adapter's bundled presence
	 * plugin uses, so a single client decoder handles both.
	 *
	 * Why `setTimeout(0)` and not `queueMicrotask`: uWS dispatches each WS
	 * message as its own JS task, and N-API drains microtasks at the C++/JS
	 * boundary between tasks. A microtask-deferred flush fires BEFORE the
	 * next socket's handler runs, so cross-socket coalescing is impossible
	 * at the microtask level - a mass-join into a populated topic produces
	 * O(N) one-entry publishes instead of one batched diff. `setTimeout(0)`
	 * lands in libuv's timers phase, which fires only after the poll phase
	 * has dispatched every ready socket message in the current iteration -
	 * so all joins arriving together end up in one flush regardless of how
	 * many task boundaries separate them. Same structural choice the
	 * 0.5.7 cursor always-tick rewrite locked in.
	 *
	 * @type {Map<string, Map<string, { op: 'join' | 'leave', data: Record<string, any> }>>}
	 */
	const pendingDiffs = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} */
	let diffFlushTimer = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let diffFlushPlatform = null;

	function armDiffFlush(platform) {
		diffFlushPlatform = platform;
		if (diffFlushTimer === null) {
			diffFlushTimer = setTimer(flushPendingDiffs, 0);
			if (diffFlushTimer.unref) diffFlushTimer.unref();
		}
	}

	function bufferDiff(topic, op, key, data, platform) {
		let entries = pendingDiffs.get(topic);
		if (!entries) {
			entries = new Map();
			pendingDiffs.set(topic, entries);
		}
		if (entries.has(key)) {
			mDiffCoalesced?.inc({ topic: mt(topic) });
		}
		// A join/leave supersedes any pending field-level update for the key:
		// the join roster re-reads publicData (durable fields included) and a
		// leave drops the user entirely, so a buffered update is moot.
		entries.set(key, { op, data });
		armDiffFlush(platform);
	}

	/**
	 * Buffer a field-level update for the next flush, collapsing against any op
	 * already pending for the key, exactly like the in-memory plugin:
	 *   - pending leave  -> drop (the user leaves this flush; the update is moot)
	 *   - pending join   -> drop (the join roster carries durable fields via
	 *     publicData; a transient change is correctly excluded on a fresh join)
	 *   - pending update -> accumulate the changed fields
	 * @param {string} topic
	 * @param {string} key
	 * @param {Record<string, any>} changed - durable + transient changed fields
	 * @param {import('svelte-adapter-uws').Platform} platform
	 */
	function bufferUpdate(topic, key, changed, platform) {
		let entries = pendingDiffs.get(topic);
		if (!entries) {
			entries = new Map();
			pendingDiffs.set(topic, entries);
		}
		const prev = entries.get(key);
		if (prev) {
			// A pending leave wins: the user is gone this flush, so the update is moot.
			if (prev.op === 'leave') return;
			if (prev.op === 'join') {
				// A LOCAL join re-reads publicData(localData) at flush, so its durable
				// fields are already current and the update is redundant (transient is
				// excluded on a fresh local join, matching the in-memory plugin). A
				// RELAYED join (the user is not presented on this instance, so flush
				// uses the buffered payload verbatim) must absorb the change, or a
				// cross-instance field update landing in the same tick as the relayed
				// join / updated event for that user is silently lost.
				if (!localData.get(topic)?.get(key) && prev.data && typeof prev.data === 'object') {
					Object.assign(prev.data, changed);
					armDiffFlush(platform);
				}
				return;
			}
			Object.assign(prev.changed, changed);
			armDiffFlush(platform);
			return;
		}
		entries.set(key, { op: 'update', changed: { ...changed } });
		armDiffFlush(platform);
	}

	function flushPendingDiffs() {
		if (diffFlushTimer !== null) {
			clearTimer(diffFlushTimer);
			diffFlushTimer = null;
		}
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
			/** @type {Record<string, Record<string, any>> | null} */
			let updates = null;
			const localUsers = localData.get(topic);
			for (const [key, e] of entries) {
				if (e.op === 'join') {
					// Re-read the live local entry so the join roster carries the
					// user's latest durable fields (publicData strips transient).
					// A relayed join for a user this instance does not present has
					// no local entry and falls back to the relayed payload.
					const localEntry = localUsers && localUsers.get(key);
					joins[key] = localEntry ? publicData(localEntry) : e.data;
				} else if (e.op === 'leave') {
					leaves[key] = e.data;
				} else {
					if (!updates) updates = {};
					updates[key] = e.changed;
				}
			}
			// Keep the common `{ joins, leaves }` shape byte-identical when no
			// field-level update is pending, so a deployment that never calls
			// update() sees an unchanged wire. `updates` is additive: an old
			// client ignores it.
			const diff = updates ? { joins, leaves, updates } : { joins, leaves };
			try {
				// Presence WS frames opt INTO compression. They are low-frequency -
				// diffs coalesce per tick, heartbeat is periodic, state is on-attach -
				// so per-subscriber deflate CPU is amortized and the roster JSON
				// compresses well. This is the deliberate counterpart to the cursor
				// plugin's compress:false 60Hz hot path, and matches the bundled
				// in-memory presence plugin. No-op while websocket.compression is off
				// (the default): the adapter resolves the flag to false regardless.
				emit('__presence:' + topic, 'diff', diff, platform, { relay: false });
				mDiffFrames?.inc({ topic: mt(topic) });
			} catch { /* platform unavailable mid-flight */ }
		}
		pendingDiffs.clear();
	}

	// Per-topic hash: one field per unique user on the topic. Backs list() / count().
	function topicHashKey(topic) {
		return client.key('presence:topic:{' + topic + '}');
	}

	// Per-user hash for a topic: one field per instance currently presenting this
	// user. HLEN drives the JOIN/LEAVE broadcast decision.
	function userHashKey(topic, userKey) {
		return client.key('presence:user:{' + topic + '}:' + userKey);
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
	const heartbeatTimer = setIntervalTimer(() => {
		const tickStart = monotonicNow();
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
			lastHeartbeatLatency = monotonicNow() - tickStart;
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
		const commands = [];
		for (const topic of activeTopics) {
			const data = localData.get(topic);
			if (data && data.size > 0) {
				const topicHash = topicHashKey(topic);
				for (const userKey of data.keys()) {
					commands.push(['hpexpire', userHashKey(topic, userKey), presenceTtlMs, 'FIELDS', 1, instanceId]);
					commands.push(['hpexpire', topicHash, presenceTtlMs, 'FIELDS', 1, userKey]);
				}
				if (activePlatform) {
					// Publish a `{userKey: data}` map (instead of a key-only
					// array) so a client whose entry aged out between
					// heartbeats can re-add it from the heartbeat alone.
					// Pre-fix, the wire carried only `keys` and the client
					// handler could only refresh `existing` entries; an
					// entry the client swept (cross-replica relay latency,
					// brief backpressure, JS thread saturation) could never
					// be recovered without a diff for that user.
					// Older clients fall back gracefully: they see an
					// object instead of an array and skip the legacy
					// "refresh-existing" branch, but the next diff
					// or state still reconciles them.
					/** @type {Record<string, any>} */
					const dataMap = {};
					for (const [userKey, entry] of data) dataMap[userKey] = publicData(entry);
					emit('__presence:' + topic, 'heartbeat', dataMap, activePlatform);
				}
			}
		}
		execMultiSlot(redis, commands).catch(() => {});
		lastHeartbeatLatency = monotonicNow() - tickStart;
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
					} else if (ev === INTERNAL_EVENTS.FIELDS) {
						// Field-level update from another instance. Fan it out to this
						// instance's local subscribers as an `updates` diff entry
						// (durable + transient changed fields together). If this
						// instance also presents the user (multi-instance multi-tab),
						// merge into the local field view so this instance's heartbeat
						// carries the durable value and its own change detection stays
						// consistent (transient is held but stripped by publicData).
						const key = payload?.key;
						if (typeof key === 'string') {
							const durable = (payload.durable && typeof payload.durable === 'object') ? payload.durable : {};
							const transient = (payload.transient && typeof payload.transient === 'object') ? payload.transient : {};
							const changed = { ...durable, ...transient };
							if (Object.keys(changed).length > 0) {
								bufferUpdate(topic, key, changed, activePlatform);
								const localEntry = localData.get(topic)?.get(key);
								if (localEntry) {
									if (!localEntry.fields) localEntry.fields = {};
									Object.assign(localEntry.fields, durable, transient);
								}
							}
						}
					}
				} catch {
					// Malformed, skip
				}
			});
			if (keyspaceNotifications) {
				// The per-topic hash key expires only when every field has
				// expired (no live instances presenting any user on this
				// topic). That is the "whole topic empty" signal we forward
				// as an empty state to local subscribers. Per-user
				// hash keys (presence:user:{topic}:{userKey}) and the events
				// channel are filtered out.
				const topicPrefix = client.key('presence:topic:{');
				subscriber.on('pmessage', (_pattern, _channel, expiredKey) => {
					if (typeof expiredKey !== 'string') return;
					if (!expiredKey.startsWith(topicPrefix)) return;
					const topic = expiredKey.slice(topicPrefix.length, -1); // drop the '}' closing the {topic} hash tag
					if (activePlatform) {
						emit('__presence:' + topic, 'state', {}, activePlatform, { relay: false });
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
			clearTimer(idleTimer);
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
				idleTimer = setTimer(() => {
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
				setLocalData(topicData, key, prevData);
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
			const ts = now();
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
							setLocalData(topicData, key, newest);
							const ts = now();
							try {
								await redis.eval(
									JOIN_SCRIPT, 2,
									userHashKey(topic, key), topicHashKey(topic),
									instanceId, key, JSON.stringify({ data: newest, ts }), ts, presenceTtlMs
								);
								bufferDiff(topic, 'join', key, newest, platform);
								publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: publicData(topicData.get(key)) });
							} catch {
								setLocalData(topicData, key, cached.data);
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
			setLocalData(topicData, key, newest);
			const ts = now();
			try {
				await redis.eval(
					JOIN_SCRIPT, 2,
					userHashKey(topic, key), topicHashKey(topic),
					instanceId, key, JSON.stringify({ data: newest, ts }), ts, presenceTtlMs
				);
				bufferDiff(topic, 'join', key, newest, platform);
				pendingUpdatedRelays.push(publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: publicData(topicData.get(key)) }));
			} catch {
				setLocalData(topicData, key, cached.data);
			}
		}

		// Step 2: async Redis cleanup. Local state is already clean so the
		// heartbeat will not refresh any of these entries. The pipeline
		// batches all LEAVE_SCRIPT evals into a single round-trip; under
		// mass disconnect (1000+ connections) this avoids saturating the
		// Redis command queue. Each LEAVE_SCRIPT is now O(1) Redis-blocked
		// Lua time, so the total Redis-blocked time scales linearly with
		// N (the disconnect count), not with N x M (where M was the topic
		// hash size in the previous storage layout).
		const unsubPromises = [];
		for (const { needsUnsub, topic } of pendingLeaves) {
			if (needsUnsub) {
				unsubPromises.push(unsubscribeFromTopic(topic));
			}
		}
		if (unsubPromises.length > 0) await Promise.all(unsubPromises);

		const commands = [];
		for (const { topic, key } of pendingLeaves) {
			commands.push([
				'eval', LEAVE_SCRIPT, 2,
				userHashKey(topic, key), topicHashKey(topic),
				instanceId, key
			]);
		}

		let results;
		let skipPipeline = false;
		if (b) { try { b.guard(); } catch { skipPipeline = true; } }
		if (!skipPipeline) {
			try {
				results = await execMultiSlot(redis, commands);
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

	// Throw helper for "ws closed during async gap" paths inside join(). All
	// five callsites need the same metric label and the same typed error;
	// inlining a helper avoids drift between them and keeps each callsite
	// single-line.
	function throwWsClosed(topic) {
		mJoinsAborted?.inc({ topic: mt(topic), reason: 'ws_closed' });
		throw new WsClosedError('presence.join', topic);
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

			// ws closed during `await subscribeToTopic`. The close hook already
			// ran leaveAll, which swept localCounts / wsTopics for this ws;
			// no compensating undoJoin needed. Throw so the caller sees the
			// abort instead of a silent success.
			if (!wsTopics.has(ws)) throwWsClosed(topic);

			try { ws.getBufferedAmount(); } catch {
				await undoJoin(ws, topic, key, data, prevCount, prevData, false, false, platform);
				throwWsClosed(topic);
			}

			let didRedisWrite = false;
			let isNewUser = false;
			// `serializedData` is computed above to surface non-JSON-serializable
			// data eagerly via the throw. The body below stringifies the full
			// {data, ts} envelope per call since ts is fresh.

			if (prevCount === 0) {
				const ts = now();
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
					// the per-user hash entry does not linger past TTL, then
					// surface the abort to the caller.
					await redis.eval(
						LEAVE_SCRIPT, 2,
						userHashKey(topic, key), topicHashKey(topic),
						instanceId, key
					).catch(() => {});
					throwWsClosed(topic);
				}
			} else if (prevData !== undefined && !deepEqual(prevData, data)) {
				// Same instance, same user, different `select()` output.
				// JOIN_SCRIPT's newer-ts conditional set overwrites the per-
				// topic data, and refreshes the per-user-hash TTL so this
				// path counts as an implicit heartbeat for our entry.
				try {
					const ts = now();
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
				if (td) setLocalData(td, key, data);

				bufferDiff(topic, 'join', key, data, platform);
				await publishEvent(topic, INTERNAL_EVENTS.UPDATED, { key, data: td ? publicData(td.get(key)) : data });
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
				throwWsClosed(topic);
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
				throwWsClosed(topic);
			}

			// Commit localData and activeTopics now that the join is
			// fully committed. The heartbeat reads from these, so they
			// must not be visible during any of the async gaps above.
			let topicData = localData.get(topic);
			if (!topicData) {
				topicData = new Map();
				localData.set(topic, topicData);
			}
			setLocalData(topicData, key, data);
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
				state[userKey] = publicData(entry);
			}
			try {
				emitTo(ws, '__presence:' + topic, 'state', state, platform);
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
				state[userKey] = publicData(entry);
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
				emitTo(ws, presenceTopic, 'state', state, platform);
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

		async update(ws, topic, fields, platform) {
			if (topic.startsWith('__')) return;
			if (!fields || typeof fields !== 'object' || Array.isArray(fields)) return;
			// Resolve the user this connection represents on the topic from local
			// state. A connection that is not present (never joined, or its join
			// has not committed localData yet during an async gap) is a silent
			// no-op - presence is best-effort. The update applies to the user (per
			// dedup key), so any of a multi-tab user's connections may set it.
			const connTopics = wsTopics.get(ws);
			const connEntry = connTopics && connTopics.get(topic);
			if (!connEntry) return;
			const key = connEntry.key;
			const topicData = localData.get(topic);
			const entry = topicData && topicData.get(key);
			if (!entry) return;
			if (!entry.fields) entry.fields = {};
			// Per-field change detection against this instance's field view. Only
			// fields whose value actually changed are merged and broadcast (the
			// field-level delta). Durable and transient changes are split: durable
			// is persisted to Redis so a cross-instance state read includes it;
			// transient is relay-only and never persisted.
			/** @type {Record<string, any>} */
			const changedDurable = {};
			/** @type {Record<string, any>} */
			const changedTransient = {};
			let any = false;
			for (const k of Object.keys(fields)) {
				const v = fields[k];
				if (!deepEqual(entry.fields[k], v)) {
					entry.fields[k] = v;
					if (transientFields.has(k)) changedTransient[k] = v;
					else changedDurable[k] = v;
					any = true;
				}
			}
			if (!any) return;

			// Local fan-out: buffer the update diff (durable + transient together)
			// for this instance's subscribers. Coalesces with same-tick ops per
			// the bufferUpdate collapse rules.
			bufferUpdate(topic, key, { ...changedDurable, ...changedTransient }, platform);

			// Persist durable fields to the per-topic hash value so a cross-instance
			// state read (HGETALL) reconstructs them. Best-effort under the breaker:
			// the field still relays + buffers locally if the write is skipped, and
			// a later durable update reconciles the cross-instance read.
			const durableKeys = Object.keys(changedDurable);
			if (durableKeys.length > 0) {
				let skip = false;
				if (b) { try { b.guard(); } catch { skip = true; } }
				if (!skip) {
					try {
						const ts = now();
						await redis.eval(
							UPDATE_SCRIPT, 1, topicHashKey(topic),
							key, JSON.stringify(changedDurable), ts, presenceTtlMs
						);
						b?.success();
					} catch (err) {
						b?.failure(err);
					}
				}
			}

			// Relay to other instances (durable + transient) so their local
			// subscribers see the same field-level update. The receiving instance
			// buffers it as an `updates` diff entry and, if it also presents the
			// user, merges the durable value into its own field view.
			await publishEvent(topic, INTERNAL_EVENTS.FIELDS, { key, durable: changedDurable, transient: changedTransient });
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
			if (diffFlushTimer !== null) {
				clearTimer(diffFlushTimer);
				diffFlushTimer = null;
			}
			diffFlushPlatform = null;
			connCounter = 0;
		},

		destroy() {
			clearIntervalTimer(heartbeatTimer);
			if (idleTimer) {
				clearTimer(idleTimer);
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
			if (diffFlushTimer !== null) {
				clearTimer(diffFlushTimer);
				diffFlushTimer = null;
			}
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
			message(ws, { data, platform }) {
				// Client-initiated reconnect-snapshot. The presence plugin
				// client sends `{type:'presence-snapshot', topic}` on every
				// status==='open' (initial connect + reconnect). Re-emits
				// `state` to the requesting ws via `tracker.sync`,
				// which is the same path that fires on a fresh subscribe.
				// Symmetric to cursor's `cursor-snapshot` text frame.
				//
				// Without this, board-scoped presence stayed stale across
				// reconnects: a tab that had joined via an RPC saw no
				// diff during the disconnect window, and on
				// reconnect its in-memory map was whatever it last knew.
				// Global presence accidentally self-healed because most
				// apps call `presence.join('global')` from the `open` hook
				// which fires on every reconnect; per-board presence does
				// not have an equivalent auto-rejoin.
				if (data && data.type === 'presence-snapshot' && typeof data.topic === 'string') {
					tracker.sync(ws, data.topic, platform).catch(() => { /* surfaced via breaker */ });
				}
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
