/**
 * Presence tracker context assembler.
 *
 * Builds the per-createPresence state and helpers that the tracker nucleus closes
 * over: the resolved option config, the public-view projection, the wire codec and
 * its emit/emitTo, the Redis client handle + the 7.4 feature probe, the metric
 * handles, the participant-state collections (wsTopics/localCounts/localData/
 * syncObservers/syncCounts/hgetallInflight), the per-topic key/channel builders, and
 * the HGETALL coalescer. Pure construction (no timers, no subscriptions, no tracker
 * behavior); the nucleus in presence.js wires the sub-factories and the lifecycle on
 * top of this context. The participant collections are returned by reference so the
 * nucleus mutates the live maps.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/state
 */

import { randomBytes } from '../../shared/runtime.js';
import { stripInternal, createSensitiveWarner } from '../../shared/sensitive.js';
import { hashFieldTTLSupport } from '../../shared/redis-version.js';
import { createPresenceWireCodec } from 'svelte-adapter-uws/plugins/presence';
import { makeKeys } from './keys.js';
import { makePublicData } from './data.js';

/**
 * Assemble the presence context for one createPresence instance.
 *
 * @param {import('../index.js').RedisClient} client
 * @param {import('../presence.js').RedisPresenceOptions} [options]
 */
export function createPresenceState(client, options = {}) {
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

	// publicData() projects a participant's public view (identity + durable
	// fields, transient fields stripped); it closes over transientFields, so
	// data.js curries it via makePublicData.
	const publicData = makePublicData(transientFields);

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

	// Per-field hash TTL (HPEXPIRE / HEXPIRE) requires Redis 7.4+ or Valkey 9.0+.
	// Valkey pins redis_version at 7.2.4 and reports its real version in
	// valkey_version, so the probe is server-aware (see hashFieldTTLSupport).
	// Deferred to first use so createPresence() stays synchronous and fast; the
	// probe runs once, caches its result, and rejects any further redis call with
	// a clear error if the server is too old. Mirrors the gating pattern
	// createShardedBus uses for SPUBLISH / SSUBSCRIBE. (Name kept for the existing
	// internal callers; it now accepts Valkey 9.0+ too.)
	let featureProbe = null;
	function ensureRedis74() {
		if (!featureProbe) {
			featureProbe = redis.info('server').then((info) => {
				const support = hashFieldTTLSupport(info);
				if (support.supported === false) {
					throw new Error(
						'redis presence: requires Redis 7.4+ or Valkey 9.0+ for per-field TTL (HEXPIRE/HPEXPIRE); ' +
						'got ' + support.server + ' ' + support.version + '. Upgrade the server or use the in-memory ' +
						'createPresence plugin from svelte-adapter-uws/plugins/presence.'
					);
				}
				// null (unparseable) falls through - assume compatible rather than
				// locking out an unrecognized server.
			}).catch((err) => {
				// Reset on transient INFO failures so we re-probe on next call.
				// Hard errors (version mismatch) re-throw verbatim from the await.
				if (err && /per-field TTL/.test(err.message)) throw err;
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
	const mKeyspaceCleanups = m?.counter('presence_keyspace_cleanups_total', 'Topics whose hash removal triggered a local empty-list emit');
	const mDiffFrames = m?.counter('presence_diff_frames_total', 'diff frames published to topic subscribers', ['topic']);
	const mDiffCoalesced = m?.counter('presence_diff_coalesced_total', 'Buffered diff entries overwritten by a later op in the same tick', ['topic']);

	const warnSensitive = createSensitiveWarner('redis/presence');

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

	// Redis key + channel builders. The {topic} hash-tag colocation that keeps a
	// topic's two keys on one cluster slot lives in keys.js (kept in lockstep with
	// the KEYS arity documented in lua.js).
	const { topicHashKey, userHashKey, eventChannel } = makeKeys(client);

	function coalesceHgetall(topic) {
		const key = topicHashKey(topic);
		let pending = hgetallInflight.get(key);
		if (!pending) {
			pending = redis.hgetall(key).finally(() => hgetallInflight.delete(key));
			hgetallInflight.set(key, pending);
		}
		return pending;
	}

	return {
		keyField,
		select,
		heartbeatInterval,
		presenceTtl,
		presenceTtlMs,
		transientFields,
		publicData,
		wireCodec,
		emit,
		emitTo,
		instanceId,
		redis,
		keyspaceNotifications,
		ensureRedis74,
		b,
		m,
		mt,
		mJoins,
		mJoinsAborted,
		mLeaves,
		mHeartbeats,
		mTotalOnline,
		mHeartbeatLatency,
		mKeyspaceCleanups,
		mDiffFrames,
		mDiffCoalesced,
		warnSensitive,
		wsTopics,
		localCounts,
		localData,
		syncObservers,
		syncCounts,
		hgetallInflight,
		topicHashKey,
		userHashKey,
		eventChannel,
		coalesceHgetall
	};
}
