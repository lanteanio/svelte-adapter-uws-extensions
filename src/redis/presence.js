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
 * Storage layout (Redis 7.4+ HEXPIRE for per-field TTL):
 *   - `{prefix}presence:topic:{topic}` - hash, field=userKey, value=JSON{data,ts,fields?}
 *       One entry per unique user on the topic. Backs `list()` / `count()`.
 *   - `{prefix}presence:pending:{topic}` - hash, field=userKey, private identity generations
 *       and committed-identity fence. Per-user expiry, excluded from public roster reads.
 *   - `{prefix}presence:user:{topic}:{userKey}` - hash, field=instanceId, value=membership token
 *       One entry per instance presenting or reserving this user. HLEN determines
 *       final-membership removal; first public identity commit determines JOIN.
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
	wallEpoch,
	setTimer,
	clearTimer,
	setIntervalTimer,
	clearIntervalTimer
} from '../shared/runtime.js';
import { evalCached } from '../shared/eval-cached.js';
import { scanAndUnlink, scanKeys } from '../shared/redis-scan.js';
import { topicInTenant } from '../shared/tenant-topic.js';
import { execMultiSlot, firstCommandError } from '../shared/cluster.js';
import { coalescedWork } from '../shared/coalesced-work.js';
import { MAX_PROJECTION_DEPTH, exceedsDepth } from '../shared/sensitive-names.js';
import { withBreaker } from '../shared/breaker.js';
import { guardBreaker } from '../shared/breaker-admission.js';
import { MAX_PRESENCE_WS, MAX_PRESENCE_TOPICS } from '../shared/caps.js';
import { WsClosedError } from '../shared/errors.js';
import { isFramableTopic } from '../shared/frame-topic.js';
import { addWsSubscription, removeWsSubscription, OBSERVER_LANE } from '../shared/ws-subscriptions.js';
import { createPresenceWireCodec } from 'svelte-adapter-uws/plugins/presence';
import { JOIN_SCRIPT, FINALIZE_JOIN_SCRIPT, ROLLBACK_JOIN_SCRIPT, RESTORE_IDENTITY_SCRIPT, LEAVE_SCRIPT, UPDATE_SCRIPT, INTERNAL_EVENTS } from './presence/lua.js';
import { makeKeys } from './presence/keys.js';
import { deepEqual, parseEntries, setLocalData, makePublicData } from './presence/data.js';
import { createLocalIndex } from './presence/local-index.js';
import { newFieldMap, withoutReservedNames } from './presence/field-policy.js';
import { createDiffBuffer } from './presence/diff-buffer.js';
import { createSubscriber } from './presence/subscriber.js';
import { createPresenceState } from './presence/state.js';
import { buildPresenceAuditSnapshot } from './presence/audit-snapshot.js';
import { assert, fatal } from '../shared/assert.js';
import { createConsistencyAuditor } from '../shared/auditor.js';
import { checkRedisPresenceLocalIndex } from '../shared/invariants.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure } from '../shared/diagnostic.js';

const onlineMetricCollectors = new WeakMap();
function collectOnlineMetric(gauge, mapTopic, counts) {
	let collectors = onlineMetricCollectors.get(gauge);
	if (!collectors) {
		collectors = new Set();
		onlineMetricCollectors.set(gauge, collectors);
		gauge.collect(() => {
			for (const entry of collectors) {
				for (const [topic, users] of entry.counts) gauge.inc({ topic: entry.mapTopic(topic) }, users.size);
			}
		});
	}
	const entry = { mapTopic, counts };
	collectors.add(entry);
	return () => collectors.delete(entry);
}


const EMPTY_FIELDS = Object.freeze(Object.create(null));

function fieldValueBytes(value) {
	if (value === null) return 4;
	if (typeof value === 'boolean') return value ? 4 : 5;
	if (typeof value === 'number') return Number.isFinite(value) ? String(value).length : 4;
	return Buffer.byteLength(JSON.stringify(value) ?? '');
}

export { WsClosedError };

/**
 * @typedef {Object} RedisPresenceOptions
 * @property {string} [key='id'] - Field in the selected data for user dedup. A credential-shaped or transport field name is not copied by the default select, so each connection then gets its own entry and the value never becomes a roster property name.
 * @property {(userData: any) => Record<string, any>} [select] - Extract the public roster data from userData. By default only the `key` field is copied, and only when its value is a string or finite number. An explicit select is the application's own policy and its result is used as-is, apart from top-level `__`-prefixed, `constructor` and `prototype` names, which every instance drops.
 * @property {number} [maxFieldsBytes=8192] - Maximum serialized bytes in one update; oversized updates are dropped.
 * @property {number} [maxTotalFieldsBytes=65536] - Maximum cumulative field bytes per user, including serialized names and framing.
 * @property {number} [maxTopicsPerConnection=100] - Maximum presence topics per connection; excess joins are ignored.
 * @property {number} [topicThrottle=16] - Minimum diff batching interval in milliseconds; zero flushes on the next timer turn.
 * @property {number} [heartbeat=30000] - Heartbeat interval in ms (how often to refresh per-field TTLs)
 * @property {number} [ttl=90] - TTL in seconds for presence entries (should be > heartbeat * 3). Applied per-field via HPEXPIRE; fields auto-expire field-by-field rather than at whole-key granularity.
 * @property {boolean} [keyspaceNotifications=false] - Subscribe to `__keyevent@*__:del` so a topic's local subscribers receive an empty `state` event the moment its per-topic presence hash is removed (instance-died scenario: the server deletes the hash when its last field's TTL lapses). Requires `CONFIG SET notify-keyspace-events Eg` (or any flagset including `E` and `g`) on every master; Redis accepts the subscription without it, so the tracker reads the setting when it subscribes and reports flags that cannot deliver the deletion (a server that refuses CONFIG is not checked). Per-field expiry that leaves other users on the topic does not fire it.
 * @property {number} [maxEnvelopeBytes=1048576] - Byte ceiling for one cross-instance presence envelope, enforced before inbound JSON.parse and before outbound publish. Oversized outbound lifecycle events warn and drop rather than failing the connection operation.
 * @property {string[]} [clientUpdateFields] - Field names a client's wire `presence-update` frame may write. Unset (the default), a client frame writes nothing. Set, it is also the whole rule for direct `update()` calls, replacing the reserved-name guard; prototype and `__`-prefixed names are always left out.
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
	const ctx = createPresenceState(client, options);
	const {
		maxFieldsBytes, maxTotalFieldsBytes, maxTopicsPerConnection, topicThrottle,
		keyField, select, fieldNames, warnReservedField, heartbeatInterval, presenceTtlMs, transientFields, publicData,
		emit, emitTo, instanceId, redis, keyspaceNotifications, maxEnvelopeBytes, ensureRedis74, b, mt,
		mJoins, mJoinsAborted, mLeaves, mHeartbeats, mTotalOnline, mHeartbeatLatency,
		mKeyspaceCleanups, mDiffFrames, mDiffCoalesced, mSelfPreserveActive, mSelfPreserveHeld,
		mSelfPreserveActivations, selfPreservation, wsTopics, localCounts,
		localData, syncObservers, syncCounts, topicHashKey, userHashKey, pendingHashKey, eventChannel,
		coalesceHgetall
	} = ctx;

	let lastHeartbeatLatency = 0;
	// Set while heartbeat publishes keep failing, so a failure streak is
	// reported once; a heartbeat with no failure clears it.
	let heartbeatPublishReported = false;
	let staleCleanedTotal = 0;
	let connCounter = 0;

	function resolveKey(data) {
		if (data && (typeof data === 'object' || typeof data === 'function') && Object.prototype.hasOwnProperty.call(data, keyField) && data[keyField] != null) {
			const key = String(data[keyField]);
			// A roster key is a plain-object property name in every snapshot,
			// diff and accumulator downstream. `__proto__` there would target
			// the prototype instead of the roster (the entry silently vanishes
			// from state and diffs), so a user whose keyField carries one of
			// these falls back to a connection-scoped key, exactly as the
			// adapter's presence plugin does.
			if (key !== '__proto__' && key !== 'constructor' && key !== 'prototype') return key;
		}
		return '__conn:' + (++connCounter);
	}

	// Local reverse index (topic, key) -> ws connections; reads the shared wsTopics map.
	const { indexAdd, indexRemove, findOtherWsData, topicKeyCount, clear: clearIndex } = createLocalIndex(wsTopics);

	// Per-topic diff coalescer; disposeDiffBuffer() is the clear()/destroy() teardown.
	const { bufferDiff, bufferUpdate, flushPendingDiffs, dispose: disposeDiffBuffer } = createDiffBuffer({
		emit,
		localData,
		publicData,
		transientFields,
		topicThrottle,
		mt,
		mDiffCoalesced,
		mDiffFrames
	});

	// Heartbeat: refresh timestamps on local entries, TTL on hash keys,
	// and clean up stale fields from crashed instances
	/** @type {Set<string>} */
	const activeTopics = new Set();
	// Self-preservation hold state: dead sockets whose eviction is deferred
	// because they died as part of a suspected network event (a large
	// fraction of the tracked population failing the probe in one tick).
	// While held, a socket stays in wsTopics/localData, so the refresh loop
	// below keeps its cluster TTLs alive - the roster survives the blip and
	// the user's reconnect lands on an unbroken entry instead of a
	// leave/join flap. Keyed by ws; value is the hold start (monotonic ms).
	/** @type {Map<any, number>} */
	const spHeld = new Map();
	let spActive = false;
	let spSince = 0;
	// Collect current memberships at scrape time, summing topics mapped to the
	// same class without retaining departed topic labels or work on every tick.
	const stopOnlineMetric = mTotalOnline ? collectOnlineMetric(mTotalOnline, mt, localCounts) : null;

	const heartbeatTick = coalescedWork(async () => {
		const tickStart = monotonicNow();
		mHeartbeats?.inc();
		// Detect dead connections whose close handler never fired.
		// Under mass disconnect, the runtime may drop close events.
		// Probe each tracked ws; if the probe throws the socket is
		// dead and we synchronously purge it from local state so the
		// refresh loop below never touches it.
		if (subscriberCtx.activePlatform) {
			const dead = [];
			let total = 0;
			for (const [ws] of wsTopics) {
				total++;
				try { ws.getBufferedAmount(); } catch { dead.push(ws); }
			}
			if (selfPreservation === null) {
				for (const ws of dead) {
					// Full leave (sync Step 1 + async Step 2 fire-and-forget)
					tracker.leave(ws, subscriberCtx.activePlatform).catch(() => {});
				}
			} else {
				// A socket held earlier that has since left the tracked map
				// (a late close handler ran, or clear() swept it) needs no
				// eviction from here.
				for (const ws of spHeld.keys()) {
					if (!wsTopics.has(ws)) spHeld.delete(ws);
				}
				const fresh = [];
				for (const ws of dead) {
					if (!spHeld.has(ws)) fresh.push(ws);
				}
				// A dead socket never revives (the probe throwing means the
				// native handle is gone), so the guard's protection is TIME:
				// hold the leave storm while the users behind a correlated
				// mass drop reconnect on new sockets. The activation test is
				// per-tick simultaneity - a slow trickle of individual deaths
				// never reads as a network event. Hold timing runs on the
				// wall clock (the operator-facing axis the hold window is
				// specified in), not the tick-latency monotonic read.
				const nowWall = wallEpoch();
				if (!spActive
					&& total >= selfPreservation.minPopulation
					&& fresh.length / total >= selfPreservation.threshold) {
					spActive = true;
					spSince = nowWall;
					mSelfPreserveActivations?.inc();
					try { selfPreservation.onChange?.(true, { held: fresh.length, total }); } catch { /* app hook must not break the tick */ }
				}
				if (spActive) {
					for (const ws of fresh) spHeld.set(ws, nowWall);
					for (const [ws, heldAt] of spHeld) {
						if (nowWall - heldAt >= selfPreservation.holdMaxMs) {
							// The hold window elapsed with no reconnect visible
							// to us: this one really left. Evict for real.
							spHeld.delete(ws);
							tracker.leave(ws, subscriberCtx.activePlatform).catch(() => {});
						}
					}
					if (spHeld.size === 0) {
						spActive = false;
						try { selfPreservation.onChange?.(false, { held: 0, total }); } catch { /* app hook must not break the tick */ }
					}
				} else {
					for (const ws of fresh) {
						tracker.leave(ws, subscriberCtx.activePlatform).catch(() => {});
					}
				}
				mSelfPreserveActive?.set(spActive ? 1 : 0);
				mSelfPreserveHeld?.set(spHeld.size);
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
		let publishFailed = 0;
		let published = 0;
		let firstPublishError = null;
		let firstFailedTopic = '';
		for (const topic of activeTopics) {
			const data = localData.get(topic);
			if (data && data.size > 0) {
				const topicHash = topicHashKey(topic);
				for (const userKey of data.keys()) {
					commands.push(['hpexpire', userHashKey(topic, userKey), presenceTtlMs, 'FIELDS', 1, instanceId]);
					commands.push(['hpexpire', topicHash, presenceTtlMs, 'FIELDS', 1, userKey]);
					commands.push(['hpexpire', pendingHashKey(topic), presenceTtlMs, 'FIELDS', 1, userKey]);
				}
				if (subscriberCtx.activePlatform) {
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
					const dataMap = Object.create(null);
					for (const [userKey, entry] of data) dataMap[userKey] = publicData(entry);
					// Per topic: a publish that throws for one topic must not end the
					// tick, which would skip every other topic's heartbeat and, worse,
					// the TTL refresh below for all of them, letting the whole
					// instance's roster expire. The failure is reported, not dropped.
					published++;
					try {
						emit('__presence:' + topic, 'heartbeat', dataMap, subscriberCtx.activePlatform);
					} catch (err) {
						if (publishFailed++ === 0) { firstPublishError = err; firstFailedTopic = topic; }
					}
				}
			}
		}
		await execMultiSlot(redis, commands).then(results => {
			const error = firstCommandError(results);
			if (error) b?.failure(error);
			else if (commands.length) b?.success();
		}).catch(error => b?.failure(error));
		if (publishFailed === 0) {
			heartbeatPublishReported = false;
		} else if (!heartbeatPublishReported) {
			heartbeatPublishReported = true;
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_HEARTBEAT_PUBLISH, {
				error: firstPublishError,
				redact: [firstFailedTopic, '__presence:' + firstFailedTopic],
				attributes: { failed: publishFailed, topics: published }
			});
		}
		lastHeartbeatLatency = monotonicNow() - tickStart;
		mHeartbeatLatency?.set(lastHeartbeatLatency);
	});
	const heartbeatTimer = setIntervalTimer(heartbeatTick, heartbeatInterval);
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Cross-instance subscriber: receives peers' join/leave/update events on the
	// per-topic channels, routes them into the diff buffer, and forwards topic-key
	// expiry as an empty state. Owns the duplicate connection, the channel set, the
	// idle timer, and the active platform. Lives in subscriber.js.
	const subscriberCtx = createSubscriber({
		client,
		instanceId,
		maxEnvelopeBytes,
		keyspaceNotifications,
		bufferDiff,
		bufferUpdate,
		localData,
		emit,
		eventChannel,
		mKeyspaceCleanups,
		acceptsField: fieldNames.fromServer,
		transientFields
	});
	const { subscribeToTopic, unsubscribeFromTopic } = subscriberCtx;

	// UTF-8 needs at most three bytes per UTF-16 code unit.
	const safeEnvelopeChars = Math.floor(maxEnvelopeBytes / 3);
	const safeFieldsChars = Math.floor(maxFieldsBytes / 3);
	// Internal relay metadata keeps selected identity separate from durable
	// fields that may shadow it. The public data member remains old-peer readable;
	// true means that public data is already the complete selected identity.
	function identityPayload(key, entry) {
		const payload = { key, data: publicData(entry), identity: entry.fields ? entry.data : true };
		if (entry.authorityToken) payload.authority = entry.authorityToken;
		if (entry.fields) {
			const fields = newFieldMap();
			for (const name of Object.keys(entry.fields)) {
				if (!transientFields.has(name)) fields[name] = entry.fields[name];
			}
			payload.fields = fields;
		}
		return payload;
	}

	// Reuse the eager identity encoding when building the Redis entry.
	let joinSequence = 0;
	function storedValue(serializedData, ts) {
		return serializedData === undefined ? JSON.stringify({ ts })
			: '{"data":' + serializedData + ',"ts":' + JSON.stringify(ts) + '}';
	}

	async function publishEvent(topic, event, payload) {
		const ch = eventChannel(topic);
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		// Hold outbound events to the bound every peer enforces inbound.
		// Publishing past it fans out locally and to no remote instance,
		// which reads as a partial roster rather than as an error.
		if (msg.length > safeEnvelopeChars && Buffer.byteLength(msg) > maxEnvelopeBytes) {
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_ENVELOPE_OVERSIZED, {
				attributes: { event, bytes: Buffer.byteLength(msg), maxEnvelopeBytes }
			});
			return;
		}
		await redis.publish(ch, msg).catch(() => {});
	}

	/** Remove this reservation and clean up only membership still owned by it. */
	async function undoJoin(ws, topic, key, reservation, write, platform) {
		const connTopics = wsTopics.get(ws);
		if (connTopics?.get(topic) === reservation) {
			connTopics.delete(topic);
			if (connTopics.size === 0) wsTopics.delete(ws);
			indexRemove(topic, key, ws);
			if (!syncObservers.get(ws)?.has(topic)) {
				try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				removeWsSubscription(ws, '__presence:' + topic);
			}
			const counts = localCounts.get(topic);
			const remaining = (counts?.get(key) || 0) - 1;
			if (remaining > 0) counts.set(key, remaining);
			else counts?.delete(key);
			if (counts?.size === 0) { localCounts.delete(topic); activeTopics.delete(topic); }
		}
		const removeMembership = !localCounts.get(topic)?.has(key);
		const topicData = localData.get(topic);
		if (removeMembership && topicData) {
			topicData.delete(key);
			if (topicData.size === 0) localData.delete(topic);
		}
		const cached = topicData?.get(key);
		const cachedIdentity = cached?.data;
		const cachedLegacy = cached?.legacyData;
		// A read-only duplicate can outlive the last committed local tab.
		// Resolve its inherited membership only on failure, then recheck local
		// reservations before conditionally removing that exact Redis token.
		if (!write && removeMembership) {
			const token = await redis.hget(userHashKey(topic, key), instanceId).catch(() => null);
			if (token && !localCounts.get(topic)?.has(key)) write = { token };
		}
		if (write) {
			const result = await evalCached(redis, ROLLBACK_JOIN_SCRIPT, 3,
				userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic), instanceId, key,
				write.token, removeMembership ? '1' : '0', presenceTtlMs
			).catch(() => null);
			if (result?.[0]) {
				const restored = parseEntries({ [key]: result[0] }, fieldNames.fromServer).get(key);
				const current = localData.get(topic)?.get(key);
				// A later relay or local join owns any cache it changed while the
				// compensation was in flight. Never replace that newer identity.
				if (restored && current && current === cached && current.data === cachedIdentity && current.legacyData === cachedLegacy && !deepEqual(current.data, restored.data)) {
					setLocalData(localData.get(topic), key, restored.data);
					localData.get(topic).get(key).remoteIdentity = true;
				}
				if (restored && result[2] === 1) {
					mLeaves?.inc({ topic: mt(topic) });
					const data = publicData(restored);
					bufferDiff(topic, 'leave', key, data, platform);
					await publishEvent(topic, INTERNAL_EVENTS.LEAVE, { key, data });
				} else if (restored && result[1] === 1) {
					await publishEvent(topic, INTERNAL_EVENTS.UPDATED, identityPayload(key, restored));
				}
			}
		}
		if (!localCounts.has(topic) && !syncCounts.has(topic)) await unsubscribeFromTopic(topic);
	}

	async function restoreIdentity(topic, key, newest, cached, platform) {
		const expected = cached.data;
		const legacy = cached.legacyData;
		const authority = cached.authorityToken;
		cached.fieldRevisions ||= newFieldMap();
		const revisions = { ...cached.fieldRevisions };
		try {
			const result = await evalCached(redis, RESTORE_IDENTITY_SCRIPT, 3,
				userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic),
				instanceId, key, JSON.stringify(expected), JSON.stringify(newest), now(), presenceTtlMs, authority || '', instanceId + ':' + (++joinSequence)
			);
			const topicData = localData.get(topic);
			const current = topicData?.get(key);
			if (!result?.[0] || current !== cached || current.data !== expected || current.legacyData !== legacy || current.authorityToken !== authority) return;
			const restored = parseEntries({ [key]: result[0] }, fieldNames.fromServer).get(key);
			if (!restored) return;
			setLocalData(topicData, key, restored.data);
			const entry = topicData.get(key);
			entry.authorityToken = result[1];
			if (restored.fields) {
				entry.fields ||= newFieldMap();
				for (const name of Object.keys(restored.fields)) {
					if (entry.fieldRevisions[name] === revisions[name]) {
						entry.fields[name] = restored.fields[name];
						entry.fieldRevisions[name] = (entry.fieldRevisions[name] || 0) + 1;
					}
				}
			}
			bufferDiff(topic, 'join', key, publicData(entry), platform);
			await publishEvent(topic, INTERNAL_EVENTS.UPDATED, identityPayload(key, entry));
		} catch { /* leave remains local if Redis is unavailable */ }
	}

	async function leaveTopic(ws, platform, topic) {
		const connTopics = wsTopics.get(ws);
		// Peel one role at a time: a participant leave removes ONLY the participant
		// role (a co-resident sync-observer of the same topic survives, so its
		// roster does not freeze); an observer-only leave removes the observer role.
		const wasParticipant = !!(connTopics && connTopics.has(topic));
		if (wasParticipant) {
			const { key, data } = connTopics.get(topic);
			connTopics.delete(topic);
			if (connTopics.size === 0) wsTopics.delete(ws);
			indexRemove(topic, key, ws);

			// Release the wire subscription only if this socket is not ALSO a
			// sync-observer of the topic. A participant leaving must not evict a
			// co-resident observer role (whose roster would then freeze); the
			// observer is released on its own __presence: unsubscribe path or on
			// socket close. The Redis topic-level subscription is separately
			// refcounted below via syncCounts.
			if (!syncObservers.get(ws)?.has(topic)) {
				try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				removeWsSubscription(ws, '__presence:' + topic);
			}

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
							userGone = await evalCached(redis,
								LEAVE_SCRIPT, 3,
								userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic),
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
						if (newest && cached && !cached.remoteIdentity && !deepEqual(newest, cached.data)) {
							await restoreIdentity(topic, key, newest, cached, platform);
						}
					}
				}
			}
		}

		// Observer-only leave: the socket holds no participant role on this topic,
		// so remove its sync-observer role and release the wire (no participant
		// remains to keep it). Guarded by !wasParticipant so a participant leave
		// does NOT fall through and tear down a co-resident observer - the reported
		// roster-freeze bug. This per-topic observer teardown is what the public
		// leave(ws, topic) and the __presence: unsubscribe hook rely on.
		if (!wasParticipant) {
			const syncTopics = syncObservers.get(ws);
			if (syncTopics && syncTopics.has(topic)) {
				syncTopics.delete(topic);
				if (syncTopics.size === 0) syncObservers.delete(ws);

				try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				removeWsSubscription(ws, '__presence:' + topic);

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
				removeWsSubscription(ws, '__presence:' + topic);
			}
		}
		if (syncTopics) {
			for (const topic of syncTopics) {
				try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
				removeWsSubscription(ws, '__presence:' + topic);
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
						if (newest && cached && !cached.remoteIdentity && !deepEqual(newest, cached.data)) {
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
			pendingUpdatedRelays.push(restoreIdentity(topic, key, newest, cached, platform));
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
				'eval', LEAVE_SCRIPT, 3,
				userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic),
				instanceId, key
			]);
		}

		let results;
		let skipPipeline = false;
		if (b) { try { b.guard(); } catch { skipPipeline = true; } }
		if (!skipPipeline) {
			try {
				results = await execMultiSlot(redis, commands);
				const error = firstCommandError(results);
				if (error) b?.failure(error);
				else b?.success();
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
	function throwWsClosed(topic, cancelAdmission) {
		cancelAdmission();
		mJoinsAborted?.inc({ topic: mt(topic), reason: 'ws_closed' });
		throw new WsClosedError('presence.join', topic);
	}

	// Per-instance consistency auditor: a slow, unref'd, seam-jittered background
	// check that this instance's local member-count map and local reverse index
	// track the same distinct-user set per topic. Both are mutated in the same
	// synchronous frame on join, leave, and rollback, so a mismatch is a genuine
	// bookkeeping divergence (the local DATA map is deliberately NOT compared - a
	// join defers its data commit past the count increment, so count > data is a
	// legitimate in-flight transient, not a divergence). It NEVER runs on the hot
	// path (join / leave / sync / heartbeat pay nothing); the only cost is the
	// bookkeeping they already do. Default on (5000ms); set
	// `consistencyAuditIntervalMs: 0` to disable entirely (no timer scheduled). A
	// desync logs + increments the assertion counter (the soft tier); only one
	// that PERSISTS across two consecutive audits of the same window escalates to
	// the hard tier (a deferred process restart), so a healthy or transient state
	// is never killed. At scale, when an instance tracks more topics than the
	// per-tick window, the round-robin window rotates and the persistence gate only
	// completes once the whole population fits one window - escalation is strictly
	// harder at scale, never a false kill.
	const consistencyAuditIntervalMs = Number.isFinite(options.consistencyAuditIntervalMs)
		? options.consistencyAuditIntervalMs
		: 5000;
	let consistencyAuditor = null;
	if (consistencyAuditIntervalMs > 0) {
		consistencyAuditor = createConsistencyAuditor({
			snapshot: ({ offset, limit }) => buildPresenceAuditSnapshot({ localCounts, topicKeyCount, offset, limit }),
			assert,
			fatal,
			predicates: [checkRedisPresenceLocalIndex],
			hardCategories: ['redis.presence.local-index-desync'],
			intervalMs: consistencyAuditIntervalMs
		});
		consistencyAuditor.start();
	}

	/** @type {RedisPresenceTracker} */
	const tracker = {
		async join(ws, topic, platform) {
			if (typeof topic !== 'string') throw new TypeError('Topic must be a string');
			if (!isFramableTopic(topic)) throw new TypeError('Topic must not contain quotes, backslashes, or control characters');
			if (topic.startsWith('__')) return;

			let connTopics = wsTopics.get(ws);
			if (connTopics && connTopics.has(topic)) return;
			if (connTopics && connTopics.size >= maxTopicsPerConnection) return;

			const selected = select(ws.getUserData());
			// A roster entry is an object; anything else would be stored as the
			// user's data and skipped by every read-back. Thrown before any state
			// is staged and before the breaker guard, as the in-memory plugin does.
			if (!selected || typeof selected !== 'object') {
				throw new TypeError(`redis presence: select() must return a plain object, got ${selected === null ? 'null' : typeof selected}`);
			}
			// The relayed join and the read-back from Redis both drop these
			// names, so the local join does too: otherwise this instance's peers
			// would see an entry that every other instance's peers do not.
			const data = withoutReservedNames(selected);
			// The key is part of every roster object's wire shape, so it must be
			// resolved from the selected data rather than the raw connection data.
			// A default-dropped email/session/ip therefore falls back to __conn:N
			// instead of leaking as the roster property name. An explicit select is
			// the deliberate opt-in path for a non-secret custom key.
			const key = resolveKey(data);
			let serializedData;
			try { serializedData = JSON.stringify(data); } catch {
				throw new Error('redis presence: select() must return JSON-serializable data');
			}

			// Reserve this connection independently of concurrent joins.
			const existingCounts = localCounts.get(topic);
			const prevCount = existingCounts ? (existingCounts.get(key) || 0) : 0;
			let joinWrite = null;

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
			const reservation = { key, data, committed: false };
			connTopics.set(topic, reservation);
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

			let cancelAdmission;
			try {
				cancelAdmission = guardBreaker(b);
			} catch (err) {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throw err;
			}

			try {
				// Redis 7.4+ feature gate (HEXPIRE). Probe once, cache. Throwing
				// here treats version mismatch as a join failure - same shape
				// as any other startup-time misconfiguration.
				await ensureRedis74();
			} catch (err) {
				b?.failure(err);
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throw err;
			}

			try {
				await subscribeToTopic(topic, platform);
			} catch (err) {
				b?.failure(err);
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throw err;
			}

			// ws closed during `await subscribeToTopic`. The close hook already
			// ran leaveAll, which swept localCounts / wsTopics for this ws;
			// no compensating undoJoin needed. Throw so the caller sees the
			// abort instead of a silent success.
			if (!wsTopics.has(ws)) throwWsClosed(topic, cancelAdmission);

			try { ws.getBufferedAmount(); } catch {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throwWsClosed(topic, cancelAdmission);
			}

			let isNewUser = false;
			let identityChanged = false;
			let authorityToken;
			let reservedIdentity = false;
			const currentEntry = localData.get(topic)?.get(key);

			if (prevCount === 0) {
				const ts = now();
				joinWrite = { token: instanceId + ':' + (++joinSequence) };
				const value = storedValue(serializedData, ts);
				try {
					const joinResult = await evalCached(redis,
						JOIN_SCRIPT, 3,
						userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic),
						instanceId, key, value, ts, presenceTtlMs, joinWrite.token
					);
					joinWrite.version = joinResult[3];
					joinWrite.epoch = joinResult[4];
					joinWrite.fallback = !joinResult[2] && joinResult[5] === 1;
					isNewUser = joinResult[0] === 1;
					reservedIdentity = joinResult[2] === 1;
					if (!isNewUser && joinResult[2] === 1) {
						let previous;
						try { previous = JSON.parse(joinResult[1]); } catch { /* replace malformed data */ }
						identityChanged = !previous || !deepEqual(previous.data, data);
					}
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, reservation, joinWrite, platform);
					throw err;
				}

				if (!wsTopics.has(ws)) {
					// ws closed during the eval. Roll back our Redis write so
					// the per-user hash entry does not linger past TTL, then
					// surface the abort to the caller.
					await undoJoin(ws, topic, key, reservation, joinWrite, platform);
					throwWsClosed(topic, cancelAdmission);
				}
			} else if (!currentEntry || currentEntry?.legacyData || (currentEntry && currentEntry.serializedIdentity !== serializedData && !deepEqual(currentEntry.data, data))) {
				// Defer cache changes and notifications until the authoritative
				// read-back commits, just as on a first local join.
				try {
					const ts = now();
					joinWrite = { token: instanceId + ':' + (++joinSequence) };
					const joinResult = await evalCached(redis,
						JOIN_SCRIPT, 3,
						userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic),
						instanceId, key, storedValue(serializedData, ts), ts, presenceTtlMs, joinWrite.token
					);
					joinWrite.version = joinResult[3];
					joinWrite.epoch = joinResult[4];
					joinWrite.fallback = !joinResult[2] && joinResult[5] === 1;
					reservedIdentity = joinResult[2] === 1;
					identityChanged = reservedIdentity;
					if (reservedIdentity && currentEntry?.legacyData) {
						let previous;
						try { previous = JSON.parse(joinResult[1]); } catch { /* replace malformed data */ }
						identityChanged = !previous || !deepEqual(previous.data, data);
					}
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, reservation, joinWrite, platform);
					throw err;
				}
			}

			let all;
			try {
				all = await coalesceHgetall(topic);
			} catch (err) {
				b?.failure(err);
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throw err;
			}

			// Subscribe ws to presence channel (may have closed during async gap)
			try {
				ws.subscribe('__presence:' + topic);
			} catch {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throwWsClosed(topic, cancelAdmission);
			}
			// Mirror into the subscription registry so the adapter's binary
			// publish walk delivers to this member (native membership alone is
			// invisible to it).
			addWsSubscription(ws, '__presence:' + topic);

			// If ws closed after subscribe, leave() already handled
			// local cleanup and leave events. Just clean the Redis state.
			if (!wsTopics.has(ws)) {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throwWsClosed(topic, cancelAdmission);
			}

			if (reservedIdentity || joinWrite?.fallback) {
				try {
					const finalize = () => evalCached(redis, FINALIZE_JOIN_SCRIPT, 3,
						userHashKey(topic, key), topicHashKey(topic), pendingHashKey(topic), instanceId, key,
						joinWrite.token, joinWrite.epoch, joinWrite.version, serializedData, now(), presenceTtlMs);
					// Finalization is idempotent. A lost reply must be reconciled
					// before compensating; rollback never undoes a committed value.
					let current;
					try { current = await finalize(); } catch { current = await finalize(); }
					if (!current?.[0]) throw new Error('presence join expired before identity commit');
					isNewUser = current[1] === 1;
					authorityToken = current[2];
					all = { ...all, [key]: current[0] };
				} catch (err) {
					b?.failure(err);
					await undoJoin(ws, topic, key, reservation, joinWrite, platform);
					throw err;
				}
			}
			if (wsTopics.get(ws)?.get(topic) !== reservation) {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throwWsClosed(topic, cancelAdmission);
			}
			try { ws.getBufferedAmount(); } catch {
				await undoJoin(ws, topic, key, reservation, joinWrite, platform);
				throwWsClosed(topic, cancelAdmission);
			}
			b?.success();

			// Commit localData and activeTopics now that the join is
			// fully committed. The heartbeat reads from these, so they
			// must not be visible during any of the async gaps above.
			let topicData = localData.get(topic);
			if (!topicData) {
				topicData = new Map();
				localData.set(topic, topicData);
			}
			const entries = parseEntries(all, fieldNames.fromServer);
			const stored = entries.get(key);
			const matchesSelection = !stored || deepEqual(stored.data, data);
			let committed = topicData.get(key);
			if (!matchesSelection || committed?.serializedIdentity !== serializedData) {
				setLocalData(topicData, key, stored ? stored.data : data);
				committed = topicData.get(key);
			}
			if (authorityToken !== undefined) committed.authorityToken = authorityToken;
			if (matchesSelection) {
				// join already encoded this selection for its eager JSON check.
				// Reuse it for the next local join's identity equality guard.
				committed.serializedIdentity = serializedData;
				if (reservedIdentity) {
					committed.remoteIdentity = false;
					delete committed.legacyData;
				}
			} else {
				committed.remoteIdentity = true;
			}
			if (stored?.fields) {
				const committed = topicData.get(key);
				committed.fields = Object.assign(newFieldMap(), committed.fields, stored.fields);
				if (committed.fieldRevisions) for (const name of Object.keys(stored.fields)) committed.fieldRevisions[name] = (committed.fieldRevisions[name] || 0) + 1;
				committed.fieldsBytes = undefined;
			}
			reservation.committed = true;
			activeTopics.add(topic);

			// Buffer join only after the operation is fully committed.
			// Prevents orphaned diffs when snapshot or subscribe fails - the
			// compensating leave inside undoJoin would collapse with this
			// join in the buffer anyway, but skipping the buffer entirely
			// keeps the cross-instance pubsub clean.
			if (isNewUser) {
				mJoins?.inc({ topic: mt(topic) });
				bufferDiff(topic, 'join', key, data, platform);
				await publishEvent(topic, INTERNAL_EVENTS.JOIN, identityPayload(key, topicData.get(key)));
			} else if (identityChanged) {
				const current = publicData(topicData.get(key));
				bufferDiff(topic, 'join', key, current, platform);
				await publishEvent(topic, INTERNAL_EVENTS.UPDATED, identityPayload(key, topicData.get(key)));
			}

			// Send current snapshot to this connection. Flat `{[key]: data}`
			// shape mirrors the adapter's bundled presence plugin so a single
			// client decoder handles both implementations.
			/** @type {Record<string, Record<string, any>>} */
			const state = Object.create(null);
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

		/**
		 * Emit the current roster for `topic` to one socket.
		 * @returns {Promise<string | undefined>} The denial reason when the
		 * platform refuses the topic, so a wire-subscribe caller can deny the
		 * subscribe itself instead of only withholding the roster. Existing
		 * callers that ignore the return value are unaffected.
		 */
		async sync(ws, topic, platform) {
			// Presence tracks no `__` topic (join and update skip them), so
			// there is no roster to send and no event channel to open for one.
			// Before the breaker guard, which would otherwise spend a probe.
			if (typeof topic !== 'string' || topic.startsWith('__')) return;
			if (!isFramableTopic(topic)) return;
			// Authorize against the REAL topic before granting tap-channel
			// membership (mirrors in-memory presence sync + the cursor snapshot):
			// the presence-snapshot message is otherwise an un-authorized path to
			// join __presence:{topic} and read its roster, around the wire-level
			// `__`-subscribe block. Gate it on the same check a wire-subscribe to
			// `topic` would run, before even opening the cross-instance Redis
			// subscription. Fail CLOSED when the platform cannot authorize
			// entirely: the adapter peer floor has provided checkSubscribe for
			// many releases, so an absent check means a custom platform.
			// Degrade availability, never authorization.
			if (!platform || typeof platform.checkSubscribe !== 'function') return 'FORBIDDEN';
			{
				let denial;
				// OBSERVER lane: this asks "may this connection SEE the roster
				// it is requesting?", not "may it be granted the topic".
				// Without the flag, a pure-grant deployment - wire-subscribe
				// authorization armed and no app subscribe hook - has no hook
				// for the gate to consult, so `checkSubscribe` allows every
				// topic name and this lane hands any connected socket any
				// room's roster. The peer floor implements the flag and its own
				// bundled presence sync asks the same question, so this gate is
				// load-bearing across the supported adapter range.
				try { denial = await platform.checkSubscribe(ws, topic, OBSERVER_LANE); } catch { return 'FORBIDDEN'; }
				// `false` is a denial in the adapter's subscribe-hook
				// vocabulary; reading it as allow would invert the check on
				// the custom platforms this branch exists for.
				if (denial === false) return 'FORBIDDEN';
				if (denial) return denial;
			}
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
			const entries = parseEntries(all, fieldNames.fromServer);
			/** @type {Record<string, Record<string, any>>} */
			const state = Object.create(null);
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
				addWsSubscription(ws, presenceTopic);
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

		async update(ws, topic, fields, platform, fromClient = false) {
			if (topic.startsWith('__')) return;
			if (!fields || typeof fields !== 'object' || Array.isArray(fields)) return;
			// A client frame is cut down to the listed names before anything reads
			// a value. With no allowlist it returns without touching the fields at
			// all, so an unlisted getter or toJSON never runs.
			let effective = fields;
			if (fromClient) {
				if (!fieldNames.listsClientFields) return;
				effective = newFieldMap();
				let accepted = false;
				for (const k of Object.keys(fields)) {
					if (!fieldNames.fromClient(k)) continue;
					effective[k] = fields[k];
					accepted = true;
				}
				if (!accepted) return;
			}
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
			try {
				const encoded = JSON.stringify(effective);
				if (encoded.length > safeFieldsChars && Buffer.byteLength(encoded) > maxFieldsBytes) return;
			} catch { return; }
			if (!entry.fields) entry.fields = newFieldMap();
			// Per-field change detection against this instance's field view. Only
			// fields whose value actually changed are merged and broadcast (the
			// field-level delta). Durable and transient changes are split: durable
			// is persisted to Redis so a cross-instance state read includes it;
			// transient is relay-only and never persisted.
			/** @type {Record<string, any> | null} */
			let changedDurable = null;
			/** @type {Record<string, any> | null} */
			let changedTransient = null;
			let totalBytes = entry.fieldsBytes;
			if (totalBytes === undefined) {
				totalBytes = 0;
				try {
					for (const k of Object.keys(entry.fields)) {
						totalBytes += Buffer.byteLength(JSON.stringify(k)) + 2 + fieldValueBytes(entry.fields[k]);
					}
				} catch { return; }
			}
			let any = false;
			let nested = false;
			try {
				for (const k of Object.keys(effective)) {
					const v = effective[k];
					if (v !== null && typeof v === 'object') nested = true;
					// The server rule: the allowlist when there is one, otherwise no
					// identity and no credential, personal-data or transport name. A
					// client frame has already been cut down to listed names, which
					// this passes. The relay and the read-back apply the same rule.
					if (!fieldNames.fromServer(k)) {
						if (warnReservedField) warnReservedField(k);
						continue;
					}
					if (!deepEqual(entry.fields[k], v)) {
						totalBytes += fieldValueBytes(v);
						if (Object.prototype.hasOwnProperty.call(entry.fields, k)) totalBytes -= fieldValueBytes(entry.fields[k]);
						else totalBytes += Buffer.byteLength(JSON.stringify(k)) + 2;
						if (transientFields.has(k)) (changedTransient ??= newFieldMap())[k] = v;
						else (changedDurable ??= newFieldMap())[k] = v;
						any = true;
					}
				}
			} catch { return; }
			if (!any || (nested && exceedsDepth(effective, MAX_PROJECTION_DEPTH))) return;
			const changed = changedDurable && changedTransient ? { ...changedDurable, ...changedTransient } : (changedDurable || changedTransient);

			if (totalBytes > maxTotalFieldsBytes) return;
			for (const k of Object.keys(changed)) {
				entry.fields[k] = changed[k];
				if (entry.fieldRevisions) entry.fieldRevisions[k] = (entry.fieldRevisions[k] || 0) + 1;
			}
			entry.fieldsBytes = totalBytes;

			// Local fan-out: buffer the update diff (durable + transient together)
			// for this instance's subscribers. Coalesces with same-tick ops per
			// the bufferUpdate collapse rules.
			bufferUpdate(topic, key, changed, platform);

			// Persist durable fields to the per-topic hash value so a cross-instance
			// state read (HGETALL) reconstructs them. Best-effort under the breaker:
			// the field still relays + buffers locally if the write is skipped, and
			// a later durable update reconciles the cross-instance read.
			if (changedDurable) {
				let skip = false;
				if (b) { try { b.guard(); } catch { skip = true; } }
				if (!skip) {
					try {
						const ts = now();
						await evalCached(redis,
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
			await publishEvent(topic, INTERNAL_EVENTS.FIELDS, { key, durable: changedDurable || EMPTY_FIELDS, transient: changedTransient || EMPTY_FIELDS });
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
			// publicData(), not entry.data: the identity plus the durable update()
			// fields, minus transient - the value the `state` snapshot sends, so an
			// SSR render and the client's first snapshot agree. The read-back is the
			// snapshot's, with the same field rule. Every call parses the hash
			// afresh, so nothing returned is shared; only an entry rebuilt with a
			// null prototype is copied, so the caller always gets ordinary objects.
			for (const [, entry] of parseEntries(all, fieldNames.fromServer)) {
				const data = publicData(entry);
				result.push(Object.getPrototypeOf(data) === null ? { ...data } : data);
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

		/**
		 * Live state of the mass-disconnect self-preservation guard: whether
		 * the hold is active, since when (epoch ms), and how many dead
		 * sockets it is currently holding from eviction. Always inactive when
		 * the guard is disabled. Synchronous - safe to poll from a health
		 * endpoint or render as a degraded banner source.
		 */
		selfPreservation() {
			return {
				enabled: selfPreservation !== null,
				active: spActive,
				since: spActive ? spSince : null,
				held: spHeld.size
			};
		},

		flushDiffs() {
			flushPendingDiffs();
		},

		/**
		 * Right-to-erasure (`live.forget`): remove a user from every presence topic
		 * across the cluster. Scans `presence:user:{*}:{userId}` (the per-user hash
		 * carries the userKey OUTSIDE the {topic} tag), and for each topic DELetes
		 * the whole per-user hash (every instance's entry, not just this one) +
		 * HDELs the topic-hash field + publishes a leave so subscribers drop the
		 * roster entry. The two keys share the {topic} slot, so the DEL+HDEL is one
		 * execMultiSlot round trip per topic.
		 *
		 * Matches when the configured `keyField` makes the presence key equal the
		 * userId (the default `id` case); a custom key that is not the userId is
		 * not addressable here. A tenant matches only `@t/<tenantId>/` topics;
		 * null matches only topics outside the tenant namespace.
		 * An actively-present forgotten user on a PEER instance may keep a stale
		 * in-memory roster entry until they disconnect; the durable Redis state is
		 * erased and a leave is broadcast.
		 *
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} topics the user was removed from
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			// The user id is interpolated into the SCAN MATCH glob below, so a
			// metacharacter in it WIDENS the scan instead of narrowing it:
			// userId '*' matches every user in every topic. The exact
			// startsWith/endsWith re-check further down keeps the deletes
			// correct, so this is scan amplification rather than wrong-row
			// erasure - but an unbounded SCAN over a large keyspace is its own
			// problem, and this is the last sibling of the `clear()` pair that
			// carries the same guard. It throws rather than returning 0 because
			// the composite forget store settles each leg independently: a
			// refusal reaches the operator, where a 0 reads as "nothing to
			// erase" on a right-to-erasure path.
			if (/[\0*?[\]\\]/.test(userId)) {
				throw new Error('redis presence: purgeUser user id must not contain NUL or glob metacharacters (* ? [ ] \\)');
			}
			const prefix = client.key('presence:user:{');
			const pendingPrefix = client.key('presence:pending:{');
			const suffix = '}:' + userId;
			let keys;
			try {
				const pendingKeys = await scanKeys(redis, pendingPrefix + '*}');
				keys = await scanKeys(redis, prefix + '*' + suffix);
				const seen = new Set(keys);
				for (const pending of pendingKeys) {
					if (!pending.startsWith(pendingPrefix) || !pending.endsWith('}')) continue;
					const topic = pending.slice(pendingPrefix.length, -1);
					if (!topicInTenant(tenantId, topic) || !(await redis.hget(pending, userId))) continue;
					const member = userHashKey(topic, userId);
					if (!seen.has(member)) { seen.add(member); keys.push(member); }
				}
			} catch { return 0; }
			let n = 0;
			for (const k of keys) {
				if (!k.startsWith(prefix) || !k.endsWith(suffix)) continue;
				const topic = k.slice(prefix.length, k.length - suffix.length);
				if (!topicInTenant(tenantId, topic)) continue;
				try {
					// A refused DEL or HDEL resolves as a per-command error; the user's
					// data is still there, so the topic is not counted as erased.
					const err = firstCommandError(await execMultiSlot(redis, [
						['del', userHashKey(topic, userId)],
						['hdel', topicHashKey(topic), userId],
						['hdel', pendingHashKey(topic), userId]
					]));
					if (err) throw err;
					b?.success();
				} catch (err) {
					b?.failure(err);
					continue;
				}
				// Drop local-instance state for the user on this topic.
				const counts = localCounts.get(topic);
				if (counts) { counts.delete(userId); if (counts.size === 0) { localCounts.delete(topic); activeTopics.delete(topic); } }
				const td = localData.get(topic);
				if (td) { td.delete(userId); if (td.size === 0) localData.delete(topic); }
				// syncCounts is a topic-level OBSERVER refcount (topic -> number),
				// not per-user state - purging a user must leave it untouched.
				try { await publishEvent(topic, INTERNAL_EVENTS.LEAVE, { key: userId, data: null }); } catch { /* leave broadcast best-effort */ }
				n++;
			}
			return n;
		},

		async clear() {
			await withBreaker(b, () => scanAndUnlink(redis, client.key('presence:*')));

			for (const [ws, connTopics] of wsTopics) {
				for (const topic of connTopics.keys()) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
					removeWsSubscription(ws, '__presence:' + topic);
				}
			}
			for (const [ws, topics] of syncObservers) {
				for (const topic of topics) {
					try { ws.unsubscribe('__presence:' + topic); } catch { /* closed */ }
					removeWsSubscription(ws, '__presence:' + topic);
				}
			}

			await subscriberCtx.unsubscribeAllChannels();

			wsTopics.clear();
			localCounts.clear();
			localData.clear();
			clearIndex();
			activeTopics.clear();
			syncObservers.clear();
			syncCounts.clear();
			disposeDiffBuffer();
			connCounter = 0;
			spHeld.clear();
			spActive = false;
		},

		destroy() {
			stopOnlineMetric?.();
			heartbeatTick.stop();
			clearIntervalTimer(heartbeatTimer);
			if (consistencyAuditor) consistencyAuditor.stop();
			subscriberCtx.dispose();
			disposeDiffBuffer();
		},

		// Internal: the per-instance consistency auditor (null when disabled via
		// `consistencyAuditIntervalMs: 0`). Exposed for tests to drive a single
		// audit pass deterministically against live state; not part of the public
		// contract.
		_consistencyAuditor: consistencyAuditor,

		hooks: {
			async subscribe(ws, topic, { platform }) {
				if (topic.startsWith('__presence:')) {
					const realTopic = topic.slice('__presence:'.length);
					// Return the denial: the adapter treats anything that is
					// not `false` or a string as ALLOW, so swallowing it would
					// withhold the roster while still subscribing the socket
					// to the tap channel and feeding it every later diff.
					return await tracker.sync(ws, realTopic, platform);
				}
				await tracker.join(ws, topic, platform);
			},
			message(ws, { data, msg, platform }) {
				let env = msg && typeof msg === 'object' ? msg : null;
				if (!env && data && typeof data === 'object' && !(data instanceof ArrayBuffer) && !ArrayBuffer.isView(data)) env = data;
				if (!env) {
					try { env = JSON.parse(new TextDecoder().decode(data)); } catch { return; }
				}
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
				if (env && env.type === 'presence-snapshot' && typeof env.topic === 'string') {
					tracker.sync(ws, env.topic, platform).catch(() => { /* surfaced via breaker */ });
					return true;
				}
				if (env && env.type === 'presence-update' && typeof env.topic === 'string' && env.fields && typeof env.fields === 'object') {
					// Client-pushed field update (mirrors the in-memory presence
					// plugin). `update` takes only the names clientUpdateFields lists,
					// self-gates on membership and field shape, then splits
					// durable/transient and relays the change cross-instance.
					tracker.update(ws, env.topic, env.fields, platform, true).catch(() => { /* surfaced via breaker */ });
					return true;
				}
			},
			async unsubscribe(ws, topic, { platform }) {
				if (topic.startsWith('__presence:')) {
					const realTopic = topic.slice('__presence:'.length);
					const syncTopics = syncObservers.get(ws);
					if (syncTopics && syncTopics.has(realTopic)) {
						syncTopics.delete(realTopic);
						if (syncTopics.size === 0) syncObservers.delete(ws);

						// Release the wire subscription only if this socket is not
						// also a participant of the topic (symmetric to leaveTopic's
						// participant guard), so an observer leaving does not evict a
						// co-resident participant role.
						if (!wsTopics.get(ws)?.has(realTopic)) {
							try { ws.unsubscribe(topic); } catch { /* closed */ }
							removeWsSubscription(ws, topic);
						}

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
