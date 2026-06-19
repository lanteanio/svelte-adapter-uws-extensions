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
import { addWsSubscription, removeWsSubscription } from '../shared/ws-subscriptions.js';
import { createPresenceWireCodec } from 'svelte-adapter-uws/plugins/presence';
import { JOIN_SCRIPT, LEAVE_SCRIPT, UPDATE_SCRIPT, INTERNAL_EVENTS } from './presence/lua.js';
import { makeKeys } from './presence/keys.js';
import { deepEqual, parseEntries, setLocalData, makePublicData } from './presence/data.js';
import { createLocalIndex } from './presence/local-index.js';
import { createDiffBuffer } from './presence/diff-buffer.js';
import { createSubscriber } from './presence/subscriber.js';
import { createPresenceState } from './presence/state.js';
import { buildPresenceAuditSnapshot } from './presence/audit-snapshot.js';
import { assert, fatal } from '../shared/assert.js';
import { createConsistencyAuditor } from '../shared/auditor.js';
import { checkRedisPresenceLocalIndex } from '../shared/invariants.js';

export { WsClosedError };

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
	const ctx = createPresenceState(client, options);
	const {
		keyField, select, heartbeatInterval, presenceTtlMs, transientFields, publicData,
		emit, emitTo, instanceId, redis, keyspaceNotifications, ensureRedis74, b, mt,
		mJoins, mJoinsAborted, mLeaves, mHeartbeats, mTotalOnline, mHeartbeatLatency,
		mKeyspaceCleanups, mDiffFrames, mDiffCoalesced, warnSensitive, wsTopics, localCounts,
		localData, syncObservers, syncCounts, topicHashKey, userHashKey, eventChannel,
		coalesceHgetall
	} = ctx;

	let lastHeartbeatLatency = 0;
	let staleCleanedTotal = 0;
	let connCounter = 0;

	function resolveKey(data) {
		if (data && keyField in data && data[keyField] != null) {
			return String(data[keyField]);
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
		mt,
		mDiffCoalesced,
		mDiffFrames
	});

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
		if (subscriberCtx.activePlatform) {
			const dead = [];
			for (const [ws] of wsTopics) {
				try { ws.getBufferedAmount(); } catch { dead.push(ws); }
			}
			for (const ws of dead) {
				// Full leave (sync Step 1 + async Step 2 fire-and-forget)
				tracker.leave(ws, subscriberCtx.activePlatform).catch(() => {});
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
					const dataMap = {};
					for (const [userKey, entry] of data) dataMap[userKey] = publicData(entry);
					emit('__presence:' + topic, 'heartbeat', dataMap, subscriberCtx.activePlatform);
				}
			}
		}
		execMultiSlot(redis, commands).catch(() => {});
		lastHeartbeatLatency = monotonicNow() - tickStart;
		mHeartbeatLatency?.set(lastHeartbeatLatency);
	}, heartbeatInterval);
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Cross-instance subscriber: receives peers' join/leave/update events on the
	// per-topic channels, routes them into the diff buffer, and forwards topic-key
	// expiry as an empty state. Owns the duplicate connection, the channel set, the
	// idle timer, and the active platform. Lives in subscriber.js.
	const subscriberCtx = createSubscriber({
		client,
		instanceId,
		keyspaceNotifications,
		bufferDiff,
		bufferUpdate,
		localData,
		emit,
		eventChannel,
		mKeyspaceCleanups
	});
	const { subscribeToTopic, unsubscribeFromTopic } = subscriberCtx;

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
			snapshot: ({ offset, limit }) => buildPresenceAuditSnapshot({ localCounts, topicKeyCount, mt, offset, limit }),
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
			// Mirror into the subscription registry so the adapter's binary
			// publish walk delivers to this member (native membership alone is
			// invisible to it).
			addWsSubscription(ws, '__presence:' + topic);

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
			// Authorize against the REAL topic before granting tap-channel
			// membership (mirrors in-memory presence sync + the cursor snapshot):
			// the presence-snapshot message is otherwise an un-authorized path to
			// join __presence:{topic} and read its roster, around the wire-level
			// `__`-subscribe block. Gate it on the same check a wire-subscribe to
			// `topic` would run, before even opening the cross-instance Redis
			// subscription. Optional-chained (checkSubscribe was added to the
			// platform later); the snapshot is low-frequency so the await is fine.
			if (platform && typeof platform.checkSubscribe === 'function') {
				let denial;
				try { denial = await platform.checkSubscribe(ws, topic); } catch { return; }
				if (denial) return;
			}
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
		},

		destroy() {
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
