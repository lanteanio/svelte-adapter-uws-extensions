/**
 * Cross-instance subscriber for the Redis-backed presence tracker.
 *
 * Owns the duplicate Redis connection that receives peer instances' join/leave/
 * update events on the per-topic event channels, routes them into the local diff
 * buffer for client fan-out, and (when keyspace notifications are on) forwards a
 * whole-topic key expiry as an empty `state`. Also owns the per-topic channel
 * subscription set, the idle-shutdown timer, and the active platform (the most
 * recent platform to subscribe - the heartbeat and the event router both
 * broadcast through it). The event-channel pub/sub uses an ordinary
 * client.duplicate() subscriber (regular PUBLISH propagates cluster-wide, so one
 * connection sees every instance's events). The keyspace `del` cleanup uses a
 * single psubscribe on that connection on standalone, but ONE subscriber per
 * master on a cluster: keyspace events are node-local (a node fires `del` only
 * for keys it owns and does not propagate it across the cluster bus), so a
 * single Cluster.psubscribe would miss dels on every other master. NO ssubscribe
 * here - the sharded pub/sub per-master pattern lives in sharded-pubsub.js.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/subscriber
 */

import { setTimer, clearTimer, monotonicNow } from '../../shared/runtime.js';
import { isCluster } from '../../shared/cluster.js';
import { createBusValidator } from '../../shared/bus-validate.js';
import { mergeFields, newFieldMap } from './field-policy.js';
import { INTERNAL_EVENTS } from './lua.js';
import { EXTENSIONS_ERROR_IDS } from '../../error-registry.js';
import { emitRegisteredFailure } from '../../shared/diagnostic.js';
import { openSubscriber, watchLiveness, closeConnection, isReplyError } from '../../shared/redis-subscriber.js';

// A sweep that left a master without its keyspace subscription runs again
// after a delay that doubles from the first value up to the cap.
const KEYSPACE_RETRY_FIRST_MS = 1000;
const KEYSPACE_RETRY_MAX_MS = 30000;

// A master that does not answer CLUSTER MYID or the PSUBSCRIBE within this
// long is missed by the sweep, so one that stopped answering without closing
// its connections cannot hold up every later sweep.
const KEYSPACE_STEP_TIMEOUT_MS = 10000;

/**
 * `reply`, or a rejection once KEYSPACE_STEP_TIMEOUT_MS passed without it.
 * @template T
 * @param {Promise<T>} reply
 * @param {string} what - the command, for the rejection's message
 * @returns {Promise<T>}
 */
function answeredInTime(reply, what) {
	/** @type {any} */
	let timer;
	const late = new Promise((_, reject) => {
		timer = setTimer(() => reject(new Error(what + ' was not answered within ' + KEYSPACE_STEP_TIMEOUT_MS + ' ms')), KEYSPACE_STEP_TIMEOUT_MS);
		if (timer.unref) timer.unref();
	});
	return Promise.race([reply, late]).finally(() => clearTimer(timer));
}

/**
 * What kind of failure an error is, to tell one cause from the next: a
 * system error's code (ECONNREFUSED), else the reply's leading word (NOPERM,
 * WRONGPASS). Compared only, never logged.
 * @param {unknown} err
 * @returns {string}
 */
function failureKind(err) {
	const e = /** @type {any} */ (err);
	if (typeof e?.code === 'string' && e.code !== '') return e.code;
	return String(e?.message ?? '').split(' ', 1)[0];
}

/**
 * A node's address, as ioredis keys its node list.
 * @param {unknown} node
 * @returns {string}
 */
function addressOf(node) {
	const options = /** @type {any} */ (node)?.options;
	return options ? options.host + ':' + options.port : '';
}

/**
 * JSON object shape used by every presence event payload container.
 * Arrays are objects in JavaScript but are not keyed presence records.
 * @param {unknown} value
 * @returns {value is Record<string, any>}
 */
function isRecord(value) {
	return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Validate the event-specific payload before it reaches the diff buffer.
 * The buffer deliberately trusts its callers and coerces Map keys back to
 * object-property names at flush time, so an object-valued forged key would
 * otherwise become "[object Object]" in a live roster diff.
 *
 * LEAVE permits null data: purgeUser publishes that shape when no public
 * roster value is available. Every other identity event carries a record,
 * and FIELDS always carries the two record maps produced by publishEvent.
 *
 * @param {unknown} event
 * @param {unknown} payload
 * @returns {boolean}
 */
function isValidPresencePayload(event, payload) {
	if (!isRecord(payload) || typeof payload.key !== 'string') return false;
	if (event === INTERNAL_EVENTS.JOIN || event === INTERNAL_EVENTS.UPDATED) {
		return isRecord(payload.data);
	}
	if (event === INTERNAL_EVENTS.LEAVE) {
		return payload.data === null || isRecord(payload.data);
	}
	if (event === INTERNAL_EVENTS.FIELDS) {
		return isRecord(payload.durable) && isRecord(payload.transient);
	}
	return false;
}

/**
 * Create the cross-instance subscriber subsystem.
 *
 * @param {{
 *   client: import('../index.js').RedisClient,
 *   instanceId: string,
 *   maxEnvelopeBytes: number,
 *   keyspaceNotifications: boolean,
 *   bufferDiff: (topic: string, op: string, key: any, data: any, platform: any) => void,
 *   bufferUpdate: (topic: string, key: string, changed: Record<string, any>, platform: any) => void,
 *   localData: Map<string, Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>>,
 *   emit: (fullTopic: string, event: string, data: any, platform: any, opts?: any) => void,
 *   eventChannel: (topic: string) => string,
 *   mKeyspaceCleanups: { inc: () => void } | null | undefined
 * }} deps
 */
export function createSubscriber({ client, instanceId, maxEnvelopeBytes, keyspaceNotifications, bufferDiff, bufferUpdate, localData, emit, eventChannel, mKeyspaceCleanups }) {
	// Redis subscriber for cross-instance join/leave events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	/** @type {Set<string>} - channels we have subscribed to */
	const subscribedChannels = new Set();
	let idleTimer = null;
	let keyspaceSubscribed = false;

	// Per-master keyspace subscriber connections, used only on a cluster. A
	// keyspace `del` event is node-local: each node fires it only for keys it
	// owns and does NOT propagate it across the cluster bus, so a single
	// Cluster.psubscribe lands on one node and misses dels on every other master.
	// We therefore psubscribe `__keyevent@*__:del` on every master directly. Each
	// del is delivered on exactly one master (the key's owner), so there are no
	// duplicates to dedupe. On standalone the ordinary psubscribe on `subscriber`
	// catches everything and this map stays empty.
	/** @type {Map<string, import('ioredis').Redis>} master node id -> keyspace subscriber */
	const keyspaceSubs = new Map();
	// Master node id -> its address, for every master a sweep has read.
	/** @type {Map<string, string>} */
	const keyspaceAddresses = new Map();
	let keyspaceReconcileTimer = null;
	// When the pending sweep is due, so one asked for now replaces a retry
	// still waiting out its backoff.
	let keyspaceReconcileDueAt = 0;
	let keyspaceRetryMs = 0;
	// Set by a sweep that missed a master the node list may be wrong about; the
	// retry reads the slot map first (see refreshTopology).
	let keyspaceRefreshDue = false;
	// Whether a '+node' already swept since the last retry (see the listener).
	let keyspaceNodeSweepTaken = false;
	/** @type {Promise<void> | null} single-flight guard for the master sweep */
	let keyspaceReconciling = null;
	let keyspaceRerun = false;
	// Masters whose failed subscription is reported for the current streak, with
	// the kinds of failure reported (see failureKind).
	/** @type {Map<string, Set<string>>} */
	const keyspaceRefused = new Map();
	// Addresses of masters that did not answer CLUSTER MYID, reported once
	// until the master answers or a refreshed slot map drops it.
	/** @type {Set<string>} */
	const keyspaceUnread = new Set();
	// The masters' addresses at the last refresh (see pruneKeyspaceMasters), as
	// a set and as a comparable signature; null before the first.
	/** @type {Set<string> | null} */
	let keyspaceLive = null;
	/** @type {string | null} */
	let keyspaceMasterSet = null;
	// Connections closed on purpose because their master left, whose pending
	// subscription is then no failure.
	/** @type {WeakSet<object>} */
	const keyspaceRetired = new WeakSet();
	// Connections whose subscription stands, which are asked for a PING (see
	// watchLiveness) once the cleanup is set up.
	/** @type {WeakSet<object>} */
	const keyspaceStanding = new WeakSet();
	/** @type {WeakMap<object, string>} connection -> the id of its master */
	const keyspaceIdOf = new WeakMap();
	/** @type {{ stop(): void } | null} */
	let keyspaceLiveness = null;
	// A sweep that failed as a whole is reported once per failure streak.
	let keyspaceReconcileFailing = false;
	// Set once dispose() runs so a sweep that is mid-await (or a post-dispose error /
	// node event) cannot repopulate keyspaceSubs with connections nothing will quit.
	let disposed = false;
	// The cluster `+node` / `refresh` handlers, kept so dispose() can detach them.
	// ioredis does not surface a slot migration to a freshly scaled-out master as an
	// error on the existing per-master psubscribe connections (a pattern
	// subscription is not slot-bound), so the error-driven reconcile alone would
	// miss it; a node joining re-sweeps and covers the new master, and a refreshed
	// slot map drops the masters that left (pruneKeyspaceMasters).
	let keyspaceNodeListener = null;
	let keyspaceRefreshListener = null;

	// The per-topic hash key prefix; a `del` of a key under it means the topic
	// emptied (its last per-field TTL lapsed), which we forward as an empty state.
	const topicPrefix = client.key('presence:topic:{');

	// Forward a whole-topic-hash deletion as an empty state to local subscribers.
	// The per-topic hash has no whole-key TTL; it is REMOVED (a `del` keyevent) the
	// moment its last field's per-field TTL (HPEXPIRE) lapses - i.e. when the last
	// instance presenting any user on the topic has stopped heartbeating. That
	// removal is the "whole topic empty" signal. Per-user hash keys
	// (presence:user:{topic}:{userKey}) and the events channel are filtered out by
	// the prefix.
	//
	// We listen for `del`, not `expired`/`hexpired`: a hash that loses its last
	// field via TTL is deleted, and key deletion is a `del` event in the generic
	// (`g`) class on BOTH Redis 7.4 and Valkey 9.0 - one portable flag. The
	// hash-field-expiry event itself (`hexpired`) is class `h` on Redis but class
	// `x` on Valkey, so keying on it would need a server-specific flagset.
	//
	// A `del` can race a fresh join that recreates the key (and an explicit
	// last-leave also deletes it, but the leave path already emitted its diff), so
	// we re-check existence: emit the empty snapshot only when the key is actually
	// gone. If it exists again a user is present and the join's own diff carries the
	// truth. The existence re-check runs on the main client, which routes to the
	// key's owning node on a cluster.
	function onKeyspaceDel(_pattern, _channel, deletedKey) {
		if (typeof deletedKey !== 'string') return;
		if (!deletedKey.startsWith(topicPrefix)) return;
		if (!activePlatform) return;
		const topic = deletedKey.slice(topicPrefix.length, -1); // drop the '}' closing the {topic} hash tag
		client.redis.exists(deletedKey).then((stillThere) => {
			if (stillThere || !activePlatform) return;
			emit('__presence:' + topic, 'state', {}, activePlatform, { relay: false });
			mKeyspaceCleanups?.inc();
		}).catch(() => {});
	}

	// Redis accepts the psubscribe whether or not keyspace notifications are
	// enabled, so a subscription that succeeds says nothing about whether a
	// deletion will ever arrive. Read the setting where the server allows it;
	// managed services and ACLs often refuse CONFIG, and that is not reported.
	function checkKeyspaceEvents(conn, master) {
		if (typeof conn?.config !== 'function') return;
		let reply;
		try { reply = conn.config('GET', 'notify-keyspace-events'); } catch { return; }
		Promise.resolve(reply).then((r) => {
			const flags = Array.isArray(r) ? r[1] : r?.['notify-keyspace-events'];
			if (typeof flags !== 'string' || disposed) return;
			// `del` is a keyevent (E) of the generic class (g, also part of A).
			if (flags.includes('E') && (flags.includes('g') || flags.includes('A'))) return;
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_EVENTS_OFF, {
				attributes: master === null ? { flags } : { master, flags }
			});
		}, () => {});
	}

	// Subscribe the del keyevent on every current master (cluster path). Idempotent
	// and single-flight: adds a connection for any master not yet covered and is
	// safe to call repeatedly. A subscriber-connection error reschedules it so a
	// moved master is re-covered, and a sweep that leaves a master it lists
	// uncovered (refused, failed, timed out, or its id unread) runs again after a
	// backoff, since nothing else would ask once the cause is fixed. A master
	// ioredis dropped from its node list (its own connection to it ended) comes
	// back through '+node' or a refreshed slot map, which the retry of a sweep
	// reads first (refreshTopology) when the sweep missed a master that did not
	// answer, or one the last slot map listed that ioredis no longer does.
	async function reconcileKeyspaceMasters() {
		if (disposed) return;
		if (keyspaceReconciling) {
			// The running sweep may already be past the master that asked; run
			// once more when it finishes.
			keyspaceRerun = true;
			return keyspaceReconciling;
		}
		let swept = false;
		let missed = false;
		keyspaceReconciling = (async () => {
			let masters;
			try {
				masters = /** @type {any} */ (client.redis).nodes('master');
			} catch (err) {
				missed = true;
				reportSweepFailure(err);
				return;
			}
			// A master the last refreshed slot map listed that ioredis no longer
			// does (its connection to it ended) and that has no subscription here
			// is missed too, and the retry reads the slot map again first.
			const covered = new Set();
			for (const [id, s] of keyspaceSubs) {
				if (keyspaceStanding.has(s)) covered.add(keyspaceAddresses.get(id));
			}
			if (keyspaceLive !== null) {
				const listed = new Set(masters.map(addressOf));
				for (const address of keyspaceLive) {
					if (listed.has(address) || covered.has(address)) continue;
					missed = true;
					keyspaceRefreshDue = true;
				}
			}
			await Promise.all(masters.map(async (node) => {
				const address = addressOf(node);
				// Its subscription stands: nothing to ask it, and an answer that does
				// not come over ioredis's own connection to it says nothing of it.
				if (covered.has(address)) return;
				let id;
				try {
					id = await answeredInTime(node.cluster('MYID'), 'CLUSTER MYID');
				} catch (err) {
					if (disposed) return;
					missed = true;
					// Not answering, unlike refusing the command, may mean the master
					// moved or left: the retry reads the slot map again first.
					if (!isReplyError(err)) keyspaceRefreshDue = true;
					// ioredis drops a node whose connection failed from its list
					// until a refreshed slot map lists it again, so this is reported
					// once until the master answers or the slot map drops it. A
					// node the last refreshed map does not list (a refresh dropped it
					// while this was out) is no master to report.
					if (keyspaceLive !== null && !keyspaceLive.has(address)) return;
					if (!keyspaceUnread.has(address)) {
						keyspaceUnread.add(address);
						emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE, { error: err });
					}
					return;
				}
				keyspaceUnread.delete(address);
				// dispose() may have run during the MYID round trip; bail before
				// opening a connection nothing would ever quit. So may a refresh
				// that found the node no master (a reconnect lists its startup
				// nodes as masters until the slot map says otherwise), whose prune
				// this would undo.
				if (disposed || !isMaster(node)) return;
				keyspaceAddresses.set(id, address);
				if (keyspaceSubs.has(id)) return;
				const s = node.duplicate({ enableReadyCheck: false });
				// Until the subscription stands, a failure of this connection (a
				// refused login, a dropped connect) rejects the psubscribe below,
				// which reports it and leaves the master to the retry backoff; an
				// immediate sweep from here would redial it in a tight loop.
				let subscribed = false;
				/** @type {unknown} what failed the connection before it subscribed */
				let cause = null;
				s.on('error', (err) => {
					if (!subscribed) {
						cause = err;
						return;
					}
					emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBER, { error: err });
					scheduleKeyspaceReconcile();
				});
				s.on('pmessage', onKeyspaceDel);
				// ioredis gives cluster node connections no retry strategy and a
				// duplicate inherits it, so this connection never reconnects: one the
				// master drops, or that stopped answering, ends for good and is
				// replaced.
				s.on('end', () => {
					if (keyspaceSubs.get(id) !== s) return;
					keyspaceSubs.delete(id);
					if (subscribed) scheduleKeyspaceReconcile();
				});
				keyspaceSubs.set(id, s); // now tracked: a dispose() during psubscribe will close it
				keyspaceIdOf.set(s, id);
				// A master that accepted the connection and does not answer: closing
				// it rejects the call below as closed, and this says why.
				const stalled = setTimer(() => {
					if (cause === null) cause = new Error('PSUBSCRIBE was not answered within ' + KEYSPACE_STEP_TIMEOUT_MS + ' ms');
					closeConnection(s);
				}, KEYSPACE_STEP_TIMEOUT_MS);
				if (stalled.unref) stalled.unref();
				try {
					await s.psubscribe('__keyevent@*__:del');
					// Pruned or disposed while subscribing: already quit.
					if (disposed || keyspaceRetired.has(s)) return;
					subscribed = true;
					keyspaceStanding.add(s);
					keyspaceSubscribed = true;
					keyspaceRefused.delete(id);
					checkKeyspaceEvents(node, id);
				} catch (err) {
					// Pruned or disposed while subscribing: closed on purpose.
					if (disposed || keyspaceRetired.has(s)) return;
					keyspaceSubs.delete(id);
					s.removeAllListeners();
					closeConnection(s);
					missed = true;
					// A connection that failed rejects the call only as closed; the
					// error it raised first says why. Retried until the master
					// accepts it, so each kind of failure is reported once per
					// streak (an outage that ends in a refusal shows both).
					const error = cause !== null && /** @type {any} */ (err)?.message === 'Connection is closed.' ? cause : err;
					const kind = failureKind(error);
					let kinds = keyspaceRefused.get(id);
					if (!kinds) keyspaceRefused.set(id, kinds = new Set());
					if (!kinds.has(kind)) {
						kinds.add(kind);
						emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE, { error, attributes: { master: id } });
					}
				} finally {
					clearTimer(stalled);
				}
			}));
			keyspaceReconcileFailing = false;
			swept = true;
		})().finally(() => {
			keyspaceReconciling = null;
			if (disposed) return;
			if (keyspaceRerun) {
				keyspaceRerun = false;
				scheduleKeyspaceReconcile();
				return;
			}
			if (swept && !missed) {
				keyspaceRetryMs = 0;
				keyspaceRefreshDue = false;
				keyspaceNodeSweepTaken = false;
				return;
			}
			keyspaceRetryMs = keyspaceRetryMs === 0
				? KEYSPACE_RETRY_FIRST_MS
				: Math.min(keyspaceRetryMs * 2, KEYSPACE_RETRY_MAX_MS);
			scheduleKeyspaceReconcile(keyspaceRetryMs, true);
		});
		return keyspaceReconciling;
	}

	// Drop what belongs to masters that left: the connections, the refusal
	// record (a master that comes back starts over) and the unread report. Run
	// as ioredis emits 'refresh', right after it rebuilt its node list from the
	// slot map, when the list names exactly the cluster's masters; between
	// refreshes it also lacks a master whose ioredis connection merely ended
	// (an idle timeout), whose keyspace connection keeps working.
	// Returns whether the masters differ from those at the last refresh, which
	// is the one thing a refresh says that '+node' and the connections' own
	// ends do not: a replica promoted in place joins the masters without a
	// '+node'. ioredis also refreshes on every MOVED and slotsRefreshInterval
	// tick, which must not cut a retry's backoff short.
	function pruneKeyspaceMasters() {
		let masters;
		try { masters = /** @type {any} */ (client.redis).nodes('master'); } catch { return false; }
		// A map that lists no master came from a node with no slots, one reset
		// after it was removed for example, and says nothing of the others.
		if (masters.length === 0) return false;
		const live = new Set(masters.map(addressOf));
		const signature = [...live].sort().join(' ');
		const changed = signature !== keyspaceMasterSet;
		keyspaceMasterSet = signature;
		keyspaceLive = live;
		for (const [id, address] of keyspaceAddresses) {
			if (live.has(address)) continue;
			keyspaceAddresses.delete(id);
			keyspaceRefused.delete(id);
			const s = keyspaceSubs.get(id);
			if (!s) continue;
			keyspaceSubs.delete(id);
			keyspaceRetired.add(s);
			s.removeAllListeners();
			closeConnection(s);
		}
		for (const address of keyspaceUnread) {
			if (!live.has(address)) keyspaceUnread.delete(address);
		}
		return changed;
	}

	// ioredis reads the slot map again after MOVED or a failed command, which
	// nothing here sends, so a master that restarted, moved to a new address or
	// whose idle connection ioredis closed can stay off its node list. A sweep
	// that missed such a master, or one that did not answer, reads it before its
	// retry; the 'refresh' that follows prunes, and its '+node' sweeps again.
	function refreshTopology() {
		const redis = /** @type {any} */ (client.redis);
		if (typeof redis.refreshSlotsCache === 'function') redis.refreshSlotsCache();
	}

	/** Whether ioredis lists `node` among the masters now. */
	function isMaster(node) {
		try {
			return /** @type {any} */ (client.redis).nodes('master').includes(node);
		} catch {
			return false;
		}
	}

	// A sweep that failed as a whole (the master list could not be read, or a
	// connection could not be created) is retried after a backoff, so it is
	// reported once per failure streak; a sweep that completes ends the streak.
	function reportSweepFailure(err) {
		if (keyspaceReconcileFailing) return;
		keyspaceReconcileFailing = true;
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_RECONCILE, { error: err });
	}

	/**
	 * @param {number} [delayMs]
	 * @param {boolean} [retry] - a missed sweep's retry, which reads the slot map
	 *   first when that sweep asked for it
	 */
	function scheduleKeyspaceReconcile(delayMs = 0, retry = false) {
		if (disposed) return;
		const dueAt = monotonicNow() + delayMs;
		if (keyspaceReconcileTimer) {
			// A dropped connection or a topology change does not wait out a
			// retry's backoff.
			if (dueAt >= keyspaceReconcileDueAt) return;
			clearTimer(keyspaceReconcileTimer);
		}
		keyspaceReconcileDueAt = dueAt;
		keyspaceReconcileTimer = setTimer(() => {
			keyspaceReconcileTimer = null;
			if (retry) {
				keyspaceNodeSweepTaken = false;
				if (keyspaceRefreshDue) {
					keyspaceRefreshDue = false;
					refreshTopology();
				}
			}
			reconcileKeyspaceMasters().catch(reportSweepFailure);
		}, delayMs);
		if (keyspaceReconcileTimer.unref) keyspaceReconcileTimer.unref();
	}

	// Wire the keyspace-del cleanup. On a cluster, psubscribe every master directly
	// (node-local events); on standalone, the ordinary psubscribe on the shared
	// subscriber connection catches everything.
	async function setupKeyspaceCleanup() {
		if (isCluster(client.redis)) {
			// Re-sweep when a node joins, so a master added by a scale-out (which
			// does not error the existing slot-bound-free pattern subscriptions)
			// still gets a keyspace subscriber, and prune after each refreshed slot
			// map. Attached once: an idle shutdown sets the subscriber up again.
			if (keyspaceNodeListener === null) {
				// ioredis emits '+node' for every node connection it creates, and
				// each slot map read creates one again for a master that refuses
				// connections, so while sweeps are failing one sweep per retry
				// comes from it; a later node is covered by that retry.
				keyspaceNodeListener = () => {
					if (keyspaceRetryMs > 0) {
						if (keyspaceNodeSweepTaken) return;
						keyspaceNodeSweepTaken = true;
					}
					scheduleKeyspaceReconcile();
				};
				keyspaceRefreshListener = () => {
					if (pruneKeyspaceMasters()) scheduleKeyspaceReconcile();
				};
				try {
					/** @type {any} */ (client.redis).on('+node', keyspaceNodeListener);
					/** @type {any} */ (client.redis).on('refresh', keyspaceRefreshListener);
				} catch {
					keyspaceNodeListener = null;
					keyspaceRefreshListener = null;
				}
			}
			// The node list now stands for the last refreshed slot map until a
			// refresh comes, so a master it loses is missed rather than forgotten.
			if (keyspaceLive === null) pruneKeyspaceMasters();
			// One that stopped answering is closed, and its 'end' replaces it.
			if (keyspaceLiveness === null) {
				keyspaceLiveness = watchLiveness(
					'presence-keyspace',
					() => [...keyspaceSubs.values()].filter((s) => keyspaceStanding.has(s)),
					closeConnection,
					{ identify: (s) => keyspaceIdOf.get(s) }
				);
			}
			await reconcileKeyspaceMasters();
			return;
		}
		subscriber.on('pmessage', onKeyspaceDel);
		try {
			await subscriber.psubscribe('__keyevent@*__:del');
			keyspaceSubscribed = true;
			checkKeyspaceEvents(client.redis, null);
		} catch (err) {
			emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_KEYSPACE_SUBSCRIBE, { error: err });
		}
	}

	async function ensureSubscriber(platform) {
		// Disposed for good: nothing would close what this opened.
		if (disposed) return;
		activePlatform = platform;
		if (!subscriber) {
			// Inbound bus guard: presence:events:* accepts messages from a
			// shared transport, so cap raw bytes before JSON.parse like every
			// sibling bus module.
			const busValidator = createBusValidator({ label: 'redis presence', maxBytes: maxEnvelopeBytes });
			const prefix = client.key('presence:events:');
			subscriber = openSubscriber(client, 'presence');
			subscriber.on('error', (err) => {
				emitRegisteredFailure(EXTENSIONS_ERROR_IDS.PRESENCE_SUBSCRIBER, { error: err });
			});
			subscriber.on('message', (ch, message) => {
				if (!ch.startsWith(prefix)) return;
				if (!busValidator.acceptRaw(message)) return;
				try {
					const parsed = JSON.parse(message);
					const topic = ch.slice(prefix.length);
					if (!isRecord(parsed)) return;
					if (typeof parsed.instanceId !== 'string' || parsed.instanceId.length === 0) return;
					// The topic is the one the channel names, and this instance
					// subscribes only the channels of topics it tracks. No
					// wire-topic shape rule here: its 256-character limit would
					// drop a peer's events for a longer topic both instances track.
					if (parsed.topic !== topic) return;
					if (!isValidPresencePayload(parsed.event, parsed.payload)) return;
					if (parsed.instanceId === instanceId) return;
					if (!activePlatform) return;
					const ev = parsed.event;
					const payload = parsed.payload;
					// Relayed entry data gets the field rule of every other
					// ingress (local update, Redis read-back, FIELDS below). A
					// peer's join and leave data is its select output after
					// stripInternal, and its update data publicData, both of which
					// already drop these names, so only a forged envelope loses
					// anything.
					if (ev === INTERNAL_EVENTS.JOIN || ev === INTERNAL_EVENTS.UPDATED) {
						bufferDiff(topic, 'join', payload.key, mergeFields(newFieldMap(), payload.data), activePlatform);
					} else if (ev === INTERNAL_EVENTS.LEAVE) {
						bufferDiff(topic, 'leave', payload.key, payload.data === null ? null : mergeFields(newFieldMap(), payload.data), activePlatform);
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
							// Filtered BEFORE the emptiness test, not after. A
							// spread of the raw envelope is non-empty whenever it
							// carries any key at all, including ones the policy
							// rejects - so an all-reserved envelope passed the
							// test and fanned an `update` diff carrying nothing
							// out to every local subscriber.
							const changed = mergeFields(newFieldMap(), durable);
							mergeFields(changed, transient);
							if (Object.keys(changed).length > 0) {
								bufferUpdate(topic, key, changed, activePlatform);
								const localEntry = localData.get(topic)?.get(key);
								if (localEntry) {
									if (!localEntry.fields) localEntry.fields = newFieldMap();
									// Same rule as the local presence-update ingress:
									// a forged envelope must not land a field name the
									// client path refuses, or the bus becomes a way to
									// inject phantom fields into every roster frame.
									mergeFields(localEntry.fields, durable);
									mergeFields(localEntry.fields, transient);
								}
							}
						}
					}
				} catch {
					// Malformed, skip
				}
			});
			if (keyspaceNotifications) {
				await setupKeyspaceCleanup();
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
						const sub = subscriber;
						subscriber = null;
						sub.quit().catch(() => sub.disconnect());
					}
				}, 30000);
				if (idleTimer.unref) idleTimer.unref();
			}
		}
	}

	async function unsubscribeAllChannels() {
		if (subscriber) {
			for (const ch of subscribedChannels) {
				await subscriber.unsubscribe(ch).catch(() => {});
			}
			subscribedChannels.clear();
		}
	}

	function dispose() {
		// Set first: a sweep awaiting MYID/psubscribe, or a `+node`/error-driven
		// reconcile, must not repopulate keyspaceSubs with connections nothing quits.
		disposed = true;
		if (idleTimer) {
			clearTimer(idleTimer);
			idleTimer = null;
		}
		if (keyspaceReconcileTimer) {
			clearTimer(keyspaceReconcileTimer);
			keyspaceReconcileTimer = null;
		}
		if (keyspaceLiveness !== null) {
			keyspaceLiveness.stop();
			keyspaceLiveness = null;
		}
		if (keyspaceNodeListener) {
			try {
				/** @type {any} */ (client.redis).removeListener('+node', keyspaceNodeListener);
				/** @type {any} */ (client.redis).removeListener('refresh', keyspaceRefreshListener);
			} catch { /* not a cluster / already gone */ }
			keyspaceNodeListener = null;
			keyspaceRefreshListener = null;
		}
		if (subscriber) {
			const sub = subscriber;
			subscriber = null;
			sub.quit().catch(() => sub.disconnect());
		}
		for (const s of keyspaceSubs.values()) {
			// Drop the error handler first so a close-time socket error cannot
			// schedule a reconcile (the disposed guard also blocks it, this is
			// belt-and-braces). Closed at once: a QUIT waits forever on a master
			// that stopped answering.
			s.removeAllListeners();
			closeConnection(s);
		}
		keyspaceSubs.clear();
		subscribedChannels.clear();
		keyspaceSubscribed = false;
		activePlatform = null;
	}

	return {
		subscribeToTopic,
		unsubscribeFromTopic,
		unsubscribeAllChannels,
		dispose,
		get activePlatform() {
			return activePlatform;
		}
	};
}
