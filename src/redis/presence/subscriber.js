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

import { setTimer, clearTimer } from '../../shared/runtime.js';
import { isCluster } from '../../shared/cluster.js';
import { INTERNAL_EVENTS } from './lua.js';

/**
 * Create the cross-instance subscriber subsystem.
 *
 * @param {{
 *   client: import('../index.js').RedisClient,
 *   instanceId: string,
 *   keyspaceNotifications: boolean,
 *   bufferDiff: (topic: string, op: string, key: any, data: any, platform: any) => void,
 *   bufferUpdate: (topic: string, key: string, changed: Record<string, any>, platform: any) => void,
 *   localData: Map<string, Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>>,
 *   emit: (fullTopic: string, event: string, data: any, platform: any, opts?: any) => void,
 *   eventChannel: (topic: string) => string,
 *   mKeyspaceCleanups: { inc: () => void } | null | undefined
 * }} deps
 */
export function createSubscriber({ client, instanceId, keyspaceNotifications, bufferDiff, bufferUpdate, localData, emit, eventChannel, mKeyspaceCleanups }) {
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
	let keyspaceReconcileTimer = null;
	/** @type {Promise<void> | null} single-flight guard for the master sweep */
	let keyspaceReconciling = null;
	// Set once dispose() runs so a sweep that is mid-await (or a post-dispose error /
	// node event) cannot repopulate keyspaceSubs with connections nothing will quit.
	let disposed = false;
	// The cluster `+node`/`-node` handler, kept so dispose() can detach it. ioredis
	// does not surface a slot migration to a freshly scaled-out master as an error on
	// the existing per-master psubscribe connections (a pattern subscription is not
	// slot-bound), so the error-driven reconcile alone would miss it; reacting to a
	// node joining/leaving the cluster re-sweeps and covers the new master.
	let keyspaceNodeListener = null;

	// The per-topic hash key prefix; a `del` of a key under it means the topic
	// emptied (its last per-field TTL lapsed), which we forward as an empty state.
	const topicPrefix = client.key('presence:topic:{');

	const KEYSPACE_PSUBSCRIBE_FAIL =
		'[redis/presence] keyspace notifications: psubscribe failed - ' +
		'enable on the server with `CONFIG SET notify-keyspace-events Eg` (or any flagset including `K`/`E` and `g`): ';

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

	// Subscribe the del keyevent on every current master (cluster path). Idempotent
	// and single-flight: adds a connection for any master not yet covered, prunes
	// one whose master is gone (failover / reshard), and is safe to call
	// repeatedly. A subscriber-connection error reschedules it so a moved master is
	// re-covered.
	async function reconcileKeyspaceMasters() {
		if (disposed) return;
		if (keyspaceReconciling) return keyspaceReconciling;
		keyspaceReconciling = (async () => {
			let masters;
			try { masters = /** @type {any} */ (client.redis).nodes('master'); } catch { return; }
			const liveIds = new Set();
			await Promise.all(masters.map(async (node) => {
				let id;
				try { id = await node.cluster('MYID'); } catch { return; }
				// dispose() may have run during the MYID round trip; bail before
				// opening a connection nothing would ever quit.
				if (disposed) return;
				liveIds.add(id);
				if (keyspaceSubs.has(id)) return;
				const s = node.duplicate({ enableReadyCheck: false });
				s.on('error', (err) => {
					console.error('presence keyspace subscriber error:', err.message);
					scheduleKeyspaceReconcile();
				});
				s.on('pmessage', onKeyspaceDel);
				keyspaceSubs.set(id, s); // now tracked: a dispose() during psubscribe will quit it
				try {
					await s.psubscribe('__keyevent@*__:del');
					keyspaceSubscribed = true;
				} catch (err) {
					keyspaceSubs.delete(id);
					s.removeAllListeners();
					s.quit().catch(() => s.disconnect());
					console.warn(KEYSPACE_PSUBSCRIBE_FAIL + err.message + '\n  See: https://svti.me/redis-keyspace');
				}
			}));
			if (disposed) return;
			// Drop connections to masters that no longer exist (a failover promoted a
			// replica with a new id, or a node left); a fresh master is added above.
			for (const [id, s] of keyspaceSubs) {
				if (!liveIds.has(id)) {
					keyspaceSubs.delete(id);
					s.removeAllListeners();
					s.quit().catch(() => s.disconnect());
				}
			}
		})().finally(() => { keyspaceReconciling = null; });
		return keyspaceReconciling;
	}

	function scheduleKeyspaceReconcile() {
		if (disposed || keyspaceReconcileTimer) return;
		keyspaceReconcileTimer = setTimer(() => {
			keyspaceReconcileTimer = null;
			reconcileKeyspaceMasters().catch((err) => console.error('presence keyspace reconcile failed:', err.message));
		}, 0);
		if (keyspaceReconcileTimer.unref) keyspaceReconcileTimer.unref();
	}

	// Wire the keyspace-del cleanup. On a cluster, psubscribe every master directly
	// (node-local events); on standalone, the ordinary psubscribe on the shared
	// subscriber connection catches everything.
	async function setupKeyspaceCleanup() {
		if (isCluster(client.redis)) {
			// Re-sweep when the cluster topology grows/shrinks so a master added by a
			// scale-out (which does not error the existing slot-bound-free pattern
			// subscriptions) still gets a keyspace subscriber.
			keyspaceNodeListener = () => scheduleKeyspaceReconcile();
			try {
				/** @type {any} */ (client.redis).on('+node', keyspaceNodeListener);
				/** @type {any} */ (client.redis).on('-node', keyspaceNodeListener);
			} catch { keyspaceNodeListener = null; }
			await reconcileKeyspaceMasters();
			return;
		}
		subscriber.on('pmessage', onKeyspaceDel);
		try {
			await subscriber.psubscribe('__keyevent@*__:del');
			keyspaceSubscribed = true;
		} catch (err) {
			console.warn(KEYSPACE_PSUBSCRIBE_FAIL + err.message + '\n  See: https://svti.me/redis-keyspace');
		}
	}

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
						subscriber.quit().catch(() => subscriber.disconnect());
						subscriber = null;
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
		if (keyspaceNodeListener) {
			try {
				/** @type {any} */ (client.redis).removeListener('+node', keyspaceNodeListener);
				/** @type {any} */ (client.redis).removeListener('-node', keyspaceNodeListener);
			} catch { /* not a cluster / already gone */ }
			keyspaceNodeListener = null;
		}
		if (subscriber) {
			const sub = subscriber;
			subscriber = null;
			sub.quit().catch(() => sub.disconnect());
		}
		for (const s of keyspaceSubs.values()) {
			// Drop the error handler first so a quit-time socket error cannot schedule
			// a reconcile (the disposed guard also blocks it, this is belt-and-braces).
			s.removeAllListeners();
			s.quit().catch(() => s.disconnect());
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
