/**
 * Cross-instance subscriber for the Redis-backed presence tracker.
 *
 * Owns the duplicate Redis connection that receives peer instances' join/leave/
 * update events on the per-topic event channels, routes them into the local diff
 * buffer for client fan-out, and (when keyspace notifications are on) forwards a
 * whole-topic key expiry as an empty `state`. Also owns the per-topic channel
 * subscription set, the idle-shutdown timer, and the active platform (the most
 * recent platform to subscribe - the heartbeat and the event router both
 * broadcast through it). NO ssubscribe: ordinary pub/sub on client.duplicate()
 * plus a psubscribe keyspace pattern; the per-master sharded-subscriber pattern
 * lives in sharded-pubsub.js, not here.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/subscriber
 */

import { setTimer, clearTimer } from '../../shared/runtime.js';
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

	async function unsubscribeAllChannels() {
		if (subscriber) {
			for (const ch of subscribedChannels) {
				await subscriber.unsubscribe(ch).catch(() => {});
			}
			subscribedChannels.clear();
		}
	}

	function dispose() {
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
