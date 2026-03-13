/**
 * Redis-backed broadcast groups for svelte-adapter-uws.
 *
 * Same API as the core createGroup plugin, but membership and metadata
 * are stored in Redis so groups work across multiple server instances.
 *
 * Local members (ws connections on this instance) still get tracked locally
 * because WebSocket objects cannot be serialized. Cross-instance publish()
 * uses Redis pub/sub to reach members on other instances.
 *
 * Storage layout:
 *   - Key `{prefix}group:{name}:meta`     - hash (group metadata)
 *   - Key `{prefix}group:{name}:members`  - hash (field=memberId, value=JSON {role, instanceId})
 *   - Key `{prefix}group:{name}:closed`   - string flag ("1" if closed)
 *   - Channel `{prefix}group:{name}:events` - pub/sub for cross-instance events
 *
 * @module svelte-adapter-uws-extensions/redis/groups
 */

import { randomBytes } from 'node:crypto';

const VALID_ROLES = new Set(['member', 'admin', 'viewer']);

/**
 * @typedef {'member' | 'admin' | 'viewer'} GroupRole
 */

/**
 * @typedef {Object} RedisGroupOptions
 * @property {number} [maxMembers=Infinity] - Maximum members allowed
 * @property {Record<string, any>} [meta] - Initial group metadata
 * @property {(ws: any, role: GroupRole) => void} [onJoin]
 * @property {(ws: any, role: GroupRole) => void} [onLeave]
 * @property {(ws: any, role: GroupRole) => void} [onFull]
 * @property {() => void} [onClose]
 */

/**
 * @typedef {Object} RedisGroup
 * @property {string} name
 * @property {() => Promise<Record<string, any>>} getMeta
 * @property {(meta: Record<string, any>) => Promise<void>} setMeta
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, role?: GroupRole) => Promise<boolean>} join
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform) => Promise<void>} leave
 * @property {(platform: import('svelte-adapter-uws').Platform, event: string, data?: any, role?: GroupRole) => Promise<void>} publish
 * @property {(platform: import('svelte-adapter-uws').Platform, ws: any, event: string, data?: any) => void} send
 * @property {() => Array<{ws: any, role: GroupRole}>} localMembers - Members on this instance
 * @property {() => Promise<number>} count - Total members across all instances
 * @property {(ws: any) => boolean} has
 * @property {(platform: import('svelte-adapter-uws').Platform) => Promise<void>} close
 * @property {() => void} destroy - Stop subscriber
 */

/**
 * Create a Redis-backed broadcast group.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {string} name
 * @param {RedisGroupOptions} [options]
 * @returns {RedisGroup}
 */
export function createGroup(client, name, options = {}) {
	if (!name || typeof name !== 'string') {
		throw new Error('redis group: name must be a non-empty string');
	}

	const maxMembers = options.maxMembers ?? Infinity;
	const onJoin = options.onJoin ?? null;
	const onLeave = options.onLeave ?? null;
	const onFull = options.onFull ?? null;
	const onClose = options.onClose ?? null;

	if (typeof maxMembers !== 'number' || (!Number.isFinite(maxMembers) && maxMembers !== Infinity) || maxMembers < 1) {
		throw new Error('redis group: maxMembers must be a positive number or Infinity');
	}
	if (onJoin != null && typeof onJoin !== 'function') throw new Error('redis group: onJoin must be a function');
	if (onLeave != null && typeof onLeave !== 'function') throw new Error('redis group: onLeave must be a function');
	if (onFull != null && typeof onFull !== 'function') throw new Error('redis group: onFull must be a function');
	if (onClose != null && typeof onClose !== 'function') throw new Error('redis group: onClose must be a function');

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;

	const metaKey = client.key('group:' + name + ':meta');
	const membersKey = client.key('group:' + name + ':members');
	const closedKey = client.key('group:' + name + ':closed');
	const eventChannel = client.key('group:' + name + ':events');
	const internalTopic = '__group:' + name;

	// Local member tracking (ws objects on this instance)
	/** @type {Map<any, { role: GroupRole, memberId: string }>} */
	const localMembers = new Map();
	let memberCounter = 0;

	// Set initial metadata
	if (options.meta) {
		redis.hmset(metaKey, options.meta).catch(() => {});
	}

	// Subscriber for cross-instance events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let subscribedPlatform = null;

	async function ensureSubscriber(platform) {
		if (subscriber) return;
		subscribedPlatform = platform;
		subscriber = client.duplicate();
		subscriber.on('message', (ch, message) => {
			if (ch !== eventChannel) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (subscribedPlatform) {
					subscribedPlatform.publish(internalTopic, parsed.event, parsed.data);
				}
			} catch {
				// Malformed, skip
			}
		});
		await subscriber.subscribe(eventChannel);
	}

	async function publishEvent(event, data) {
		const msg = JSON.stringify({ instanceId, event, data });
		await redis.publish(eventChannel, msg).catch(() => {});
	}

	return {
		get name() { return name; },

		async getMeta() {
			const raw = await redis.hgetall(metaKey);
			return raw || {};
		},

		async setMeta(meta) {
			if (Object.keys(meta).length === 0) {
				await redis.del(metaKey);
			} else {
				await redis.hmset(metaKey, meta);
			}
		},

		async join(ws, platform, role = 'member') {
			// Check closed
			const closed = await redis.get(closedKey);
			if (closed === '1') return false;

			// Idempotent
			if (localMembers.has(ws)) return true;

			if (!VALID_ROLES.has(role)) {
				throw new Error(`redis group "${name}": invalid role "${role}"`);
			}

			// Check capacity
			const currentCount = await redis.hlen(membersKey);
			if (currentCount >= maxMembers) {
				if (onFull) onFull(ws, role);
				return false;
			}

			const memberId = instanceId + ':' + (++memberCounter);
			localMembers.set(ws, { role, memberId });

			// Store in Redis
			await redis.hset(membersKey, memberId, JSON.stringify({ role, instanceId }));

			// Ensure cross-instance subscriber is running
			await ensureSubscriber(platform);

			// Publish join before subscribing (joiner doesn't see own join)
			platform.publish(internalTopic, 'join', { role });
			await publishEvent('join', { role });

			ws.subscribe(internalTopic);

			// Send current member list to the joiner
			const allRaw = await redis.hgetall(membersKey);
			const membersList = [];
			for (const v of Object.values(allRaw)) {
				try {
					const parsed = JSON.parse(v);
					membersList.push({ role: parsed.role });
				} catch { /* skip corrupted */ }
			}
			platform.send(ws, internalTopic, 'members', membersList);

			if (onJoin) onJoin(ws, role);
			return true;
		},

		async leave(ws, platform) {
			const entry = localMembers.get(ws);
			if (!entry) return;

			localMembers.delete(ws);
			ws.unsubscribe(internalTopic);

			// Remove from Redis
			await redis.hdel(membersKey, entry.memberId);

			platform.publish(internalTopic, 'leave', { role: entry.role });
			await publishEvent('leave', { role: entry.role });

			if (onLeave) onLeave(ws, entry.role);
		},

		async publish(platform, event, data, role) {
			const closed = await redis.get(closedKey);
			if (closed === '1') return;

			if (role == null) {
				// Broadcast to all via topic
				platform.publish(internalTopic, event, data);
				await publishEvent(event, data);
				return;
			}

			// Role-filtered: send individually to local members with that role
			for (const [ws, entry] of localMembers) {
				if (entry.role === role) {
					platform.send(ws, internalTopic, event, data);
				}
			}
			// For remote instances, publish the event with role filter info
			// Remote instances will handle their own local role filtering
			await publishEvent('__role_filtered', { event, data, role });
		},

		send(platform, ws, event, data) {
			if (!localMembers.has(ws)) {
				throw new Error(`redis group "${name}": ws is not a member`);
			}
			platform.send(ws, internalTopic, event, data);
		},

		localMembers() {
			const result = [];
			for (const [ws, entry] of localMembers) {
				result.push({ ws, role: entry.role });
			}
			return result;
		},

		async count() {
			return redis.hlen(membersKey);
		},

		has(ws) {
			return localMembers.has(ws);
		},

		async close(platform) {
			const alreadyClosed = await redis.get(closedKey);
			if (alreadyClosed === '1') return;

			await redis.set(closedKey, '1');

			platform.publish(internalTopic, 'close', null);
			await publishEvent('close', null);

			for (const [ws] of localMembers) {
				ws.unsubscribe(internalTopic);
			}
			localMembers.clear();

			// Clean up Redis keys
			await redis.del(membersKey);

			if (onClose) onClose();
		},

		destroy() {
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscribedPlatform = null;
		}
	};
}
