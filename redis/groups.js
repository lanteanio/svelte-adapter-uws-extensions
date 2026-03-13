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
 *   - Key `{prefix}group:{name}:members`  - hash (field=memberId, value=JSON {role, instanceId, ts})
 *   - Key `{prefix}group:{name}:closed`   - string flag ("1" if closed)
 *   - Channel `{prefix}group:{name}:events` - pub/sub for cross-instance events
 *
 * @module svelte-adapter-uws-extensions/redis/groups
 */

import { randomBytes } from 'node:crypto';

const VALID_ROLES = new Set(['member', 'admin', 'viewer']);

/**
 * Lua script for atomic join: check capacity (excluding stale entries),
 * clean up stale entries, and insert the new member in one roundtrip.
 *
 * KEYS[1] = members hash key
 * ARGV[1] = maxMembers
 * ARGV[2] = memberId
 * ARGV[3] = member data JSON
 * ARGV[4] = now (ms)
 * ARGV[5] = memberTtl (ms)
 *
 * Returns 1 on success, 0 if full.
 */
const JOIN_SCRIPT = `
local key = KEYS[1]
local maxMembers = tonumber(ARGV[1])
local memberId = ARGV[2]
local memberData = ARGV[3]
local now = tonumber(ARGV[4])
local memberTtl = tonumber(ARGV[5])

local all = redis.call('hgetall', key)
local liveCount = 0
for i = 1, #all, 2 do
  local ok, val = pcall(cjson.decode, all[i+1])
  if ok and val.ts and (now - val.ts) <= memberTtl then
    liveCount = liveCount + 1
  else
    redis.call('hdel', key, all[i])
  end
end

if liveCount >= maxMembers then
  return 0
end
redis.call('hset', key, memberId, memberData)
return 1
`;

/**
 * @typedef {'member' | 'admin' | 'viewer'} GroupRole
 */

/**
 * @typedef {Object} RedisGroupOptions
 * @property {number} [maxMembers=Infinity] - Maximum members allowed
 * @property {Record<string, any>} [meta] - Initial group metadata
 * @property {number} [memberTtl=120] - Member entry TTL in seconds. Entries from crashed instances expire after this.
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
	const memberTtl = options.memberTtl ?? 120;
	const memberTtlMs = memberTtl * 1000;
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

	// Heartbeat: refresh timestamps on local member entries and
	// remove stale entries from crashed instances.
	const heartbeatTimer = setInterval(() => {
		const now = Date.now();
		for (const [, entry] of localMembers) {
			const memberData = JSON.stringify({ role: entry.role, instanceId, ts: now });
			redis.hset(membersKey, entry.memberId, memberData).catch(() => {});
		}
		// Clean up stale entries from dead instances so the hash
		// does not grow forever after crashes.
		redis.hgetall(membersKey).then((all) => {
			if (!all) return;
			for (const [field, v] of Object.entries(all)) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (now - parsed.ts) > memberTtlMs) {
						redis.hdel(membersKey, field).catch(() => {});
					}
				} catch {
					redis.hdel(membersKey, field).catch(() => {});
				}
			}
		}).catch(() => {});
	}, Math.max(memberTtlMs / 3, 5000));
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Subscriber for cross-instance events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let subscribedPlatform = null;

	async function ensureSubscriber(platform) {
		subscribedPlatform = platform;
		if (subscriber) return;
		subscriber = client.duplicate();
		subscriber.on('message', (ch, message) => {
			if (ch !== eventChannel) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (subscribedPlatform) {
					// Handle role-filtered events: deliver only to matching local members
					if (parsed.event === '__role_filtered') {
						const { event, data, role } = parsed.data;
						for (const [ws, entry] of localMembers) {
							if (entry.role === role) {
								subscribedPlatform.send(ws, internalTopic, event, data);
							}
						}
					} else if (parsed.event === 'close') {
						// Remote close: clean up local members just like a local close().
						// relay: false -- each worker has its own subscriber.
						subscribedPlatform.publish(internalTopic, 'close', parsed.data, { relay: false });
						for (const [ws] of localMembers) {
							ws.unsubscribe(internalTopic);
						}
						localMembers.clear();
						if (onClose) onClose();
					} else {
						// relay: false -- each worker has its own subscriber.
						subscribedPlatform.publish(internalTopic, parsed.event, parsed.data, { relay: false });
					}
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

			const memberId = instanceId + ':' + (++memberCounter);
			const now = Date.now();
			const memberData = JSON.stringify({ role, instanceId, ts: now });

			// Atomic capacity check + insert (skips stale entries from crashed instances)
			if (Number.isFinite(maxMembers)) {
				const result = await redis.eval(
					JOIN_SCRIPT, 1, membersKey,
					maxMembers, memberId, memberData, now, memberTtlMs
				);
				if (result === 0) {
					if (onFull) onFull(ws, role);
					return false;
				}
			} else {
				// No capacity limit, just insert
				await redis.hset(membersKey, memberId, memberData);
			}

			localMembers.set(ws, { role, memberId });

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
					// Filter out stale entries
					if (parsed.ts && (now - parsed.ts) > memberTtlMs) continue;
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
			// For remote instances, publish with role filter info
			// Remote subscriber handler will filter by role locally
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
			const all = await redis.hgetall(membersKey);
			if (!all) return 0;
			const now = Date.now();
			let liveCount = 0;
			for (const v of Object.values(all)) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (now - parsed.ts) > memberTtlMs) continue;
					liveCount++;
				} catch { /* skip corrupted */ }
			}
			return liveCount;
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
			clearInterval(heartbeatTimer);
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscribedPlatform = null;
		}
	};
}
