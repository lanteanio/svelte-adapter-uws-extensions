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
import { CLEANUP_SCRIPT, COUNT_SCRIPT } from '../shared/scripts.js';

const VALID_ROLES = new Set(['member', 'admin', 'viewer']);

/**
 * Lua script for atomic join: check closed flag, check capacity
 * (excluding stale entries), clean up stale entries, and insert
 * the new member in one roundtrip.
 *
 * KEYS[1] = members hash key
 * KEYS[2] = closed flag key
 * ARGV[1] = maxMembers
 * ARGV[2] = memberId
 * ARGV[3] = member data JSON
 * ARGV[4] = now (ms)
 * ARGV[5] = memberTtl (ms)
 *
 * Returns {-1} if closed, {0} if full, {1, ...live} on success.
 */
const JOIN_SCRIPT = `
local key = KEYS[1]
local closedFlag = KEYS[2]
local maxMembers = tonumber(ARGV[1])
local memberId = ARGV[2]
local memberData = ARGV[3]
local now = tonumber(ARGV[4])
local memberTtl = tonumber(ARGV[5])

if redis.call('get', closedFlag) == '1' then
  return {-1}
end

local all = redis.call('hgetall', key)
local liveCount = 0
local stale = {}
local live = {}
for i = 1, #all, 2 do
  local ok, val = pcall(cjson.decode, all[i+1])
  if ok and val.ts and (now - val.ts) <= memberTtl then
    liveCount = liveCount + 1
    live[#live + 1] = all[i+1]
  else
    stale[#stale + 1] = all[i]
  end
end
if #stale > 0 then
  redis.call('hdel', key, unpack(stale))
end

if liveCount >= maxMembers then
  return {0}
end
redis.call('hset', key, memberId, memberData)
live[#live + 1] = memberData
return {1, unpack(live)}
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
	if (typeof memberTtl !== 'number' || !Number.isFinite(memberTtl) || memberTtl < 1) {
		throw new Error('redis group: memberTtl must be a positive number (seconds)');
	}
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

	const b = options.breaker;
	const m = options.metrics;
	const mJoins = m?.counter('group_joins_total', 'Group join events', ['group']);
	const mRejected = m?.counter('group_joins_rejected_total', 'Group joins rejected (full)', ['group']);
	const mGroupLeaves = m?.counter('group_leaves_total', 'Group leave events', ['group']);
	const mPublishes = m?.counter('group_publishes_total', 'Group publish events', ['group']);

	const metaKey = client.key('group:' + name + ':meta');
	const membersKey = client.key('group:' + name + ':members');
	const closedKey = client.key('group:' + name + ':closed');
	const eventChannel = client.key('group:' + name + ':events');
	const internalTopic = '__group:' + name;

	// Local member tracking (ws objects on this instance)
	/** @type {Map<any, { role: GroupRole, memberId: string }>} */
	const localMembers = new Map();
	let memberCounter = 0;
	let isClosed = false;

	let pendingMeta = null;
	let metaInitError = null;
	if (options.meta) {
		const initialMeta = options.meta;
		pendingMeta = redis.hmset(metaKey, initialMeta)
			.then(() => { pendingMeta = null; })
			.catch((err) => {
				pendingMeta = null;
				metaInitError = err;
				console.warn('groups: initial meta write failed for "' + name + '":', err.message);
			});
	}

	// Heartbeat: refresh timestamps on local member entries and
	// remove stale entries from crashed instances.
	const heartbeatTimer = setInterval(() => {
		if (b && !b.isHealthy) return;
		const now = Date.now();
		const pipe = redis.pipeline();
		for (const [, entry] of localMembers) {
			const memberData = JSON.stringify({ role: entry.role, instanceId, ts: now });
			pipe.hset(membersKey, entry.memberId, memberData);
		}
		pipe.eval(CLEANUP_SCRIPT, 1, membersKey, now, memberTtlMs);
		pipe.exec().catch((err) => {
			if (err) console.warn('groups heartbeat: pipeline failed for group "' + name + '":', err.message);
		});
	}, memberTtlMs < 15000 ? Math.floor(memberTtlMs / 3) : 5000);
	if (heartbeatTimer.unref) heartbeatTimer.unref();

	// Subscriber for cross-instance events
	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let subscribedPlatform = null;

	async function ensureSubscriber(platform) {
		subscribedPlatform = platform;
		if (subscriber) return;
		const sub = client.duplicate({ enableReadyCheck: false });
		sub.on('error', (err) => {
			console.error('groups subscriber error:', err.message);
		});
		sub.on('message', (ch, message) => {
			if (ch !== eventChannel) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (subscribedPlatform) {
					if (parsed.event === '__role_filtered') {
						const { event, data, role } = parsed.data;
						for (const [ws, entry] of localMembers) {
							if (entry.role === role) {
								subscribedPlatform.send(ws, internalTopic, event, data);
							}
						}
					} else if (parsed.event === 'close') {
						isClosed = true;
						subscribedPlatform.publish(internalTopic, 'close', parsed.data, { relay: false });
						for (const [ws] of localMembers) {
							try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
						}
						localMembers.clear();
						if (onClose) onClose();
					} else {
						subscribedPlatform.publish(internalTopic, parsed.event, parsed.data, { relay: false });
					}
				}
			} catch {
				// Malformed, skip
			}
		});
		try {
			await sub.subscribe(eventChannel);
		} catch (err) {
			sub.quit().catch(() => sub.disconnect());
			throw err;
		}
		subscriber = sub;
	}

	async function publishEvent(event, data) {
		const msg = JSON.stringify({ instanceId, event, data });
		await redis.publish(eventChannel, msg);
	}

	/** @type {RedisGroup} */
	const group = {
		get name() { return name; },

		async getMeta() {
			if (b) b.guard();
			if (pendingMeta) await pendingMeta;
			if (metaInitError && options.meta) {
				try {
					await redis.hmset(metaKey, options.meta);
					metaInitError = null;
				} catch (err) {
					b?.failure(err);
					throw err;
				}
			}
			try {
				const raw = await redis.hgetall(metaKey);
				b?.success();
				return raw || {};
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async setMeta(meta) {
			if (b) b.guard();
			try {
				if (Object.keys(meta).length === 0) {
					await redis.del(metaKey);
				} else {
					await redis.hmset(metaKey, meta);
				}
				metaInitError = null;
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		async join(ws, platform, role = 'member') {
			if (isClosed) return false;

			// Idempotent
			if (localMembers.has(ws)) return true;

			if (!VALID_ROLES.has(role)) {
				throw new Error(`redis group "${name}": invalid role "${role}"`);
			}

			const memberId = instanceId + ':' + (++memberCounter);
			const now = Date.now();
			const memberData = JSON.stringify({ role, instanceId, ts: now });

			try {
				await ensureSubscriber(platform);
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			const effectiveMax = Number.isFinite(maxMembers) ? maxMembers : 999999999;
			b?.guard();
			let result;
			try {
				result = await redis.eval(
					JOIN_SCRIPT, 2, membersKey, closedKey,
					effectiveMax, memberId, memberData, now, memberTtlMs
				);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			if (result[0] === -1) {
				isClosed = true;
				return false;
			}
			if (result[0] === 0) {
				mRejected?.inc({ group: name });
				if (onFull) onFull(ws, role);
				return false;
			}

			localMembers.set(ws, { role, memberId });

			try {
				ws.subscribe(internalTopic);
			} catch {
				localMembers.delete(ws);
				try {
					await redis.hdel(membersKey, memberId);
				} catch (rollbackErr) {
					throw new Error('redis group "' + name + '": join rollback failed, orphaned member in Redis: ' + rollbackErr.message);
				}
				return false;
			}

			let freshAll;
			try {
				freshAll = await redis.hgetall(membersKey);
			} catch (err) {
				localMembers.delete(ws);
				try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
				try {
					await redis.hdel(membersKey, memberId);
				} catch (rollbackErr) {
					throw new Error('redis group "' + name + '": join rollback failed, orphaned member in Redis: ' + rollbackErr.message);
				}
				throw err;
			}

			// Publish join event only after the snapshot succeeded.
			// This prevents orphaned join events when the snapshot step
			// fails, eliminating the need for compensating leave events.
			platform.publish(internalTopic, 'join', { role });
			await publishEvent('join', { role }).catch(() => {});

			const freshNow = Date.now();
			const membersList = [];
			for (const [, v] of Object.entries(freshAll)) {
				try {
					const parsed = JSON.parse(v);
					if (parsed.ts && (freshNow - parsed.ts) <= memberTtlMs) {
						membersList.push({ role: parsed.role });
					}
				} catch { /* skip corrupted */ }
			}
			try {
				platform.send(ws, internalTopic, 'members', membersList);
			} catch {
				// ws closed after subscribe
			}

			mJoins?.inc({ group: name });
			if (onJoin) onJoin(ws, role);
			return true;
		},

		async leave(ws, platform) {
			const entry = localMembers.get(ws);
			if (!entry) return;

			let skipHdel = false;
			if (b) { try { b.guard(); } catch { skipHdel = true; } }

			if (skipHdel) {
				return;
			}

			try {
				await redis.hdel(membersKey, entry.memberId);
				b?.success();
			} catch (err) {
				b?.failure(err);
				return;
			}

			localMembers.delete(ws);
			try { ws.unsubscribe(internalTopic); } catch { /* closed */ }

			mGroupLeaves?.inc({ group: name });
			const leavePayload = { role: entry.role };
			platform.publish(internalTopic, 'leave', leavePayload);
			await publishEvent('leave', leavePayload).catch(() => {});

			if (onLeave) onLeave(ws, entry.role);
		},

		async publish(platform, event, data, role) {
			if (isClosed) return;
			mPublishes?.inc({ group: name });

			if (role == null) {
				// Broadcast to all via topic
				platform.publish(internalTopic, event, data);
				await publishEvent(event, data).catch(() => {});
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
			await publishEvent('__role_filtered', { event, data, role }).catch(() => {});
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
			if (b) b.guard();
			const now = Date.now();
			try {
				const result = await redis.eval(COUNT_SCRIPT, 1, membersKey, now, memberTtlMs);
				b?.success();
				return result;
			} catch (err) {
				b?.failure(err);
				throw err;
			}
		},

		has(ws) {
			return localMembers.has(ws);
		},

		async close(platform) {
			b?.guard();
			try {
				const alreadyClosed = await redis.get(closedKey);
				if (alreadyClosed === '1') {
					isClosed = true;
					platform.publish(internalTopic, 'close', null);
					await publishEvent('close', null);
					for (const [ws] of localMembers) {
						try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
					}
					localMembers.clear();
					await redis.del(membersKey);
					b?.success();
					if (onClose) onClose();
					return;
				}

				await redis.set(closedKey, '1');
				isClosed = true;

				platform.publish(internalTopic, 'close', null);
				await publishEvent('close', null);

				for (const [ws] of localMembers) {
					try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
				}
				localMembers.clear();

				await redis.del(membersKey);
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			if (onClose) onClose();
		},

		destroy() {
			clearInterval(heartbeatTimer);
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			subscribedPlatform = null;
		},

		hooks: {
			async subscribe(ws, topic, { platform }) {
				if (topic === internalTopic) {
					await group.join(ws, platform);
				}
			},
			async unsubscribe(ws, topic, { platform }) {
				if (topic === internalTopic) {
					await group.leave(ws, platform);
				}
			},
			async close(ws, { platform }) {
				await group.leave(ws, platform);
			}
		}
	};

	return group;
}
