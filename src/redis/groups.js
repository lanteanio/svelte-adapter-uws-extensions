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
 * Storage layout (the group name is wrapped in braces so all keys for one
 * group share a Redis hash tag and co-locate on a single cluster slot):
 *   - Key `{prefix}group:{name}:meta`     - hash (group metadata)
 *   - Key `{prefix}group:{name}:members`  - hash (field=memberId, value=JSON {role, instanceId, ts})
 *   - Key `{prefix}group:{name}:closed`   - string flag ("1" if closed)
 *   - Channel `{prefix}group:{name}:events` - pub/sub for cross-instance events
 *
 * @module svelte-adapter-uws-extensions/redis/groups
 */

import { randomBytes, now, setIntervalTimer, clearIntervalTimer } from '../shared/runtime.js';
import { evalCached, evalCachedName } from '../shared/eval-cached.js';
import { CLEANUP_SCRIPT, COUNT_SCRIPT } from '../shared/scripts.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_GROUPS_LOCAL_MEMBERS } from '../shared/caps.js';
import { addWsSubscription, removeWsSubscription } from '../shared/ws-subscriptions.js';
import { createBusValidator } from '../shared/bus-validate.js';

const VALID_ROLES = new Set(['member', 'admin', 'viewer']);

/** Wire-protocol event names this module emits. */
const EVENTS = Object.freeze({
	JOIN: 'join',
	LEAVE: 'leave',
	CLOSE: 'close',
	MEMBERS: 'members',
	ROLE_FILTERED: '__role_filtered'
});

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
if maxMembers == nil or now == nil or memberTtl == nil then
  return redis.error_reply('JOIN: maxMembers/now/memberTtl must be numeric')
end

if redis.call('get', closedFlag) == '1' then
  return {-1}
end

local all = redis.call('hgetall', key)
local liveCount = 0
local stale = {}
local live = {}
for i = 1, #all, 2 do
  local ok, val = pcall(cjson.decode, all[i+1])
  local ts = ok and val and tonumber(val.ts) or nil
  if ts and (now - ts) <= memberTtl then
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
 * @property {number} [maxEnvelopeBytes=1048576] - Reject inbound bus envelopes larger than this many bytes BEFORE `JSON.parse` runs, and refuse to publish outbound events past the same bound (every peer would drop them on receipt). Defends against bus-side DoS in shared-Redis deployments.
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
	// Key-level TTL margin for the members and meta hashes so a fully-abandoned
	// group (every instance crashed or destroyed) self-expires instead of
	// leaking its member roster forever. The heartbeat refreshes this while any
	// instance holds the group open; it comfortably exceeds the heartbeat
	// cadence (memberTtlMs/3 or 5s), so a live group is never reaped underneath
	// itself. The `closed` flag is deliberately NOT expired - closing a group is
	// a terminal decision and its tombstone must outlive the roster.
	const keyExpiryMs = 2 * memberTtlMs;
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

	const metaKey = client.key('group:{' + name + '}:meta');
	const membersKey = client.key('group:{' + name + '}:members');
	const closedKey = client.key('group:{' + name + '}:closed');
	const eventChannel = client.key('group:{' + name + '}:events');
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
	// remove stale entries from crashed instances. The cleanup script rides the
	// pipeline as a CACHED command (registered before the first pipeline is
	// built, so every batch carries the SHA-backed name, not the script body -
	// this was the one remaining per-call full-script eval on a hot cadence).
	const cleanupCmd = evalCachedName(redis, CLEANUP_SCRIPT);
	const heartbeatTimer = setIntervalTimer(() => {
		if (b && !b.isHealthy) return;
		// Retry a close whose authoritative flag was unreadable when its bus
		// event arrived, so a Redis blip delays the local close instead of
		// losing it (this instance would otherwise keep its members
		// subscribed and keep re-adding them to the roster below).
		if (closeVerifyPending && subscribedPlatform) {
			const platform = subscribedPlatform;
			const data = pendingCloseData;
			eventChain = eventChain
				.then(() => verifyAndLatchClose(platform, data))
				.catch(() => { /* retried on the next beat */ });
		}
		const nowTs = now();
		const pipe = redis.pipeline();
		for (const [, entry] of localMembers) {
			const memberData = JSON.stringify({ role: entry.role, instanceId, ts: nowTs });
			pipe.hset(membersKey, entry.memberId, memberData);
		}
		pipe[cleanupCmd](1, membersKey, nowTs, memberTtlMs);
		// Refresh the key-level TTL so the roster and meta persist while this
		// instance holds the group open, and expire once every instance stops.
		pipe.pexpire(membersKey, keyExpiryMs);
		pipe.pexpire(metaKey, keyExpiryMs);
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

	// Inbound bus guard: the event channel accepts messages from a shared
	// transport, so cap raw bytes before JSON.parse like every sibling bus
	// module.
	const busValidator = createBusValidator({ label: 'redis group', maxBytes: options.maxEnvelopeBytes });
	const maxEnvelopeBytes = busValidator.maxBytes;

	// Inbound events are applied strictly in arrival order through this
	// chain. CLOSE must consult Redis before it may latch, and without the
	// chain that await would let events published after the close overtake
	// it and reach members the close had already removed.
	let eventChain = Promise.resolve();

	// Set when a CLOSE arrived but its authoritative flag could not be read.
	// Latching an unverifiable close would let a forged bus event brick the
	// group; dropping it would lose a real close forever. So it is held and
	// retried on the heartbeat until Redis answers one way or the other.
	let closeVerifyPending = false;
	let pendingCloseData = null;

	// Set by destroy(). Every await in the close path is a window in which the
	// group can be torn down underneath it, and the platform reference these
	// handlers carry was captured before that happened.
	let destroyed = false;

	// CLOSE coalescing state (see verifyCloseCoalesced). `closeQueued` means a
	// verification is on the chain or running; `closeVerifyStarted` means it
	// has begun its read, so a newly arrived CLOSE needs a follow-up rather
	// than being folded into it.
	let closeQueued = false;
	let closeVerifyStarted = false;
	let closeArrivedDuringVerify = false;
	let coalescedCloseData = null;

	function applyClose(platform, data) {
		// `platform` was captured at dispatch, so nulling `subscribedPlatform`
		// in destroy() does not reach it - an event already on the chain, or
		// one sitting behind the flag read below, still arrives here with a
		// live reference and publishes to a torn-down group. The old handler
		// was synchronous and could not be caught out this way; making the
		// close path async is what opened it.
		if (destroyed) return;
		isClosed = true;
		closeVerifyPending = false;
		try {
			platform?.publish(internalTopic, EVENTS.CLOSE, data, { relay: false });
		} catch { /* platform torn down mid-close */ }
		for (const [ws] of localMembers) {
			try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
			removeWsSubscription(ws, internalTopic);
		}
		localMembers.clear();
		if (onClose) onClose();
	}

	/**
	 * CLOSE is authoritative in Redis, not on the bus. The closing instance
	 * sets the flag BEFORE it publishes, so a real close verifies and a
	 * bus-only forgery finds the flag unset and is dropped.
	 * @param {any} platform
	 * @param {any} data
	 */
	async function verifyAndLatchClose(platform, data) {
		if (destroyed) return;
		// The queued CLOSE is now reading, so a CLOSE arriving from here on
		// cannot be folded into it - see the coalescing rule in the subscriber.
		closeVerifyStarted = true;
		if (isClosed) {
			// Already closed by another route (a local close, or a join that
			// found the flag set). Clear the pending retry, or the heartbeat
			// re-enqueues this no-op onto the event chain forever and pins
			// the payload it captured.
			closeVerifyPending = false;
			pendingCloseData = null;
			return;
		}
		let flag;
		try {
			flag = await redis.get(closedKey);
		} catch {
			// Unverifiable: hold it for the heartbeat to retry.
			closeVerifyPending = true;
			pendingCloseData = data;
			return;
		}
		// Re-checked AFTER the await: destroy() can land while this read is in
		// flight, and everything below it publishes or invokes onClose.
		if (destroyed) return;
		closeVerifyPending = false;
		if (flag !== '1' || isClosed) return;
		applyClose(platform, data);
	}

	/**
	 * Verify a CLOSE, then honour any CLOSE that arrived while it ran.
	 *
	 * Every inbound CLOSE used to cost its own `GET` on the serialized event
	 * chain, so a burst of forged ones held the chain for a round trip each -
	 * 50 of them delayed a legitimate event by 3.07s. They cannot simply be
	 * dropped: the flag is authoritative and a real close published during the
	 * burst has to latch. Coalescing to at most one FOLLOW-UP verification is
	 * what makes both true. The follow-up's read happens strictly after the
	 * arrival of every CLOSE it stands in for, so it observes at least what
	 * each of them would have, and a burst of any size costs two reads rather
	 * than N.
	 * @param {any} platform
	 * @param {any} data
	 */
	async function verifyCloseCoalesced(platform, data) {
		await verifyAndLatchClose(platform, data);
		while (closeArrivedDuringVerify && !isClosed && !destroyed) {
			closeArrivedDuringVerify = false;
			const followUp = coalescedCloseData;
			closeVerifyStarted = false;
			await verifyAndLatchClose(platform, followUp);
		}
		closeVerifyStarted = false;
		closeQueued = false;
	}

	/**
	 * @param {any} platform - Captured at dispatch: `subscribedPlatform` can
	 *   be nulled by destroy() while this sits behind an await.
	 * @param {any} parsed
	 */
	async function applyEvent(platform, parsed) {
		// Events queued before destroy() are still on the chain, holding the
		// platform they captured.
		if (destroyed) return;
		if (parsed.event === EVENTS.ROLE_FILTERED) {
			const { event, data, role } = parsed.data ?? {};
			for (const [ws, entry] of localMembers) {
				if (entry.role === role) platform.send(ws, internalTopic, event, data);
			}
			return;
		}
		if (parsed.event === EVENTS.CLOSE) {
			await verifyCloseCoalesced(platform, parsed.data);
			return;
		}
		platform.publish(internalTopic, parsed.event, parsed.data, { relay: false });
	}

	async function ensureSubscriber(platform) {
		subscribedPlatform = platform;
		if (subscriber) return;
		const sub = client.duplicate({ enableReadyCheck: false });
		sub.on('error', (err) => {
			console.error('groups subscriber error:', err.message);
		});
		sub.on('message', (ch, message) => {
			if (ch !== eventChannel) return;
			if (!busValidator.acceptRaw(message)) return;
			let parsed;
			try {
				parsed = JSON.parse(message);
			} catch {
				return; // malformed
			}
			if (!parsed || typeof parsed !== 'object' || typeof parsed.event !== 'string') return;
			if (parsed.instanceId === instanceId) return;
			if (destroyed) return;
			const platform = subscribedPlatform;
			if (!platform) return;
			if (parsed.event === EVENTS.CLOSE) {
				// Fold into the verification already queued or running rather
				// than adding another round trip to the chain. If it has not
				// started reading, its own read still happens after this
				// message arrived and covers it; if it has, the loop in
				// verifyCloseCoalesced schedules exactly one follow-up.
				if (closeQueued) {
					coalescedCloseData = parsed.data;
					if (closeVerifyStarted) closeArrivedDuringVerify = true;
					return;
				}
				closeQueued = true;
				closeVerifyStarted = false;
				closeArrivedDuringVerify = false;
			}
			eventChain = eventChain
				.then(() => applyEvent(platform, parsed))
				.catch(() => { /* one bad event must not break the chain */ });
		});
		try {
			await sub.subscribe(eventChannel);
		} catch (err) {
			sub.quit().catch(() => sub.disconnect());
			throw err;
		}
		subscriber = sub;
	}

	/**
	 * Hold outbound events to the bound every peer enforces on the way in.
	 * Called BEFORE any local fan-out, so an oversized payload fails the
	 * caller outright instead of reaching local members and silently no
	 * remote instance - a split-brain the sender is the only side that can
	 * detect.
	 * @param {string} event
	 * @param {any} data
	 * @returns {string} The encoded envelope.
	 */
	function encodeEvent(event, data) {
		const msg = JSON.stringify({ instanceId, event, data });
		if (Buffer.byteLength(msg) > maxEnvelopeBytes) {
			throw new Error(
				`groups: "${event}" envelope exceeds maxEnvelopeBytes (${maxEnvelopeBytes} bytes); ` +
				'every peer instance would drop it on receipt'
			);
		}
		return msg;
	}

	/** Publish an envelope already encoded (and therefore already bounded). */
	async function publishRaw(raw) {
		await redis.publish(eventChannel, raw);
	}

	async function publishEvent(event, data) {
		await publishRaw(encodeEvent(event, data));
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
			await withBreaker(b, () =>
				Object.keys(meta).length === 0 ? redis.del(metaKey) : redis.hmset(metaKey, meta)
			);
			metaInitError = null;
		},

		async join(ws, platform, role = 'member') {
			if (isClosed) return false;

			// Idempotent
			if (localMembers.has(ws)) return true;

			if (!VALID_ROLES.has(role)) {
				throw new Error(`redis group "${name}": invalid role "${role}"`);
			}

			const memberId = instanceId + ':' + (++memberCounter);
			const nowTs = now();
			const memberData = JSON.stringify({ role, instanceId, ts: nowTs });

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
				result = await evalCached(redis, 
					JOIN_SCRIPT, 2, membersKey, closedKey,
					effectiveMax, memberId, memberData, nowTs, memberTtlMs
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

			// Stamp the key-level TTL at join time so a member that joins and
			// then crashes before the first heartbeat still expires. Best-effort:
			// the heartbeat re-applies it while the group stays open.
			try {
				const ttlPipe = redis.pipeline();
				ttlPipe.pexpire(membersKey, keyExpiryMs);
				if (options.meta) ttlPipe.pexpire(metaKey, keyExpiryMs);
				await ttlPipe.exec();
			} catch { /* best-effort; the heartbeat re-applies the TTL */ }

			// Per-instance cap on local member count. Treat saturation
			// the same as a "group full" rejection - the caller already
			// has graceful handling for that path.
			if (!localMembers.has(ws) && localMembers.size >= MAX_GROUPS_LOCAL_MEMBERS) {
				mRejected?.inc({ group: name });
				if (onFull) onFull(ws, role);
				try { await redis.hdel(membersKey, memberId); } catch { /* ignore */ }
				return false;
			}

			localMembers.set(ws, { role, memberId });

			try {
				ws.subscribe(internalTopic);
				addWsSubscription(ws, internalTopic);
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
				removeWsSubscription(ws, internalTopic);
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
			platform.publish(internalTopic, EVENTS.JOIN, { role });
			await publishEvent(EVENTS.JOIN, { role }).catch(() => {});

			const freshNow = now();
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
				platform.send(ws, internalTopic, EVENTS.MEMBERS, membersList);
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
			removeWsSubscription(ws, internalTopic);

			mGroupLeaves?.inc({ group: name });
			const leavePayload = { role: entry.role };
			platform.publish(internalTopic, EVENTS.LEAVE, leavePayload);
			await publishEvent(EVENTS.LEAVE, leavePayload).catch(() => {});

			if (onLeave) onLeave(ws, entry.role);
		},

		async publish(platform, event, data, role) {
			if (isClosed) return;
			// Encode the envelope that will ACTUALLY be published, before the
			// local fan-out below. Checking a differently-shaped envelope
			// would let the role-filtered wrapper (which is ~50 bytes larger)
			// pass the check and then be dropped by every peer, delivering
			// locally and nowhere else with the caller told nothing.
			const raw = role == null
				? encodeEvent(event, data)
				: encodeEvent(EVENTS.ROLE_FILTERED, { event, data, role });
			mPublishes?.inc({ group: name });

			if (role == null) {
				// Broadcast to all via topic
				platform.publish(internalTopic, event, data);
				await publishRaw(raw).catch(() => {});
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
			await publishRaw(raw).catch(() => {});
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
			const nowTs = now();
			return withBreaker(b, () => evalCached(redis, COUNT_SCRIPT, 1, membersKey, nowTs, memberTtlMs));
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
					platform.publish(internalTopic, EVENTS.CLOSE, null);
					await publishEvent(EVENTS.CLOSE, null);
					for (const [ws] of localMembers) {
						try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
						removeWsSubscription(ws, internalTopic);
					}
					localMembers.clear();
					await redis.del(membersKey);
					b?.success();
					if (onClose) onClose();
					return;
				}

				await redis.set(closedKey, '1');
				isClosed = true;

				platform.publish(internalTopic, EVENTS.CLOSE, null);
				await publishEvent(EVENTS.CLOSE, null);

				for (const [ws] of localMembers) {
					try { ws.unsubscribe(internalTopic); } catch { /* closed */ }
					removeWsSubscription(ws, internalTopic);
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
			// Set BEFORE tearing anything down, so an event already on the
			// chain - or one behind the authoritative flag read - sees it and
			// declines to publish or fire onClose against a dead group.
			destroyed = true;
			closeVerifyPending = false;
			pendingCloseData = null;
			clearIntervalTimer(heartbeatTimer);
			if (subscriber) {
				const sub = subscriber;
				subscriber = null;
				sub.quit().catch(() => sub.disconnect());
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
