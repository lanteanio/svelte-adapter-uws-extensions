/**
 * Cluster-wide connection registry + push-with-reply for svelte-adapter-uws.
 *
 * Tracks `userId -> {instanceId, sessionId, ts}` in Redis so any instance
 * can route a request to whichever instance currently owns a given user's
 * WebSocket connection. Wraps the adapter's `platform.request(ws, ...)`
 * primitive with cluster routing: the origin instance looks up the owning
 * instance, forwards the request envelope on the per-instance push channel,
 * and waits for the reply envelope on its own push channel.
 *
 * Storage layout:
 *   - Hash `{prefix}conns:{userId}` with fields `instanceId`, `sessionId`, `ts`.
 *     Most-recent-connection-wins -- a second device replaces the first.
 *     Sliding TTL refreshed on every `hooks.open` and on the heartbeat tick.
 *
 * Wire envelopes on `{prefix}__push:{instanceId}`:
 *   - `{type:'request', ref, sessionId, event, data, replyTo, timeoutMs}`
 *   - `{type:'reply', ref, data}` or `{type:'reply', ref, error}`
 *
 * Self-targeting (the origin instance owns the user) short-circuits to a
 * local `platform.request(ws, ...)` without round-tripping Redis.
 *
 * @module svelte-adapter-uws-extensions/redis/registry
 */

import { randomBytes } from 'node:crypto';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { now as cachedNow } from '../shared/time.js';

/**
 * Lua-atomic compare-and-delete: only removes the registry entry if the
 * stored `instanceId` field still matches this instance. Prevents a close
 * hook from clobbering a registration that already migrated to another
 * instance via a fast laptop-then-phone reconnect.
 */
const COMPARE_AND_DELETE = `
local key = KEYS[1]
local ours = ARGV[1]
local current = redis.call('hget', key, 'instanceId')
if current == ours then
  redis.call('unlink', key)
  return 1
end
return 0
`;

/**
 * @typedef {Object} RegistryOptions
 * @property {(ws: any) => string | null | undefined} identify - Extract the user identity from a WebSocket. Return `null` / `undefined` for anonymous connections; the registry will skip them.
 * @property {(ws: any) => Record<string, string | number | boolean> | null | undefined} [attributes] - Extract per-user attributes captured at registration time. Used by `sendTo(criteria, ...)` for tenant- / role- / cohort-scoped broadcasts. Shallow values only (string / number / boolean); compound queries are deliberately out of scope.
 * @property {string} [keyPrefix=''] - Prefix prepended to all registry keys and channels. Stacks with the underlying client's `keyPrefix`.
 * @property {number} [ttl=90] - Expiry on registry entries in seconds. Should be greater than `heartbeat * 3` so a missed beat doesn't drop a live user.
 * @property {number} [heartbeat=30000] - Refresh interval in ms.
 * @property {number} [requestTimeoutMs=5000] - Default timeout for `request(...)`.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker for Redis operations.
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics] - Optional Prometheus metrics registry.
 */

/**
 * @typedef {Object} RegistryEntry
 * @property {string} instanceId
 * @property {string} sessionId
 * @property {number} ts
 */

/**
 * @typedef {Object} ConnectionRegistry
 * @property {string} instanceId - Stable id for this instance, also the name of its push channel.
 * @property {(userId: string) => Promise<RegistryEntry | null>} lookup - Resolve a userId to its current owning instance, or null if offline.
 * @property {<T = unknown>(target: string, event: string, data?: unknown, options?: { timeoutMs?: number }) => Promise<T>} request - Cluster-routed request/reply. Resolves with the reply, rejects on timeout / offline / handler error.
 * @property {() => number} size - Count of users registered to THIS instance (local view, scrape-time).
 * @property {{ open: (ws: any, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void>, close: (ws: any, ctx: { platform: import('svelte-adapter-uws').Platform }) => Promise<void> }} hooks
 * @property {() => Promise<void>} destroy - Stop the heartbeat and Redis subscriber.
 */

const DEFAULT_TTL_SEC = 90;
const DEFAULT_HEARTBEAT_MS = 30000;
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;

/**
 * Create a cluster-aware connection registry.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RegistryOptions} options
 * @returns {ConnectionRegistry}
 *
 * @example
 * ```js
 * import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
 * import { createConnectionRegistry } from 'svelte-adapter-uws-extensions/redis/registry';
 *
 * const redis = createRedisClient({ url: 'redis://localhost:6379' });
 * const registry = createConnectionRegistry(redis, {
 *   identify: (ws) => ws.getUserData()?.userId
 * });
 *
 * // hooks.ws.js
 * export const open = registry.hooks.open;
 * export const close = registry.hooks.close;
 *
 * // From any instance:
 * const reply = await registry.request('user-123', 'confirm-action', { op: 'delete' });
 * ```
 */
export function createConnectionRegistry(client, options) {
	if (!options || typeof options !== 'object') {
		throw new Error('registry: options object is required');
	}
	const identify = options.identify;
	if (typeof identify !== 'function') {
		throw new Error('registry: identify must be a function');
	}
	const attributes = options.attributes;
	if (attributes !== undefined && typeof attributes !== 'function') {
		throw new Error('registry: attributes must be a function returning a flat object of string|number|boolean');
	}
	const keyPrefix = options.keyPrefix == null ? '' : String(options.keyPrefix);
	const ttl = options.ttl ?? DEFAULT_TTL_SEC;
	if (typeof ttl !== 'number' || !Number.isFinite(ttl) || ttl < 1) {
		throw new Error('registry: ttl must be a positive number (seconds)');
	}
	const heartbeatInterval = options.heartbeat ?? DEFAULT_HEARTBEAT_MS;
	if (typeof heartbeatInterval !== 'number' || !Number.isFinite(heartbeatInterval) || heartbeatInterval < 1) {
		throw new Error('registry: heartbeat must be a positive number (ms)');
	}
	const defaultRequestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
	if (typeof defaultRequestTimeoutMs !== 'number' || !Number.isFinite(defaultRequestTimeoutMs) || defaultRequestTimeoutMs < 1) {
		throw new Error('registry: requestTimeoutMs must be a positive number (ms)');
	}

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const breaker = options.breaker;

	const m = options.metrics;
	const mRequests = m?.counter('push_requests_total', 'Cluster requests by outcome', ['result']);
	const mReplyLatency = m?.histogram(
		'push_reply_latency_ms',
		'Latency from request publish to reply receive (ms)'
	);
	const mRegistrySize = m?.gauge('push_registry_size', 'Connections registered to this instance');
	const mLateReply = m?.counter('push_late_replies_total', 'Replies that arrived after their request expired or migrated');
	const mCoalesced = m?.counter('push_coalesced_total', 'Cluster coalesced sends by outcome', ['result']);
	const mSends = m?.counter('push_sends_total', 'Cluster sends by outcome', ['result']);
	const mSendTo = m?.counter('push_sendto_total', 'Cluster attribute-targeted broadcasts by outcome', ['result']);

	function userKey(userId) {
		return client.key(keyPrefix + 'conns:' + userId);
	}
	function pushChannel(targetInstanceId) {
		return client.key(keyPrefix + '__push:' + targetInstanceId);
	}
	function userKeyPattern() {
		return client.key(keyPrefix + 'conns:*');
	}
	const ownPushChannel = pushChannel(instanceId);
	const eventsChannel = client.key(keyPrefix + '__registry-events');

	/**
	 * Local sessionId -> ws map. Used by the receive path to resolve which
	 * connection a routed request targets without scanning every ws.
	 * @type {Map<string, any>}
	 */
	const sessionToWs = new Map();

	/**
	 * Local userId -> sessionId map for the heartbeat refresh + close hook.
	 * @type {Map<string, string>}
	 */
	const localUsers = new Map();

	/**
	 * In-flight outbound requests: ref -> { resolve, reject, timer, startTime }.
	 * Keyed by per-request UUID. Cleared on reply, timeout, or destroy.
	 * @type {Map<string, { resolve: (value: any) => void, reject: (err: Error) => void, timer: any, startTime: number }>}
	 */
	const pending = new Map();

	/**
	 * Cluster-wide userId -> instanceId map, populated by the events
	 * subscriber. Sender uses this to group `sendTo` matches by their
	 * owning instance.
	 * @type {Map<string, string>}
	 */
	const userToInstance = new Map();

	/**
	 * Shadow per-userId attribute snapshot used to keep the secondary
	 * index consistent across re-registrations and close events.
	 * @type {Map<string, Record<string, string>>}
	 */
	const userIdAttrs = new Map();

	/**
	 * Secondary index for `sendTo` lookups. Each attribute key has a
	 * value-bucketed map of userId sets. Shallow equality only.
	 * @type {Map<string, Map<string, Set<string>>>}
	 */
	const secondaryIndex = new Map();

	function indexUser(userId, attrs) {
		if (!attrs) return;
		for (const [k, v] of Object.entries(attrs)) {
			let byValue = secondaryIndex.get(k);
			if (!byValue) {
				byValue = new Map();
				secondaryIndex.set(k, byValue);
			}
			let users = byValue.get(v);
			if (!users) {
				users = new Set();
				byValue.set(v, users);
			}
			users.add(userId);
		}
	}

	function unindexUser(userId, attrs) {
		if (!attrs) return;
		for (const [k, v] of Object.entries(attrs)) {
			const byValue = secondaryIndex.get(k);
			if (!byValue) continue;
			const users = byValue.get(v);
			if (!users) continue;
			users.delete(userId);
			if (users.size === 0) byValue.delete(v);
			if (byValue.size === 0) secondaryIndex.delete(k);
		}
	}

	function applyOpenEvent(userId, ownerInstanceId, attrs) {
		const prevAttrs = userIdAttrs.get(userId);
		if (prevAttrs) unindexUser(userId, prevAttrs);
		userToInstance.set(userId, ownerInstanceId);
		userIdAttrs.set(userId, attrs || {});
		indexUser(userId, attrs);
	}

	function applyCloseEvent(userId, ownerInstanceId) {
		// Only remove if the close came from the recorded owner. A stale
		// close from a previous owner (after the user already migrated to a
		// different instance) must not clear a live registration.
		if (userToInstance.get(userId) !== ownerInstanceId) return;
		const prevAttrs = userIdAttrs.get(userId);
		if (prevAttrs) unindexUser(userId, prevAttrs);
		userIdAttrs.delete(userId);
		userToInstance.delete(userId);
	}

	/**
	 * Coerce attribute values to strings for index-key consistency. Numbers
	 * and booleans round-trip via `String()`; objects/arrays/null are
	 * dropped (shallow values only per credo rule 1).
	 *
	 * @param {Record<string, unknown> | null | undefined} raw
	 * @returns {Record<string, string>}
	 */
	function normalizeAttrs(raw) {
		if (!raw || typeof raw !== 'object') return {};
		const out = {};
		for (const [k, v] of Object.entries(raw)) {
			if (typeof k !== 'string' || k.length === 0) continue;
			if (v === null || v === undefined) continue;
			const t = typeof v;
			if (t !== 'string' && t !== 'number' && t !== 'boolean') continue;
			out[k] = String(v);
		}
		return out;
	}

	let bootstrapped = false;

	if (mRegistrySize) {
		mRegistrySize.collect(() => mRegistrySize.set(localUsers.size));
	}

	let activePlatform = null;
	let subscriber = null;
	let heartbeatTimer = null;
	let destroyed = false;

	function withBreakerGuard() {
		if (!breaker) return true;
		try { breaker.guard(); return true; } catch { return false; }
	}

	async function setEntry(userId, sessionId, attrs) {
		const key = userKey(userId);
		const now = cachedNow();
		try {
			if (attrs && Object.keys(attrs).length > 0) {
				await redis.hset(
					key,
					'instanceId', instanceId,
					'sessionId', sessionId,
					'ts', now,
					'attrs', JSON.stringify(attrs)
				);
			} else {
				await redis.hset(key, 'instanceId', instanceId, 'sessionId', sessionId, 'ts', now);
				// Best-effort clear: a previous registration may have left an
				// attrs field; HDEL is fire-and-forget so a missing field is
				// not an error.
				try { await redis.hdel(key, 'attrs'); } catch { /* ignore */ }
			}
			await redis.expire(key, ttl);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	function parseAttrsField(raw) {
		if (typeof raw !== 'string' || raw.length === 0) return {};
		try {
			const parsed = JSON.parse(raw);
			return normalizeAttrs(parsed);
		} catch {
			return {};
		}
	}

	async function publishEvent(envelope) {
		try {
			await redis.publish(eventsChannel, JSON.stringify(envelope));
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			// Best-effort: a missed event leaves the cluster's index slightly
			// stale until the next refresh / reconnect. The Redis hash stays
			// authoritative.
		}
	}

	async function deleteIfOurs(userId) {
		const key = userKey(userId);
		try {
			await redis.eval(COMPARE_AND_DELETE, 1, key, instanceId);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			// Best-effort; sliding TTL backstops a missed delete.
		}
	}

	async function refreshTtl(userId) {
		const key = userKey(userId);
		try {
			await redis.expire(key, ttl);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
		}
	}

	async function lookup(userId) {
		if (!withBreakerGuard()) return null;
		try {
			const result = await redis.hgetall(userKey(userId));
			breaker?.success();
			if (!result || !result.instanceId) return null;
			return {
				instanceId: result.instanceId,
				sessionId: result.sessionId || '',
				ts: Number(result.ts) || 0,
				attrs: parseAttrsField(result.attrs)
			};
		} catch (err) {
			breaker?.failure(err);
			return null;
		}
	}

	async function ensureSubscriber(platform) {
		activePlatform = platform || activePlatform;
		if (subscriber) return;
		subscriber = client.duplicate({ enableReadyCheck: false });
		subscriber.on('error', (err) => {
			console.error('[redis/registry] subscriber error:', err.message);
		});
		subscriber.on('message', (ch, raw) => {
			if (ch === ownPushChannel) {
				let envelope;
				try { envelope = JSON.parse(raw); } catch { return; }
				handleInbound(envelope);
				return;
			}
			if (ch === eventsChannel) {
				let envelope;
				try { envelope = JSON.parse(raw); } catch { return; }
				handleRegistryEvent(envelope);
			}
		});
		await subscriber.subscribe(ownPushChannel, eventsChannel);

		// Bootstrap the secondary index from existing entries. Subscribe
		// first (so we don't miss live events fired during the SCAN), then
		// SCAN; live events that race with the SCAN are idempotent under
		// set semantics. Skipped when no `attributes` option was supplied
		// since neither the index nor `sendTo` would have anything to do.
		if (attributes && !bootstrapped) {
			bootstrapped = true;
			bootstrapIndex().catch(() => {
				// Bootstrap is best-effort; live events fill in over time
				// even without the initial SCAN.
				bootstrapped = false;
			});
		}

		if (!heartbeatTimer) {
			heartbeatTimer = setInterval(heartbeatTick, heartbeatInterval);
			if (heartbeatTimer.unref) heartbeatTimer.unref();
		}
	}

	async function bootstrapIndex() {
		if (!withBreakerGuard()) return;
		const pattern = userKeyPattern();
		let cursor = '0';
		const seen = new Set();
		do {
			let res;
			try {
				res = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				return;
			}
			cursor = res[0];
			const keys = res[1] || [];
			for (const k of keys) {
				if (seen.has(k)) continue;
				seen.add(k);
				let entry;
				try {
					entry = await redis.hgetall(k);
					breaker?.success();
				} catch (err) {
					breaker?.failure(err);
					continue;
				}
				if (!entry || !entry.instanceId) continue;
				// Strip the prefix back off to recover the userId. Both
				// keyPrefix (registry-level) and the client's own keyPrefix
				// stack on the front of the key.
				const userId = userIdFromKey(k);
				if (!userId) continue;
				const attrs = parseAttrsField(entry.attrs);
				applyOpenEvent(userId, entry.instanceId, attrs);
			}
		} while (cursor !== '0');
	}

	function userIdFromKey(fullKey) {
		// Recover the userId by stripping the longest common prefix the
		// registry knows about. The full key is `client.keyPrefix +
		// keyPrefix + 'conns:' + userId`. `client.key(...)` builds it; we
		// invert by matching the same prefix back off.
		const builtPrefix = client.key(keyPrefix + 'conns:');
		if (fullKey.startsWith(builtPrefix)) return fullKey.slice(builtPrefix.length);
		return null;
	}

	function handleRegistryEvent(envelope) {
		if (!envelope || typeof envelope !== 'object') return;
		const { type, userId, instanceId: ownerInstanceId } = envelope;
		if (typeof userId !== 'string' || typeof ownerInstanceId !== 'string') return;
		if (type === 'open') {
			const attrs = normalizeAttrs(envelope.attrs);
			applyOpenEvent(userId, ownerInstanceId, attrs);
		} else if (type === 'close') {
			applyCloseEvent(userId, ownerInstanceId);
		}
	}

	function heartbeatTick() {
		if (destroyed || localUsers.size === 0) return;
		if (!withBreakerGuard()) return;
		// Refresh TTL on every locally-owned entry. A pipeline keeps this
		// to one round trip regardless of N.
		const pipe = redis.pipeline();
		for (const userId of localUsers.keys()) {
			pipe.expire(userKey(userId), ttl);
		}
		pipe.exec().then(() => breaker?.success()).catch((err) => breaker?.failure(err));
	}

	function handleInbound(envelope) {
		if (!envelope || typeof envelope !== 'object') return;
		switch (envelope.type) {
			case 'request': handleInboundRequest(envelope); break;
			case 'reply': handleInboundReply(envelope); break;
			case 'coalesced': handleInboundCoalesced(envelope); break;
			case 'send': handleInboundSend(envelope); break;
			case 'sendTo': handleInboundSendTo(envelope); break;
			default: /* unknown type, ignore for forward compatibility */ break;
		}
	}

	function handleInboundSendTo(env) {
		const { criteria, topic, event, data } = env;
		if (!activePlatform || !criteria || typeof criteria !== 'object') return;
		if (typeof topic !== 'string' || typeof event !== 'string') return;
		const matches = resolveMatches(criteria);
		if (matches.size === 0) return;
		for (const userId of matches) {
			// Only deliver to userIds we currently own locally. The sender's
			// pre-grouping already targeted us, but a fast migration can
			// invalidate that pre-grouping by the time the envelope arrives;
			// authoritative match against this instance's own local map
			// keeps the contract honest.
			const sessionId = localUsers.get(userId);
			if (!sessionId) continue;
			const ws = sessionToWs.get(sessionId);
			if (!ws) continue;
			try {
				activePlatform.send(ws, topic, event, data);
			} catch {
				// fire-and-forget: a thrown send surfaces as a missing frame
				// on the wire. sendTo callers needing a delivery signal
				// should fan out via registry.request per userId.
			}
		}
	}

	function handleInboundSend(env) {
		const { sessionId, topic, event, data } = env;
		if (typeof sessionId !== 'string' || typeof topic !== 'string' || typeof event !== 'string') return;
		const ws = sessionToWs.get(sessionId);
		if (!ws || !activePlatform) {
			mSends?.inc({ result: 'late' });
			return;
		}
		try {
			activePlatform.send(ws, topic, event, data);
		} catch {
			// fire-and-forget: a thrown send on the receiver surfaces as a
			// missing frame on the wire. Callers needing a delivery signal
			// should use registry.request instead.
		}
	}

	function handleInboundCoalesced(env) {
		const { sessionId, key, topic, event, data } = env;
		if (typeof sessionId !== 'string' || typeof key !== 'string') return;
		const ws = sessionToWs.get(sessionId);
		if (!ws || !activePlatform) {
			mCoalesced?.inc({ result: 'late' });
			return;
		}
		try {
			activePlatform.sendCoalesced(ws, { key, topic, event, data });
		} catch {
			// fire-and-forget: a thrown sendCoalesced on the receiver
			// surfaces as a missing frame on the wire, which the next
			// per-key send will overwrite anyway.
		}
	}

	async function handleInboundRequest(env) {
		const { ref, sessionId, event, data, replyTo, timeoutMs } = env;
		if (typeof ref !== 'string' || typeof event !== 'string' || typeof replyTo !== 'string') return;
		const ws = sessionToWs.get(sessionId);
		if (!ws || !activePlatform) {
			await sendReplyEnvelope(replyTo, ref, undefined, 'offline');
			return;
		}
		try {
			const reply = await activePlatform.request(ws, event, data, { timeoutMs });
			await sendReplyEnvelope(replyTo, ref, reply, null);
		} catch (err) {
			const message = (err && err.message) ? err.message : 'handler error';
			await sendReplyEnvelope(replyTo, ref, undefined, message);
		}
	}

	function handleInboundReply(env) {
		const { ref } = env;
		if (typeof ref !== 'string') return;
		const slot = pending.get(ref);
		if (!slot) {
			mLateReply?.inc();
			return;
		}
		clearTimeout(slot.timer);
		pending.delete(ref);
		const elapsed = Date.now() - slot.startTime;
		mReplyLatency?.observe(elapsed);
		if (env.error) {
			mRequests?.inc({ result: 'error' });
			slot.reject(new Error(String(env.error)));
		} else {
			mRequests?.inc({ result: 'ok' });
			slot.resolve(env.data);
		}
	}

	async function sendReplyEnvelope(replyTo, ref, data, error) {
		const envelope = error
			? { type: 'reply', ref, error }
			: { type: 'reply', ref, data };
		try {
			await redis.publish(pushChannel(replyTo), JSON.stringify(envelope));
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			// The origin will time out; cluster latency / Redis failure here
			// is indistinguishable from owner-instance crash from the origin's
			// view, which is the right behavior.
		}
	}

	async function request(target, event, data, opts = {}) {
		if (typeof target !== 'string' || target.length === 0) {
			throw new Error('registry.request: target must be a non-empty userId string');
		}
		if (typeof event !== 'string' || event.length === 0) {
			throw new Error('registry.request: event must be a non-empty string');
		}
		const timeoutMs = opts.timeoutMs ?? defaultRequestTimeoutMs;

		const entry = await lookup(target);
		if (!entry) {
			mRequests?.inc({ result: 'offline' });
			throw new Error(`registry.request: target user "${target}" is offline`);
		}

		// Self-targeting: short-circuit to local platform.request, no Redis hop.
		if (entry.instanceId === instanceId) {
			const ws = sessionToWs.get(entry.sessionId);
			if (!ws || !activePlatform) {
				mRequests?.inc({ result: 'offline' });
				throw new Error(`registry.request: target user "${target}" is offline`);
			}
			const start = Date.now();
			try {
				const reply = await activePlatform.request(ws, event, data, { timeoutMs });
				mReplyLatency?.observe(Date.now() - start);
				mRequests?.inc({ result: 'ok' });
				return reply;
			} catch (err) {
				mRequests?.inc({ result: 'error' });
				throw err;
			}
		}

		// Cross-instance: publish request, wait for reply on own push channel.
		await ensureSubscriber(activePlatform);
		const ref = randomBytes(12).toString('hex');
		const envelope = {
			type: 'request',
			ref,
			sessionId: entry.sessionId,
			event,
			data,
			replyTo: instanceId,
			timeoutMs
		};

		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				if (!pending.delete(ref)) return;
				mRequests?.inc({ result: 'timeout' });
				reject(new Error(`registry.request: timed out after ${timeoutMs}ms`));
			}, timeoutMs);
			if (timer.unref) timer.unref();
			pending.set(ref, { resolve, reject, timer, startTime: Date.now() });

			redis.publish(pushChannel(entry.instanceId), JSON.stringify(envelope))
				.then(() => breaker?.success())
				.catch((err) => {
					breaker?.failure(err);
					if (!pending.delete(ref)) return;
					clearTimeout(timer);
					mRequests?.inc({ result: 'error' });
					reject(err);
				});
		});
	}

	async function sendCoalesced(target, message) {
		if (typeof target !== 'string' || target.length === 0) {
			throw new Error('registry.sendCoalesced: target must be a non-empty userId string');
		}
		if (!message || typeof message !== 'object') {
			throw new Error('registry.sendCoalesced: message must be an object');
		}
		if (typeof message.key !== 'string' || message.key.length === 0) {
			throw new Error('registry.sendCoalesced: message.key must be a non-empty string');
		}
		const { key, topic, event, data } = message;

		const entry = await lookup(target);
		if (!entry) {
			mCoalesced?.inc({ result: 'offline' });
			return;
		}

		// Self-targeting: short-circuit to local platform.sendCoalesced.
		if (entry.instanceId === instanceId) {
			const ws = sessionToWs.get(entry.sessionId);
			if (!ws || !activePlatform) {
				mCoalesced?.inc({ result: 'offline' });
				return;
			}
			try {
				activePlatform.sendCoalesced(ws, { key, topic, event, data });
				mCoalesced?.inc({ result: 'self' });
			} catch {
				mCoalesced?.inc({ result: 'error' });
			}
			return;
		}

		// Cross-instance: publish a fire-and-forget envelope on the owning
		// instance's push channel. No reply path, no per-message ref --
		// per-key replacement happens on the receiver via the existing
		// platform.sendCoalesced semantics, so a duplicate or out-of-order
		// envelope from a flaky link just gets coalesced on arrival.
		await ensureSubscriber(activePlatform);
		const envelope = {
			type: 'coalesced',
			sessionId: entry.sessionId,
			key,
			topic,
			event,
			data
		};
		try {
			await redis.publish(pushChannel(entry.instanceId), JSON.stringify(envelope));
			breaker?.success();
			mCoalesced?.inc({ result: 'ok' });
		} catch (err) {
			breaker?.failure(err);
			mCoalesced?.inc({ result: 'error' });
		}
	}

	async function send(target, topic, event, data) {
		if (typeof target !== 'string' || target.length === 0) {
			throw new Error('registry.send: target must be a non-empty userId string');
		}
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('registry.send: topic must be a non-empty string');
		}
		if (typeof event !== 'string' || event.length === 0) {
			throw new Error('registry.send: event must be a non-empty string');
		}

		const entry = await lookup(target);
		if (!entry) {
			mSends?.inc({ result: 'offline' });
			return;
		}

		// Self-targeting: short-circuit to local platform.send.
		if (entry.instanceId === instanceId) {
			const ws = sessionToWs.get(entry.sessionId);
			if (!ws || !activePlatform) {
				mSends?.inc({ result: 'offline' });
				return;
			}
			try {
				activePlatform.send(ws, topic, event, data);
				mSends?.inc({ result: 'self' });
			} catch {
				mSends?.inc({ result: 'error' });
			}
			return;
		}

		// Cross-instance: fire-and-forget envelope on the owning instance's
		// push channel. No reply path. A user that disconnects between the
		// lookup and the receive surfaces as a `late` increment on the
		// receiver side; this method does not surface that to the caller.
		await ensureSubscriber(activePlatform);
		const envelope = {
			type: 'send',
			sessionId: entry.sessionId,
			topic,
			event,
			data
		};
		try {
			await redis.publish(pushChannel(entry.instanceId), JSON.stringify(envelope));
			breaker?.success();
			mSends?.inc({ result: 'ok' });
		} catch (err) {
			breaker?.failure(err);
			mSends?.inc({ result: 'error' });
		}
	}

	/**
	 * Resolve criteria to a Set of matching userIds via the secondary
	 * index. Empty / malformed criteria returns the empty set so callers
	 * can no-op without a special case. Compound criteria intersect across
	 * keys (AND semantics).
	 *
	 * @param {Record<string, string | number | boolean>} criteria
	 */
	function resolveMatches(criteria) {
		const norm = normalizeAttrs(criteria);
		const keys = Object.keys(norm);
		if (keys.length === 0) return new Set();
		// Iterate in ascending bucket-size order so the first intersection
		// is against the smallest set; cheaper than starting with a large
		// bucket.
		const buckets = [];
		for (const k of keys) {
			const byValue = secondaryIndex.get(k);
			if (!byValue) return new Set();
			const users = byValue.get(norm[k]);
			if (!users || users.size === 0) return new Set();
			buckets.push(users);
		}
		buckets.sort((a, b) => a.size - b.size);
		const out = new Set();
		for (const userId of buckets[0]) {
			let inAll = true;
			for (let i = 1; i < buckets.length; i++) {
				if (!buckets[i].has(userId)) { inAll = false; break; }
			}
			if (inAll) out.add(userId);
		}
		return out;
	}

	async function sendTo(criteria, topic, event, data) {
		if (!criteria || typeof criteria !== 'object') {
			throw new Error('registry.sendTo: criteria must be a non-empty object');
		}
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('registry.sendTo: topic must be a non-empty string');
		}
		if (typeof event !== 'string' || event.length === 0) {
			throw new Error('registry.sendTo: event must be a non-empty string');
		}
		if (!attributes) {
			throw new Error('registry.sendTo: requires `attributes` option on createConnectionRegistry');
		}
		const norm = normalizeAttrs(criteria);
		if (Object.keys(norm).length === 0) {
			throw new Error('registry.sendTo: criteria must include at least one attribute key');
		}

		const matches = resolveMatches(norm);
		if (matches.size === 0) {
			mSendTo?.inc({ result: 'empty' });
			return;
		}

		// Group matching userIds by their owning instance. Users without a
		// recorded owner (registered locally only, before the events
		// channel propagated) fall into the self bucket so we still deliver.
		const byOwner = new Map();
		for (const userId of matches) {
			const owner = userToInstance.get(userId) || instanceId;
			let bucket = byOwner.get(owner);
			if (!bucket) {
				bucket = [];
				byOwner.set(owner, bucket);
			}
			bucket.push(userId);
		}

		await ensureSubscriber(activePlatform);
		const envelope = { type: 'sendTo', criteria: norm, topic, event, data };

		let publishErrored = false;
		const remotePublishes = [];

		for (const [owner, userIds] of byOwner) {
			if (owner === instanceId) {
				// Self bucket: iterate locally, no Redis hop.
				if (!activePlatform) continue;
				for (const userId of userIds) {
					const sessionId = localUsers.get(userId);
					if (!sessionId) continue;
					const ws = sessionToWs.get(sessionId);
					if (!ws) continue;
					try {
						activePlatform.send(ws, topic, event, data);
					} catch {
						// fire-and-forget on the wire
					}
				}
				continue;
			}
			remotePublishes.push(
				redis.publish(pushChannel(owner), JSON.stringify(envelope))
					.then(() => breaker?.success())
					.catch((err) => {
						breaker?.failure(err);
						publishErrored = true;
					})
			);
		}

		if (remotePublishes.length > 0) {
			await Promise.all(remotePublishes);
		}
		mSendTo?.inc({ result: publishErrored ? 'error' : 'ok' });
	}

	const tracker = /** @type {ConnectionRegistry} */ ({
		instanceId,
		lookup,
		request,
		send,
		sendCoalesced,
		sendTo,
		size() { return localUsers.size; },
		hooks: {
			async open(ws, ctx) {
				await ensureSubscriber(ctx?.platform);
				const userId = identify(ws);
				if (!userId) return;
				const ud = ws.getUserData ? ws.getUserData() : {};
				// Read the session id via the adapter's slot symbol.
				const sessionId = sessionIdFromUserData(ud);
				if (!sessionId) return;

				// If this user was previously local with a different sessionId,
				// drop the stale local entry first so the new session wins
				// cleanly on the local sessionToWs map.
				const prevSession = localUsers.get(userId);
				if (prevSession && prevSession !== sessionId) {
					sessionToWs.delete(prevSession);
				}
				localUsers.set(userId, sessionId);
				sessionToWs.set(sessionId, ws);

				const attrs = attributes ? normalizeAttrs(attributes(ws)) : {};

				// Update our own view of the secondary index synchronously
				// before the broadcast lands -- self-targeting `sendTo` calls
				// fired between the open hook and the events round trip
				// still match against this newly-registered user.
				if (attributes) applyOpenEvent(userId, instanceId, attrs);

				try {
					await setEntry(userId, sessionId, attrs);
				} catch {
					// Best-effort: a write failure leaves the local maps populated
					// so the user is still reachable from this instance until the
					// heartbeat retries.
				}
				if (attributes) {
					await publishEvent({ type: 'open', userId, instanceId, attrs });
				}
			},
			async close(ws, _ctx) {
				const userId = identify(ws);
				if (!userId) return;
				const sessionId = localUsers.get(userId);
				if (sessionId) {
					localUsers.delete(userId);
					sessionToWs.delete(sessionId);
				}
				await deleteIfOurs(userId);
				// Local index update and broadcast happen unconditionally
				// once the local maps clear: the compare-and-delete above
				// already protects the Redis row from clobbering a migrated
				// entry, and the recipients of the close event guard with
				// `applyCloseEvent`'s owner check before unindexing.
				if (attributes) {
					applyCloseEvent(userId, instanceId);
					await publishEvent({ type: 'close', userId, instanceId });
				}
			}
		},
		async destroy() {
			destroyed = true;
			if (heartbeatTimer) {
				clearInterval(heartbeatTimer);
				heartbeatTimer = null;
			}
			for (const slot of pending.values()) {
				clearTimeout(slot.timer);
				slot.reject(new Error('registry: destroyed'));
			}
			pending.clear();
			localUsers.clear();
			sessionToWs.clear();
			userToInstance.clear();
			userIdAttrs.clear();
			secondaryIndex.clear();
			bootstrapped = false;
			if (subscriber) {
				const sub = subscriber;
				subscriber = null;
				try { await sub.quit(); } catch { try { sub.disconnect(); } catch { /* ignore */ } }
			}
			activePlatform = null;
		}
	});

	return tracker;
}

/**
 * Read the per-connection session id from the adapter's userData slot.
 * Falls back to a plain string key so test mocks that stamp `sessionId`
 * directly onto userData work without booting a real adapter.
 *
 * @param {Record<string | symbol, any>} ud
 */
function sessionIdFromUserData(ud) {
	if (!ud) return null;
	if (ud[WS_SESSION_ID]) return String(ud[WS_SESSION_ID]);
	if (ud.sessionId) return String(ud.sessionId);
	return null;
}
