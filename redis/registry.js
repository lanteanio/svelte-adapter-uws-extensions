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

	function userKey(userId) {
		return client.key(keyPrefix + 'conns:' + userId);
	}
	function pushChannel(targetInstanceId) {
		return client.key(keyPrefix + '__push:' + targetInstanceId);
	}
	const ownPushChannel = pushChannel(instanceId);

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

	async function setEntry(userId, sessionId) {
		const key = userKey(userId);
		const now = cachedNow();
		try {
			await redis.hset(key, 'instanceId', instanceId, 'sessionId', sessionId, 'ts', now);
			await redis.expire(key, ttl);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
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
				ts: Number(result.ts) || 0
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
			if (ch !== ownPushChannel) return;
			let envelope;
			try { envelope = JSON.parse(raw); } catch { return; }
			handleInbound(envelope);
		});
		await subscriber.subscribe(ownPushChannel);

		if (!heartbeatTimer) {
			heartbeatTimer = setInterval(heartbeatTick, heartbeatInterval);
			if (heartbeatTimer.unref) heartbeatTimer.unref();
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
			default: /* unknown type, ignore for forward compatibility */ break;
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

	const tracker = /** @type {ConnectionRegistry} */ ({
		instanceId,
		lookup,
		request,
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

				try {
					await setEntry(userId, sessionId);
				} catch {
					// Best-effort: a write failure leaves the local maps populated
					// so the user is still reachable from this instance until the
					// heartbeat retries.
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
