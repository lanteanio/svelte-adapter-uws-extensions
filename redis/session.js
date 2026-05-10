/**
 * Cluster-wide session store for svelte-adapter-uws.
 *
 * The adapter's bundled `Session` plugin
 * (`svelte-adapter-uws/plugins/session`) is in-process: a session
 * created on instance A is invisible to instance B after a
 * load-balancer hop. This is the Redis-backed swap. Same get / set /
 * delete / touch / clear shape; same sliding-TTL semantics.
 *
 * Pairs with B24's connection registry: when both modules are wired,
 * the session provides the durable per-user state (survives
 * disconnect, persists across reconnect), while the registry provides
 * the live "where is this user right now" pointer (tracks the
 * currently-owning instance).
 *
 * Storage layout:
 *   - String `{prefix}sess:{token}` -> JSON-encoded session data.
 *     Sliding TTL refreshed by every `set` and (by default) every
 *     `get`. JSON blob keeps the contract atomic: a single SET / GET
 *     replaces / reads the whole record without HGETALL field parsing
 *     or DEL+HSET dances.
 *
 * @module svelte-adapter-uws-extensions/redis/session
 */

/**
 * @typedef {Object} DistributedSessionOptions
 * @property {string} [keyPrefix='sess:'] - Prefix prepended (after the client `keyPrefix`) to every session key.
 * @property {number} [ttlMs=86400000] - Time to live in milliseconds. Each `set` refreshes to `ttlMs`. By default `get` and `touch` also refresh (sliding window). Default 24 hours.
 * @property {boolean} [refreshOnGet=true] - Whether `get(token)` extends the TTL on a hit. Set to `false` for read-only flows where reads should not act as liveness signals.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 */

const DEFAULT_KEY_PREFIX = 'sess:';
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * Create a Redis-backed session store with sliding TTL.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {DistributedSessionOptions} [options]
 *
 * @example
 * ```js
 * import { createDistributedSession } from 'svelte-adapter-uws-extensions/redis/session';
 *
 * const sessions = createDistributedSession(redis, {
 *   ttlMs: 30 * 60 * 1000  // 30 minutes
 * });
 *
 * await sessions.set('token-abc', { userId: 42, role: 'admin' });
 * const data = await sessions.get('token-abc'); // { userId: 42, role: 'admin' }
 * await sessions.touch('token-abc');             // extend window without reading
 * await sessions.delete('token-abc');            // explicit logout
 * ```
 */
export function createDistributedSession(client, options = {}) {
	if (!client || !client.redis) {
		throw new Error('session: client (from createRedisClient) is required');
	}
	const keyPrefix = options.keyPrefix !== undefined ? String(options.keyPrefix) : DEFAULT_KEY_PREFIX;
	const ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
	if (!Number.isFinite(ttlMs) || ttlMs < 1) {
		throw new Error('session: ttlMs must be a positive number (ms)');
	}
	const refreshOnGet = options.refreshOnGet !== false;
	const breaker = options.breaker;
	const redis = client.redis;

	const m = options.metrics;
	const mGet = m?.counter('session_get_total', 'Session get calls by hit/miss', ['result']);
	const mSet = m?.counter('session_set_total', 'Session set calls');
	const mDelete = m?.counter('session_delete_total', 'Session delete calls by present/absent', ['result']);
	const mTouch = m?.counter('session_touch_total', 'Session touch calls by present/absent', ['result']);

	function fullKey(token) {
		return client.key(keyPrefix + token);
	}

	function validateToken(token) {
		if (typeof token !== 'string' || token.length === 0) {
			throw new Error('session: token must be a non-empty string');
		}
	}

	async function get(token) {
		validateToken(token);
		const key = fullKey(token);
		try {
			const raw = await redis.get(key);
			breaker?.success();
			if (raw == null) {
				mGet?.inc({ result: 'miss' });
				return null;
			}
			let data;
			try {
				data = JSON.parse(raw);
			} catch {
				// Corrupt entry - treat as miss; let the next set overwrite.
				mGet?.inc({ result: 'miss' });
				return null;
			}
			if (refreshOnGet) {
				try {
					await redis.pexpire(key, ttlMs);
					breaker?.success();
				} catch (err) {
					breaker?.failure(err);
					// TTL refresh is best-effort; the read still succeeds.
				}
			}
			mGet?.inc({ result: 'hit' });
			return data;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	async function set(token, data) {
		validateToken(token);
		const key = fullKey(token);
		const raw = JSON.stringify(data);
		try {
			await redis.set(key, raw, 'PX', ttlMs);
			breaker?.success();
			mSet?.inc();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	async function touch(token) {
		validateToken(token);
		const key = fullKey(token);
		try {
			const r = await redis.pexpire(key, ttlMs);
			breaker?.success();
			const refreshed = Number(r) === 1;
			mTouch?.inc({ result: refreshed ? 'present' : 'absent' });
			return refreshed;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	async function del(token) {
		validateToken(token);
		const key = fullKey(token);
		try {
			const r = await redis.unlink(key);
			breaker?.success();
			const removed = Number(r) === 1;
			mDelete?.inc({ result: removed ? 'present' : 'absent' });
			return removed;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	async function clear() {
		// SCAN-based cleanup. Cluster-wide cost scales with total session
		// count; not a hot-path operation. Use for graceful-shutdown
		// teardowns, test harnesses, or operator-initiated wipes.
		const pattern = client.key(keyPrefix + '*');
		let cursor = '0';
		do {
			let res;
			try {
				res = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
			cursor = res[0];
			const keys = res[1] || [];
			if (keys.length > 0) {
				try {
					await redis.unlink(...keys);
					breaker?.success();
				} catch (err) {
					breaker?.failure(err);
					throw err;
				}
			}
		} while (cursor !== '0');
	}

	return {
		get,
		set,
		touch,
		delete: del,
		clear
	};
}
