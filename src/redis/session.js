/**
 * Cluster-wide session store for svelte-adapter-uws.
 *
 * The adapter's bundled `Session` plugin
 * (`svelte-adapter-uws/plugins/session`) is in-process: a session
 * created on instance A is invisible to instance B after a
 * load-balancer hop. This is the Redis-backed swap. Same get / set /
 * delete / touch / clear shape; same sliding-TTL semantics.
 *
 * Pairs with the connection registry: when both modules are wired,
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

import { scanAndUnlink } from '../shared/redis-scan.js';
import { createHashFieldTTLProbe } from '../shared/redis-version.js';
import { randomBytes, wallEpoch } from '../shared/runtime.js';

// Per-connection slot for the loaded session on `ws.getUserData()`. `Symbol.for`
// so a duplicated module instance (SvelteKit bundles dep files into build/)
// resolves the same key - the same convention the adapter uses for `WS_CAPS`.
const SESSION_SLOT = Symbol.for('svelte-adapter-uws-extensions.session');

// Internal sentinel: a load that must fail the connection closed (onLoadError
// 'reject'), distinct from a clean miss (anonymous).
const REJECT = Symbol('session.reject');

// Token entropy in bytes (256-bit). Minted via the runtime RNG seam, whose
// default is node:crypto `randomBytes` (a CSPRNG); a seeded sim override only
// applies inside a harness, never in production.
const TOKEN_BYTES = 32;

// Defense-in-depth: a client-presented token is used as a Redis key, so cap its
// length - a hostile cookie cannot then push pathologically large keys. Minted
// tokens are 64 hex chars, far under this.
const MAX_TOKEN_LENGTH = 4096;

/**
 * @typedef {Object} DistributedSessionOptions
 * @property {string} [keyPrefix='sess:'] - Prefix prepended (after the client `keyPrefix`) to every session key.
 * @property {number} [ttlMs=86400000] - Time to live in milliseconds. Each `set` refreshes to `ttlMs`. By default `get` and `touch` also refresh (sliding window). Default 24 hours.
 * @property {boolean} [refreshOnGet=true] - Whether `get(token)` extends the TTL on a hit. Set to `false` for read-only flows where reads should not act as liveness signals.
 * @property {(ctx: any) => (string | null | undefined)} [identify] - Extract the session token from the WS upgrade context (e.g. a cookie). Required for `withHooks`/`of`; absent or falsy => anonymous connection.
 * @property {number} [maxAgeMs] - Absolute session lifetime in ms (independent of the sliding `ttlMs`). A session older than this is treated as expired on load. Off by default.
 * @property {'reject' | 'anonymous'} [onLoadError='reject'] - `withHooks` behavior when the store is unreachable at connect or `identify` throws: `'reject'` fails closed (refuse the upgrade), `'anonymous'` fails open. An unknown/expired token is always a clean anonymous connection, not an error.
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

	// Lifecycle-wiring options (used by create/withHooks/of; the raw
	// get/set/touch/delete/clear surface ignores them).
	const identify = options.identify;
	if (identify !== undefined && typeof identify !== 'function') {
		throw new Error('session: identify must be a function (ctx) => token');
	}
	// Right-to-erasure: a session value is app-defined, so the store cannot find
	// a user's tokens on its own. When set, this extracts the owning userId from
	// the session data at write time and records the token in a per-user index so
	// `live.forget` can revoke every session a user holds. Unset => sessions are
	// not user-purgeable (the token is opaque).
	const forgetUserId = options.forgetUserId;
	if (forgetUserId !== undefined && typeof forgetUserId !== 'function') {
		throw new Error('session: forgetUserId must be a function (data) => userId');
	}
	const maxAgeMs = options.maxAgeMs;
	if (maxAgeMs !== undefined && (typeof maxAgeMs !== 'number' || !Number.isFinite(maxAgeMs) || maxAgeMs < 1)) {
		throw new Error('session: maxAgeMs must be a positive number (ms)');
	}
	const onLoadError = options.onLoadError ?? 'reject';
	if (onLoadError !== 'reject' && onLoadError !== 'anonymous') {
		throw new Error("session: onLoadError must be 'reject' or 'anonymous'");
	}

	const breaker = options.breaker;
	const redis = client.redis;
	// Soft capability gate for per-field index TTLs. Valkey-aware: routes off
	// valkey_version (9.0+ has HPEXPIRE), not the pinned redis_version:7.2.4.
	const hexpireProbe = createHashFieldTTLProbe(redis);

	const m = options.metrics;
	const mGet = m?.counter('session_get_total', 'Session get calls by hit/miss', ['result']);
	const mSet = m?.counter('session_set_total', 'Session set calls');
	const mDelete = m?.counter('session_delete_total', 'Session delete calls by present/absent', ['result']);
	const mTouch = m?.counter('session_touch_total', 'Session touch calls by present/absent', ['result']);
	const mCreate = m?.counter('session_create_total', 'Sessions minted via create()');
	const mLifecycle = m?.counter('session_lifecycle_total', 'Lifecycle session loads by outcome', ['result']);

	function fullKey(token) {
		return client.key(keyPrefix + token);
	}

	// Per-user token index (a HASH whose fields are the user's tokens) for
	// right-to-erasure. Keyed by raw userId - session tokens are globally unique,
	// so the index relies on globally-unique userIds (the same trade as the
	// connection registry). The store has no sweep loop, so each index field
	// expires alongside its own session record via per-field HPEXPIRE (Redis
	// 7.4+ / Valkey 9.0+); older servers fall back to a whole-key sliding
	// PEXPIRE. A stale field whose token already expired is a no-op DEL.
	function byUserKey(userId) {
		return client.key(keyPrefix + 'byuser:' + userId);
	}
	async function indexToken(data, token) {
		if (!forgetUserId) return;
		let uid;
		try { uid = forgetUserId(data); } catch { return; }
		if (typeof uid !== 'string' || uid.length === 0) return;
		const idxKey = byUserKey(uid);
		try {
			const hexOk = await hexpireProbe.ready();
			const tx = redis.multi();
			tx.hset(idxKey, token, '1');
			if (hexOk) tx.hpexpire(idxKey, ttlMs, 'FIELDS', 1, token);
			else tx.pexpire(idxKey, ttlMs);
			await tx.exec();
			breaker?.success();
		} catch (err) { breaker?.failure(err); /* index best-effort; the session is written */ }
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
				// The index field must slide with the record, or a read-kept
				// session outlives its index entry and escapes purgeUser. One
				// extra round-trip, only incurred when erasure indexing is wired.
				await indexToken(data, token);
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
		await indexToken(data, token);
	}

	async function touch(token) {
		validateToken(token);
		const key = fullKey(token);
		try {
			const r = await redis.pexpire(key, ttlMs);
			breaker?.success();
			const refreshed = Number(r) === 1;
			mTouch?.inc({ result: refreshed ? 'present' : 'absent' });
			if (refreshed && forgetUserId) {
				// The index field must slide with the record, or a touch-kept
				// session outlives its index entry and escapes purgeUser. touch
				// carries no data, so the owning user is derived with one extra
				// GET - only incurred when erasure indexing is wired, and
				// best-effort: an index miss must not fail the touch.
				try {
					const raw = await redis.get(key);
					breaker?.success();
					if (raw != null) {
						let data;
						let parsed = false;
						try {
							const p = JSON.parse(raw);
							// Unwrap a lifecycle record ({ d, c }); raw-layer data passes through.
							data = (p !== null && typeof p === 'object' && 'd' in p && 'c' in p) ? p.d : p;
							parsed = true;
						} catch { /* corrupt record: nothing to index */ }
						if (parsed) await indexToken(data, token);
					}
				} catch (err) { breaker?.failure(err); }
			}
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
		//
		// Routed through the shared scanAndUnlink helper so the wipe is
		// correct on a Redis Cluster: it runs an independent SCAN per master
		// node and unlinks one key at a time (session keys are independent
		// and span hash slots, so a batched multi-key UNLINK would
		// CROSSSLOT). On standalone it batches into a single SCAN loop with
		// batched UNLINK, preserving the original throughput shape.
		const pattern = client.key(keyPrefix + '*');
		try {
			await scanAndUnlink(redis, pattern);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	// --- Lifecycle layer: create() token mint + WS hook wiring ----------------
	//
	// The lifecycle layer stores a WRAPPED record `{ d: data, c: createdAt }` at
	// the same key, so the minted-at stamp (for maxAgeMs) rides with the session.
	// The raw get/set above are the LOW-LEVEL layer (unwrapped data) for direct
	// token management; delete/touch/clear are shape-agnostic and serve both. Do
	// not mix raw `set`/`get` with `create()` on the same token.

	async function readRecord(token) {
		const key = fullKey(token);
		let raw;
		try {
			raw = await redis.get(key);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		if (raw == null) return null;
		let rec;
		try { rec = JSON.parse(raw); } catch { return null; }
		if (!rec || typeof rec !== 'object' || !('d' in rec)) return null;
		const createdAt = Number(rec.c) || 0;
		// Absolute timeout. With maxAgeMs set, a record we cannot prove is fresh -
		// too old, OR carrying a missing/non-positive creation stamp - is treated
		// as expired (fail-safe; a corrupt stamp can never yield an ageless
		// session) and the stale key is best-effort reaped.
		if (maxAgeMs != null && (createdAt <= 0 || (wallEpoch() - createdAt) > maxAgeMs)) {
			try { await redis.unlink(key); breaker?.success(); } catch (err) { breaker?.failure(err); }
			return null;
		}
		if (refreshOnGet) {
			try { await redis.pexpire(key, ttlMs); breaker?.success(); } catch (err) { breaker?.failure(err); }
			// Slide the index field with the record (see get()); best-effort.
			await indexToken(rec.d, token);
		}
		return { data: rec.d, createdAt };
	}

	async function writeRecord(token, data, createdAt) {
		const key = fullKey(token);
		const raw = JSON.stringify({ d: data, c: createdAt });
		try {
			await redis.set(key, raw, 'PX', ttlMs);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		await indexToken(data, token);
	}

	/**
	 * Mint a NEW session server-side and return its opaque token (256-bit
	 * CSPRNG via the runtime seam). Call at login - or in the adapter's
	 * `authenticate` hook - and set the returned token as an httpOnly + secure +
	 * sameSite cookie; `identify` then reads it on connect. Minting tokens only
	 * server-side (never trusting a client-presented one) is what makes the
	 * lifecycle immune to session fixation.
	 *
	 * @param {*} [data] - Initial session data (JSON-serializable). Defaults to `{}`.
	 * @returns {Promise<string>} the new token
	 */
	async function create(data) {
		const token = randomBytes(TOKEN_BYTES).toString('hex');
		await writeRecord(token, data ?? {}, wallEpoch());
		mCreate?.inc();
		return token;
	}

	// Resolve + load the session for an upgrade ctx. Returns the session record,
	// `null` (anonymous: no identify, no/blank/oversized token, or an unknown
	// token - LOAD-ONLY never creates one), or REJECT (a storage/identify error
	// under onLoadError: 'reject', i.e. fail-closed).
	async function loadSessionForCtx(ctx) {
		if (typeof identify !== 'function') return null;
		let token;
		try {
			token = identify(ctx);
		} catch {
			return onLoadError === 'anonymous' ? null : REJECT;
		}
		if (!token || typeof token !== 'string' || token.length > MAX_TOKEN_LENGTH) return null;
		let rec;
		try {
			rec = await readRecord(token);
		} catch {
			return onLoadError === 'anonymous' ? null : REJECT;
		}
		if (!rec) return null; // unknown / expired token -> anonymous, never created
		return { token, data: rec.data, createdAt: rec.createdAt };
	}

	async function persistFromWs(ws) {
		let ud;
		try { ud = typeof ws?.getUserData === 'function' ? ws.getUserData() : null; }
		catch { return; } // socket closed mid-await: getUserData throws
		const session = ud && ud[SESSION_SLOT];
		if (!session || typeof session.token !== 'string') return;
		// Best-effort: a close hook must never throw.
		try { await writeRecord(session.token, session.data, session.createdAt); }
		catch { /* not fatal at close; the next successful set re-persists */ }
	}

	/**
	 * Compose session load/persist into the adapter `hooks.ws`. Returns
	 * `{ upgrade, close }` to spread into your WS handler. Your own upgrade/close
	 * still run: upgrade runs FIRST (a `false` rejection short-circuits with no
	 * session load), then the session is loaded (LOAD-ONLY) and attached to the
	 * connection; close persists the session then runs your close.
	 *
	 * @param {{ upgrade?: Function, close?: Function }} [userHooks]
	 */
	function withHooks(userHooks = {}) {
		const userUpgrade = userHooks && userHooks.upgrade;
		const userClose = userHooks && userHooks.close;
		return {
			async upgrade(ctx) {
				let result;
				if (typeof userUpgrade === 'function') {
					result = await userUpgrade(ctx);
					if (result === false) return false; // app rejected: no session load
				}
				// Normalize the base userData to attach to, preserving an
				// upgradeResponse() wrapper if the app returned one.
				let wrapper = null;
				let userData;
				if (result && result.__upgradeResponse === true) {
					wrapper = result;
					userData = (result.userData && typeof result.userData === 'object') ? result.userData : {};
				} else if (result && typeof result === 'object') {
					userData = result;
				} else {
					userData = {};
				}
				const session = await loadSessionForCtx(ctx);
				if (session === REJECT) { mLifecycle?.inc({ result: 'rejected' }); return false; }
				if (session) {
					userData[SESSION_SLOT] = session;
					mLifecycle?.inc({ result: 'loaded' });
				} else {
					mLifecycle?.inc({ result: 'anonymous' });
				}
				if (wrapper) { wrapper.userData = userData; return wrapper; }
				return userData;
			},
			async close(ws, ctx) {
				await persistFromWs(ws);
				if (typeof userClose === 'function') await userClose(ws, ctx);
			}
		};
	}

	/**
	 * The live, mutable session data for a connection wired via `withHooks`, or
	 * `null` for an anonymous connection (no/unknown token) or a closed socket.
	 * Mutate the returned object in place; it is persisted once, on disconnect.
	 *
	 * @param {*} ws
	 * @returns {*}
	 */
	function of(ws) {
		let ud;
		try { ud = typeof ws?.getUserData === 'function' ? ws.getUserData() : null; }
		catch { return null; }
		const session = ud && ud[SESSION_SLOT];
		return session ? session.data : null;
	}

	/**
	 * Right-to-erasure (`live.forget`): revoke every session a user holds. Reads
	 * the per-user token index (maintained when a `forgetUserId` extractor is
	 * configured) and UNLINKs each token, then drops the index. A no-op when no
	 * extractor is wired - session values are opaque to the store, so without one
	 * a user's tokens are not addressable. `tenantId` is accepted for the uniform
	 * store contract; the index is keyed by raw userId.
	 * @param {string | null} tenantId
	 * @param {string} userId
	 * @returns {Promise<number>} sessions revoked
	 */
	async function purgeUser(tenantId, userId) {
		if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
		const idxKey = byUserKey(userId);
		let tokens;
		try { tokens = await redis.hkeys(idxKey); breaker?.success(); }
		catch (err) { breaker?.failure(err); return 0; }
		if (!tokens || tokens.length === 0) {
			try { await redis.unlink(idxKey); } catch { /* best-effort */ }
			return 0;
		}
		// Per-token UNLINK: each token key routes to its own slot under cluster.
		const results = await Promise.all(tokens.map((t) => redis.unlink(fullKey(t)).catch(() => 0)));
		try { await redis.unlink(idxKey); } catch { /* best-effort */ }
		return results.reduce((n, r) => n + (Number(r) > 0 ? 1 : 0), 0);
	}

	return {
		get,
		set,
		touch,
		delete: del,
		clear,
		create,
		purgeUser,
		withHooks,
		of
	};
}
