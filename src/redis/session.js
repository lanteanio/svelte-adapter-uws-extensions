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
 *   - Hash `{prefix}sess:byuser:{userId}` (only with `forgetUserId`), one
 *     field per token the user holds. A field is written or refreshed before
 *     its record is written or slid, and outlives the record, so the index
 *     always covers every record and `purgeUser` reads nothing else.
 *
 * @module svelte-adapter-uws-extensions/redis/session
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { evalCachedName } from '../shared/eval-cached.js';
import { execMultiSlot, firstCommandError } from '../shared/cluster.js';
import { MAX_REDIS_PIPELINE_COMMANDS } from '../shared/caps.js';
import { createHashFieldTTLProbe } from '../shared/redis-version.js';
import { randomBytes, wallEpoch } from '../shared/runtime.js';

// Delete only the exact record inspected; a concurrent replacement survives.
const ERASE_RECORD = `
-- SESSION_ERASE_RECORD
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('UNLINK', KEYS[1])
end
return 0
`;

// How long an index field outlives the record it points at. The field is
// refreshed just before the record is written or slid, so the margin covers
// the time between those two commands.
const INDEX_GRACE_MS = 60_000;

// purgeUser handles this many indexed tokens per pipelined round trip.
const PURGE_CHUNK = MAX_REDIS_PIPELINE_COMMANDS;

// purgeUser re-reads a record that changed between inspection and deletion at
// most this many times before reporting the erasure incomplete.
const MAX_ERASE_ATTEMPTS = 8;

function incompleteIndex(cause) {
	const error = new Error('session: erasure index incomplete', { cause });
	error.code = 'ERASURE_INDEX_INCOMPLETE';
	error.erasureIndexIncomplete = true;
	return error;
}

// The exact shape the lifecycle layer stores: `{ d: data, c: createdAt }`.
// Ownership is read from `d`, so the application's extractor only ever sees
// application data. A raw value of exactly this shape is attributed the same
// way, which keeps every record's owner a function of its stored value.
function isLifecycleRecord(value) {
	return value !== null && typeof value === 'object' && !Array.isArray(value)
		&& typeof value.c === 'number' && Object.hasOwn(value, 'd') && Object.keys(value).length === 2;
}

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
	const indexTtlMs = ttlMs + INDEX_GRACE_MS;
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
	// expires shortly after its own session record via per-field HPEXPIRE (Redis
	// 7.4+ / Valkey 9.0+); older servers fall back to a whole-key sliding
	// PEXPIRE. A stale field whose record is gone is dropped by the next purge.
	function byUserKey(userId) {
		return client.key(keyPrefix + 'byuser:' + userId);
	}

	// The user owning `data`, or null when it names none. Extractor failures
	// and non-string results are data errors: they reject without counting
	// against the breaker.
	function extractOwner(data) {
		let owner;
		try { owner = forgetUserId(data); } catch (err) { throw incompleteIndex(err); }
		if (owner == null || owner === '') return null;
		if (typeof owner !== 'string') {
			throw incompleteIndex(new TypeError('session: forgetUserId must return a string, null or undefined'));
		}
		return owner;
	}

	// The owner of a stored value, unwrapping a lifecycle record first.
	function recordOwner(value) {
		return extractOwner(isLifecycleRecord(value) ? value.d : value);
	}

	// Write or refresh `token` in its owner's index. Callers run this BEFORE
	// writing or sliding the record and skip that step when it rejects, so no
	// record is ever stored or extended without an index field outliving it. In
	// per-field mode the GT whole-key PEXPIRE can only extend a key TTL left by
	// the fallback (an older server, or a probe that timed out), which would
	// otherwise drop every field of the key at once.
	async function indexOwner(owner, token) {
		const idxKey = byUserKey(owner);
		let result;
		try {
			const perField = await hexpireProbe.ready();
			const tx = redis.multi();
			tx.hset(idxKey, token, '1');
			if (perField) {
				tx.hpexpire(idxKey, indexTtlMs, 'FIELDS', 1, token);
				tx.pexpire(idxKey, indexTtlMs, 'GT');
			} else {
				tx.pexpire(idxKey, indexTtlMs);
			}
			result = await tx.exec();
		} catch (err) {
			breaker?.failure(err);
			throw incompleteIndex(err);
		}
		const err = Array.isArray(result) ? firstCommandError(result) : new Error('session: index transaction discarded');
		if (err) {
			breaker?.failure(err);
			throw incompleteIndex(err);
		}
		breaker?.success();
	}

	// Index the owner of a value about to be written. A no-op without an
	// extractor or for a value with no owner.
	async function indexValue(value, token) {
		if (!forgetUserId) return;
		const owner = recordOwner(value);
		if (owner !== null) await indexOwner(owner, token);
	}

	// Index serialized data the way purgeUser will read it back, so a getter,
	// a toJSON or anything else JSON does not carry cannot index a session
	// under an owner the purge then fails to recognize.
	async function indexStored(raw, token) {
		// An empty record holds no data and no owner, as purgeUser reads it.
		if (!forgetUserId || raw === '') return;
		await indexValue(JSON.parse(raw), token);
	}

	// Slide a stored record's TTL after refreshing its owner's index field, and
	// reject without sliding when the field cannot be refreshed. The refresh
	// writes the field rather than only extending it, so a record stored
	// without one gains it here. Resolves whether the record was still present.
	async function slide(key, token, value) {
		await indexValue(value, token);
		try {
			const r = await redis.pexpire(key, ttlMs);
			breaker?.success();
			return Number(r) === 1;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	function validateToken(token) {
		if (typeof token !== 'string' || token.length === 0) {
			throw new Error('session: token must be a non-empty string');
		}
		if (token.startsWith('byuser:')) {
			throw new Error('session: token must not begin with reserved prefix byuser:');
		}
	}

	async function get(token) {
		validateToken(token);
		const key = fullKey(token);
		let raw;
		try {
			raw = await redis.get(key);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
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
			// The refresh is best-effort: a read never fails because of it. When
			// the index field cannot be refreshed first, the record keeps its
			// current TTL instead of outliving its field. Redis failures were
			// already reported to the breaker where they happened.
			try { await slide(key, token, data); } catch { /* not slid */ }
		}
		mGet?.inc({ result: 'hit' });
		return data;
	}

	async function set(token, data) {
		validateToken(token);
		const key = fullKey(token);
		// A value with no JSON form (undefined) is stored as an empty record,
		// which get reads as a miss, rather than as whatever the client makes of it.
		const raw = JSON.stringify(data) ?? '';
		// The index field is written first: a failed index write rejects before
		// the record exists, so purgeUser can always find what set stored.
		await indexStored(raw, token);
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
		let refreshed = false;
		if (!forgetUserId) {
			try {
				refreshed = Number(await redis.pexpire(key, ttlMs)) === 1;
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
		} else {
			// The owner's index field is refreshed before the record slides, and
			// touch carries no data, so the owner is read from the record first.
			let raw;
			try {
				raw = await redis.get(key);
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
			let value;
			let readable = raw != null;
			if (readable) {
				// A corrupt record reads as a miss, as in get(), and is not slid.
				try { value = JSON.parse(raw); } catch { readable = false; }
			}
			if (readable) refreshed = await slide(key, token, value);
		}
		mTouch?.inc({ result: refreshed ? 'present' : 'absent' });
		return refreshed;
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
		validateToken(token);
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
			// Best-effort, as in get(): a load never fails because of it.
			try { await slide(key, token, rec); } catch { /* not slid */ }
		}
		return { data: rec.d, createdAt };
	}

	async function writeRecord(token, data, createdAt) {
		validateToken(token);
		const key = fullKey(token);
		const record = { d: data, c: createdAt };
		const raw = JSON.stringify(record);
		// Index first, as in set().
		await indexStored(raw, token);
		try {
			await redis.set(key, raw, 'PX', ttlMs);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
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

	// One pipelined round trip of independent single-key commands, routed per
	// slot on a Cluster. Never rejects: a failure fills every row. Counts once
	// against the breaker.
	async function batch(commands) {
		let results;
		try {
			results = await execMultiSlot(redis, commands);
		} catch (err) {
			results = commands.map(() => [err, undefined]);
		}
		const err = firstCommandError(results);
		if (err) breaker?.failure(err);
		else breaker?.success();
		return results;
	}

	// Erase one bounded chunk of a user's indexed tokens. A record is deleted
	// only while it still holds the exact bytes inspected and those bytes name
	// this user. A missing record, or one that now belongs to someone else or
	// to nobody, only loses its index field. A record that cannot be read or
	// attributed keeps its field so a retry revisits it.
	async function purgeTokens(idxKey, userId, tokens) {
		let removed = 0;
		let failure = null;
		const resolved = [];
		let pending = tokens;
		for (let attempt = 0; attempt < MAX_ERASE_ATTEMPTS && pending.length > 0; attempt++) {
			const reads = await batch(pending.map((token) => ['get', fullKey(token)]));
			const owned = [];
			for (let i = 0; i < pending.length; i++) {
				const [err, raw] = reads[i];
				if (err) { failure ??= err; continue; }
				// An empty record (a token set to undefined) holds no data and no owner.
				if (raw == null || raw === '') { resolved.push(pending[i]); continue; }
				let owner;
				try { owner = recordOwner(JSON.parse(raw)); } catch (error) { failure ??= error; continue; }
				if (owner === userId) owned.push({ token: pending[i], raw });
				else resolved.push(pending[i]);
			}
			pending = [];
			if (owned.length === 0) break;
			const erase = evalCachedName(redis, ERASE_RECORD);
			const erased = await batch(owned.map(({ token, raw }) => [erase, 1, fullKey(token), raw]));
			for (let i = 0; i < owned.length; i++) {
				const [err, n] = erased[i];
				if (err) failure ??= err;
				else if (Number(n) === 1) { removed++; resolved.push(owned[i].token); }
				else pending.push(owned[i].token); // changed since it was read: inspect again
			}
		}
		if (pending.length > 0) failure ??= new Error('session: records kept changing during erasure');
		if (resolved.length > 0) {
			try {
				await redis.hdel(idxKey, ...resolved);
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				failure ??= err;
			}
		}
		return { removed, failure };
	}

	/**
	 * Right-to-erasure (`live.forget`): revoke every session a user holds. Reads
	 * only this user's token index, which covers every record written or slid
	 * for the user, and deletes each indexed record that still belongs to them;
	 * deletes compare the inspected bytes, so a concurrent replacement survives.
	 * Cost scales with the user's own sessions, never with the keyspace. A no-op
	 * when no extractor is wired - session values are opaque to the store, so
	 * without one a user's tokens are not addressable. `tenantId` is accepted
	 * for the uniform store contract; the index is keyed by raw userId.
	 * @param {string | null} tenantId
	 * @param {string} userId
	 * @returns {Promise<number>} sessions revoked
	 */
	async function purgeUser(tenantId, userId) {
		if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
		const idxKey = byUserKey(userId);
		let tokens;
		try {
			tokens = await redis.hkeys(idxKey);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw incompleteIndex(err);
		}
		// Refuse legacy namespace collisions before deleting any indexed key.
		// An old caller token may otherwise name another user's index.
		for (const token of tokens) validateToken(token);
		let removed = 0;
		let failure = null;
		// Every chunk is attempted, so one unreadable record still lets the
		// rest of the user's sessions go; the purge then reports incomplete.
		for (let i = 0; i < tokens.length; i += PURGE_CHUNK) {
			const outcome = await purgeTokens(idxKey, userId, tokens.slice(i, i + PURGE_CHUNK));
			removed += outcome.removed;
			failure ??= outcome.failure;
		}
		if (failure) throw failure.erasureIndexIncomplete ? failure : incompleteIndex(failure);
		return removed;
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
