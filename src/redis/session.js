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
 *   - With `forgetIdentity` instead, the record is
 *     `{"identity:v1":[tenantId,userId],"r":<record>}` and the index is
 *     `{prefix}sess:identity:v1:byuser:{base64url(JSON [tenantId,userId])}`.
 *
 * @module svelte-adapter-uws-extensions/redis/session
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { evalCached, evalCachedName } from '../shared/eval-cached.js';
import { execMultiSlot, firstCommandError, isCluster, clusterTopology } from '../shared/cluster.js';
import { MAX_REDIS_PIPELINE_COMMANDS } from '../shared/caps.js';
import { createHashFieldTTLProbe } from '../shared/redis-version.js';
import { defineOptions, option, wholeExpiry } from '../shared/options.js';
import { randomBytes, wallEpoch, monotonicNow } from '../shared/runtime.js';
import { readIdentity, purgeIdentity, sameIdentity, identityKey, identityTuple, tupleIdentity, legacyResidueError } from '../shared/forget-identity.js';

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

// Replace a legacy-layout record with its qualified form, extending its TTL,
// only while it still holds the exact bytes read.
const ADOPT_RECORD = `
-- SESSION_ADOPT_RECORD
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0
`;

// Qualified mode keeps an erasure counter in each identity's index hash. The
// field name begins with the reserved token prefix, so no token collides with
// it. purgeUser increments it before reading the index; a write that started
// before that increment sees the change after storing its record and deletes
// the record again. The counter lives long enough for any write in flight.
const ERASURE_FIELD = 'identity:erasure';
const MARKER_KEY = 'identity:v1:legacy';

// The first qualified purge scans for legacy sessions for at most this long
// and otherwise reports residue for `ttlMs`. The `scanning` marker's lease
// covers the scan, its settle and slow round trips; a scanner that dies frees
// the marker when the lease ends.
const SCAN_BUDGET_MS = 5_000;
const SCAN_LEASE_MS = 3 * SCAN_BUDGET_MS;

// Kept alive by stores without forgetIdentity while their sessions may live.
const COVERAGE_KEY = 'identity:v1:legacy-active';
const COVERAGE_VALUE = '{"legacyActive":true}';

// The first bytes of every qualified record.
const QUALIFIED_HEAD = '{"identity:v1":';

// The legacy marker, or null for anything else.
function readMarker(raw) {
	let marker;
	try { marker = JSON.parse(raw); } catch { return null; }
	if (!marker || typeof marker !== 'object' || Array.isArray(marker) || !Number.isFinite(marker.since)) return null;
	if (marker.state !== 'clean' && marker.state !== 'residue' && marker.state !== 'scanning') return null;
	if (Object.keys(marker).some((key) => key !== 'since' && key !== 'state' && key !== 'id')) return null;
	return marker;
}

// Replace the legacy marker only while it holds the exact value written.
const SETTLE_MARKER = `
-- SESSION_SETTLE_MARKER
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2])
  return 1
end
return 0
`;
const ERASURE_FENCE_MS = INDEX_GRACE_MS;

// Qualified mode wraps the record the legacy layout would store with the
// identity extracted when it was written. Refreshes, replacements and purges
// read the identity from here and never call the extractor again.
const QUALIFIED_FIELD = 'identity:v1';
function wrapQualified(identity, record) {
	return '{"' + QUALIFIED_FIELD + '":' + (identity === null ? 'null' : identityTuple(identity)) + ',"r":' + record + '}';
}

// `{ identity, record }` for a qualified record, or null for one in the legacy
// layout (written before `forgetIdentity` was enabled, so it has no tenant).
function unwrapQualified(value) {
	if (value === null || typeof value !== 'object' || Array.isArray(value)) return null;
	if (!Object.hasOwn(value, QUALIFIED_FIELD) || !Object.hasOwn(value, 'r') || Object.keys(value).length !== 2) return null;
	const tuple = value[QUALIFIED_FIELD];
	if (tuple === null) return { identity: null, record: value.r };
	const identity = tupleIdentity(tuple);
	return identity === null ? null : { identity, record: value.r };
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

const validateOptions = defineOptions({
	factory: 'createDistributedSession',
	label: 'session',
	types: 'DistributedSessionOptions in redis/session.d.ts'
}, {
	keyPrefix: option.known,
	// Sent to Redis as PX / PEXPIRE.
	ttlMs: wholeExpiry(),
	refreshOnGet: option.boolean,
	identify: option.known,
	maxAgeMs: option.known,
	onLoadError: option.known,
	forgetUserId: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

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
	options = validateOptions(options);
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
	// Tenant-qualified alternative: the identity is extracted once per write,
	// stored with the record, and indexed by the exact (tenantId, userId).
	const forgetIdentity = options.forgetIdentity;
	if (forgetIdentity !== undefined && typeof forgetIdentity !== 'function') {
		throw new Error('session: forgetIdentity must be a function (data) => { tenantId, userId }');
	}
	if (forgetUserId !== undefined && forgetIdentity !== undefined) {
		throw new Error('session: forgetUserId and forgetIdentity are mutually exclusive');
	}
	const qualified = forgetIdentity !== undefined;
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
	function byIdentityKey(identity) {
		return client.key(keyPrefix + 'identity:v1:byuser:' + identityKey(identity));
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

	// The application data of a stored value, unwrapping a lifecycle record.
	function unwrapData(value) {
		return isLifecycleRecord(value) ? value.d : value;
	}

	// The owner of a stored value, unwrapping a lifecycle record first.
	function recordOwner(value) {
		return extractOwner(unwrapData(value));
	}

	// The identity owning data about to be written, or null for an explicit
	// absent identity. A throwing or malformed extractor rejects the write.
	function extractIdentity(data) {
		try { return readIdentity(forgetIdentity(data), 'session: forgetIdentity'); }
		catch (err) { throw incompleteIndex(err); }
	}

	// Write or refresh `token` in its owner's index. Callers run this BEFORE
	// writing or sliding the record and skip that step when it rejects, so no
	// record is ever stored or extended without an index field outliving it. In
	// per-field mode the GT whole-key PEXPIRE can only extend a key TTL left by
	// the fallback (an older server, or a probe that timed out), which would
	// otherwise drop every field of the key at once.
	// In qualified mode it resolves the identity's erasure counter, read in
	// the same transaction.
	async function indexOwner(owner, token) {
		const idxKey = qualified ? byIdentityKey(owner) : byUserKey(owner);
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
			if (qualified) tx.hget(idxKey, ERASURE_FIELD);
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
		return qualified ? result[result.length - 1][1] ?? null : undefined;
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
		return Number(await legacyCommand(['pexpire', key, ttlMs])) === 1;
	}

	// Slide a qualified record after extending the index field of the identity
	// stored with it. The field is only extended, never created: a slide that
	// races an erasure must not leave a field behind for the erased record. A
	// record whose field is gone is not extended. A record with an explicit
	// absent identity has no field.
	async function slideQualified(key, token, identity) {
		if (identity !== null && !(await extendIndexField(identity, token))) return false;
		try {
			const r = await redis.pexpire(key, ttlMs);
			breaker?.success();
			return Number(r) === 1;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	// Whether the index field of `token` exists, with its TTL extended.
	async function extendIndexField(identity, token) {
		const idxKey = byIdentityKey(identity);
		let result;
		try {
			const perField = await hexpireProbe.ready();
			const tx = redis.multi();
			if (perField) {
				tx.hpexpire(idxKey, indexTtlMs, 'FIELDS', 1, token);
				tx.pexpire(idxKey, indexTtlMs, 'GT');
			} else {
				tx.hexists(idxKey, token);
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
		const first = result[0][1];
		return Number(Array.isArray(first) ? first[0] : first) === 1;
	}

	// After a qualified record is stored: delete it again when an erasure of
	// its identity started after the index field was written (the counter
	// moved), and restore the field when a concurrent erasure or replacement
	// removed it. Rejects with the record deleted when it cannot be confirmed.
	async function confirmIndexed(key, token, identity, raw, epoch) {
		const idxKey = byIdentityKey(identity);
		let cause = null;
		for (let attempt = 0; attempt < MAX_ERASE_ATTEMPTS; attempt++) {
			let result;
			try {
				const tx = redis.multi();
				tx.hget(idxKey, ERASURE_FIELD);
				tx.hexists(idxKey, token);
				result = await tx.exec();
				const err = Array.isArray(result) ? firstCommandError(result) : new Error('session: index transaction discarded');
				if (err) throw err;
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				throw incompleteIndex(err);
			}
			if ((result[0][1] ?? null) !== epoch) {
				cause = new Error('session: the identity was erased while this write was in flight');
				break;
			}
			if (Number(result[1][1]) === 1) return;
			// Restore the field and check again; the counter stays the one read
			// before the record was stored.
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
				const restored = await tx.exec();
				const err = Array.isArray(restored) ? firstCommandError(restored) : new Error('session: index transaction discarded');
				if (err) throw err;
				breaker?.success();
			} catch (err) {
				breaker?.failure(err);
				cause = err;
				break;
			}
		}
		cause ??= new Error('session: the index field kept disappearing');
		try {
			await evalCached(redis, ERASE_RECORD, 1, key, raw);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
		}
		throw incompleteIndex(cause);
	}

	// Whether a session in the legacy layout may still exist in this keyspace.
	// Such a session names no tenant and has no index a qualified purge can
	// read. The answer is kept in a marker under the reserved `identity:`
	// prefix: `clean` when a scan of the keyspace found none, `residue` when
	// it found some or could not finish (each is adopted or expires within
	// `ttlMs` of `since`), and `scanning`, with a lease, while a scan runs. A
	// missing or unreadable marker is established by scanning. Sessions that
	// a store without forgetIdentity writes or slides later are covered by
	// its coverage key instead (see coverLegacyWrites).
	function markerKey() {
		return client.key(keyPrefix + MARKER_KEY);
	}

	async function legacyMayExist() {
		let marker;
		try {
			const [stored, covered] = await Promise.all([redis.get(markerKey()), redis.exists(coverageKey())]);
			breaker?.success();
			// A store without forgetIdentity wrote or slid a session recently.
			if (Number(covered) === 1) return true;
			marker = readMarker(stored);
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		marker ??= await establishMarker();
		if (marker.state === 'clean') return false;
		return wallEpoch() < marker.since + ttlMs;
	}

	async function establishMarker() {
		const since = wallEpoch();
		const scanning = JSON.stringify({ since, state: 'scanning', id: randomBytes(8).toString('hex') });
		try {
			if (await redis.set(markerKey(), scanning, 'PX', SCAN_LEASE_MS, 'NX') === null) {
				const current = readMarker(await redis.get(markerKey()));
				// Another purge is establishing it, or did: use its answer.
				if (current) { breaker?.success(); return current; }
				// An unreadable marker is replaced.
				await redis.set(markerKey(), scanning, 'PX', SCAN_LEASE_MS);
			}
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		let state;
		try {
			state = await scanForLegacy();
		} catch (err) {
			// Give the marker back, so the next purge scans again instead of
			// reporting residue for the rest of the lease.
			try { await evalCached(redis, ERASE_RECORD, 1, markerKey(), scanning); } catch { /* the lease ends it */ }
			throw err;
		}
		const marker = { since, state };
		try {
			// Kept only while it is still this scan's marker: a legacy write
			// that deleted it meanwhile may have added a session the scan missed.
			const settled = Number(await evalCached(redis, SETTLE_MARKER, 1, markerKey(), scanning, JSON.stringify(marker))) === 1;
			breaker?.success();
			return settled ? marker : { since, state: 'residue' };
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	// One pass over the session keyspace: 'residue' at the first record in the
	// legacy layout, when the pass takes longer than SCAN_BUDGET_MS, or when
	// the Cluster topology moved during it (a resharded key can slip past
	// the per-master scans); otherwise 'clean'. Each page of up to 100 keys
	// costs one pipelined GETRANGE of the first 15 bytes per key, which is all
	// a qualified record needs; only other values are read in full. A value is
	// a legacy session by its shape: valid JSON that is neither a qualified
	// record nor a marker. Non-string keys, and values get() would read as a
	// miss, are not sessions.
	async function scanForLegacy() {
		const started = monotonicNow();
		const topology = await clusterTopology(redis);
		if (topology === null) return 'residue';
		const pattern = client.key(keyPrefix) + '*';
		for (const node of isCluster(redis) ? redis.nodes('master') : [redis]) {
			let cursor = '0';
			do {
				const [next, keys] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
				cursor = String(next);
				const heads = await readValues(keys.map((key) => ['getrange', key, 0, QUALIFIED_HEAD.length - 1]));
				const others = keys.filter((_, i) => heads[i] != null && heads[i] !== '' && heads[i] !== QUALIFIED_HEAD);
				const values = await readValues(others.map((key) => ['get', key]));
				for (const raw of values) {
					if (raw == null || raw === '') continue;
					let value;
					try { value = JSON.parse(raw); } catch { continue; }
					if (unwrapQualified(value) === null && readMarker(raw) === null && raw !== COVERAGE_VALUE) return 'residue';
				}
				if (monotonicNow() - started > SCAN_BUDGET_MS) return 'residue';
			} while (cursor !== '0');
		}
		return (await clusterTopology(redis)) === topology ? 'clean' : 'residue';
	}

	// Values of single-key reads, in order; null for a key of another type.
	async function readValues(commands) {
		if (commands.length === 0) return [];
		const results = await execMultiSlot(redis, commands);
		return results.map(([err, value]) => {
			if (!err) return value;
			if (/WRONGTYPE/.test(String(err.message))) return null;
			throw err;
		});
	}

	// Coverage of the sessions a store without forgetIdentity writes. Such a
	// session has no index a qualified purge could read, so the store keeps a
	// coverage key alive, on the server's clock, for as long as a session it
	// wrote or slid may live; a qualified purge reports residue while the key
	// exists. A session lives at most `ttlMs` after the command that wrote or
	// slid it reached the server.
	//
	// A renewal is `SET key value NX PX 1.5 * ttlMs` plus `PEXPIRE key
	// 1.5 * ttlMs GT` (see sendRenewal), plain commands a client allowed SET but
	// not EVAL can send. A renewal only ever moves the expiry forward, whatever
	// the renewing store's ttlMs.
	//
	// Before a write or slide the store awaits a renewal when its estimate of
	// what is left falls under AWAIT_BELOW = 1.25 * ttlMs: the command is sent
	// with at least that much coverage left, so one delayed by up to
	// `ttlMs / 4` (offline queue, MOVED or TRYAGAIN retries, a GC pause) still
	// lands inside it. If that renewal fails the command is not sent and
	// rejects with the renewal's error, so no legacy session lands without
	// coverage in place first; the next command tries again. Under
	// RENEW_BELOW = 1.375 * ttlMs it renews in the background without waiting,
	// so a renewal succeeds at most once per instance per `ttlMs / 8`; one is
	// in flight at a time, shared by concurrent callers, and a failed
	// background renewal is not retried in the background for RETRY_AFTER_MS.
	//
	// The estimate runs on the monotonic clock from before the SET was sent,
	// so it never outlasts the server's expiry. It is dropped when the wall
	// clock has moved on by more than `ttlMs / 4` more than the monotonic one
	// since that renewal started (suspended hosts, summed over every suspend),
	// and when an EXISTS of the key finds it gone (eviction, a flush, a
	// failover that lost it). That EXISTS is sent alongside a legacy command,
	// not awaited, at most one at a time and at most once per CHECK_EVERY_MS;
	// when it finds the key gone it starts a background renewal.
	let coveredUntil = -Infinity;
	let coverage = null;
	let failedAt = -Infinity;
	let renewedWall = 0;
	let renewedMono = 0;
	let checking = false;
	let checkedAt = -Infinity;
	const AWAIT_BELOW = 1.25 * ttlMs;
	const RENEW_BELOW = 1.375 * ttlMs;
	const COVERAGE_TTL = Math.ceil(1.5 * ttlMs);
	const RETRY_AFTER_MS = Math.min(1_000, ttlMs / 4);
	const CHECK_EVERY_MS = Math.min(1_000, ttlMs / 16);
	function coverageKey() {
		return client.key(keyPrefix + COVERAGE_KEY);
	}
	function renewCoverage() {
		if (coverage) return coverage;
		// Assigned before it can settle, and cleared only while it is still the
		// renewal in flight, so a SET that throws synchronously cannot leave a
		// settled promise behind.
		const renewal = sendRenewal().finally(() => { if (coverage === renewal) coverage = null; });
		coverage = renewal;
		return renewal;
	}
	// SET NX creates the key; PEXPIRE GT then only ever moves its expiry
	// forward, so a store with a shorter ttlMs on the same prefix cannot
	// shorten what a longer one set. Both are sent in the same tick, one round
	// trip on the key's connection. A server older than Redis 7 rejects GT;
	// the store then renews with a plain SET PX, which can shorten it.
	let gtUnsupported = false;
	async function sendRenewal() {
		const started = monotonicNow();
		const wall = wallEpoch();
		try {
			if (gtUnsupported) {
				await redis.set(coverageKey(), COVERAGE_VALUE, 'PX', COVERAGE_TTL);
			} else {
				const created = redis.set(coverageKey(), COVERAGE_VALUE, 'NX', 'PX', COVERAGE_TTL);
				const extended = redis.pexpire(coverageKey(), COVERAGE_TTL, 'GT');
				const [, outcome] = await Promise.allSettled([created, extended]);
				await created;
				if (outcome.status === 'rejected') {
					if (!/syntax|wrong number|unsupported|unknown/i.test(String(outcome.reason?.message))) throw outcome.reason;
					gtUnsupported = true;
					await redis.set(coverageKey(), COVERAGE_VALUE, 'PX', COVERAGE_TTL);
				}
			}
			breaker?.success();
			coveredUntil = started + COVERAGE_TTL;
			renewedMono = started;
			renewedWall = wall;
		} catch (err) {
			breaker?.failure(err);
			failedAt = monotonicNow();
			throw err;
		}
	}
	function renewInBackground() {
		if (coverage || monotonicNow() - failedAt < RETRY_AFTER_MS) return;
		renewCoverage().catch(() => { /* the next command that needs it retries */ });
	}
	function coverLegacyWrites() {
		const mono = monotonicNow();
		if (coveredUntil > -Infinity && (wallEpoch() - renewedWall) - (mono - renewedMono) > ttlMs / 4) coveredUntil = -Infinity;
		const left = coveredUntil - mono;
		if (left >= RENEW_BELOW) return null;
		if (left >= AWAIT_BELOW) {
			renewInBackground();
			return null;
		}
		return renewCoverage();
	}
	function checkCoverage() {
		const mono = monotonicNow();
		if (checking || mono - checkedAt < CHECK_EVERY_MS) return;
		checking = true;
		checkedAt = mono;
		redis.exists(coverageKey()).then((found) => {
			checking = false;
			if (Number(found) === 0) {
				coveredUntil = -Infinity;
				renewInBackground();
			}
		}, () => { checking = false; });
	}

	// One write or slide of a store without forgetIdentity, sent once the
	// coverage above holds; rejects with the renewal's error otherwise.
	async function legacyCommand([method, ...args]) {
		const pending = coverLegacyWrites();
		if (pending) await pending;
		checkCoverage();
		try {
			const result = await redis[method](...args);
			breaker?.success();
			return result;
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	// Adopt a record in the legacy layout where a legacy store would slide it:
	// extract its identity, index it, and replace the record with the
	// qualified layout and a fresh TTL while it still holds `raw`. Resolves
	// whether it was adopted. A record whose identity cannot be extracted is
	// left as it is and not extended.
	async function adoptLegacy(key, token, raw, value) {
		let identity;
		try { identity = readIdentity(forgetIdentity(unwrapData(value)), 'session: forgetIdentity'); } catch { return false; }
		const epoch = identity !== null ? await indexOwner(identity, token) : null;
		const wrapped = wrapQualified(identity, raw);
		let swapped;
		try {
			swapped = Number(await evalCached(redis, ADOPT_RECORD, 1, key, raw, wrapped, ttlMs)) === 1;
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		if (!swapped) return false;
		if (identity !== null) await confirmIndexed(key, token, identity, wrapped, epoch);
		return true;
	}

	// Write a qualified record: index the new identity, then replace the record
	// and read the one it replaced in one transaction, so the previous
	// identity's index field can be dropped. `record` is what the legacy layout
	// stores ('' for no data). Resolves whether the record was written.
	async function writeQualified(key, token, record, onlyExisting) {
		const identity = record === '' ? null : extractIdentity(unwrapData(JSON.parse(record)));
		const epoch = identity !== null ? await indexOwner(identity, token) : null;
		const raw = record === '' ? '' : wrapQualified(identity, record);
		let result;
		try {
			const tx = redis.multi();
			tx.get(key);
			if (onlyExisting) tx.set(key, raw, 'PX', ttlMs, 'XX');
			else tx.set(key, raw, 'PX', ttlMs);
			result = await tx.exec();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		const err = Array.isArray(result) ? firstCommandError(result) : new Error('session: write transaction discarded');
		if (err) {
			breaker?.failure(err);
			throw err;
		}
		breaker?.success();
		if (result[1][1] == null) {
			// The record vanished before the conditional write: the field just
			// written must not outlive it.
			if (identity !== null) await dropIndexField(identity, token);
			return false;
		}
		await releasePrevious(key, result[0][1], identity, token);
		if (identity !== null) await confirmIndexed(key, token, identity, raw, epoch);
		return true;
	}

	async function dropIndexField(identity, token) {
		try {
			await redis.hdel(byIdentityKey(identity), token);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
	}

	// A replacement for another identity, or for none, removes the token from
	// the previous identity's index. When that fails the field stays behind,
	// pointing at a record the previous identity no longer owns; purgeUser
	// checks the stored identity before deleting, so it only drops the field.
	// A write for the previous identity can land between the replacement and
	// the drop, so the record is read again and the field restored for it.
	async function releasePrevious(key, previous, identity, token) {
		if (previous == null || previous === '') return;
		let stored;
		try { stored = unwrapQualified(JSON.parse(previous)); } catch { return; }
		if (stored === null || stored.identity === null) return;
		if (identity !== null && sameIdentity(stored.identity, identity)) return;
		try {
			await dropIndexField(stored.identity, token);
			const raw = await redis.get(key);
			let current = null;
			try { current = raw ? unwrapQualified(JSON.parse(raw)) : null; } catch { /* not a qualified record */ }
			if (current?.identity && sameIdentity(current.identity, stored.identity)) await indexOwner(stored.identity, token);
		} catch { /* reported to the breaker */ }
	}

	function validateToken(token) {
		if (typeof token !== 'string' || token.length === 0) {
			throw new Error('session: token must be a non-empty string');
		}
		if (token.startsWith('byuser:')) {
			throw new Error('session: token must not begin with reserved prefix byuser:');
		}
		// Reserved in both modes, so a store upgraded before qualified writers
		// start already refuses a token that could name a qualified index.
		if (token.startsWith('identity:')) {
			throw new Error('session: token must not begin with reserved prefix identity:');
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
		if (qualified) {
			// A record in the legacy layout is adopted into the qualified one
			// where a legacy store would have slid it.
			const stored = unwrapQualified(data);
			if (stored !== null) data = stored.record;
			if (refreshOnGet) {
				try {
					if (stored !== null) await slideQualified(key, token, stored.identity);
					else await adoptLegacy(key, token, raw, data);
				} catch { /* not slid */ }
			}
			mGet?.inc({ result: 'hit' });
			return data;
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
		if (qualified) {
			await writeQualified(key, token, raw, false);
			mSet?.inc();
			return;
		}
		// The index field is written first: a failed index write rejects before
		// the record exists, so purgeUser can always find what set stored.
		await indexStored(raw, token);
		await legacyCommand(['set', key, raw, 'PX', ttlMs]);
		mSet?.inc();
	}

	async function touch(token) {
		validateToken(token);
		const key = fullKey(token);
		let refreshed = false;
		if (!forgetUserId && !qualified) {
			refreshed = Number(await legacyCommand(['pexpire', key, ttlMs])) === 1;
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
			if (readable && qualified) {
				const stored = unwrapQualified(value);
				refreshed = stored !== null
					? await slideQualified(key, token, stored.identity)
					: await adoptLegacy(key, token, raw, value);
			} else if (readable) refreshed = await slide(key, token, value);
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
			// The coverage key is put back: stores without forgetIdentity that
			// still run rely on it until it expires.
			const left = Number(await redis.pttl(coverageKey()));
			await scanAndUnlink(redis, pattern);
			if (left > 0) await redis.set(coverageKey(), COVERAGE_VALUE, 'PX', left, 'NX');
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
		// Qualified mode reads the record inside its identity wrapper; one in
		// the legacy layout is adopted when the load slides it.
		const stored = qualified ? unwrapQualified(rec) : null;
		const legacyLayout = qualified && stored === null ? rec : undefined;
		if (stored !== null) rec = stored.record;
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
			try {
				if (stored !== null) await slideQualified(key, token, stored.identity);
				else if (qualified) await adoptLegacy(key, token, raw, legacyLayout);
				else await slide(key, token, rec);
			} catch { /* not slid */ }
		}
		return { data: rec.d, createdAt };
	}

	async function writeRecord(token, data, createdAt) {
		validateToken(token);
		const key = fullKey(token);
		const record = { d: data, c: createdAt };
		const raw = JSON.stringify(record);
		if (qualified) {
			await writeQualified(key, token, raw, false);
			return;
		}
		// Index first, as in set().
		await indexStored(raw, token);
		await legacyCommand(['set', key, raw, 'PX', ttlMs]);
	}

	// Update a record only while it still exists, so a session deleted while
	// its socket was open (logout, or erasure purging the user) is never
	// recreated by the close that follows. With an owner index, the existence
	// check keeps an erased user's index from being written again, the index is
	// still written before the record, and a record that vanishes between the
	// check and the conditional write has the index field removed again.
	async function updateRecord(token, data, createdAt) {
		validateToken(token);
		const key = fullKey(token);
		const raw = JSON.stringify({ d: data, c: createdAt });
		let written;
		if (qualified) {
			try {
				const present = Number(await redis.exists(key)) === 1;
				breaker?.success();
				if (!present) return;
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
			await writeQualified(key, token, raw, true);
			return;
		}
		if (forgetUserId) {
			try {
				const present = Number(await redis.exists(key)) === 1;
				breaker?.success();
				if (!present) return;
			} catch (err) {
				breaker?.failure(err);
				throw err;
			}
			await indexStored(raw, token);
		}
		written = await legacyCommand(['set', key, raw, 'PX', ttlMs, 'XX']);
		if (written != null || !forgetUserId) return;
		const owner = recordOwner(JSON.parse(raw));
		if (owner === null) return;
		try {
			await redis.hdel(byUserKey(owner), token);
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
		try { await updateRecord(session.token, session.data, session.createdAt); }
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

	// How a stored record relates to the identity being purged: 'owned' (delete
	// it), 'other' (it belongs to another identity or to none, so only the
	// index field goes) or 'residue' (qualified mode found a record in the
	// legacy layout, which names no tenant and is neither deleted nor
	// attributed). Throws when the record cannot be parsed or attributed.
	function classify(raw, target) {
		const value = JSON.parse(raw);
		if (!qualified) return recordOwner(value) === target ? 'owned' : 'other';
		const stored = unwrapQualified(value);
		if (stored === null) return 'residue';
		return stored.identity !== null && sameIdentity(stored.identity, target) ? 'owned' : 'other';
	}

	// Erase one bounded chunk of a user's indexed tokens. A record is deleted
	// only while it still holds the exact bytes inspected and those bytes name
	// this user. A missing record, or one that now belongs to someone else or
	// to nobody, only loses its index field. A record that cannot be read or
	// attributed keeps its field so a retry revisits it.
	async function purgeTokens(idxKey, target, tokens) {
		let removed = 0;
		let failure = null;
		let residue = false;
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
				let relation;
				try { relation = classify(raw, target); } catch (error) { failure ??= error; continue; }
				if (relation === 'owned') owned.push({ token: pending[i], raw });
				else if (relation === 'residue') residue = true;
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
		return { removed, failure, residue };
	}

	// Whether a record in the legacy layout is still listed in the legacy index
	// of this raw user id. Such a record was written before qualified mode, so
	// it names no tenant: it may belong to any tenant's user of that id, and a
	// qualified purge can neither delete nor rule it out.
	async function legacyResidue(userId) {
		let tokens;
		try {
			tokens = await redis.hkeys(byUserKey(userId));
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw err;
		}
		for (let i = 0; i < tokens.length; i += PURGE_CHUNK) {
			const chunk = tokens.slice(i, i + PURGE_CHUNK);
			const reads = await batch(chunk.map((token) => ['get', fullKey(token)]));
			for (const [err, raw] of reads) {
				if (err) throw err;
				if (raw == null || raw === '') continue;
				let stored;
				try { stored = unwrapQualified(JSON.parse(raw)); } catch { return true; }
				if (stored === null) return true;
			}
		}
		return false;
	}

	// Move the identity's erasure counter, then list its indexed tokens, in one
	// transaction: every write whose index field this listing misses read the
	// old counter and deletes its own record when it sees the new one. The
	// counter field outlives the purge by ERASURE_FENCE_MS.
	async function fenceAndList(idxKey) {
		const perField = await hexpireProbe.ready();
		const tx = redis.multi();
		tx.hincrby(idxKey, ERASURE_FIELD, 1);
		if (perField) {
			tx.hpexpire(idxKey, ERASURE_FENCE_MS, 'FIELDS', 1, ERASURE_FIELD);
		} else {
			tx.pexpire(idxKey, ERASURE_FENCE_MS, 'NX');
			tx.pexpire(idxKey, ERASURE_FENCE_MS, 'GT');
		}
		tx.hkeys(idxKey);
		const result = await tx.exec();
		const err = Array.isArray(result) ? firstCommandError(result) : new Error('session: erasure transaction discarded');
		if (err) throw err;
		return result[result.length - 1][1].filter((field) => field !== ERASURE_FIELD);
	}

	/**
	 * Right-to-erasure (`live.forget`): revoke every session a user holds. Reads
	 * only this user's token index, which covers every record written or slid
	 * for the user, and deletes each indexed record that still belongs to them;
	 * deletes compare the inspected bytes, so a concurrent replacement survives.
	 * Cost scales with the user's own sessions, never with the keyspace. A no-op
	 * when no extractor is wired - session values are opaque to the store, so
	 * without one a user's tokens are not addressable.
	 *
	 * With `forgetUserId`, `tenantId` is accepted for the uniform store
	 * contract and the index is keyed by raw userId. With `forgetIdentity` the
	 * purge addresses exactly `(tenantId, userId)`, null being only the
	 * untenanted scope, and rejects as incomplete while the legacy index of
	 * that raw user id still lists a record written before qualified mode.
	 * @param {string | null} tenantId
	 * @param {string} userId
	 * @returns {Promise<number>} sessions revoked
	 */
	async function purgeUser(tenantId, userId) {
		if ((!forgetUserId && !qualified) || typeof userId !== 'string' || userId.length === 0) return 0;
		const target = qualified ? purgeIdentity(tenantId, userId, 'session: purgeUser') : userId;
		const idxKey = qualified ? byIdentityKey(target) : byUserKey(userId);
		let tokens;
		try {
			tokens = qualified ? await fenceAndList(idxKey) : await redis.hkeys(idxKey);
			breaker?.success();
		} catch (err) {
			breaker?.failure(err);
			throw Object.assign(incompleteIndex(err), { removed: 0 });
		}
		// Refuse legacy namespace collisions before deleting any indexed key.
		// An old caller token may otherwise name another user's index.
		for (const token of tokens) validateToken(token);
		let removed = 0;
		let failure = null;
		let residue = false;
		// Every chunk is attempted, so one unreadable record still lets the
		// rest of the user's sessions go; the purge then reports incomplete.
		for (let i = 0; i < tokens.length; i += PURGE_CHUNK) {
			const outcome = await purgeTokens(idxKey, target, tokens.slice(i, i + PURGE_CHUNK));
			removed += outcome.removed;
			failure ??= outcome.failure;
			residue ||= outcome.residue;
		}
		if (qualified && !failure && !residue) {
			try {
				// Sessions in the legacy layout carry no index this purge could
				// read; those still listed by a forgetUserId index are checked
				// one by one.
				residue = await legacyMayExist() || await legacyResidue(userId);
			} catch (err) { failure = err; }
		}
		if (failure) {
			const error = failure.erasureIndexIncomplete ? failure : incompleteIndex(failure);
			throw Object.assign(error, { removed });
		}
		if (residue) {
			throw legacyResidueError('ERASURE_INCOMPLETE', 'session: a session written before forgetIdentity was enabled names no tenant and may hold this user id', removed);
		}
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
