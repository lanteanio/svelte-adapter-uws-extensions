/**
 * Redis-backed idempotency store.
 *
 * Caches the result of an effectful operation under a stable key so that
 * retries within `ttl` return the original outcome rather than re-executing.
 * Useful for HTTP/RPC retries, webhook redeliveries, and any handler where
 * the caller may legitimately repeat a request that must execute at most
 * once (charge-customer, send-email, create-order).
 *
 * Three states are exposed via the `acquire(key)` return value:
 *   - acquired: the caller is the owner; runs the work, then `commit(result)` or `abort()`.
 *   - pending:  another caller acquired the slot and has not committed yet.
 *   - result:   a previous run committed; the cached result is returned.
 *
 * A short `acquireTtl` (default 60s) bounds how long a pending sentinel
 * lives so a crashed owner cannot deadlock the key. On `commit` the long
 * `ttl` (default 48h) replaces the sentinel and governs the cache lifetime.
 *
 * Storage layout per key (one Redis string):
 *   - Pending: an owner token `__idem_pending__:<uuid>` (raw, not JSON). The
 *     prefix marks the slot as pending; the uuid identifies the exact owner, so
 *     commit and abort compare-and-set/delete against it - a stale owner whose
 *     `acquireTtl` expired and whose slot a successor re-acquired cannot
 *     overwrite (commit) or release (abort) that successor's slot.
 *   - Committed: JSON.stringify(result) (always begins with a valid JSON token).
 * The two cases are distinguishable because the pending prefix begins with `_`,
 * which is never a valid leading JSON token, so a committed value can never be
 * mistaken for a pending slot.
 *
 * @module svelte-adapter-uws-extensions/redis/idempotency
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { evalCached } from '../shared/eval-cached.js';
import { createHashFieldTTLProbe } from '../shared/redis-version.js';
import { withBreaker } from '../shared/breaker.js';
import { randomUuid } from '../shared/runtime.js';
import { MAX_IDEMPOTENCY_KEY_LENGTH } from '../shared/caps.js';
import { IdempotencyResultTooLargeError, IdempotencyLeaseLostError } from '../shared/errors.js';

export { IdempotencyResultTooLargeError, IdempotencyLeaseLostError };

const DEFAULT_MAX_RESULT_BYTES = 256 * 1024;

/**
 * Lua script for atomic acquire.
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token (`__idem_pending__:<uuid>`) written into the pending slot
 * ARGV[2] = acquireTtl (seconds)
 * ARGV[3] = pending prefix (`__idem_pending__`)
 *
 * Pending is detected by the ARGV[3] PREFIX (not equality): every owner writes
 * a distinct token, so an equality check against one owner's token would never
 * recognise another owner's pending slot.
 *
 * Returns one of:
 *   { 1, '',    0 }  acquired (caller runs work)
 *   { 0, '',    1 }  pending  (another caller is mid-flight)
 *   { 0, value, 0 }  result   (cached, value is JSON)
 */
const ACQUIRE_SCRIPT = `
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
if ok then
  return {1, '', 0}
end
local v = redis.call('GET', KEYS[1])
if v ~= false and string.sub(v, 1, string.len(ARGV[3])) == ARGV[3] then
  return {0, '', 1}
end
return {0, v, 0}
`;

/**
 * Lua compare-and-set for commit. Writes the committed result under the long
 * TTL only if this owner still holds the pending slot (its token is still
 * stored). Returns 1 on success, 0 if the lease was lost (a successor
 * re-acquired) - the caller then throws IdempotencyLeaseLostError.
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token
 * ARGV[2] = committed value (JSON)
 * ARGV[3] = ttl (seconds)
 */
const COMMIT_SCRIPT = `
-- IDEM_COMMIT
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0
`;

/**
 * Lua compare-and-delete for abort. Releases the pending slot only if this
 * owner still holds it. Returns 1 if released, 0 if the lease was already
 * lost - a no-op the caller ignores (nothing of ours to release).
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = owner token
 */
const ABORT_SCRIPT = `
-- IDEM_ABORT
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;

const PENDING_PREFIX = '__idem_pending__';

/**
 * @typedef {Object} RedisIdempotencyOptions
 * @property {string} [keyPrefix='idem:'] - Prefix prepended (after the client keyPrefix) to every key.
 * @property {number} [ttl=172800] - Result cache lifetime in seconds. Default 48 hours.
 * @property {number} [acquireTtl=60] - Pending-slot lifetime in seconds (anti-deadlock). Default 60 seconds.
 * @property {number} [maxResultBytes=262144] - Cap on the JSON-encoded byte length
 *   of a committed result. Past the cap, `commit(result)` rejects with
 *   `IdempotencyResultTooLargeError` (`code: 'IDEMPOTENCY_RESULT_TOO_LARGE'`) and
 *   the slot is NOT committed. The default 256 KB matches the operational shape
 *   Redis pubsub / Postgres NOTIFY and the cluster bus can absorb without
 *   becoming meta-stable. Operators with legitimately larger payloads opt up
 *   explicitly. Pass `Infinity` to disable.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker.
 * @property {any} [metrics] - Optional metrics registry (Prometheus).
 */

/**
 * @typedef {Object} IdempotencySlot
 * @property {boolean} acquired
 * @property {boolean} [pending] - True when another caller currently owns the slot.
 * @property {unknown} [result] - The cached result, if a prior run committed.
 * @property {(value: unknown) => Promise<void>} [commit] - Store the result and start the long TTL. Only present when acquired.
 * @property {() => Promise<void>} [abort] - Release the slot so retries may re-execute. Only present when acquired.
 */

/**
 * @typedef {Object} RedisIdempotencyStore
 * @property {(key: string, ttlSec?: number, meta?: { user?: string, tenant?: string | null }) => Promise<IdempotencySlot>} acquire
 * @property {(key: string) => Promise<void>} purge
 * @property {(tenantId: string | null, userId: string) => Promise<number>} purgeUser - Right-to-erasure: delete this user's cached results.
 * @property {() => Promise<void>} clear
 */

/**
 * Create a Redis-backed idempotency store.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisIdempotencyOptions} [options]
 * @returns {RedisIdempotencyStore}
 */
export function createIdempotencyStore(client, options = {}) {
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 1 || !Number.isInteger(options.ttl)) {
			throw new Error(`redis idempotency: ttl must be a positive integer, got ${options.ttl}`);
		}
	}
	if (options.acquireTtl !== undefined) {
		if (typeof options.acquireTtl !== 'number' || options.acquireTtl < 1 || !Number.isInteger(options.acquireTtl)) {
			throw new Error(`redis idempotency: acquireTtl must be a positive integer, got ${options.acquireTtl}`);
		}
	}
	const maxResultBytes = options.maxResultBytes ?? DEFAULT_MAX_RESULT_BYTES;
	if (maxResultBytes !== Infinity && (!Number.isInteger(maxResultBytes) || maxResultBytes < 1)) {
		throw new Error(`redis idempotency: maxResultBytes must be a positive integer or Infinity, got ${maxResultBytes}`);
	}
	if (options.keyPrefix !== undefined && typeof options.keyPrefix !== 'string') {
		throw new Error('redis idempotency: keyPrefix must be a string');
	}

	const keyPrefix = options.keyPrefix !== undefined ? options.keyPrefix : 'idem:';
	const ttl = options.ttl || 48 * 3600;
	const acquireTtl = options.acquireTtl || 60;
	const redis = client.redis;
	// Soft capability gate for per-field index TTLs. Valkey-aware: routes off
	// valkey_version (9.0+ has HPEXPIRE), not the pinned redis_version:7.2.4.
	const hexpireProbe = createHashFieldTTLProbe(redis);

	const b = options.breaker;
	const m = options.metrics;
	const mAcquired = m?.counter('idempotency_acquired_total', 'Slots acquired (caller runs work)');
	const mHits = m?.counter('idempotency_hits_total', 'Cached results returned');
	const mPending = m?.counter('idempotency_pending_total', 'Slots reported as pending');
	const mCommits = m?.counter('idempotency_commits_total', 'Results committed');
	const mAborts = m?.counter('idempotency_aborts_total', 'Slots aborted');

	function fullKey(userKey) {
		return client.key(keyPrefix + userKey);
	}

	// Right-to-erasure reverse index: a Redis HASH per (tenant, user) whose FIELDS
	// are the FULL cache keys that user committed, so `live.forget` can delete
	// exactly a user's entries instead of substring-matching the opaque key (a
	// collision could over-delete or miss). A hash (vs a set) keeps it portable
	// and lets fields carry TTLs. The store has no sweep loop (pure EX TTL), so
	// each index field expires with its own cache entry via per-field HPEXPIRE
	// (Redis 7.4+ / Valkey 9.0+); older servers fall back to a whole-key sliding
	// EXPIRE, where a busy user's writes keep every field alive - fields can then
	// outlive their cache entries, and any stale field is a no-op DEL on purge.
	// The composite index key is NUL-delimited, so neither segment may
	// contain NUL - otherwise distinct (tenant,user) pairs collide
	// (('a\0b','c') === ('a','b\0c')) and one identity's purgeUser deletes
	// another's committed results. Same guard as ratelimit's bucketKey,
	// enforced at the store boundary.
	function validateForgetIdentity(tenantId, userId) {
		if (tenantId != null && String(tenantId).indexOf('\0') !== -1) {
			throw new Error('redis idempotency: tenant id must not contain a NUL byte (it is the index-key delimiter)');
		}
		if (typeof userId === 'string' && userId.indexOf('\0') !== -1) {
			throw new Error('redis idempotency: user id must not contain a NUL byte (it is the index-key delimiter)');
		}
	}

	// One coercion for both index writers. `acquire` reaches this through a
	// `typeof === 'string'` filter on meta.tenant while `purgeUser` takes
	// the caller's argument raw, so a non-string tenant used to write under
	// one key and erase under another - a right-to-erasure miss of exactly
	// the family the NUL guard above addresses.
	function tenantSegment(tenantId) {
		return tenantId == null ? '' : String(tenantId);
	}

	function byUserKey(tenantId, userId) {
		return client.key(keyPrefix + 'byuser:' + tenantSegment(tenantId) + '\0' + userId);
	}

	function validateKey(userKey) {
		if (typeof userKey !== 'string' || userKey.length === 0) {
			throw new Error('redis idempotency: key must be a non-empty string');
		}
		if (userKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
			throw new Error('redis idempotency: key must be at most ' + MAX_IDEMPOTENCY_KEY_LENGTH + ' characters');
		}
	}

	return {
		async acquire(userKey, _ttlSec, meta) {
			validateKey(userKey);
			const k = fullKey(userKey);
			// The realtime idempotent wrapper passes the raw (user, tenant) so this
			// committed key can be recorded under the user for right-to-erasure.
			const forgetUser = meta && typeof meta.user === 'string' ? meta.user : null;
			// Coerced, not typeof-filtered: purgeUser(tenant, user) coerces its
			// argument, so dropping a non-string tenant here would index the
			// entry under the untenanted scope while the later erasure looks
			// for it under the tenant, and the purge would silently miss.
			const forgetTenant = meta && meta.tenant != null ? String(meta.tenant) : null;
			if (forgetUser !== null) validateForgetIdentity(forgetTenant, forgetUser);

			// A distinct token per acquire. The pending slot stores this token so
			// commit/abort can compare-and-set/delete against it: an owner whose
			// acquireTtl expired and whose slot a successor re-acquired holds a
			// stale token and can neither overwrite (commit) nor release (abort)
			// the successor's slot.
			const ownerToken = PENDING_PREFIX + ':' + randomUuid();
			const raw = await withBreaker(b, () =>
				evalCached(redis, ACQUIRE_SCRIPT, 1, k, ownerToken, acquireTtl, PENDING_PREFIX)
			);

			const status = raw[0];
			const value = raw[1];
			const isPending = raw[2];

			if (status === 1) {
				mAcquired?.inc();
				return {
					acquired: true,
					async commit(result) {
						const payload = JSON.stringify(result === undefined ? null : result);
						const bytes = Buffer.byteLength(payload);
						if (bytes > maxResultBytes) {
							// Throw BEFORE writing to Redis so the slot stays as
							// the pending token. The caller can decide to
							// abort() (release the slot) or fall through (let
							// the acquireTtl expire); either path keeps the
							// store in a coherent state.
							throw new IdempotencyResultTooLargeError(bytes, maxResultBytes);
						}
						// Resolve the per-field-TTL probe concurrently with the CAS so
						// its (first-call-only) latency hides under the commit round
						// trip; it is only awaited if the CAS wins and an index write
						// follows.
						const hexReady = forgetUser !== null ? hexpireProbe.ready() : null;
						// Compare-and-set: only write the result if this owner still
						// holds the pending slot. If the acquireTtl expired and a
						// successor re-acquired, the token no longer matches and the
						// script leaves their slot untouched (returns 0).
						const won = await withBreaker(b, () =>
							evalCached(redis, COMMIT_SCRIPT, 1, k, ownerToken, payload, ttl)
						);
						if (won !== 1) {
							// The pending slot expired and a successor re-acquired it
							// before this owner committed; refuse rather than clobber.
							throw new IdempotencyLeaseLostError(userKey);
						}
						// Record the committed key under its user for right-to-erasure -
						// only now that the CAS won, so the index never references a key
						// this owner did not commit. Gating on the win is what keeps the
						// Redis backend in line with Postgres, where the user_id lives on
						// the row the token-guarded UPDATE could not have touched on a
						// lost lease: a stale index field here would point at the cache
						// key the SUCCESSOR now owns, so this user's purgeUser would
						// delete another user's result. The cost is one extra round trip
						// on the authenticated-commit path (the CAS and the index cannot
						// share a batch - they hash to different slots and a cross-slot
						// MULTI fails on Redis Cluster); it is a per-effectful-operation
						// commit, not a hot loop. Best-effort: a failure must not fail
						// the already-committed result.
						if (forgetUser !== null) {
							const idxKey = byUserKey(forgetTenant, forgetUser);
							const hexOk = await hexReady;
							const tx = redis.multi();
							tx.hset(idxKey, k, '1');
							// Per-field TTL bounds each index field to its own cache
							// entry's lifetime; the whole-key fallback slides instead,
							// so on old servers a busy user's index is bounded by their
							// LAST commit rather than per entry.
							if (hexOk) tx.hpexpire(idxKey, ttl * 1000, 'FIELDS', 1, k);
							else tx.expire(idxKey, ttl);
							await tx.exec().catch(() => { /* index best-effort; the cache entry is committed */ });
						}
						mCommits?.inc();
					},
					async abort() {
						// Compare-and-delete: release the slot only if this owner
						// still holds it. A token mismatch (a successor re-acquired)
						// is a silent no-op - never delete their slot.
						await withBreaker(b, () =>
							evalCached(redis, ABORT_SCRIPT, 1, k, ownerToken)
						);
						mAborts?.inc();
					}
				};
			}

			if (isPending === 1) {
				mPending?.inc();
				return { acquired: false, pending: true };
			}

			mHits?.inc();
			let parsed;
			try {
				parsed = JSON.parse(value);
			} catch {
				// Stored value is not JSON: treat as missing so the caller can retry.
				return { acquired: false, pending: true };
			}
			return { acquired: false, result: parsed };
		},

		async purge(userKey) {
			validateKey(userKey);
			await withBreaker(b, () => redis.del(fullKey(userKey)));
		},

		/**
		 * Right-to-erasure: delete every cached result this user committed, found
		 * via the byuser SET index. Each cache key is deleted individually so the
		 * cluster routes each DEL to its own slot (a single cross-slot pipeline
		 * would silently no-op). `tenantId` disambiguates the user across tenants.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} cache entries removed
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			validateForgetIdentity(tenantId, userId);
			return withBreaker(b, async () => {
				const idxKey = byUserKey(tenantId, userId);
				const keys = await redis.hkeys(idxKey);
				if (!keys || keys.length === 0) {
					await redis.del(idxKey);
					return 0;
				}
				// Per-key DEL: each key routes to its own slot under cluster mode.
				const results = await Promise.all(keys.map((k) => redis.del(k)));
				await redis.del(idxKey);
				return results.reduce((n, r) => n + (r > 0 ? 1 : 0), 0);
			});
		},

		async clear() {
			await withBreaker(b, () => scanAndUnlink(redis, client.key(keyPrefix + '*')));
		},

		// Symmetry with the Postgres idempotency store. The Redis backend
		// has no DDL to run, so this resolves immediately. Provided so
		// callers can write generic boot code: `await store.ready()`
		// regardless of which backend is wired.
		ready() {
			return Promise.resolve();
		}
	};
}
