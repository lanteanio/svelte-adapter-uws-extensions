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
 *   - Committed: a versioned envelope retaining the owner token and JSON result.
 *     Legacy JSON results remain readable; old writers must be drained on upgrade.
 * Distinct non-JSON prefixes separate pending ownership and versioned results.
 *
 * @module svelte-adapter-uws-extensions/redis/idempotency
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { RESULT_PREFIX, ACQUIRE_SCRIPT, COMMIT_SCRIPT, ABORT_SCRIPT, OWNERSHIP_SCRIPT, ERASE_SCRIPT, LEGACY_ERASE_SCRIPT } from './_idempotency-ownership.js';
import { evalCached } from '../shared/eval-cached.js';
import { createHashFieldTTLProbe } from '../shared/redis-version.js';
import { withBreaker } from '../shared/breaker.js';
import { randomUuid } from '../shared/runtime.js';
import { MAX_IDEMPOTENCY_KEY_LENGTH } from '../shared/caps.js';
import { IdempotencyResultTooLargeError, IdempotencyLeaseLostError } from '../shared/errors.js';

export { IdempotencyResultTooLargeError, IdempotencyLeaseLostError };

const DEFAULT_MAX_RESULT_BYTES = 256 * 1024;

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

	// A v2 per-user hash records every pending owner before primary acquisition.
	// Fields carry owner tokens, values carry state + full primary key. Unarmed
	// registrations keep the hash persistent; acknowledged claims use a safe
	// primary-lifetime bound. Old one-NUL indexes remain readable for migration.
	// Neither tenant nor user may contain NUL, preserving both identity and
	// version boundaries without changing Redis Cluster routing restrictions.
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

	function legacyByUserKey(tenantId, userId) {
		return client.key(keyPrefix + 'byuser:' + tenantSegment(tenantId) + '\0' + userId);
	}

	function byUserKey(tenantId, userId) {
		// Two NUL delimiters make this disjoint from every legacy one-NUL index.
		return client.key(keyPrefix + 'byuser:v2\0' + tenantSegment(tenantId) + '\0' + userId);
	}

	function validateKey(userKey) {
		if (typeof userKey !== 'string' || userKey.length === 0) {
			throw new Error('redis idempotency: key must be a non-empty string');
		}
		if (userKey.startsWith('byuser:')) {
			throw new Error('redis idempotency: key must not start with reserved prefix byuser:');
		}
		if (userKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
			throw new Error('redis idempotency: key must be at most ' + MAX_IDEMPOTENCY_KEY_LENGTH + ' characters');
		}
	}

	return {
		async acquire(userKey, ttlSec, meta) {
			validateKey(userKey);
			const resultTtl = ttlSec === undefined ? ttl : ttlSec;
			if (!Number.isInteger(resultTtl) || resultTtl < 0) {
				throw new Error('redis idempotency: ttlSec must be a non-negative integer');
			}
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
			const idxKey = forgetUser !== null ? byUserKey(forgetTenant, forgetUser) : null;
			const ownership = (op, ...args) => withBreaker(b, () =>
				evalCached(redis, OWNERSHIP_SCRIPT, 1, idxKey, op, ownerToken, k, ...args));
			let raw;
			if (idxKey !== null) {
				// Cache hits need no new owner registration. A miss is only a hint:
				// the later SET NX still decides ownership atomically.
				const existing = await withBreaker(b, () => redis.get(k));
				if (existing !== null) raw = existing.startsWith(PENDING_PREFIX) ? [0, '', 1] : [0, existing, 0];
			}
			if (raw === undefined) {
				if (idxKey !== null && await ownership('register') !== 1) throw new IdempotencyLeaseLostError(userKey);
				// A command error can have an ambiguous outcome. Keep its durable
				// registration so a later purge can retry erasing the primary.
				raw = await withBreaker(b, () =>
					evalCached(redis, ACQUIRE_SCRIPT, 1, k, ownerToken, acquireTtl, PENDING_PREFIX));
				if (idxKey !== null) {
					if (raw[0] === 1) {
						const hexOk = await hexpireProbe.ready();
						// Start the retention bound AFTER primary SET acknowledgement.
						// If purge revoked registration during a delayed SET, never
						// return a handle that could run work or commit afterward.
						const armed = await ownership('arm', (acquireTtl + resultTtl) * 1000, hexOk ? '1' : '0');
						if (armed !== 1) {
							await withBreaker(b, () => evalCached(redis, ABORT_SCRIPT, 1, k, ownerToken));
							throw new IdempotencyLeaseLostError(userKey);
						}
					} else await ownership('drop');
				}
			}

			const status = raw[0];
			const value = raw[1];
			const isPending = raw[2];

			if (status === 1) {
				mAcquired?.inc();
				return {
					acquired: true,
					async commit(result) {
						if (resultTtl === 0) {
							const won = await withBreaker(b, () => evalCached(redis, ABORT_SCRIPT, 1, k, ownerToken));
							if (won !== 1) throw new IdempotencyLeaseLostError(userKey);
							if (idxKey !== null) await ownership('drop');
							mCommits?.inc();
							return;
						}
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
						const envelope = RESULT_PREFIX + '[' + JSON.stringify(ownerToken) + ',' + payload + ']';
						// Purge deletes by owner token on this same primary slot. If
						// it wins before a delayed commit, this CAS cannot recreate
						// the key; if commit wins first, purge recognizes its owner.
						const won = await withBreaker(b, () =>
							evalCached(redis, COMMIT_SCRIPT, 1, k, ownerToken, envelope, resultTtl));
						if (won !== 1) throw new IdempotencyLeaseLostError(userKey);
						mCommits?.inc();
					},
					async abort() {
						// Compare-and-delete: release the slot only if this owner
						// still holds it. A token mismatch (a successor re-acquired)
						// is a silent no-op - never delete their slot.
						const removed = await withBreaker(b, () =>
							evalCached(redis, ABORT_SCRIPT, 1, k, ownerToken)
						);
						// An abort after commit must not drop that result's index.
						if (removed === 1 && idxKey !== null) await ownership('drop');
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
				if (typeof value === 'string' && value.startsWith(RESULT_PREFIX)) {
					const envelope = JSON.parse(value.slice(RESULT_PREFIX.length));
					if (!Array.isArray(envelope) || envelope.length !== 2 || typeof envelope[0] !== 'string') throw new Error('invalid idempotency result envelope');
					parsed = envelope[1];
				} else parsed = JSON.parse(value);
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
		 * Right-to-erasure: revoke pending owners and delete their cached results.
		 * Index revocation and each owner-checked primary deletion run separately
		 * on their respective Redis slots. Failures retain retryable ownership.
		 * `tenantId` disambiguates the user across tenants.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} pending or committed primary entries removed
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			validateForgetIdentity(tenantId, userId);
			return withBreaker(b, async () => {
				const idxKey = byUserKey(tenantId, userId);
				const legacyKey = legacyByUserKey(tenantId, userId);
				const legacyKeys = await redis.hkeys(legacyKey);
				const prefix = client.key(keyPrefix);
				for (const key of legacyKeys) {
					if (!key.startsWith(prefix)) throw new Error('redis idempotency: invalid legacy index key');
					validateKey(key.slice(prefix.length));
				}
				const claims = await evalCached(redis, OWNERSHIP_SCRIPT, 1, idxKey, 'revoke', '', '');
				let removed = 0;
				// Revoke first, then delete by owner on each primary slot. Retain
				// every unacknowledged deletion for a retry; never DEL the whole
				// index because another acquisition may have registered meanwhile.
				let next = 0, failed = false, failure;
				// Bound cross-slot work. Stop admitting owners on the first failure,
				// then await every admitted chain before reporting that failure.
				await Promise.all(Array.from({ length: Math.min(8, claims.length / 2) }, async () => {
					while (!failed && next < claims.length) {
						const i = next;
						next += 2;
						const owner = claims[i], key = claims[i + 1];
						try {
							const count = Number(await evalCached(redis, ERASE_SCRIPT, 1, key, owner, RESULT_PREFIX));
							removed += count;
							await evalCached(redis, OWNERSHIP_SCRIPT, 1, idxKey, 'drop', owner, key);
						} catch (error) {
							if (!failed) { failed = true; failure = error; }
						}
					}
				}));
				if (failed) throw failure;
				for (const key of legacyKeys) {
					const value = await redis.get(key);
					if (value !== null) removed += Number(await evalCached(redis, LEGACY_ERASE_SCRIPT, 1, key, value, RESULT_PREFIX, PENDING_PREFIX));
					await redis.hdel(legacyKey, key);
				}
				return removed;
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
