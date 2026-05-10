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
 *   - Pending: the literal sentinel string (raw, not JSON).
 *   - Committed: JSON.stringify(result) (always begins with a valid JSON token).
 * The two cases are distinguishable because the bare sentinel is not a
 * valid JSON value on its own and never equals JSON.stringify of any user
 * payload.
 *
 * @module svelte-adapter-uws-extensions/redis/idempotency
 */

import { scanAndUnlink } from '../shared/redis-scan.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_IDEMPOTENCY_KEY_LENGTH } from '../shared/caps.js';

/**
 * Lua script for atomic acquire.
 *
 * KEYS[1] = idempotency key
 * ARGV[1] = pending sentinel
 * ARGV[2] = acquireTtl (seconds)
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
if v == ARGV[1] then
  return {0, '', 1}
end
return {0, v, 0}
`;

const PENDING_SENTINEL = '__idem_pending__';

/**
 * @typedef {Object} RedisIdempotencyOptions
 * @property {string} [keyPrefix='idem:'] - Prefix prepended (after the client keyPrefix) to every key.
 * @property {number} [ttl=172800] - Result cache lifetime in seconds. Default 48 hours.
 * @property {number} [acquireTtl=60] - Pending-slot lifetime in seconds (anti-deadlock). Default 60 seconds.
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
 * @property {(key: string) => Promise<IdempotencySlot>} acquire
 * @property {(key: string) => Promise<void>} purge
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
	if (options.keyPrefix !== undefined && typeof options.keyPrefix !== 'string') {
		throw new Error('redis idempotency: keyPrefix must be a string');
	}

	const keyPrefix = options.keyPrefix !== undefined ? options.keyPrefix : 'idem:';
	const ttl = options.ttl || 48 * 3600;
	const acquireTtl = options.acquireTtl || 60;
	const redis = client.redis;

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

	function validateKey(userKey) {
		if (typeof userKey !== 'string' || userKey.length === 0) {
			throw new Error('redis idempotency: key must be a non-empty string');
		}
		if (userKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
			throw new Error('redis idempotency: key must be at most ' + MAX_IDEMPOTENCY_KEY_LENGTH + ' characters');
		}
	}

	return {
		async acquire(userKey) {
			validateKey(userKey);
			const k = fullKey(userKey);

			const raw = await withBreaker(b, () =>
				redis.eval(ACQUIRE_SCRIPT, 1, k, PENDING_SENTINEL, acquireTtl)
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
						await withBreaker(b, () => redis.set(k, payload, 'EX', ttl));
						mCommits?.inc();
					},
					async abort() {
						await withBreaker(b, () => redis.del(k));
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
