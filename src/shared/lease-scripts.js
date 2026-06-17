/**
 * Shared Lua scripts for Redis lease primitives.
 *
 * Both `redis/lock` (request-scoped `withLock(fn)`) and `redis/leader`
 * (long-lived `isLeader()` observer) implement leases on top of
 * `SET key value NX PX ttlMs`. While the holder is alive, a periodic
 * tick refreshes the TTL via compare-and-pexpire; on release, a
 * compare-and-delete clears the key only if we still own it.
 *
 * Centralized here so the two consumers can't drift on the value-guard
 * semantics. Both scripts are O(1) and Redis-cluster-safe (single key).
 *
 * @module svelte-adapter-uws-extensions/shared/lease-scripts
 */

/**
 * Compare-and-pexpire: refresh the TTL only if the value still matches.
 *
 * KEYS[1] = lease key
 * ARGV[1] = expected value (this holder's identity)
 * ARGV[2] = new TTL in milliseconds
 *
 * Returns 1 if still owned (TTL refreshed), 0 if not (key absent or
 * holds a different value).
 */
export const LEASE_RENEW_SCRIPT = `
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

/**
 * Compare-and-delete: clear the key only if the value still matches.
 *
 * KEYS[1] = lease key
 * ARGV[1] = expected value (this holder's identity)
 *
 * Returns 1 if released, 0 if not.
 */
export const LEASE_RELEASE_SCRIPT = `
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;
