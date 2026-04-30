/**
 * Redis-backed fence provider for the Postgres task runner.
 *
 * Plugs into `createTaskRunner({ fence })` to add a second source of
 * truth for "is this attempt's fence still alive." The Postgres row
 * remains the canonical record of task state; this provider stores a
 * mirror of the fence value in Redis under a short TTL, refreshed by
 * heartbeat. Two layers of fence-loss detection then run on every tick:
 * the runner's Postgres conditional UPDATE plus this provider's Redis
 * value-equality check. Either reporting "lost" aborts the handler.
 *
 * The primary value is force-takeover detection. If an operator manually
 * deletes the fence key in Redis (or another instance forces it via
 * `release`), the heartbeat sees the divergence and bails immediately,
 * even if the Postgres row's fence_expires_at would still pass.
 *
 * @module svelte-adapter-uws-extensions/redis/fence
 */

/**
 * Lua: refresh the fence's TTL only if our value still owns the key.
 *
 * KEYS[1] = fence key
 * ARGV[1] = expected fence value
 * ARGV[2] = new TTL in milliseconds
 *
 * Returns 1 if still owned (TTL refreshed), 0 if not (key absent or
 * holds a different fence).
 */
const HEARTBEAT_SCRIPT = `
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`;

/**
 * Lua: delete the fence key only if our value still owns it.
 *
 * KEYS[1] = fence key
 * ARGV[1] = expected fence value
 *
 * Returns 1 if released, 0 if not (key absent or holds a different
 * fence).
 */
const RELEASE_SCRIPT = `
local v = redis.call('GET', KEYS[1])
if v == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`;

/**
 * @typedef {Object} RedisFenceOptions
 * @property {string} [keyPrefix='fence:'] - Prefix prepended (after the client keyPrefix) to every fence key.
 */

/**
 * @typedef {Object} RedisFenceProvider
 * @property {(taskId: string, fence: string, ttlSec: number) => Promise<void>} acquire
 * @property {(taskId: string, fence: string, ttlSec: number) => Promise<boolean>} heartbeat
 * @property {(taskId: string, fence: string) => Promise<void>} release
 */

/**
 * Create a Redis fence provider. Pass the returned object as the
 * `fence` option of `createTaskRunner`.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisFenceOptions} [options]
 * @returns {RedisFenceProvider}
 */
export function createRedisFence(client, options = {}) {
	if (options.keyPrefix !== undefined && typeof options.keyPrefix !== 'string') {
		throw new Error('redis fence: keyPrefix must be a string');
	}
	const keyPrefix = options.keyPrefix !== undefined ? options.keyPrefix : 'fence:';
	const redis = client.redis;

	function fullKey(taskId) {
		if (typeof taskId !== 'string' || taskId.length === 0) {
			throw new Error('redis fence: taskId must be a non-empty string');
		}
		return client.key(keyPrefix + taskId);
	}

	function validateFence(fence) {
		if (typeof fence !== 'string' || fence.length === 0) {
			throw new Error('redis fence: fence must be a non-empty string');
		}
	}

	function validateTtl(ttlSec) {
		if (!Number.isInteger(ttlSec) || ttlSec < 1) {
			throw new Error('redis fence: ttlSec must be a positive integer');
		}
	}

	return {
		async acquire(taskId, fence, ttlSec) {
			const key = fullKey(taskId);
			validateFence(fence);
			validateTtl(ttlSec);
			await redis.set(key, fence, 'EX', ttlSec);
		},

		async heartbeat(taskId, fence, ttlSec) {
			const key = fullKey(taskId);
			validateFence(fence);
			validateTtl(ttlSec);
			const r = await redis.eval(HEARTBEAT_SCRIPT, 1, key, fence, ttlSec * 1000);
			return Number(r) === 1;
		},

		async release(taskId, fence) {
			const key = fullKey(taskId);
			validateFence(fence);
			await redis.eval(RELEASE_SCRIPT, 1, key, fence);
		}
	};
}
