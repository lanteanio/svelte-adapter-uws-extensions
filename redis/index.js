/**
 * Redis client factory for svelte-adapter-uws-extensions.
 *
 * Wraps ioredis with lifecycle management and graceful shutdown
 * via the SvelteKit `sveltekit:shutdown` event.
 *
 * @module svelte-adapter-uws-extensions/redis
 */

import Redis from 'ioredis';
import { ConnectionError } from '../shared/errors.js';

/**
 * @typedef {Object} RedisClientOptions
 * @property {string} [url='redis://localhost:6379'] - Redis connection URL
 * @property {string} [keyPrefix=''] - Prefix for all keys written by extensions
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect
 * @property {import('ioredis').RedisOptions} [options] - Extra ioredis options (merged on top of URL parsing)
 */

/**
 * @typedef {Object} RedisClient
 * @property {import('ioredis').Redis} redis - The underlying ioredis instance
 * @property {string} keyPrefix - The key prefix
 * @property {(key: string) => string} key - Prefix a key: `keyPrefix + key`
 * @property {(overrides?: import('ioredis').RedisOptions) => import('ioredis').Redis} duplicate - Create a new connection with the same config (for subscribers)
 * @property {() => Promise<void>} quit - Gracefully disconnect
 */

/**
 * Create a Redis client.
 *
 * @param {RedisClientOptions} [options]
 * @returns {RedisClient}
 *
 * @example
 * ```js
 * import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
 *
 * export const redis = createRedisClient({
 *   url: 'redis://localhost:6379',
 *   keyPrefix: 'myapp:'
 * });
 * ```
 */
export function createRedisClient(options = {}) {
	const url = options.url || 'redis://localhost:6379';
	const keyPrefix = options.keyPrefix || '';
	const autoShutdown = options.autoShutdown !== false;

	/** @type {import('ioredis').RedisOptions} */
	const redisOpts = {
		maxRetriesPerRequest: null,
		enableReadyCheck: true,
		lazyConnect: false,
		...(options.options || {})
	};

	let redis;
	try {
		redis = new Redis(url, redisOpts);
	} catch (err) {
		throw new ConnectionError('redis', `failed to create client for ${url}`, err);
	}

	// Track duplicate connections for cleanup
	/** @type {import('ioredis').Redis[]} */
	const duplicates = [];

	/** @type {boolean} */
	let shuttingDown = false;

	async function quit() {
		if (shuttingDown) return;
		shuttingDown = true;
		const all = [redis, ...duplicates];
		await Promise.allSettled(all.map((c) => c.quit().catch(() => c.disconnect())));
	}

	if (autoShutdown && typeof process !== 'undefined') {
		process.once('sveltekit:shutdown', quit);
	}

	return {
		redis,
		keyPrefix,

		key(k) {
			return keyPrefix + k;
		},

		duplicate(overrides) {
			const dup = overrides ? redis.duplicate(overrides) : redis.duplicate();
			duplicates.push(dup);
			return dup;
		},

		quit
	};
}
