import type { Redis, RedisOptions } from 'ioredis';

export interface RedisClientOptions {
	/** Redis connection URL. @default 'redis://localhost:6379' */
	url?: string;
	/** Prefix for all keys written by extensions. @default '' */
	keyPrefix?: string;
	/** Listen for `sveltekit:shutdown` and disconnect. @default true */
	autoShutdown?: boolean;
	/** Extra ioredis options (merged on top of URL parsing). */
	options?: RedisOptions;
}

export interface RedisClient {
	/** The underlying ioredis instance. */
	readonly redis: Redis;
	/** The key prefix. */
	readonly keyPrefix: string;
	/** Prefix a key: `keyPrefix + key`. */
	key(key: string): string;
	/** Create a new connection with the same config (for subscribers). */
	duplicate(): Redis;
	/** Gracefully disconnect all connections. */
	quit(): Promise<void>;
}

/**
 * Create a Redis client with lifecycle management.
 */
export function createRedisClient(options?: RedisClientOptions): RedisClient;
