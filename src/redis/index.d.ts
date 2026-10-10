import type { Redis, RedisOptions } from 'ioredis';

export interface RedisClientOptions {
	/** Redis connection URL. @default 'redis://localhost:6379' */
	url?: string;
	/** Prefix for all keys written by extensions. @default '' */
	keyPrefix?: string;
	/** Listen for `sveltekit:shutdown` and disconnect. @default true */
	autoShutdown?: boolean;
	/** Override ioredis defaults: one retry, 5s command/connect/socket deadlines, bounded startup buffering and no offline buffering after first ready. */
	options?: RedisOptions;
}

export interface RedisClient {
	/** The underlying ioredis instance. */
	readonly redis: Redis;
	/** The key prefix. */
	readonly keyPrefix: string;
	/** Prefix a key: `keyPrefix + key`. */
	key(key: string): string;
	/** Create a subscriber connection with bounded offline buffering and the same finite deadlines. Pass overrides to change options (e.g. `{ enableReadyCheck: false }` for subscriber clients). */
	duplicate(overrides?: RedisOptions): Redis;
	/** Gracefully disconnect all connections. */
	quit(): Promise<void>;
}

/**
 * Create a Redis client with lifecycle management.
 */
export function createRedisClient(options?: RedisClientOptions): RedisClient;
