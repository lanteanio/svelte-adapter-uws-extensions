import type { RedisClient } from './index.js';

export interface RedisFenceOptions {
	/** Prefix prepended (after the client keyPrefix) to every fence key. @default 'fence:' */
	keyPrefix?: string;
}

/**
 * Fence provider contract consumed by the Postgres task runner via the
 * `fence` option. All three methods are called by the runner's state
 * machine; the runner pairs them with its own Postgres operations.
 */
export interface FenceProvider {
	/** Set the fence in the external store with the given TTL. Called once per attempt right after the Postgres row is inserted or rearmed. */
	acquire(taskId: string, fence: string, ttlSec: number): Promise<void>;
	/** Refresh the fence's TTL conditionally on value match. Returns true if still owned, false if lost. */
	heartbeat(taskId: string, fence: string, ttlSec: number): Promise<boolean>;
	/** Delete the fence (conditional on value match) when the attempt commits, fails terminally, or is aborted. */
	release(taskId: string, fence: string): Promise<void>;
}

export interface RedisFenceProvider extends FenceProvider {}

/**
 * Create a Redis fence provider for the Postgres task runner. The
 * Postgres row remains the canonical record of task state; this
 * provider mirrors the fence value to a Redis key with a short TTL
 * refreshed by heartbeat. The runner consults both sources on every
 * heartbeat tick; either reporting "lost" aborts the handler.
 */
export function createRedisFence(client: RedisClient, options?: RedisFenceOptions): RedisFenceProvider;
