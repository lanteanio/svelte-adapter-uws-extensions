import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';

export interface RedisPresenceOptions {
	/** Field in selected data for user dedup. @default 'id' */
	key?: string;
	/** Extract public fields from userData. @default identity */
	select?: (userData: any) => Record<string, any>;
	/** Heartbeat interval in ms (refresh TTL). @default 30000 */
	heartbeat?: number;
	/** TTL in seconds for presence hash entries. @default 90 */
	ttl?: number;
}

export interface RedisPresenceTracker {
	/**
	 * Add a connection to a topic's presence.
	 * Ignores `__`-prefixed topics. Idempotent.
	 */
	join(ws: any, topic: string, platform: Platform): Promise<void>;

	/** Remove a connection from all topics. */
	leave(ws: any, platform: Platform): Promise<void>;

	/** Send current presence list without joining. */
	sync(ws: any, topic: string, platform: Platform): Promise<void>;

	/** Get the current presence list for a topic. */
	list(topic: string): Promise<Record<string, any>[]>;

	/** Get the number of unique users present on a topic. */
	count(topic: string): Promise<number>;

	/** Clear all presence state. */
	clear(): Promise<void>;

	/** Stop heartbeat timer and Redis subscriber. */
	destroy(): void;
}

/**
 * Create a Redis-backed presence tracker.
 */
export function createPresence(client: RedisClient, options?: RedisPresenceOptions): RedisPresenceTracker;
