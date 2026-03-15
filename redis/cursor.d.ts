import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';

export interface RedisCursorOptions {
	/**
	 * Minimum ms between broadcasts per user per topic.
	 * Trailing-edge timer ensures the final position is always sent.
	 * @default 50
	 */
	throttle?: number;

	/**
	 * Minimum ms between aggregate broadcasts per topic, across all
	 * connections. Caps total Redis writes regardless of connection count.
	 * Set to ~16 (60 broadcasts/sec) to prevent Redis saturation under
	 * high concurrency. 0 disables the aggregate throttle.
	 * @default 0
	 */
	topicThrottle?: number;

	/**
	 * Extract user-identifying data from userData.
	 * Broadcast alongside cursor data so other clients know who the cursor belongs to.
	 * @default identity
	 */
	select?: (userData: any) => any;

	/**
	 * TTL in seconds for Redis hash entries.
	 * Entries are refreshed on every broadcast. Stale cursors from crashed
	 * instances are cleaned up automatically after this period.
	 * @default 30
	 */
	ttl?: number;
}

export interface CursorEntry {
	/** Unique connection key. */
	key: string;
	/** Selected user data. */
	user: any;
	/** Latest cursor/position data. */
	data: any;
}

export interface RedisCursorTracker {
	/**
	 * Broadcast a cursor position update. Throttled per user per topic.
	 * Call this from your `message` hook when you receive cursor data.
	 */
	update(ws: any, topic: string, data: any, platform: Platform): void;

	/**
	 * Remove a connection's cursor state from a specific topic, or all topics if omitted.
	 * Call this from your `close` hook.
	 */
	remove(ws: any, platform: Platform, topic?: string): Promise<void>;

	/**
	 * Get current cursor positions for a topic across all instances.
	 */
	list(topic: string): Promise<CursorEntry[]>;

	/** Clear all cursor state (local and Redis). */
	clear(): Promise<void>;

	/** Stop the Redis subscriber and clear local timers. */
	destroy(): void;
}

/**
 * Create a Redis-backed cursor tracker.
 */
export function createCursor(client: RedisClient, options?: RedisCursorOptions): RedisCursorTracker;
