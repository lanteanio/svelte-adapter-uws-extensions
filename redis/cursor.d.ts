import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisCursorOptions {
	/**
	 * Minimum ms between broadcasts per user per topic.
	 * Trailing-edge timer ensures the final position is always sent.
	 * Default 16 (60Hz) matches the world-state tick rate so an individual
	 * cursor's motion stays smooth at the per-peer wire rate.
	 * @default 16
	 */
	throttle?: number;

	/**
	 * World-state tick rate, in ms. Per-topic aggregate cap on broadcasts:
	 * each topic emits at most one frame per window, carrying the latest
	 * position for every cursor that moved. Bandwidth per peer scales with
	 * active-mover count, not with mover-count times per-mover rate.
	 * Default 16 (60Hz) suits typical small-to-medium rooms; raise to 33
	 * (30Hz) for high-density rooms where wire bytes matter.
	 * 0 disables the tick; per-cursor `throttle` then governs broadcast rate.
	 * @default 16
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
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
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
	 * Send all current cursor positions for a topic to a single connection.
	 * Sends a `bulk` event on `__cursor:{topic}` with the full cursor list.
	 * Called automatically by `hooks.subscribe` when a client subscribes.
	 */
	snapshot(ws: any, topic: string, platform: Platform): Promise<void>;

	/**
	 * Get current cursor positions for a topic across all instances.
	 */
	list(topic: string): Promise<CursorEntry[]>;

	/** Clear all cursor state (local and Redis). */
	clear(): Promise<void>;

	/** Stop the Redis subscriber and clear local timers. */
	destroy(): void;

	/**
	 * Ready-made WebSocket hooks for zero-config cursor tracking.
	 *
	 * `subscribe` sends a snapshot of existing cursors when a client subscribes
	 * to a `__cursor:*` topic.
	 * `message` handles incoming `{ type: 'cursor', topic, data }` messages.
	 * `close` removes the connection's cursors from all topics.
	 *
	 * @example
	 * ```js
	 * import { cursor } from '$lib/server/cursor';
	 * export const { subscribe, message, close } = cursor.hooks;
	 * ```
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void> | void;
		message(ws: any, ctx: { data: any; platform: Platform }): void;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a Redis-backed cursor tracker.
 */
export function createCursor(client: RedisClient, options?: RedisCursorOptions): RedisCursorTracker;
