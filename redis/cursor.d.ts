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

	/**
	 * Reject inbound cursor envelopes larger than this many bytes before
	 * `JSON.parse` runs. The inner topic is always validated against the
	 * `__` denylist (the module constructs its own `__cursor:` wrapper
	 * prefix), so no `allowSystemTopics` knob is exposed here.
	 * @default 1048576 (1 MB)
	 */
	maxEnvelopeBytes?: number;
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
	 * Opt this connection into receiving cursor updates for `topic`.
	 * Subscribes the connection to the internal `__cursor:` channel via
	 * the platform-trust path (which intentionally bypasses the wire-level
	 * `__`-prefix gate) and sends the current cursor state. Call from your
	 * "join room" RPC, mirroring `presence.join`.
	 *
	 * Without `attach`, the publishes in `update` fan out to an empty
	 * subscriber set and no client ever sees a cursor frame.
	 */
	attach(ws: any, topic: string, platform: Platform): Promise<void>;

	/**
	 * Stop this connection from receiving cursor updates for `topic`. Safe
	 * to call on a closed connection. uWS releases subscriptions on
	 * disconnect automatically, so `detach` is only needed when a
	 * still-connected user leaves a room.
	 */
	detach(ws: any, topic: string, platform: Platform): void;

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
	 * Folded into `attach` for typical use; exposed for advanced callers
	 * that want to resend a snapshot without re-subscribing.
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
	 * Ready-made WebSocket hooks for cursor tracking.
	 *
	 * `message` handles incoming `{ type: 'cursor', topic, data }` messages.
	 * `close` removes the connection's cursors from all topics.
	 *
	 * `subscribe` is a no-op under current adapters: the adapter's wire-level
	 * `__`-prefix gate denies any inbound `__cursor:*` subscribe frame, so
	 * this hook never fires. Kept for backward source-compat; new code should
	 * call `tracker.attach(ws, topic, platform)` from the app's "join room"
	 * RPC instead.
	 *
	 * @example
	 * ```js
	 * import { cursor } from '$lib/server/cursor';
	 * export const { message, close } = cursor.hooks;
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
