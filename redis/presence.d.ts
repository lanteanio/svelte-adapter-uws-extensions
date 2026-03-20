import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisPresenceOptions {
	/** Field in selected data for user dedup. @default 'id' */
	key?: string;
	/** Extract public fields from userData. @default identity */
	select?: (userData: any) => Record<string, any>;
	/** Heartbeat interval in ms (refresh TTL). @default 30000 */
	heartbeat?: number;
	/** TTL in seconds for presence hash entries. @default 90 */
	ttl?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface RedisPresenceTracker {
	/**
	 * Add a connection to a topic's presence.
	 * Ignores `__`-prefixed topics. Idempotent.
	 */
	join(ws: any, topic: string, platform: Platform): Promise<void>;

	/** Remove a connection from a specific topic, or all topics if omitted. */
	leave(ws: any, platform: Platform, topic?: string): Promise<void>;

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

	/**
	 * Ready-made WebSocket hooks for zero-config presence.
	 *
	 * `subscribe` handles both regular topics (calls `join`) and `__presence:*`
	 * topics (calls `sync` so the client gets the current list immediately).
	 * `unsubscribe` removes presence from a single topic when the client
	 * unsubscribes (requires core adapter v0.4.0+).
	 * `close` calls `leave`.
	 *
	 * @example
	 * ```js
	 * import { presence } from '$lib/server/presence';
	 * export const { subscribe, unsubscribe, close } = presence.hooks;
	 * ```
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		unsubscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a Redis-backed presence tracker.
 */
export function createPresence(client: RedisClient, options?: RedisPresenceOptions): RedisPresenceTracker;
