import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface ShardedBusOptions {
	/**
	 * Prefix for sharded pub/sub channels. Each topic maps to
	 * `channelPrefix + shardKey(topic)`.
	 *
	 * @default 'uws:sharded:'
	 */
	channelPrefix?: string;
	/**
	 * Map a topic to a shard label. The channel is
	 * `channelPrefix + shardKey(topic)`. Default is identity --
	 * each topic gets its own channel. Use a coarser shardKey
	 * (e.g. `(topic) => topic.split(':')[0]`) when you want
	 * multiple topics to share a single channel.
	 */
	shardKey?: (topic: string) => string;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface ShardedBus {
	/**
	 * Returns a Platform whose `publish()` and `batch()` send to
	 * Redis via SPUBLISH (and to the local platform). Mirrors the
	 * shape of `createPubSubBus().wrap(platform)`.
	 */
	wrap(platform: Platform): Platform;

	/**
	 * Open the subscriber connection. Runs `INFO server` and throws
	 * if the Redis version is older than 7. Idempotent.
	 */
	activate(platform: Platform): Promise<void>;

	/** Close the subscriber and release tracked state. */
	deactivate(): Promise<void>;

	/**
	 * Increment the follower count for `topic` and SSUBSCRIBE the
	 * underlying channel if this is the first follower for that
	 * channel. Must be called after `activate()`.
	 */
	follow(topic: string): Promise<void>;

	/**
	 * Decrement the follower count for `topic`. SUNSUBSCRIBE the
	 * channel if this was the last follower of any topic mapping to
	 * it.
	 */
	unfollow(topic: string): Promise<void>;

	/**
	 * Ready-made WebSocket hooks. `subscribe` calls `follow`,
	 * `unsubscribe` calls `unfollow`, `close` unfollows every topic
	 * the connection had subscribed to.
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		unsubscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a sharded Redis pub/sub bus. Requires Redis 7+ for
 * SPUBLISH / SSUBSCRIBE; use `createPubSubBus` on older servers.
 */
export function createShardedBus(client: RedisClient, options?: ShardedBusOptions): ShardedBus;
