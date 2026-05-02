import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface SubscribersAggregatorLike {
	subscribersOf(topic: string): number;
}

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
	/**
	 * Optional aggregator (typically from `redis/publish-rate`) that
	 * provides cluster-wide subscriber counts. When wired,
	 * `bus.subscribers(topic)` returns the cluster-wide count;
	 * otherwise it returns the local count only.
	 *
	 * Wire `bus.localSubjects(platform)` as the aggregator's
	 * `subjects` source so this bus's followed topics contribute to
	 * the cluster broadcast.
	 */
	subscribersAggregator?: SubscribersAggregatorLike;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface ShardedBus {
	/**
	 * Returns a Platform whose `publish()` / `batch()` / `publishBatched()`
	 * send to Redis via SPUBLISH (and to the local platform). Other Platform
	 * methods (`send`, `sendCoalesced`, `request`, `pressure`, etc.) pass
	 * through unchanged. Mirrors the shape of `createPubSubBus().wrap(platform)`.
	 *
	 * `publishBatched` groups its message list by shard channel and ships one
	 * SPUBLISH envelope per channel per call. Receivers fan out via local
	 * `platform.publishBatched` so each subscriber sees one wire frame per
	 * batched envelope.
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
	 * Bulk follow. Groups input topics by shard channel and SSUBSCRIBE
	 * the new channels in one round trip. Refcount semantics for
	 * individual topics match `follow`: each call bumps every input
	 * topic's refcount by 1, and only channel transitions trigger Redis
	 * traffic. Empty arrays no-op; duplicate topics in the input collapse
	 * to one refcount bump.
	 *
	 * Pairs with the adapter's `subscribeBatch` hook so an N-topic
	 * subscribe batch lands as one round-trip-per-channel rather than
	 * one round-trip-per-topic. With the adapter's next.7 client-side
	 * coalescing, the win covers initial-mount subscribes too, not just
	 * reconnect resubscribes.
	 */
	followBatch(topics: string[]): Promise<void>;

	/**
	 * Decrement the follower count for `topic`. SUNSUBSCRIBE the
	 * channel if this was the last follower of any topic mapping to
	 * it.
	 */
	unfollow(topic: string): Promise<void>;

	/**
	 * Snapshot every topic this bus is currently following with its
	 * local subscriber count from the supplied platform. Topics with 0
	 * local subscribers are omitted. Use as the `subjects` callback on
	 * `createPublishRateAggregator` so the aggregator can broadcast
	 * subscriber counts cluster-wide.
	 *
	 * Topics subscribed outside the bus's hooks (raw `ws.subscribe`
	 * bypass) are not enumerated; they will not propagate cluster-wide
	 * via this path.
	 */
	localSubjects(platform: Platform): Array<{ topic: string; count: number }>;

	/**
	 * Cluster-wide subscriber count for a topic. Returns
	 * `aggregator.subscribersOf(topic)` when a `subscribersAggregator`
	 * was wired; otherwise returns the local count
	 * (`platform.subscribers(topic)`).
	 *
	 * Eventually-consistent within the aggregator's `publishInterval`
	 * for the remote contribution; the local read is always live. For
	 * exact counts, track a Redis SET cluster-wide and `SCARD` it.
	 */
	subscribers(topic: string): number;

	/**
	 * Ready-made WebSocket hooks. `subscribe` calls `follow`,
	 * `subscribeBatch` calls `followBatch` (skipping `__`-prefixed
	 * topics like `subscribe` does), `unsubscribe` calls `unfollow`,
	 * `close` unfollows every topic the connection had subscribed to.
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		subscribeBatch(ws: any, topics: string[], ctx: { platform: Platform }): Promise<void>;
		unsubscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a sharded Redis pub/sub bus. Requires Redis 7+ for
 * SPUBLISH / SSUBSCRIBE; use `createPubSubBus` on older servers.
 */
export function createShardedBus(client: RedisClient, options?: ShardedBusOptions): ShardedBus;
