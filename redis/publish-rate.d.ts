import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface PublishRateAggregatorOptions {
	/**
	 * Redis pub/sub channel for slice broadcasts.
	 * @default 'uws:pressure:rates'
	 */
	channel?: string;

	/**
	 * How often this instance broadcasts its slice (ms).
	 * @default 5000
	 */
	publishInterval?: number;

	/**
	 * Drop a remote instance's slice from the merge if no fresher one
	 * arrives within this window (ms). Should be at least `2 *
	 * publishInterval` to tolerate a missed beat.
	 * @default 12000
	 */
	staleAfter?: number;

	/**
	 * Cap on the per-instance slice and on the merged result. Bounds
	 * storage cost to `O(instances * topN)` per instance.
	 * @default 20
	 */
	topN?: number;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface ClusterTopicRate {
	topic: string;
	/** Sum across contributing instances. */
	messagesPerSec: number;
	/** Sum across contributing instances. */
	bytesPerSec: number;
	/** How many instances had this topic in their last broadcast slice. */
	contributingInstances: number;
}

export interface PublishRateAggregator {
	/** Stable id for this instance. */
	readonly instanceId: string;

	/** Open the subscriber and start the broadcast timer. Idempotent. */
	activate(platform: Platform): Promise<void>;

	/** Stop the timer, drop the subscriber, and clear cached slices. */
	deactivate(): Promise<void>;

	/**
	 * Cluster-wide top publishers, merged from this instance's local
	 * slice (read fresh from `platform.pressure.topPublishers`) and the
	 * cached remote slices (stale entries dropped). Sorted descending by
	 * `messagesPerSec`, capped at `topN`. Pure memory computation; no
	 * Redis traffic on the hot path.
	 */
	readonly topPublishers: ClusterTopicRate[];

	/**
	 * Cluster-wide messagesPerSec for a topic, or 0 if the topic is not
	 * in the merged top-N. Used by the cluster `topPublisher` admission
	 * rule for per-topic load shedding.
	 */
	rateOf(topic: string): number;
}

/**
 * Create a cluster-wide publish-rate aggregator. Each instance
 * broadcasts its own slice; every instance merges all live slices into
 * a cluster-wide view exposed via `topPublishers` and `rateOf(topic)`.
 *
 * Pairs with the cluster `topPublisher` admission rule (extends
 * `createAdmissionControl`) and `wireClusterPublishRateMetrics`.
 */
export function createPublishRateAggregator(
	client: RedisClient,
	options?: PublishRateAggregatorOptions
): PublishRateAggregator;
