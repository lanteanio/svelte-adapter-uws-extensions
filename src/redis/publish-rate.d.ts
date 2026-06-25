import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface ClusterSubject {
	topic: string;
	/** Local subscriber count for this topic on the contributing instance. */
	count: number;
}

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

	/**
	 * Cluster-wide per-topic messages/sec threshold for `onPublishRate`
	 * crossing callbacks. Mirrors the adapter's
	 * `pressure.topicPublishRatePerSec`, except the rate compared here is the
	 * cluster-wide SUM across instances. Set `false` to disable crossing
	 * callbacks entirely. Does not affect `topPublishers` / `rateOf`.
	 * @default 5000
	 */
	topicPublishRatePerSec?: number | false;

	/**
	 * Optional contributor for cluster-wide subscriber counts. Called
	 * fresh on every broadcast tick to gather this instance's per-topic
	 * subscriber list. Sorted descending by `count` and capped at `topN`
	 * before broadcast. When wired, the broadcast envelope grows a `subs`
	 * field; `subscribersOf(topic)` returns the merged sum.
	 *
	 * The sharded bus exposes `bus.localSubjects(platform)` as the
	 * natural source: `subjects: () => bus.localSubjects(platform)`.
	 */
	subjects?: () => ClusterSubject[];

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

	/**
	 * Cluster-wide subscriber count for a topic, summed across this
	 * instance's live local count (from `subjects()`) and all non-stale
	 * remote contributions. Returns 0 when no `subjects` callback is
	 * wired and no remote instance has reported the topic.
	 *
	 * Eventually-consistent within `publishInterval`; bounded by
	 * `staleAfter`. The sharded bus's `bus.subscribers(topic)` delegates
	 * here when an aggregator is wired.
	 */
	subscribersOf(topic: string): number;

	/**
	 * Register a callback that fires when one or more topics cross the
	 * configured cluster-wide `topicPublishRatePerSec` threshold. Mirrors the
	 * adapter's per-instance `platform.onPublishRate(cb)` at the cluster layer.
	 * Evaluated once per broadcast tick over the merged view, edge-triggered: a
	 * topic fires the tick it rises to or above the threshold and re-arms only
	 * after it drops below and rises again, so a persistently-hot topic fires
	 * once rather than every tick. The callback receives the `ClusterTopicRate`
	 * entries that crossed on this tick; a throwing callback is contained
	 * (logged, never breaks the timer or sibling subscribers). Returns an
	 * unsubscribe function. No-op (never fires) when `topicPublishRatePerSec`
	 * is `false`.
	 *
	 * Pairs with the cluster `topPublisher` admission rule: the rule sheds at
	 * the cluster layer continuously, while this is the event-push hook for
	 * acting on a crossing (alerting, scaling, targeted backpressure).
	 */
	onPublishRate(callback: (events: ClusterTopicRate[]) => void): () => void;
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
