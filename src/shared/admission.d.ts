import type { MetricsRegistry } from '../prometheus/index.js';

export type PressureReason = 'NONE' | 'PUBLISH_RATE' | 'SUBSCRIBERS' | 'MEMORY';

export interface PressureSnapshot {
	readonly active: boolean;
	readonly subscriberRatio: number;
	readonly publishRate: number;
	readonly memoryMB: number;
	readonly reason: PressureReason;
}

/**
 * Cluster-aware rule shape: this class is rejected when the
 * `topic` argument's cluster-wide messagesPerSec (summed across
 * contributing instances) is at or above `threshold`. Requires the
 * controller to be constructed with an `aggregator` (typically from
 * `redis/publish-rate`) and `shouldAccept` to be called with a
 * `{topic}` third argument.
 */
export interface ClusterTopPublisherRule {
	clusterTopPublisher: { threshold: number };
}

/**
 * A per-class rule. One of:
 * - an array of reasons - rejected when `pressure.reason` is in the list
 * - a predicate - rejected when the predicate returns true for the current snapshot
 * - a `clusterTopPublisher` object - rejected when the topic's cluster-wide rate is at threshold
 */
export type AdmissionRule =
	| readonly PressureReason[]
	| ((snapshot: PressureSnapshot) => boolean)
	| ClusterTopPublisherRule;

export interface PublishRateAggregatorLike {
	rateOf(topic: string): number;
}

export interface AdmissionControlOptions {
	/**
	 * Map of class name to admission rule. Each class is independently
	 * configured. `shouldAccept(className, ...)` with an unknown name
	 * throws - typos surface immediately rather than silently accepting.
	 */
	classes: Record<string, AdmissionRule>;
	/**
	 * Cluster publish-rate aggregator. Required when any class uses the
	 * `clusterTopPublisher` rule shape; ignored otherwise.
	 */
	aggregator?: PublishRateAggregatorLike;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
}

export interface AdmissionControl {
	/**
	 * Returns `true` when this class is admitted under the platform's
	 * current pressure snapshot, `false` when the class should be shed.
	 *
	 * For local rules (array, predicate), `opts` is unused. For the
	 * `clusterTopPublisher` rule shape, pass the topic the request is
	 * about to publish to: `shouldAccept('nonCritical', platform, { topic })`.
	 *
	 * Reads `platform.pressure` - a property access, no I/O. Safe to
	 * call on every request hot path. The reason-precedence math
	 * (memory > publish rate > subscribers > none) lives in the
	 * adapter; this method only checks the resolved `reason` against
	 * the configured rule.
	 *
	 * Throws if `className` was not configured at construction time, or
	 * if the class uses `clusterTopPublisher` and `opts.topic` is missing.
	 */
	shouldAccept(
		className: string,
		platform: { readonly pressure: PressureSnapshot },
		opts?: { topic?: string }
	): boolean;
}

/**
 * Create a pressure-aware admission controller. Complements (does not
 * replace) the dependency circuit breaker - the breaker answers "is
 * the backend up?", admission control answers "are we OK to take more
 * work right now?"
 */
export function createAdmissionControl(options: AdmissionControlOptions): AdmissionControl;
