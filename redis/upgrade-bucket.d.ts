import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface UpgradeBudget {
	/** Upgrades permitted per IP per minute in this posture. Must be a positive integer. */
	perMinute: number;
	/** Auto-ban duration in ms once the budget is spent. 0 = no ban. */
	blockDuration?: number;
}

export interface UpgradeBucketOptions {
	/** Default budget when posture is 'normal'. Must be a positive integer. */
	perMinute: number;
	/** Default auto-ban duration in ms once a budget is spent. 0 = no ban. @default 0 */
	blockDuration?: number;
	/** Budget override for the 'elevated' posture. Inherits 'normal' when omitted. */
	elevated?: UpgradeBudget;
	/** Budget override for the 'siege' posture. Inherits the resolved 'elevated' when omitted. */
	siege?: UpgradeBudget;
	/** Fail-open circuit breaker wrapping the Redis call. */
	breaker?: CircuitBreaker;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
}

export interface LocalUpgradeBucketOptions extends UpgradeBucketOptions {
	/** Hard ceiling on the IP map before LRU eviction. @default 100000 */
	maxEntries?: number;
}

export interface UpgradeBucket {
	/**
	 * Decide whether to admit an upgrade from `ip` under the given posture string.
	 * Returns `true` to admit, `false` to reject. The Redis variant fails OPEN
	 * (admits) if the backend is unavailable.
	 */
	admit(ip: string, posture?: string): Promise<boolean>;
	/** Clear the bucket for one IP. */
	reset(ip: string): Promise<void>;
	/** Reset all per-IP state. */
	clear(): Promise<void>;
}

/**
 * Create a Redis-backed per-IP upgrade-admission bucket. Reuses the application
 * rate limiter's atomic Lua token bucket; fails open through the breaker.
 */
export function createUpgradeBucket(client: RedisClient, options: UpgradeBucketOptions): UpgradeBucket;

/**
 * Create a local (no-Redis) per-IP upgrade-admission bucket with a hard size cap
 * and LRU eviction. For deployments without Redis or as an in-process floor.
 */
export function createLocalUpgradeBucket(options: LocalUpgradeBucketOptions): UpgradeBucket;
