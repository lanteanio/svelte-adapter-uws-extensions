import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface LeaderOptions {
	/**
	 * Redis key for the lease (prefixed by the client's `keyPrefix`).
	 * @default 'leader'
	 */
	key?: string;
	/**
	 * This worker's identity. Override only if you want a stable
	 * identity for diagnostics; correctness does not depend on it.
	 * @default randomBytes(8).toString('hex')
	 */
	instanceId?: string;
	/**
	 * TTL on the lease in milliseconds. Worst-case window between
	 * leader death and successor takeover.
	 * @default 30000
	 */
	leaseMs?: number;
	/**
	 * Renewal interval in milliseconds. Must be `< leaseMs`. Also the
	 * interval at which non-leaders attempt fresh acquire.
	 * @default leaseMs / 3
	 */
	renewMs?: number;
	/**
	 * Called on every Redis failure (renewal, fresh acquire, release).
	 * Use for structured logging. Errors never escape the renewal
	 * interval regardless - `onError` is observability, not control flow.
	 */
	onError?: (err: Error) => void;
	/**
	 * Map the lease key to a bounded label value for cardinality control
	 * on the four `leader_*` counters. Default: identity.
	 */
	mapKey?: (key: string) => string;
	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface Leader {
	/**
	 * Synchronous cached check. Microsecond-cost. Call at the top of
	 * every cron tick / scheduled job.
	 */
	isLeader(): boolean;
	/**
	 * Single-GET diagnostic read of the current owner's instanceId.
	 * Returns `null` if unowned or on Redis failure (also calls `onError`
	 * in the failure case).
	 */
	currentLeader(): Promise<string | null>;
	/**
	 * Stop the renewal interval and best-effort release the lease via
	 * compare-and-delete so a sibling can take over within `renewMs`
	 * instead of `leaseMs`. Idempotent. Never throws - a release
	 * failure just means the lease will expire on its own.
	 */
	stop(): Promise<void>;
	/** This worker's identity (provided or generated). */
	readonly instanceId: string;
	/** The fully-prefixed lease key (useful for diagnostics). */
	readonly key: string;
}

/**
 * Create a cluster-wide leader-election primitive.
 */
export function createLeader(client: RedisClient, options?: LeaderOptions): Leader;
