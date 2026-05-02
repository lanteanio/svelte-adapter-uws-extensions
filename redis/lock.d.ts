import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface DistributedLockOptions {
	/**
	 * Prefix prepended (after the client `keyPrefix`) to every lock key.
	 * @default 'lock:'
	 */
	keyPrefix?: string;

	/**
	 * Default TTL on a held lock in milliseconds. The heartbeat refreshes
	 * this back to the original value before it elapses; if the holder
	 * dies and stops heartbeating, the lock auto-expires after at most
	 * `ttlMs` so cluster work isn't permanently blocked.
	 * @default 30000
	 */
	defaultTtlMs?: number;

	/**
	 * Sleep between acquire retries when the key is held by another
	 * instance. Constant retry; no exponential backoff in this default
	 * implementation.
	 * @default 50
	 */
	retryDelayMs?: number;

	/**
	 * Total time to wait for the key to free up before throwing a
	 * `LockAcquireTimeoutError`. Override per call via
	 * `withLock(key, fn, { maxWaitMs })`.
	 * @default 5000
	 */
	maxWaitMs?: number;

	/**
	 * Heartbeat refresh interval in milliseconds. Defaults to
	 * `defaultTtlMs / 3` so a single missed beat still leaves margin
	 * before the TTL elapses.
	 */
	heartbeatMs?: number;

	/**
	 * Map lock key names to bounded label values for cardinality control
	 * on the lock counters. Mirrors the `mapTopic` shape from the
	 * Prometheus registry.
	 */
	mapKey?: (key: string) => string;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface WithLockOptions {
	/** Override the holder TTL for this call. */
	ttlMs?: number;

	/** Override the maximum wait for this call. */
	maxWaitMs?: number;

	/**
	 * External cancellation signal. Aborts the acquire loop and the
	 * inner-`fn` execution; the lock is still released cleanly on the
	 * way out (we held it; we give it back).
	 */
	signal?: AbortSignal;
}

/**
 * Thrown by `withLock` when `maxWaitMs` elapses without a successful
 * acquire.
 */
export class LockAcquireTimeoutError extends Error {
	readonly key: string;
	readonly waitedMs: number;
}

/**
 * Surfaced via `controller.abort(new LockLostError(key))` on the
 * `signal` passed to the user fn when the heartbeat detects we no
 * longer own the key (operator force-takeover, TTL elapsed, etc.).
 */
export class LockLostError extends Error {
	readonly key: string;
}

export interface DistributedLock {
	/**
	 * Run `fn` while holding a cluster-wide mutex on `key`. Acquires
	 * via `SET <key> <fenceToken> NX PX <ttlMs>` with a retry loop;
	 * releases via Lua compare-and-delete on completion.
	 *
	 * The heartbeat refreshes the TTL while `fn` is running; if the
	 * heartbeat detects we no longer own the key, the supplied
	 * `AbortSignal` fires with a `LockLostError`. User code should
	 * react via `signal.aborted` or `signal.addEventListener('abort')`.
	 *
	 * `fn`'s return value is forwarded through. Errors thrown by `fn`
	 * propagate after the lock is released.
	 *
	 * @example
	 * ```js
	 * await lock.withLock('order-42', async (signal) => {
	 *   if (signal.aborted) return;
	 *   await processOrder(42);
	 * });
	 * ```
	 */
	withLock<T>(
		key: string,
		fn: (signal: AbortSignal) => Promise<T> | T,
		options?: WithLockOptions
	): Promise<T>;
}

/**
 * Create a cluster-wide distributed lock factory.
 */
export function createDistributedLock(
	client: RedisClient,
	options?: DistributedLockOptions
): DistributedLock;
