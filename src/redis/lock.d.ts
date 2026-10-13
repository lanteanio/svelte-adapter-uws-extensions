import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface DistributedLockOptions {
	/**
	 * Pass a persistent monotonic fence as the callback's second argument.
	 * The protected sink must atomically reject fences not greater than its
	 * last accepted fence. Counter keys have no TTL and must not be deleted or
	 * rolled back while the sink retains their values. The client-relative
	 * `lock-fences:` namespace is reserved for these counters. All holders
	 * protecting the same resource must use the same Redis client keyPrefix.
	 * @default false
	 */
	fence?: boolean;

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
	 * Prometheus registry. Defaults to the identifier-free class `all`.
	 */
	mapKey?: (key: string) => string;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface WithLockOptions {
	/** Override whether this call receives a monotonic fence. */
	fence?: boolean;

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
	constructor(key: string, waitedMs: number);
	readonly code: 'LOCK_ACQUIRE_TIMEOUT';
	readonly key: string;
	readonly waitedMs: number;
}

/**
 * Surfaced via `controller.abort(new LockLostError(key))` on the
 * `signal` passed to the user fn when ownership changes or its confirmed
 * lease expires before a renewal succeeds.
 * Also rejects `withLock` after an otherwise successful callback when the
 * heartbeat or final compare-and-delete confirms ownership loss, or a renewal
 * deadline expires without renewed ownership.
 */
export class LockLostError extends Error {
	constructor(key: string);
	readonly code: 'LOCK_LOST';
	readonly key: string;
}

export interface DistributedLock {
	/**
	 * Run `fn` while holding a cluster-wide COOPERATIVE lease on `key`.
	 * Acquires via `SET <key> <leaseToken> NX PX <ttlMs>` with a retry loop;
	 * releases via Lua compare-and-delete on completion.
	 *
	 * This is a TTL lease with cooperative release, not a hard fencing mutex:
	 * a holder that stalls past `ttlMs` loses the lease while still running and
	 * a successor can overlap it. It gives mutual exclusion under normal
	 * operation (responsive holders, no stall longer than `ttlMs`); for strict
	 * single-writer correctness across a stall, make the protected resource
	 * reject stale writes or use the task runner's Postgres fence.
	 *
	 * The heartbeat refreshes the TTL while `fn` is running; if the
	 * heartbeat detects loss or the last confirmed lease expires, the supplied
	 * `AbortSignal` fires with a `LockLostError`. User code should
	 * react via `signal.aborted` or `signal.addEventListener('abort')`.
	 *
	 * `fn`'s return value is forwarded only while ownership remains valid.
	 * A confirmed loss or expired renewal deadline rejects with `LockLostError` even if `fn` ignored its
	 * signal. Errors thrown by `fn` retain precedence after cleanup.
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
		fn: (signal: AbortSignal, fence?: number) => Promise<T> | T,
		options?: WithLockOptions
	): Promise<T>;
}

/**
 * The lock returned for `fence: true`. Its callbacks receive a `number` fence
 * unless a call passes `fence: false`; it is still a `DistributedLock`.
 */
export interface FencedDistributedLock extends DistributedLock {
	withLock<T>(
		key: string,
		fn: (signal: AbortSignal) => Promise<T> | T,
		options: WithLockOptions & { fence: false }
	): Promise<T>;
	withLock<T>(
		key: string,
		fn: (signal: AbortSignal, fence: number) => Promise<T> | T,
		options?: WithLockOptions & { fence?: true }
	): Promise<T>;
	withLock<T>(
		key: string,
		fn: (signal: AbortSignal, fence?: number) => Promise<T> | T,
		options?: WithLockOptions
	): Promise<T>;
}

/**
 * Create a cluster-wide distributed lock factory.
 */
export function createDistributedLock(
	client: RedisClient,
	options: DistributedLockOptions & { fence: true }
): FencedDistributedLock;
export function createDistributedLock(
	client: RedisClient,
	options?: DistributedLockOptions
): DistributedLock;
