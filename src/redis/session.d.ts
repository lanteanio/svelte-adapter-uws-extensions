import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface DistributedSessionOptions {
	/**
	 * Prefix prepended (after the client `keyPrefix`) to every session key.
	 * @default 'sess:'
	 */
	keyPrefix?: string;

	/**
	 * Time to live in milliseconds. Each `set` refreshes to `ttlMs`. By
	 * default `get` and `touch` also refresh (sliding window). Default
	 * 24 hours.
	 * @default 86400000
	 */
	ttlMs?: number;

	/**
	 * Whether `get(token)` extends the TTL on a hit. Set to `false` for
	 * read-only flows where reads should not act as liveness signals.
	 * @default true
	 */
	refreshOnGet?: boolean;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface DistributedSession<T = unknown> {
	/**
	 * Look up by token. Returns the stored data if present and not yet
	 * expired, else `null`. By default refreshes the TTL on a hit
	 * (sliding window); disable via `refreshOnGet: false`.
	 *
	 * Returns `null` for a missing token, an expired key, or a corrupt
	 * (non-JSON) entry. The corrupt-entry path is treated as a miss so
	 * the next `set` cleanly overwrites.
	 */
	get(token: string): Promise<T | null>;

	/**
	 * Store or replace data for `token`. Resets the TTL via `SET PX`.
	 * The data must be JSON-serializable.
	 */
	set(token: string, data: T): Promise<void>;

	/**
	 * Extend TTL without reading data. Returns `true` if the entry was
	 * present and refreshed, `false` if the token was missing or already
	 * expired.
	 */
	touch(token: string): Promise<boolean>;

	/**
	 * Remove an entry. Returns `true` if the token was present, `false`
	 * if it was missing or already expired.
	 */
	delete(token: string): Promise<boolean>;

	/**
	 * Remove every session under this store's `keyPrefix`. SCAN-based
	 * cleanup - cluster-wide cost scales with total session count.
	 * Not a hot-path operation; use for graceful-shutdown teardowns,
	 * test harnesses, or operator-initiated wipes.
	 */
	clear(): Promise<void>;
}

/**
 * Create a Redis-backed session store with sliding TTL. Mirrors the
 * shape of the adapter's bundled `createSession` plugin
 * (`svelte-adapter-uws/plugins/session`) but cluster-wide.
 *
 * Pairs with `createConnectionRegistry`: when both are wired,
 * `session` provides the durable per-user state and `registry`
 * provides the live "where are they right now" pointer.
 */
export function createDistributedSession<T = unknown>(
	client: RedisClient,
	options?: DistributedSessionOptions
): DistributedSession<T>;
