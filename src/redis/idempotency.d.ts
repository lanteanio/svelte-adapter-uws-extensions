import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisIdempotencyOptions {
	/** Prefix prepended (after the client keyPrefix) to every key. @default 'idem:' */
	keyPrefix?: string;
	/** Result cache lifetime in seconds. @default 172800 (48 hours) */
	ttl?: number;
	/** Pending-slot lifetime in seconds (anti-deadlock for crashed owners). @default 60 */
	acquireTtl?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface IdempotencySlotAcquired {
	acquired: true;
	/** Store the result and start the long TTL. Call exactly once. */
	commit(result: unknown): Promise<void>;
	/** Release the slot so retries may re-execute. Call on error paths. */
	abort(): Promise<void>;
}

export interface IdempotencySlotPending {
	acquired: false;
	pending: true;
}

export interface IdempotencySlotResult<T = unknown> {
	acquired: false;
	result: T;
}

export type IdempotencySlot<T = unknown> =
	| IdempotencySlotAcquired
	| IdempotencySlotPending
	| IdempotencySlotResult<T>;

export interface RedisIdempotencyStore {
	/** Try to claim ownership of a key. Returns one of three slot shapes. */
	acquire<T = unknown>(key: string): Promise<IdempotencySlot<T>>;
	/** Drop a single cached result. */
	purge(key: string): Promise<void>;
	/** Drop every key under this store's prefix. */
	clear(): Promise<void>;
	/**
	 * Symmetry with the Postgres idempotency store. The Redis backend
	 * has no DDL to run, so this resolves immediately. Provided so
	 * callers can write generic boot code: `await store.ready()`
	 * regardless of which backend is wired.
	 */
	ready(): Promise<void>;
}

/**
 * Create a Redis-backed idempotency store. Caches the outcome of an
 * effectful operation so retries within `ttl` return the original result
 * rather than re-executing.
 */
export function createIdempotencyStore(
	client: RedisClient,
	options?: RedisIdempotencyOptions
): RedisIdempotencyStore;
