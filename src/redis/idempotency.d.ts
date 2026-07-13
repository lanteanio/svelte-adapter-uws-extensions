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
	/** Cap on the JSON-encoded byte length of a committed result; `commit()` rejects with `IdempotencyResultTooLargeError` past this. `Infinity` disables. @default 262144 (256 KB) */
	maxResultBytes?: number;
}

export interface IdempotencySlotAcquired {
	acquired: true;
	/**
	 * Store the result and start the long TTL. Call exactly once. Rejects with
	 * `IdempotencyLeaseLostError` if this owner's `acquireTtl` expired and a
	 * successor re-acquired the key before the commit (the common case - a
	 * handler that finishes within `acquireTtl` - never rejects).
	 */
	commit(result: unknown): Promise<void>;
	/**
	 * Release the slot so retries may re-execute. Call on error paths. A no-op
	 * (never throws) if the lease was already lost to a successor.
	 */
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

/**
 * Thrown by `commit(result)` when the JSON-encoded result exceeds the
 * store's `maxResultBytes` cap. Cross-backend (Redis + Postgres); catch on
 * `err.code === 'IDEMPOTENCY_RESULT_TOO_LARGE'` regardless of backend.
 */
export class IdempotencyResultTooLargeError extends Error {
	name: 'IdempotencyResultTooLargeError';
	code: 'IDEMPOTENCY_RESULT_TOO_LARGE';
	/** Actual JSON-encoded byte length of the result. */
	bytes: number;
	/** Configured cap. */
	maxBytes: number;
	constructor(bytes: number, maxBytes: number);
}

/**
 * Thrown by `commit(result)` when the caller lost its lease: the `acquireTtl`
 * expired and a successor re-acquired the key before this owner committed.
 * Cross-backend (Redis + Postgres); catch on
 * `err.code === 'IDEMPOTENCY_LEASE_LOST'` regardless of backend. `abort()`
 * never throws on lease loss - it is a silent no-op.
 */
export class IdempotencyLeaseLostError extends Error {
	name: 'IdempotencyLeaseLostError';
	code: 'IDEMPOTENCY_LEASE_LOST';
	/** The idempotency key whose lease was lost. */
	key: string;
	constructor(key: string);
}

export interface RedisIdempotencyStore {
	/**
	 * Try to claim ownership of a key. Returns one of three slot shapes. The
	 * optional `meta` records the committing user for `live.forget` right-to-
	 * erasure (the realtime idempotent wrapper supplies it).
	 */
	acquire<T = unknown>(key: string, ttlSec?: number, meta?: { user?: string; tenant?: string | null }): Promise<IdempotencySlot<T>>;
	/** Drop a single cached result. */
	purge(key: string): Promise<void>;
	/** Right-to-erasure: delete every cached result this user committed. */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
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
