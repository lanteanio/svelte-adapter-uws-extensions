import type { PostgresStoreReadiness } from './index.js';
import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { IdempotencySlot } from '../redis/idempotency.js';

export interface PgIdempotencyOptions {
	/** Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. @default 'svti_idempotency' */
	table?: string;
	/** Result cache lifetime in seconds. @default 172800 (48 hours) */
	ttl?: number;
	/** Pending-slot lifetime in seconds (anti-deadlock for crashed owners). @default 60 */
	acquireTtl?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
	/** Maximum jitter delay between bounded maintenance passes, in ms. 0 disables; integer up to 2147483647. @default 60000 */
	cleanupInterval?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
	/** Cap on the JSON-encoded byte length of a committed result; `commit()` rejects with `IdempotencyResultTooLargeError` past this. `Infinity` disables. @default 262144 (256 KB) */
	maxResultBytes?: number;
}

export interface PgIdempotencyStore extends PostgresStoreReadiness {
	/**
	 * Try to claim ownership of an idempotency key. Returns one of three slot
	 * shapes. The optional `meta` records the committing user (denormalized into
	 * user_id/tenant_id columns) for `live.forget` right-to-erasure.
	 */
	/** ttlSec overrides the store default; zero releases the lease without retaining a result. Must be a non-negative integer. */
	acquire<T = unknown>(idempotencyKey: string, ttlSec?: number, meta?: { user?: string; tenant?: string | null }): Promise<IdempotencySlot<T>>;
	/** Drop a single cached result. */
	purge(idempotencyKey: string): Promise<void>;
	/** Right-to-erasure: delete every row this user owns (DELETE WHERE tenant_id, user_id). */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
	/** Drop every row in the store's table. */
	clear(): Promise<void>;
	/** Await automatic migration, or read-only schema validation with autoMigrate: false. */
	ready(): Promise<void>;
	/** Stop the cleanup timer (call on shutdown if not relying on autoShutdown). */
	destroy(): void;
}

/**
 * Create a Postgres-backed idempotency store. Same contract as the Redis
 * backend, durable on disk.
 */
export function createIdempotencyStore(
	client: PgClient,
	options?: PgIdempotencyOptions
): PgIdempotencyStore;

/**
 * Re-exported cross-backend errors:
 * - `IdempotencyResultTooLargeError` - `commit(result)` rejects when the
 *   JSON-encoded result exceeds `maxResultBytes`
 *   (`err.code === 'IDEMPOTENCY_RESULT_TOO_LARGE'`).
 * - `IdempotencyLeaseLostError` - `commit(result)` rejects when the owner's
 *   `acquireTtl` expired and a successor re-acquired the key first
 *   (`err.code === 'IDEMPOTENCY_LEASE_LOST'`).
 * Both are identical across the Redis and Postgres backends.
 */
export { IdempotencyResultTooLargeError, IdempotencyLeaseLostError } from '../redis/idempotency.js';
