import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { IdempotencySlot } from '../redis/idempotency.js';

export interface PgIdempotencyOptions {
	/** Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. @default 'ws_idempotency' */
	table?: string;
	/** Result cache lifetime in seconds. @default 172800 (48 hours) */
	ttl?: number;
	/** Pending-slot lifetime in seconds (anti-deadlock for crashed owners). @default 60 */
	acquireTtl?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
	/** How often to delete expired rows (ms). 0 disables. @default 60000 */
	cleanupInterval?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface PgIdempotencyStore {
	/** Try to claim ownership of a key. Returns one of three slot shapes. */
	acquire<T = unknown>(key: string): Promise<IdempotencySlot<T>>;
	/** Drop a single cached result. */
	purge(key: string): Promise<void>;
	/** Drop every row in the store's table. */
	clear(): Promise<void>;
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
