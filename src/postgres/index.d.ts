import type { Pool, PoolClient, PoolConfig, QueryResult, QueryConfig, Client } from 'pg';
import type { MetricsRegistry } from '../prometheus/index.js';

export interface PgClientOptions {
	/** Postgres connection string. Required UNLESS `pool` is provided. */
	connectionString?: string;
	/**
	 * An existing `pg.Pool` to wrap instead of constructing a new one. Use
	 * when your app already maintains a pool (raw `pg` use elsewhere,
	 * another framework integration) and you want a single connection
	 * footprint against the database. When provided, `autoShutdown`
	 * defaults to `false` (the caller owns the pool's lifecycle) and
	 * `end()` becomes a no-op.
	 *
	 * `connectionString` and `pool` are mutually exclusive in spirit, but
	 * you may pass `connectionString` alongside `pool` to enable
	 * `createClient()` (a dedicated `pg.Client` for LISTEN/NOTIFY) without
	 * losing the shared-pool ownership story.
	 */
	pool?: Pool;
	/** Listen for `sveltekit:shutdown` and disconnect. @default `true` when the client owns the pool, `false` when `pool` is provided. */
	autoShutdown?: boolean;
	/** Owned pools default to connectionTimeoutMillis: 10000, query_timeout: 30000,
	 * statement_timeout: 30000. Override each with 0 to disable it. Does not reconfigure an external pool; still configures createClient() when connectionString is supplied. */
	options?: PoolConfig;
	/** Maximum unsettled wrapper query/connect operations; 0 disables. @default 100 for owned pools, 0 for external pools */
	maxInFlight?: number;
	/** Optional bounded-label admission, duration, timeout and driver waiter metrics. */
	metrics?: MetricsRegistry;
}

export interface PgOperationOptions {
	/** Stops the caller's wait, not the database operation. Capacity remains reserved
	 * until the underlying operation settles. An abandoned checkout is released when acquired.
	 * Use AbortSignal.timeout(ms) for a caller deadline. SQL may still execute after abort. */
	signal?: AbortSignal;
}

export interface PgClient {
	/** Bounded read-only connectivity and PostgreSQL 16+ check. */
	check(options?: { timeoutMs?: number }): Promise<{ serverVersionNum: number }>;
	/** The underlying pg Pool. */
	readonly pool: Pool;
	/** Run a query. */
	query(text: string | QueryConfig, values?: any[], options?: PgOperationOptions): Promise<QueryResult>;
	/**
	 * Acquire a dedicated connection from the pool. Prefer this over
	 * `pool.connect()`: connection acquisition is the failure mode whose pg
	 * error text carries the DSN, and a raw handle bypasses redaction
	 * entirely. The returned client's `query` redacts too; `release` is
	 * unchanged.
	 */
	connect(options?: PgOperationOptions): Promise<PoolClient>;
	/**
	 * Create a standalone pg.Client with the same connection config (not
	 * from the pool). Owned factories track it until end(); external-pool wrappers leave it caller-owned. Throws if no connectionString was provided or owned shutdown has begun.
	 */
	createClient(): Client;
	/**
	 * Stop registered notification bridges, close owned standalone sessions, then close the pool. No-op when the client wraps an
	 * externally-provided pool (the caller owns that lifecycle).
	 */
	end(): Promise<void>;
}

/**
 * Create a Postgres client with lifecycle management.
 */
export function createPgClient(options: PgClientOptions): PgClient;

export type PostgresStore = 'replay' | 'idempotency' | 'jobs' | 'tasks' | 'dead-letter' | 'alarm-store';
export interface PostgresSchemaOptions {
	store: PostgresStore;
	/** Defaults to the corresponding store factory's table name. */
	table?: string;
}
export interface PostgresMigration {
	version: number;
	checksum: string;
	statements: string[];
}
export interface PostgresMigrationOptions extends PostgresSchemaOptions {
	/** Database lock wait limit in milliseconds. Positive and finite. @default 5000 */
	lockTimeoutMs?: number;
	/** Database execution limit per statement in milliseconds. Positive and finite. @default 30000 */
	statementTimeoutMs?: number;
}
export interface PostgresStoreStatus {
	state: 'idle' | 'checking' | 'ready' | 'failed';
	error: Error | null;
}
export interface PostgresStoreReadiness {
	/** Migrate automatically, or inspect the schema read-only when autoMigrate is false. Failed calls can be retried. */
	ready(): Promise<void>;
	/** Initialization state; a successful check is a point-in-time result. */
	status(): PostgresStoreStatus;
}
/** Ordered, checksummed SQL for the selected store. Does not access the database. */
export function postgresMigrations(options: PostgresSchemaOptions): PostgresMigration[];
/** Apply resumable migrations with a schema-owner client and a dedicated session. */
export function migrate(client: PgClient, options: PostgresMigrationOptions): Promise<void>;
/** Read-only catalog, required privileges and present migration-ledger validation. */
export function validateSchema(client: PgClient, options: PostgresSchemaOptions): Promise<void>;
/** Check connectivity and await all supplied store readiness gates. Rejects with AggregateError for store failures. */
export function postgresReadiness(client: PgClient, components?: readonly Pick<PostgresStoreReadiness, 'ready'>[]): Promise<void>;
