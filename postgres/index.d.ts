import type { Pool, PoolConfig, QueryResult, Client } from 'pg';

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
	/** Extra pg Pool options. Ignored when `pool` is provided. */
	options?: PoolConfig;
}

export interface PgClient {
	/** The underlying pg Pool. */
	readonly pool: Pool;
	/** Run a query. */
	query(text: string, values?: any[]): Promise<QueryResult>;
	/**
	 * Create a standalone pg.Client with the same connection config (not
	 * from the pool). Throws if neither `connectionString` was provided.
	 */
	createClient(): Client;
	/**
	 * Gracefully close the pool. No-op when the client wraps an
	 * externally-provided pool (the caller owns that lifecycle).
	 */
	end(): Promise<void>;
}

/**
 * Create a Postgres client with lifecycle management.
 */
export function createPgClient(options: PgClientOptions): PgClient;
