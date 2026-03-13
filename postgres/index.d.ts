import type { Pool, PoolConfig, QueryResult, Client } from 'pg';

export interface PgClientOptions {
	/** Postgres connection string. Required. */
	connectionString: string;
	/** Listen for `sveltekit:shutdown` and disconnect. @default true */
	autoShutdown?: boolean;
	/** Extra pg Pool options. */
	options?: PoolConfig;
}

export interface PgClient {
	/** The underlying pg Pool. */
	readonly pool: Pool;
	/** Run a query. */
	query(text: string, values?: any[]): Promise<QueryResult>;
	/** Create a standalone pg.Client with the same connection config (not from the pool). */
	createClient(): Client;
	/** Gracefully close the pool. */
	end(): Promise<void>;
}

/**
 * Create a Postgres client with lifecycle management.
 */
export function createPgClient(options: PgClientOptions): PgClient;
