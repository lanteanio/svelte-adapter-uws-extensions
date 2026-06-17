/**
 * Postgres transaction helper for `pg.Pool`-wrapping clients.
 *
 * The default `pgClient.query(...)` delegates to `pool.query(...)`,
 * which checks out a fresh connection per call. Multi-statement
 * sequences ("DELETE FROM a; DELETE FROM b;") issued through that path
 * are NOT in the same transaction - they may even land on different
 * Postgres backends - so a crash between them leaves the table state
 * partially updated.
 *
 * `withTransaction(client, fn)` checks out a single connection,
 * issues BEGIN, runs `fn(tx)` where `tx` exposes a `query` method
 * pinned to the same connection, and finally COMMITs (or ROLLBACKs
 * + re-throws on error). The connection is released regardless.
 *
 * @module svelte-adapter-uws-extensions/shared/pg-tx
 */

/**
 * @typedef {Object} PgTxClient
 * @property {(text: string, values?: any[]) => Promise<import('pg').QueryResult>} query
 */

/**
 * Run `fn(tx)` inside a Postgres transaction on a single pooled
 * connection. The transaction is committed when `fn` resolves and
 * rolled back if `fn` throws (the original error is re-thrown).
 *
 * @template T
 * @param {import('../postgres/index.js').PgClient} client
 * @param {(tx: PgTxClient) => Promise<T>} fn
 * @returns {Promise<T>}
 */
export async function withTransaction(client, fn) {
	const pgClient = await client.pool.connect();
	try {
		await pgClient.query('BEGIN');
		let result;
		try {
			result = await fn(pgClient);
		} catch (err) {
			try { await pgClient.query('ROLLBACK'); } catch { /* best-effort */ }
			throw err;
		}
		await pgClient.query('COMMIT');
		return result;
	} finally {
		pgClient.release();
	}
}
