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
 * + re-throws on error). The connection is released regardless; one
 * whose transaction state could not be restored (failed BEGIN, COMMIT,
 * or ROLLBACK) is released WITH the error so the pool destroys it
 * instead of handing it to a later checkout.
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
	// client.connect(), not client.pool.connect(): the wrapper redacts the
	// connection DSN out of pg's error text, and connection acquisition is
	// the failure mode that carries it. Falls back to the raw pool for a
	// caller-supplied client shape that predates the wrapper.
	const pgClient = typeof client.connect === 'function'
		? await client.connect()
		: await client.pool.connect();
	// When set, the connection's transaction state is uncertain (failed
	// BEGIN/COMMIT, or a ROLLBACK that itself failed). pg-pool destroys a
	// client released with a truthy error instead of returning it to the
	// pool - an uncertain connection must never serve a later checkout.
	let unsafe;
	try {
		try {
			await pgClient.query('BEGIN');
		} catch (err) {
			unsafe = err;
			throw err;
		}
		let result;
		try {
			result = await fn(pgClient);
		} catch (err) {
			// A successful ROLLBACK cleanly aborts the transaction; the
			// connection is reusable and only the work error propagates.
			try { await pgClient.query('ROLLBACK'); } catch (rbErr) { unsafe = rbErr; }
			throw err;
		}
		try {
			await pgClient.query('COMMIT');
		} catch (err) {
			unsafe = err;
			throw err;
		}
		return result;
	} finally {
		pgClient.release(unsafe);
	}
}
