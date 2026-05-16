/**
 * Postgres-backed idempotency store.
 *
 * Caches the result of an effectful operation under a stable key so that
 * retries within `ttl` return the original outcome rather than re-executing.
 * Same contract as the Redis backend, durable on disk.
 *
 * Three states are exposed via the `acquire(idempotencyKey)` return value:
 *   - acquired: the caller owns the slot; runs the work, then `commit(result)` or `abort()`.
 *   - pending:  another caller acquired the slot and has not committed yet.
 *   - result:   a previous run committed; the cached result is returned.
 *
 * A short `acquireTtl` (default 60s) bounds how long a pending row lives so
 * a crashed owner cannot deadlock the key. On `commit` the long `ttl`
 * (default 48h) governs the cache lifetime. The periodic cleanup job
 * deletes rows whose `expires_at` has passed, so stale pending rows
 * naturally clear without manual intervention.
 *
 * Table schema (auto-created if autoMigrate is true):
 *   svti_idempotency (
 *     svti_idempotency_key TEXT        PRIMARY KEY,
 *     status               TEXT        NOT NULL,
 *     result               JSONB,
 *     expires_at           TIMESTAMPTZ NOT NULL
 *   )
 *   + index on (expires_at) for cheap cleanup
 *
 * @module svelte-adapter-uws-extensions/postgres/idempotency
 */

import { safeCreate, assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_IDEMPOTENCY_KEY_LENGTH } from '../shared/caps.js';
import { IdempotencyResultTooLargeError } from '../shared/errors.js';

export { IdempotencyResultTooLargeError };

const DEFAULT_MAX_RESULT_BYTES = 256 * 1024;

/**
 * @typedef {Object} PgIdempotencyOptions
 * @property {string} [table='svti_idempotency'] - Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`.
 * @property {number} [ttl=172800] - Result cache lifetime in seconds. Default 48 hours.
 * @property {number} [acquireTtl=60] - Pending-slot lifetime in seconds. Default 60 seconds.
 * @property {number} [maxResultBytes=262144] - Cap on the JSON-encoded byte length
 *   of a committed result. Past the cap, `commit(result)` rejects with
 *   `IdempotencyResultTooLargeError` (`code: 'IDEMPOTENCY_RESULT_TOO_LARGE'`) and
 *   the row is left in the pending state for the `acquireTtl` to expire. Same
 *   default and semantics as the Redis backend so the two stores are
 *   drop-in interchangeable. Pass `Infinity` to disable.
 * @property {boolean} [autoMigrate=true] - Auto-create table on first use.
 * @property {number} [cleanupInterval=60000] - How often expired rows are deleted (ms). 0 disables.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker.
 * @property {any} [metrics] - Optional metrics registry (Prometheus).
 */

/**
 * @typedef {Object} PgIdempotencyStore
 * @property {(key: string) => Promise<import('../redis/idempotency.js').IdempotencySlot>} acquire
 * @property {(key: string) => Promise<void>} purge
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop the cleanup timer.
 */

/**
 * Create a Postgres-backed idempotency store.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgIdempotencyOptions} [options]
 * @returns {PgIdempotencyStore}
 */
export function createIdempotencyStore(client, options = {}) {
	if (options.ttl !== undefined) {
		if (typeof options.ttl !== 'number' || options.ttl < 1 || !Number.isInteger(options.ttl)) {
			throw new Error(`postgres idempotency: ttl must be a positive integer, got ${options.ttl}`);
		}
	}
	if (options.acquireTtl !== undefined) {
		if (typeof options.acquireTtl !== 'number' || options.acquireTtl < 1 || !Number.isInteger(options.acquireTtl)) {
			throw new Error(`postgres idempotency: acquireTtl must be a positive integer, got ${options.acquireTtl}`);
		}
	}
	const maxResultBytes = options.maxResultBytes ?? DEFAULT_MAX_RESULT_BYTES;
	if (maxResultBytes !== Infinity && (!Number.isInteger(maxResultBytes) || maxResultBytes < 1)) {
		throw new Error(`postgres idempotency: maxResultBytes must be a positive integer or Infinity, got ${maxResultBytes}`);
	}

	const table = options.table || 'svti_idempotency';
	const ttl = options.ttl || 48 * 3600;
	const acquireTtl = options.acquireTtl || 60;
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	assertSafeTableName(table, 'postgres idempotency');

	const b = options.breaker;
	const m = options.metrics;
	const mAcquired = m?.counter('idempotency_acquired_total', 'Slots acquired (caller runs work)');
	const mHits = m?.counter('idempotency_hits_total', 'Cached results returned');
	const mPending = m?.counter('idempotency_pending_total', 'Slots reported as pending');
	const mCommits = m?.counter('idempotency_commits_total', 'Results committed');
	const mAborts = m?.counter('idempotency_aborts_total', 'Slots aborted');

	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				svti_idempotency_key TEXT        PRIMARY KEY,
				status               TEXT        NOT NULL,
				result               JSONB,
				expires_at           TIMESTAMPTZ NOT NULL
			)
		`, { table, columns: ['svti_idempotency_key', 'status', 'result', 'expires_at'] });
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_expires_at ON ${table} (expires_at)
		`);
		migrated = true;
	}

	// One-shot ready() promise: kicks off ensureTable() at construction so
	// callers that need the table to exist before they start polling can
	// `await idempotency.ready()`. Subsequent ensureTable() calls are
	// no-ops via the migrated flag.
	const readyPromise = autoMigrate
		? ensureTable().catch((err) => { throw err; })
		: Promise.resolve();

	function validateKey(idempotencyKey) {
		if (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0) {
			throw new Error('postgres idempotency: idempotencyKey must be a non-empty string');
		}
		if (idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
			throw new Error('postgres idempotency: idempotencyKey must be at most ' + MAX_IDEMPOTENCY_KEY_LENGTH + ' characters');
		}
	}

	let cleanupTimer = null;
	let cleanupRunning = false;
	if (cleanupInterval > 0) {
		cleanupTimer = setInterval(async () => {
			if (cleanupRunning) return;
			if (b && !b.isHealthy) return;
			cleanupRunning = true;
			try {
				await ensureTable();
				await client.query(`DELETE FROM ${table} WHERE expires_at < now()`);
				b?.success();
			} catch (err) {
				b?.failure(err);
			} finally {
				cleanupRunning = false;
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	function commitFor(idempotencyKey) {
		return async function commit(result) {
			const payload = JSON.stringify(result === undefined ? null : result);
			const bytes = Buffer.byteLength(payload);
			if (bytes > maxResultBytes) {
				// Throw BEFORE writing so the row stays in the pending state.
				// The caller can call abort() to release it; otherwise the
				// acquireTtl expiration sweeps it.
				throw new IdempotencyResultTooLargeError(bytes, maxResultBytes);
			}
			await withBreaker(b, () => client.query({
				name: 'idem_commit_' + table,
				text: `UPDATE ${table}
				          SET status = 'committed',
				              result = $2::jsonb,
				              expires_at = now() + ($3 || ' seconds')::interval
				        WHERE svti_idempotency_key = $1`,
				values: [idempotencyKey, payload, ttl]
			}));
			mCommits?.inc();
		};
	}

	function abortFor(idempotencyKey) {
		return async function abort() {
			await withBreaker(b, () => client.query({
				name: 'idem_abort_' + table,
				text: `DELETE FROM ${table} WHERE svti_idempotency_key = $1`,
				values: [idempotencyKey]
			}));
			mAborts?.inc();
		};
	}

	async function attemptAcquire(idempotencyKey) {
		// Insert a fresh pending row, OR take over an existing row whose
		// expires_at has passed (crashed owner / expired cached result).
		// `xmax = 0` distinguishes a fresh insert from a takeover but we
		// only need to know "did we end up owning this row" - both paths
		// return at least one row.
		const ins = await client.query({
			name: 'idem_acquire_' + table,
			text: `INSERT INTO ${table} (svti_idempotency_key, status, result, expires_at)
			       VALUES ($1, 'pending', NULL, now() + ($2 || ' seconds')::interval)
			       ON CONFLICT (svti_idempotency_key) DO UPDATE
			         SET status = 'pending',
			             result = NULL,
			             expires_at = now() + ($2 || ' seconds')::interval
			         WHERE ${table}.expires_at < now()
			       RETURNING status`,
			values: [idempotencyKey, acquireTtl]
		});

		if (ins.rowCount > 0) {
			return { acquired: true };
		}

		const sel = await client.query({
			name: 'idem_read_' + table,
			text: `SELECT status, result FROM ${table} WHERE svti_idempotency_key = $1 AND expires_at >= now()`,
			values: [idempotencyKey]
		});

		if (sel.rowCount === 0) {
			return null;
		}

		const row = sel.rows[0];
		if (row.status === 'pending') {
			return { acquired: false, pending: true };
		}
		return { acquired: false, result: row.result };
	}

	return {
		async acquire(idempotencyKey) {
			validateKey(idempotencyKey);

			b?.guard();
			let outcome;
			try {
				await ensureTable();
				outcome = await attemptAcquire(idempotencyKey);
				if (outcome === null) {
					// Race: row was deleted between the conflict and the read.
					// One retry catches it; if it still happens, treat as pending.
					outcome = await attemptAcquire(idempotencyKey);
					if (outcome === null) {
						outcome = { acquired: false, pending: true };
					}
				}
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			if (outcome.acquired) {
				mAcquired?.inc();
				return {
					acquired: true,
					commit: commitFor(idempotencyKey),
					abort: abortFor(idempotencyKey)
				};
			}
			if (outcome.pending) {
				mPending?.inc();
				return { acquired: false, pending: true };
			}
			mHits?.inc();
			return { acquired: false, result: outcome.result };
		},

		async purge(idempotencyKey) {
			validateKey(idempotencyKey);
			await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'idem_purge_' + table,
					text: `DELETE FROM ${table} WHERE svti_idempotency_key = $1`,
					values: [idempotencyKey]
				});
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				return client.query(`DELETE FROM ${table}`);
			});
		},

		ready() {
			return readyPromise;
		},

		destroy() {
			if (cleanupTimer) {
				clearInterval(cleanupTimer);
				cleanupTimer = null;
			}
		}
	};
}
