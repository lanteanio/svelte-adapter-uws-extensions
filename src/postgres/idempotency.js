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

import { assertSafeTableName } from '../shared/pg-migrate.js';
import { createStoreSchema } from './migrations.js';
import { withBreaker, pgFailure, guardBreaker } from './_breaker.js';
import { randomUuid } from '../shared/runtime.js';
import { scheduleCleanup } from './_cleanup.js';
import { MAX_IDEMPOTENCY_KEY_LENGTH } from '../shared/caps.js';
import { IdempotencyResultTooLargeError, IdempotencyLeaseLostError } from '../shared/errors.js';

export { IdempotencyResultTooLargeError, IdempotencyLeaseLostError };

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
 * @property {number} [cleanupInterval=60000] - Maximum jitter delay between bounded maintenance passes (ms). 0 disables.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker.
 * @property {any} [metrics] - Optional metrics registry (Prometheus).
 */

/**
 * @typedef {Object} PgIdempotencyStore
 * @property {(key: string, ttlSec?: number, meta?: { user?: string, tenant?: string | null }) => Promise<import('../redis/idempotency.js').IdempotencySlot>} acquire
 * @property {(key: string) => Promise<void>} purge
 * @property {(tenantId: string | null, userId: string) => Promise<number>} purgeUser - Right-to-erasure: delete this user's rows.
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

	const schema = createStoreSchema(client, 'idempotency', table, autoMigrate);
	const ensureTable = schema.ensure;

	if (autoMigrate) schema.ready().catch(() => {});


	function validateKey(idempotencyKey) {
		if (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0) {
			throw new Error('postgres idempotency: idempotencyKey must be a non-empty string');
		}
		if (idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
			throw new Error('postgres idempotency: idempotencyKey must be at most ' + MAX_IDEMPOTENCY_KEY_LENGTH + ' characters');
		}
	}

	const cleanup = scheduleCleanup({
		client, table, kind: 'idempotency', interval: cleanupInterval, metrics: m,
		ready: ensureTable, healthy: () => !b || b.isHealthy,
		success: () => b?.success(), failure: (error) => pgFailure(b, error),
		async work(budget) {
			while (budget.available) {
				const limit = budget.limit;
				const count = await budget.remove(`WITH expired AS (
					SELECT svti_idempotency_key FROM ${table} WHERE expires_at < now()
					ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED
				) DELETE FROM ${table} r USING expired
				WHERE r.svti_idempotency_key = expired.svti_idempotency_key`, [limit]);
				if (count < limit) break;
			}
		}
	});

	function commitFor(idempotencyKey, ownerToken, resultTtl) {
		return async function commit(result) {
			if (resultTtl === 0) {
				const res = await withBreaker(b, () => client.query({
					name: 'idem_discard_' + table,
					text: `DELETE FROM ${table} WHERE svti_idempotency_key = $1 AND owner_token = $2`,
					values: [idempotencyKey, ownerToken]
				}));
				if (!res || res.rowCount === 0) throw new IdempotencyLeaseLostError(idempotencyKey);
				mCommits?.inc();
				return;
			}
			const payload = JSON.stringify(result === undefined ? null : result);
			const bytes = Buffer.byteLength(payload);
			if (bytes > maxResultBytes) {
				// Throw BEFORE writing so the row stays in the pending state.
				// The caller can call abort() to release it; otherwise the
				// acquireTtl expiration sweeps it.
				throw new IdempotencyResultTooLargeError(bytes, maxResultBytes);
			}
			// Token-guarded UPDATE: only commit if this owner still holds the row.
			// A stale owner whose acquireTtl expired and whose row a successor took
			// over matches zero rows here - refuse rather than clobber the successor.
			const res = await withBreaker(b, () => client.query({
				name: 'idem_commit_' + table,
				text: `UPDATE ${table}
				          SET status = 'committed',
				              result = $2::jsonb,
				              expires_at = now() + ($3 || ' seconds')::interval
				        WHERE svti_idempotency_key = $1 AND owner_token = $4`,
				values: [idempotencyKey, payload, resultTtl, ownerToken]
			}));
			if (!res || res.rowCount === 0) {
				throw new IdempotencyLeaseLostError(idempotencyKey);
			}
			mCommits?.inc();
		};
	}

	function abortFor(idempotencyKey, ownerToken) {
		return async function abort() {
			// Token-guarded DELETE: release the row only if this owner still holds
			// it. A token mismatch (a successor re-acquired) matches zero rows and
			// is a silent no-op - never delete the successor's row.
			await withBreaker(b, () => client.query({
				name: 'idem_abort_' + table,
				text: `DELETE FROM ${table} WHERE svti_idempotency_key = $1 AND owner_token = $2`,
				values: [idempotencyKey, ownerToken]
			}));
			mAborts?.inc();
		};
	}

	async function attemptAcquire(idempotencyKey, userId, tenantId) {
		// Insert a fresh pending row, OR take over an existing row whose
		// expires_at has passed (crashed owner / expired cached result).
		// `xmax = 0` distinguishes a fresh insert from a takeover but we
		// only need to know "did we end up owning this row" - both paths
		// return at least one row. user_id/tenant_id ride along (right-to-erasure);
		// a takeover re-stamps them so the columns track the current owner.
		// A distinct owner_token is minted here and re-stamped on takeover, so a
		// prior owner's token no longer matches the row - its commit/abort no-op.
		const ownerToken = randomUuid();
		const ins = await client.query({
			name: 'idem_acquire_' + table,
			text: `INSERT INTO ${table} (svti_idempotency_key, status, result, expires_at, user_id, tenant_id, owner_token)
			       VALUES ($1, 'pending', NULL, now() + ($2 || ' seconds')::interval, $3, $4, $5)
			       ON CONFLICT (svti_idempotency_key) DO UPDATE
			         SET status = 'pending',
			             result = NULL,
			             expires_at = now() + ($2 || ' seconds')::interval,
			             user_id = EXCLUDED.user_id,
			             tenant_id = EXCLUDED.tenant_id,
			             owner_token = EXCLUDED.owner_token
			         WHERE ${table}.expires_at < now()
			       RETURNING owner_token`,
			values: [idempotencyKey, acquireTtl, userId ?? null, tenantId ?? null, ownerToken]
		});

		if (ins.rowCount > 0) {
			return { acquired: true, ownerToken: ins.rows[0].owner_token };
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
		async acquire(idempotencyKey, ttlSec, meta) {
			validateKey(idempotencyKey);
			const resultTtl = ttlSec === undefined ? ttl : ttlSec;
			if (!Number.isInteger(resultTtl) || resultTtl < 0) {
				throw new Error('postgres idempotency: ttlSec must be a non-negative integer');
			}
			// The caller's user and tenant identify the row for erasure.
			const userId = meta && typeof meta.user === 'string' ? meta.user : null;
			const tenantId = meta && typeof meta.tenant === 'string' ? meta.tenant : null;

			const cancelProbe = guardBreaker(b);
			let outcome;
			try {
				await ensureTable();
				outcome = await attemptAcquire(idempotencyKey, userId, tenantId);
				if (outcome === null) {
					// Race: row was deleted between the conflict and the read.
					// One retry catches it; if it still happens, treat as pending.
					outcome = await attemptAcquire(idempotencyKey, userId, tenantId);
					if (outcome === null) {
						outcome = { acquired: false, pending: true };
					}
				}
				b?.success();
			} catch (err) {
				pgFailure(b, err, cancelProbe);
				throw err;
			}

			if (outcome.acquired) {
				mAcquired?.inc();
				return {
					acquired: true,
					commit: commitFor(idempotencyKey, outcome.ownerToken, resultTtl),
					abort: abortFor(idempotencyKey, outcome.ownerToken)
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

		/**
		 * Right-to-erasure: delete every cached/pending row this user owns.
		 * `tenantId` disambiguates the user across tenants (a userId alone is
		 * insufficient); pass `null` for the single-tenant default. `IS NOT
		 * DISTINCT FROM` makes the NULL tenant match NULL-stamped rows.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} rows deleted
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			return withBreaker(b, async () => {
				await ensureTable();
				const res = await client.query({
					name: 'idem_purge_user_' + table,
					text: `DELETE FROM ${table} WHERE user_id = $2 AND tenant_id IS NOT DISTINCT FROM $1`,
					values: [tenantId ?? null, userId]
				});
				return res.rowCount || 0;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				return client.query(`DELETE FROM ${table}`);
			});
		},

		ready: schema.ready,
		status: schema.status,

		destroy() {
			cleanup.stop();
		}
	};
}
