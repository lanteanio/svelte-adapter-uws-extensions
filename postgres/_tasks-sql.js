/**
 * SQL helper factory for the Postgres task runner.
 *
 * `createTaskSql({ client, table, fenceTtl, rowTtl, autoMigrate })` returns
 * the bag of statements the state machine and dispatch loops use. Schema
 * migration is owned here too: `ensureTable()` is idempotent and short-
 * circuits after the first call via an internal flag.
 *
 * Statement names embed the table name so each (factory, table) pair gets
 * its own prepared-statement cache slot.
 *
 * @module svelte-adapter-uws-extensions/postgres/_tasks-sql
 */

import { safeCreate } from '../shared/pg-migrate.js';
import { serialiseError } from './_tasks-errors.js';

/**
 * @param {{
 *   client: import('./index.js').PgClient,
 *   table: string,
 *   fenceTtl: number,
 *   rowTtl: number,
 *   autoMigrate: boolean
 * }} ctx
 */
export function createTaskSql({ client, table, fenceTtl, rowTtl, autoMigrate }) {
	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				svti_tasks_id        UUID         PRIMARY KEY,
				name                 TEXT         NOT NULL,
				input                JSONB,
				svti_idempotency_key TEXT,
				request_id           TEXT,
				status               TEXT         NOT NULL,
				result               JSONB,
				error                JSONB,
				fence                UUID         NOT NULL,
				fence_expires_at     TIMESTAMPTZ  NOT NULL,
				attempts             INT          NOT NULL DEFAULT 1,
				created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
				updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
			)
		`);
		// Forward-migrate existing 0.5.0-next.1 deployments. ADD COLUMN IF NOT
		// EXISTS is idempotent on Postgres 9.6+; safeCreate swallows the
		// duplicate-column error path on older versions defensively.
		await safeCreate(client, `
			ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS request_id TEXT
		`);
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_running_fence
			    ON ${table} (fence_expires_at)
			 WHERE status = 'running'
		`);
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_terminal_updated
			    ON ${table} (updated_at)
			 WHERE status IN ('committed', 'failed')
		`);
		migrated = true;
	}

	async function insertAttempt(taskId, name, input, idempotencyKey, fence, requestId) {
		await client.query({
			name: 'tasks_insert_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, request_id, status, fence, fence_expires_at)
			       VALUES
			          ($1, $2, $3::jsonb, $4, $5, 'running', $6, now() + ($7 || ' seconds')::interval)`,
			values: [taskId, name, JSON.stringify(input ?? null), idempotencyKey ?? null, requestId ?? null, fence, fenceTtl]
		});
	}

	async function rearmAttempt(taskId, fence, attempt) {
		await client.query({
			name: 'tasks_rearm_' + table,
			text: `UPDATE ${table}
			          SET fence = $2,
			              fence_expires_at = now() + ($3 || ' seconds')::interval,
			              attempts = $4,
			              updated_at = now()
			        WHERE svti_tasks_id = $1`,
			values: [taskId, fence, fenceTtl, attempt]
		});
	}

	async function heartbeatFence(taskId, fence) {
		const res = await client.query({
			name: 'tasks_heartbeat_' + table,
			text: `UPDATE ${table}
			          SET fence_expires_at = now() + ($3 || ' seconds')::interval,
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND fence = $2 AND status = 'running'`,
			values: [taskId, fence, fenceTtl]
		});
		return res.rowCount > 0;
	}

	async function commitRow(taskId, fence, result) {
		const res = await client.query({
			name: 'tasks_commit_' + table,
			text: `UPDATE ${table}
			          SET status = 'committed',
			              result = $3::jsonb,
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND fence = $2 AND status = 'running'`,
			values: [taskId, fence, JSON.stringify(result === undefined ? null : result)]
		});
		return res.rowCount > 0;
	}

	async function failRow(taskId, fence, err) {
		const res = await client.query({
			name: 'tasks_fail_' + table,
			text: `UPDATE ${table}
			          SET status = 'failed',
			              error = $3::jsonb,
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND fence = $2 AND status = 'running'`,
			values: [taskId, fence, JSON.stringify(serialiseError(err))]
		});
		return res.rowCount > 0;
	}

	async function readRow(taskId) {
		const res = await client.query({
			name: 'tasks_read_' + table,
			text: `SELECT status, result, error, attempts, request_id FROM ${table} WHERE svti_tasks_id = $1`,
			values: [taskId]
		});
		return res.rows[0] || null;
	}

	/**
	 * List recent rows. Filters compose with AND. Newest first by
	 * `created_at`. The status filter accepts a single status; null/undefined
	 * means "all". The name filter is similar.
	 *
	 * Returns rows shaped for the public API (camelCase, Date instances for
	 * timestamps, parsed JSON for input/result/error). The internal `fence`
	 * column is intentionally excluded.
	 */
	async function listRows({ name = null, status = null, limit = 50, offset = 0 } = {}) {
		const clauses = [];
		const values = [];
		if (name !== null && name !== undefined) {
			values.push(name);
			clauses.push(`name = $${values.length}`);
		}
		if (status !== null && status !== undefined) {
			values.push(status);
			clauses.push(`status = $${values.length}`);
		}
		const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
		values.push(limit);
		const limitIdx = values.length;
		values.push(offset);
		const offsetIdx = values.length;
		const res = await client.query({
			text: `SELECT svti_tasks_id AS id,
			              name,
			              input,
			              status,
			              result,
			              error,
			              attempts,
			              request_id,
			              created_at,
			              updated_at,
			              fence_expires_at
			         FROM ${table}
			         ${where}
			        ORDER BY created_at DESC
			        LIMIT $${limitIdx} OFFSET $${offsetIdx}`,
			values
		});
		return res.rows;
	}

	/**
	 * Status counts grouped by status. Optional name filter.
	 * Always returns the full bucket set so callers don't have to
	 * normalise zeros.
	 */
	async function countByStatus({ name = null } = {}) {
		const values = [];
		let where = '';
		if (name !== null && name !== undefined) {
			values.push(name);
			where = `WHERE name = $1`;
		}
		const res = await client.query({
			text: `SELECT status, COUNT(*)::int AS n
			         FROM ${table}
			         ${where}
			        GROUP BY status`,
			values
		});
		const out = { pending: 0, running: 0, committed: 0, failed: 0, total: 0 };
		for (const r of res.rows) {
			if (r.status in out) out[r.status] = r.n;
			out.total += r.n;
		}
		return out;
	}

	/**
	 * Force-takeover a running row by expiring its fence. The recovery
	 * sweep on any live instance will reclaim the row on its next tick;
	 * the in-flight handler's heartbeat will detect the loss and abort.
	 *
	 * Returns the row's current fence UUID if a row was running and got
	 * taken over, or `null` if the row is no longer running (already
	 * terminal, never existed at this status, or somebody else expired it
	 * first). Caller can pass the returned fence to the external fence
	 * provider's release() to cut the abort latency from
	 * `heartbeatInterval` to one tick.
	 */
	async function expireFence(taskId) {
		const res = await client.query({
			name: 'tasks_expire_fence_' + table,
			text: `UPDATE ${table}
			          SET fence_expires_at = now() - interval '1 second',
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND status = 'running'
			    RETURNING fence`,
			values: [taskId]
		});
		return res.rows[0] ? res.rows[0].fence : null;
	}

	async function insertPending(taskId, name, input, idempotencyKey, requestId) {
		await client.query({
			name: 'tasks_enqueue_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, request_id, status, fence, fence_expires_at, attempts)
			       VALUES
			          ($1, $2, $3::jsonb, $4, $5, 'pending', gen_random_uuid(), now(), 0)`,
			values: [taskId, name, JSON.stringify(input ?? null), idempotencyKey ?? null, requestId ?? null]
		});
	}

	async function claimPending(limit) {
		const res = await client.query({
			name: 'tasks_claim_pending_' + table,
			text: `WITH claimed AS (
			         SELECT svti_tasks_id FROM ${table}
			          WHERE status = 'pending'
			          ORDER BY created_at ASC
			          LIMIT $1
			          FOR UPDATE SKIP LOCKED
			       )
			       UPDATE ${table} t
			          SET status = 'running',
			              fence = gen_random_uuid(),
			              fence_expires_at = now() + ($2 || ' seconds')::interval,
			              attempts = t.attempts + 1,
			              updated_at = now()
			         FROM claimed
			        WHERE t.svti_tasks_id = claimed.svti_tasks_id
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key, t.request_id, t.fence, t.attempts`,
			values: [limit, fenceTtl]
		});
		return res.rows;
	}

	async function reclaimStuck(limit) {
		const res = await client.query({
			name: 'tasks_reclaim_' + table,
			text: `WITH claimed AS (
			         SELECT svti_tasks_id FROM ${table}
			          WHERE status = 'running' AND fence_expires_at < now()
			          ORDER BY fence_expires_at ASC
			          LIMIT $1
			          FOR UPDATE SKIP LOCKED
			       )
			       UPDATE ${table} t
			          SET fence = gen_random_uuid(),
			              fence_expires_at = now() + ($2 || ' seconds')::interval,
			              attempts = t.attempts + 1,
			              updated_at = now()
			         FROM claimed
			        WHERE t.svti_tasks_id = claimed.svti_tasks_id
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key, t.request_id, t.fence, t.attempts`,
			values: [limit, fenceTtl]
		});
		return res.rows;
	}

	async function deleteOldTerminal() {
		const res = await client.query({
			name: 'tasks_cleanup_' + table,
			text: `DELETE FROM ${table}
			        WHERE status IN ('committed', 'failed')
			          AND updated_at < now() - ($1 || ' seconds')::interval`,
			values: [rowTtl]
		});
		return res.rowCount;
	}

	return {
		ensureTable,
		insertAttempt,
		rearmAttempt,
		heartbeatFence,
		commitRow,
		failRow,
		readRow,
		listRows,
		countByStatus,
		expireFence,
		insertPending,
		claimPending,
		reclaimStuck,
		deleteOldTerminal
	};
}
