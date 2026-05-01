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

	async function insertAttempt(taskId, name, input, idempotencyKey, fence) {
		await client.query({
			name: 'tasks_insert_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, status, fence, fence_expires_at)
			       VALUES
			          ($1, $2, $3::jsonb, $4, 'running', $5, now() + ($6 || ' seconds')::interval)`,
			values: [taskId, name, JSON.stringify(input ?? null), idempotencyKey ?? null, fence, fenceTtl]
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
			text: `SELECT status, result, error, attempts FROM ${table} WHERE svti_tasks_id = $1`,
			values: [taskId]
		});
		return res.rows[0] || null;
	}

	async function insertPending(taskId, name, input, idempotencyKey) {
		await client.query({
			name: 'tasks_enqueue_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, status, fence, fence_expires_at, attempts)
			       VALUES
			          ($1, $2, $3::jsonb, $4, 'pending', gen_random_uuid(), now(), 0)`,
			values: [taskId, name, JSON.stringify(input ?? null), idempotencyKey ?? null]
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
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key, t.fence, t.attempts`,
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
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key, t.fence, t.attempts`,
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
		insertPending,
		claimPending,
		reclaimStuck,
		deleteOldTerminal
	};
}
