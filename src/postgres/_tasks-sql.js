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
import { serialiseError, unserialisableErrorShape } from './_tasks-errors.js';
import { MAX_STORE_PAYLOAD_BYTES } from '../shared/caps.js';

/**
 * JSON-encode a task payload, refusing anything past the payload cap.
 * Returns the encoded string so callers that validate at their own
 * boundary can hand it straight down instead of stringifying twice - a
 * second pass costs ~190us on a 250KB payload.
 *
 * @param {unknown} value
 * @param {string} what - 'input' or 'result', for the error message.
 * @param {number} [maxBytes=MAX_STORE_PAYLOAD_BYTES] - The runner's
 *   `maxPayloadBytes`. Defaults to the shared store bound so a caller
 *   outside the runner keeps the previous behaviour.
 * @returns {string}
 */
export function encodeTaskPayload(value, what, maxBytes = MAX_STORE_PAYLOAD_BYTES) {
	let json;
	try {
		json = JSON.stringify(value ?? null);
	} catch (err) {
		// BigInt, a circular structure, or a throwing toJSON. Raised as our
		// own error so the terminal row says what is wrong with the value
		// instead of surfacing a bare TypeError from deep in JSON. A `toJSON`
		// is free to `throw undefined`, so the reason is read defensively -
		// reaching through it for `.message` would raise the very
		// nothing-to-do-with-the-task TypeError this exists to replace.
		let reason;
		try {
			reason = err instanceof Error ? err.message : String(err);
		} catch {
			reason = 'unknown';
		}
		throw new Error(
			`postgres tasks: ${what} is not JSON-serialisable (${reason})`,
			{ cause: err }
		);
	}
	// A function, a symbol, or an object whose toJSON returns undefined has
	// no JSON representation at all: stringify yields undefined, and passing
	// that to Buffer.byteLength raises ERR_INVALID_ARG_TYPE, terminally
	// failing the task for an unrelated-looking reason.
	if (typeof json !== 'string') {
		throw new Error(
			`postgres tasks: ${what} is not JSON-serialisable (${typeof value} has no JSON representation)`
		);
	}
	const bytes = Buffer.byteLength(json);
	if (bytes > maxBytes) {
		// Names the lever. On the RESULT path this is terminal and the
		// handler's side effects have already landed, so an operator meeting
		// it for the first time needs to know in the message itself that the
		// bound is theirs to set - not to go looking for whether one exists.
		throw new Error(
			`postgres tasks: ${what} is ${bytes} bytes, past the ${maxBytes}-byte payload cap ` +
			'(raise `maxPayloadBytes` on createTaskRunner, or store the payload elsewhere and pass a reference)'
		);
	}
	return json;
}

/**
 * JSON-encode a task error for the `error` column.
 *
 * Deliberately NOT `encodeTaskPayload`: this is the terminal path, and it
 * must never throw. Refusing to write an oversized error would leave the
 * row `running` under a live fence, and the recovery sweep would then hand
 * the handler - and its side effects - back to a worker once per fence
 * expiry, forever. An error past the cap is replaced by a stand-in that
 * keeps the identifying fields and reports the truncation, so the row
 * still reaches a terminal state and the dashboard still shows why.
 *
 * Size is not the only way encoding can fail. `serialiseError` copies
 * `code` verbatim and, when opted in, a non-Error `cause` verbatim too, so a
 * handler that attaches a BigInt code or a circular object makes
 * `JSON.stringify` throw. That throw would escape `failRow`'s parameter list
 * and strand the row exactly as an oversized error once did, so every failure
 * mode degrades to a stand-in instead.
 *
 * @param {unknown} err
 * @param {boolean} includeCause
 * @param {number} [maxBytes=MAX_STORE_PAYLOAD_BYTES] - The runner's
 *   `maxPayloadBytes`. The error shares the payload column, so it shares
 *   the bound - but it TRUNCATES to fit where a payload refuses.
 * @returns {string}
 */
export function encodeTaskError(err, includeCause, maxBytes = MAX_STORE_PAYLOAD_BYTES) {
	try {
		const full = JSON.stringify(serialiseError(err, { includeCause }));
		if (Buffer.byteLength(full) <= maxBytes) return full;
		const lean = serialiseError(err, { includeCause: false });
		delete lean.stack;
		if (typeof lean.message === 'string') {
			lean.message = lean.message.slice(0, 2048);
		}
		lean.truncated = true;
		const trimmed = JSON.stringify(lean);
		if (Buffer.byteLength(trimmed) <= maxBytes) return trimmed;
		const discarded = JSON.stringify({
			name: typeof lean.name === 'string' ? lean.name.slice(0, 256) : 'Error',
			message: `error exceeded the ${maxBytes}-byte payload cap and was discarded`,
			truncated: true
		});
		return Buffer.byteLength(discarded) <= maxBytes ? discarded : TRUNCATED_ERROR_JSON;
	} catch {
		// The stand-in is built from primitives read once and coerced, so
		// encoding it cannot fail - but this is the branch that must not
		// throw under any circumstances, so it has a constant floor.
		try {
			const fallback = JSON.stringify(unserialisableErrorShape(err));
			return Buffer.byteLength(fallback) <= maxBytes ? fallback : UNSERIALISABLE_ERROR_JSON;
		} catch {
			return UNSERIALISABLE_ERROR_JSON;
		}
	}
}

/** Smallest useful oversized-error shape; fixed so its size cannot drift. */
const TRUNCATED_ERROR_JSON =
	'{"name":"Error","message":"error exceeded payload cap","truncated":true}';

/** Floor for the terminal encode. Written out so producing it cannot fail. */
const UNSERIALISABLE_ERROR_JSON =
	'{"name":"Error","message":"error could not be serialised","unserialisable":true}';
export const MIN_TASK_PAYLOAD_BYTES = Math.max(
	Buffer.byteLength(TRUNCATED_ERROR_JSON),
	Buffer.byteLength(UNSERIALISABLE_ERROR_JSON)
);



/**
 * @param {{
 *   client: import('./index.js').PgClient,
 *   table: string,
 *   fenceTtl: number,
 *   rowTtl: number,
 *   autoMigrate: boolean,
 *   maxPayloadBytes?: number
 * }} ctx
 */
export function createTaskSql({ client, table, fenceTtl, rowTtl, autoMigrate, serializeErrorCause = false, maxPayloadBytes = MAX_STORE_PAYLOAD_BYTES }) {
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
		`, {
			table,
			// `request_id` is intentionally NOT in the expected set: it's
			// added by the ALTER TABLE below for forward-migrated next.1
			// deployments, so the table may legitimately pre-exist without
			// it before the ALTER runs.
			columns: ['svti_tasks_id', 'name', 'input', 'svti_idempotency_key', 'status', 'result', 'error', 'fence', 'fence_expires_at', 'attempts', 'created_at', 'updated_at']
		});
		// Forward-migrate existing 0.5.0-next.1 deployments. ADD COLUMN IF NOT
		// EXISTS is idempotent on Postgres 9.6+; safeCreate swallows the
		// duplicate-column error path on older versions defensively.
		await safeCreate(client, `
			ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS request_id TEXT
		`);
		// Right-to-erasure column: a task input may carry the enqueuing user's PII,
		// so row DELETE is the only clean erasure. Stamped from the runner's
		// forgetUserId extractor; ADD COLUMN IF NOT EXISTS forward-migrates.
		await safeCreate(client, `
			ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS user_id TEXT
		`);
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_user ON ${table} (user_id)
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

	/**
	 * Takes the already-encoded input that run() validated at its caller
	 * boundary. Re-encoding here would invoke a user-defined toJSON twice:
	 * the first value could pass the cap and the second could fail inside the
	 * storage breaker, misclassifying caller input as a transient DB failure.
	 */
	async function insertAttempt(taskId, name, encodedInput, idempotencyKey, fence, requestId, userId) {
		await client.query({
			name: 'tasks_insert_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, request_id, status, fence, fence_expires_at, user_id)
			       VALUES
			          ($1, $2, $3::jsonb, $4, $5, 'running', $6, now() + ($7 || ' seconds')::interval, $8)`,
			values: [taskId, name, encodedInput, idempotencyKey ?? null, requestId ?? null, fence, fenceTtl, userId ?? null]
		});
	}

	async function rearmAttempt(taskId, priorFence, nextFence, attempt) {
		// Fence-guarded: only rotate the fence if this worker still holds the row
		// (fence = priorFence). A worker whose fence was taken over mid-handler
		// matches zero rows here, so its retry cannot re-steal the row back from
		// the successor. Returns false on a lost fence.
		const res = await client.query({
			name: 'tasks_rearm_' + table,
			text: `UPDATE ${table}
			          SET fence = $3,
			              fence_expires_at = now() + ($4 || ' seconds')::interval,
			              attempts = $5,
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND fence = $2`,
			values: [taskId, priorFence, nextFence, fenceTtl, attempt]
		});
		return res.rowCount > 0;
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

	/**
	 * Takes the ALREADY-ENCODED result JSON, not the raw value: the caller
	 * validates against the payload cap at its own boundary and hands the
	 * string straight down, so the row is stringified once rather than twice.
	 * The contract is deliberately not polymorphic - a task whose result is
	 * itself a string would be indistinguishable from a pre-encoded one, and
	 * guessing would write unquoted text into a jsonb column.
	 */
	async function commitRow(taskId, fence, encodedResult) {
		const res = await client.query({
			name: 'tasks_commit_' + table,
			text: `UPDATE ${table}
			          SET status = 'committed',
			              result = $3::jsonb,
			              updated_at = now()
			        WHERE svti_tasks_id = $1 AND fence = $2 AND status = 'running'`,
			values: [taskId, fence, encodedResult]
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
			values: [taskId, fence, encodeTaskError(err, serializeErrorCause, maxPayloadBytes)]
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

	/** Same already-encoded contract as insertAttempt(), for enqueue(). */
	async function insertPending(taskId, name, encodedInput, idempotencyKey, requestId, userId) {
		await client.query({
			name: 'tasks_enqueue_' + table,
			text: `INSERT INTO ${table}
			          (svti_tasks_id, name, input, svti_idempotency_key, request_id, user_id, status, fence, fence_expires_at, attempts)
			       VALUES
			          ($1, $2, $3::jsonb, $4, $5, $6, 'pending', gen_random_uuid(), now(), 0)`,
			values: [taskId, name, encodedInput, idempotencyKey ?? null, requestId ?? null, userId ?? null]
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

	// Right-to-erasure: delete every task row stamped with this user's id.
	async function deleteByUser(userId) {
		const res = await client.query({
			name: 'tasks_delete_user_' + table,
			text: `DELETE FROM ${table} WHERE user_id = $1`,
			values: [userId]
		});
		return res.rowCount || 0;
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
		deleteOldTerminal,
		deleteByUser
	};
}
