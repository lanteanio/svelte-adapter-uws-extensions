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

import { createStoreSchema } from './migrations.js';
import { serialiseError, unserialisableErrorShape } from './_tasks-errors.js';
import { MAX_STORE_PAYLOAD_BYTES } from '../shared/caps.js';
import { withTransaction } from '../shared/pg-tx.js';

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
	const schema = createStoreSchema(client, 'tasks', table, autoMigrate);
	const ensureTable = schema.ensure;

	/*
	 * A row stamped with an erasable identity is inserted under a shared
	 * transaction-level advisory lock on that identity; deleteByIdentity takes
	 * the same lock exclusively before its DELETE. An insert that is in flight
	 * when an erasure starts therefore commits before the DELETE takes its
	 * snapshot and is erased with the rest; one that starts after the erasure
	 * committed is a later write. The key hashes the table's OID with the
	 * identity, so every spelling of one table that resolves to it on the
	 * search path shares the lock.
	 */
	const COLUMN_TYPES = {
		svti_tasks_id: 'uuid', name: 'text', svti_idempotency_key: 'text', request_id: 'text', fence: 'uuid',
		user_id: 'text', traceparent: 'text', tracestate: 'text', identity_version: 'integer', tenant_id: 'text'
	};
	const FENCE_KEY = (first) => `hashtextextended($${first}::regclass::oid::text || ':' || $${first + 1}::text, 0)`;
	function insertStatement(name, columns, expressions, values, stamp) {
		const fenced = stamp.version === 1 && stamp.userId !== null;
		if (!fenced) {
			return { name: name + table, text: `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${expressions.join(', ')})`, values };
		}
		// INSERT ... SELECT infers no parameter types from the target columns,
		// so every bare parameter is cast to its column's type.
		const typed = expressions.map((expression, i) => (/^\$\d+$/.test(expression) ? `${expression}::${COLUMN_TYPES[columns[i]]}` : expression));
		return {
			name: name + 'fenced_' + table,
			text: `INSERT INTO ${table} (${columns.join(', ')})
			       SELECT ${typed.join(', ')}
			         FROM (SELECT pg_advisory_xact_lock_shared(${FENCE_KEY(values.length + 1)})) AS fence`,
			values: [...values, table, JSON.stringify([stamp.tenantId, stamp.userId])]
		};
	}

	/**
	 * Takes the already-encoded input that run() validated at its caller
	 * boundary. Re-encoding here would invoke a user-defined toJSON twice:
	 * the first value could pass the cap and the second could fail inside the
	 * storage breaker, misclassifying caller input as a transient DB failure.
	 */
	async function insertAttempt(taskId, name, encodedInput, idempotencyKey, fence, requestId, stamp, traceContext) {
		const columns = ['svti_tasks_id', 'name', 'input', 'svti_idempotency_key', 'request_id', 'status', 'fence', 'fence_expires_at', 'user_id'];
		const expressions = ['$1', '$2', '$3::jsonb', '$4', '$5', "'running'", '$6', "now() + ($7 || ' seconds')::interval", '$8'];
		const values = [taskId, name, encodedInput, idempotencyKey ?? null, requestId ?? null, fence, fenceTtl, stamp.userId];
		if (traceContext != null) {
			columns.push('traceparent', 'tracestate');
			expressions.push('$9', '$10');
			values.push(traceContext.traceparent, traceContext.tracestate ?? null);
		}
		columns.push('identity_version', 'tenant_id');
		expressions.push('$' + (values.length + 1), '$' + (values.length + 2));
		values.push(stamp.version, stamp.tenantId);
		await client.query(insertStatement(traceContext != null ? 'tasks_insert_trace_' : 'tasks_insert_', columns, expressions, values, stamp));
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

	async function heartbeatFence(taskId, fence, timeoutMs) {
		const res = await client.query({
			name: 'tasks_heartbeat_' + table,
			query_timeout: timeoutMs,
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
			text: `SELECT status, result, error, attempts, request_id, fence FROM ${table} WHERE svti_tasks_id = $1`,
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
			              traceparent,
			              tracestate,
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
	async function insertPending(taskId, name, encodedInput, idempotencyKey, requestId, stamp, traceContext) {
		const columns = ['svti_tasks_id', 'name', 'input', 'svti_idempotency_key', 'request_id', 'user_id'];
		const expressions = ['$1', '$2', '$3::jsonb', '$4', '$5', '$6'];
		const values = [taskId, name, encodedInput, idempotencyKey ?? null, requestId ?? null, stamp.userId];
		if (traceContext != null) {
			columns.push('traceparent', 'tracestate');
			expressions.push('$7', '$8');
			values.push(traceContext.traceparent, traceContext.tracestate ?? null);
		}
		columns.push('status', 'fence', 'fence_expires_at', 'attempts', 'identity_version', 'tenant_id');
		expressions.push("'pending'", 'gen_random_uuid()', 'now()', '0', '$' + (values.length + 1), '$' + (values.length + 2));
		values.push(stamp.version, stamp.tenantId);
		await client.query(insertStatement(traceContext != null ? 'tasks_enqueue_trace_' : 'tasks_enqueue_', columns, expressions, values, stamp));
	}

	/*
	 * Durable enqueue receipts. One statement either inserts the receipt for
	 * a free (or expired) scope together with its pending task row, or
	 * changes nothing: the task insert reads its id from the receipt CTE's
	 * RETURNING, so a conflict that keeps a live receipt inserts no task, and
	 * a failure of either insert rolls back both. Concurrent producers of one
	 * scope serialize on the receipts primary key. An expired receipt is
	 * replaced in place by the ON CONFLICT update under the same row lock.
	 * Times are database epoch milliseconds from the statement's now().
	 */
	const receipts = table + '_receipts';
	const NOW_MS = 'floor(extract(epoch FROM now()) * 1000)::bigint';

	async function insertReceiptTask(scope, taskId, name, encodedInput, idempotencyKey, requestId, stamp, traceContext, ttlMs) {
		const fenced = stamp.version === 1 && stamp.userId !== null;
		const values = [scope, taskId, encodedInput, stamp.version, stamp.tenantId, stamp.userId, ttlMs, name, idempotencyKey, requestId ?? null,
			traceContext?.traceparent ?? null, traceContext?.tracestate ?? null];
		if (fenced) values.push(table, JSON.stringify([stamp.tenantId, stamp.userId]));
		const res = await client.query({
			name: (fenced ? 'tasks_receipt_fenced_' : 'tasks_receipt_') + table,
			text: `WITH receipt AS (
			         INSERT INTO ${receipts} AS r (scope, svti_tasks_id, input, identity_version, tenant_id, user_id, created_at, dedup_expires_at)
			         SELECT $1::text, $2::uuid, $3::text, $4::integer, $5::text, $6::text, c.now_ms, c.now_ms + $7::bigint
			           FROM (SELECT ${NOW_MS} AS now_ms${fenced ? `, pg_advisory_xact_lock_shared(${FENCE_KEY(13)}) AS fence` : ''}) AS c
			         ON CONFLICT (scope) DO UPDATE
			            SET svti_tasks_id = EXCLUDED.svti_tasks_id, input = EXCLUDED.input,
			                identity_version = EXCLUDED.identity_version, tenant_id = EXCLUDED.tenant_id, user_id = EXCLUDED.user_id,
			                created_at = EXCLUDED.created_at, dedup_expires_at = EXCLUDED.dedup_expires_at
			          WHERE r.dedup_expires_at <= EXCLUDED.created_at
			      RETURNING r.svti_tasks_id, r.dedup_expires_at
			       ), task AS (
			         INSERT INTO ${table} (svti_tasks_id, name, input, svti_idempotency_key, request_id, user_id, traceparent, tracestate,
			                               status, fence, fence_expires_at, attempts, identity_version, tenant_id)
			         SELECT receipt.svti_tasks_id, $8::text, $3::text::jsonb, $9::text, $10::text, $6::text, $11::text, $12::text,
			                'pending', gen_random_uuid(), now(), 0, $4::integer, $5::text
			           FROM receipt
			       )
			       SELECT svti_tasks_id AS id, dedup_expires_at::text AS expires FROM receipt`,
			values
		});
		const row = res.rows[0];
		return row ? { taskId: row.id, dedupExpiresAt: Number(row.expires) } : null;
	}

	// The scope's mapping as the primary sees it now, live or not.
	async function readReceipt(scope) {
		const res = await client.query({
			name: 'tasks_receipt_read_' + table,
			text: `SELECT svti_tasks_id AS id, input, identity_version, tenant_id, user_id, dedup_expires_at::text AS expires,
			              dedup_expires_at > ${NOW_MS} AS live
			         FROM ${receipts} WHERE scope = $1`,
			values: [scope]
		});
		return res.rows[0] ?? null;
	}

	async function deleteExpiredReceipts(budget, limit) {
		return budget.remove(`WITH expired AS (
			SELECT scope FROM ${receipts}
			WHERE dedup_expires_at <= ${NOW_MS}
			ORDER BY dedup_expires_at LIMIT $1 FOR UPDATE SKIP LOCKED
		) DELETE FROM ${receipts} r USING expired WHERE r.scope = expired.scope AND r.dedup_expires_at <= ${NOW_MS}`, [limit]);
	}

	/*
	 * Pending dispatch is fair across tenant scopes. A scope is one tenant of
	 * qualified rows, the untenanted qualified scope, or the rows written
	 * without forgetIdentity. Each handler name keeps its own rotation over
	 * the scopes with pending rows of that name, in the order LEGACY,
	 * UNTENANTED, then tenant ids in index order. A claim takes the next
	 * scopes of each of its names' rotations, interleaved name by name
	 * (the name whose position moved longest ago first), up to `limit`
	 * scopes.
	 * A name's position then moves past the leading scopes of its rotation
	 * that the claim chose, and no further: a scope is never passed over
	 * because a claim chose later scopes for other names, and a claim that
	 * chose none of a name's scopes leaves that name where it was. Rows are
	 * then claimed round by round (every chosen scope's oldest row before any
	 * scope's second), FIFO inside a scope, up to `limit` rows in all.
	 *
	 * Every read goes through the pending-scope index
	 * (identity_version, name, tenant_id, created_at) with equality on the
	 * name, so rows of names this runner does not handle are never read and
	 * the cost is bounded by the number of names n, not by any backlog.
	 * Finding the scopes takes at most n * (2 * limit + 2) single-entry
	 * probes. With k chosen scopes each may contribute up to limit - k + 1
	 * rows (when one scope has that many the others' first rows still fill
	 * the claim, and when none has, every row fits), read as at most
	 * n * k * (limit - k + 1) index entries. Those reads lock their rows with
	 * SKIP LOCKED, so rows another claim holds are passed over for the next
	 * ones instead of filling the scope's share; the rows read but not
	 * claimed stay pending and are unlocked when the claim's transaction
	 * ends.
	 */
	const LEGACY = 1, UNTENANTED = 2;
	/** @type {Map<string, 1 | 2 | string>} rotation position per handler name */
	const cursors = new Map();
	/** @type {Map<string, number>} claim number each name was last served in */
	const lastServed = new Map();
	let claims = 0;

	const scopeProbe = (where) => `EXISTS (SELECT 1 FROM ${table} p
	          WHERE p.status = 'pending' AND p.name = q.name AND ${where})`;
	const nextTenant = (after) => `(SELECT p.tenant_id FROM ${table} p
	          WHERE p.status = 'pending' AND p.identity_version = 1 AND p.name = q.name AND ${after}
	          ORDER BY p.identity_version, p.name, p.tenant_id LIMIT 1)`;
	const tenantWalk = (first) => `ARRAY(
	         WITH RECURSIVE walk(tenant_id, n) AS (
	           SELECT ${nextTenant(first)}, 1
	           UNION ALL
	           SELECT ${nextTenant('p.tenant_id > w.tenant_id')}, w.n + 1
	             FROM walk w WHERE w.tenant_id IS NOT NULL AND w.n < $2
	         ) SELECT tenant_id FROM walk WHERE tenant_id IS NOT NULL ORDER BY n)`;
	const scopeRows = (where) => `SELECT p.svti_tasks_id, p.created_at FROM ${table} p
	           WHERE p.status = 'pending' AND p.name = q.name AND ${where}
	           ORDER BY p.identity_version, p.name, p.tenant_id, p.created_at LIMIT $5
	           FOR UPDATE OF p SKIP LOCKED`;

	// One name's rotation from its position: the scopes after it, then the
	// ones from the start up to and including it.
	function rotation(cursor, found) {
		const virtual = [];
		if (found.legacy === true) virtual.push(LEGACY);
		if (found.untenanted === true) virtual.push(UNTENANTED);
		const after = Array.isArray(found.after) ? found.after : [];
		const head = Array.isArray(found.head) ? found.head : [];
		// The walk after the cursor holds every tenant past it whenever it is
		// shorter than `limit`, so the head walk minus those is exactly the
		// wrap-around part, in index order and without comparing ids here.
		if (typeof cursor === 'string') {
			const seen = new Set(after);
			return [...after, ...virtual, ...head.filter(tenant => !seen.has(tenant))];
		}
		if (cursor === LEGACY) return [...virtual.filter(scope => scope !== LEGACY), ...head, ...virtual.filter(scope => scope === LEGACY)];
		if (cursor === UNTENANTED) return [...head, ...virtual];
		return [...virtual, ...head];
	}

	/**
	 * @param {number} limit
	 * @param {string[]} registeredNames
	 */
	async function claimPending(limit, registeredNames) {
		const names = [...registeredNames];
		if (names.length === 0) return [];
		const found = (await client.query({
			name: 'tasks_claim_scopes_' + table,
			text: `SELECT q.name,
			              ${scopeProbe('p.identity_version IS NULL AND p.tenant_id IS NULL')} AS legacy,
			              ${scopeProbe('p.identity_version = 1 AND p.tenant_id IS NULL')} AS untenanted,
			              ${tenantWalk('p.tenant_id > q.cursor')} AS after,
			              ${tenantWalk('p.tenant_id IS NOT NULL')} AS head
			         FROM unnest($1::text[], $3::text[]) AS q(name, cursor)`,
			values: [names, limit, names.map((name) => { const cursor = cursors.get(name); return typeof cursor === 'string' ? cursor : null; })]
		})).rows;
		const rotations = new Map();
		for (const row of found) rotations.set(row.name, rotation(cursors.get(row.name) ?? null, row));
		// Interleave the names' rotations, the name served longest ago first,
		// so a limit smaller than the number of names still serves each in turn.
		const claim = ++claims;
		const turn = names
			.filter((name) => rotations.get(name)?.length > 0)
			.sort((a, b) => (lastServed.get(a) ?? 0) - (lastServed.get(b) ?? 0));
		const chosen = [], picked = new Set();
		for (let i = 0; chosen.length < limit; i++) {
			let more = false;
			for (const name of turn) {
				const scope = rotations.get(name)[i];
				if (scope === undefined) continue;
				more = true;
				if (picked.has(scope)) continue;
				picked.add(scope);
				chosen.push(scope);
				if (chosen.length === limit) break;
			}
			if (!more) break;
		}
		// Each name moves past the leading run of its rotation that was chosen.
		for (const name of turn) {
			let last;
			for (const scope of rotations.get(name)) {
				if (!picked.has(scope)) break;
				last = scope;
			}
			// A name counts as served only when its own position moved.
			if (last !== undefined) {
				cursors.set(name, last);
				lastServed.set(name, claim);
			}
		}
		if (chosen.length === 0) return [];
		const tenants = [], ords = [];
		let legacyOrd = 0, untenantedOrd = 0;
		chosen.forEach((scope, index) => {
			if (scope === LEGACY) legacyOrd = index + 1;
			else if (scope === UNTENANTED) untenantedOrd = index + 1;
			else { tenants.push(scope); ords.push(index + 1); }
		});
		const res = await client.query({
			name: 'tasks_claim_registered_' + table,
			text: `WITH candidates AS (
			         SELECT c.svti_tasks_id, c.created_at, s.ord
			           FROM unnest($4::text[], $6::int[]) AS s(tenant_id, ord)
			          CROSS JOIN unnest($3::text[]) AS q(name)
			          CROSS JOIN LATERAL (${scopeRows('p.identity_version = 1 AND p.tenant_id = s.tenant_id')}) c
			         UNION ALL
			         SELECT c.svti_tasks_id, c.created_at, $7::int
			           FROM unnest($3::text[]) AS q(name)
			          CROSS JOIN LATERAL (${scopeRows('p.identity_version IS NULL AND p.tenant_id IS NULL')}) c
			          WHERE $7::int > 0
			         UNION ALL
			         SELECT c.svti_tasks_id, c.created_at, $8::int
			           FROM unnest($3::text[]) AS q(name)
			          CROSS JOIN LATERAL (${scopeRows('p.identity_version = 1 AND p.tenant_id IS NULL')}) c
			          WHERE $8::int > 0
			       ),
			       ranked AS (
			         SELECT svti_tasks_id, ord, row_number() OVER (PARTITION BY ord ORDER BY created_at, svti_tasks_id) AS pos
			           FROM candidates
			       ),
			       claimed AS (
			         SELECT t.svti_tasks_id
			           FROM ranked r JOIN ${table} t ON t.svti_tasks_id = r.svti_tasks_id
			          WHERE t.status = 'pending'
			          ORDER BY r.pos, r.ord
			          LIMIT $1
			       )
			       UPDATE ${table} t
			          SET status = 'running',
			              fence = gen_random_uuid(),
			              fence_expires_at = now() + ($2 || ' seconds')::interval,
			              attempts = t.attempts + 1,
			              updated_at = now()
			         FROM claimed
			        WHERE t.svti_tasks_id = claimed.svti_tasks_id AND t.status = 'pending'
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key,
			              t.request_id, t.traceparent, t.tracestate, t.fence, t.attempts`,
			values: [limit, fenceTtl, names, tenants, limit - chosen.length + 1, ords, legacyOrd, untenantedOrd]
		});
		return res.rows;
	}

	async function reclaimStuck(limit, registeredNames) {
		const res = await client.query({
			name: 'tasks_reclaim_registered_' + table,
			text: `WITH claimed AS (
			         SELECT svti_tasks_id FROM ${table}
			          WHERE status = 'running' AND fence_expires_at < now()
			          AND name = ANY($3::text[])
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
			    RETURNING t.svti_tasks_id AS id, t.name, t.input, t.svti_idempotency_key AS idempotency_key,
			              t.request_id, t.traceparent, t.tracestate, t.fence, t.attempts`,
			values: [limit, fenceTtl, registeredNames]
		});
		return res.rows;
	}

	async function deleteOldTerminal(budget, limit) {
		return budget.remove(`WITH expired AS (
			SELECT svti_tasks_id FROM ${table}
			WHERE status IN ('committed', 'failed')
			AND updated_at < now() - ($1 || ' seconds')::interval
			ORDER BY updated_at LIMIT $2 FOR UPDATE SKIP LOCKED
		) DELETE FROM ${table} r USING expired WHERE r.svti_tasks_id = expired.svti_tasks_id`, [rowTtl, limit]);
	}

	// Right-to-erasure: delete every unqualified task row and enqueue receipt
	// stamped with this user's id, in one statement. Rows a forgetIdentity
	// runner stamped belong to one tenant's user and are erased only through
	// their exact identity.
	async function deleteByUser(userId) {
		const res = await client.query({
			name: 'tasks_delete_user_' + table,
			text: `WITH receipts AS (
			         DELETE FROM ${receipts} WHERE user_id = $1 AND identity_version IS NULL RETURNING 1
			       ), tasks AS (
			         DELETE FROM ${table} WHERE user_id = $1 AND identity_version IS NULL RETURNING 1
			       )
			       SELECT ((SELECT count(*) FROM receipts) + (SELECT count(*) FROM tasks))::int AS removed`,
			values: [userId]
		});
		return Number(res.rows[0]?.removed) || 0;
	}

	// Qualified erasure: delete the task rows and enqueue receipts stamped with
	// exactly (tenantId, userId), and in the same statement report whether
	// unqualified task rows or receipts remain
	// that could still be this user's: rows with the same raw user id, or with
	// none. Their tenant is unknown, so they are neither deleted nor ruled out.
	// The exclusive identity lock first waits out inserts of this identity
	// already in flight (see insertStatement).
	async function deleteByIdentity(identity) {
		const untenanted = identity.tenantId === null;
		return withTransaction(client, async (tx) => {
			await tx.query({
				name: 'tasks_erase_fence_' + table,
				text: `SELECT pg_advisory_xact_lock(${FENCE_KEY(1)})`,
				values: [table, JSON.stringify([identity.tenantId, identity.userId])]
			});
			return deleteIdentityRows(tx, identity, untenanted);
		});
	}

	async function deleteIdentityRows(tx, identity, untenanted) {
		const res = await tx.query({
			name: (untenanted ? 'tasks_delete_untenanted_' : 'tasks_delete_identity_') + table,
			text: `WITH removed AS (
			         DELETE FROM ${table}
			          WHERE identity_version = 1 AND ${untenanted ? 'tenant_id IS NULL' : 'tenant_id = $2'} AND user_id = $1
			      RETURNING 1
			       ), erased_receipts AS (
			         DELETE FROM ${receipts}
			          WHERE identity_version = 1 AND ${untenanted ? 'tenant_id IS NULL' : 'tenant_id = $2'} AND user_id = $1
			      RETURNING 1
			       )
			       SELECT ((SELECT count(*) FROM removed) + (SELECT count(*) FROM erased_receipts))::int AS removed,
			              (EXISTS (SELECT 1 FROM ${table} WHERE identity_version IS NULL AND tenant_id IS NULL AND user_id = $1)
			               OR EXISTS (SELECT 1 FROM ${table} WHERE identity_version IS NULL AND tenant_id IS NULL AND user_id IS NULL)
			               OR EXISTS (SELECT 1 FROM ${receipts} WHERE identity_version IS NULL AND tenant_id IS NULL AND user_id = $1)
			               OR EXISTS (SELECT 1 FROM ${receipts} WHERE identity_version IS NULL AND tenant_id IS NULL AND user_id IS NULL)) AS residue`,
			values: untenanted ? [identity.userId] : [identity.userId, identity.tenantId]
		});
		const row = res.rows[0] ?? {};
		return { removed: Number(row.removed) || 0, residue: row.residue === true };
	}

	return {
		ensureTable,
		schema,
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
		deleteByUser,
		deleteByIdentity,
		insertReceiptTask,
		readReceipt,
		deleteExpiredReceipts
	};
}
