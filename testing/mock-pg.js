import { randomUUID } from 'node:crypto';

/**
 * In-memory mock that implements the PgClient interface.
 * Parses SQL enough to simulate:
 *   - the svti_replay table + svti_replay_seq counter
 *   - the svti_idempotency key/value/expires_at table
 *   - the svti_tasks task-runner state machine
 *
 * SQL is dispatched by shape, not by table name, so custom table names
 * passed via options work as long as the column shape is recognisable.
 * Markers:
 *   - replay:        `topic`, `seq`
 *   - idempotency:   `expires_at` (without prefix), `WHERE svti_idempotency_key = $1`
 *   - tasks:         `fence_expires_at`, `svti_tasks_id`, `gen_random_uuid()`
 */
export function mockPgClient() {
	/** @type {Array<{svti_replay_id: number, topic: string, seq: number, event: string, data: any, created_at: Date}>} */
	let rows = [];
	let nextId = 1;
	let tableCreated = false;

	/** @type {Map<string, number>} topic -> seq */
	const seqCounters = new Map();

	/** @type {Map<string, {status: string, result: any, expires_at: number}>} */
	const idemRows = new Map();

	/** @type {Map<string, {id: string, name: string, input: any, idempotency_key: string|null, status: string, result: any, error: any, fence: string, fence_expires_at: number, attempts: number, created_at: number, updated_at: number}>} */
	const taskRows = new Map();

	/** @type {Map<number, {id: number, queue: string, payload: any, claimed_at: number|null, claimed_until: number|null, attempts: number, created_at: Date}>} */
	const jobRows = new Map();
	let jobNextId = 1;

	function idemNow() {
		return Date.now();
	}

	const client = {
		// `pool.connect()` returns a pinned-connection wrapper. The mock
		// does not model Postgres-side transaction semantics (statements
		// queued between BEGIN and COMMIT just go through the same query
		// dispatch); production atomicity is verified at the integration
		// tier. BEGIN / COMMIT / ROLLBACK are accepted as no-ops so the
		// shared `withTransaction` helper works against this mock.
		pool: {
			async connect() {
				return {
					query: (textOrObj, values) => client.query(textOrObj, values),
					release: () => {}
				};
			}
		},

		async query(textOrObj, values) {
			if (typeof textOrObj === 'object' && textOrObj !== null) {
				values = textOrObj.values || [];
				textOrObj = textOrObj.text;
			}
			if (!values) values = [];
			const sql = textOrObj.trim().replace(/\s+/g, ' ');

			// Transaction-control statements: accepted as no-ops by the mock.
			// The mock does not model atomicity; rely on the integration tier
			// for that assertion.
			if (sql === 'BEGIN' || sql === 'COMMIT' || sql === 'ROLLBACK') {
				return { rows: [], rowCount: 0 };
			}

			// CREATE TABLE
			if (sql.startsWith('CREATE TABLE')) {
				tableCreated = true;
				return { rows: [], rowCount: 0 };
			}

			// CREATE INDEX
			if (sql.startsWith('CREATE INDEX')) {
				return { rows: [], rowCount: 0 };
			}

			// ALTER TABLE - no-op: the mock's row shapes are dynamic so any
			// added column is automatically accepted by INSERT / SELECT branches
			// that reference it.
			if (sql.startsWith('ALTER TABLE')) {
				return { rows: [], rowCount: 0 };
			}

			// ----- Idempotency dispatch (matched first; markers: `expires_at`, `WHERE key`)

			// Acquire: INSERT ... ON CONFLICT (svti_idempotency_key) DO UPDATE ... WHERE expires_at < now() RETURNING status
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('ON CONFLICT (svti_idempotency_key)') &&
				sql.includes('expires_at')
			) {
				const key = values[0];
				const acquireTtlSec = Number(values[1]);
				const expiresAt = idemNow() + acquireTtlSec * 1000;
				const existing = idemRows.get(key);
				if (!existing) {
					idemRows.set(key, { status: 'pending', result: null, expires_at: expiresAt });
					return { rows: [{ status: 'pending' }], rowCount: 1 };
				}
				if (existing.expires_at < idemNow()) {
					idemRows.set(key, { status: 'pending', result: null, expires_at: expiresAt });
					return { rows: [{ status: 'pending' }], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// Read: SELECT status, result FROM ... WHERE svti_idempotency_key = $1 AND expires_at >= now()
			if (
				sql.startsWith('SELECT status, result FROM') &&
				sql.includes('WHERE svti_idempotency_key = $1') &&
				sql.includes('expires_at')
			) {
				const key = values[0];
				const row = idemRows.get(key);
				if (!row || row.expires_at < idemNow()) {
					return { rows: [], rowCount: 0 };
				}
				return { rows: [{ status: row.status, result: row.result }], rowCount: 1 };
			}

			// Commit: UPDATE ... SET status = 'committed', result = $2::jsonb, expires_at = ...
			// (Idempotency-specific: WHERE svti_idempotency_key = $1.  Task commits match a different branch below.)
			if (
				sql.startsWith('UPDATE') &&
				sql.includes("status = 'committed'") &&
				sql.includes('WHERE svti_idempotency_key = $1')
			) {
				const key = values[0];
				const result = typeof values[1] === 'string' ? JSON.parse(values[1]) : values[1];
				const ttlSec = Number(values[2]);
				const row = idemRows.get(key);
				if (row) {
					row.status = 'committed';
					row.result = result;
					row.expires_at = idemNow() + ttlSec * 1000;
				}
				return { rows: [], rowCount: row ? 1 : 0 };
			}

			// Cleanup: DELETE FROM ... WHERE expires_at < now()
			if (sql.startsWith('DELETE FROM') && sql.includes('expires_at < now()')) {
				let removed = 0;
				const now = idemNow();
				for (const [k, v] of idemRows) {
					if (v.expires_at < now) {
						idemRows.delete(k);
						removed++;
					}
				}
				return { rows: [], rowCount: removed };
			}

			// Abort / purge: DELETE FROM ... WHERE svti_idempotency_key = $1
			if (sql.startsWith('DELETE FROM') && sql.includes('WHERE svti_idempotency_key = $1')) {
				const key = values[0];
				const had = idemRows.delete(key);
				return { rows: [], rowCount: had ? 1 : 0 };
			}

			// Idempotency clear (default table name).  Custom table names
			// fall through to the replay-table catch-all and would clear
			// replay rows instead - tests should use the default name.
			if (sql.startsWith('DELETE FROM svti_idempotency') && !sql.includes('WHERE')) {
				const before = idemRows.size;
				idemRows.clear();
				return { rows: [], rowCount: before };
			}

			// ----- Task runner dispatch (markers: `fence_expires_at`, `svti_tasks_id`)

			// Task INSERT (run path): row creation with fresh fence, status='running'
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('svti_tasks_id') &&
				sql.includes('fence_expires_at') &&
				sql.includes('svti_idempotency_key') &&
				!sql.includes('gen_random_uuid()')
			) {
				const fenceTtlSec = Number(values[6]);
				const now = Date.now();
				taskRows.set(values[0], {
					id: values[0],
					name: values[1],
					input: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					idempotency_key: values[3],
					request_id: values[4] ?? null,
					status: 'running',
					result: null,
					error: null,
					fence: values[5],
					fence_expires_at: now + fenceTtlSec * 1000,
					attempts: 1,
					created_at: now,
					updated_at: now
				});
				return { rows: [], rowCount: 1 };
			}

			// Task INSERT (enqueue path): status='pending', server-generated fence, attempts=0
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('svti_tasks_id') &&
				sql.includes('fence_expires_at') &&
				sql.includes('svti_idempotency_key') &&
				sql.includes("'pending'") &&
				sql.includes('gen_random_uuid()')
			) {
				const now = Date.now();
				taskRows.set(values[0], {
					id: values[0],
					name: values[1],
					input: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					idempotency_key: values[3],
					request_id: values[4] ?? null,
					status: 'pending',
					result: null,
					error: null,
					fence: randomUUID(),
					fence_expires_at: now,
					attempts: 0,
					created_at: now,
					updated_at: now
				});
				return { rows: [], rowCount: 1 };
			}

			// Task heartbeat: extend fence_expires_at while owning the fence
			if (
				sql.startsWith('UPDATE') &&
				sql.includes('fence_expires_at = now() +') &&
				sql.includes('WHERE svti_tasks_id = $1 AND fence = $2 AND status = \'running\'') &&
				!sql.includes("status = 'committed'") &&
				!sql.includes("status = 'failed'")
			) {
				const taskId = values[0];
				const fence = values[1];
				const ttlSec = Number(values[2]);
				const row = taskRows.get(taskId);
				if (row && row.fence === fence && row.status === 'running') {
					row.fence_expires_at = Date.now() + ttlSec * 1000;
					row.updated_at = Date.now();
					return { rows: [], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// Task commit: conditional set status='committed' guarded by fence
			if (
				sql.startsWith('UPDATE') &&
				sql.includes("status = 'committed'") &&
				sql.includes('WHERE svti_tasks_id = $1 AND fence = $2 AND status = \'running\'')
			) {
				const taskId = values[0];
				const fence = values[1];
				const result = typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2];
				const row = taskRows.get(taskId);
				if (row && row.fence === fence && row.status === 'running') {
					row.status = 'committed';
					row.result = result;
					row.updated_at = Date.now();
					return { rows: [], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// Task fail: conditional set status='failed' guarded by fence
			if (
				sql.startsWith('UPDATE') &&
				sql.includes("status = 'failed'") &&
				sql.includes('WHERE svti_tasks_id = $1 AND fence = $2 AND status = \'running\'')
			) {
				const taskId = values[0];
				const fence = values[1];
				const error = typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2];
				const row = taskRows.get(taskId);
				if (row && row.fence === fence && row.status === 'running') {
					row.status = 'failed';
					row.error = error;
					row.updated_at = Date.now();
					return { rows: [], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// Task rearm: unconditionally rotate the fence (used for retries)
			if (
				sql.startsWith('UPDATE') &&
				sql.includes('fence = $2') &&
				sql.includes('attempts = $4') &&
				sql.includes('WHERE svti_tasks_id = $1')
			) {
				const taskId = values[0];
				const fence = values[1];
				const ttlSec = Number(values[2]);
				const attempts = Number(values[3]);
				const row = taskRows.get(taskId);
				if (row) {
					row.fence = fence;
					row.fence_expires_at = Date.now() + ttlSec * 1000;
					row.attempts = attempts;
					row.updated_at = Date.now();
					return { rows: [], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// Task read: status, result, error, attempts, request_id
			if (
				sql.startsWith('SELECT status, result, error, attempts, request_id FROM') &&
				sql.includes('WHERE svti_tasks_id = $1')
			) {
				const taskId = values[0];
				const row = taskRows.get(taskId);
				if (!row) return { rows: [], rowCount: 0 };
				return {
					rows: [{
						status: row.status,
						result: row.result,
						error: row.error,
						attempts: row.attempts,
						request_id: row.request_id ?? null
					}],
					rowCount: 1
				};
			}

			// Task claim-pending: dispatch sweep for enqueued rows
			if (sql.includes('WITH claimed') && sql.includes("WHERE status = 'pending'")) {
				const limit = Number(values[0]);
				const ttlSec = Number(values[1]);
				const now = Date.now();
				const pending = [];
				for (const row of taskRows.values()) {
					if (row.status === 'pending') pending.push(row);
				}
				pending.sort((a, b) => a.created_at - b.created_at);
				const claimed = pending.slice(0, limit);
				const out = [];
				for (const row of claimed) {
					row.status = 'running';
					row.fence = randomUUID();
					row.fence_expires_at = now + ttlSec * 1000;
					row.attempts += 1;
					row.updated_at = now;
					out.push({
						id: row.id,
						name: row.name,
						input: row.input,
						idempotency_key: row.idempotency_key,
						request_id: row.request_id ?? null,
						fence: row.fence,
						attempts: row.attempts
					});
				}
				return { rows: out, rowCount: out.length };
			}

			// Task reclaim: stuck-row sweep with CTE + gen_random_uuid()
			if (sql.includes('WITH claimed') && sql.includes('gen_random_uuid()')) {
				const limit = Number(values[0]);
				const ttlSec = Number(values[1]);
				const now = Date.now();
				const stuck = [];
				for (const row of taskRows.values()) {
					if (row.status === 'running' && row.fence_expires_at < now) {
						stuck.push(row);
					}
				}
				stuck.sort((a, b) => a.fence_expires_at - b.fence_expires_at);
				const claimed = stuck.slice(0, limit);
				const out = [];
				for (const row of claimed) {
					row.fence = randomUUID();
					row.fence_expires_at = now + ttlSec * 1000;
					row.attempts += 1;
					row.updated_at = now;
					out.push({
						id: row.id,
						name: row.name,
						input: row.input,
						idempotency_key: row.idempotency_key,
						request_id: row.request_id ?? null,
						fence: row.fence,
						attempts: row.attempts
					});
				}
				return { rows: out, rowCount: out.length };
			}

			// Task list: SELECT svti_tasks_id AS id, name, input, status, ... ORDER BY created_at DESC
			if (
				sql.startsWith('SELECT svti_tasks_id AS id') &&
				sql.includes('ORDER BY created_at DESC') &&
				sql.includes('LIMIT')
			) {
				let rows = [...taskRows.values()];
				let valueIdx = 0;
				if (sql.includes('name = $')) {
					const filterName = values[valueIdx++];
					rows = rows.filter((r) => r.name === filterName);
				}
				if (sql.includes('status = $')) {
					const filterStatus = values[valueIdx++];
					rows = rows.filter((r) => r.status === filterStatus);
				}
				rows.sort((a, b) => b.created_at - a.created_at);
				const limit = Number(values[valueIdx++]);
				const offset = Number(values[valueIdx++]);
				const sliced = rows.slice(offset, offset + limit);
				const out = sliced.map((r) => ({
					id: r.id,
					name: r.name,
					input: r.input,
					status: r.status,
					result: r.result,
					error: r.error,
					attempts: r.attempts,
					request_id: r.request_id ?? null,
					created_at: new Date(r.created_at),
					updated_at: new Date(r.updated_at),
					fence_expires_at: new Date(r.fence_expires_at)
				}));
				return { rows: out, rowCount: out.length };
			}

			// Task counts: SELECT status, COUNT(*)::int AS n ... GROUP BY status
			if (
				sql.startsWith('SELECT status, COUNT(*)::int AS n') &&
				sql.includes('GROUP BY status')
			) {
				let rows = [...taskRows.values()];
				if (sql.includes('WHERE name = $1')) {
					rows = rows.filter((r) => r.name === values[0]);
				}
				const counts = {};
				for (const r of rows) counts[r.status] = (counts[r.status] || 0) + 1;
				const out = Object.entries(counts).map(([status, n]) => ({ status, n }));
				return { rows: out, rowCount: out.length };
			}

			// Task takeover: expire fence_expires_at, RETURNING fence
			if (
				sql.startsWith('UPDATE') &&
				sql.includes("fence_expires_at = now() - interval '1 second'") &&
				sql.includes("status = 'running'") &&
				sql.includes('RETURNING fence')
			) {
				const taskId = values[0];
				const row = taskRows.get(taskId);
				if (!row || row.status !== 'running') return { rows: [], rowCount: 0 };
				const previousFence = row.fence;
				row.fence_expires_at = Date.now() - 1000;
				row.updated_at = Date.now();
				return { rows: [{ fence: previousFence }], rowCount: 1 };
			}

			// Task cleanup: delete terminal rows older than rowTtl
			if (
				sql.startsWith('DELETE FROM') &&
				sql.includes("status IN ('committed', 'failed')") &&
				sql.includes('updated_at <')
			) {
				const ttlSec = Number(values[0]);
				const cutoff = Date.now() - ttlSec * 1000;
				let removed = 0;
				for (const [k, v] of taskRows) {
					if ((v.status === 'committed' || v.status === 'failed') && v.updated_at < cutoff) {
						taskRows.delete(k);
						removed++;
					}
				}
				return { rows: [], rowCount: removed };
			}

			// Task clear (default table name)
			if (sql.startsWith('DELETE FROM svti_tasks') && !sql.includes('WHERE')) {
				const before = taskRows.size;
				taskRows.clear();
				return { rows: [], rowCount: before };
			}

			// ----- Job queue dispatch (markers: `queue` column, no `svti_tasks_id`/`status`)

			// Job enqueue: INSERT INTO svti_jobs (queue, payload, request_id) VALUES ($1, $2, $3) RETURNING svti_jobs_id AS id
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('(queue, payload, request_id)') &&
				sql.includes('RETURNING svti_jobs_id AS id')
			) {
				const id = jobNextId++;
				jobRows.set(id, {
					id,
					queue: values[0],
					payload: typeof values[1] === 'string' ? JSON.parse(values[1]) : values[1],
					request_id: values[2] ?? null,
					claimed_at: null,
					claimed_until: null,
					attempts: 0,
					created_at: new Date()
				});
				return { rows: [{ id }], rowCount: 1 };
			}

			// Job claim: WITH claimed AS (SELECT svti_jobs_id FROM svti_jobs WHERE queue=$1 AND (claimed_at IS NULL OR claimed_until < now()) ...) UPDATE ... RETURNING ...
			if (sql.includes('WITH claimed') && sql.includes('claimed_at IS NULL OR claimed_until')) {
				const queue = values[0];
				const limit = Number(values[1]);
				const visibilityMs = Number(values[2]);
				const now = Date.now();
				const candidates = [];
				for (const row of jobRows.values()) {
					if (row.queue !== queue) continue;
					if (row.claimed_at === null || (row.claimed_until !== null && row.claimed_until < now)) {
						candidates.push(row);
					}
				}
				candidates.sort((a, b) => a.id - b.id);
				const claimed = candidates.slice(0, limit);
				const out = [];
				for (const row of claimed) {
					row.claimed_at = now;
					row.claimed_until = now + visibilityMs;
					row.attempts += 1;
					out.push({
						id: row.id,
						queue: row.queue,
						payload: row.payload,
						request_id: row.request_id ?? null,
						attempts: row.attempts,
						created_at: row.created_at
					});
				}
				return { rows: out, rowCount: out.length };
			}

			// Job complete: DELETE FROM svti_jobs WHERE svti_jobs_id = ANY($1::bigint[]) RETURNING queue
			if (
				sql.startsWith('DELETE FROM') &&
				sql.includes('svti_jobs_id = ANY($1::bigint[])') &&
				sql.includes('RETURNING queue')
			) {
				const ids = values[0];
				const out = [];
				for (const id of ids) {
					const row = jobRows.get(Number(id));
					if (row) {
						out.push({ queue: row.queue });
						jobRows.delete(Number(id));
					}
				}
				return { rows: out, rowCount: out.length };
			}

			// Job fail: UPDATE svti_jobs SET claimed_at = NULL, claimed_until = NULL WHERE svti_jobs_id = ANY($1::bigint[]) RETURNING queue
			if (
				sql.startsWith('UPDATE') &&
				sql.includes('claimed_at = NULL') &&
				sql.includes('claimed_until = NULL') &&
				sql.includes('RETURNING queue')
			) {
				const ids = values[0];
				const out = [];
				for (const id of ids) {
					const row = jobRows.get(Number(id));
					if (row) {
						row.claimed_at = null;
						row.claimed_until = null;
						out.push({ queue: row.queue });
					}
				}
				return { rows: out, rowCount: out.length };
			}

			// Job extend: UPDATE svti_jobs SET claimed_until = claimed_until + ... WHERE svti_jobs_id = ANY($1::bigint[]) AND claimed_at IS NOT NULL
			if (
				sql.startsWith('UPDATE') &&
				sql.includes('claimed_until = claimed_until +') &&
				sql.includes('claimed_at IS NOT NULL')
			) {
				const ids = values[0];
				const additionalMs = Number(values[1]);
				let count = 0;
				for (const id of ids) {
					const row = jobRows.get(Number(id));
					if (row && row.claimed_at !== null) {
						row.claimed_until = (row.claimed_until ?? Date.now()) + additionalMs;
						count++;
					}
				}
				return { rows: [], rowCount: count };
			}

			// Job pending count for one queue
			if (
				sql.includes('pending_count') &&
				sql.includes('queue = $1') &&
				sql.includes('claimed_at IS NULL')
			) {
				const queue = values[0];
				let count = 0;
				for (const row of jobRows.values()) {
					if (row.queue === queue && row.claimed_at === null) count++;
				}
				return { rows: [{ pending_count: count }], rowCount: 1 };
			}

			// Job pending count across all queues
			if (sql.includes('pending_count') && sql.includes('claimed_at IS NULL')) {
				let count = 0;
				for (const row of jobRows.values()) {
					if (row.claimed_at === null) count++;
				}
				return { rows: [{ pending_count: count }], rowCount: 1 };
			}

			// Job clear scoped to a queue
			if (sql.startsWith('DELETE FROM svti_jobs') && sql.includes('WHERE queue = $1')) {
				const queue = values[0];
				let removed = 0;
				for (const [id, row] of jobRows) {
					if (row.queue === queue) {
						jobRows.delete(id);
						removed++;
					}
				}
				return { rows: [], rowCount: removed };
			}

			// Job clear all
			if (sql.startsWith('DELETE FROM svti_jobs') && !sql.includes('WHERE')) {
				const before = jobRows.size;
				jobRows.clear();
				return { rows: [], rowCount: before };
			}

			// CTE publish: atomic seq increment + insert in one query
			if (sql.includes('WITH new_seq') && sql.includes('ON CONFLICT') && sql.includes('RETURNING seq')) {
				const topic = values[0];
				const current = seqCounters.get(topic) || 0;
				const next = current + 1;
				seqCounters.set(topic, next);
				const row = {
					svti_replay_id: nextId++,
					topic,
					seq: next,
					event: values[1],
					data: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					created_at: new Date()
				};
				rows.push(row);
				return { rows: [{ seq: String(next) }], rowCount: 1 };
			}

			// INSERT INTO *_seq (atomic sequence generation)
			if (sql.includes('ON CONFLICT') && sql.includes('RETURNING seq')) {
				const topic = values[0];
				const current = seqCounters.get(topic) || 0;
				const next = current + 1;
				seqCounters.set(topic, next);
				return { rows: [{ seq: String(next) }], rowCount: 1 };
			}

			// INSERT
			if (sql.startsWith('INSERT INTO')) {
				const row = {
					svti_replay_id: nextId++,
					topic: values[0],
					seq: parseInt(values[1], 10),
					event: values[2],
					data: typeof values[3] === 'string' ? JSON.parse(values[3]) : values[3],
					created_at: new Date()
				};
				rows.push(row);
				return { rows: [row], rowCount: 1 };
			}

			// SELECT COALESCE(seq, 0) FROM _seq table
			if (sql.includes('current_seq') && sql.includes('_seq')) {
				const topic = values[0];
				const seq = seqCounters.get(topic);
				if (seq !== undefined) {
					return { rows: [{ current_seq: String(seq) }], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// SELECT COALESCE(MAX(seq)
			if (sql.includes('MAX(seq)')) {
				const topic = values[0];
				const topicRows = rows.filter((r) => r.topic === topic);
				const maxSeq = topicRows.reduce((max, r) => Math.max(max, r.seq), 0);
				return { rows: [{ max_seq: String(maxSeq) }], rowCount: 1 };
			}

			// SELECT COUNT
			if (sql.includes('COUNT(*)')) {
				const topic = values[0];
				const message_count = rows.filter((r) => r.topic === topic).length;
				return { rows: [{ message_count }], rowCount: 1 };
			}

			// SELECT seq FROM ... WHERE topic = $1 AND seq >= $2 ORDER BY seq ASC LIMIT 1 (gap probe)
			if (sql.startsWith('SELECT seq FROM') && sql.includes('seq >=') && sql.includes('LIMIT 1')) {
				const topic = values[0];
				const target = parseInt(values[1], 10);
				const matches = rows
					.filter((r) => r.topic === topic && r.seq >= target)
					.sort((a, b) => a.seq - b.seq);
				if (matches.length > 0) {
					return { rows: [{ seq: String(matches[0].seq) }], rowCount: 1 };
				}
				return { rows: [], rowCount: 0 };
			}

			// SELECT seq, topic, event, data ... WHERE topic = $1 AND seq > $2
			if (sql.includes('SELECT seq, topic, event, data')) {
				const topic = values[0];
				const since = parseInt(values[1], 10);
				const result = rows
					.filter((r) => r.topic === topic && r.seq > since)
					.sort((a, b) => a.seq - b.seq)
					.map((r) => ({
						seq: String(r.seq),
						topic: r.topic,
						event: r.event,
						data: r.data
					}));
				return { rows: result, rowCount: result.length };
			}

			// Seq-based inline trim: DELETE WHERE topic = $1 AND seq <= $2
			if (sql.includes('DELETE FROM') && sql.includes('seq <=') && !sql.includes('OFFSET') && !sql.includes('cutoff_seq')) {
				const topic = values[0];
				const cutoffSeq = parseInt(values[1], 10);
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic || r.seq > cutoffSeq);
				return { rows: [], rowCount: before - rows.length };
			}

			// Range-based inline trim: DELETE WHERE topic = $1 AND seq <= (SELECT ... OFFSET $2 LIMIT 1)
			if (sql.includes('DELETE FROM') && sql.includes('seq <=') && sql.includes('OFFSET')) {
				const topic = values[0];
				const offset = parseInt(values[1], 10);
				const topicRows = rows
					.filter((r) => r.topic === topic)
					.sort((a, b) => b.seq - a.seq);
				if (offset < topicRows.length) {
					const cutoffSeq = topicRows[offset].seq;
					const before = rows.length;
					rows = rows.filter((r) => r.topic !== topic || r.seq > cutoffSeq);
					return { rows: [], rowCount: before - rows.length };
				}
				return { rows: [], rowCount: 0 };
			}

			// Range-based periodic cleanup: DELETE using OFFSET-based cutoff per topic
			if (sql.includes('DELETE FROM') && sql.includes('cutoff_seq') && sql.includes('DISTINCT topic')) {
				const offset = parseInt(values[0], 10);
				const topics = [...new Set(rows.map((r) => r.topic))];
				let totalRemoved = 0;
				for (const topic of topics) {
					const topicRows = rows
						.filter((r) => r.topic === topic)
						.sort((a, b) => b.seq - a.seq);
					if (offset < topicRows.length) {
						const cutoffSeq = topicRows[offset].seq;
						const before = rows.length;
						rows = rows.filter((r) => r.topic !== topic || r.seq > cutoffSeq);
						totalRemoved += before - rows.length;
					}
				}
				return { rows: [], rowCount: totalRemoved };
			}

			// DELETE FROM table WHERE topic = $1 AND svti_replay_id NOT IN (... LIMIT $2)
			if (sql.includes('DELETE FROM') && sql.includes('NOT IN') && sql.includes('LIMIT')) {
				const topic = values[0];
				const limit = parseInt(values[1], 10);
				const topicRows = rows
					.filter((r) => r.topic === topic)
					.sort((a, b) => b.seq - a.seq);
				const keepIds = new Set(topicRows.slice(0, limit).map((r) => r.svti_replay_id));
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic || keepIds.has(r.svti_replay_id));
				return { rows: [], rowCount: before - rows.length };
			}

			// DELETE FROM *_seq WHERE topic = $1
			if (sql.includes('DELETE FROM') && sql.includes('_seq') && sql.includes('WHERE topic')) {
				const topic = values[0];
				seqCounters.delete(topic);
				return { rows: [], rowCount: 1 };
			}

			// DELETE FROM table WHERE topic = $1
			if (sql.includes('DELETE FROM') && sql.includes('WHERE topic')) {
				const topic = values[0];
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic);
				return { rows: [], rowCount: before - rows.length };
			}

			// DELETE FROM *_seq (clear all sequences)
			if (sql.includes('DELETE FROM') && sql.includes('_seq')) {
				seqCounters.clear();
				return { rows: [], rowCount: 0 };
			}

			// DELETE FROM table (clear all)
			if (sql.startsWith('DELETE FROM')) {
				const before = rows.length;
				rows = [];
				return { rows: [], rowCount: before };
			}

			return { rows: [], rowCount: 0 };
		},

		async end() {},

		// Test helpers
		_getRows() { return rows; },
		_getSeqCounters() { return seqCounters; },
		_getIdemRows() { return idemRows; },
		_getTaskRows() { return taskRows; },
		_getJobRows() { return jobRows; },
		_reset() { rows = []; nextId = 1; tableCreated = false; seqCounters.clear(); idemRows.clear(); taskRows.clear(); jobRows.clear(); jobNextId = 1; }
	};
	return client;
}
