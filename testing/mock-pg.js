import { randomUUID } from 'node:crypto';

/**
 * In-memory mock that implements the PgClient interface.
 * Parses SQL enough to simulate:
 *   - the ws_replay table + ws_replay_seq counter
 *   - the ws_idempotency key/value/expires_at table
 *   - the ws_tasks task-runner state machine
 *
 * SQL is dispatched by shape, not by table name, so custom table names
 * passed via options work as long as the column shape is recognisable.
 * Markers:
 *   - replay:        `topic`, `seq`
 *   - idempotency:   `expires_at` (without prefix), `WHERE key = $1`
 *   - tasks:         `fence_expires_at`, `task_id`, `gen_random_uuid()`
 */
export function mockPgClient() {
	/** @type {Array<{ws_replay_id: number, topic: string, seq: number, event: string, data: any, created_date: Date}>} */
	let rows = [];
	let nextId = 1;
	let tableCreated = false;

	/** @type {Map<string, number>} topic -> seq */
	const seqCounters = new Map();

	/** @type {Map<string, {status: string, result: any, expires_at: number}>} */
	const idemRows = new Map();

	/** @type {Map<string, {task_id: string, name: string, input: any, idempotency_key: string|null, status: string, result: any, error: any, fence: string, fence_expires_at: number, attempts: number, created_at: number, updated_at: number}>} */
	const taskRows = new Map();

	function idemNow() {
		return Date.now();
	}

	return {
		pool: {},

		async query(textOrObj, values) {
			if (typeof textOrObj === 'object' && textOrObj !== null) {
				values = textOrObj.values || [];
				textOrObj = textOrObj.text;
			}
			if (!values) values = [];
			const sql = textOrObj.trim().replace(/\s+/g, ' ');

			// CREATE TABLE
			if (sql.startsWith('CREATE TABLE')) {
				tableCreated = true;
				return { rows: [], rowCount: 0 };
			}

			// CREATE INDEX
			if (sql.startsWith('CREATE INDEX')) {
				return { rows: [], rowCount: 0 };
			}

			// ----- Idempotency dispatch (matched first; markers: `expires_at`, `WHERE key`)

			// Acquire: INSERT ... ON CONFLICT (key) DO UPDATE ... WHERE expires_at < now() RETURNING status
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('ON CONFLICT (key)') &&
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

			// Read: SELECT status, result FROM ... WHERE key = $1 AND expires_at >= now()
			if (
				sql.startsWith('SELECT status, result FROM') &&
				sql.includes('WHERE key = $1') &&
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
			// (Idempotency-specific: WHERE key = $1.  Task commits match a different branch below.)
			if (
				sql.startsWith('UPDATE') &&
				sql.includes("status = 'committed'") &&
				sql.includes('WHERE key = $1')
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

			// Abort / purge: DELETE FROM ... WHERE key = $1
			if (sql.startsWith('DELETE FROM') && sql.includes('WHERE key = $1')) {
				const key = values[0];
				const had = idemRows.delete(key);
				return { rows: [], rowCount: had ? 1 : 0 };
			}

			// Idempotency clear (default table name).  Custom table names
			// fall through to the replay-table catch-all and would clear
			// replay rows instead -- tests should use the default name.
			if (sql.startsWith('DELETE FROM ws_idempotency') && !sql.includes('WHERE')) {
				const before = idemRows.size;
				idemRows.clear();
				return { rows: [], rowCount: before };
			}

			// ----- Task runner dispatch (markers: `fence_expires_at`, `task_id`)

			// Task INSERT (run path): row creation with fresh fence, status='running'
			if (
				sql.startsWith('INSERT INTO') &&
				sql.includes('task_id') &&
				sql.includes('fence_expires_at') &&
				sql.includes('idempotency_key') &&
				!sql.includes('gen_random_uuid()')
			) {
				const fenceTtlSec = Number(values[5]);
				const now = Date.now();
				taskRows.set(values[0], {
					task_id: values[0],
					name: values[1],
					input: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					idempotency_key: values[3],
					status: 'running',
					result: null,
					error: null,
					fence: values[4],
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
				sql.includes('task_id') &&
				sql.includes('fence_expires_at') &&
				sql.includes('idempotency_key') &&
				sql.includes("'pending'") &&
				sql.includes('gen_random_uuid()')
			) {
				const now = Date.now();
				taskRows.set(values[0], {
					task_id: values[0],
					name: values[1],
					input: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					idempotency_key: values[3],
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
				sql.includes('WHERE task_id = $1 AND fence = $2 AND status = \'running\'') &&
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
				sql.includes('WHERE task_id = $1 AND fence = $2 AND status = \'running\'')
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
				sql.includes('WHERE task_id = $1 AND fence = $2 AND status = \'running\'')
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
				sql.includes('WHERE task_id = $1')
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

			// Task read: status, result, error, attempts
			if (
				sql.startsWith('SELECT status, result, error, attempts FROM') &&
				sql.includes('WHERE task_id = $1')
			) {
				const taskId = values[0];
				const row = taskRows.get(taskId);
				if (!row) return { rows: [], rowCount: 0 };
				return {
					rows: [{
						status: row.status,
						result: row.result,
						error: row.error,
						attempts: row.attempts
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
						task_id: row.task_id,
						name: row.name,
						input: row.input,
						idempotency_key: row.idempotency_key,
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
						task_id: row.task_id,
						name: row.name,
						input: row.input,
						idempotency_key: row.idempotency_key,
						fence: row.fence,
						attempts: row.attempts
					});
				}
				return { rows: out, rowCount: out.length };
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
			if (sql.startsWith('DELETE FROM ws_tasks') && !sql.includes('WHERE')) {
				const before = taskRows.size;
				taskRows.clear();
				return { rows: [], rowCount: before };
			}

			// CTE publish: atomic seq increment + insert in one query
			if (sql.includes('WITH new_seq') && sql.includes('ON CONFLICT') && sql.includes('RETURNING seq')) {
				const topic = values[0];
				const current = seqCounters.get(topic) || 0;
				const next = current + 1;
				seqCounters.set(topic, next);
				const row = {
					ws_replay_id: nextId++,
					topic,
					seq: next,
					event: values[1],
					data: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
					created_date: new Date()
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
					ws_replay_id: nextId++,
					topic: values[0],
					seq: parseInt(values[1], 10),
					event: values[2],
					data: typeof values[3] === 'string' ? JSON.parse(values[3]) : values[3],
					created_date: new Date()
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

			// DELETE FROM table WHERE topic = $1 AND ws_replay_id NOT IN (... LIMIT $2)
			if (sql.includes('DELETE FROM') && sql.includes('NOT IN') && sql.includes('LIMIT')) {
				const topic = values[0];
				const limit = parseInt(values[1], 10);
				const topicRows = rows
					.filter((r) => r.topic === topic)
					.sort((a, b) => b.seq - a.seq);
				const keepIds = new Set(topicRows.slice(0, limit).map((r) => r.ws_replay_id));
				const before = rows.length;
				rows = rows.filter((r) => r.topic !== topic || keepIds.has(r.ws_replay_id));
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
		_reset() { rows = []; nextId = 1; tableCreated = false; seqCounters.clear(); idemRows.clear(); taskRows.clear(); }
	};
}
