import { wallEpoch, randomUuid, microtask, setTimer } from '../shared/runtime.js';

/**
 * In-memory mock that implements the PgClient interface.
 * Parses SQL enough to simulate:
 *   - the svti_replay table + svti_replay_seq counter
 *   - the svti_idempotency key/value/expires_at table
 *   - the svti_tasks task-runner state machine
 *   - the svti_jobs queue (FOR UPDATE SKIP LOCKED claim)
 *   - pg_try_advisory_lock / pg_advisory_unlock session locks
 *   - LISTEN / NOTIFY delivery via dedicated clients
 *
 * SQL is dispatched by shape, not by table name, so custom table names
 * passed via options work as long as the column shape is recognisable.
 * Markers:
 *   - replay:        `topic`, `seq`
 *   - idempotency:   `expires_at` (without prefix), `WHERE svti_idempotency_key = $1`
 *   - tasks:         `fence_expires_at`, `svti_tasks_id`, `gen_random_uuid()`
 *
 * The clock and ids follow the injectable runtime seam (now / randomUuid),
 * so a seeded harness that swaps in a virtual clock and a seeded RNG makes
 * every TTL window, fence id, and created_at timestamp the double produces
 * reproducible. Row locks (advisory + SKIP LOCKED) and NOTIFY/LISTEN
 * delivery let the double surface concurrency behaviour - double-claim
 * races and cross-connection lock contention - that the previous naive
 * slice-based stub silently hid.
 */
export function mockPgClient(options = {}) {
	// Optional seeded fault engine for cross-instance LISTEN/NOTIFY delivery (a
	// simulation harness passes one). When set, each notification delivery to a
	// listener is drawn independently and deferred on the seam timer
	// (drop / delay / reorder / duplicate / corrupt). Absent (the default, and
	// every native / integration caller) => delivery stays the ordered microtask
	// flush, so existing behaviour is unchanged. Same `plan(payload)` shape as the
	// adapter's createFaultEngine.
	const notifyFaultEngine = (options.faultEngine && typeof options.faultEngine.plan === 'function')
		? options.faultEngine
		: null;
	/** @type {Array<{svti_replay_id: number, topic: string, seq: number, event: string, data: any, created_at: Date}>} */
	let rows = [];
	let nextId = 1;
	let tableCreated = false;

	/**
	 * table -> Set(column names). Populated from CREATE TABLE column lists and
	 * ALTER TABLE ... ADD COLUMN so the double can answer the
	 * information_schema.columns lookup that pg-migrate's drift guard now runs
	 * on every ensureTable (real Postgres answers it; the mock must too, or the
	 * guard reports every table as missing all its columns).
	 * @type {Map<string, Set<string>>}
	 */
	const tableColumns = new Map();

	/** Extract the table name and top-level column names from a CREATE TABLE DDL. */
	function parseCreateTable(ddl) {
		const open = ddl.indexOf('(');
		const close = ddl.lastIndexOf(')');
		const nameMatch = ddl.slice(0, open < 0 ? undefined : open)
			.match(/CREATE TABLE(?:\s+IF NOT EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
		const table = nameMatch ? nameMatch[1] : null;
		const columns = [];
		if (open >= 0 && close > open) {
			const body = ddl.slice(open + 1, close);
			let depth = 0, cur = '';
			const parts = [];
			for (const ch of body) {
				if (ch === '(') depth++;
				else if (ch === ')') depth--;
				if (ch === ',' && depth === 0) { parts.push(cur); cur = ''; } else cur += ch;
			}
			if (cur.trim()) parts.push(cur);
			const constraintKw = /^(PRIMARY|UNIQUE|CONSTRAINT|CHECK|FOREIGN|EXCLUDE)\b/i;
			for (const part of parts) {
				const t = part.trim();
				if (!t || constraintKw.test(t)) continue;
				const im = t.match(/^([a-zA-Z_][a-zA-Z0-9_]*)/);
				if (im) columns.push(im[1]);
			}
		}
		return { table, columns };
	}

	function recordColumns(table, columns) {
		if (!table) return;
		let set = tableColumns.get(table);
		if (!set) { set = new Set(); tableColumns.set(table, set); }
		for (const c of columns) set.add(c);
	}

	/** @type {Map<string, number>} topic -> seq */
	const seqCounters = new Map();

	/**
	 * topic -> epoch. Lives on the same logical row as seqCounters (the
	 * svti_replay_seq row), mirroring the durable `epoch` column. A
	 * never-published topic has no entry and reads 0; the first publish of a
	 * fresh seq space and every clearTopic bump it by 1, and the row survives
	 * data cleanup so a reaped-then-republished topic keeps climbing.
	 * @type {Map<string, number>}
	 */
	const epochCounters = new Map();

	/** @type {Map<string, {status: string, result: any, expires_at: number}>} */
	const idemRows = new Map();

	/** @type {Map<string, {id: string, name: string, input: any, idempotency_key: string|null, status: string, result: any, error: any, fence: string, fence_expires_at: number, attempts: number, created_at: number, updated_at: number, _ins: number}>} */
	const taskRows = new Map();
	// Monotonic insertion counter. A virtual clock can stamp several rows with
	// the same created_at, so created_at alone is not a total order; `_ins`
	// breaks ties deterministically in insertion order (the durable analogue is
	// the BIGSERIAL primary key Postgres assigns each INSERT).
	let taskInsSeq = 1;

	/** @type {Map<number, {id: number, queue: string, payload: any, claimed_at: number|null, claimed_until: number|null, attempts: number, created_at: Date}>} */
	const jobRows = new Map();
	let jobNextId = 1;

	// ----- Advisory locks (session-scoped, like Postgres pg_advisory_lock).
	//
	// Keyed by lock id; the value is the holder's connection identity so a
	// SECOND connection that asks for an already-held lock is refused. Locks
	// taken on the shared client (connId 0) and on dedicated clients
	// (createClient(), connId >= 1) live in the same map, so contention is
	// queryable across claimers exactly as it is in real Postgres. A
	// connection's end() releases every lock it still holds.
	/** @type {Map<number, number>} lockId -> holderConnId */
	const advisoryLocks = new Map();
	let nextConnId = 1;

	// ----- Row-lock model for FOR UPDATE SKIP LOCKED.
	//
	// A claim CTE that locks a row marks it here under the claiming
	// connection's identity. A concurrent claimer SKIPS any row another
	// connection currently holds (SKIP LOCKED), so two claimers never return
	// the same row. The lock is released when the holding connection commits,
	// rolls back, or releases - the shared autocommit client releases its
	// locks at the end of each query() call (every statement is its own
	// transaction), while a pinned pool connection holds them across the
	// BEGIN..COMMIT window. Keyed `"<kind>:<rowKey>"` so task rows and job
	// rows never collide.
	/** @type {Map<string, number>} lockKey -> holderConnId */
	const rowLocks = new Map();

	// ----- LISTEN / NOTIFY.
	//
	// Each LISTEN registers the issuing dedicated client on a channel. NOTIFY
	// enqueues the payload to every currently-registered listener and flushes
	// it on a microtask, so delivery is asynchronous (as the pg driver's
	// 'notification' event is) but ordered FIFO per channel. The shape mirrors
	// notify.js's dedicated-client bridge: createClient() returns an object
	// with on('notification', fn) / query('LISTEN ...') / connect() / end().
	/** @type {Map<string, Set<object>>} channel -> set of dedicated clients listening */
	const channelListeners = new Map();

	function deliverNotification(channel, payload) {
		const listeners = channelListeners.get(channel);
		if (!listeners || listeners.size === 0) return;
		// Snapshot so a listener that UNLISTENs during delivery does not
		// mutate the set we are iterating. Per-channel FIFO is preserved
		// because each NOTIFY schedules its own flush in call order.
		const snapshot = [...listeners];
		if (notifyFaultEngine) {
			// Fault-gated delivery: each listener's notification is drawn
			// independently and deferred on the seam timer (refed so a delayed /
			// reordered notify still lands before the run quiesces). A byte-flipped
			// payload reaches the listener as-is and then either fails the notify
			// bridge's JSON.parse, is dropped by its envelope/validator gate, or
			// parses to a valid-but-mutated envelope that is delivered.
			for (const conn of snapshot) {
				const plan = notifyFaultEngine.plan(payload);
				for (const d of plan) {
					const p = d.payload;
					setTimer(() => {
						if (!channelListeners.get(channel)?.has(conn)) return;
						conn._emit('notification', { channel, payload: p, processId: 0 });
					}, d.delayMs);
				}
			}
			return;
		}
		// Schedule via the runtime seam so a virtual timer wheel can drive
		// delivery deterministically; under the native default this is a plain
		// microtask, matching the pg driver's async 'notification' dispatch.
		microtask(() => {
			for (const conn of snapshot) {
				if (!channelListeners.get(channel)?.has(conn)) continue;
				conn._emit('notification', { channel, payload, processId: 0 });
			}
		});
	}

	// The double's clock. Reads the exact wall-clock epoch through the runtime
	// seam, so it has the same millisecond precision Postgres `now()` /
	// CURRENT_TIMESTAMP carry (the 1Hz-cached `now()` helper would quantize TTL
	// windows and created_at ordering to whole seconds), while a seeded harness
	// that installs a virtual clock via setRuntimeEnv still drives every TTL,
	// fence expiry, and timestamp this double produces.
	function now() {
		return wallEpoch();
	}

	function idemNow() {
		return now();
	}

	/**
	 * Run the SQL dispatch for one statement. `connId` is the identity of the
	 * connection issuing the statement (0 = the shared autocommit client,
	 * >= 1 = a pinned pool connection or a dedicated LISTEN client). `inTx`
	 * is true while a pinned connection is between BEGIN and COMMIT/ROLLBACK,
	 * which is what keeps its row locks held across the transaction window.
	 */
	function runQuery(textOrObj, values, connId, conn, txState) {
		if (typeof textOrObj === 'object' && textOrObj !== null) {
			values = textOrObj.values || [];
			textOrObj = textOrObj.text;
		}
		if (!values) values = [];
		const sql = textOrObj.trim().replace(/\s+/g, ' ');

		// Transaction-control statements. On the shared autocommit client these
		// are no-ops (the mock does not model atomicity; the integration tier
		// asserts that). On a pinned pool connection they open / close the
		// row-lock window: locks a claim CTE takes inside BEGIN..COMMIT stay
		// held until COMMIT or ROLLBACK, so a concurrent claimer skips them.
		if (sql === 'BEGIN') {
			if (txState) txState.inTx = true;
			return { rows: [], rowCount: 0 };
		}
		if (sql === 'COMMIT' || sql === 'ROLLBACK') {
			if (txState) {
				txState.inTx = false;
				releaseRowLocks(connId);
			}
			return { rows: [], rowCount: 0 };
		}

		// LISTEN / UNLISTEN: register or drop this dedicated client on a
		// channel. The bridge issues `LISTEN "channel"` (delimited identifier),
		// so strip the surrounding double-quotes to recover the raw name.
		if (sql.startsWith('LISTEN ')) {
			const channel = unquoteIdent(sql.slice('LISTEN '.length).trim());
			if (!channelListeners.has(channel)) channelListeners.set(channel, new Set());
			if (conn) channelListeners.get(channel).add(conn);
			return { rows: [], rowCount: 0 };
		}
		if (sql.startsWith('UNLISTEN ')) {
			const channel = unquoteIdent(sql.slice('UNLISTEN '.length).trim());
			channelListeners.get(channel)?.delete(conn);
			return { rows: [], rowCount: 0 };
		}

		// NOTIFY channel, 'payload' and the pg_notify(channel, payload)
		// function form. Delivery is enqueued to every current listener.
		if (sql.startsWith('NOTIFY ')) {
			const rest = sql.slice('NOTIFY '.length);
			const comma = rest.indexOf(',');
			const channel = unquoteIdent((comma === -1 ? rest : rest.slice(0, comma)).trim());
			const payload = comma === -1 ? '' : unquoteLiteral(rest.slice(comma + 1).trim());
			deliverNotification(channel, payload);
			return { rows: [], rowCount: 0 };
		}
		if (/^SELECT\s+pg_notify\s*\(/i.test(sql)) {
			const channel = values[0];
			const payload = values[1] ?? '';
			deliverNotification(channel, payload);
			return { rows: [], rowCount: 0 };
		}

		// Advisory locks. pg_try_advisory_lock($1) claims the lock for this
		// connection when free (or already self-held) and returns true; it
		// returns false when another connection holds it. pg_advisory_unlock
		// releases it (true if this connection held it, false otherwise).
		if (sql.includes('pg_try_advisory_lock')) {
			const lockId = values[0];
			const holder = advisoryLocks.get(lockId);
			const acquired = holder === undefined || holder === connId;
			if (acquired) advisoryLocks.set(lockId, connId);
			return { rows: [{ acquired }], rowCount: 1 };
		}
		if (sql.includes('pg_advisory_unlock')) {
			const lockId = values[0];
			const released = advisoryLocks.get(lockId) === connId;
			if (released) advisoryLocks.delete(lockId);
			return { rows: [{ pg_advisory_unlock: released }], rowCount: 1 };
		}

		// information_schema.columns lookup - pg-migrate's drift guard. Return
		// the columns recorded for the queried table so a correctly-migrated
		// table verifies clean. Matched before the generic SELECT dispatch.
		if (sql.includes('information_schema.columns')) {
			const table = values[0];
			const set = tableColumns.get(table);
			const rowsOut = set ? [...set].map((column_name) => ({ column_name })) : [];
			return { rows: rowsOut, rowCount: rowsOut.length };
		}

		// CREATE TABLE
		if (sql.startsWith('CREATE TABLE')) {
			tableCreated = true;
			const parsed = parseCreateTable(sql);
			recordColumns(parsed.table, parsed.columns);
			return { rows: [], rowCount: 0 };
		}

		// CREATE INDEX
		if (sql.startsWith('CREATE INDEX')) {
			return { rows: [], rowCount: 0 };
		}

		// ALTER TABLE - row shapes are dynamic so any added column is accepted
		// by INSERT / SELECT branches; also record `ADD COLUMN` names so the
		// information_schema drift guard sees the migrated shape.
		if (sql.startsWith('ALTER TABLE')) {
			const m = sql.match(/ALTER TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
			const addCols = [...sql.matchAll(/ADD COLUMN(?:\s+IF NOT EXISTS)?\s+([a-zA-Z_][a-zA-Z0-9_]*)/gi)].map((x) => x[1]);
			if (m && addCols.length) recordColumns(m[1], addCols);
			return { rows: [], rowCount: 0 };
		}

		// ----- Idempotency dispatch (matched first; markers: `expires_at`, `WHERE key`)

		// Acquire: INSERT ... ON CONFLICT (svti_idempotency_key) DO UPDATE ... WHERE expires_at < now() RETURNING owner_token.
		// A fresh insert or an expired-row takeover stamps the caller's owner_token
		// (values[4]) and returns it so commit/abort can compare against it.
		if (
			sql.startsWith('INSERT INTO') &&
			sql.includes('ON CONFLICT (svti_idempotency_key)') &&
			sql.includes('expires_at')
		) {
			const key = values[0];
			const acquireTtlSec = Number(values[1]);
			const ownerToken = values[4];
			const expiresAt = idemNow() + acquireTtlSec * 1000;
			const existing = idemRows.get(key);
			if (!existing || existing.expires_at < idemNow()) {
				idemRows.set(key, { status: 'pending', result: null, expires_at: expiresAt, owner_token: ownerToken });
				return { rows: [{ owner_token: ownerToken }], rowCount: 1 };
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
		//   WHERE svti_idempotency_key = $1 AND owner_token = $4.
		// (Idempotency-specific: WHERE svti_idempotency_key = $1.  Task commits match a different branch below.)
		// Token-guarded: a stale owner whose row a successor took over (different
		// owner_token) matches zero rows, so the real store throws lease-lost.
		if (
			sql.startsWith('UPDATE') &&
			sql.includes("status = 'committed'") &&
			sql.includes('WHERE svti_idempotency_key = $1')
		) {
			const key = values[0];
			const result = typeof values[1] === 'string' ? JSON.parse(values[1]) : values[1];
			const ttlSec = Number(values[2]);
			const ownerToken = values[3];
			const row = idemRows.get(key);
			if (row && row.owner_token === ownerToken) {
				row.status = 'committed';
				row.result = result;
				row.expires_at = idemNow() + ttlSec * 1000;
				return { rows: [], rowCount: 1 };
			}
			return { rows: [], rowCount: 0 };
		}

		// Cleanup: DELETE FROM ... WHERE expires_at < now()
		if (sql.startsWith('DELETE FROM') && sql.includes('expires_at < now()')) {
			let removed = 0;
			const cutoff = idemNow();
			for (const [k, v] of idemRows) {
				if (v.expires_at < cutoff) {
					idemRows.delete(k);
					removed++;
				}
			}
			return { rows: [], rowCount: removed };
		}

		// Abort: DELETE FROM ... WHERE svti_idempotency_key = $1 AND owner_token = $2
		// (token-guarded). Matched BEFORE purge because purge's key-only matcher is
		// a prefix of this one; a stale owner (mismatched token) is a no-op.
		if (
			sql.startsWith('DELETE FROM') &&
			sql.includes('WHERE svti_idempotency_key = $1') &&
			sql.includes('owner_token')
		) {
			const key = values[0];
			const ownerToken = values[1];
			const row = idemRows.get(key);
			if (row && row.owner_token === ownerToken) {
				idemRows.delete(key);
				return { rows: [], rowCount: 1 };
			}
			return { rows: [], rowCount: 0 };
		}

		// Purge: DELETE FROM ... WHERE svti_idempotency_key = $1 (key-only, unconditional).
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
			const ts = now();
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
				fence_expires_at: ts + fenceTtlSec * 1000,
				attempts: 1,
				created_at: ts,
				updated_at: ts,
				_ins: taskInsSeq++
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
			const ts = now();
			taskRows.set(values[0], {
				id: values[0],
				name: values[1],
				input: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
				idempotency_key: values[3],
				request_id: values[4] ?? null,
				status: 'pending',
				result: null,
				error: null,
				fence: randomUuid(),
				fence_expires_at: ts,
				attempts: 0,
				created_at: ts,
				updated_at: ts,
				_ins: taskInsSeq++
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
				row.fence_expires_at = now() + ttlSec * 1000;
				row.updated_at = now();
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
				row.updated_at = now();
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
				row.updated_at = now();
				return { rows: [], rowCount: 1 };
			}
			return { rows: [], rowCount: 0 };
		}

		// Task rearm: fence-guarded fence rotation for retries. Only rotates if
		// this worker still holds the row (row.fence === priorFence), so a
		// taken-over worker's retry cannot re-steal the row from its successor.
		if (
			sql.startsWith('UPDATE') &&
			sql.includes('SET fence = $3') &&
			sql.includes('WHERE svti_tasks_id = $1 AND fence = $2')
		) {
			const taskId = values[0];
			const priorFence = values[1];
			const nextFence = values[2];
			const ttlSec = Number(values[3]);
			const attempts = Number(values[4]);
			const row = taskRows.get(taskId);
			if (row && row.fence === priorFence) {
				row.fence = nextFence;
				row.fence_expires_at = now() + ttlSec * 1000;
				row.attempts = attempts;
				row.updated_at = now();
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

		// Task claim-pending: dispatch sweep for enqueued rows. Models FOR
		// UPDATE SKIP LOCKED: rows another connection currently holds are
		// invisible, so two concurrent claimers get disjoint sets. Ordering is
		// deterministic (FIFO by created_at, then row id as a stable tie-break).
		if (sql.includes('WITH claimed') && sql.includes("WHERE status = 'pending'")) {
			const limit = Number(values[0]);
			const ttlSec = Number(values[1]);
			const ts = now();
			const pending = [];
			for (const row of taskRows.values()) {
				if (row.status !== 'pending') continue;
				if (isRowLockedByOther('task', row.id, connId)) continue;
				pending.push(row);
			}
			pending.sort(byCreatedThenIns);
			const claimed = pending.slice(0, limit);
			const out = [];
			for (const row of claimed) {
				lockRow('task', row.id, connId);
				row.status = 'running';
				row.fence = randomUuid();
				row.fence_expires_at = ts + ttlSec * 1000;
				row.attempts += 1;
				row.updated_at = ts;
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
			// Autocommit: the shared client releases the row locks at the end of
			// the statement (each query is its own transaction). A pinned
			// connection holds them until its COMMIT / ROLLBACK.
			if (!txState || !txState.inTx) releaseRowLocks(connId);
			return { rows: out, rowCount: out.length };
		}

		// Task reclaim: stuck-row sweep with CTE + gen_random_uuid(). Same
		// SKIP LOCKED row-lock model as claim-pending above.
		if (sql.includes('WITH claimed') && sql.includes('gen_random_uuid()')) {
			const limit = Number(values[0]);
			const ttlSec = Number(values[1]);
			const ts = now();
			const stuck = [];
			for (const row of taskRows.values()) {
				if (!(row.status === 'running' && row.fence_expires_at < ts)) continue;
				if (isRowLockedByOther('task', row.id, connId)) continue;
				stuck.push(row);
			}
			stuck.sort((a, b) => a.fence_expires_at - b.fence_expires_at || a._ins - b._ins);
			const claimed = stuck.slice(0, limit);
			const out = [];
			for (const row of claimed) {
				lockRow('task', row.id, connId);
				row.fence = randomUuid();
				row.fence_expires_at = ts + ttlSec * 1000;
				row.attempts += 1;
				row.updated_at = ts;
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
			if (!txState || !txState.inTx) releaseRowLocks(connId);
			return { rows: out, rowCount: out.length };
		}

		// Task list: SELECT svti_tasks_id AS id, name, input, status, ... ORDER BY created_at DESC
		if (
			sql.startsWith('SELECT svti_tasks_id AS id') &&
			sql.includes('ORDER BY created_at DESC') &&
			sql.includes('LIMIT')
		) {
			let listed = [...taskRows.values()];
			let valueIdx = 0;
			if (sql.includes('name = $')) {
				const filterName = values[valueIdx++];
				listed = listed.filter((r) => r.name === filterName);
			}
			if (sql.includes('status = $')) {
				const filterStatus = values[valueIdx++];
				listed = listed.filter((r) => r.status === filterStatus);
			}
			// Newest first; insertion order breaks created_at ties so rows
			// stamped with the same virtual-clock instant stay deterministic.
			listed.sort((a, b) => b.created_at - a.created_at || b._ins - a._ins);
			const limit = Number(values[valueIdx++]);
			const offset = Number(values[valueIdx++]);
			const sliced = listed.slice(offset, offset + limit);
			const out = sliced.map((r) => ({
				id: r.id,
				name: r.name,
				input: r.input,
				status: r.status,
				result: r.result,
				error: r.error,
				attempts: r.attempts,
				request_id: r.request_id ?? null,
				created_at: new Date(r.created_at), // determinism-allow: r.created_at is a seam-sourced (now()) epoch ms
				updated_at: new Date(r.updated_at), // determinism-allow: r.updated_at is a seam-sourced (now()) epoch ms
				fence_expires_at: new Date(r.fence_expires_at) // determinism-allow: derived from a seam-sourced epoch ms
			}));
			return { rows: out, rowCount: out.length };
		}

		// Task counts: SELECT status, COUNT(*)::int AS n ... GROUP BY status
		if (
			sql.startsWith('SELECT status, COUNT(*)::int AS n') &&
			sql.includes('GROUP BY status')
		) {
			let counted = [...taskRows.values()];
			if (sql.includes('WHERE name = $1')) {
				counted = counted.filter((r) => r.name === values[0]);
			}
			const counts = {};
			for (const r of counted) counts[r.status] = (counts[r.status] || 0) + 1;
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
			row.fence_expires_at = now() - 1000;
			row.updated_at = now();
			return { rows: [{ fence: previousFence }], rowCount: 1 };
		}

		// Task cleanup: delete terminal rows older than rowTtl
		if (
			sql.startsWith('DELETE FROM') &&
			sql.includes("status IN ('committed', 'failed')") &&
			sql.includes('updated_at <')
		) {
			const ttlSec = Number(values[0]);
			const cutoff = now() - ttlSec * 1000;
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
				created_at: new Date(now()) // determinism-allow: created_at is the seam clock (now()) as a Date
			});
			return { rows: [{ id }], rowCount: 1 };
		}

		// Job claim: WITH claimed AS (SELECT svti_jobs_id FROM svti_jobs WHERE queue=$1 AND (claimed_at IS NULL OR claimed_until < now()) ...) UPDATE ... RETURNING ...
		// Models FOR UPDATE SKIP LOCKED: a row another connection currently
		// holds is skipped, so two concurrent claimers get disjoint rows.
		if (sql.includes('WITH claimed') && sql.includes('claimed_at IS NULL OR claimed_until')) {
			const queue = values[0];
			const limit = Number(values[1]);
			const visibilityMs = Number(values[2]);
			const ts = now();
			const candidates = [];
			for (const row of jobRows.values()) {
				if (row.queue !== queue) continue;
				if (!(row.claimed_at === null || (row.claimed_until !== null && row.claimed_until < ts))) continue;
				if (isRowLockedByOther('job', row.id, connId)) continue;
				candidates.push(row);
			}
			candidates.sort((a, b) => a.id - b.id);
			const claimed = candidates.slice(0, limit);
			const out = [];
			for (const row of claimed) {
				lockRow('job', row.id, connId);
				row.claimed_at = ts;
				row.claimed_until = ts + visibilityMs;
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
			if (!txState || !txState.inTx) releaseRowLocks(connId);
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
					row.claimed_until = (row.claimed_until ?? now()) + additionalMs;
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

		// Batched epoch read (resume hook): SELECT topic, COALESCE(epoch, 0)
		// FROM *_seq WHERE topic = ANY($1). Topics with no seq row are simply
		// absent from the result (the store reads them as the baseline 0).
		if (sql.includes('AS epoch') && sql.includes('_seq') && sql.includes('WHERE topic = ANY($1)')) {
			const rows = [];
			for (const topic of values[0] || []) {
				if (seqCounters.has(topic) || epochCounters.has(topic)) {
					rows.push({ topic, epoch: String(epochCounters.get(topic) || 0) });
				}
			}
			return { rows, rowCount: rows.length };
		}

		// Epoch read-through: SELECT COALESCE(epoch, 0) FROM *_seq WHERE topic = $1.
		// A topic with no seq row reads the baseline 0.
		if (sql.includes('AS epoch') && sql.includes('_seq') && sql.includes('WHERE topic = $1')) {
			const topic = values[0];
			if (seqCounters.has(topic) || epochCounters.has(topic)) {
				return { rows: [{ epoch: String(epochCounters.get(topic) || 0) }], rowCount: 1 };
			}
			return { rows: [], rowCount: 0 };
		}

		// clearTopic upsert: keep the seq-table row, reset seq to 0, bump epoch.
		// INSERT ... ON CONFLICT DO UPDATE SET seq = 0, epoch = epoch + 1 RETURNING epoch.
		if (
			sql.includes('INSERT INTO') &&
			sql.includes('_seq') &&
			sql.includes('ON CONFLICT') &&
			sql.includes('epoch + 1') &&
			sql.includes('RETURNING epoch')
		) {
			const topic = values[0];
			const next = (epochCounters.get(topic) || 0) + 1;
			epochCounters.set(topic, next);
			// Keep the row with seq reset to 0 so the next publish's UPDATE
			// branch issues seq = 1 (and carries the climbed epoch forward).
			seqCounters.set(topic, 0);
			return { rows: [{ epoch: String(next) }], rowCount: 1 };
		}

		// clear (global): bump every topic's epoch and zero its seq, keeping
		// rows. UPDATE *_seq SET seq = 0, epoch = epoch + 1.
		if (sql.startsWith('UPDATE') && sql.includes('_seq') && sql.includes('epoch + 1')) {
			for (const topic of seqCounters.keys()) {
				seqCounters.set(topic, 0);
				epochCounters.set(topic, (epochCounters.get(topic) || 0) + 1);
			}
			return { rows: [], rowCount: seqCounters.size };
		}

		// Batch CTE publish: UNNEST arrays -> per-topic seq bump + multi-row
		// insert in one statement. Mirrors the single-publish branches exactly:
		// a topic with no seq row seeds seq=n, epoch=1 (INSERT branch); an
		// existing row takes seq+=n and carries its epoch forward (UPDATE
		// branch). Per-topic seqs are assigned contiguously in caller order.
		// Returns one row per distinct topic: { topic, new_high, epoch }.
		if (sql.includes('UNNEST') && sql.includes('ON CONFLICT') && sql.includes('new_high')) {
			const [batchTopics, batchEvents, batchDatas, batchUserIds] = values;
			const perTopic = new Map();
			for (let i = 0; i < batchTopics.length; i++) {
				const topic = batchTopics[i];
				if (!perTopic.has(topic)) perTopic.set(topic, []);
				perTopic.get(topic).push(i);
			}
			const out = [];
			for (const [topic, indexes] of perTopic) {
				const hadRow = seqCounters.has(topic);
				const base = seqCounters.get(topic) || 0;
				const high = base + indexes.length;
				seqCounters.set(topic, high);
				let epoch;
				if (!hadRow) {
					epoch = 1;
					epochCounters.set(topic, epoch);
				} else {
					epoch = epochCounters.get(topic) || 0;
				}
				let seq = base;
				for (const i of indexes) {
					seq += 1;
					rows.push({
						svti_replay_id: nextId++,
						topic,
						seq,
						event: batchEvents[i],
						data: typeof batchDatas[i] === 'string' ? JSON.parse(batchDatas[i]) : batchDatas[i],
						user_id: batchUserIds?.[i] ?? null,
						created_at: new Date(now()) // determinism-allow: created_at is the seam clock (now()) as a Date
					});
				}
				out.push({ topic, new_high: String(high), epoch: String(epoch) });
			}
			return { rows: out, rowCount: out.length };
		}

		// CTE publish: atomic seq increment + insert in one query.
		// The reset edge is the INSERT branch (a topic with no seq row, i.e.
		// brand-new or fully cleared): seq starts at 1 and epoch is seeded to
		// 1. An existing row takes the UPDATE branch (seq + 1) and carries its
		// epoch forward unchanged, so steady-state publishes never move the
		// epoch.
		if (sql.includes('WITH new_seq') && sql.includes('ON CONFLICT') && sql.includes('RETURNING seq')) {
			const topic = values[0];
			const hadRow = seqCounters.has(topic);
			const current = seqCounters.get(topic) || 0;
			const next = current + 1;
			seqCounters.set(topic, next);
			let epoch;
			if (!hadRow) {
				epoch = 1;
				epochCounters.set(topic, epoch);
			} else {
				epoch = epochCounters.get(topic) || 0;
			}
			const row = {
				svti_replay_id: nextId++,
				topic,
				seq: next,
				event: values[1],
				data: typeof values[2] === 'string' ? JSON.parse(values[2]) : values[2],
				user_id: values[3] ?? null,
				created_at: new Date(now()) // determinism-allow: created_at is the seam clock (now()) as a Date
			};
			rows.push(row);
			return { rows: [{ seq: String(next), epoch: String(epoch) }], rowCount: 1 };
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
				created_at: new Date(now()) // determinism-allow: created_at is the seam clock (now()) as a Date
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

		// Right-to-erasure purge: DELETE FROM table WHERE user_id = $1
		if (sql.startsWith('DELETE FROM') && sql.includes('WHERE user_id = $1')) {
			const userId = values[0];
			let count = 0;
			for (let i = rows.length - 1; i >= 0; i--) {
				if (rows[i].user_id === userId) {
					rows.splice(i, 1);
					count++;
				}
			}
			return { rows: [], rowCount: count };
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
	}

	// ----- Row-lock helpers (SKIP LOCKED model). ------------------------------

	function lockKey(kind, rowKey) {
		return kind + ':' + rowKey;
	}

	function isRowLockedByOther(kind, rowKey, connId) {
		const holder = rowLocks.get(lockKey(kind, rowKey));
		return holder !== undefined && holder !== connId;
	}

	function lockRow(kind, rowKey, connId) {
		rowLocks.set(lockKey(kind, rowKey), connId);
	}

	function releaseRowLocks(connId) {
		for (const [k, holder] of rowLocks) {
			if (holder === connId) rowLocks.delete(k);
		}
	}

	// FIFO order for pending-task claims: oldest created_at first, insertion
	// order as a stable tie-break so claims are deterministic under a virtual
	// clock that can stamp several rows with the same now() value.
	function byCreatedThenIns(a, b) {
		return a.created_at - b.created_at || a._ins - b._ins;
	}

	// Strip a delimited SQL identifier ("name" -> name, ""x"" -> "x"). Bare
	// identifiers pass through unchanged.
	function unquoteIdent(token) {
		if (token.length >= 2 && token[0] === '"' && token[token.length - 1] === '"') {
			return token.slice(1, -1).replace(/""/g, '"');
		}
		return token;
	}

	// Strip a single-quoted SQL string literal ('x' -> x, ''y'' -> 'y').
	function unquoteLiteral(token) {
		if (token.length >= 2 && token[0] === "'" && token[token.length - 1] === "'") {
			return token.slice(1, -1).replace(/''/g, "'");
		}
		return token;
	}

	// Build a dedicated client (the LISTEN/NOTIFY connection notify.js opens
	// via createClient()). It carries its own connection identity so its
	// advisory locks and row locks are distinct from the pool's, and an
	// onNotification listener registry mirroring the pg.Client event surface.
	function makeDedicatedClient() {
		const connId = nextConnId++;
		const listeners = new Map();
		let open = false;
		const dedicated = {
			_connId: connId,
			_emit(event, arg) {
				const fns = listeners.get(event);
				if (!fns) return;
				for (const fn of [...fns]) fn(arg);
			},
			on(event, fn) {
				if (!listeners.has(event)) listeners.set(event, new Set());
				listeners.get(event).add(fn);
				return dedicated;
			},
			removeListener(event, fn) {
				listeners.get(event)?.delete(fn);
				return dedicated;
			},
			async connect() {
				open = true;
			},
			query(textOrObj, vals) {
				return Promise.resolve(runQuery(textOrObj, vals, connId, dedicated, null));
			},
			async end() {
				open = false;
				// Session end releases this connection's advisory + row locks
				// and drops it from every channel it was listening on, matching
				// Postgres session-scoped semantics.
				releaseConn(connId);
				for (const set of channelListeners.values()) set.delete(dedicated);
				listeners.clear();
			},
			_isOpen() { return open; }
		};
		return dedicated;
	}

	function releaseConn(connId) {
		for (const [lockId, holder] of advisoryLocks) {
			if (holder === connId) advisoryLocks.delete(lockId);
		}
		releaseRowLocks(connId);
	}

	const client = {
		// `pool.connect()` returns a pinned-connection wrapper. The pinned
		// connection has its own connection identity, so row locks a claim CTE
		// takes inside its BEGIN..COMMIT window stay held against concurrent
		// claimers until the transaction ends (release() also drops them, in
		// case a caller forgets to COMMIT/ROLLBACK). The shared autocommit
		// `client.query()` path (connId 0) releases its locks per statement.
		pool: {
			async connect() {
				const connId = nextConnId++;
				const txState = { inTx: false };
				return {
					query: (textOrObj, vals) =>
						Promise.resolve(runQuery(textOrObj, vals, connId, null, txState)),
					release: () => {
						txState.inTx = false;
						releaseRowLocks(connId);
					}
				};
			}
		},

		async query(textOrObj, values) {
			// The shared client is autocommit: connId 0, no transaction window,
			// so each statement's row locks are released the moment it returns.
			return runQuery(textOrObj, values, 0, null, null);
		},

		// Dedicated LISTEN/NOTIFY connection (notify.js opens one via this).
		createClient() {
			return makeDedicatedClient();
		},

		async end() {},

		// Test helpers
		_getRows() { return rows; },
		_getSeqCounters() { return seqCounters; },
		_getIdemRows() { return idemRows; },
		_getTaskRows() { return taskRows; },
		_getJobRows() { return jobRows; },
		_getAdvisoryLocks() { return advisoryLocks; },
		_getRowLocks() { return rowLocks; },
		_getChannelListeners() { return channelListeners; },
		// Fire a NOTIFY as if a foreign session emitted it (the trigger /
		// pg_notify side notify.js does not own). Delivers to every dedicated
		// client currently LISTENing on the channel.
		_notify(channel, payload) { deliverNotification(channel, payload); },
		_reset() {
			rows = [];
			nextId = 1;
			tableCreated = false;
			tableColumns.clear();
			seqCounters.clear();
			epochCounters.clear();
			idemRows.clear();
			taskRows.clear();
			taskInsSeq = 1;
			jobRows.clear();
			jobNextId = 1;
			advisoryLocks.clear();
			rowLocks.clear();
			channelListeners.clear();
			nextConnId = 1;
		}
	};
	return client;
}
