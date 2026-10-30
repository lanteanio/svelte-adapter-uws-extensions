/**
 * Durable enqueue receipts against a real PostgreSQL server: concurrent
 * producers, statement rollback, restart, lost replies, cleanup retention,
 * erasure coordination and concurrent expiry replacement.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createPgClient, migrate } from '../../../src/postgres/index.js';
import { createTaskRunner, TaskEnqueueConflictError, TaskEnqueueOutcomeUnknownError } from '../../../src/postgres/tasks.js';
import { forgetFixtureMigrations } from '../helpers/postgres-schema.js';

const TABLE = 'once_tasks';
const RECEIPTS = TABLE + '_receipts';
const quiet = { table: TABLE, recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0 };
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const forgetIdentity = (input) => input?.owner ?? null;

let client;
const runners = [];
function runner(options = {}, pg = client) {
	const r = createTaskRunner(pg, { ...quiet, ...options });
	runners.push(r);
	return r;
}
const count = async (table) => (await client.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n;
async function waitFor(check, timeout = 5000) {
	const start = Date.now();
	while (!(await check())) {
		if (Date.now() - start > timeout) throw new Error('condition not reached');
		await wait(20);
	}
}

beforeAll(() => {
	client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
});
beforeEach(async () => {
	while (runners.length) runners.pop().destroy();
	await client.query(`DROP TABLE IF EXISTS ${TABLE}, ${RECEIPTS}`);
	await forgetFixtureMigrations(client, [TABLE]);
	await migrate(client, { store: 'tasks', table: TABLE });
});
afterAll(async () => {
	while (runners.length) runners.pop().destroy();
	await client.query(`DROP TABLE IF EXISTS ${TABLE}, ${RECEIPTS}`);
	await forgetFixtureMigrations(client, [TABLE]);
	await client.end();
});

describe('postgres tasks enqueueOnce (integration)', () => {
	it('lets one of many simultaneous equal producers insert and returns its receipt to all', async () => {
		const producers = [runner(), runner(), runner()];
		const options = { tenantId: 'acme', idempotencyKey: 'order-1', input: { amount: 5 } };
		const receipts = await Promise.all(Array.from({ length: 24 }, (_, i) => producers[i % 3].enqueueOnce('charge', options)));
		expect(new Set(receipts.map((receipt) => receipt.taskId)).size).toBe(1);
		for (const receipt of receipts) expect(receipt).toEqual(receipts[0]);
		expect(await count(TABLE)).toBe(1);
		expect(await count(RECEIPTS)).toBe(1);
		const { rows } = await client.query(`SELECT svti_tasks_id, status, svti_idempotency_key FROM ${TABLE}`);
		expect(rows).toEqual([{ svti_tasks_id: receipts[0].taskId, status: 'pending', svti_idempotency_key: 'order-1' }]);
	});

	it('lets exactly one of simultaneous conflicting producers insert', async () => {
		const r = runner();
		const settled = await Promise.allSettled(Array.from({ length: 12 }, (_, i) => r.enqueueOnce('charge', { tenantId: null, idempotencyKey: 'k', input: { i } })));
		const won = settled.filter((entry) => entry.status === 'fulfilled');
		expect(won).toHaveLength(1);
		for (const entry of settled.filter((item) => item.status === 'rejected')) expect(entry.reason).toBeInstanceOf(TaskEnqueueConflictError);
		expect(await count(TABLE)).toBe(1);
	});

	it('separates tenants, the null scope and task names, and compares exact input bytes', async () => {
		const r = runner();
		const ids = new Set();
		for (const [name, tenantId] of [['a', null], ['a', 'null'], ['a', 't1'], ['b', 't1']]) {
			ids.add((await r.enqueueOnce(name, { tenantId, idempotencyKey: 'same', input: { a: 1, b: 2 } })).taskId);
		}
		expect(ids.size).toBe(4);
		// jsonb would call these equal; the receipt compares the submitted bytes.
		await expect(r.enqueueOnce('a', { tenantId: 't1', idempotencyKey: 'same', input: { b: 2, a: 1 } })).rejects.toBeInstanceOf(TaskEnqueueConflictError);
		const { rows } = await client.query(`SELECT input FROM ${RECEIPTS} WHERE scope = $1`, [JSON.stringify(['t1', 'a', 'same'])]);
		expect(rows).toEqual([{ input: '{"a":1,"b":2}' }]);
		expect(await count(TABLE)).toBe(4);
	});

	it('rolls back the receipt when the task insert fails in the same statement', async () => {
		const r = runner();
		await r.ready();
		await client.query(`ALTER TABLE ${TABLE} ADD CONSTRAINT once_tasks_refuse CHECK (name <> 'refused') NOT VALID`);
		try {
			await expect(r.enqueueOnce('refused', { tenantId: null, idempotencyKey: 'k' })).rejects.toMatchObject({ code: '23514' });
		} finally {
			await client.query(`ALTER TABLE ${TABLE} DROP CONSTRAINT once_tasks_refuse`);
		}
		expect(await count(RECEIPTS)).toBe(0);
		expect(await count(TABLE)).toBe(0);
		expect(await r.lookupEnqueueReceipt('refused', { tenantId: null, idempotencyKey: 'k' })).toBeNull();
	});

	it('recovers the same receipt after a process restart', async () => {
		const first = runner();
		const receipt = await first.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: [1] });
		first.destroy();
		const restarted = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
		try {
			const second = runner({}, restarted);
			expect(await second.lookupEnqueueReceipt('send', { tenantId: 't1', idempotencyKey: 'k' })).toEqual(receipt);
			expect(await second.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: [1] })).toEqual(receipt);
		} finally {
			while (runners.length) runners.pop().destroy();
			await restarted.end();
		}
		expect(await count(TABLE)).toBe(1);
	});

	it('returns the committed receipt when the reply to a committed statement is lost', async () => {
		const query = client.query.bind(client);
		const lossy = Object.create(client);
		lossy.query = async (config, ...rest) => {
			const result = await query(config, ...rest);
			if (String(config?.text ?? '').trimStart().startsWith('WITH receipt AS (')) throw new Error('Connection terminated unexpectedly');
			return result;
		};
		const r = runner({}, lossy);
		const receipt = await r.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' });
		const { rows } = await client.query(`SELECT svti_tasks_id FROM ${TABLE}`);
		expect(rows).toEqual([{ svti_tasks_id: receipt.taskId }]);
	});

	it('reports an unknown outcome when the connection dies mid-statement, and the same retry then settles it', async () => {
		const r = runner({ forgetIdentity });
		const submission = { tenantId: 'ta', idempotencyKey: 'k', input: { owner: { tenantId: 'ta', userId: 'u' } } };
		const holder = await client.connect();
		let attempt;
		try {
			await holder.query('BEGIN');
			// An erasure of the identity in flight holds the statement at its shared lock.
			await holder.query(`SELECT pg_advisory_xact_lock(hashtextextended($1::regclass::oid::text || ':' || $2::text, 0))`, [TABLE, JSON.stringify(['ta', 'u'])]);
			attempt = r.enqueueOnce('send', submission).catch((error) => error);
			let pid;
			await waitFor(async () => {
				const { rows } = await client.query(`SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%${RECEIPTS}%' AND pid <> pg_backend_pid()`);
				pid = rows[0]?.pid;
				return pid !== undefined;
			});
			await client.query('SELECT pg_terminate_backend($1)', [pid]);
			const error = await attempt;
			expect(error).toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
			expect(error).toMatchObject({ tenantId: 'ta', taskName: 'send', idempotencyKey: 'k' });
			expect(error.taskId).toBeUndefined();
			await holder.query('COMMIT');
		} finally {
			holder.release();
		}
		expect(await count(RECEIPTS)).toBe(0);
		const receipt = await r.enqueueOnce('send', submission);
		expect(await r.enqueueOnce('send', submission)).toEqual(receipt);
		expect(await count(TABLE)).toBe(1);
	});

	it('keeps live receipts through task cleanup and removes only expired ones', async () => {
		const r = runner({ rowTtl: 1, cleanupInterval: 50 });
		const live = await r.enqueueOnce('send', { tenantId: null, idempotencyKey: 'live' });
		await client.query(`UPDATE ${TABLE} SET status = 'committed', result = '"ok"', updated_at = now() - interval '1 hour'`);
		await client.query(`INSERT INTO ${RECEIPTS} (scope, svti_tasks_id, input, created_at, dedup_expires_at) VALUES ('["x","send","old"]', gen_random_uuid(), 'null', 1, 2)`);
		await waitFor(async () => (await count(TABLE)) === 0 && (await count(RECEIPTS)) === 1);
		expect(await r.lookupEnqueueReceipt('send', { tenantId: null, idempotencyKey: 'live' })).toEqual(live);
		expect(await r.enqueueOnce('send', { tenantId: null, idempotencyKey: 'live' })).toEqual(live);
		expect(await count(TABLE)).toBe(0);
		await expect(r.await(live.taskId)).rejects.toMatchObject({ code: 'TASK_NOT_FOUND' });
	});

	it('replaces an expired receipt exactly once under concurrent producers and keeps the old task', async () => {
		const r = runner();
		const old = await r.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: 'old', dedupTtlMs: 1 });
		await wait(20);
		expect(await r.lookupEnqueueReceipt('send', { tenantId: 't1', idempotencyKey: 'k' })).toBeNull();
		const receipts = await Promise.all(Array.from({ length: 16 }, () => r.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: 'new' })));
		expect(new Set(receipts.map((receipt) => receipt.taskId)).size).toBe(1);
		expect(receipts[0].taskId).not.toBe(old.taskId);
		expect(await count(TABLE)).toBe(2);
		expect(await count(RECEIPTS)).toBe(1);

		const conflicting = await r.enqueueOnce('send', { tenantId: 't2', idempotencyKey: 'k', input: 0, dedupTtlMs: 1 });
		await wait(20);
		const settled = await Promise.allSettled(Array.from({ length: 12 }, (_, i) => r.enqueueOnce('send', { tenantId: 't2', idempotencyKey: 'k', input: i + 1 })));
		expect(settled.filter((entry) => entry.status === 'fulfilled')).toHaveLength(1);
		expect(settled.find((entry) => entry.status === 'fulfilled').value.taskId).not.toBe(conflicting.taskId);
		expect(await count(TABLE)).toBe(4);
	});

	it('erases receipts with task rows and lets an insertion in flight commit before the erasure reads', async () => {
		const r = runner({ forgetIdentity });
		const submission = { tenantId: 'ta', idempotencyKey: 'k', input: { owner: { tenantId: 'ta', userId: 'u' } } };
		const other = await r.enqueueOnce('send', { tenantId: 'tb', idempotencyKey: 'k', input: { owner: { tenantId: 'tb', userId: 'u' } } });
		const blocker = await client.connect();
		let enqueued, purged;
		try {
			await blocker.query('BEGIN');
			// An uncommitted insert of the same scope holds the enqueue after it took its shared identity lock.
			await blocker.query(`INSERT INTO ${RECEIPTS} (scope, svti_tasks_id, input, created_at, dedup_expires_at) VALUES ($1, gen_random_uuid(), 'null', 1, 2)`, [JSON.stringify(['ta', 'send', 'k'])]);
			enqueued = r.enqueueOnce('send', submission);
			await waitFor(async () => (await client.query(`SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'WITH receipt AS%' AND pid <> pg_backend_pid()`)).rows.length > 0);
			let settled = false;
			purged = r.purgeUser('ta', 'u').finally(() => { settled = true; });
			await wait(200);
			expect(settled).toBe(false);
			await blocker.query('ROLLBACK');
		} finally {
			blocker.release();
		}
		const receipt = await enqueued;
		expect(await purged).toBe(2);
		expect(await r.lookupEnqueueReceipt('send', { tenantId: 'ta', idempotencyKey: 'k' })).toBeNull();
		expect((await client.query(`SELECT 1 FROM ${TABLE} WHERE svti_tasks_id = $1`, [receipt.taskId])).rows).toEqual([]);
		expect(await r.lookupEnqueueReceipt('send', { tenantId: 'tb', idempotencyKey: 'k' })).toEqual(other);
		// A later, newly authorized request creates a new task.
		expect((await r.enqueueOnce('send', submission)).taskId).not.toBe(receipt.taskId);
	});

	it('holds a stamped enqueueOnce until an erasure in flight commits, then inserts', async () => {
		const r = runner({ forgetIdentity });
		const holder = await client.connect();
		let enqueued;
		try {
			await holder.query('BEGIN');
			await holder.query(`SELECT pg_advisory_xact_lock(hashtextextended($1::regclass::oid::text || ':' || $2::text, 0))`, [TABLE, JSON.stringify(['ta', 'u'])]);
			let settled = false;
			enqueued = r.enqueueOnce('send', { tenantId: 'ta', idempotencyKey: 'k', input: { owner: { tenantId: 'ta', userId: 'u' } } }).finally(() => { settled = true; });
			await r.enqueueOnce('send', { tenantId: 'tb', idempotencyKey: 'k', input: { owner: { tenantId: 'tb', userId: 'u' } } });
			await wait(200);
			expect(settled).toBe(false);
			await holder.query('COMMIT');
		} finally {
			holder.release();
		}
		await enqueued;
		expect(await count(RECEIPTS)).toBe(2);
	});
});
