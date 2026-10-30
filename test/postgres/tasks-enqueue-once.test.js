import { afterEach, describe, expect, it, vi } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner, TaskEnqueueConflictError, TaskEnqueueOutcomeUnknownError } from '../../src/postgres/tasks.js';
import { postgresMigrations } from '../../src/postgres/migrations.js';

const DAY = 24 * 3600 * 1000;
const runners = [];
afterEach(() => { for (const runner of runners.splice(0)) runner.destroy(); vi.useRealTimers(); vi.restoreAllMocks(); });
function create(client, options = {}) {
	const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0, ...options }); runners.push(runner); return runner;
}
const receiptStatement = (config) => String(config?.text ?? config).trimStart().startsWith('WITH receipt AS (');
const receiptRead = (config) => String(config?.text ?? config).includes('dedup_expires_at > floor');
const lost = () => Object.assign(new Error('Connection terminated unexpectedly'), {});

describe('enqueueOnce', () => {
	it('inserts one pending task per scope and returns the original receipt to equal retries', async () => {
		const client = mockPgClient();
		const producers = [create(client), create(client)];
		await Promise.all(producers.map((runner) => runner.ready()));
		const options = { tenantId: 'acme', idempotencyKey: 'order-1', input: { amount: 5 } };
		const receipts = await Promise.all([producers[0].enqueueOnce('charge', options), producers[1].enqueueOnce('charge', options), producers[0].enqueueOnce('charge', options)]);
		expect(new Set(receipts.map((receipt) => receipt.taskId)).size).toBe(1);
		expect(receipts[1]).toEqual(receipts[0]);
		expect(client._getTaskRows().size).toBe(1);
		expect([...client._getTaskRows().values()][0]).toMatchObject({ status: 'pending', name: 'charge', idempotency_key: 'order-1', input: { amount: 5 } });
		expect(await producers[1].lookupEnqueueReceipt('charge', { tenantId: 'acme', idempotencyKey: 'order-1' })).toEqual(receipts[0]);
	});

	it('dispatches the task once and forwards the original key to its handler', async () => {
		const client = mockPgClient();
		const producer = create(client);
		const idempotency = { acquire: vi.fn(async () => { throw new Error('enqueueOnce must not consult the run cache'); }) };
		const worker = create(client, { dispatchInterval: 10, idempotency });
		const seen = [];
		worker.register('charge', async ({ input, idempotencyKey }) => { seen.push({ input, idempotencyKey }); return 'done'; });
		await Promise.all([producer.ready(), worker.ready()]);
		const first = await producer.enqueueOnce('charge', { tenantId: null, idempotencyKey: 'k', input: [1] });
		await producer.enqueueOnce('charge', { tenantId: null, idempotencyKey: 'k', input: [1] });
		expect(await worker.await(first.taskId, { pollInterval: 5, timeout: 2000 })).toBe('done');
		expect(seen).toEqual([{ input: [1], idempotencyKey: 'k' }]);
		expect(idempotency.acquire).not.toHaveBeenCalled();
	});

	it('compares the exact validated JSON bytes and rejects other input without inserting', async () => {
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		await runner.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: { a: 1, b: 2 } });
		const reordered = runner.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: { b: 2, a: 1 } });
		await expect(reordered).rejects.toBeInstanceOf(TaskEnqueueConflictError);
		await expect(runner.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: { a: 1, b: 2 } })).resolves.toBeDefined();
		const error = await runner.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: null }).catch((e) => e);
		expect(error).toMatchObject({ name: 'TaskEnqueueConflictError', code: 'TASK_ENQUEUE_CONFLICT', tenantId: 't1', taskName: 'send', idempotencyKey: 'k' });
		expect(error.taskId).toBeUndefined();
		expect(client._getTaskRows().size).toBe(1);
	});

	it('serializes input once, so a stateful toJSON cannot make the stored and compared bytes differ', async () => {
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		let calls = 0;
		const input = { toJSON: () => ({ call: ++calls }) };
		await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input });
		expect(calls).toBe(1);
		const [receipt] = client._getReceiptRows().values();
		expect(receipt.input).toBe('{"call":1}');
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input })).rejects.toBeInstanceOf(TaskEnqueueConflictError);
		expect(calls).toBe(2);
	});

	it('scopes keys by exact tenant and task name, null being distinct from every string', async () => {
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const ids = [];
		for (const [name, tenantId] of [['a', null], ['a', 'null'], ['a', 't1'], ['a', 't2'], ['b', null], ['b', 't1']]) {
			ids.push((await runner.enqueueOnce(name, { tenantId, idempotencyKey: 'same', input: 1 })).taskId);
		}
		expect(new Set(ids).size).toBe(6);
		expect(client._getTaskRows().size).toBe(6);
		// Delimiter-like keys do not collide with a split elsewhere.
		const x = await runner.enqueueOnce('a', { tenantId: 't1', idempotencyKey: 'x","y', input: 1 });
		const y = await runner.enqueueOnce('a', { tenantId: 't1', idempotencyKey: 'x', input: 1 });
		expect(x.taskId).not.toBe(y.taskId);
		expect(await runner.lookupEnqueueReceipt('b', { tenantId: 'null', idempotencyKey: 'same' })).toBeNull();
	});

	it('fixes expiry at first insertion from database time; retries and other TTLs never extend it', async () => {
		vi.useFakeTimers({ now: 1_700_000_000_000, toFake: ['Date'] });
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const first = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' });
		expect(first.dedupExpiresAt).toBe(1_700_000_000_000 + DAY);
		vi.setSystemTime(1_700_000_000_000 + DAY - 1);
		const retry = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', dedupTtlMs: 30 * DAY });
		expect(retry).toEqual(first);
		const custom = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'short', dedupTtlMs: 1000 });
		expect(custom.dedupExpiresAt).toBe(1_700_000_000_000 + DAY - 1 + 1000);
	});

	it('replaces an expired receipt with a new task while the old task remains, and lookup returns null once expired', async () => {
		vi.useFakeTimers({ now: 1_000_000, toFake: ['Date'] });
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const query = { tenantId: 'acme', idempotencyKey: 'k' };
		const old = await runner.enqueueOnce('send', { ...query, dedupTtlMs: 1000 });
		vi.setSystemTime(1_000_000 + 1000);
		expect(await runner.lookupEnqueueReceipt('send', query)).toBeNull();
		// Expired: other input is no conflict, the scope is free.
		const fresh = await runner.enqueueOnce('send', { ...query, input: 'other' });
		expect(fresh.taskId).not.toBe(old.taskId);
		expect(fresh.dedupExpiresAt).toBe(1_001_000 + DAY);
		expect(client._getTaskRows().has(old.taskId)).toBe(true);
		expect(await runner.lookupEnqueueReceipt('send', query)).toEqual(fresh);
	});

	it('inserts again when the receipt that blocked it expired before it could be read', async () => {
		vi.useFakeTimers({ now: 5_000_000, toFake: ['Date'] });
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const old = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 'old', dedupTtlMs: 1000 });
		const query = client.query.bind(client);
		client.query = async (config, ...rest) => {
			const result = await query(config, ...rest);
			// The expiry passes between the blocked insert and its read.
			if (receiptStatement(config) && result.rows.length === 0) vi.setSystemTime(5_000_000 + 1000);
			return result;
		};
		const fresh = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 'new' });
		expect(fresh.taskId).not.toBe(old.taskId);
		expect(client._getTaskRows().size).toBe(2);
	});

	it('gives up with an ordinary error, claiming no receipt, when the scope never settles', async () => {
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const query = client.query.bind(client);
		let statements = 0;
		client.query = async (config, ...rest) => {
			if (receiptStatement(config)) { statements++; return { rows: [], rowCount: 0 }; }
			return query(config, ...rest);
		};
		const error = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' }).catch((e) => e);
		expect(error).not.toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
		expect(error.message).toContain('inserted nothing');
		expect(statements).toBe(3);
	});

	it('keeps a live receipt across task cleanup; awaiting the cleaned task reports it missing, not a replacement', async () => {
		const client = mockPgClient();
		const runner = create(client, { rowTtl: 1, cleanupInterval: 30 });
		runner.register('send', async () => 'ok');
		await runner.ready();
		const receipt = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' });
		const row = client._getTaskRows().get(receipt.taskId);
		Object.assign(row, { status: 'committed', result: 'ok', updated_at: Date.now() - 5000 });
		client._getReceiptRows().set('["t","send","expired"]', { id: '00000000-0000-4000-8000-000000000000', input: 'null', identity_version: null, tenant_id: null, user_id: null, created_at: 1, dedup_expires_at: 2 });
		await vi.waitFor(() => expect(client._getTaskRows().size).toBe(0), { timeout: 2000, interval: 20 });
		await vi.waitFor(() => expect(client._getReceiptRows().size).toBe(1), { timeout: 2000, interval: 20 });
		expect(await runner.lookupEnqueueReceipt('send', { tenantId: null, idempotencyKey: 'k' })).toEqual(receipt);
		expect(await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' })).toEqual(receipt);
		expect(client._getTaskRows().size).toBe(0);
		await expect(runner.await(receipt.taskId)).rejects.toMatchObject({ code: 'TASK_NOT_FOUND' });
	});

	it('validates scope, key, TTL and name before touching the database', async () => {
		const client = mockPgClient();
		const runner = create(client);
		await runner.ready();
		const spy = vi.spyOn(client, 'query');
		for (const options of [undefined, {}, { idempotencyKey: 'k' }, { tenantId: undefined, idempotencyKey: 'k' }, { tenantId: '', idempotencyKey: 'k' }, { tenantId: 'a'.repeat(65), idempotencyKey: 'k' }, { tenantId: 'a b', idempotencyKey: 'k' }]) {
			await expect(runner.enqueueOnce('send', options)).rejects.toThrow(TypeError);
			await expect(runner.lookupEnqueueReceipt('send', options)).rejects.toThrow(TypeError);
		}
		for (const idempotencyKey of [undefined, '', 'k'.repeat(257), 7]) {
			await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey })).rejects.toThrow('idempotencyKey');
			await expect(runner.lookupEnqueueReceipt('send', { tenantId: null, idempotencyKey })).rejects.toThrow('idempotencyKey');
		}
		for (const dedupTtlMs of [0, -1, 1.5, 30 * DAY + 1, '1000', Number.NaN]) {
			await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', dedupTtlMs })).rejects.toThrow('dedupTtlMs');
		}
		await expect(runner.enqueueOnce('bad name', { tenantId: null, idempotencyKey: 'k' })).rejects.toThrow('invalid task name');
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 1n })).rejects.toThrow('not JSON-serialisable');
		expect(spy).not.toHaveBeenCalled();
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k'.repeat(256), dedupTtlMs: 30 * DAY })).resolves.toBeDefined();
	});
});

describe('enqueueOnce erasure owner', () => {
	const owner = (input) => input?.user ? { tenantId: input.tenant ?? null, userId: input.user } : null;

	it('requires a non-null extracted owner in the enqueue tenant', async () => {
		const client = mockPgClient();
		const runner = create(client, { forgetIdentity: owner });
		await runner.ready();
		await expect(runner.enqueueOnce('send', { tenantId: 't2', idempotencyKey: 'k', input: { tenant: 't1', user: 'u' } })).rejects.toThrow('another tenant');
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: { tenant: 't1', user: 'u' } })).rejects.toThrow('another tenant');
		expect(client._getTaskRows().size).toBe(0);
		// An explicit absent owner is unattributed data in any tenant.
		await expect(runner.enqueueOnce('send', { tenantId: 't2', idempotencyKey: 'k', input: { note: 1 } })).resolves.toBeDefined();
		expect([...client._getReceiptRows().values()][0]).toMatchObject({ identity_version: 1, tenant_id: null, user_id: null });
	});

	it('rejects the same input bytes under a different owner stamp', async () => {
		const client = mockPgClient();
		const stamping = create(client, { forgetIdentity: () => ({ tenantId: 't1', userId: 'u' }) });
		const plain = create(client);
		await Promise.all([stamping.ready(), plain.ready()]);
		await stamping.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: 1 });
		await expect(plain.enqueueOnce('send', { tenantId: 't1', idempotencyKey: 'k', input: 1 })).rejects.toBeInstanceOf(TaskEnqueueConflictError);
		expect(client._getTaskRows().size).toBe(1);
	});

	it('erases the identity\'s receipts with its task rows and keeps every other identity', async () => {
		const client = mockPgClient();
		const runner = create(client, { forgetIdentity: owner });
		await runner.ready();
		const target = { tenantId: 't1', idempotencyKey: 'k', input: { tenant: 't1', user: 'u' } };
		await runner.enqueueOnce('send', target);
		await runner.enqueueOnce('send', { tenantId: 't2', idempotencyKey: 'k', input: { tenant: 't2', user: 'u' } });
		await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: { user: 'u' } });
		// The task row alone was already cleaned up; the receipt still counts.
		const [first] = client._getReceiptRows().values();
		client._getTaskRows().delete(first.id);
		expect(await runner.purgeUser('t1', 'u')).toBe(1);
		expect(await runner.lookupEnqueueReceipt('send', { tenantId: 't1', idempotencyKey: 'k' })).toBeNull();
		expect(await runner.lookupEnqueueReceipt('send', { tenantId: 't2', idempotencyKey: 'k' })).not.toBeNull();
		expect(await runner.lookupEnqueueReceipt('send', { tenantId: null, idempotencyKey: 'k' })).not.toBeNull();
		expect(await runner.purgeUser(null, 'u')).toBe(2);
		// Erasure removes the deduplication memory: a newly authorized request creates a new task.
		const again = await runner.enqueueOnce('send', target);
		expect(again.taskId).not.toBe(first.id);
	});

	it('reports unstamped receipts that may hold the user as residue', async () => {
		const client = mockPgClient();
		const legacy = create(client);
		const qualified = create(client, { forgetIdentity: owner });
		await Promise.all([legacy.ready(), qualified.ready()]);
		const { taskId } = await legacy.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' });
		client._getTaskRows().delete(taskId);
		await expect(qualified.purgeUser('t1', 'u')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true });
	});

	it('erases legacy receipts with forgetUserId', async () => {
		const client = mockPgClient();
		const runner = create(client, { forgetUserId: (input) => input.user });
		await runner.ready();
		await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: { user: 'u' } });
		await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'other', input: { user: 'v' } });
		expect(await runner.purgeUser(null, 'u')).toBe(2);
		expect(client._getReceiptRows().size).toBe(1);
	});
});

describe('enqueueOnce reply loss', () => {
	async function setup(onReceipt, onRead) {
		const client = mockPgClient();
		const query = client.query.bind(client);
		client.query = async (config, ...rest) => {
			if (receiptStatement(config) && onReceipt) return onReceipt(() => query(config, ...rest));
			if (receiptRead(config) && onRead) return onRead(() => query(config, ...rest));
			return query(config, ...rest);
		};
		const runner = create(client);
		await runner.ready();
		return { client, runner };
	}

	it('returns the receipt when the authoritative read finds its own committed task', async () => {
		let reads = 0;
		const { client, runner } = await setup(async (run) => { await run(); throw lost(); }, (run) => { reads++; return run(); });
		const receipt = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' });
		expect(reads).toBe(1);
		expect(client._getTaskRows().has(receipt.taskId)).toBe(true);
	});

	it('rejects with an unknown outcome, never a task id, when nothing is visible yet', async () => {
		const { client, runner } = await setup(async () => { throw lost(); });
		const error = await runner.enqueueOnce('send', { tenantId: 'acme', idempotencyKey: 'k' }).catch((e) => e);
		expect(error).toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
		expect(error).toMatchObject({ code: 'TASK_ENQUEUE_OUTCOME_UNKNOWN', tenantId: 'acme', taskName: 'send', idempotencyKey: 'k' });
		expect(error.taskId).toBeUndefined();
		expect(error.cause.message).toBe('Connection terminated unexpectedly');
		expect(client._getTaskRows().size).toBe(0);
	});

	it('rejects with an unknown outcome when the recovery read itself fails', async () => {
		const { runner } = await setup(async (run) => { await run(); throw lost(); }, async () => { throw lost(); });
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' })).rejects.toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
	});

	it('returns an equal submission\'s live receipt, and reports a different one as unknown rather than a conflict', async () => {
		let fail = false;
		const { runner } = await setup(async (run) => { if (fail) throw lost(); return run(); });
		const original = await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 1 });
		fail = true;
		expect(await runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 1 })).toEqual(original);
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k', input: 2 })).rejects.toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
	});

	it('rethrows a statement error PostgreSQL reported, which rolled the statement back, without a recovery read', async () => {
		let reads = 0;
		const failure = Object.assign(new Error('violates check constraint'), { code: '23514', severity: 'ERROR' });
		const { runner } = await setup(async () => { throw failure; }, (run) => { reads++; return run(); });
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' })).rejects.toBe(failure);
		expect(reads).toBe(0);
		for (const code of ['08006', '57P01']) {
			const fatal = Object.assign(new Error('terminating connection'), { code, severity: 'FATAL' });
			const { runner: other } = await setup(async () => { throw fatal; });
			await expect(other.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' })).rejects.toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
		}
	});

	it('never falls back to enqueue or runs the handler inline', async () => {
		const handler = vi.fn(async () => 'ran');
		const { client, runner } = await setup(async () => { throw lost(); });
		runner.register('send', handler);
		await expect(runner.enqueueOnce('send', { tenantId: null, idempotencyKey: 'k' })).rejects.toBeInstanceOf(TaskEnqueueOutcomeUnknownError);
		expect(handler).not.toHaveBeenCalled();
		expect(client._getTaskRows().size).toBe(0);
	});
});

describe('enqueue receipt schema', () => {
	it('adds the receipts table, its checks and indexes as tasks version 5 without changing versions 1 to 4', () => {
		const versions = postgresMigrations({ store: 'tasks' });
		expect(versions.map((entry) => entry.version)).toEqual([1, 2, 3, 4, 5]);
		expect(versions[4].statements[0]).toBe('CREATE TABLE IF NOT EXISTS svti_tasks_receipts (scope TEXT NOT NULL, svti_tasks_id UUID NOT NULL, input TEXT NOT NULL, identity_version INTEGER, tenant_id TEXT, user_id TEXT, created_at BIGINT NOT NULL, dedup_expires_at BIGINT NOT NULL, PRIMARY KEY (scope))');
		expect(versions[4].statements.slice(-2)).toEqual([
			'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_svti_tasks_rcpt_expiry ON svti_tasks_receipts (dedup_expires_at)',
			'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_svti_tasks_rcpt_owner ON svti_tasks_receipts (identity_version, tenant_id, user_id)'
		]);
		// No foreign key: task cleanup can never cascade into a live receipt.
		expect(versions[4].statements.join(' ')).not.toMatch(/REFERENCES|FOREIGN KEY/);
	});

	it('keeps every generated receipt identifier within the PostgreSQL limit for the longest table name', () => {
		const table = 'a'.repeat(42);
		const names = postgresMigrations({ store: 'tasks', table }).flatMap((entry) => entry.statements)
			.flatMap((statement) => [...statement.matchAll(/\b(?:idx|chk)_[a-z0-9_]+|\ba{42}_[a-z_]+/g)].map((match) => match[0]));
		expect(names).toContain(table + '_receipts');
		for (const name of names) expect(name.length, name).toBeLessThanOrEqual(63);
	});
});
