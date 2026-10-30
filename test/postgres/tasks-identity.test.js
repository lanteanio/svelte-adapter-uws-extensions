import { describe, it, expect, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { createTaskSql } from '../../src/postgres/_tasks-sql.js';
import { createForgetStore } from '../../src/shared/forget-store.js';

const quiet = { recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0 };
const forgetIdentity = (input) => input?.owner;
const owner = (tenantId, userId) => ({ owner: { tenantId, userId } });

const runners = [];
function runner(client, options = {}) {
	const r = createTaskRunner(client, { ...quiet, forgetIdentity, ...options });
	runners.push(r);
	return r;
}
afterEach(() => {
	while (runners.length) runners.pop().destroy();
});

const rowsOf = (client) => [...client._getTaskRows().values()];

describe('postgres tasks tenant-qualified identity', () => {
	it('refuses forgetUserId together with forgetIdentity and a non-function extractor', () => {
		const client = mockPgClient();
		expect(() => createTaskRunner(client, { ...quiet, forgetIdentity, forgetUserId: () => 'u' })).toThrow('mutually exclusive');
		expect(() => createTaskRunner(client, { ...quiet, forgetIdentity: 'owner' })).toThrow('forgetIdentity must be a function');
	});

	it('stamps each row with its identity, an explicit absent one, or none without the option', async () => {
		const client = mockPgClient();
		const r = runner(client);
		r.register('job', async () => 'ok');
		const a = await r.enqueue('job', { input: owner('ta', '42') });
		const n = await r.enqueue('job', { input: owner(null, '42') });
		const none = await r.enqueue('job', { input: { owner: null } });
		await r.run('job', { input: owner('tb', 'x:y') });
		const legacy = runner(client, { forgetIdentity: undefined, forgetUserId: (input) => input?.user });
		const old = await legacy.enqueue('job', { input: { user: '42' } });
		const rows = client._getTaskRows();
		expect(rows.get(a)).toMatchObject({ identity_version: 1, tenant_id: 'ta', user_id: '42' });
		expect(rows.get(n)).toMatchObject({ identity_version: 1, tenant_id: null, user_id: '42' });
		expect(rows.get(none)).toMatchObject({ identity_version: 1, tenant_id: null, user_id: null });
		expect(rowsOf(client).find((row) => row.tenant_id === 'tb')).toMatchObject({ identity_version: 1, user_id: 'x:y', status: 'committed' });
		expect(rows.get(old)).toMatchObject({ identity_version: null, tenant_id: null, user_id: '42' });
	});

	it('writes no row when the extractor throws or returns a malformed identity', async () => {
		for (const extractor of [() => { throw new Error('no owner'); }, () => ({ userId: 'u' }), () => ({ tenantId: 'a.b', userId: 'u' }), () => ({ tenantId: 't', userId: '' }), () => 'u']) {
			const client = mockPgClient();
			const r = runner(client, { forgetIdentity: extractor });
			r.register('job', async () => 'ok');
			await expect(r.enqueue('job', { input: {} })).rejects.toThrow();
			await expect(r.run('job', { input: {} })).rejects.toThrow();
			expect(client._getTaskRows().size).toBe(0);
		}
	});

	it('erases one identity and keeps equal user ids in other tenants and the null tenant', async () => {
		const client = mockPgClient();
		const r = runner(client);
		for (const input of [owner('ta', '42'), owner('ta', '42'), owner('tb', '42'), owner(null, '42'), owner('ta', '43'), owner('ta', '["ta","42"]')]) {
			await r.enqueue('job', { input });
		}
		expect(await r.purgeUser('ta', '42')).toBe(2);
		const left = () => rowsOf(client).map((row) => [row.tenant_id, row.user_id]).sort();
		expect(left()).toEqual([[null, '42'], ['ta', '43'], ['ta', '["ta","42"]'], ['tb', '42']].sort());
		// No target row: nothing else goes.
		expect(await r.purgeUser('ta', '42')).toBe(0);
		expect(await r.purgeUser('tc', '42')).toBe(0);
		expect(left()).toHaveLength(4);
		expect(await r.purgeUser(null, '42')).toBe(1);
		expect(left()).toEqual([['ta', '43'], ['ta', '["ta","42"]'], ['tb', '42']].sort());
	});

	it('validates the purge identity the same way directly and through createForgetStore', async () => {
		const client = mockPgClient();
		const r = runner(client);
		const store = createForgetStore({ tasks: r });
		await r.enqueue('job', { input: owner('ta', '42') });
		await r.enqueue('job', { input: owner('tb', '42') });
		await expect(r.purgeUser('a.b', '42')).rejects.toThrow(TypeError);
		await expect(store.purgeUser('a.b', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE' });
		await expect(r.purgeUser(undefined, '42')).rejects.toThrow(/tenantId/);
		await expect(store.purgeUser(undefined, '42')).rejects.toThrow(/tenant id is required/);
		expect(client._getTaskRows().size).toBe(2);
		expect(await store.purgeUser('ta', '42')).toEqual({ tasks: 1 });
		expect(await r.purgeUser('tb', '42')).toBe(1);
	});

	it('reports rows written without an identity stamp as incomplete and keeps them', async () => {
		const client = mockPgClient();
		const legacy = runner(client, { forgetIdentity: undefined, forgetUserId: (input) => input?.user });
		const r = runner(client);
		const old = await legacy.enqueue('job', { input: { user: '42' } });
		await r.enqueue('job', { input: owner('ta', '42') });
		await r.enqueue('job', { input: owner('ta', '43') });
		await expect(createForgetStore({ tasks: r }).purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', partialCounts: { tasks: 1 } });
		await expect(r.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, erasureIncomplete: true, removed: 0 });
		expect(client._getTaskRows().has(old)).toBe(true);
		// Another user's erasure is complete.
		expect(await r.purgeUser('ta', '43')).toBe(1);
		// The legacy runner erases its own rows and never a stamped one.
		await r.enqueue('job', { input: owner('ta', '42') });
		expect(await legacy.purgeUser(null, '42')).toBe(1);
		expect(rowsOf(client).map((row) => [row.identity_version, row.tenant_id, row.user_id])).toEqual([[1, 'ta', '42']]);
		expect(await r.purgeUser('ta', '42')).toBe(1);
	});

	it('treats an unstamped row without a user id as residue for every identity', async () => {
		const client = mockPgClient();
		const plain = runner(client, { forgetIdentity: undefined });
		const r = runner(client);
		const old = await plain.enqueue('job', { input: {} });
		await expect(r.purgeUser('ta', '42')).rejects.toMatchObject({ legacyResidue: true, removed: 0 });
		// An explicitly unattributed qualified row is not residue.
		client._getTaskRows().delete(old);
		await r.enqueue('job', { input: { owner: null } });
		expect(await r.purgeUser('ta', '42')).toBe(0);
	});
});

describe('postgres tasks per-tenant dispatch', () => {
	async function setup() {
		const client = mockPgClient();
		const r = runner(client);
		const sql = createTaskSql({ client, table: 'svti_tasks', fenceTtl: 30, rowTtl: 3600, autoMigrate: true });
		await sql.ensureTable();
		const enqueue = (tenantId, n, name = 'job') => Promise.all(Array.from({ length: n }, (_, i) => r.enqueue(name, { input: owner(tenantId, 'u' + i) })));
		const claim = async (limit = 10, names = ['job']) => (await sql.claimPending(limit, names)).map((row) => client._getTaskRows().get(row.id));
		return { client, r, sql, enqueue, claim };
	}

	it('claims a later tenant in the first sweep behind a large backlog', async () => {
		const { enqueue, claim } = await setup();
		await enqueue('hostile', 100);
		const [victim] = await enqueue('victim', 1);
		const rows = await claim(10);
		expect(rows).toHaveLength(10);
		expect(rows.map((row) => row.id)).toContain(victim);
		expect(rows.filter((row) => row.tenant_id === 'hostile')).toHaveLength(9);
	});

	it('takes every scope\'s oldest row before any second row and keeps FIFO inside a scope', async () => {
		const { client, enqueue, claim } = await setup();
		const legacy = runner(client, { forgetIdentity: undefined });
		await enqueue('ta', 3);
		const legacyIds = [await legacy.enqueue('job', { input: {} }), await legacy.enqueue('job', { input: {} })];
		await enqueue(null, 2);
		await enqueue('tb', 1);
		const rows = await claim(5);
		const scope = (row) => (row.identity_version == null ? 'legacy' : row.tenant_id ?? 'untenanted');
		expect(rows.map(scope).sort()).toEqual(['legacy', 'legacy', 'ta', 'tb', 'untenanted'].sort());
		expect(rows.filter((row) => scope(row) === 'legacy').map((row) => row.id)).toEqual(legacyIds);
		expect(rows.find((row) => row.tenant_id === 'ta').user_id).toBe('u0');
		// The next sweep continues with what is left, oldest first per scope.
		const next = await claim(5);
		expect(next.map((row) => [scope(row), row.user_id])).toEqual(expect.arrayContaining([['ta', 'u1'], ['ta', 'u2'], ['untenanted', 'u1']]));
		expect(next).toHaveLength(3);
	});

	it('rotates through more tenants than one sweep takes', async () => {
		const { enqueue, claim } = await setup();
		const tenants = Array.from({ length: 15 }, (_, i) => 't' + String(i).padStart(2, '0'));
		for (const tenant of tenants) await enqueue(tenant, 2);
		const first = await claim(10);
		const second = await claim(10);
		// Every tenant gets its first row within two sweeps, though each sweep
		// could have been filled by the first ten tenants' backlog alone.
		const served = new Set([...first, ...second].map((row) => row.tenant_id));
		expect([...served].sort()).toEqual(tenants);
		expect(new Set(first.map((row) => row.tenant_id)).size).toBe(10);
	});

	it('claims only registered names', async () => {
		const { enqueue, claim } = await setup();
		await enqueue('ta', 2, 'other');
		const [mine] = await enqueue('tb', 1);
		expect((await claim(10)).map((row) => row.id)).toEqual([mine]);
	});

	it('runs a later tenant\'s task in the first dispatch sweep', async () => {
		const client = mockPgClient();
		const ran = [];
		const r = runner(client, { dispatchInterval: 20, dispatchBatchSize: 5 });
		r.register('job', async ({ input }) => { ran.push(input.owner.tenantId); });
		for (let i = 0; i < 50; i++) await r.enqueue('job', { input: owner('hostile', 'u' + i) });
		const victim = await r.enqueue('job', { input: owner('victim', 'v') });
		await r.await(victim, { timeout: 2000, pollInterval: 5 });
		expect(ran.indexOf('victim')).toBeLessThan(5);
	});
});
