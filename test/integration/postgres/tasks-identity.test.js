/**
 * Tenant-qualified task identity and per-tenant dispatch against a real
 * PostgreSQL server: concurrent claims, the erasure lock against in-flight
 * inserts, and the version 4 upgrade of a populated version 3 table.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createPgClient, migrate, postgresMigrations } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createTaskSql } from '../../../src/postgres/_tasks-sql.js';
import { createForgetStore } from '../../../src/shared/forget-store.js';
import { forgetFixtureMigrations } from '../helpers/postgres-schema.js';

const TABLE = 'identity_tasks';
const UPGRADE = 'identity_tasks_v3';
const quiet = { table: TABLE, recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0 };
const owner = (tenantId, userId) => ({ owner: { tenantId, userId } });
const forgetIdentity = (input) => input?.owner;
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let client;
const runners = [];
function runner(options = {}) {
	const r = createTaskRunner(client, { ...quiet, forgetIdentity, ...options });
	runners.push(r);
	return r;
}

beforeAll(() => {
	client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
});
beforeEach(async () => {
	while (runners.length) runners.pop().destroy();
	await client.query(`DROP TABLE IF EXISTS ${TABLE}, ${UPGRADE}`);
	await forgetFixtureMigrations(client, [TABLE, UPGRADE]);
	await migrate(client, { store: 'tasks', table: TABLE });
});
afterAll(async () => {
	while (runners.length) runners.pop().destroy();
	await client.query(`DROP TABLE IF EXISTS ${TABLE}, ${UPGRADE}`);
	await forgetFixtureMigrations(client, [TABLE, UPGRADE]);
	await client.end();
});

describe('postgres tasks tenant-qualified identity (integration)', () => {
	it('erases one identity and keeps equal user ids in other tenants, the null tenant and unicode ids', async () => {
		const r = runner();
		const unicode = 'Zo' + String.fromCharCode(0xeb);
		for (const input of [owner('ta', '42'), owner('tb', '42'), owner(null, '42'), owner('ta', unicode), owner('ta', unicode.toLowerCase())]) {
			await r.enqueue('job', { input });
		}
		expect(await createForgetStore({ tasks: r }).purgeUser('ta', '42')).toEqual({ tasks: 1 });
		expect(await r.purgeUser('ta', unicode)).toBe(1);
		const { rows } = await client.query(`SELECT tenant_id, user_id FROM ${TABLE} ORDER BY tenant_id NULLS FIRST, user_id`);
		expect(rows).toEqual([{ tenant_id: null, user_id: '42' }, { tenant_id: 'ta', user_id: unicode.toLowerCase() }, { tenant_id: 'tb', user_id: '42' }]);
		expect(await r.purgeUser('ta', '42')).toBe(0);
	});

	it('reports unstamped rows as residue and keeps them', async () => {
		const legacy = runner({ forgetIdentity: undefined, forgetUserId: (input) => input?.user });
		const r = runner();
		await legacy.enqueue('job', { input: { user: '42' } });
		await r.enqueue('job', { input: owner('ta', '42') });
		await expect(r.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, removed: 1 });
		const { rows } = await client.query(`SELECT identity_version, user_id FROM ${TABLE}`);
		expect(rows).toEqual([{ identity_version: null, user_id: '42' }]);
	});

	it('waits for an insert in flight and erases its row', async () => {
		const tuple = JSON.stringify(['ta', '42']);
		const connection = await client.connect();
		let purged;
		try {
			await connection.query('BEGIN');
			// The shared lock a stamped insert holds until it commits.
			await connection.query(`SELECT pg_advisory_xact_lock_shared(hashtextextended($1::regclass::oid::text || ':' || $2::text, 0))`, [TABLE, tuple]);
			await connection.query(`INSERT INTO ${TABLE} (svti_tasks_id, name, status, fence, fence_expires_at, attempts, identity_version, tenant_id, user_id)
				VALUES (gen_random_uuid(), 'job', 'pending', gen_random_uuid(), now(), 0, 1, 'ta', '42')`);
			const r = runner();
			let settled = false;
			purged = r.purgeUser('ta', '42').finally(() => { settled = true; });
			await wait(200);
			expect(settled).toBe(false);
			await connection.query('COMMIT');
		} finally {
			connection.release();
		}
		expect(await purged).toBe(1);
		expect((await client.query(`SELECT count(*)::int AS n FROM ${TABLE}`)).rows[0].n).toBe(0);
	});

	it('holds a stamped insert until an erasure in flight commits', async () => {
		const r = runner();
		const connection = await client.connect();
		let enqueued;
		try {
			await connection.query('BEGIN');
			// Another spelling of the same table takes the same lock.
			await connection.query(`SELECT pg_advisory_xact_lock(hashtextextended($1::regclass::oid::text || ':' || $2::text, 0))`, ['public.' + TABLE.toUpperCase(), JSON.stringify(['ta', '42'])]);
			let settled = false;
			enqueued = r.enqueue('job', { input: owner('ta', '42') }).finally(() => { settled = true; });
			// Another identity is not held up.
			await r.enqueue('job', { input: owner('tb', '42') });
			await wait(200);
			expect(settled).toBe(false);
			await connection.query('COMMIT');
		} finally {
			connection.release();
		}
		await enqueued;
		expect((await client.query(`SELECT count(*)::int AS n FROM ${TABLE}`)).rows[0].n).toBe(2);
	});
});

describe('postgres tasks per-tenant dispatch (integration)', () => {
	const sql = () => createTaskSql({ client, table: TABLE, fenceTtl: 30, rowTtl: 3600, autoMigrate: false });

	it('keeps a separate position for each name across claims', async () => {
		const r = runner();
		const enqueue = async (tenant, n, name) => { for (let i = 0; i < n; i++) await r.enqueue(name, { input: owner(tenant, name + i) }); };
		await enqueue('aaa', 10, 'heavy');
		await enqueue('mmm', 1, 'heavy');
		await enqueue('zzz', 10, 'heavy');
		await enqueue('zzz', 10, 'light');
		await enqueue('bbb', 10, 'other');
		const s = sql();
		const tenants = async (limit, names) => (await s.claimPending(limit, names)).map((row) => row.input.owner.tenantId).sort();
		expect(await tenants(1, ['heavy'])).toEqual(['aaa']);
		// light and other waited longer and fill the claim; heavy's mmm was not
		// chosen, so heavy stays at aaa.
		expect(await tenants(2, ['heavy', 'light', 'other'])).toEqual(['bbb', 'zzz']);
		expect(await tenants(1, ['heavy'])).toEqual(['mmm']);
		// light moved past zzz, other past bbb: each continues from its own place.
		expect(await tenants(1, ['light'])).toEqual(['zzz']);
		expect(await tenants(1, ['other'])).toEqual(['bbb']);
	});

	it('reaches a victim of one name while a tenant floods two names, one scope per claim', async () => {
		const r = runner();
		for (let i = 0; i < 30; i++) await r.enqueue('email', { input: owner('aaa', 'e' + i) });
		for (let i = 0; i < 30; i++) await r.enqueue('report', { input: owner('aaa', 'r' + i) });
		const victim = await r.enqueue('report', { input: owner('zzz', 'v') });
		const s = sql();
		const served = [];
		for (let i = 0; i < 4; i++) served.push(...(await s.claimPending(1, ['email', 'report'])).map((row) => row.id));
		expect(served).toContain(victim);
	});

	it('claims a later tenant and the unstamped scope in the first sweep behind a large backlog', async () => {
		const r = runner();
		const legacy = runner({ forgetIdentity: undefined });
		for (let i = 0; i < 200; i++) await r.enqueue('job', { input: owner('hostile', 'u' + i) });
		const victim = await r.enqueue('job', { input: owner('victim', 'v') });
		const old = await legacy.enqueue('job', { input: {} });
		const claimed = await sql().claimPending(10, ['job']);
		const ids = claimed.map((row) => row.id);
		expect(ids).toHaveLength(10);
		expect(ids).toContain(victim);
		expect(ids).toContain(old);
	});

	it('never claims a row twice and keeps claiming under concurrent sweeps', async () => {
		const r = runner();
		const total = 300;
		for (let i = 0; i < total; i++) await r.enqueue('job', { input: owner('t' + (i % 7), 'u' + i) });
		const sweepers = Array.from({ length: 6 }, () => sql());
		const seen = [];
		let starved = 0;
		await Promise.all(sweepers.map(async (s) => {
			while (seen.length < total) {
				const unclaimed = total - seen.length;
				const rows = await s.claimPending(10, ['job']);
				// Overlapping claims lock at most 7 * 4 rows each, so with more
				// than that many per sweeper left every claim finds rows.
				if (rows.length === 0 && unclaimed > 6 * 28) starved++;
				if (rows.length === 0) await wait(1);
				seen.push(...rows.map((row) => row.id));
			}
		}));
		expect(starved).toBe(0);
		expect(seen).toHaveLength(total);
		expect(new Set(seen).size).toBe(total);
		const { rows } = await client.query(`SELECT max(attempts)::int AS max, count(*) FILTER (WHERE status = 'running')::int AS running FROM ${TABLE}`);
		expect(rows[0]).toEqual({ max: 1, running: total });
	});

	it.each(['legacy', 'tenant'])('takes the next rows while another claim holds the oldest (%s scope)', async (scope) => {
		const r = scope === 'legacy' ? runner({ forgetIdentity: undefined }) : runner();
		for (let i = 0; i < 25; i++) await r.enqueue('job', { input: owner('ta', 'u' + i) });
		const connection = await client.connect();
		try {
			await connection.query('BEGIN');
			const first = await createTaskSql({ client: connection, table: TABLE, fenceTtl: 30, rowTtl: 3600, autoMigrate: false }).claimPending(10, ['job']);
			expect(first).toHaveLength(10);
			const second = await sql().claimPending(10, ['job']);
			expect(second).toHaveLength(10);
			expect(second.filter((row) => first.some((held) => held.id === row.id))).toEqual([]);
			await connection.query('COMMIT');
		} finally {
			connection.release();
		}
	});

	it('reads the pending-scope index with the name and never rows of other names', async () => {
		const r = runner();
		for (let i = 0; i < 200; i++) await r.enqueue('other', { input: owner('t1', 'o' + i) });
		for (let i = 0; i < 50; i++) await r.enqueue('job', { input: owner('t' + (i % 5), 'u' + i) });
		await client.query(`ANALYZE ${TABLE}`);
		const connection = await client.connect();
		try {
			await connection.query('SET enable_seqscan = off');
			const plan = await connection.query(`EXPLAIN (ANALYZE, BUFFERS OFF, FORMAT JSON) SELECT svti_tasks_id FROM ${TABLE}
				WHERE status = 'pending' AND identity_version = 1 AND name = 'job' AND tenant_id = 't1'
				ORDER BY identity_version, name, tenant_id, created_at LIMIT 10`);
			const text = JSON.stringify(plan.rows[0]['QUERY PLAN']);
			expect(text).toContain(`idx_${TABLE}_pending_scope`);
			expect(text).not.toContain('Rows Removed by Filter');
			// The claim serves the registered name and leaves the others pending.
			const claimed = await sql().claimPending(10, ['job']);
			expect(claimed).toHaveLength(10);
			expect(claimed.every((row) => row.name === 'job')).toBe(true);
		} finally {
			connection.release(true);
		}
	});
});

describe('postgres tasks version 4 upgrade (integration)', () => {
	it('upgrades a populated version 3 table and keeps its rows unstamped', async () => {
		const versions = postgresMigrations({ store: 'tasks', table: UPGRADE });
		for (const version of versions.slice(0, 3)) {
			for (const statement of version.statements) await client.query(statement);
			await client.query('INSERT INTO svti_schema_migrations (table_name, store, version, checksum) VALUES ($1, $2, $3, $4)', [UPGRADE, 'tasks', version.version, version.checksum]);
		}
		await client.query(`INSERT INTO ${UPGRADE} (svti_tasks_id, name, status, fence, fence_expires_at, attempts, user_id)
			VALUES (gen_random_uuid(), 'job', 'pending', gen_random_uuid(), now(), 0, '42'),
			       (gen_random_uuid(), 'job', 'committed', gen_random_uuid(), now(), 1, '43'),
			       (gen_random_uuid(), 'job', 'pending', gen_random_uuid(), now(), 0, NULL)`);

		await migrate(client, { store: 'tasks', table: UPGRADE });

		const ledger = await client.query('SELECT version FROM svti_schema_migrations WHERE table_name = $1 ORDER BY version', [UPGRADE]);
		expect(ledger.rows.map((row) => row.version)).toEqual([1, 2, 3, 4]);
		const indexes = (await client.query('SELECT indexname FROM pg_indexes WHERE tablename = $1', [UPGRADE])).rows.map((row) => row.indexname);
		expect(indexes).toEqual(expect.arrayContaining([`idx_${UPGRADE}_identity`, `idx_${UPGRADE}_pending_scope`]));
		expect(indexes).not.toContain(`idx_${UPGRADE}_pending_created`);
		const rows = (await client.query(`SELECT identity_version, tenant_id, user_id, status FROM ${UPGRADE} ORDER BY user_id NULLS LAST`)).rows;
		expect(rows).toEqual([
			{ identity_version: null, tenant_id: null, user_id: '42', status: 'pending' },
			{ identity_version: null, tenant_id: null, user_id: '43', status: 'committed' },
			{ identity_version: null, tenant_id: null, user_id: null, status: 'pending' }
		]);
		// The stamp check refuses a tenant on an unstamped row.
		await expect(client.query(`UPDATE ${UPGRADE} SET tenant_id = 'ta' WHERE user_id = '42'`)).rejects.toMatchObject({ code: '23514' });

		const r = createTaskRunner(client, { ...quiet, table: UPGRADE, forgetIdentity, autoMigrate: false });
		runners.push(r);
		await r.enqueue('job', { input: owner('ta', '42') });
		await expect(r.purgeUser('ta', '42')).rejects.toMatchObject({ code: 'ERASURE_INCOMPLETE', legacyResidue: true, removed: 1 });
		// The old pending rows are still dispatched, alongside the new scope.
		await r.enqueue('job', { input: owner('ta', '7') });
		const claimed = await createTaskSql({ client, table: UPGRADE, fenceTtl: 30, rowTtl: 3600, autoMigrate: false }).claimPending(10, ['job']);
		expect(claimed).toHaveLength(3);
		// Backfilled from authoritative ownership data, the rows stop being residue.
		await client.query(`UPDATE ${UPGRADE} SET identity_version = 1, tenant_id = 'tb' WHERE identity_version IS NULL AND user_id IS NOT NULL`);
		await client.query(`DELETE FROM ${UPGRADE} WHERE identity_version IS NULL`);
		expect(await r.purgeUser('ta', '42')).toBe(0);
		expect(await r.purgeUser('tb', '42')).toBe(1);
	});
});
