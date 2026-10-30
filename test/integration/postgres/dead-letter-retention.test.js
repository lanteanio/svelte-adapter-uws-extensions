import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { postgresMigrations, validateSchema } from '../../../src/postgres/migrations.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../../src/shared/runtime.js';

const table = 'svti_dlq_retention';
const pk = table + '_id';
const record = () => ({ webhookId: 'hook', topic: 'live', event: 'event', data: { retained: true }, attempts: 2, error: 'delivery failed', failedAt: Date.now() });
let client;
const stores = [];
function store(connection, options = {}) {
	const result = createDeadLetter(connection, { table, cleanupInterval: 10000, ...options });
	stores.push(result);
	return result;
}
function timers() {
	const ticks = [];
	setRuntimeEnv({ rng: { float: () => 1 }, timers: { set(cb, ms) { const handle = { cb, ms }; if (ms === 10000) ticks.push(handle); return handle; }, clear() {} } });
	return ticks;
}
async function physicalCount() { return (await client.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n; }
// Rows as the store writes them for the untenanted topic 'retained'.
async function seed(count, expired = false) {
	await client.query(`INSERT INTO ${table} (topic, failed_at, scope)
		SELECT 'retained', (extract(epoch from now()) * 1000)::bigint - $2, '~' FROM generate_series(1, $1::int)`, [count, expired ? 3600000 : 0]);
}
async function scopeCounts() {
	return (await client.query(`SELECT scope, records::int AS records FROM ${table}_scopes ORDER BY scope COLLATE "C"`)).rows;
}
async function actualCounts() {
	return (await client.query(`SELECT scope, count(*)::int AS records FROM ${table} GROUP BY scope ORDER BY scope COLLATE "C"`)).rows;
}
beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
afterEach(async () => {
	for (const item of stores.splice(0)) item.destroy();
	resetRuntimeEnv();
	await client.query(`DROP TABLE IF EXISTS ${table}, ${table}_purges, ${table}_scopes`);
	await client.query(`DROP FUNCTION IF EXISTS ${table}_scopes_sync()`);
	await client.query('DELETE FROM svti_schema_migrations WHERE table_name = $1', [table]);
	await client.query('DROP FUNCTION IF EXISTS svti_dlq_slow_delete()');
});
afterAll(async () => { await client.end(); });

describe('bounded PostgreSQL dead-letter retention', () => {
	it('limits write cleanup to 1000 rows and idle passes restore the cap without TTL', async () => {
		const ticks = timers();
		const dlq = store(client, { max: 2 });
		await dlq.add(record());
		await client.query(`TRUNCATE ${table}`);
		await seed(2502);
		const newest = await dlq.add(record());
		expect(await physicalCount()).toBe(1503);
		await ticks[0].cb();
		expect(await physicalCount()).toBe(503);
		await ticks[0].cb();
		expect(await physicalCount()).toBe(2);
		expect((await dlq.list()).map(r => r.id)).toEqual([newest, String(Number(newest) - 1)]);
	});
	it('excludes expired rows before bounded idle cleanup physically drains them', async () => {
		const ticks = timers();
		const dlq = store(client, { max: 5000, ttlMs: 60000 });
		const live = await dlq.add(record());
		await seed(1500, true);
		const expired = (await client.query(`SELECT max(${pk})::text AS id FROM ${table}`)).rows[0].id;
		expect(await physicalCount()).toBe(1501);
		expect(await dlq.get(expired)).toBeNull();
		expect(await dlq.count()).toBe(1);
		expect(await dlq.count({ topic: 'retained' })).toBe(0);
		expect((await dlq.list()).map(r => r.id)).toEqual([live]);
		expect(await dlq.list({ topic: 'retained' })).toEqual([]);
		expect(await dlq.summary()).toMatchObject({ total: 1, byTopic: { live: 1 } });
		await ticks[0].cb();
		expect(await physicalCount()).toBe(501);
		await ticks[0].cb();
		expect(await physicalCount()).toBe(1);
		expect((await dlq.get(live)).id).toBe(live);
	});
	it('two replicas skip a held cleanup lock without rejecting committed captures', async () => {
		const ticks = timers(), metrics = createMetrics();
		const peer = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
		const a = store(client, { max: 2, metrics });
		const b = store(peer, { max: 2, metrics });
		let holder;
		try {
			await a.add(record()); await b.count();
			await client.query(`TRUNCATE ${table}`);
			await seed(1500);
			holder = await client.connect(); await holder.query('BEGIN');
			await holder.query('SELECT pg_advisory_xact_lock(1937134697, $1::regclass::oid::integer)', [table]);
			const ids = await Promise.all([a.add(record()), b.add(record())]);
			expect(await physicalCount()).toBe(1502);
			expect(metrics.serialize()).toContain('postgres_cleanup_runs_total{result="contended",store="dead-letter"} 2');
			await holder.query('COMMIT'); holder.release(); holder = null;
			await ticks[0].cb(); expect(await physicalCount()).toBe(502);
			await ticks[1].cb(); expect(await physicalCount()).toBe(2);
			expect((await a.list()).map(r => r.id).sort()).toEqual(ids.sort());
		} finally {
			if (holder) { await holder.query('ROLLBACK'); holder.release(); }
			b.destroy(); await peer.end();
		}
	});
	it('rolls back timed-out housekeeping and returns the committed capture receipt', async () => {
		const dlq = store(client, { max: 1, cleanupInterval: 0 });
		await dlq.add(record());
		await client.query(`TRUNCATE ${table}`);
		await seed(300);
		await client.query(`CREATE FUNCTION svti_dlq_slow_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.01); RETURN OLD; END $$`);
		await client.query(`CREATE TRIGGER slow_retention BEFORE DELETE ON ${table} FOR EACH ROW EXECUTE FUNCTION svti_dlq_slow_delete()`);
		const id = await dlq.add(record());
		expect(id).toMatch(/^\d+$/);
		expect(await physicalCount()).toBe(301);
		expect((await dlq.get(id)).data).toEqual({ retained: true });
		await client.query(`DROP TRIGGER slow_retention ON ${table}`);
		const latest = await dlq.add(record());
		expect(await physicalCount()).toBe(1);
		expect((await dlq.list()).map(r => r.id)).toEqual([latest]);
	});
	it('trims captures arriving after a cleanup snapshot in one trailing pass', async () => {
		let resume, entered;
		const blocked = new Promise(resolve => { resume = resolve; });
		const reached = new Promise(resolve => { entered = resolve; });
		let hold = true;
		const connection = {
			...client,
			async connect() {
				const tx = await client.connect();
				return {
					async query(sql, values) {
						const result = await tx.query(sql, values);
						if (sql.text?.trim().startsWith('SELECT scope, records,') && hold) {
							hold = false; entered(); await blocked;
						}
						return result;
					},
					release: error => tx.release(error),
					on: (...args) => tx.on(...args),
					removeListener: (...args) => tx.removeListener(...args)
				};
			}
		};
		const dlq = store(connection, { max: 2, cleanupInterval: 0 });
		const leading = dlq.add(record());
		try {
			await reached;
			const ids = await Promise.all(Array.from({ length: 10 }, () => dlq.add(record())));
			expect(await physicalCount()).toBe(11);
			resume(); await leading;
			await expect.poll(physicalCount).toBe(2);
			expect((await dlq.list()).map(r => r.id).sort()).toEqual(ids.sort((a, b) => Number(b) - Number(a)).slice(0, 2).sort());
		} finally { resume(); await leading; }
	});
	it('prunes expired erasure tombstones during bounded write cleanup', async () => {
		const dlq = store(client, { cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await dlq.purgeUser(null, 'alice');
		expect((await client.query(`SELECT count(*)::int AS n FROM ${table}_purges`)).rows[0].n).toBe(1);
		// Age the fence past its ten-minute window.
		await client.query(`UPDATE ${table}_purges SET purged_at = (extract(epoch from now()) * 1000)::bigint - 600100`);
		expect(await dlq.add({ ...record(), data: { user: 'alice' }, startedAt: 0 })).not.toBeNull();
		expect((await client.query(`SELECT count(*)::int AS n FROM ${table}_purges`)).rows[0].n).toBe(0);
	});
});

describe('fair-share PostgreSQL dead-letter eviction', () => {
	const tenantRecord = (topic, n) => ({ ...record(), topic, data: { n } });

	it('evicts the tenant holding the most records and never one under its share', async () => {
		const metrics = createMetrics();
		const dlq = store(client, { max: 10, cleanupInterval: 0, metrics });
		for (let n = 0; n < 5; n++) await dlq.add(tenantRecord('@t/victim/orders', n));
		for (let n = 0; n < 30; n++) await dlq.add(tenantRecord('@t/noisy/orders', n));
		expect((await dlq.list({ topic: '@t/victim/orders' })).map(r => r.data.n)).toEqual([4, 3, 2, 1, 0]);
		expect((await dlq.list({ topic: '@t/noisy/orders' })).map(r => r.data.n)).toEqual([29, 28, 27, 26, 25]);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 25');
		expect(await scopeCounts()).toEqual([{ scope: 'noisy', records: 5 }, { scope: 'victim', records: 5 }]);
	});

	it('keeps the per-scope counts exact for every statement that changes rows', async () => {
		const dlq = store(client, { max: 1000, cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await dlq.add({ ...record(), topic: '@t/a/orders', data: { user: 'u1' } });
		await dlq.add({ ...record(), topic: '@t/a/orders', data: { user: 'u2' } });
		const removable = await dlq.add({ ...record(), topic: 'orders' });
		await dlq.add({ ...record(), topic: '@t/' });
		expect(await scopeCounts()).toEqual([{ scope: '', records: 1 }, { scope: 'a', records: 2 }, { scope: '~', records: 1 }]);
		expect(await scopeCounts()).toEqual(await actualCounts());
		expect(await dlq.remove(removable)).toBe(true);
		expect(await dlq.purgeUser('a', 'u1')).toBe(1);
		// Writers outside the store are counted the same way.
		await client.query(`INSERT INTO ${table} (topic, failed_at, scope) SELECT '@t/b/x', 1, 'b' FROM generate_series(1, 3)`);
		await client.query(`UPDATE ${table} SET scope = 'c' WHERE ${pk} = (SELECT min(${pk}) FROM ${table} WHERE scope = 'b')`);
		await client.query(`DELETE FROM ${table} WHERE scope = 'b'`);
		expect(await scopeCounts()).toEqual(await actualCounts());
		expect((await scopeCounts()).map(r => r.scope)).not.toContain('b');
		await client.query(`TRUNCATE ${table}`);
		expect(await scopeCounts()).toEqual([]);
		await dlq.clear();
		expect(await scopeCounts()).toEqual([]);
	});

	it('migrates rows written before the scope column into the scope of their topic', async () => {
		// The table as version 3 left it, holding six captures.
		const versions = postgresMigrations({ store: 'dead-letter', table });
		await client.query('CREATE TABLE IF NOT EXISTS svti_schema_migrations (table_name TEXT NOT NULL, store TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (table_name, version))');
		for (const { statements } of versions.slice(0, 3)) for (const statement of statements) await client.query(statement);
		for (const { version, checksum } of versions.slice(0, 3)) {
			await client.query('INSERT INTO svti_schema_migrations (table_name, store, version, checksum) VALUES ($1, $2, $3, $4)', [table, 'dead-letter', version, checksum]);
		}
		await client.query(`INSERT INTO ${table} (topic, failed_at) SELECT CASE WHEN i <= 4 THEN '@t/earlier/orders' WHEN i = 5 THEN 'orders' ELSE '@t/x' END, i FROM generate_series(1, 6) i`);
		const dlq = store(client, { max: 8, cleanupInterval: 0 });
		await dlq.ready();
		expect(await scopeCounts()).toEqual([{ scope: '', records: 1 }, { scope: 'earlier', records: 4 }, { scope: '~', records: 1 }]);
		for (let n = 0; n < 4; n++) await dlq.add(tenantRecord('@t/fresh/orders', n));
		// earlier is largest at the first overflow, fresh at the second.
		expect((await dlq.list({ topic: '@t/earlier/orders' })).map(r => r.failedAt)).toEqual([4, 3, 2]);
		expect(await scopeCounts()).toEqual([{ scope: '', records: 1 }, { scope: 'earlier', records: 3 }, { scope: 'fresh', records: 3 }, { scope: '~', records: 1 }]);
	});

	it('breaks a tie between equally large scopes on the oldest record, whatever the tenant names', async () => {
		for (const order of [['a', 'b', 'z'], ['z', 'b', 'a']]) {
			const dlq = store(client, { max: 2, cleanupInterval: 0, forgetUserId: r => r.data?.user });
			await dlq.clear();
			const ids = [];
			for (const tenant of order) ids.push(await dlq.add(tenantRecord(`@t/${tenant}/x`, 0)));
			// The capture just made survives; the oldest record's tenant pays.
			expect(await dlq.get(ids[2])).not.toBeNull();
			expect((await dlq.list()).map(r => r.topic).sort()).toEqual(order.slice(1).map(t => `@t/${t}/x`).sort());
		}
	});

	it('drains equally large scopes a level per statement', async () => {
		const dlq = store(client, { max: 4, cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await dlq.ready();
		await client.query(`INSERT INTO ${table} (topic, failed_at, scope) SELECT '@t/' || s || '/x', 1, s FROM unnest(ARRAY['c', 'c', 'a', 'a', 'd', 'd', 'b', 'b']) s`);
		await dlq.add(tenantRecord('@t/e/x', 0));
		expect(await scopeCounts()).toEqual([{ scope: 'a', records: 1 }, { scope: 'b', records: 1 }, { scope: 'd', records: 1 }, { scope: 'e', records: 1 }]);
		expect(await scopeCounts()).toEqual(await actualCounts());
	});

	it('repairs counts that drifted outside the triggers on ready()', async () => {
		const dlq = store(client, { max: 10, cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await dlq.ready();
		for (let n = 0; n < 3; n++) await dlq.add(tenantRecord('@t/a/x', n));
		// A data-only restore with triggers enabled counts restored rows twice.
		await client.query(`UPDATE ${table}_scopes SET records = records + 9`);
		const restarted = store(client, { max: 10, cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await restarted.ready();
		expect(await scopeCounts()).toEqual([{ scope: 'a', records: 3 }]);
		await restarted.add(tenantRecord('@t/a/x', 3));
		expect(await dlq.count()).toBe(4);
	});

	it('updates the counts of the record table whatever the writer search_path', async () => {
		const dlq = store(client, { cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await dlq.ready();
		await client.query('DROP SCHEMA IF EXISTS svti_dlq_decoy CASCADE');
		await client.query('CREATE SCHEMA svti_dlq_decoy');
		try {
			await client.query(`CREATE TABLE svti_dlq_decoy.${table}_scopes (scope TEXT PRIMARY KEY, records BIGINT NOT NULL, oldest BIGINT NOT NULL)`);
			const writer = await client.connect();
			try {
				await writer.query('SET search_path = svti_dlq_decoy, public');
				await writer.query(`INSERT INTO public.${table} (topic, failed_at, scope) VALUES ('@t/a/x', 1, 'a')`);
			} finally { writer.release(true); }
			expect(await scopeCounts()).toEqual([{ scope: 'a', records: 1 }]);
			expect((await client.query(`SELECT count(*)::int AS n FROM svti_dlq_decoy.${table}_scopes`)).rows[0].n).toBe(0);
		} finally {
			await client.query('DROP SCHEMA svti_dlq_decoy CASCADE');
		}
	});

	it('keeps the breaker closed while cleanup contends with concurrent captures on the count rows', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		try {
			const dlq = store(client, { max: 20, cleanupInterval: 0, breaker, forgetUserId: r => r.data?.user });
			await dlq.ready();
			// Each capture holds one admission slot of the client's default
			// maxInFlight of 100 while it waits for a connection, as a single
			// statement did, so the bursts stay under it.
			const ids = [];
			for (let wave = 0; wave < 5; wave++) {
				ids.push(...await Promise.all(Array.from({ length: 60 }, (_, n) => dlq.add(tenantRecord(`@t/t${n % 3}/x`, wave * 60 + n)))));
			}
			expect(ids).toHaveLength(300);
			expect(ids.every(id => /^\d+$/.test(id))).toBe(true);
			expect(breaker.state).toBe('healthy');
			await dlq.add(tenantRecord('@t/t0/x', 300));
			expect(await scopeCounts()).toEqual(await actualCounts());
			expect(await dlq.count()).toBeLessThanOrEqual(20);
		} finally { breaker.destroy(); }
	});

	it('captures concurrently for one tenant under a repeatable-read default', async () => {
		const strict = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, options: { max: 8, options: '-c default_transaction_isolation=repeatable\\ read' } });
		try {
			expect((await strict.query('SHOW default_transaction_isolation')).rows[0].default_transaction_isolation).toBe('repeatable read');
			const dlq = store(strict, { max: 5, cleanupInterval: 0, forgetUserId: r => r.data?.user });
			await dlq.ready();
			// Every capture updates the same count row; half of them are fenced.
			const ids = await Promise.all(Array.from({ length: 40 }, (_, n) => dlq.add({ ...record(), topic: '@t/noisy/orders', data: n % 2 ? { n, user: 'u' + n } : { n } })));
			expect(ids.every(id => /^\d+$/.test(id))).toBe(true);
			await dlq.clear();
			expect(await scopeCounts()).toEqual([]);
		} finally {
			for (const item of stores.splice(0)) item.destroy();
			await strict.end();
		}
	});

	it('refuses readiness when a scope-count trigger is disabled', async () => {
		const dlq = store(client, { cleanupInterval: 0 });
		await dlq.ready();
		await client.query(`ALTER TABLE ${table} DISABLE TRIGGER ${table}_scopes_insert`);
		await expect(validateSchema(client, { store: 'dead-letter', table })).rejects.toThrow(`missing or incompatible trigger ${table}_scopes_insert`);
	});
});
