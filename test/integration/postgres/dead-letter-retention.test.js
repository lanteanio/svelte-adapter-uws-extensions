import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { createMetrics } from '../../../src/prometheus/index.js';
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
async function seed(count, expired = false) {
	await client.query(`INSERT INTO ${table} (topic, failed_at)
		SELECT 'retained', (extract(epoch from now()) * 1000)::bigint - $2 FROM generate_series(1, $1::int)`, [count, expired ? 3600000 : 0]);
}
beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
afterEach(async () => {
	for (const item of stores.splice(0)) item.destroy();
	resetRuntimeEnv();
	await client.query(`DROP TABLE IF EXISTS ${table}`);
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
						if (sql.text?.trim().startsWith('WITH excess AS') && hold) {
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
});
