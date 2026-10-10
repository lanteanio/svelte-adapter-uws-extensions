import { afterAll, afterEach, beforeAll, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { runCleanupPass } from '../../../src/postgres/_cleanup.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../../src/shared/runtime.js';
let client;
const tables = ['svti_retention_cache', 'svti_retention_tasks', 'svti_retention_replay', 'svti_retention_replay_seq'];
beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
afterEach(async () => { resetRuntimeEnv(); for (const table of tables) await client.query('DROP TABLE IF EXISTS ' + table); });
afterAll(async () => { await client.end(); });
function timers() {
	const ticks = [];
	setRuntimeEnv({ rng: { float: () => 1 }, timers: { set(cb, ms) { const handle = { cb, ms }; ticks.push(handle); return handle; }, clear() {} } });
	return ticks;
}
function metric(metrics, store, rows) { expect(metrics.serialize()).toContain(`postgres_cleanup_rows_total{store="${store}"} ${rows}`); }

it('two store replicas skip the held table lock, then drain only 1,000 expired rows per pass', async () => {
	const ticks = timers(), metrics = createMetrics();
	const a = createIdempotencyStore(client, { table: tables[0], cleanupInterval: 1000, metrics });
	await a.ready();
	const peer = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	const b = createIdempotencyStore(peer, { table: tables[0], cleanupInterval: 1000, metrics });
	let holder;
	try {
		await Promise.all([a.ready(), b.ready()]);
		await client.query(`INSERT INTO ${tables[0]} (svti_idempotency_key,status,expires_at) SELECT 'expired-'||i,'committed',now()-interval '1 hour' FROM generate_series(1,1500) i`);
		await client.query(`INSERT INTO ${tables[0]} (svti_idempotency_key,status,expires_at) VALUES ('live','pending',now()+interval '1 hour')`);
		holder = await client.connect(); await holder.query('BEGIN');
		await holder.query('SELECT pg_advisory_xact_lock(1937134697,$1::regclass::oid::integer)', [tables[0]]);
		await Promise.all([ticks[0].cb(), ticks[1].cb()]);
		expect(Number((await client.query(`SELECT count(*) n FROM ${tables[0]}`)).rows[0].n)).toBe(1501);
		expect(metrics.serialize()).toContain('postgres_cleanup_runs_total{result="contended",store="idempotency"} 2');
		await holder.query('COMMIT'); holder.release(); holder = null;
		await ticks[0].cb(); expect(Number((await client.query(`SELECT count(*) n FROM ${tables[0]}`)).rows[0].n)).toBe(501); metric(metrics,'idempotency',1000);
		await ticks[1].cb(); expect((await client.query(`SELECT svti_idempotency_key FROM ${tables[0]}`)).rows).toEqual([{ svti_idempotency_key: 'live' }]); metric(metrics,'idempotency',1500);
	} finally { a.destroy(); b.destroy(); if (holder) { await holder.query('ROLLBACK'); holder.release(); } await peer.end(); }
});

it('task cleanup preserves pending/running rows and drains terminal rows over bounded passes', async () => {
	const ticks = timers(), metrics = createMetrics();
	const runner = createTaskRunner(client, { table: tables[1], recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 1000, rowTtl: 1, metrics });
	try {
		await runner.ready();
		await client.query(`INSERT INTO ${tables[1]} (svti_tasks_id,name,status,fence,fence_expires_at,updated_at) SELECT gen_random_uuid(),'work','committed',gen_random_uuid(),now(),now()-interval '1 hour' FROM generate_series(1,1100)`);
		await runner.enqueue('work', { input: 'pending' });
		await client.query(`INSERT INTO ${tables[1]} (svti_tasks_id,name,status,fence,fence_expires_at,updated_at) VALUES(gen_random_uuid(),'work','running',gen_random_uuid(),now(),now()-interval '1 hour')`);
		await ticks[0].cb(); expect(await runner.counts()).toMatchObject({ committed: 100, pending: 1, running: 1 }); metric(metrics,'tasks',1000);
		await ticks[0].cb(); expect(await runner.counts()).toMatchObject({ committed: 0, pending: 1, running: 1 });
	} finally { runner.destroy(); }
});

it('replay size traversal retains newest rows and TTL cleanup uses the created-at index', async () => {
	const ticks = timers(), metrics = createMetrics();
	const replay = createReplay(client, { table: tables[2], size: 2, ttl: 60, cleanupInterval: 1000, metrics });
	try {
		await replay.seq('initial');
		await client.query(`INSERT INTO ${tables[2]} (topic,seq,event,created_at) SELECT 'active',i,'event',now() FROM generate_series(1,1202) i`);
		await client.query(`INSERT INTO ${tables[3]} (topic,seq) VALUES ('active',1202)`);
		await ticks[0].cb(); expect(Number((await client.query(`SELECT count(*) n FROM ${tables[2]}`)).rows[0].n)).toBe(202); metric(metrics,'replay',1000);
		await ticks[0].cb(); expect((await client.query(`SELECT seq::int FROM ${tables[2]} ORDER BY seq`)).rows).toEqual([{seq:1201},{seq:1202}]);
		await client.query(`UPDATE ${tables[2]} SET created_at=now()-interval '2 minutes'`);
		await ticks[0].cb(); expect(Number((await client.query(`SELECT count(*) n FROM ${tables[2]}`)).rows[0].n)).toBe(0);
		expect((await client.query('SELECT indexname FROM pg_indexes WHERE tablename=$1',[tables[2]])).rows.map(r=>r.indexname)).toContain('idx_'+tables[2]+'_created_at');
	} finally { replay.destroy(); }
});

it('a server deadline rolls back prior deletes and releases ownership for the next pass', async () => {
	await client.query(`CREATE TABLE ${tables[0]} (id int PRIMARY KEY)`); await client.query(`INSERT INTO ${tables[0]} VALUES(1)`);
	await expect(runCleanupPass(client,tables[0],async budget=> {
		await budget.remove(`DELETE FROM ${tables[0]}`); await budget.query('SELECT pg_sleep(5)');
	})).rejects.toThrow();
	expect((await client.query(`SELECT id FROM ${tables[0]}`)).rows).toEqual([{id:1}]);
	const result = await runCleanupPass(client,tables[0],async budget=> { await budget.remove(`DELETE FROM ${tables[0]}`); });
	expect(result).toEqual({result:'complete',rows:1});
});

it('a hot early replay topic cannot starve retention for later topics', async () => {
	const ticks = timers();
	const replay = createReplay(client, { table: tables[2], size: 2, cleanupInterval: 1000 });
	try {
		await replay.seq('initial');
		await client.query(`INSERT INTO ${tables[2]} (topic,seq,event) SELECT 'a',i,'event' FROM generate_series(1,2502) i`);
		await client.query(`INSERT INTO ${tables[2]} (topic,seq,event) SELECT 'z',i,'event' FROM generate_series(1,5) i`);
		await client.query(`INSERT INTO ${tables[3]} (topic,seq) VALUES ('a',2502),('z',5)`);
		await ticks[0].cb();
		expect(Number((await client.query(`SELECT count(*) n FROM ${tables[2]} WHERE topic='a'`)).rows[0].n)).toBe(1502);
		await ticks[0].cb();
		expect(Number((await client.query(`SELECT count(*) n FROM ${tables[2]} WHERE topic='a'`)).rows[0].n)).toBe(1502);
		expect((await client.query(`SELECT seq::int FROM ${tables[2]} WHERE topic='z' ORDER BY seq`)).rows).toEqual([{seq:4},{seq:5}]);
	} finally { replay.destroy(); }
});
