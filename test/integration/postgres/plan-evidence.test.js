import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { collectPostgresPlans } from '../../../scripts/collect-postgres-plans.js';
let client;
beforeAll(async () => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); await client.query('CREATE EXTENSION IF NOT EXISTS pg_stat_statements'); await client.query("CREATE TABLE public.plan_jobs(marker text); INSERT INTO public.plan_jobs VALUES('preserve-existing')"); });
afterAll(async () => { await client.query('DROP TABLE public.plan_jobs'); await client.end(); });
function nodes(plan) { return [plan, ...(plan.children || []).flatMap(nodes)]; }
it('captures real plans, statement I/O and churn across all durable stores in an owned schema', async () => {
	const before = (await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows;
	const report = await collectPostgresPlans({ connectionString: process.env.INTEGRATION_POSTGRES_URL, rows: 100000 });
	expect(report.server.major).toBeGreaterThanOrEqual(16);
	expect(report.plans.length).toBeGreaterThan(30);
	for (const store of ['replay', 'jobs', 'tasks', 'idempotency', 'alarms', 'deadLetter'])
		expect(report.plans.some(p => p.operation.startsWith(store + '.'))).toBe(true);
	for (const operation of ['replay.cleanup', 'replay.cleanup.size', 'idempotency.cleanup', 'tasks.cleanup'])
		expect(report.plans.some(p => p.operation === operation)).toBe(true);
	for (const operation of ['replay.since.cold', 'idempotency.hit', 'alarms.due']) {
		const statements = report.plans.filter(p => p.operation === operation);
		expect(statements.length).toBeGreaterThan(0);
		expect(statements.some(p => nodes(p.plan).some(n => n.index)), operation).toBe(true);
	}
	for (const operation of ['jobs.claim.hot', 'jobs.claim.cold', 'tasks.claim', 'tasks.reclaim', 'replay.cleanup', 'replay.cleanup.size', 'idempotency.cleanup', 'tasks.cleanup']) {
		const statements = report.plans.filter(p => p.operation === operation);
		expect(statements.length, operation).toBeGreaterThan(0);
		for (const statement of statements) {
			const scans = nodes(statement.plan).filter(n => n.relation && n.node.includes('Scan'));
			for (const scan of scans) {
				const visited = ((scan.actualRows || 0) + (scan.filteredRows || 0)) * (scan.loops || 1);
				expect(visited, operation + ': ' + JSON.stringify(scan)).toBeLessThan(5000);
			}
			expect(nodes(statement.plan).every(n => !(n.tempReads || n.tempWrites)), operation).toBe(true);
		}
	}
	expect(report.statementStats.available).toBe(true);
	expect(report.statementStats.records.length).toBeGreaterThan(0);
	expect(report.statementStats.records.some(r => r.calls > 0 && r.shared_blks_hit > 0)).toBe(true);
	expect((await client.query('SELECT marker FROM public.plan_jobs')).rows).toEqual([{marker:'preserve-existing'}]);
	expect(report.storage).toHaveLength(8);
	expect(report.sourceSha256['replay.js']).toMatch(/^[a-f0-9]{64}$/);
	expect(report.collectorSha256).toMatch(/^[a-f0-9]{64}$/);
	expect(report.lockfileSha256).toMatch(/^[a-f0-9]{64}$/);
	expect(report.vacuum).toHaveLength(8);
	expect(report.afterVacuum).toHaveLength(8);
	expect(report.afterVacuum.map(row => row.relname)).toEqual(report.tableHealth.map(row => row.relname));
	for (const row of report.afterVacuum) {
		expect(Number.isFinite(Number(row.n_dead_tup))).toBe(true);
		expect(Number(row.n_dead_tup)).toBeGreaterThanOrEqual(0);
	}
	expect(report.vacuum.every(row => Number.isFinite(row.elapsedMs) && row.elapsedMs >= 0)).toBe(true);
	expect(JSON.stringify(report)).not.toContain(process.env.INTEGRATION_POSTGRES_URL);
	const after = (await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows;
	expect(after).toEqual(before);
	const output = resolve('source/postgres-plan-evidence.json');
	mkdirSync(resolve('source'), { recursive: true });
	writeFileSync(output, JSON.stringify(report, null, 2) + '\n');
}, 60000);

it('removes only its owned schema after a native DDL failure', async () => {
	const before=(await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows;
	await client.query("CREATE FUNCTION public.reject_probe_index() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture DDL denied'; END $$");
	await client.query("CREATE EVENT TRIGGER reject_probe_index ON ddl_command_start WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION public.reject_probe_index()");
	try {
		await expect(collectPostgresPlans({connectionString:process.env.INTEGRATION_POSTGRES_URL,rows:1000})).rejects.toThrow('fixture DDL denied');
		expect((await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows).toEqual(before);
		expect((await client.query('SELECT marker FROM public.plan_jobs')).rows).toEqual([{marker:'preserve-existing'}]);
	} finally {
		await client.query('DROP EVENT TRIGGER reject_probe_index');
		await client.query('DROP FUNCTION public.reject_probe_index()');
	}
});

it('keeps URL session options from redirecting fixture writes into an existing schema', async () => {
	const url = new URL(process.env.INTEGRATION_POSTGRES_URL);
	url.searchParams.set('options', '-c search_path=public -c statement_timeout=0');
	url.searchParams.set('statement_timeout', '0');
	url.searchParams.set('query_timeout', '0');
	const before = (await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows;
	const report = await collectPostgresPlans({ connectionString: url.href, rows: 1000 });
	expect(report.plans.length).toBeGreaterThan(30);
	expect((await client.query('SELECT marker FROM public.plan_jobs')).rows).toEqual([{marker:'preserve-existing'}]);
	expect((await client.query("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'plan_probe_%' ORDER BY nspname")).rows).toEqual(before);
}, 60000);

it('requires statement statistics by default and labels an explicit opt-out', async () => {
	await client.query('DROP EXTENSION pg_stat_statements');
	try {
		await expect(collectPostgresPlans({connectionString:process.env.INTEGRATION_POSTGRES_URL,rows:1000})).rejects.toThrow('Enable shared_preload_libraries');
		const report=await collectPostgresPlans({connectionString:process.env.INTEGRATION_POSTGRES_URL,rows:1000,requireStatementStats:false});
		expect(report.statementStats).toEqual({available:false,reason:'extension_unavailable'});
	} finally {
		await client.query('CREATE EXTENSION pg_stat_statements');
	}
});
