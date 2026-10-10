import { createHash, randomUUID } from 'node:crypto';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import { types } from 'pg';
import { createPgClient } from '../src/postgres/index.js';
import { createReplay } from '../src/postgres/replay.js';
import { createJobQueue } from '../src/postgres/jobs.js';
import { createTaskRunner } from '../src/postgres/tasks.js';
import { createTaskSql } from '../src/postgres/_tasks-sql.js';
import { createIdempotencyStore } from '../src/postgres/idempotency.js';
import { createAlarmStore } from '../src/postgres/alarm-store.js';
import { createDeadLetter } from '../src/postgres/dead-letter.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../src/shared/runtime.js';
import { verifyPostgresVersion } from './check-postgres-support.js';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const tables = ['plan_replay', 'plan_replay_seq', 'plan_jobs', 'plan_tasks', 'plan_idempotency', 'plan_alarms', 'plan_dead_letter'];
const digest = value => createHash('sha256').update(value).digest('hex');
const sqlText = value => String(value).trim().replace(/\s+/g, ' ');
const numeric = value => value === undefined ? undefined : Number(value);
export function normalizePlan(node) {
	if (!node || typeof node['Node Type'] !== 'string')
		throw new Error('Missing PostgreSQL plan node');
	const result = { node: node['Node Type'] };
	for (const [source, target] of [['Relation Name', 'relation'], ['Index Name', 'index'], ['Join Type', 'join'], ['Strategy', 'strategy'], ['Scan Direction', 'direction']]) {
		if (node[source] !== undefined)
			result[target] = node[source];
	}
	for (const [source, target] of [['Plan Rows', 'estimatedRows'], ['Actual Rows', 'actualRows'], ['Actual Loops', 'loops'], ['Rows Removed by Filter', 'filteredRows'], ['Shared Hit Blocks', 'sharedHits'], ['Shared Read Blocks', 'sharedReads'], ['Temp Read Blocks', 'tempReads'], ['Temp Written Blocks', 'tempWrites'], ['WAL Records', 'walRecords'], ['WAL Bytes', 'walBytes']]) {
		if (node[source] !== undefined)
			result[target] = numeric(node[source]);
	}
	if (Array.isArray(node.Plans))
		result.children = node.Plans.map(normalizePlan);
	return result;
}
async function seed(client, rows) {
	await client.query(`INSERT INTO plan_replay(topic,seq,event,data,created_at)
		SELECT CASE WHEN i % 10 = 0 THEN 'cold-'||(i%100) ELSE 'hot' END,i,'event',jsonb_build_object('value',i),now()-interval '1 second'*(i%7200) FROM generate_series(1,$1::int) i`, [rows]);
	await client.query('INSERT INTO plan_replay_seq(topic,seq) SELECT topic,max(seq) FROM plan_replay GROUP BY topic ON CONFLICT(topic) DO UPDATE SET seq=excluded.seq');
	await client.query(`INSERT INTO plan_jobs(queue,payload,claimed_at,claimed_until,created_at)
		SELECT CASE WHEN i%20=0 THEN 'cold' ELSE 'hot' END,jsonb_build_object('value',i),CASE WHEN i%3=0 THEN now()-interval '2 minutes' END,CASE WHEN i%3=0 THEN now()-interval '1 minute' END,now()-interval '1 second'*i FROM generate_series(1,$1::int) i`, [rows]);
	await client.query(`INSERT INTO plan_tasks(svti_tasks_id,name,input,status,fence,fence_expires_at,updated_at)
		SELECT md5('task-'||i)::uuid,CASE WHEN i%20=0 THEN 'cold' ELSE 'hot' END,jsonb_build_object('value',i),CASE (i/20)%4 WHEN 0 THEN 'running' WHEN 1 THEN 'pending' WHEN 2 THEN 'committed' ELSE 'failed' END,md5('fence-'||i)::uuid,now()-interval '1 minute',now()-interval '1 hour' FROM generate_series(1,$1::int) i`, [rows]);
	await client.query(`INSERT INTO plan_idempotency(svti_idempotency_key,status,result,expires_at)
		SELECT 'key-'||i,'committed',jsonb_build_object('value',i),now()+interval '1 second'*CASE WHEN i%5=0 THEN -3600 ELSE 3600 END FROM generate_series(1,$1::int) i`, [rows]);
	await client.query(`INSERT INTO plan_alarms(topic,fire_at,meta)
		SELECT 'alarm-'||i,(extract(epoch from now())*1000)::bigint+CASE WHEN i%20=0 THEN -1000 ELSE 3600000 END,jsonb_build_object('value',i) FROM generate_series(1,$1::int) i`, [rows]);
	await client.query(`INSERT INTO plan_dead_letter(topic,event,data,failed_at)
		SELECT CASE WHEN i%20=0 THEN 'cold' ELSE 'hot' END,'event',jsonb_build_object('value',i),(extract(epoch from now())*1000)::bigint-i*1000 FROM generate_series(1,$1::int) i`, [rows]);
	// Deterministic update churn leaves dead tuples for the before-vacuum sample.
	await client.query("UPDATE plan_jobs SET attempts=attempts+1 WHERE svti_jobs_id%3=0");
	await client.query("UPDATE plan_tasks SET attempts=attempts+1 WHERE status='running'");
	for (const table of tables)
		await client.query('ANALYZE ' + table);
}
async function statistics(client, queryIds) {
	const result = await client.query(`SELECT queryid::text,calls,total_exec_time,rows,shared_blks_hit,shared_blks_read,
		temp_blks_read,temp_blks_written,wal_bytes::text,query FROM public.pg_stat_statements
		WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND userid=(SELECT usesysid FROM pg_user WHERE usename=current_user)
		AND queryid=ANY($1::bigint[])`, [queryIds]);
	return new Map(result.rows.map(row => [row.queryid, row]));
}
async function health(client, schema) {
	await client.query('SELECT pg_stat_clear_snapshot()');
	return (await client.query(`SELECT relname,n_live_tup,n_dead_tup,n_mod_since_analyze,last_analyze::text,last_autovacuum::text FROM pg_stat_user_tables WHERE schemaname=$1 AND relname=ANY($2::text[]) ORDER BY relname`, [schema, tables])).rows;
}
export async function collectPostgresPlans({ connectionString, rows = 20000, requireStatementStats = true } = {}) {
	if (!connectionString)
		throw new Error('Set DATABASE_URL to an isolated PostgreSQL database');
	if (!Number.isInteger(rows) || rows < 1000 || rows > 250000)
		throw new Error('rows must be an integer from 1000 through 250000');
	const schema = 'plan_probe_' + randomUUID().replaceAll('-', '');
	// pg gives URL parameters precedence over separate connection options.
	// Pin isolation and deadlines in the URL so every pooled session inherits them.
	const scopedUrl = new URL(connectionString);
	scopedUrl.searchParams.set('options', '-c search_path=' + schema);
	scopedUrl.searchParams.set('statement_timeout', '5000');
	scopedUrl.searchParams.set('query_timeout', '6000');
	scopedUrl.searchParams.set('connect_timeout', '5');
	const client = createPgClient({ connectionString: scopedUrl.href, autoShutdown: false, options: { max: 2, connectionTimeoutMillis: 5000, query_timeout: 6000, statement_timeout: 5000 } });
	let created = false, label = null, runtimeOverridden = false;
	const stores = [], queries = new Map();
	const started = performance.now();
	const checkTime = () => { if (performance.now() - started > 60000)
		throw new Error('PostgreSQL plan collection exceeded its 60-second budget'); };
	const record = (input, values) => {
		const text = typeof input === 'string' ? input : input.text;
		if (label && /\bplan_(?:replay|replay_seq|jobs|tasks|idempotency|alarms|dead_letter)\b/.test(text) && /^(?:SELECT|INSERT|UPDATE|DELETE|WITH)\b/i.test(text.trim()) && !text.includes('set_config(') && !text.includes('pg_try_advisory_xact_lock')) {
			const fingerprint = digest(sqlText(text)), key = label + ':' + fingerprint;
			if (!queries.has(key)) {
				if (queries.size >= 128)
					throw new Error('PostgreSQL plan statement budget exceeded');
				queries.set(key, { operation: label, fingerprint, text, values: structuredClone(typeof input === 'string' ? values : input.values) });
			}
		}
	};
	const observed = { ...client, query(input, values) { record(input, values); return client.query(input, values); }, async connect() { const tx = await client.connect(); return { query(input, values) { record(input, values); return tx.query(input, values); }, release: error => tx.release(error), on: (...args) => tx.on(...args), removeListener: (...args) => tx.removeListener(...args) }; } };
	const capture = async (name, action) => { checkTime(); label = name; try {
		return await action();
	}
	finally {
		label = null;
	} };
	try {
		const server = await verifyPostgresVersion(client);
		const settings = (await client.query("SELECT name,setting,unit FROM pg_settings WHERE name=ANY($1::text[]) ORDER BY name", [['work_mem','shared_buffers','autovacuum','max_connections','random_page_cost','effective_cache_size','track_io_timing','pg_stat_statements.track']])).rows;
		let statsAvailable = false;
		try {
			await statistics(client, []);
			statsAvailable = true;
		}
		catch (error) {
			if (requireStatementStats)
				throw new Error('Enable shared_preload_libraries=pg_stat_statements and CREATE EXTENSION pg_stat_statements in the isolated database', { cause: error });
		}
		await client.query('CREATE SCHEMA ' + schema);
		created = true;
		const replay = createReplay(observed, { table: 'plan_replay', size: rows + 100, ttl: 60, cleanupInterval: 0 });
		stores.push(replay);
		const jobs = createJobQueue(observed, { table: 'plan_jobs' });
		const tasks = createTaskRunner(observed, { table: 'plan_tasks', dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0 });
		stores.push(tasks);
		const idem = createIdempotencyStore(observed, { table: 'plan_idempotency', cleanupInterval: 0 });
		stores.push(idem);
		const alarms = createAlarmStore(observed, { table: 'plan_alarms' });
		stores.push(alarms);
		const dlq = createDeadLetter(observed, { table: 'plan_dead_letter', max: rows + 100, ttlMs: 3600000, cleanupInterval: 0 });
		stores.push(dlq);
		await Promise.all([tasks.ready(), idem.ready(), alarms.ready(), replay.seq('initial'), jobs.pending(), dlq.count()]);
		await seed(client, rows);
		const platform = { publish: () => true, publishBatch: () => true };
		await capture('replay.publish', () => replay.publish(platform, 'hot', 'event', { value: 1 }));
		await capture('replay.publishBatch', () => replay.publishBatch(platform, [{ topic: 'hot', event: 'event', data: { value: 2 } }, { topic: 'cold-0', event: 'event', data: { value: 3 } }]));
		await capture('replay.since.hot', () => replay.since('hot', rows - 100));
		await capture('replay.since.cold', () => replay.since('cold-0', rows - 100));
		await capture('replay.seq', () => replay.seq('hot'));
		await capture('replay.gap', () => replay.gap('hot', rows - 50));
		await capture('jobs.enqueue', () => jobs.enqueue('hot', { value: 1 }));
		const claimed = await capture('jobs.claim.hot', () => jobs.claim('hot', { batchSize: 25 }));
		await capture('jobs.claim.cold', () => jobs.claim('cold', { batchSize: 25 }));
		await capture('jobs.extend', () => jobs.extend(claimed.map(row => row.id), 1000));
		await capture('jobs.fail', () => jobs.fail(claimed[0].id));
		await capture('jobs.complete', () => jobs.complete(claimed[1].id));
		await capture('jobs.pending', () => jobs.pending('cold'));
		tasks.register('hot', async () => ({ ok: true }));
		await capture('tasks.run', () => tasks.run('hot', { input: { value: 1 } }));
		await capture('tasks.enqueue', () => tasks.enqueue('hot', { input: { value: 1 } }));
		await capture('tasks.list', () => tasks.list({ name: 'cold', limit: 25 }));
		await capture('tasks.counts', () => tasks.counts());
		const sql = createTaskSql({ client: observed, table: 'plan_tasks', fenceTtl: 30, rowTtl: 60, autoMigrate: false });
		const pending = await capture('tasks.claim', () => sql.claimPending(25, ['hot']));
		await capture('tasks.reclaim', () => sql.reclaimStuck(25, ['hot']));
		if (pending.length)
			await capture('tasks.heartbeat', () => sql.heartbeatFence(pending[0].id, pending[0].fence, 1000));
		if (pending.length) {
			const row = pending[0];
			await capture('tasks.read', () => sql.readRow(row.id));
			await capture('tasks.fail', () => sql.failRow(row.id, row.fence, new Error('fixture failure')));
			await capture('tasks.rearm', () => sql.rearmAttempt(row.id, row.fence, randomUUID(), 2));
			await capture('tasks.expire', () => sql.expireFence(row.id));
		}
		await capture('idempotency.hit', () => idem.acquire('key-1'));
		const expired = await capture('idempotency.expired', () => idem.acquire('key-5'));
		await expired.abort();
		const slot = await capture('idempotency.acquire', () => idem.acquire('new-key'));
		await capture('idempotency.commit', () => slot.commit({ ok: true }));
		const abort = await idem.acquire('abort-key');
		await capture('idempotency.abort', () => abort.abort());
		await capture('alarms.set', () => alarms.set('new-alarm', Date.now() + 1000, { value: 1 }));
		await capture('alarms.due', () => alarms.due(Date.now()));
		await capture('alarms.delete', () => alarms.delete('alarm-20'));
		const id = await capture('deadLetter.add', () => dlq.add({ webhookId: 'hook', topic: 'hot', event: 'event', data: { value: 1 }, attempts: 1, error: 'timeout' }));
		await capture('deadLetter.get', () => dlq.get(id));
		await capture('deadLetter.list', () => dlq.list({ topic: 'cold', limit: 25 }));
		await capture('deadLetter.count', () => dlq.count({ topic: 'cold' }));
		await capture('deadLetter.remove', () => dlq.remove(id));
		// Trigger the actual maintenance scheduler once, without waiting for its
		// jitter timer or starting any automatic background loop in this process.
		const ticks = [];
		setRuntimeEnv({ rng: { float: () => 1 }, timers: { set: (cb, ms) => { const timer = { cb, ms }; ticks.push(timer); return timer; }, clear: () => { } } });
		runtimeOverridden = true;
		for (const [name, factory, table] of [['replay', createReplay, 'plan_replay'], ['idempotency', createIdempotencyStore, 'plan_idempotency'], ['tasks', createTaskRunner, 'plan_tasks']]) {
			const offset = ticks.length;
			const store = factory(observed, { table, cleanupInterval: 1000, autoMigrate: false, size: 100, ttl: 60, rowTtl: 60, dispatchInterval: 0, recoveryInterval: 0 });
			stores.push(store);
			await capture(name + '.cleanup', () => ticks[offset].cb());
			if (name === 'replay')
				await capture(name + '.cleanup.size', () => ticks[offset].cb());
			store.destroy();
		}
		resetRuntimeEnv();
		runtimeOverridden = false;
		// Restore the same skew/churn cardinality before EXPLAIN so an earlier
		// captured cleanup does not shrink the fixture seen by a later plan.
		await client.query('TRUNCATE ' + tables.join(',') + ' RESTART IDENTITY');
		await seed(client, rows);
		const plans = [], statementIds = new Map();
		for (const statement of queries.values()) {
			checkTime();
			const tx = await client.connect();
			let releaseError;
			try {
				await tx.query('BEGIN');
				// Preserve the signed 64-bit query identifier before JSON parsing.
				const result = await tx.query({ text: 'EXPLAIN (ANALYZE, BUFFERS, WAL, VERBOSE, FORMAT JSON, TIMING OFF) ' + statement.text, values: statement.values, types: { getTypeParser(oid, format) { return oid === 114 ? value => value : types.getTypeParser(oid, format); } } });
				const raw = result.rows[0]['QUERY PLAN'];
				const queryId = /"Query Identifier":\s*(-?\d+)/.exec(raw)?.[1];
				if (queryId)
					statementIds.set(queryId, statement.fingerprint);
				const plan = JSON.parse(raw)[0];
				plans.push({ operation: statement.operation, sqlSha256: statement.fingerprint, planningMs: plan['Planning Time'], executionMs: plan['Execution Time'], plan: normalizePlan(plan.Plan) });
			}
			finally {
				try {
					await tx.query('ROLLBACK');
				}
				catch (error) {
					releaseError = error;
					throw error;
				}
				finally {
					tx.release(releaseError);
				}
			}
		}
		const tableHealth = await health(client, schema), vacuum = [];
		for (const table of tables) {
			checkTime();
			const start = performance.now();
			await client.query('VACUUM (ANALYZE) ' + table);
			vacuum.push({ table, elapsedMs: performance.now() - start });
		}
		const afterVacuum = await health(client, schema);
		const storage = (await client.query(`SELECT c.relname,pg_total_relation_size(c.oid)::text total_bytes,age(c.relfrozenxid) xid_age FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind='r' AND c.relname=ANY($2::text[]) ORDER BY c.relname`, [schema, tables])).rows;
		let statementStats = { available: false, reason: 'extension_unavailable' };
		if (statsAvailable) {
			const after = await statistics(client, [...statementIds.keys()]), records = [];
			for (const [queryId, row] of after) {
				const item = { sqlSha256: statementIds.get(queryId), calls: Number(row.calls) };
				for (const key of ['total_exec_time', 'rows', 'shared_blks_hit', 'shared_blks_read', 'temp_blks_read', 'temp_blks_written', 'wal_bytes'])
					item[key] = Number(row[key]);
				records.push(item);
			}
			if (requireStatementStats && records.length === 0) throw new Error('No fixture statement statistics were recorded');
			statementStats = { available: true, scope: 'statements referencing the newly created fixture relations', records };
		}
		const sourceSha256 = {};
		for (const file of ['replay.js', 'jobs.js', 'tasks.js', '_tasks-sql.js', 'idempotency.js', 'alarm-store.js', 'dead-letter.js', '_cleanup.js'])
			sourceSha256[file] = digest(readFileSync(resolve(root, 'src/postgres', file)));
		return { schemaVersion: 1, packageVersion: JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8')).version, collectorSha256: digest(readFileSync(fileURLToPath(import.meta.url))), lockfileSha256: digest(readFileSync(resolve(root, 'package-lock.json'))), server, settings, fixture: { rowsPerStore: rows, hotPercent: { replay: 90, jobs: 95, tasks: 95, deadLetter: 95 }, churn: 'every third job and each running task', scope: 'isolated synthetic schema; not production capacity qualification' }, sourceSha256, plans, statementStats, tableHealth, afterVacuum, vacuum, storage, elapsedMs: performance.now() - started };
	}
	finally {
		if (runtimeOverridden)
			resetRuntimeEnv();
		try {
			const cleanup = await Promise.allSettled(stores.map(async store => store.destroy?.()));
			const failed = cleanup.find(result => result.status === 'rejected');
			if (failed) throw failed.reason;
		} finally {
			try {
				if (created) await client.query('DROP SCHEMA ' + schema + ' CASCADE');
			} finally {
				await client.end();
			}
		}
	}
}
async function main() {
	const report = await collectPostgresPlans({ connectionString: process.env.DATABASE_URL, rows: Number(process.env.PG_PLAN_ROWS || 20000), requireStatementStats: process.env.PG_PLAN_REQUIRE_STATS !== '0' });
	const output = resolve(process.env.PG_PLAN_OUTPUT || 'source/postgres-plans-' + Date.now() + '.json');
	mkdirSync(dirname(output), { recursive: true });
	writeFileSync(output, JSON.stringify(report, null, 2) + '\n', { flag: 'wx' });
	console.log('PostgreSQL plans: ' + report.plans.length + ' statements; evidence saved to ' + output);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
	main().catch(() => { console.error('PostgreSQL plan collection failed; verify the isolated database configuration and collection limits.'); process.exitCode = 1; });
