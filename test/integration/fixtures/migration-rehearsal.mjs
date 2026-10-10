import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';
import { createJobQueue } from 'svelte-adapter-uws-extensions/postgres/jobs';
import { createTaskRunner } from 'svelte-adapter-uws-extensions/postgres/tasks';

const fixture = JSON.parse(readFileSync(new URL('./migration-0.5.10.json', import.meta.url), 'utf8'));
const migration = readFileSync(new URL('./MIGRATION.md', import.meta.url), 'utf8');
const manual = migration.match(/```sql\n(ALTER TABLE svti_jobs[\s\S]+?)\n```/)?.[1];
assert.ok(manual, 'The manual migration must come from the shipped guide');
const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
const traceContext = { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01', tracestate: 'vendor=value' };
try {
	for (const autoMigrate of [true, false]) {
		const suffix = autoMigrate ? 'automatic' : 'manual';
		const jobsTable = `migration_jobs_${suffix}`;
		const tasksTable = `migration_tasks_${suffix}`;
		let runner;
		try {
			await client.query(fixture.jobs.replaceAll('${table}', jobsTable));
			await client.query(fixture.tasks.replaceAll('${table}', tasksTable));
			await client.query(`INSERT INTO ${jobsTable} (queue, payload, request_id) VALUES ('legacy', '{"kept":true}', 'old-request')`);
			await client.query(`INSERT INTO ${tasksTable} (svti_tasks_id, name, input, status, result, fence, fence_expires_at)
				VALUES ('00000000-0000-4000-8000-000000000001', 'legacy', '{"kept":true}', 'committed', '{"old":true}',
				'00000000-0000-4000-8000-000000000002', now())`);
			if (!autoMigrate) {
				const sql = manual.replaceAll('svti_jobs', jobsTable).replaceAll('svti_tasks', tasksTable);
				await client.query(sql);
				await client.query(sql); // A retry must be safe.
			}
			const jobs = createJobQueue(client, { table: jobsTable, autoMigrate });
			runner = createTaskRunner(client, { table: tasksTable, autoMigrate, dispatchInterval: 0, recoveryInterval: 0 });
			const newJob = await jobs.enqueue('new', { fresh: true }, { traceContext });
			const newTask = await runner.enqueue('new', { input: { fresh: true }, traceContext });
			assert.deepEqual(await runner.await('00000000-0000-4000-8000-000000000001'), { old: true });
			const [oldJob] = await jobs.claim('legacy');
			assert.deepEqual(oldJob.payload, { kept: true });
			assert.equal(oldJob.requestId, 'old-request');
			assert.equal(oldJob.traceContext, null);
			const [tracedJob] = await jobs.claim('new');
			assert.equal(String(tracedJob.id), String(newJob));
			assert.deepEqual(tracedJob.traceContext, traceContext);
			const tasks = await client.query(`SELECT input, result, traceparent, tracestate FROM ${tasksTable} WHERE svti_tasks_id = $1`, [newTask]);
			assert.deepEqual(tasks.rows[0].input, { fresh: true });
			assert.equal(tasks.rows[0].traceparent, traceContext.traceparent);
			assert.equal(tasks.rows[0].tracestate, traceContext.tracestate);
			// Old column-only readers still work without dropping additive columns.
			const legacy = await client.query(`SELECT input, result FROM ${tasksTable} WHERE name = 'legacy'`);
			assert.deepEqual(legacy.rows, [{ input: { kept: true }, result: { old: true } }]);
			const oldColumns = await client.query(`SELECT payload, request_id FROM ${jobsTable} WHERE queue = 'legacy'`);
			assert.deepEqual(oldColumns.rows, [{ payload: { kept: true }, request_id: 'old-request' }]);
			console.log(`packed migration ${suffix}: preserved legacy rows and trace round-trip`);
		} finally {
			runner?.destroy();
			await client.query(`DROP TABLE IF EXISTS ${jobsTable}, ${tasksTable}`);
		}
	}
} finally {
	await client.end();
}
