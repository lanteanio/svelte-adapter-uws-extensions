import { afterEach, beforeEach, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createPgClient, migrate, postgresReadiness, validateSchema } from '../../../src/postgres/index.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createJobQueue } from '../../../src/postgres/jobs.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { getRuntimeEnv, resetRuntimeEnv, setRuntimeEnv } from '../../../src/shared/runtime.js';

let admin, deployment, runtime, schema, owner, role;
const stores = [];
const definitions = ['replay', 'jobs', 'tasks', 'idempotency', 'alarm-store', 'dead-letter'];
const tables = ['svti_replay', 'svti_replay_seq', 'svti_jobs', 'svti_tasks', 'svti_idempotency', 'svti_alarms', 'svti_dead_letter'];
const connect = (name, options = {}) => {
	const url = new URL(process.env.INTEGRATION_POSTGRES_URL);
	if (name) { url.username = name; url.password = 'schema-role-fixture'; }
	return createPgClient({ connectionString: url.href, autoShutdown: false, options });
};
beforeEach(async () => {
	const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
	schema = 'runtime_schema_' + suffix; owner = 'schema_owner_' + suffix; role = 'schema_runtime_' + suffix;
	admin = connect();
	for (const name of [owner, role]) await admin.query(`CREATE ROLE ${name} LOGIN PASSWORD 'schema-role-fixture' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`);
	await admin.query(`CREATE SCHEMA ${schema} AUTHORIZATION ${owner}`);
	deployment = connect(owner, { options: `-c search_path=${schema}` });
	for (const store of definitions) await migrate(deployment, { store });
	await deployment.query(`GRANT USAGE ON SCHEMA ${schema} TO ${role}`);
	await deployment.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ${tables.join(', ')} TO ${role}`);
	await deployment.query(`GRANT SELECT, INSERT, UPDATE ON svti_alarms_generations TO ${role}`);
	await deployment.query(`GRANT SELECT ON svti_schema_migrations TO ${role}`);
	await deployment.query(`GRANT USAGE ON ALL SEQUENCES IN SCHEMA ${schema} TO ${role}`);
	runtime = connect(role, { options: `-c search_path=${schema}` });
});
afterEach(async () => {
	for (const store of stores.splice(0)) await store.destroy?.();
	resetRuntimeEnv();
	await runtime?.end(); await deployment?.end();
	if (admin) {
		await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		for (const name of [role, owner]) { await admin.query(`DROP OWNED BY ${name}`); await admin.query(`DROP ROLE ${name}`); }
		await admin.end();
	}
});

it.each([['replay', 'svti_replay'], ['dead-letter', 'svti_dead_letter']])('rejects missing UPDATE needed by %s retention before readiness', async (store, table) => {
	await deployment.query(`REVOKE UPDATE ON ${table} FROM ${role}`);
	await expect(validateSchema(runtime, { store })).rejects.toThrow(`missing UPDATE privilege on ${table}`);
	await deployment.query(`GRANT UPDATE ON ${table} TO ${role}`);
	await validateSchema(runtime, { store });
});

it('runs all six stores and maintenance using a real non-owner login with only declared grants', async () => {
	const timers = new Map(), intervals = new Map();
	const native = getRuntimeEnv().timers;
	setRuntimeEnv({ rng: { float: () => 1 }, timers: {
		set(cb, ms) {
			if (![1011, 1012, 1013].includes(ms)) return native.set(cb, ms);
			const handle = { cb, ms }; timers.set(ms, cb); return handle;
		}, clear(handle) { if (!handle?.cb) native.clear(handle); },
		setInterval(cb, ms) { intervals.set(ms, cb); return { unref() {} }; }, clearInterval() {}
	} });
	const replay = createReplay(runtime, { autoMigrate: false, cleanupInterval: 1011, size: 2, ttl: 60 });
	const replayCleanup = timers.get(1011);
	const jobs = createJobQueue(runtime, { autoMigrate: false });
	const tasks = createTaskRunner(runtime, { autoMigrate: false, cleanupInterval: 1013, rowTtl: 1, recoveryInterval: 2011, dispatchInterval: 2012 });
	const taskCleanup = timers.get(1013);
	const idempotency = createIdempotencyStore(runtime, { autoMigrate: false, cleanupInterval: 1012 });
	const idempotencyCleanup = timers.get(1012);
	const alarms = createAlarmStore(runtime, { autoMigrate: false });
	const deadLetter = createDeadLetter(runtime, { autoMigrate: false, cleanupInterval: 0, max: 2, ttlMs: 60000 });
	stores.push(replay, jobs, tasks, idempotency, alarms, deadLetter);
	await postgresReadiness(runtime, stores);
	const identity = (await runtime.query('SELECT current_user AS name, session_user AS login, has_schema_privilege($1, $2) AS ddl', [schema, 'CREATE'])).rows[0];
	expect(identity).toEqual({ name: role, login: role, ddl: false });
	await expect(runtime.query('CREATE TABLE forbidden (id int)')).rejects.toMatchObject({ code: '42501' });
	await expect(runtime.query('UPDATE svti_schema_migrations SET checksum = checksum')).rejects.toMatchObject({ code: '42501' });

	const platform = { publish() {} };
	await replay.publish(platform, 'room', 'event', { kept: true });
	expect(await replay.since('room', 0)).toHaveLength(1);
	await runtime.query("UPDATE svti_replay SET created_at = now() - interval '2 minutes'");
	await replayCleanup();
	expect((await runtime.query('SELECT count(*)::int AS count FROM svti_replay')).rows[0].count).toBe(0);

	const jobId = await jobs.enqueue('work', { kept: true });
	expect(await jobs.pending('work')).toBe(1);
	expect(await jobs.claim('work')).toMatchObject([{ id: jobId, attempts: 1 }]);
	await jobs.extend(jobId, 1000); await jobs.fail(jobId);
	expect(await jobs.claim('work')).toMatchObject([{ id: jobId, attempts: 2 }]);
	await runtime.query("UPDATE svti_jobs SET claimed_at = now() - interval '2 minutes', claimed_until = now() - interval '1 minute'");
	expect(await jobs.claim('work')).toMatchObject([{ id: jobId, attempts: 3 }]);
	await jobs.complete(jobId);
	expect(await jobs.pending('work')).toBe(0);

	tasks.register('work', async context => context.input);
	expect(await tasks.run('work', { input: 'inline' })).toBe('inline');
	const pending = await tasks.enqueue('work', { input: 'dispatch' });
	await intervals.get(2012)();
	expect(await tasks.await(pending, { pollInterval: 1, timeout: 5000 })).toBe('dispatch');
	const orphan = await tasks.enqueue('work', { input: 'recovery' });
	await runtime.query("UPDATE svti_tasks SET status = 'running', fence_expires_at = now() - interval '1 minute' WHERE svti_tasks_id = $1", [orphan]);
	await intervals.get(2011)();
	expect(await tasks.await(orphan, { pollInterval: 1, timeout: 5000 })).toBe('recovery');
	await runtime.query("UPDATE svti_tasks SET updated_at = now() - interval '1 hour'");
	await taskCleanup();
	expect((await runtime.query('SELECT count(*)::int AS count FROM svti_tasks')).rows[0].count).toBe(0);

	const slot = await idempotency.acquire('result'); await slot.commit({ kept: true });
	expect(await idempotency.acquire('result')).toMatchObject({ acquired: false, result: { kept: true } });
	const aborted = await idempotency.acquire('aborted'); await aborted.abort();
	await runtime.query("UPDATE svti_idempotency SET expires_at = now() - interval '1 minute'");
	await idempotencyCleanup();
	expect((await runtime.query('SELECT count(*)::int AS count FROM svti_idempotency')).rows[0].count).toBe(0);

	const generation = await alarms.nextGeneration('room');
	expect(await alarms.set('room', 1, { kept: true }, generation)).toBe(true);
	expect(await alarms.get('room')).toMatchObject({ generation });
	expect(await alarms.due(2)).toHaveLength(1);
	expect(await alarms.delete('room', generation)).toBe(true);
	const next = await alarms.nextGeneration('room');
	await alarms.set('room', 1, null, next); await alarms.clear();
	expect(await alarms.get('room')).toBeNull();

	let latest;
	for (let i = 0; i < 4; i++) latest = await deadLetter.add({ webhookId: 'hook', topic: 'room', event: 'event', data: i, attempts: 1, error: 'fixture' });
	expect(await deadLetter.get(latest)).toMatchObject({ data: 3 });
	expect(await deadLetter.count()).toBe(2);
	expect((await runtime.query('SELECT count(*)::int AS count FROM svti_dead_letter')).rows[0].count).toBe(2);
	await deadLetter.add({ webhookId: 'hook', topic: 'room', attempts: 1, failedAt: 1, error: 'expired' });
	expect((await runtime.query('SELECT count(*)::int AS count FROM svti_dead_letter WHERE failed_at = 1')).rows[0].count).toBe(0);
	await deadLetter.clear();
	expect(await deadLetter.count()).toBe(0);
});
