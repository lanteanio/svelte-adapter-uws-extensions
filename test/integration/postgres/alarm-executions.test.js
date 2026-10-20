import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createPgClient, migrate, postgresMigrations } from '../../../src/postgres/index.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';
import { alarmExecutionContract } from '../../helpers/alarm-execution-contract.js';

const schema = 'alarm_executions_it';
let admin, a, b, first, second;
const connect = () => createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, options: { options: `-c search_path=${schema}` } });
beforeEach(async () => {
	admin = connect();
	await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
	await admin.query(`CREATE SCHEMA ${schema}`);
	a = connect(); b = connect();
	first = createAlarmStore(a); second = createAlarmStore(b);
	await Promise.all([first.ready(), second.ready()]);
});
afterEach(async () => {
	await Promise.all([a?.end(), b?.end()]);
	await admin?.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
	await admin?.end();
});
async function schedule(store, topic, at = 1, meta = null) {
	const generation = await store.nextGeneration(topic);
	expect(await store.set(topic, at, meta, generation)).toBe(true);
	return generation;
}

describe('PostgreSQL retained alarm executions', () => {
	alarmExecutionContract({
		stores: () => [first, second],
		restart: async () => {
			const store = createAlarmStore(connect());
			await store.ready();
			return store;
		},
		fill: async count => {
			await admin.query(`INSERT INTO svti_alarms_executions (topic, generation, fire_at, meta, tenant, attempt, status, claimed_at)
				SELECT 'fill:' || lpad(n::text, 6, '0'), 1, 1, NULL, NULL, 1, 'claimed', 1 FROM generate_series(1, $1::int - (SELECT count(*)::int FROM svti_alarms_executions)) n`, [count]);
		}
	});

	it('racing claims on independent pools retain exactly one attempt', async () => {
		for (let round = 0; round < 25; round++) {
			const topic = 'race:' + round;
			const generation = await schedule(first, topic);
			const results = await Promise.all([first.claim(topic, generation), second.claim(topic, generation), first.claim(topic, generation)]);
			expect(results.filter(Boolean)).toEqual([{ attempt: 1 }]);
		}
		expect((await admin.query('SELECT count(*)::int AS count FROM svti_alarms_executions')).rows[0].count).toBe(25);
	});

	it('an abandoned requeue racing the late completion of its old worker resolves to one outcome', async () => {
		for (let round = 0; round < 20; round++) {
			const topic = 'late:' + round;
			const generation = await schedule(first, topic);
			await first.claim(topic, generation);
			const [completed, requeued] = await Promise.all([first.complete(topic, generation, 1), second.requeue(topic, generation, 1, { at: 5, allowAbandoned: true })]);
			expect(completed !== requeued).toBe(true);
			expect(Boolean(await first.get(topic))).toBe(requeued);
			expect(await first.getExecution(topic, generation)).toBeNull();
		}
	});

	it('requeue racing a newer reservation either retries or refuses, never both', async () => {
		for (let round = 0; round < 20; round++) {
			const topic = 'fence:' + round;
			const generation = await schedule(first, topic);
			await first.claim(topic, generation);
			await first.fail(topic, generation, 1, { errorClass: 'handler', failedAt: 1 });
			const [requeued, reserved] = await Promise.all([second.requeue(topic, generation, 1, { at: 5 }), first.nextGeneration(topic)]);
			expect(await first.set(topic, 9, null, reserved)).toBe(true);
			const execution = await first.getExecution(topic, generation);
			if (requeued) expect(execution).toBeNull();
			else expect(execution).toMatchObject({ status: 'failed', attempt: 1 });
			expect(await first.get(topic)).toMatchObject({ at: 9, generation: reserved });
		}
	});

	it('database constraints reject impossible execution records', async () => {
		const insert = (status, failedAt, errorClass, attempt = 1) => admin.query(`INSERT INTO svti_alarms_executions (topic, generation, fire_at, attempt, status, claimed_at, failed_at, error_class) VALUES ('x', 1, 1, $1, $2, 1, $3, $4)`, [attempt, status, failedAt, errorClass]);
		await expect(insert('claimed', 1, null)).rejects.toMatchObject({ code: '23514' });
		await expect(insert('failed', null, 'handler')).rejects.toMatchObject({ code: '23514' });
		await expect(insert('failed', 1, 'Error: secret')).rejects.toMatchObject({ code: '23514' });
		await expect(insert('running', null, null)).rejects.toMatchObject({ code: '23514' });
		await expect(insert('claimed', null, null, 0)).rejects.toMatchObject({ code: '23514' });
		await expect(insert('claimed', null, null, '9007199254740992')).rejects.toMatchObject({ code: '23514' });
		await expect(admin.query("INSERT INTO svti_alarms (topic, fire_at, generation, attempt) VALUES ('y', 1, 1, 0)")).rejects.toMatchObject({ code: '23514' });
	});

	it('version 4 upgrades a version 3 store, keeping its schedules claimable at attempt one', async () => {
		await admin.query(`DROP SCHEMA ${schema} CASCADE`);
		await admin.query(`CREATE SCHEMA ${schema}`);
		const versions = postgresMigrations({ store: 'alarm-store' });
		expect(versions.map(version => version.version)).toEqual([1, 2, 3, 4]);
		const owner = connect();
		try {
			await owner.query('CREATE TABLE svti_schema_migrations (table_name TEXT NOT NULL, store TEXT NOT NULL, version INTEGER NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (table_name, version))');
			for (const version of versions.slice(0, 3)) {
				for (const statement of version.statements) await owner.query(statement);
				await owner.query("INSERT INTO svti_schema_migrations (table_name, store, version, checksum) VALUES ('svti_alarms', 'alarm-store', $1, $2)", [version.version, version.checksum]);
			}
			await owner.query("INSERT INTO svti_alarms_generations (topic, reserved, committed) VALUES ('kept', 3, 3)");
			await owner.query(`INSERT INTO svti_alarms (topic, fire_at, meta, tenant, generation) VALUES ('kept', 77, '{"alarmId":"k"}', NULL, 3)`);
			expect((await owner.query("SELECT to_regclass('svti_alarms_executions') AS present")).rows[0].present).toBeNull();
			await migrate(owner, { store: 'alarm-store' });
			const store = createAlarmStore(owner);
			await store.ready();
			expect(await store.get('kept')).toEqual({ topic: 'kept', at: 77, meta: { alarmId: 'k' }, generation: '3' });
			expect(await store.claim('kept', '3')).toEqual({ attempt: 1 });
			expect((await owner.query("SELECT version FROM svti_schema_migrations WHERE table_name = 'svti_alarms' ORDER BY version")).rows.map(row => row.version)).toEqual([1, 2, 3, 4]);
		} finally {
			await owner.end();
		}
	});
});
