import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createPgClient, migrate, postgresMigrations, postgresReadiness, validateSchema } from '../../../src/postgres/index.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createJobQueue } from '../../../src/postgres/jobs.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createAlarmStore } from '../../../src/postgres/alarm-store.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';

const schema = 'schema_contracts';
let admin, client;
const stores = [];
const connect = (options = {}) => createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, options });

beforeEach(async () => {
	admin = connect();
	await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
	await admin.query(`CREATE SCHEMA ${schema}`);
	client = connect({ options: `-c search_path=${schema}` });
});
afterEach(async () => {
	for (const store of stores.splice(0)) await store.destroy?.();
	await client?.end();
	if (admin) { await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`); await admin.end(); }
});

const definitions = [
	['replay', createReplay], ['jobs', createJobQueue], ['tasks', createTaskRunner],
	['idempotency', createIdempotencyStore], ['alarm-store', createAlarmStore], ['dead-letter', createDeadLetter]
];

describe('PostgreSQL schema contracts', () => {
	it.each(definitions)('migrates %s and supports a read-only startup gate', async (store, factory) => {
		const table = `contract_${store.replace('-', '_')}`;
		await migrate(client, { store, table });
		await migrate(client, { store, table });
		const runtime = connect({ options: `-c search_path=${schema} -c default_transaction_read_only=on` });
		const component = factory(runtime, { table, autoMigrate: false, cleanupInterval: 0, recoveryInterval: 0, dispatchInterval: 0 });
		stores.push(component);
		try {
			await postgresReadiness(runtime, [component]);
			expect(component.status()).toEqual({ state: 'ready', error: null });
		} finally { await runtime.end(); }
		expect((await client.query('SELECT version FROM svti_schema_migrations WHERE table_name = $1 ORDER BY version', [table])).rows).toEqual((['alarm-store', 'tasks'].includes(store) ? [1, 2, 3] : [1, 2]).map(version => ({ version })));
	});

	it('serializes independent cold starts without duplicate ledger entries', async () => {
		await Promise.all(Array.from({ length: 4 }, () => migrate(client, { store: 'jobs' })));
		expect((await client.query('SELECT count(*)::int AS count FROM svti_schema_migrations')).rows[0].count).toBe(2);
		await validateSchema(client, { store: 'jobs' });
	});
	it('supports the longest permitted table names without truncated generated identifiers', async () => {
		for (const store of ['tasks', 'replay']) {
			const table = store + '_'.repeat(42 - store.length);
			await migrate(client, { store, table });
			await validateSchema(client, { store, table });
		}
		expect(() => postgresMigrations({ store: 'tasks', table: 'a'.repeat(43) })).toThrow('maximum is 42');
	});
	it('rejects a serial default redirected away from its owned sequence', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('CREATE SEQUENCE foreign_authority');
		await client.query("ALTER TABLE svti_jobs ALTER COLUMN svti_jobs_id SET DEFAULT nextval('foreign_authority')");
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible serial default');
		await expect(migrate(client, { store: 'jobs' })).rejects.toThrow('incompatible serial default');
	});
	it('rejects extra NOT NULL restrictions on nullable payloads', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs ALTER COLUMN payload SET NOT NULL');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible type or nullability');
		await expect(createJobQueue(client, { autoMigrate: false }).ready()).rejects.toThrow('incompatible type or nullability');
	});
	it('rejects changed numeric defaults and unexpected string defaults', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs ALTER COLUMN attempts SET DEFAULT 1');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible default');
		await migrate(client, { store: 'idempotency' });
		await client.query("ALTER TABLE svti_idempotency ALTER COLUMN status SET DEFAULT 'PENDING'");
		await expect(validateSchema(client, { store: 'idempotency' })).rejects.toThrow('incompatible default');
	});
	it.each(['', ' DEFAULT 1'])('rejects an unknown required column%s', async fallback => {
		await migrate(client, { store: 'jobs' });
		await client.query(`ALTER TABLE svti_jobs ADD COLUMN external_owner INTEGER NOT NULL${fallback}`);
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('unexpected required column');
		await expect(createJobQueue(client, { autoMigrate: false }).ready()).rejects.toThrow('unexpected required column');
	});
	it.each([
		['check', ["ALTER TABLE svti_jobs ADD CONSTRAINT external_check CHECK (queue <> 'blocked')"]],
		['unique index', ['CREATE UNIQUE INDEX external_unique ON svti_jobs (queue)']],
		['foreign-key', ['CREATE TABLE queue_owners (name TEXT PRIMARY KEY)', 'ALTER TABLE svti_jobs ADD CONSTRAINT external_fk FOREIGN KEY (queue) REFERENCES queue_owners (name)']],
		['exclusion', ["ALTER TABLE svti_jobs ADD CONSTRAINT external_exclusion EXCLUDE USING gist (tstzrange(created_at, created_at, '[]') WITH &&)"]]
	])('rejects an unknown %s restriction', async (_kind, statements) => {
		await migrate(client, { store: 'jobs' });
		for (const statement of statements) await client.query(statement);
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('unexpected');
	});
	it('allows nullable extra columns and ordinary indexes', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs ADD COLUMN operator_note TEXT');
		await client.query('CREATE INDEX external_lookup ON svti_jobs (queue)');
		await validateSchema(client, { store: 'jobs' });
	});
	it('rejects nullable extra columns with throwing defaults', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query("CREATE FUNCTION reject_insert() RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected by extra default'; END $$");
		await client.query('ALTER TABLE svti_jobs ADD COLUMN operator_note TEXT');
		await client.query('ALTER TABLE svti_jobs ALTER COLUMN operator_note SET DEFAULT reject_insert()');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('unexpected computed column');
		await expect(client.query("INSERT INTO svti_jobs (queue) VALUES ('valid')")).rejects.toMatchObject({ code: 'P0001' });
	});
	it('rejects nullable extra columns with generated expressions', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs ADD COLUMN operator_note INTEGER GENERATED ALWAYS AS (1 / attempts) STORED');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('unexpected computed column');
		await expect(client.query("INSERT INTO svti_jobs (queue) VALUES ('valid')")).rejects.toMatchObject({ code: '22012' });
	});
	it('rejects identity generation on an expected explicitly written column', async () => {
		await migrate(client, { store: 'replay' });
		await client.query('ALTER TABLE svti_replay ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY');
		await expect(validateSchema(client, { store: 'replay' })).rejects.toThrow('incompatible generated or identity column');
		await expect(client.query("INSERT INTO svti_replay (topic, seq, event) VALUES ('room', 1, 'valid')")).rejects.toMatchObject({ code: '428C9' });
	});
	it('rejects a hash index replacing the expected range index', async () => {
		await migrate(client, { store: 'alarm-store' });
		await client.query('DROP INDEX idx_svti_alarms_fire_at');
		await client.query('CREATE INDEX idx_svti_alarms_fire_at ON svti_alarms USING hash (fire_at)');
		await expect(validateSchema(client, { store: 'alarm-store' })).rejects.toThrow('incompatible index');
	});

	it('rejects invalid task, job and idempotency state written directly to PostgreSQL', async () => {
		for (const store of ['tasks', 'jobs', 'idempotency']) await migrate(client, { store });
		await expect(client.query("INSERT INTO svti_idempotency (svti_idempotency_key, status, expires_at) VALUES ('test', 'unknown', now())")).rejects.toMatchObject({ code: '23514' });
		await expect(client.query("INSERT INTO svti_jobs (queue, attempts) VALUES ('test', -1)")).rejects.toMatchObject({ code: '23514' });
		await expect(client.query("INSERT INTO svti_jobs (queue, claimed_at) VALUES ('test', now())")).rejects.toMatchObject({ code: '23514' });
		await expect(client.query("INSERT INTO svti_jobs (queue, claimed_at, claimed_until) VALUES ('test', now(), now() - interval '1 second')")).rejects.toMatchObject({ code: '23514' });
		for (const [status, attempts] of [['unknown', 1], ['pending', -1]]) {
			await expect(client.query('INSERT INTO svti_tasks (svti_tasks_id, name, status, fence, fence_expires_at, attempts) VALUES (gen_random_uuid(), $1, $2, gen_random_uuid(), now(), $3)', ['test', status, attempts])).rejects.toMatchObject({ code: '23514' });
		}
	});

	it('retains invalid legacy rows and resumes after the operator repairs them', async () => {
		for (const statement of postgresMigrations({ store: 'jobs' })[0].statements) await client.query(statement);
		await client.query("INSERT INTO svti_jobs (queue, attempts) VALUES ('test', -1)");
		await expect(migrate(client, { store: 'jobs' })).rejects.toMatchObject({ code: '23514' });
		expect((await client.query('SELECT attempts FROM svti_jobs')).rows).toEqual([{ attempts: -1 }]);
		expect((await client.query('SELECT version FROM svti_schema_migrations')).rows).toEqual([{ version: 1 }]);
		await client.query('UPDATE svti_jobs SET attempts = 0');
		await migrate(client, { store: 'jobs' });
		await validateSchema(client, { store: 'jobs' });
	});

	it('does not rebuild a failed replay index on each request and retries explicitly after repair', async () => {
		for (const statement of postgresMigrations({ store: 'replay' })[0].statements) await client.query(statement);
		await client.query("INSERT INTO svti_replay (topic, seq, event) VALUES ('test', 1, 'first'), ('test', 1, 'second')");
		const replay = createReplay(client, { cleanupInterval: 0 });
		stores.push(replay);
		await expect(replay.ready()).rejects.toMatchObject({ code: '23505' });
		const failedIndex = await client.query("SELECT indexrelid::text AS oid, indisvalid FROM pg_index WHERE indexrelid = to_regclass('idx_svti_replay_identity')");
		expect(failedIndex.rows[0].indisvalid).toBe(false);
		for (let i = 0; i < 3; i++) await expect(replay.since('test', 0)).rejects.toMatchObject({ code: '23505' });
		expect((await client.query("SELECT indexrelid::text AS oid, indisvalid FROM pg_index WHERE indexrelid = to_regclass('idx_svti_replay_identity')")).rows).toEqual(failedIndex.rows);
		await client.query("DELETE FROM svti_replay WHERE event = 'second'");
		await replay.ready();
		expect(replay.status()).toEqual({ state: 'ready', error: null });
		expect(await replay.since('test', 0)).toHaveLength(1);
	});

	it('retains duplicate replay rows, recovers an invalid concurrent index and replaces the old index', async () => {
		for (const statement of postgresMigrations({ store: 'replay' })[0].statements) await client.query(statement);
		await client.query("INSERT INTO svti_replay (topic, seq, event) VALUES ('test', 1, 'first'), ('test', 1, 'second')");
		await expect(migrate(client, { store: 'replay' })).rejects.toMatchObject({ code: '23505' });
		expect((await client.query('SELECT count(*)::int AS count FROM svti_replay')).rows[0].count).toBe(2);
		await client.query("DELETE FROM svti_replay WHERE event = 'second'");
		await migrate(client, { store: 'replay' });
		await expect(client.query("INSERT INTO svti_replay (topic, seq, event) VALUES ('test', 1, 'third')")).rejects.toMatchObject({ code: '23505' });
		expect((await client.query("SELECT to_regclass('idx_svti_replay_topic_seq') AS obsolete")).rows[0].obsolete).toBeNull();
		await validateSchema(client, { store: 'replay' });
	});

	it.each(['checksum', 'future'])('refuses incompatible %s ledger records before applying SQL', async mode => {
		await migrate(client, { store: 'jobs' });
		if (mode === 'checksum') await client.query("UPDATE svti_schema_migrations SET checksum = 'incorrect' WHERE version = 1");
		else await client.query("INSERT INTO svti_schema_migrations (table_name, store, version, checksum) VALUES ('svti_jobs', 'jobs', 3, 'future')");
		await expect(migrate(client, { store: 'jobs' })).rejects.toThrow('incompatible ledger');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible ledger');
		await expect(createJobQueue(client, { autoMigrate: false }).ready()).rejects.toThrow('incompatible ledger');
	});

	it('refuses to recreate a missing table when its migration ledger is already applied', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('DROP TABLE svti_jobs');
		await expect(migrate(client, { store: 'jobs' })).rejects.toThrow('missing expected column');
		expect((await client.query("SELECT to_regclass('svti_jobs') AS table_name")).rows[0].table_name).toBeNull();
	});

	it('rejects type drift, missing indexes and wrong-case CHECK literals', async () => {
		await migrate(client, { store: 'idempotency' });
		await client.query('ALTER TABLE svti_idempotency DROP CONSTRAINT chk_svti_idempotency_status');
		await client.query("ALTER TABLE svti_idempotency ADD CONSTRAINT chk_svti_idempotency_status CHECK (status = 'PENDING' OR status = 'COMMITTED')");
		await expect(validateSchema(client, { store: 'idempotency' })).rejects.toThrow('incompatible check');
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs ALTER COLUMN attempts TYPE BIGINT');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible type');
		await client.query('ALTER TABLE svti_jobs ALTER COLUMN attempts TYPE INTEGER');
		await client.query('DROP INDEX idx_svti_jobs_visibility');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible index');
	});

	it('rejects a CHECK with different boolean grouping', async () => {
		await migrate(client, { store: 'jobs' });
		await client.query('ALTER TABLE svti_jobs DROP CONSTRAINT chk_svti_jobs_claim');
		await client.query('ALTER TABLE svti_jobs ADD CONSTRAINT chk_svti_jobs_claim CHECK (claimed_at IS NULL AND (claimed_until IS NULL OR claimed_at IS NOT NULL) AND claimed_until IS NOT NULL AND claimed_until >= claimed_at)');
		await expect(validateSchema(client, { store: 'jobs' })).rejects.toThrow('incompatible check');
	});

	it('bounds advisory-lock waits and releases failed migration checkouts', async () => {
		const holder = await client.connect();
		try {
			await holder.query("SELECT pg_advisory_lock(hashtextextended(current_database() || ':' || current_schema() || ':svti_schema_migrations', 0))");
			const started = performance.now();
			// The driver's final poll deadline can win the race with the explicit
			// advisory deadline. Both must bound the wait and release the checkout.
			await expect(migrate(client, { store: 'jobs' })).rejects.toThrow(/advisory lock deadline exceeded|Query read timeout/);
			expect(performance.now() - started).toBeLessThan(10000);
		} finally { holder.release(true); }
		await migrate(client, { store: 'jobs' });
		await validateSchema(client, { store: 'jobs' });
	}, 15000);

	it.each(['jobs', 'replay'])('validates and adopts a manually provisioned %s schema without a ledger', async store => {
		for (const version of postgresMigrations({ store })) for (const statement of version.statements) await client.query(statement);
		await validateSchema(client, { store });
		await migrate(client, { store });
		await validateSchema(client, { store });
	});

	it('checks the runtime role with read-only ledger access and no schema ownership', async () => {
		await migrate(client, { store: 'jobs' });
		await admin.query('CREATE ROLE schema_contracts_runtime');
		const runtime = connect({ max: 1, options: `-c search_path=${schema}` });
		try {
			await admin.query(`GRANT USAGE ON SCHEMA ${schema} TO schema_contracts_runtime`);
			await admin.query(`GRANT SELECT, INSERT, UPDATE, DELETE ON ${schema}.svti_jobs TO schema_contracts_runtime`);
			await admin.query(`GRANT SELECT ON ${schema}.svti_schema_migrations TO schema_contracts_runtime`);
			await admin.query(`GRANT USAGE ON SEQUENCE ${schema}.svti_jobs_svti_jobs_id_seq TO schema_contracts_runtime`);
			await runtime.query('SET ROLE schema_contracts_runtime');
			const jobs = createJobQueue(runtime, { autoMigrate: false });
			await postgresReadiness(runtime, [jobs]);
			await jobs.enqueue('test', { kept: true });
			await admin.query(`REVOKE INSERT ON ${schema}.svti_jobs FROM schema_contracts_runtime`);
			await expect(validateSchema(runtime, { store: 'jobs' })).rejects.toThrow('missing INSERT privilege');
		} finally {
			await runtime.end();
			await admin.query(`DROP OWNED BY schema_contracts_runtime`);
			await admin.query('DROP ROLE schema_contracts_runtime');
		}
	});
});
