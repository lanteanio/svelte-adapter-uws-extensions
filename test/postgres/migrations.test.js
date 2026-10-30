import { describe, expect, it, vi } from 'vitest';
import { migrate, postgresMigrations, postgresReadiness, validateSchema } from '../../src/postgres/index.js';
import { createReplay } from '../../src/postgres/replay.js';
import { createJobQueue } from '../../src/postgres/jobs.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';
import { topicTenantScopeSql } from '../../src/shared/tenant-topic.js';

describe('PostgreSQL migration contracts', () => {
	it('keeps published migration checksums immutable', () => {
		const expected = {
			replay: ['95cad9e6180f1ac2e0c2ec852f7999cbe93f6f8a93e1db3b7da36aab65dfd9ce', 'c2767b2b839b55691f7ed6f1765593557f6dac4df93fe0c135d33cfc0cbcc608'],
			jobs: ['11f28312fe2e244e9ae6d46b047f4a2a7f9a55a41376be0feacd4004229c13c9', '5ef2532f61b39c4c9624b34b8cc9bc16688da0f6bc9a909db0bb98d7cc097faa'],
			tasks: ['974a263bf65bdcaef75e66700a146bceb117b925eef1cad9435abafa2c5702ae', '0704682d4a9506e64c399c2a5df98b306c9200c89ab8d4ae7e0939a61eb678ce'],
			idempotency: ['85bbe9cab20fccf9d78a3e0e95d16cb8ee0fd2958f8a48a5beef3ee2b576ba48', 'f7d87331da8e2891e9f9eea36489a3c0af90ac70b9a72acce89da068427ee499'],
			'dead-letter': ['d71e2cd0374adc70f47e952165a315c1f1b0a0b9301cd71954a4d2d93c785a9d', '2391d3029698b6b26609c93cb1db6b8f976a2f47f71d4135d03e406a2ed75d7b'],
			'alarm-store': ['95b27e2254ed8aeff9736649f151c5f4f9953ae985c597657ab62bcabb996ab2', '641d90e61f980753b9f05b92aed76a0d9ca05abfb542b0e944b0e0555fe74499']
		};
		for (const [store, checksums] of Object.entries(expected)) expect(postgresMigrations({ store }).slice(0, 2).map(entry => entry.checksum)).toEqual(checksums);
	});
	it('adds retained alarm executions as version 4 without changing the generation migration', () => {
		const versions = postgresMigrations({ store: 'alarm-store' });
		expect(versions.map(entry => entry.version)).toEqual([1, 2, 3, 4]);
		expect(versions[2].checksum).toBe('861868ed365e9b05d3bb5afd851994312b576d7a130bd4a53de7e30dbc496570');
		expect(versions[3].statements[0]).toBe('ALTER TABLE svti_alarms ADD COLUMN IF NOT EXISTS attempt BIGINT NOT NULL DEFAULT 1');
		expect(versions[3].statements[1]).toMatch(/^CREATE TABLE IF NOT EXISTS svti_alarms_executions \(.*PRIMARY KEY \(topic, generation\)\)$/);
		// Version 4 creates the executions table and no other relation.
		expect(versions[3].statements.filter(statement => /^CREATE TABLE/.test(statement))).toHaveLength(1);
	});
	it('adds bounded dead-letter erasure fences as version 3', () => {
		const versions = postgresMigrations({ store: 'dead-letter' });
		expect(versions.map(entry => entry.version)).toEqual([1, 2, 3, 4]);
		expect(versions[2].checksum).toBe('5552bd3f6e44ed8eadfe658ff62d7fcaaa44aee25e51cdb87aa2e7f90599fe1b');
		expect(versions[2].statements).toEqual([
			'CREATE TABLE IF NOT EXISTS svti_dead_letter_purges (scope TEXT NOT NULL, user_id TEXT NOT NULL, purged_at BIGINT NOT NULL, PRIMARY KEY (scope, user_id))',
			'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_svti_dead_letter_purges_expiry ON svti_dead_letter_purges (purged_at)'
		]);
	});
	it('adds dead-letter tenant scopes, their counts and the scope index as version 4', () => {
		const { statements } = postgresMigrations({ store: 'dead-letter' })[3];
		expect(statements).toHaveLength(4);
		expect(statements[0]).toBe("ALTER TABLE svti_dead_letter ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT ''::text");
		expect(statements[1]).toBe('CREATE TABLE IF NOT EXISTS svti_dead_letter_scopes (scope TEXT NOT NULL, records BIGINT NOT NULL, oldest BIGINT NOT NULL, PRIMARY KEY (scope))');
		expect(statements[3]).toBe('CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_svti_dead_letter_scope ON svti_dead_letter (scope, svti_dead_letter_id)');
		// One locked block derives the scope of existing rows from their topics,
		// replaces the function in the table's schema with a pinned search_path,
		// recreates the four statement triggers and rebuilds the counts.
		const block = statements[2];
		expect(block).toMatch(/^DO \$migration\$ DECLARE target text; BEGIN LOCK TABLE svti_dead_letter IN SHARE ROW EXCLUSIVE MODE; SELECT '"' \|\| replace\(n\.nspname, '"', '""'\) \|\| '"' INTO target FROM pg_class c JOIN pg_namespace n ON n\.oid = c\.relnamespace WHERE c\.oid = 'svti_dead_letter'::regclass;/);
		expect(block).toContain(`UPDATE svti_dead_letter SET scope = ${topicTenantScopeSql('topic')} WHERE scope = '' AND ${topicTenantScopeSql('topic')} <> '';`);
		expect(block).toContain("EXECUTE format('CREATE OR REPLACE FUNCTION %s.%I() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS %L', target, 'svti_dead_letter_scopes_sync', replace($sync$");
		// Every table the body names carries the schema placeholder.
		const body = /\$sync\$([\s\S]*?)\$sync\$/.exec(block)[1];
		expect(body.match(/"svti_dead_letter(?:_scopes)?"/g).length).toBe(body.match(/@S@\."svti_dead_letter(?:_scopes)?"/g).length);
		expect(body).not.toMatch(/(?:FROM|INTO|UPDATE) (?!@S@)\w*svti/);
		for (const [event, referencing] of [['INSERT', 'REFERENCING NEW TABLE AS added '], ['UPDATE', 'REFERENCING OLD TABLE AS removed NEW TABLE AS added '], ['DELETE', 'REFERENCING OLD TABLE AS removed '], ['TRUNCATE', '']]) {
			expect(block).toContain(`EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER ${event} ON %s.%I ${referencing}FOR EACH STATEMENT EXECUTE FUNCTION %s.%I()', 'svti_dead_letter_scopes_${event.toLowerCase()}', target, 'svti_dead_letter', target, 'svti_dead_letter_scopes_sync');`);
		}
		expect(block).toMatch(/DELETE FROM svti_dead_letter_scopes; INSERT INTO svti_dead_letter_scopes \(scope, records, oldest\) SELECT scope, count\(\*\), min\(svti_dead_letter_id\) FROM svti_dead_letter GROUP BY scope; END \$migration\$$/);
	});
	it('keeps every generated dead-letter identifier within the PostgreSQL limit for the longest table name', () => {
		const table = 'a'.repeat(42);
		const names = postgresMigrations({ store: 'dead-letter', table }).flatMap(entry => entry.statements)
			.flatMap(statement => [...statement.matchAll(/\bidx_[a-z0-9_]+|\ba{42}_[a-z_]+/g)].map(match => match[0]));
		expect(names).toContain(table + '_scopes_truncate');
		for (const name of names) expect(name.length, name).toBeLessThanOrEqual(63);
	});
	it('refuses a dead-letter table whose scope-count trigger is disabled', async () => {
		const client = mockPgClient();
		await migrate(client, { store: 'dead-letter' });
		const query = client.query.bind(client);
		client.query = async (text, ...rest) => {
			const result = await query(text, ...rest);
			if (!String(text?.text ?? text).includes('FROM pg_trigger t')) return result;
			return { ...result, rows: result.rows.map(row => row.name.endsWith('_delete') ? { ...row, enabled: 'D' } : row) };
		};
		await expect(validateSchema(client, { store: 'dead-letter' })).rejects.toThrow('missing or incompatible trigger svti_dead_letter_scopes_delete');
	});
	it('keeps every generated alarm identifier within the PostgreSQL limit for the longest table name', () => {
		const table = 'a'.repeat(42);
		const names = postgresMigrations({ store: 'alarm-store', table }).flatMap(entry => entry.statements)
			.flatMap(statement => [...statement.matchAll(/\b(?:chk|idx)_[a-z0-9_]+|\ba{42}_[a-z_]+/g)].map(match => match[0]));
		expect(names.length).toBeGreaterThan(10);
		for (const name of names) expect(name.length, name).toBeLessThanOrEqual(63);
		expect(Math.max(...names.map(name => name.length))).toBeGreaterThan(55);
	});
	it.each(['replay', 'jobs', 'tasks', 'idempotency', 'dead-letter', 'alarm-store'])('migrates and validates %s repeatedly', async store => {
		const client = mockPgClient();
		await migrate(client, { store });
		await migrate(client, { store });
		await validateSchema(client, { store });
	});
	it.each(['replay', 'jobs', 'tasks', 'idempotency', 'dead-letter', 'alarm-store'])('adopts a manually provisioned %s schema without a ledger', async store => {
		// Every version's SQL applied by hand, as the docs describe; migrate()
		// then replays the earlier versions over the later column definitions.
		const client = mockPgClient();
		for (const version of postgresMigrations({ store })) for (const statement of version.statements) await client.query(statement);
		await validateSchema(client, { store });
		await migrate(client, { store });
		await validateSchema(client, { store });
	});
	it('does not change the migration corpus when a caller edits the returned manifest', () => {
		const original = postgresMigrations({ store: 'jobs' });
		const copy = postgresMigrations({ store: 'jobs' });
		copy[0].statements.length = 0;
		expect(postgresMigrations({ store: 'jobs' })).toEqual(original);
		expect(original.map(entry => entry.version)).toEqual([1, 2]);
		expect(original.every(entry => /^[0-9a-f]{64}$/.test(entry.checksum))).toBe(true);
	});
	it('uses one initialization flight and exposes a retryable failure', async () => {
		const client = mockPgClient();
		const connect = client.connect;
		client.connect = vi.fn().mockRejectedValueOnce(new Error('unavailable')).mockImplementation(connect);
		const jobs = createJobQueue(client);
		expect(jobs.status()).toEqual({ state: 'idle', error: null });
		const first = jobs.ready();
		expect(jobs.ready()).toBe(first);
		await expect(first).rejects.toThrow('unavailable');
		expect(jobs.status()).toMatchObject({ state: 'failed' });
		await jobs.ready();
		expect(jobs.status()).toEqual({ state: 'ready', error: null });
		expect(client.connect).toHaveBeenCalledTimes(2);
	});
	it('latches failed automatic migrations until an explicit readiness retry', async () => {
		const client = mockPgClient();
		const connect = client.connect;
		const failure = new Error('duplicate legacy identity');
		client.connect = vi.fn().mockRejectedValueOnce(failure).mockImplementation(connect);
		const jobs = createJobQueue(client);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		await expect(jobs.claim('test')).rejects.toBe(failure);
		expect(client.connect).toHaveBeenCalledTimes(1);
		await jobs.ready();
		await jobs.enqueue('test', { repaired: true });
		expect(client.connect).toHaveBeenCalledTimes(2);
		expect(jobs.status()).toEqual({ state: 'ready', error: null });
	});
	it.each(['PG_CAPACITY_EXCEEDED', '08006', '40001', '53300', '55P03', '57014', '57P01', 'ECONNRESET', 'ECONNREFUSED', 'ECONNABORTED', 'ETIMEDOUT', 'EAI_AGAIN'])('retries automatic initialization after transient %s', async code => {
		const client = mockPgClient();
		const connect = client.connect;
		const failure = Object.assign(new Error('temporary initialization refusal'), { code });
		client.connect = vi.fn().mockRejectedValueOnce(failure).mockImplementation(connect);
		const jobs = createJobQueue(client);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		expect(jobs.status()).toEqual({ state: 'idle', error: failure });
		await jobs.enqueue('test', { recovered: true });
		expect(client.connect).toHaveBeenCalledTimes(2);
		expect(jobs.status()).toEqual({ state: 'ready', error: null });
	});
	// built: what the index-state read after the failure reports (null = not consulted).
	function failingDdl(client, pattern, failure, built = null) {
		const connect = client.connect;
		let pending = true;
		client.indexReads = 0;
		client.connect = vi.fn(async (...args) => {
			const connection = await connect(...args);
			const query = connection.query;
			return { ...connection, query: (statement, values) => {
				const text = statement.text ?? statement;
				if (pending && pattern.test(text)) { pending = false; return Promise.reject(failure); }
				if (text.includes('indisready')) { client.indexReads++; return Promise.resolve({ rows: built === null ? [] : [{ built }] }); }
				return query(statement, values);
			} };
		});
	}
	it.each([
		['statement timeout', /VALIDATE CONSTRAINT/, Object.assign(new Error('canceling statement due to statement timeout'), { code: '57014' }), null],
		['lock timeout after a concurrent index build', /CREATE INDEX CONCURRENTLY/, Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' }), true]
	])('latches a DDL %s until an explicit readiness retry', async (_, pattern, failure, built) => {
		const client = mockPgClient();
		failingDdl(client, pattern, failure, built);
		const jobs = createJobQueue(client);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
		expect(jobs.status()).toEqual({ state: 'failed', error: failure });
		expect(client.connect).toHaveBeenCalledTimes(1);
		await jobs.ready();
		await jobs.enqueue('test', { repaired: true });
		expect(client.connect).toHaveBeenCalledTimes(2);
	});
	it('retries a lock deadline hit by DDL, before a concurrent build, a statement timeout outside DDL, and a driver read timeout', async () => {
		const cases = [
			[/ADD COLUMN/, Object.assign(new Error('temporary deadline'), { code: '55P03' }), null, 0],
			[/CREATE INDEX CONCURRENTLY/, Object.assign(new Error('temporary deadline'), { code: '55P03' }), false, 1],
			[/CREATE INDEX CONCURRENTLY/, Object.assign(new Error('temporary deadline'), { code: '55P03' }), null, 1],
			[/pg_try_advisory_lock/, Object.assign(new Error('temporary deadline'), { code: '57014' }), null, 0],
			[/VALIDATE CONSTRAINT/, new Error('Query read timeout'), null, 0]
		];
		for (const [pattern, failure, built, reads] of cases) {
			const client = mockPgClient();
			failingDdl(client, pattern, failure, built);
			const jobs = createJobQueue(client);
			await expect(jobs.enqueue('test', {})).rejects.toBe(failure);
			expect(jobs.status()).toEqual({ state: 'idle', error: failure });
			expect(client.indexReads).toBe(reads);
			await jobs.enqueue('test', { recovered: true });
			expect(client.connect).toHaveBeenCalledTimes(2);
		}
	});
	it('keeps disabled automatic migration read-only and reports an absent schema', async () => {
		const client = mockPgClient();
		client.connect = vi.fn(client.connect);
		client.query = vi.fn(client.query);
		const replay = createReplay(client, { autoMigrate: false, cleanupInterval: 0 });
		await expect(replay.ready()).rejects.toThrow('missing expected column');
		expect(client.connect).not.toHaveBeenCalled();
		expect(client.query.mock.calls.every(([statement]) => statement.text.startsWith('SELECT '))).toBe(true);
		replay.destroy();
	});
	it('returns every failed component from the aggregate gate, including synchronous errors', async () => {
		const first = new Error('first'), second = new Error('second');
		const client = { check: vi.fn().mockResolvedValue({}) };
		const components = [{ ready() { throw first; } }, { ready: async () => { throw second; } }];
		await expect(postgresReadiness(client, components)).rejects.toMatchObject({ errors: [first, second] });
	});
	it('destroys the migration checkout after a failed statement', async () => {
		const release = vi.fn(), error = new Error('DDL failed');
		const client = { connect: async () => ({ query: async () => { throw error; }, release }) };
		await expect(migrate(client, { store: 'jobs' })).rejects.toBe(error);
		expect(release).toHaveBeenCalledExactlyOnceWith(true);
	});

	it('adds replay last use and a seeded epoch sequence as version 3', () => {
		const versions = postgresMigrations({ store: 'replay' });
		expect(versions.map(entry => entry.version)).toEqual([1, 2, 3]);
		expect(versions[2].statements[0]).toBe('ALTER TABLE svti_replay_seq ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()');
		const seed = versions[2].statements[1];
		expect(seed).toContain('CREATE SEQUENCE IF NOT EXISTS svti_replay_seq_epoch_seq AS BIGINT OWNED BY svti_replay_seq.epoch;');
		// Seeded from the highest stored epoch before any new topic can take one.
		expect(seed.indexOf('MAX(epoch)')).toBeLessThan(seed.indexOf('SET DEFAULT nextval'));
		expect(seed).toContain("ALTER TABLE svti_replay_seq ALTER COLUMN epoch SET DEFAULT nextval('svti_replay_seq_epoch_seq'::regclass)");
		// Existing topics start from their newest event, not the migration time.
		expect(seed).toContain('UPDATE svti_replay_seq s SET last_used_at = COALESCE((SELECT r.created_at FROM svti_replay r WHERE r.topic = s.topic ORDER BY r.seq DESC LIMIT 1), to_timestamp(0))');
		const long = postgresMigrations({ store: 'replay', table: 'a'.repeat(42) })[2].statements.join(' ');
		for (const [name] of long.matchAll(/\ba{42}_[a-z_]+/g)) expect(name.length, name).toBeLessThanOrEqual(63);
	});

	it('refuses a manually provisioned column no version defines before running any DDL', async () => {
		const client = mockPgClient();
		await client.query('CREATE TABLE IF NOT EXISTS svti_replay_seq (topic TEXT NOT NULL, seq BIGINT NOT NULL DEFAULT 0, epoch BIGINT NOT NULL DEFAULT 7, PRIMARY KEY (topic))');
		await expect(migrate(client, { store: 'replay' })).rejects.toThrow('postgres schema: incompatible default for svti_replay_seq.epoch');
		// Version 1 would have created the event table first.
		expect((await client.query('SELECT to_regclass($1) IS NOT NULL AS present', ['svti_replay'])).rows).toEqual([{ present: false }]);
	});
});
