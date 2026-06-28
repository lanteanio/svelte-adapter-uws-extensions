/**
 * Integration tests for the Postgres right-to-erasure (`live.forget`) purge
 * paths against a real Postgres 16 server.
 *
 * The four Postgres forget stores (idempotency, dead-letter, replay, tasks)
 * stamp a denormalized `user_id` (idempotency also `tenant_id`) via an
 * `ALTER TABLE ADD COLUMN IF NOT EXISTS` forward-migration and erase with
 * `DELETE WHERE user_id = $1`. The mock (`mock-pg`) approximates DDL and
 * `tenant_id IS NOT DISTINCT FROM NULL` matching with in-memory JS, so the
 * actual DELETE row-counts, the real-NULL tenant match, and the forward
 * migration against a pre-existing table are only proven here. These are the
 * durable erasure paths a GDPR Article 17 request leans on, so the deletion
 * must be verified against a real server, not just the mock.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { createReplay } from '../../../src/postgres/replay.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';

// The replay/stream publish path only calls platform.publish on a storage
// failure fallback; a no-op stub is enough for the happy path.
const platform = { publish() {} };

describe('postgres right-to-erasure (purgeUser, integration)', () => {
	let client;

	beforeAll(() => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		client = createPgClient({ connectionString: url, autoShutdown: false });
	});

	afterAll(async () => { await client.end(); });

	describe('idempotency.purgeUser', () => {
		const TABLE = 'svti_idem_forget_it';
		let store;

		beforeEach(async () => {
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
			store = createIdempotencyStore(client, { table: TABLE, cleanupInterval: 0 });
		});
		afterEach(() => { if (store) store.destroy(); });
		afterAll(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });

		it('deletes only the (tenant,user) rows and matches NULL tenant via IS NOT DISTINCT FROM', async () => {
			await store.acquire('k-alice-t1', 60, { user: 'alice', tenant: 't1' });
			await store.acquire('k-bob-t1', 60, { user: 'bob', tenant: 't1' });
			await store.acquire('k-alice-t2', 60, { user: 'alice', tenant: 't2' });
			await store.acquire('k-alice-null', 60, { user: 'alice' }); // tenant null

			// Erase alice in t1 only: bob (other user), alice@t2 (other tenant),
			// and alice@null (NULL != 't1' under IS NOT DISTINCT FROM) all survive.
			expect(await store.purgeUser('t1', 'alice')).toBe(1);
			let keys = (await client.query(`SELECT svti_idempotency_key AS k FROM ${TABLE} ORDER BY k`)).rows.map((r) => r.k);
			expect(keys).toEqual(['k-alice-null', 'k-alice-t2', 'k-bob-t1']);

			// NULL tenant matches ONLY the NULL-stamped row (not t1/t2 alice).
			expect(await store.purgeUser(null, 'alice')).toBe(1);
			keys = (await client.query(`SELECT svti_idempotency_key AS k FROM ${TABLE} ORDER BY k`)).rows.map((r) => r.k);
			expect(keys).toEqual(['k-alice-t2', 'k-bob-t1']);

			expect(await store.purgeUser('t2', 'alice')).toBe(1);
			expect(await store.purgeUser('t1', 'alice')).toBe(0); // nothing left
		});
	});

	// Own describe + unique table: createIdempotencyStore eagerly auto-migrates at
	// construction, so a store-creating beforeEach would race this test's explicit
	// legacy CREATE TABLE. Here the legacy table is created before any store exists.
	describe('idempotency forward-migration (ADD COLUMN IF NOT EXISTS)', () => {
		const TABLE = 'svti_idem_fwdmig_it';
		let s;
		afterEach(() => { if (s) s.destroy(); });
		afterAll(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });

		it('migrates a pre-existing table, then stamps + erases', async () => {
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
			// A table created by a pre-erasure deployment: base columns only, no
			// user_id / tenant_id. Constructing the store must ALTER it forward.
			await client.query(`CREATE TABLE ${TABLE} (
				svti_idempotency_key TEXT PRIMARY KEY,
				status TEXT NOT NULL,
				result JSONB,
				expires_at TIMESTAMPTZ NOT NULL
			)`);
			s = createIdempotencyStore(client, { table: TABLE, cleanupInterval: 0 });
			await s.acquire('k', 60, { user: 'alice', tenant: 't1' }); // awaits ensureTable (the ALTER), then stamps
			const cols = (await client.query(
				`SELECT column_name FROM information_schema.columns WHERE table_name = $1`, [TABLE]
			)).rows.map((r) => r.column_name);
			expect(cols).toContain('user_id');
			expect(cols).toContain('tenant_id');
			const row = (await client.query(`SELECT user_id, tenant_id FROM ${TABLE} WHERE svti_idempotency_key = 'k'`)).rows[0];
			expect(row.user_id).toBe('alice');
			expect(row.tenant_id).toBe('t1');
			expect(await s.purgeUser('t1', 'alice')).toBe(1);
		});
	});

	describe('dead-letter.purgeUser', () => {
		const TABLE = 'svti_dlq_forget_it';
		beforeEach(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });
		afterAll(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });

		const rec = (u) => ({ webhookId: 'w', topic: 'hooks', event: 'e', data: { user: u }, attempts: 1, error: 'boom', failedAt: 1 });

		it('deletes the records the forgetUserId extractor maps to the user', async () => {
			const store = createDeadLetter(client, { table: TABLE, forgetUserId: (r) => r.data && r.data.user });
			await store.add(rec('alice'));
			await store.add(rec('alice'));
			await store.add(rec('bob'));
			expect(await store.purgeUser(null, 'alice')).toBe(2);
			expect(await store.count()).toBe(1);
			const left = await store.list();
			expect(left.every((r) => r.data.user === 'bob')).toBe(true);
		});

		it('is a no-op without a forgetUserId extractor', async () => {
			const store = createDeadLetter(client, { table: TABLE });
			await store.add(rec('alice'));
			expect(await store.purgeUser(null, 'alice')).toBe(0);
			expect(await store.count()).toBe(1);
		});
	});

	describe('replay.purgeUser', () => {
		const TABLE = 'svti_replay_forget_it';
		const drop = async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}, ${TABLE}_seq`); };
		beforeEach(drop);
		afterAll(drop);

		it('deletes the buffered messages the forgetUserId extractor maps to the user', async () => {
			const store = createReplay(client, { table: TABLE, forgetUserId: ({ data }) => data && data.user });
			await store.publish(platform, 'room', 'msg', { user: 'alice', n: 1 });
			await store.publish(platform, 'room', 'msg', { user: 'bob', n: 2 });
			await store.publish(platform, 'room', 'msg', { user: 'alice', n: 3 });

			expect(await store.purgeUser(null, 'alice')).toBe(2);
			const left = await store.since('room', 0);
			expect(left.map((m) => m.data.user)).toEqual(['bob']);
		});
	});

	describe('tasks.purgeUser', () => {
		const TABLE = 'svti_tasks_forget_it';
		let runner;
		beforeEach(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });
		afterEach(() => { if (runner) runner.destroy(); });
		afterAll(async () => { await client.query(`DROP TABLE IF EXISTS ${TABLE}`); });

		it('deletes the task rows the forgetUserId extractor maps to the user', async () => {
			// Never call start() - we only enqueue (inserts pending rows stamped with
			// user_id) and purge; no worker/dispatch loop is needed.
			runner = createTaskRunner(client, { table: TABLE, forgetUserId: (input) => input && input.user });
			await runner.enqueue('send', { input: { user: 'alice', m: 1 } });
			await runner.enqueue('send', { input: { user: 'alice', m: 2 } });
			await runner.enqueue('send', { input: { user: 'bob', m: 3 } });

			expect(await runner.purgeUser(null, 'alice')).toBe(2);
			const remaining = (await client.query(`SELECT count(*)::int AS n FROM ${TABLE}`)).rows[0].n;
			expect(remaining).toBe(1);
		});
	});
});
