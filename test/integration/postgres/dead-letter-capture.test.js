import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { monotonicNow } from '../../../src/shared/runtime.js';

const table = 'svti_dlq_capture';
const record = (over = {}) => ({ webhookId: 'hook', topic: 'room', event: 'event', data: { retained: true }, attempts: 2, error: 'delivery failed', failedAt: Date.now(), ...over });
describe('dead-letter capture survives database housekeeping errors', () => {
	let client, store, breaker;
	beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
	afterEach(async () => {
		store?.destroy();
		breaker?.destroy();
		breaker = undefined;
		await client.query(`DROP TABLE IF EXISTS ${table}, ${table}_purges, ${table}_scopes`);
		await client.query(`DROP FUNCTION IF EXISTS ${table}_scopes_sync()`);
		await client.query('DELETE FROM svti_schema_migrations WHERE table_name = $1', [table]);
		await client.query('DROP FUNCTION IF EXISTS svti_dlq_trim_failure()');
		await client.query('DROP FUNCTION IF EXISTS svti_dlq_pause_capture()');
	});
	afterAll(async () => { await client.end(); });
	it('keeps every prototype-named topic in its durable summary', async () => {
		store = createDeadLetter(client, { table, cleanupInterval: 0 });
		for (const topic of ['__proto__', '__proto__', 'constructor', 'prototype', 'safe']) {
			await store.add(record({ topic }));
		}
		const summary = await store.summary();
		expect(Object.getPrototypeOf(summary.byTopic)).toBe(null);
		expect(Object.keys(summary.byTopic).sort()).toEqual(['__proto__', 'constructor', 'prototype', 'safe']);
		expect(summary.byTopic.__proto__).toBe(2);
		expect(summary.byTopic.constructor).toBe(1);
		expect(summary.byTopic.prototype).toBe(1);
		expect(Object.values(summary.byTopic).reduce((n, count) => n + count, 0)).toBe(summary.total);
		expect(summary.total).toBe(5);
		expect(await store.count()).toBe(5);
		await store.clear();
		const empty = await store.summary();
		expect(Object.getPrototypeOf(empty.byTopic)).toBe(null);
		expect(Object.keys(empty.byTopic)).toEqual([]);
		expect(empty.total).toBe(0);
	});
	it.each(['size', 'ttl', 'breaker'])('returns the durable receipt when %s housekeeping fails', async phase => {
		breaker = phase === 'breaker' ? createCircuitBreaker({ failureThreshold: 1 }) : undefined;
		store = createDeadLetter(client, { table, max: 1, ttlMs: 60000, cleanupInterval: 0, breaker });
		await store.add(record());
		await client.query(`TRUNCATE ${table}`);
		// One stored record puts the capture over the cap, so the size trim deletes.
		const seeded = (await client.query(`INSERT INTO ${table} (topic, failed_at, data, scope)
			VALUES ('room', (extract(epoch from now()) * 1000)::bigint, '{"retained": true}', '~') RETURNING ${table}_id::text AS id`)).rows[0].id;
		const fragment = phase === 'ttl' ? 'failed_at <' : 'WITH victims';
		await client.query(`CREATE FUNCTION svti_dlq_trim_failure() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF strpos(current_query(), '${fragment}') > 0 THEN
				RAISE EXCEPTION 'retention statement cancelled' USING ERRCODE = '57014';
			END IF;
			RETURN NULL;
		END $$`);
		await client.query(`CREATE TRIGGER reject_retention BEFORE DELETE ON ${table} FOR EACH STATEMENT EXECUTE FUNCTION svti_dlq_trim_failure()`);
		const id = await store.add(record());
		expect(id).toMatch(/^\d+$/);
		// The failed pass rolled back, so neither record was evicted.
		const rows = (await client.query(`SELECT ${table}_id::text AS id, data FROM ${table} ORDER BY ${table}_id`)).rows;
		expect(rows).toEqual([{ id: seeded, data: { retained: true } }, { id, data: { retained: true } }]);
		if (breaker) expect(breaker.state).toBe('broken');
	});
	it('rejects an insert that PostgreSQL refuses without returning a capture receipt', async () => {
		store = createDeadLetter(client, { table, cleanupInterval: 0 });
		await expect(store.add({ ...record(), topic: null })).rejects.toMatchObject({ code: '23502' });
		expect((await client.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n).toBe(0);
	});
	it('serializes a paused old capture with purge so its payload cannot reappear', async () => {
		store = createDeadLetter(client, { table, cleanupInterval: 0, forgetUserId: r => r.data?.user });
		await store.ready();
		await client.query(`CREATE FUNCTION svti_dlq_pause_capture() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.2); RETURN NEW; END $$`);
		await client.query(`CREATE TRIGGER pause_capture BEFORE INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION svti_dlq_pause_capture()`);
		const startedAt = monotonicNow();
		const capturing = store.add(record({ data: { user: 'alice', private: true }, startedAt }));
		await expect.poll(async () => (await client.query(
			"SELECT count(*)::int AS n FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state = 'active' AND query LIKE $1",
			[`%${table}%`]
		)).rows[0].n).toBeGreaterThan(0);

		const purging = store.purgeUser(null, 'alice');
		expect(await capturing).not.toBeNull();
		expect(await purging).toBe(1);
		expect(await store.count()).toBe(0);
		expect(await store.add(record({ data: { user: 'alice', private: true }, startedAt }))).toBeNull();
	});
});
