import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createDeadLetter } from '../../../src/postgres/dead-letter.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';

const table = 'svti_dlq_capture';
const record = () => ({ webhookId: 'hook', topic: 'room', event: 'event', data: { retained: true }, attempts: 2, error: 'delivery failed', failedAt: Date.now() });
describe('dead-letter capture survives database housekeeping errors', () => {
	let client, store, breaker;
	beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
	afterEach(async () => {
		store?.destroy();
		breaker?.destroy();
		breaker = undefined;
		await client.query(`DROP TABLE IF EXISTS ${table}`);
		await client.query('DROP FUNCTION IF EXISTS svti_dlq_trim_failure()');
	});
	afterAll(async () => { await client.end(); });
	it.each(['size', 'ttl', 'breaker'])('returns the durable receipt when %s housekeeping fails', async phase => {
		breaker = phase === 'breaker' ? createCircuitBreaker({ failureThreshold: 1 }) : undefined;
		store = createDeadLetter(client, { table, max: 1, ttlMs: 60000, cleanupInterval: 0, breaker });
		await store.add(record());
		await client.query(`TRUNCATE ${table}`);
		const fragment = phase === 'ttl' ? 'failed_at <' : 'OFFSET';
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
		const rows = (await client.query(`SELECT ${table}_id::text AS id, data FROM ${table}`)).rows;
		expect(rows).toEqual([{ id, data: { retained: true } }]);
		if (breaker) expect(breaker.state).toBe('broken');
	});
	it('rejects an insert that PostgreSQL refuses without returning a capture receipt', async () => {
		store = createDeadLetter(client, { table, cleanupInterval: 0 });
		await expect(store.add({ ...record(), topic: null })).rejects.toMatchObject({ code: '23502' });
		expect((await client.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n).toBe(0);
	});
});
