import { afterAll, beforeAll, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';
let client, store;
const table = 'svti_idempotency_erasure_owner';
beforeAll(async () => {
	client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	store = createIdempotencyStore(client, { table, cleanupInterval: 0 });
	await store.ready();
});
afterAll(async () => {
	store?.destroy();
	await client?.query(`DROP TABLE IF EXISTS ${table}`);
	await client?.end();
});
it('cannot resurrect an erased pending PostgreSQL owner by committing later', async () => {
	const slot = await store.acquire('pending', 60, { tenant: 'tenant-a', user: 'alice' });
	expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
	await expect(slot.commit({ private: 'result' })).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
	expect((await client.query(`SELECT count(*) AS count FROM ${table}`)).rows[0].count).toBe('0');
});
