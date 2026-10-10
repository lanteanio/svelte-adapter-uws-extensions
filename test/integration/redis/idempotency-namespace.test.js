import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createIdempotencyStore } from '../../../src/redis/idempotency.js';

let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-idem-namespace:' }); });
afterAll(async () => { await client.quit(); });

describe.each(['idem:', 'custom:', ''])('Redis erasure-index isolation with prefix %j', (keyPrefix) => {
	it('rejects attacker-first and index-first namespace collisions', async () => {
		const store = createIdempotencyStore(client, { keyPrefix });
		try {
			await expect(store.acquire('byuser:acme\0victim')).rejects.toThrow('reserved prefix');
			await expect(store.acquire('byuser:v2\0acme\0victim')).rejects.toThrow('reserved prefix');
			await (await store.acquire('safe', 60, { tenant: 'acme', user: 'victim' })).commit({ private: true });
			expect(await client.redis.type(client.key(keyPrefix + 'byuser:v2\0acme\0victim'))).toBe('hash');
			await expect(store.purge('byuser:acme\0victim')).rejects.toThrow('reserved prefix');
			await expect(store.purge('byuser:v2\0acme\0victim')).rejects.toThrow('reserved prefix');
			expect((await store.acquire('safe')).result).toEqual({ private: true });
			expect(await store.purgeUser('acme', 'victim')).toBe(1);
			const slot = await store.acquire('safe'); expect(slot.acquired).toBe(true); await slot.abort();
		} finally { await store.clear(); }
	});
});
