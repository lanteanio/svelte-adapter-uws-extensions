import { describe, expect, it } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createIdempotencyStore } from '../../src/redis/idempotency.js';

describe.each(['idem:', 'custom:', ''])('idempotency internal namespace with prefix %j', (keyPrefix) => {
	it('refuses a colliding acquisition before it can poison an erasure index', async () => {
		const client = mockRedisClient('app:');
		const store = createIdempotencyStore(client, { keyPrefix });
		const collision = 'byuser:acme\0victim';
		await expect(store.acquire(collision)).rejects.toThrow('reserved prefix');
		await (await store.acquire('safe', 60, { tenant: 'acme', user: 'victim' })).commit('private');
		expect(await store.purgeUser('acme', 'victim')).toBe(1);
		expect((await store.acquire('safe')).acquired).toBe(true);
	});
	it('refuses a colliding purge without deleting an existing erasure index', async () => {
		const client = mockRedisClient('app:');
		const store = createIdempotencyStore(client, { keyPrefix });
		await (await store.acquire('safe', 60, { tenant: 'acme', user: 'victim' })).commit('private');
		await expect(store.purge('byuser:acme\0victim')).rejects.toThrow('reserved prefix');
		expect((await store.acquire('safe')).result).toBe('private');
		expect(await store.purgeUser('acme', 'victim')).toBe(1);
		expect((await store.acquire('safe')).acquired).toBe(true);
	});
});

it('keeps normal tenant-qualified and encoded keys available', async () => {
	const store = createIdempotencyStore(mockRedisClient());
	for (const key of ['byuser', 'Byuser:acme\0victim', 'acme\0rpc:order:byuser:value', JSON.stringify(['order', 'victim', 'byuser:value'])]) {
		const slot = await store.acquire(key); expect(slot.acquired).toBe(true); await slot.commit('ok');
		expect((await store.acquire(key)).result).toBe('ok'); await store.purge(key);
	}
});

it('does not automatically delete retained legacy data in the reserved namespace', async () => {
	const client = mockRedisClient(), store = createIdempotencyStore(client);
	const key = 'byuser:acme\0victim', full = client.key('idem:' + key);
	await client.redis.set(full, 'legacy-string');
	await expect(store.acquire(key)).rejects.toThrow('reserved prefix');
	await expect(store.purge(key)).rejects.toThrow('reserved prefix');
	expect(await client.redis.get(full)).toBe('legacy-string');
});
