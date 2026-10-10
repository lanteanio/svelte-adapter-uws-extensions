import { describe, it, expect } from 'vitest';
import { mockPgClient } from '../../src/testing/mock-pg.js';
import { createIdempotencyStore } from '../../src/postgres/idempotency.js';

describe('Postgres idempotency erasure', () => {
	it('removes only the matching user and tenant, including a null tenant', async () => {
		const client = mockPgClient();
		const store = createIdempotencyStore(client, { cleanupInterval: 0 });
		try {
			for (const [key, user, tenant] of [['a', 'one', 'team'], ['b', 'two', 'team'], ['c', 'one', null], ['d', 'one', 'other']]) {
				const slot = await store.acquire(key, undefined, { user, tenant });
				await slot.commit(key);
			}
			expect(await store.purgeUser('team', 'one')).toBe(1);
			expect((await store.acquire('a')).acquired).toBe(true);
			expect(await store.acquire('b')).toEqual({ acquired: false, result: 'b' });
			expect(await store.acquire('c')).toEqual({ acquired: false, result: 'c' });
			expect(await store.purgeUser(null, 'one')).toBe(1);
			expect((await store.acquire('c')).acquired).toBe(true);
			expect(await store.acquire('d')).toEqual({ acquired: false, result: 'd' });
		} finally { store.destroy(); }
	});

	it('a lost owner cannot change which user may erase its successor', async () => {
		const client = mockPgClient();
		const store = createIdempotencyStore(client, { cleanupInterval: 0 });
		try {
			const old = await store.acquire('key', undefined, { user: 'old', tenant: 'team' });
			client._getIdemRows().get('key').expires_at = 0;
			const next = await store.acquire('key', undefined, { user: 'next', tenant: 'team' });
			await expect(old.commit('stale')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
			expect(await store.purgeUser('team', 'old')).toBe(0);
			await next.commit('fresh');
			expect(await store.acquire('key')).toEqual({ acquired: false, result: 'fresh' });
			expect(await store.purgeUser('team', 'next')).toBe(1);
		} finally { store.destroy(); }
	});
});
