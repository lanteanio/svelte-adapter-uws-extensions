import { describe, it, expect, afterEach, vi } from 'vitest';
import { createIdempotencyStore as redisStore } from '../../src/redis/idempotency.js';
import { createIdempotencyStore as pgStore } from '../../src/postgres/idempotency.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';

afterEach(() => vi.useRealTimers());
for (const [name, makeClient, makeStore] of [['redis', mockRedisClient, redisStore], ['postgres', mockPgClient, pgStore]]) {
	describe(name + ' idempotency retention', () => {
		it.each([undefined, 0, 5, 864000])('honors per-acquire ttl %s', async (ttl) => {
			vi.useFakeTimers();
			const store = makeStore(makeClient(), { ttl: 172800, cleanupInterval: 0 });
			try {
				const slot = await store.acquire('key', ttl, { user: 'one', tenant: 'team' });
				expect(await store.acquire('key')).toEqual({ acquired: false, pending: true });
				await slot.commit('result');
				if (ttl === 0) {
					expect((await store.acquire('key')).acquired).toBe(true);
				} else {
					const seconds = ttl ?? 172800;
					vi.setSystemTime(Date.now() + seconds * 1000 - 1);
					expect(await store.acquire('key')).toEqual({ acquired: false, result: 'result' });
					vi.setSystemTime(Date.now() + 2);
					expect((await store.acquire('key')).acquired).toBe(true);
				}
			} finally { store.destroy?.(); }
		});

		it('zero retention cannot delete a successor after lease expiry', async () => {
			vi.useFakeTimers();
			const store = makeStore(makeClient(), { acquireTtl: 1, cleanupInterval: 0 });
			try {
				const old = await store.acquire('key', 0);
				vi.setSystemTime(Date.now() + 1001);
				const next = await store.acquire('key', 5);
				await expect(old.commit('old')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
				await next.commit('next');
				expect(await store.acquire('key')).toEqual({ acquired: false, result: 'next' });
			} finally { store.destroy?.(); }
		});

		it.each([-1, 1.5, Infinity, NaN, null, '5'])('refuses invalid ttl %s before acquiring', async (ttl) => {
			const store = makeStore(makeClient(), { cleanupInterval: 0 });
			try {
				await expect(store.acquire('key', ttl)).rejects.toThrow('ttlSec');
				expect((await store.acquire('key', 0)).acquired).toBe(true);
			} finally { store.destroy?.(); }
		});
	});
}
