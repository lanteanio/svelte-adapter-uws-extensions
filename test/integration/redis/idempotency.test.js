/**
 * Integration tests for redis/idempotency against a real Redis 7 server.
 *
 * Exercises the ACQUIRE_SCRIPT body (SET NX EX + sentinel disambiguation)
 * end-to-end against real SET semantics: NX-collision behaviour, the EX
 * acquireTtl bound on pending slots, the long-TTL replacement on commit,
 * and the JSON round-trip through Redis strings. The mock-based suite at
 * test/redis/idempotency.test.js stays as-is; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createIdempotencyStore } from '../../../redis/idempotency.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis idempotency (integration)', () => {
	let client;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-idem:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('ACQUIRE_SCRIPT three-state flow', () => {
		it('first acquire returns acquired with commit + abort', async () => {
			const store = createIdempotencyStore(client);
			const slot = await store.acquire('order:1');
			expect(slot.acquired).toBe(true);
			expect(typeof slot.commit).toBe('function');
			expect(typeof slot.abort).toBe('function');
		});

		it('second acquire while pending reports pending=true', async () => {
			const store = createIdempotencyStore(client);
			await store.acquire('order:2');
			const second = await store.acquire('order:2');
			expect(second.acquired).toBe(false);
			expect(second.pending).toBe(true);
			expect(second.result).toBeUndefined();
		});

		it('after commit, subsequent acquires return the cached JSON-decoded result', async () => {
			const store = createIdempotencyStore(client);
			const first = await store.acquire('order:3');
			await first.commit({ id: 3, total: 99.99, items: ['a', 'b'] });

			const second = await store.acquire('order:3');
			expect(second.acquired).toBe(false);
			expect(second.pending).toBeUndefined();
			expect(second.result).toEqual({ id: 3, total: 99.99, items: ['a', 'b'] });
		});

		it('after abort, the next acquire owns the slot again', async () => {
			const store = createIdempotencyStore(client);
			const first = await store.acquire('order:4');
			await first.abort();
			const second = await store.acquire('order:4');
			expect(second.acquired).toBe(true);
		});
	});

	describe('SET NX EX semantics (real Redis)', () => {
		it('100 concurrent acquires of the same key yield exactly one acquired and 99 pending', async () => {
			const store = createIdempotencyStore(client);
			const slots = await Promise.all(
				Array.from({ length: 100 }, () => store.acquire('race:1'))
			);
			const acquired = slots.filter((s) => s.acquired);
			const pending = slots.filter((s) => !s.acquired && s.pending);
			expect(acquired).toHaveLength(1);
			expect(pending).toHaveLength(99);
		});

		it('pending sentinel respects the acquireTtl on the Redis key', async () => {
			const store = createIdempotencyStore(client, { acquireTtl: 30 });
			await store.acquire('pending-ttl');
			const ttl = await client.redis.ttl(client.key('idem:pending-ttl'));
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(30);
		});

		it('commit replaces the sentinel with the long ttl', async () => {
			const store = createIdempotencyStore(client, { acquireTtl: 30, ttl: 7200 });
			const slot = await store.acquire('commit-ttl');
			await slot.commit({ ok: true });
			const ttl = await client.redis.ttl(client.key('idem:commit-ttl'));
			expect(ttl).toBeGreaterThan(30);
			expect(ttl).toBeLessThanOrEqual(7200);
		});

		it('a pending sentinel that expires lets the next caller re-acquire', async () => {
			const store = createIdempotencyStore(client, { acquireTtl: 1 });
			await store.acquire('expire-me');

			// Wait well past the 1-second TTL. The slack absorbs clock drift
			// between the Redis EX timer and the JS event loop on Docker-on-
			// Windows under full-suite load.
			await wait(3000);

			const next = await store.acquire('expire-me');
			expect(next.acquired).toBe(true);
		});
	});

	describe('payload JSON round-trip through Redis strings', () => {
		it('handles primitives', async () => {
			const store = createIdempotencyStore(client);
			const a = await store.acquire('p:num');
			await a.commit(42);
			expect((await store.acquire('p:num')).result).toBe(42);

			const b = await store.acquire('p:str');
			await b.commit('hello');
			expect((await store.acquire('p:str')).result).toBe('hello');

			const c = await store.acquire('p:bool');
			await c.commit(false);
			expect((await store.acquire('p:bool')).result).toBe(false);
		});

		it('treats undefined as null on commit', async () => {
			const store = createIdempotencyStore(client);
			const a = await store.acquire('p:undef');
			await a.commit(undefined);
			expect((await store.acquire('p:undef')).result).toBe(null);
		});

		it('preserves nested objects and arrays verbatim', async () => {
			const store = createIdempotencyStore(client);
			const payload = { items: [1, { nested: 'value', arr: [true, null] }], count: 2 };
			const a = await store.acquire('p:nested');
			await a.commit(payload);
			expect((await store.acquire('p:nested')).result).toEqual(payload);
		});
	});

	describe('keyPrefix isolation', () => {
		it('default idem: prefix on the wire', async () => {
			const store = createIdempotencyStore(client);
			const slot = await store.acquire('k1');
			await slot.commit('done');
			const exists = await client.redis.exists(client.key('idem:k1'));
			expect(exists).toBe(1);
		});

		it('two stores with different prefixes do not collide', async () => {
			const a = createIdempotencyStore(client, { keyPrefix: 'a:' });
			const b = createIdempotencyStore(client, { keyPrefix: 'b:' });
			const sa = await a.acquire('shared');
			await sa.commit('A wins');

			const sb = await b.acquire('shared');
			expect(sb.acquired).toBe(true);
		});
	});

	describe('purge / clear (SCAN+UNLINK)', () => {
		it('purge drops a single committed result', async () => {
			const store = createIdempotencyStore(client);
			const a = await store.acquire('purge:1');
			await a.commit('cached');
			await store.purge('purge:1');
			const fresh = await store.acquire('purge:1');
			expect(fresh.acquired).toBe(true);
		});

		it('clear wipes only keys under the prefix and leaves the rest', async () => {
			const idem = createIdempotencyStore(client, { keyPrefix: 'idem:' });
			const other = createIdempotencyStore(client, { keyPrefix: 'other:' });

			const sa = await idem.acquire('k');
			await sa.commit('A');
			const sb = await other.acquire('k');
			await sb.commit('B');

			await idem.clear();

			expect((await idem.acquire('k')).acquired).toBe(true);
			expect((await other.acquire('k')).result).toBe('B');
		});
	});

	describe('corruption resilience', () => {
		it('a stored value that does not parse as JSON is reported as pending', async () => {
			const store = createIdempotencyStore(client);
			// Plant a non-JSON, non-sentinel value directly.
			await client.redis.set(client.key('idem:corrupt'), 'not-valid-json{');
			const slot = await store.acquire('corrupt');
			expect(slot.acquired).toBe(false);
			expect(slot.pending).toBe(true);
		});
	});
});
