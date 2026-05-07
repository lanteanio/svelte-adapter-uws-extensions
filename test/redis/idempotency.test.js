import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createIdempotencyStore } from '../../redis/idempotency.js';

describe('redis idempotency', () => {
	let client;
	let store;

	beforeEach(() => {
		client = mockRedisClient('test:');
		store = createIdempotencyStore(client);
	});

	describe('createIdempotencyStore', () => {
		it('returns a store with the expected API', () => {
			expect(typeof store.acquire).toBe('function');
			expect(typeof store.purge).toBe('function');
			expect(typeof store.clear).toBe('function');
		});

		it('throws on non-positive ttl', () => {
			expect(() => createIdempotencyStore(client, { ttl: 0 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { ttl: -1 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { ttl: 1.5 })).toThrow('positive integer');
		});

		it('throws on non-positive acquireTtl', () => {
			expect(() => createIdempotencyStore(client, { acquireTtl: 0 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { acquireTtl: -1 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { acquireTtl: 1.5 })).toThrow('positive integer');
		});

		it('throws on non-string keyPrefix', () => {
			expect(() => createIdempotencyStore(client, { keyPrefix: 123 })).toThrow('keyPrefix');
		});

		it('accepts an empty keyPrefix', () => {
			const s = createIdempotencyStore(client, { keyPrefix: '' });
			expect(typeof s.acquire).toBe('function');
		});
	});

	describe('acquire - basic flow', () => {
		it('first acquire returns acquired with commit and abort', async () => {
			const slot = await store.acquire('order:42');
			expect(slot.acquired).toBe(true);
			expect(typeof slot.commit).toBe('function');
			expect(typeof slot.abort).toBe('function');
		});

		it('throws on empty key', async () => {
			await expect(store.acquire('')).rejects.toThrow('non-empty');
			await expect(store.acquire(null)).rejects.toThrow('non-empty');
			await expect(store.acquire(undefined)).rejects.toThrow('non-empty');
			await expect(store.acquire(42)).rejects.toThrow('non-empty');
		});

		it('second acquire while pending returns pending', async () => {
			await store.acquire('order:42');
			const second = await store.acquire('order:42');
			expect(second.acquired).toBe(false);
			expect(second.pending).toBe(true);
			expect(second.result).toBeUndefined();
		});

		it('after commit, subsequent acquires return the cached result', async () => {
			const first = await store.acquire('order:42');
			await first.commit({ orderId: 42, total: 99.99 });

			const second = await store.acquire('order:42');
			expect(second.acquired).toBe(false);
			expect(second.pending).toBeUndefined();
			expect(second.result).toEqual({ orderId: 42, total: 99.99 });
		});

		it('after abort, next acquire owns the slot again', async () => {
			const first = await store.acquire('order:42');
			await first.abort();

			const second = await store.acquire('order:42');
			expect(second.acquired).toBe(true);
		});
	});

	describe('result types', () => {
		it('handles primitives', async () => {
			const a = await store.acquire('k1');
			await a.commit(42);
			expect((await store.acquire('k1')).result).toBe(42);

			const b = await store.acquire('k2');
			await b.commit('hello');
			expect((await store.acquire('k2')).result).toBe('hello');

			const c = await store.acquire('k3');
			await c.commit(true);
			expect((await store.acquire('k3')).result).toBe(true);
		});

		it('handles null', async () => {
			const a = await store.acquire('k');
			await a.commit(null);
			const b = await store.acquire('k');
			expect(b.acquired).toBe(false);
			expect(b.result).toBe(null);
		});

		it('treats undefined as null', async () => {
			const a = await store.acquire('k');
			await a.commit(undefined);
			const b = await store.acquire('k');
			expect(b.acquired).toBe(false);
			expect(b.result).toBe(null);
		});

		it('handles arrays and nested objects', async () => {
			const a = await store.acquire('k');
			const payload = { items: [1, { nested: 'value' }], count: 2 };
			await a.commit(payload);
			expect((await store.acquire('k')).result).toEqual(payload);
		});
	});

	describe('keyPrefix', () => {
		it('uses the default idem: prefix', async () => {
			await store.acquire('order:42');
			const allKeys = [...client._store.keys()];
			expect(allKeys).toContain('test:idem:order:42');
		});

		it('respects a custom keyPrefix', async () => {
			const s = createIdempotencyStore(client, { keyPrefix: 'rpc:' });
			await s.acquire('charge');
			const allKeys = [...client._store.keys()];
			expect(allKeys).toContain('test:rpc:charge');
		});

		it('isolates stores with different prefixes', async () => {
			const a = createIdempotencyStore(client, { keyPrefix: 'a:' });
			const b = createIdempotencyStore(client, { keyPrefix: 'b:' });

			const slotA = await a.acquire('shared-key');
			await slotA.commit('A wins');

			// Same key under a different prefix is a different slot
			const slotB = await b.acquire('shared-key');
			expect(slotB.acquired).toBe(true);
		});
	});

	describe('purge / clear', () => {
		it('purge drops a single committed result', async () => {
			const a = await store.acquire('k');
			await a.commit('cached');
			expect((await store.acquire('k')).result).toBe('cached');

			await store.purge('k');
			const fresh = await store.acquire('k');
			expect(fresh.acquired).toBe(true);
		});

		it('purge on a missing key is a no-op', async () => {
			await store.purge('nope');
		});

		it('purge throws on empty key', async () => {
			await expect(store.purge('')).rejects.toThrow('non-empty');
		});

		it('clear drops every key under the prefix', async () => {
			const a = await store.acquire('k1');
			await a.commit(1);
			const b = await store.acquire('k2');
			await b.commit(2);

			await store.clear();

			expect((await store.acquire('k1')).acquired).toBe(true);
			expect((await store.acquire('k2')).acquired).toBe(true);
		});

		it('clear only touches keys under the configured prefix', async () => {
			const idem = createIdempotencyStore(client, { keyPrefix: 'idem:' });
			const other = createIdempotencyStore(client, { keyPrefix: 'other:' });

			const a = await idem.acquire('k');
			await a.commit('A');
			const b = await other.acquire('k');
			await b.commit('B');

			await idem.clear();

			expect((await idem.acquire('k')).acquired).toBe(true);
			// Other store untouched
			expect((await other.acquire('k')).result).toBe('B');
		});
	});

	describe('commit semantics', () => {
		it('commit overwrites a prior pending sentinel', async () => {
			const a = await store.acquire('k');
			// Before commit, a peek would see pending
			const peek = await store.acquire('k');
			expect(peek.pending).toBe(true);

			await a.commit('done');

			// After commit, it's a result
			const after = await store.acquire('k');
			expect(after.result).toBe('done');
		});

		it('a stored value that does not parse as JSON is treated as missing', async () => {
			// Plant a corrupted entry directly
			client._store.set('test:idem:k', 'not-valid-json{');

			const slot = await store.acquire('k');
			// Mock returns the raw value; the store falls back to pending
			// rather than throwing, so callers can recover.
			expect(slot.acquired).toBe(false);
			expect(slot.pending).toBe(true);
		});
	});

	describe('ready()', () => {
		it('resolves immediately (no DDL on Redis)', async () => {
			await expect(store.ready()).resolves.toBeUndefined();
		});
	});
});
