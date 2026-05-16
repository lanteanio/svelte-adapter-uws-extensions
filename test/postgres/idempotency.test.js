import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createIdempotencyStore } from '../../postgres/idempotency.js';

describe('postgres idempotency', () => {
	let client;
	let store;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockPgClient();
		store = createIdempotencyStore(client, { cleanupInterval: 0 });
	});

	afterEach(() => {
		store.destroy();
	});

	describe('createIdempotencyStore', () => {
		it('returns a store with the expected API', () => {
			expect(typeof store.acquire).toBe('function');
			expect(typeof store.purge).toBe('function');
			expect(typeof store.clear).toBe('function');
			expect(typeof store.destroy).toBe('function');
		});

		it('throws on non-positive ttl', () => {
			expect(() => createIdempotencyStore(client, { ttl: 0, cleanupInterval: 0 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { ttl: -1, cleanupInterval: 0 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { ttl: 1.5, cleanupInterval: 0 })).toThrow('positive integer');
		});

		it('throws on non-positive acquireTtl', () => {
			expect(() => createIdempotencyStore(client, { acquireTtl: 0, cleanupInterval: 0 })).toThrow('positive integer');
			expect(() => createIdempotencyStore(client, { acquireTtl: -1, cleanupInterval: 0 })).toThrow('positive integer');
		});

		it('throws on invalid table name', () => {
			expect(() => createIdempotencyStore(client, { table: 'drop table;--', cleanupInterval: 0 })).toThrow('invalid table name');
			expect(() => createIdempotencyStore(client, { table: '123bad', cleanupInterval: 0 })).toThrow('invalid table name');
		});

		it('throws on reserved Postgres schema names (pg_*, information_schema*)', () => {
			expect(() => createIdempotencyStore(client, { table: 'pg_class', cleanupInterval: 0 })).toThrow('reserved Postgres schema');
			expect(() => createIdempotencyStore(client, { table: 'pg_authid', cleanupInterval: 0 })).toThrow('reserved Postgres schema');
			expect(() => createIdempotencyStore(client, { table: 'PG_TYPE', cleanupInterval: 0 })).toThrow('reserved Postgres schema');
			expect(() => createIdempotencyStore(client, { table: 'information_schema_tables', cleanupInterval: 0 })).toThrow('reserved Postgres schema');
			expect(() => createIdempotencyStore(client, { table: 'INFORMATION_SCHEMA_columns', cleanupInterval: 0 })).toThrow('reserved Postgres schema');
		});

		it('throws on invalid maxResultBytes', () => {
			expect(() => createIdempotencyStore(client, { maxResultBytes: 0, cleanupInterval: 0 })).toThrow('maxResultBytes');
			expect(() => createIdempotencyStore(client, { maxResultBytes: -1, cleanupInterval: 0 })).toThrow('maxResultBytes');
			expect(() => createIdempotencyStore(client, { maxResultBytes: 1.5, cleanupInterval: 0 })).toThrow('maxResultBytes');
		});

		it('accepts Infinity for maxResultBytes (opt-out)', () => {
			expect(() => createIdempotencyStore(client, { maxResultBytes: Infinity, cleanupInterval: 0 })).not.toThrow();
		});
	});

	describe('maxResultBytes cap', () => {
		it('commit rejects results exceeding the default 256 KB cap', async () => {
			const s = createIdempotencyStore(client, { cleanupInterval: 0 });
			const slot = await s.acquire('k-too-big');
			expect(slot.acquired).toBe(true);
			const tooBig = { payload: 'x'.repeat(257 * 1024) };
			await expect(slot.commit(tooBig)).rejects.toMatchObject({
				name: 'IdempotencyResultTooLargeError',
				code: 'IDEMPOTENCY_RESULT_TOO_LARGE'
			});
			s.destroy();
		});

		it('commit succeeds for results under the cap', async () => {
			const s = createIdempotencyStore(client, { cleanupInterval: 0 });
			const slot = await s.acquire('k-ok');
			expect(slot.acquired).toBe(true);
			await expect(slot.commit({ result: 'small' })).resolves.toBeUndefined();
			s.destroy();
		});

		it('error carries bytes and maxBytes properties', async () => {
			const s = createIdempotencyStore(client, { maxResultBytes: 10, cleanupInterval: 0 });
			const slot = await s.acquire('k-err');
			expect(slot.acquired).toBe(true);
			try {
				await slot.commit({ longField: 'longer than ten bytes' });
				throw new Error('should have rejected');
			} catch (err) {
				expect(err.code).toBe('IDEMPOTENCY_RESULT_TOO_LARGE');
				expect(err.maxBytes).toBe(10);
				expect(err.bytes).toBeGreaterThan(10);
			}
			s.destroy();
		});

		it('failed commit does NOT issue an UPDATE (row stays pending)', async () => {
			const s = createIdempotencyStore(client, { maxResultBytes: 10, cleanupInterval: 0 });
			const slot = await s.acquire('k-pending');
			const querySpy = vi.spyOn(client, 'query');
			await expect(slot.commit({ x: 'too long' })).rejects.toMatchObject({ code: 'IDEMPOTENCY_RESULT_TOO_LARGE' });
			// commit() should have thrown BEFORE calling client.query for the UPDATE.
			const commitCalls = querySpy.mock.calls.filter(([q]) => {
				const text = typeof q === 'string' ? q : q?.text;
				return text && text.includes('SET status');
			});
			expect(commitCalls.length).toBe(0);
			s.destroy();
		});

		it('Infinity disables the cap', async () => {
			const s = createIdempotencyStore(client, { maxResultBytes: Infinity, cleanupInterval: 0 });
			const slot = await s.acquire('k-huge');
			expect(slot.acquired).toBe(true);
			const huge = { payload: 'x'.repeat(1024 * 1024) };
			await expect(slot.commit(huge)).resolves.toBeUndefined();
			s.destroy();
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
		it('handles primitives, null, and undefined-as-null', async () => {
			const a = await store.acquire('a');
			await a.commit(42);
			expect((await store.acquire('a')).result).toBe(42);

			const b = await store.acquire('b');
			await b.commit(null);
			expect((await store.acquire('b')).result).toBe(null);

			const c = await store.acquire('c');
			await c.commit(undefined);
			expect((await store.acquire('c')).result).toBe(null);
		});

		it('handles nested objects and arrays', async () => {
			const a = await store.acquire('k');
			const payload = { items: [1, { nested: 'value' }], count: 2 };
			await a.commit(payload);
			expect((await store.acquire('k')).result).toEqual(payload);
		});
	});

	describe('takeover after expiration', () => {
		it('a pending row whose acquireTtl has passed can be taken over', async () => {
			const s = createIdempotencyStore(client, { acquireTtl: 1, cleanupInterval: 0 });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			const first = await s.acquire('k');
			expect(first.acquired).toBe(true);
			// Owner crashes; slot is left pending.

			// Advance time past the acquireTtl (1 second).
			Date.now.mockReturnValue(now + 2000);

			const second = await s.acquire('k');
			expect(second.acquired).toBe(true);

			s.destroy();
		});

		it('a committed row whose ttl has passed is treated as missing', async () => {
			const s = createIdempotencyStore(client, { ttl: 1, cleanupInterval: 0 });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			const first = await s.acquire('k');
			await first.commit('cached');
			expect((await s.acquire('k')).result).toBe('cached');

			// Advance time past the result ttl (1 second).
			Date.now.mockReturnValue(now + 2000);

			const fresh = await s.acquire('k');
			expect(fresh.acquired).toBe(true);

			s.destroy();
		});
	});

	describe('cleanup', () => {
		it('cleanup removes expired rows', async () => {
			const s = createIdempotencyStore(client, { acquireTtl: 1, cleanupInterval: 50 });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await s.acquire('k1');
			await s.acquire('k2');
			expect(client._getIdemRows().size).toBe(2);

			Date.now.mockReturnValue(now + 2000);

			// Wait for one cleanup tick.
			await new Promise((r) => setTimeout(r, 80));

			expect(client._getIdemRows().size).toBe(0);

			s.destroy();
		});

		it('disabling cleanup keeps the timer off', () => {
			const s = createIdempotencyStore(client, { cleanupInterval: 0 });
			s.destroy();
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

		it('purge throws on empty key', async () => {
			await expect(store.purge('')).rejects.toThrow('non-empty');
		});

		it('clear drops every row in the table', async () => {
			const a = await store.acquire('k1');
			await a.commit(1);
			const b = await store.acquire('k2');
			await b.commit(2);

			await store.clear();

			expect((await store.acquire('k1')).acquired).toBe(true);
			expect((await store.acquire('k2')).acquired).toBe(true);
		});
	});

	describe('ready()', () => {
		it('resolves once the table migration completes', async () => {
			await expect(store.ready()).resolves.toBeUndefined();
		});

		it('is idempotent', async () => {
			await store.ready();
			await store.ready();
		});
	});
});
