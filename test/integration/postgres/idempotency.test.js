/**
 * Integration tests for postgres/idempotency against a real Postgres 16 server.
 *
 * Exercises the actual INSERT ... ON CONFLICT (key) DO UPDATE WHERE
 * expires_at < now() race resistance and the timestamptz / interval
 * arithmetic the mock approximates with Date.now(). Concurrent acquires
 * on the same key must elect exactly one winner; the rest must report
 * pending. Real Postgres timestamps + intervals are also exercised so
 * that committed results, pending takeovers, and expired-row sweeping
 * all line up with the SQL the module ships.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createIdempotencyStore } from '../../../postgres/idempotency.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('postgres idempotency (integration)', () => {
	let client;
	let store;
	const TABLE = 'svti_idempotency_inttest';

	beforeAll(() => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Drop and recreate per test so DDL is exercised and rows from
		// other tests cannot leak in.
		await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
		store = createIdempotencyStore(client, { table: TABLE, cleanupInterval: 0 });
	});

	afterEach(() => {
		if (store) store.destroy();
	});

	afterAll(async () => {
		await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
		await client.end();
	});

	describe('concurrent first-use (CREATE TABLE race-safety)', () => {
		it('two fresh stores racing their first acquire both succeed', async () => {
			// CREATE TABLE IF NOT EXISTS races on concurrent first calls.
			// ensureTable() catches 23505 / 42P07 / 42710 and treats the
			// table as already-existing.
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);

			const a = createIdempotencyStore(client, { table: TABLE, cleanupInterval: 0 });
			const b = createIdempotencyStore(client, { table: TABLE, cleanupInterval: 0 });

			const [first, second] = await Promise.all([
				a.acquire('race-key'),
				b.acquire('race-key')
			]);

			// Exactly one should be acquired; the other sees pending.
			const acquired = [first, second].filter((r) => r && r.acquired === true).length;
			const pending = [first, second].filter((r) => r && r.acquired === false && r.pending === true).length;
			expect(acquired).toBe(1);
			expect(pending).toBe(1);

			a.destroy();
			b.destroy();
		});
	});

	describe('basic acquire flow', () => {
		it('first acquire wins; second is pending', async () => {
			const first = await store.acquire('order:1');
			expect(first.acquired).toBe(true);

			const second = await store.acquire('order:1');
			expect(second.acquired).toBe(false);
			expect(second.pending).toBe(true);
		});

		it('after commit, subsequent acquires return the cached jsonb result', async () => {
			const first = await store.acquire('order:2');
			await first.commit({ id: 2, total: 99.99, items: ['a', 'b'] });

			const second = await store.acquire('order:2');
			expect(second.acquired).toBe(false);
			expect(second.result).toEqual({ id: 2, total: 99.99, items: ['a', 'b'] });
		});

		it('after abort, the next acquire owns the slot again', async () => {
			const first = await store.acquire('order:3');
			await first.abort();

			const second = await store.acquire('order:3');
			expect(second.acquired).toBe(true);
		});

		it('purge drops a single committed result', async () => {
			const a = await store.acquire('k');
			await a.commit('cached');
			expect((await store.acquire('k')).result).toBe('cached');

			await store.purge('k');
			const fresh = await store.acquire('k');
			expect(fresh.acquired).toBe(true);
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

	describe('jsonb result round-trip', () => {
		it('preserves primitives, null, and undefined-as-null', async () => {
			const a = await store.acquire('a');
			await a.commit(42);
			expect((await store.acquire('a')).result).toBe(42);

			const b = await store.acquire('b');
			await b.commit(null);
			expect((await store.acquire('b')).result).toBe(null);

			const c = await store.acquire('c');
			await c.commit(undefined);
			expect((await store.acquire('c')).result).toBe(null);

			const d = await store.acquire('d');
			await d.commit('hello \u00e9 \u4e2d\u6587');
			expect((await store.acquire('d')).result).toBe('hello \u00e9 \u4e2d\u6587');

			const e = await store.acquire('e');
			await e.commit(true);
			expect((await store.acquire('e')).result).toBe(true);
		});

		it('preserves nested objects and arrays', async () => {
			const payload = {
				items: [1, { nested: 'value', deeper: { x: [null, false, 0] } }],
				count: 2
			};
			const a = await store.acquire('k');
			await a.commit(payload);
			expect((await store.acquire('k')).result).toEqual(payload);
		});
	});

	describe('ON CONFLICT race resistance', () => {
		it('concurrent acquires on the same key elect exactly one winner', async () => {
			const results = await Promise.all(
				Array.from({ length: 10 }, () => store.acquire('race:1'))
			);

			const winners = results.filter((r) => r.acquired);
			const pending = results.filter((r) => !r.acquired && r.pending);

			expect(winners).toHaveLength(1);
			expect(pending).toHaveLength(9);
		});

		it('high-fan-out concurrent acquires across many keys keep one winner each', async () => {
			const keys = Array.from({ length: 20 }, (_, i) => `multi:${i}`);
			const results = await Promise.all(
				keys.flatMap((k) =>
					Array.from({ length: 5 }, () => store.acquire(k).then((r) => ({ k, r })))
				)
			);

			const byKey = new Map();
			for (const { k, r } of results) {
				if (!byKey.has(k)) byKey.set(k, []);
				byKey.get(k).push(r);
			}

			for (const [, rs] of byKey) {
				const winners = rs.filter((r) => r.acquired);
				expect(winners).toHaveLength(1);
				expect(rs.filter((r) => !r.acquired && r.pending).length).toBe(rs.length - 1);
			}
		});

		it('after commit, late concurrent acquires all see the cached result', async () => {
			const owner = await store.acquire('cached');
			await owner.commit({ value: 'final' });

			const results = await Promise.all(
				Array.from({ length: 8 }, () => store.acquire('cached'))
			);

			expect(results.every((r) => !r.acquired)).toBe(true);
			expect(results.every((r) => r.pending !== true)).toBe(true);
			for (const r of results) {
				expect(r.result).toEqual({ value: 'final' });
			}
		});
	});

	describe('expires_at semantics with real timestamptz / interval', () => {
		it('acquireTtl expiry lets a new caller take over a stuck pending row', async () => {
			const s = createIdempotencyStore(client, {
				table: TABLE, acquireTtl: 1, cleanupInterval: 0
			});
			try {
				const first = await s.acquire('stuck');
				expect(first.acquired).toBe(true);
				// Owner crashes; do not commit/abort.

				// Wait past the 1s acquireTtl on real Postgres clock.
				await wait(1100);

				const second = await s.acquire('stuck');
				expect(second.acquired).toBe(true);
			} finally {
				s.destroy();
			}
		});

		it('committed row whose ttl has passed is treated as missing', async () => {
			const s = createIdempotencyStore(client, {
				table: TABLE, ttl: 1, cleanupInterval: 0
			});
			try {
				const first = await s.acquire('ephemeral');
				await first.commit('temp');
				expect((await s.acquire('ephemeral')).result).toBe('temp');

				await wait(1100);

				const fresh = await s.acquire('ephemeral');
				expect(fresh.acquired).toBe(true);
			} finally {
				s.destroy();
			}
		});

		it('pending row inside its acquireTtl blocks other callers as pending', async () => {
			const s = createIdempotencyStore(client, {
				table: TABLE, acquireTtl: 30, cleanupInterval: 0
			});
			try {
				const first = await s.acquire('held');
				expect(first.acquired).toBe(true);

				const second = await s.acquire('held');
				expect(second.acquired).toBe(false);
				expect(second.pending).toBe(true);
			} finally {
				s.destroy();
			}
		});

		it('expires_at column moves forward to ttl after commit', async () => {
			const s = createIdempotencyStore(client, {
				table: TABLE, ttl: 7200, acquireTtl: 5, cleanupInterval: 0
			});
			try {
				const first = await s.acquire('exp');
				const beforeCommit = await client.query(
					`SELECT expires_at FROM ${TABLE} WHERE svti_idempotency_key = $1`, ['exp']
				);
				const beforeMs = new Date(beforeCommit.rows[0].expires_at).getTime();

				await first.commit('done');

				const afterCommit = await client.query(
					`SELECT expires_at, status FROM ${TABLE} WHERE svti_idempotency_key = $1`, ['exp']
				);
				const afterMs = new Date(afterCommit.rows[0].expires_at).getTime();

				expect(afterCommit.rows[0].status).toBe('committed');
				// Commit pushed the deadline by at least an hour past the
				// acquire deadline; tolerate clock drift.
				expect(afterMs - beforeMs).toBeGreaterThan(3600 * 1000);
			} finally {
				s.destroy();
			}
		});
	});

	describe('cleanup sweep', () => {
		it('periodic cleanup deletes rows whose expires_at has passed', async () => {
			const s = createIdempotencyStore(client, {
				table: TABLE, acquireTtl: 1, cleanupInterval: 200
			});
			try {
				await s.acquire('sweep:1');
				await s.acquire('sweep:2');
				let count = await client.query(
					`SELECT COUNT(*)::int AS n FROM ${TABLE} WHERE svti_idempotency_key LIKE 'sweep:%'`
				);
				expect(count.rows[0].n).toBe(2);

				await wait(1500);

				count = await client.query(
					`SELECT COUNT(*)::int AS n FROM ${TABLE} WHERE svti_idempotency_key LIKE 'sweep:%'`
				);
				expect(count.rows[0].n).toBe(0);
			} finally {
				s.destroy();
			}
		});
	});
});
