/**
 * Integration tests for postgres/jobs against a real Postgres 16 server.
 *
 * The point of this file is the SKIP LOCKED claim path the in-memory mock
 * cannot truly reproduce: two or three concurrent claimers reading from
 * the same queue must produce strictly disjoint subsets of jobs, with no row
 * returned twice and none lost. The visibility-timeout reclaim, the
 * partial index plan choices, and the jsonb / timestamptz round-trip all
 * also exercise behaviour the mock fakes. The mock-based suite stays at
 * test/postgres/jobs.test.js; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createJobQueue } from '../../../postgres/jobs.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('postgres jobs (integration)', () => {
	let client;
	let queue;
	const TABLE = 'svti_jobs_inttest';

	beforeAll(async () => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
		queue = createJobQueue(client, { table: TABLE });
		// Materialise the table once so the per-test TRUNCATE in beforeEach
		// has something to truncate. The race-safety of CREATE TABLE itself
		// is exercised by the dedicated concurrent-first-use test below.
		await queue.enqueue('init', null);
		await queue.clear('init');
	});

	beforeEach(async () => {
		await client.query(`TRUNCATE ${TABLE} RESTART IDENTITY`);
	});

	afterAll(async () => {
		await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
		await client.end();
	});

	describe('concurrent first-use (CREATE TABLE race-safety)', () => {
		it('two fresh queues racing their first enqueue both succeed', async () => {
			// CREATE TABLE IF NOT EXISTS races on concurrent first calls.
			// ensureTable() catches 23505 / 42P07 / 42710 and treats the
			// table as already-existing.
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);

			const a = createJobQueue(client, { table: TABLE });
			const b = createJobQueue(client, { table: TABLE });

			const [idA, idB] = await Promise.all([
				a.enqueue('race', { from: 'a' }),
				b.enqueue('race', { from: 'b' })
			]);

			expect(idA).toBeTruthy();
			expect(idB).toBeTruthy();
			expect(String(idA)).not.toBe(String(idB));

			// Recreate the singleton queue for subsequent tests.
			queue = createJobQueue(client, { table: TABLE });
		});
	});

	describe('enqueue + claim round-trip', () => {
		it('returns claimed jobs in id order with payload, attempts, created_at', async () => {
			const a = await queue.enqueue('email', { to: 'a@b.c' });
			const b = await queue.enqueue('email', { to: 'b@b.c' });

			const claimed = await queue.claim('email', { batchSize: 10 });
			expect(claimed.map((j) => Number(j.id))).toEqual([Number(a), Number(b)]);
			expect(claimed[0].payload).toEqual({ to: 'a@b.c' });
			expect(claimed[1].payload).toEqual({ to: 'b@b.c' });
			expect(claimed[0].attempts).toBe(1);
			expect(claimed[1].attempts).toBe(1);
			expect(claimed[0].created_at).toBeInstanceOf(Date);
		});

		it('round-trips arbitrary jsonb payloads', async () => {
			const payload = {
				to: 'x@y.z',
				meta: { tags: ['a', 'b'], retries: 0, nested: { ok: true, n: 1.5 } },
				str: 'hello \u00e9 \u4e2d\u6587'
			};
			await queue.enqueue('email', payload);
			const [job] = await queue.claim('email');
			expect(job.payload).toEqual(payload);
		});

		it('null payload is preserved as JSON null', async () => {
			await queue.enqueue('ping', null);
			const [job] = await queue.claim('ping');
			expect(job.payload).toBe(null);
		});

		it('respects batchSize', async () => {
			for (let i = 0; i < 5; i++) await queue.enqueue('email', { i });
			const claimed = await queue.claim('email', { batchSize: 2 });
			expect(claimed).toHaveLength(2);
		});

		it('isolates queues (claim on A does not return B jobs)', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('image', { id: 2 });
			await queue.enqueue('image', { id: 3 });

			const emails = await queue.claim('email', { batchSize: 10 });
			expect(emails).toHaveLength(1);
			expect(emails[0].payload).toEqual({ id: 1 });

			const images = await queue.claim('image', { batchSize: 10 });
			expect(images).toHaveLength(2);
		});

		it('returns empty array when queue is empty', async () => {
			expect(await queue.claim('empty-q')).toEqual([]);
		});
	});

	describe('SKIP LOCKED concurrency', () => {
		it('two concurrent claimers split the queue with no overlap and no loss', async () => {
			for (let i = 0; i < 20; i++) {
				await queue.enqueue('split-2', { i });
			}

			const [a, b] = await Promise.all([
				queue.claim('split-2', { batchSize: 20 }),
				queue.claim('split-2', { batchSize: 20 })
			]);

			const idsA = a.map((j) => String(j.id));
			const idsB = b.map((j) => String(j.id));
			const all = [...idsA, ...idsB];

			expect(all.length).toBe(20);
			expect(new Set(all).size).toBe(20);
			// No row appears in both batches.
			const overlap = idsA.filter((id) => idsB.includes(id));
			expect(overlap).toEqual([]);
		});

		it('three concurrent claimers each get a disjoint slice', async () => {
			for (let i = 0; i < 30; i++) {
				await queue.enqueue('split-3', { i });
			}

			const batches = await Promise.all([
				queue.claim('split-3', { batchSize: 30 }),
				queue.claim('split-3', { batchSize: 30 }),
				queue.claim('split-3', { batchSize: 30 })
			]);

			const all = batches.flat().map((j) => String(j.id));
			expect(all.length).toBe(30);
			expect(new Set(all).size).toBe(30);
		});

		it('once a row is claimed within visibility, a second claim cannot see it', async () => {
			await queue.enqueue('once', { id: 1 });

			const first = await queue.claim('once', { visibilityTimeoutMs: 30000 });
			expect(first).toHaveLength(1);

			const second = await queue.claim('once');
			expect(second).toEqual([]);
		});
	});

	describe('visibility timeout reclaim', () => {
		it('a job whose visibility expired becomes claimable again', async () => {
			await queue.enqueue('vis', { id: 1 });
			const first = await queue.claim('vis', { visibilityTimeoutMs: 300 });
			expect(first).toHaveLength(1);

			await wait(1000);

			const reclaim = await queue.claim('vis');
			expect(reclaim).toHaveLength(1);
			expect(String(reclaim[0].id)).toBe(String(first[0].id));
			expect(reclaim[0].attempts).toBe(2);
		});

		it('extend pushes the deadline so a job inside its window is not re-claimed', async () => {
			await queue.enqueue('ext', { id: 1 });
			const [job] = await queue.claim('ext', { visibilityTimeoutMs: 200 });
			await queue.extend(job.id, 5000);

			await wait(300);
			const second = await queue.claim('ext');
			expect(second).toEqual([]);
		});

		it('extend on an unclaimed row is a no-op (claimed_at IS NOT NULL guard)', async () => {
			const id = await queue.enqueue('noextend', { id: 1 });
			await queue.extend(id, 5000);
			expect(await queue.pending('noextend')).toBe(1);
		});
	});

	describe('complete / fail', () => {
		it('complete removes the row entirely', async () => {
			const id = await queue.enqueue('done', { id: 1 });
			const [job] = await queue.claim('done');
			await queue.complete(job.id);

			const rows = await client.query(
				`SELECT svti_jobs_id FROM ${TABLE} WHERE svti_jobs_id = $1`, [id]
			);
			expect(rows.rowCount).toBe(0);
			expect(await queue.pending('done')).toBe(0);
		});

		it('fail releases the claim and preserves the attempts counter', async () => {
			await queue.enqueue('retry', { id: 1 });
			const c1 = await queue.claim('retry');
			expect(c1[0].attempts).toBe(1);

			await queue.fail(c1[0].id);

			const c2 = await queue.claim('retry');
			expect(c2).toHaveLength(1);
			expect(c2[0].attempts).toBe(2);
		});

		it('complete on an array of ids deletes all of them', async () => {
			await queue.enqueue('bulk', { id: 1 });
			await queue.enqueue('bulk', { id: 2 });
			const claimed = await queue.claim('bulk', { batchSize: 2 });

			await queue.complete(claimed.map((j) => j.id));
			expect(await queue.pending('bulk')).toBe(0);
		});
	});

	describe('partial index drives pending lookups', () => {
		it('pending() and claim() both ignore claimed rows in the same queue', async () => {
			await queue.enqueue('idx', { id: 1 });
			await queue.enqueue('idx', { id: 2 });
			await queue.enqueue('idx', { id: 3 });
			await queue.claim('idx', { batchSize: 1 });

			expect(await queue.pending('idx')).toBe(2);

			const more = await queue.claim('idx', { batchSize: 10 });
			expect(more).toHaveLength(2);
		});

		it('pending across all queues sums only unclaimed rows', async () => {
			await queue.enqueue('q1', { id: 1 });
			await queue.enqueue('q2', { id: 2 });
			await queue.enqueue('q3', { id: 3 });
			await queue.claim('q1');

			expect(await queue.pending()).toBe(2);
		});
	});

	describe('clear', () => {
		it('clear with a queue name only drops that queue', async () => {
			await queue.enqueue('drop-me', { id: 1 });
			await queue.enqueue('keep-me', { id: 2 });
			await queue.clear('drop-me');

			expect(await queue.pending('drop-me')).toBe(0);
			expect(await queue.pending('keep-me')).toBe(1);
		});
	});
});
