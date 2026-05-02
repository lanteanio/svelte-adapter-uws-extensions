import { describe, it, expect, beforeEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createJobQueue } from '../../postgres/jobs.js';
import { createCircuitBreaker, CircuitBrokenError } from '../../shared/breaker.js';

describe('postgres job queue', () => {
	let client;
	let queue;

	beforeEach(() => {
		client = mockPgClient();
		queue = createJobQueue(client);
	});

	describe('construction', () => {
		it('rejects bad table names', () => {
			expect(() => createJobQueue(client, { table: 'drop table;--' })).toThrow('invalid table name');
		});

		it('rejects non-positive visibilityTimeout', () => {
			expect(() => createJobQueue(client, { visibilityTimeout: 0 })).toThrow('positive number');
		});

		it('returns the expected API shape', () => {
			expect(typeof queue.enqueue).toBe('function');
			expect(typeof queue.claim).toBe('function');
			expect(typeof queue.complete).toBe('function');
			expect(typeof queue.fail).toBe('function');
			expect(typeof queue.extend).toBe('function');
			expect(typeof queue.pending).toBe('function');
			expect(typeof queue.clear).toBe('function');
			expect(typeof queue.destroy).toBe('function');
		});
	});

	describe('enqueue', () => {
		it('rejects an empty queue name', async () => {
			await expect(queue.enqueue('', { x: 1 })).rejects.toThrow('non-empty string');
		});

		it('returns the inserted job id', async () => {
			const id = await queue.enqueue('email', { to: 'a@b.c' });
			expect(typeof id).toBe('number');
			expect(id).toBeGreaterThan(0);
		});

		it('stores the payload', async () => {
			await queue.enqueue('email', { to: 'a@b.c', subject: 'hi' });
			const claimed = await queue.claim('email');
			expect(claimed[0].payload).toEqual({ to: 'a@b.c', subject: 'hi' });
		});

		it('handles null payload', async () => {
			await queue.enqueue('ping', null);
			const claimed = await queue.claim('ping');
			expect(claimed[0].payload).toBe(null);
		});

		it('persists explicit requestId on the row', async () => {
			await queue.enqueue('email', { x: 1 }, { requestId: 'req-job-1' });
			const claimed = await queue.claim('email');
			expect(claimed[0].requestId).toBe('req-job-1');
		});

		it('extracts requestId from a passed platform.requestId', async () => {
			await queue.enqueue('email', { x: 1 }, { platform: { requestId: 'req-from-platform' } });
			const claimed = await queue.claim('email');
			expect(claimed[0].requestId).toBe('req-from-platform');
		});

		it('explicit requestId wins over platform.requestId when both are given', async () => {
			await queue.enqueue('email', null, {
				requestId: 'req-explicit',
				platform: { requestId: 'req-from-platform' }
			});
			const claimed = await queue.claim('email');
			expect(claimed[0].requestId).toBe('req-explicit');
		});

		it('returns requestId: null on jobs enqueued without a requestId', async () => {
			await queue.enqueue('email', { x: 1 });
			const claimed = await queue.claim('email');
			expect(claimed[0].requestId).toBeNull();
		});

		it('treats empty-string requestId as null', async () => {
			await queue.enqueue('email', null, { requestId: '' });
			const claimed = await queue.claim('email');
			expect(claimed[0].requestId).toBeNull();
		});
	});

	describe('claim', () => {
		it('returns pending jobs in id order (FIFO)', async () => {
			const a = await queue.enqueue('email', { id: 1 });
			const b = await queue.enqueue('email', { id: 2 });
			const c = await queue.enqueue('email', { id: 3 });

			const claimed = await queue.claim('email', { batchSize: 10 });
			expect(claimed.map((j) => j.id)).toEqual([a, b, c]);
		});

		it('respects batchSize', async () => {
			for (let i = 0; i < 5; i++) await queue.enqueue('email', { i });
			const claimed = await queue.claim('email', { batchSize: 2 });
			expect(claimed).toHaveLength(2);
		});

		it('does not return jobs that are already claimed and within visibility window', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.claim('email');

			const second = await queue.claim('email');
			expect(second).toEqual([]);
		});

		it('re-claims jobs whose visibility has expired', async () => {
			await queue.enqueue('email', { id: 1 });
			const first = await queue.claim('email', { visibilityTimeoutMs: 1 });
			expect(first).toHaveLength(1);

			await new Promise((r) => setTimeout(r, 5));

			const reclaim = await queue.claim('email');
			expect(reclaim).toHaveLength(1);
			expect(reclaim[0].id).toBe(first[0].id);
			expect(reclaim[0].attempts).toBe(2);
		});

		it('increments attempts on each claim', async () => {
			const id = await queue.enqueue('email', { id: 1 });
			const c1 = await queue.claim('email', { visibilityTimeoutMs: 1 });
			expect(c1[0].attempts).toBe(1);

			await new Promise((r) => setTimeout(r, 5));
			const c2 = await queue.claim('email');
			expect(c2[0].attempts).toBe(2);
		});

		it('isolates queues (claim on A does not return B jobs)', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('image', { id: 2 });

			const emails = await queue.claim('email', { batchSize: 10 });
			expect(emails).toHaveLength(1);
			expect(emails[0].payload).toEqual({ id: 1 });
		});

		it('returns empty array when queue is empty', async () => {
			expect(await queue.claim('email')).toEqual([]);
		});

		it('rejects non-integer batchSize', async () => {
			await expect(queue.claim('email', { batchSize: 1.5 })).rejects.toThrow('positive integer');
		});

		it('rejects non-positive visibilityTimeoutMs', async () => {
			await expect(queue.claim('email', { visibilityTimeoutMs: 0 })).rejects.toThrow('positive number');
		});
	});

	describe('complete', () => {
		it('deletes the claimed job', async () => {
			const id = await queue.enqueue('email', { id: 1 });
			const [job] = await queue.claim('email');
			await queue.complete(job.id);

			expect(await queue.pending('email')).toBe(0);
			expect(client._getJobRows().has(Number(id))).toBe(false);
		});

		it('handles array of ids', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('email', { id: 2 });
			const claimed = await queue.claim('email', { batchSize: 2 });

			await queue.complete(claimed.map((j) => j.id));
			expect(await queue.pending('email')).toBe(0);
		});

		it('is a no-op when given an empty array', async () => {
			await queue.complete([]);  // should not throw
		});
	});

	describe('fail', () => {
		it('releases the claim so the job becomes claimable again', async () => {
			await queue.enqueue('email', { id: 1 });
			const [job] = await queue.claim('email');
			await queue.fail(job.id);

			const reclaim = await queue.claim('email');
			expect(reclaim).toHaveLength(1);
			expect(reclaim[0].id).toBe(job.id);
		});

		it('preserves the attempts counter (caller decides when to give up)', async () => {
			await queue.enqueue('email', { id: 1 });
			const c1 = await queue.claim('email');
			await queue.fail(c1[0].id);
			const c2 = await queue.claim('email');
			expect(c2[0].attempts).toBe(2);
		});

		it('handles array of ids', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('email', { id: 2 });
			const claimed = await queue.claim('email', { batchSize: 2 });
			await queue.fail(claimed.map((j) => j.id));
			const reclaim = await queue.claim('email', { batchSize: 2 });
			expect(reclaim).toHaveLength(2);
		});
	});

	describe('extend', () => {
		it('pushes back the visibility deadline', async () => {
			await queue.enqueue('email', { id: 1 });
			const [job] = await queue.claim('email', { visibilityTimeoutMs: 1 });
			await queue.extend(job.id, 5000);

			await new Promise((r) => setTimeout(r, 5));
			// Without extend the job would have been re-claimable; with extend it stays claimed.
			const second = await queue.claim('email');
			expect(second).toEqual([]);
		});

		it('rejects non-positive additionalMs', async () => {
			await expect(queue.extend(1, 0)).rejects.toThrow('positive number');
		});

		it('does not affect unclaimed rows (claimed_at IS NOT NULL guard)', async () => {
			const id = await queue.enqueue('email', { id: 1 });
			await queue.extend(id, 5000);  // no-op since not claimed
			expect(await queue.pending('email')).toBe(1);
		});
	});

	describe('pending', () => {
		it('counts unclaimed jobs in a queue', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('email', { id: 2 });
			await queue.enqueue('image', { id: 3 });

			expect(await queue.pending('email')).toBe(2);
			expect(await queue.pending('image')).toBe(1);
		});

		it('counts across all queues when no name is given', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('image', { id: 2 });

			expect(await queue.pending()).toBe(2);
		});

		it('does not count claimed jobs', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('email', { id: 2 });
			await queue.claim('email');

			expect(await queue.pending('email')).toBe(1);
		});
	});

	describe('clear', () => {
		it('clears one queue', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('image', { id: 2 });

			await queue.clear('email');
			expect(await queue.pending('email')).toBe(0);
			expect(await queue.pending('image')).toBe(1);
		});

		it('clears all queues', async () => {
			await queue.enqueue('email', { id: 1 });
			await queue.enqueue('image', { id: 2 });

			await queue.clear();
			expect(await queue.pending()).toBe(0);
		});
	});

	describe('breaker accounting', () => {
		it('claim throws CircuitBrokenError when broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const q = createJobQueue(client, { breaker });
			await expect(q.claim('email')).rejects.toBeInstanceOf(CircuitBrokenError);

			breaker.destroy();
		});

		it('records failure when query fails', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const q = createJobQueue(client, { breaker });

			const orig = client.query;
			client.query = async () => { throw new Error('connection lost'); };

			await expect(q.enqueue('email', { id: 1 })).rejects.toThrow('connection lost');
			expect(breaker.failures).toBe(1);

			client.query = orig;
			breaker.destroy();
		});
	});

	describe('metrics', () => {
		it('counts enqueue, claim, complete, fail per queue', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const q = createJobQueue(client, { metrics });

			await q.enqueue('email', { id: 1 });
			await q.enqueue('email', { id: 2 });
			const claimed = await q.claim('email', { batchSize: 2 });
			await q.complete(claimed[0].id);
			await q.fail(claimed[1].id);

			const out = metrics.serialize();
			expect(out).toMatch(/jobs_enqueued_total\{queue="email"\} 2/);
			expect(out).toMatch(/jobs_claimed_total\{queue="email"\} 2/);
			expect(out).toMatch(/jobs_completed_total\{queue="email"\} 1/);
			expect(out).toMatch(/jobs_failed_total\{queue="email"\} 1/);
		});
	});
});
