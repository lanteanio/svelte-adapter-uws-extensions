import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner, TaskInFlightError, UnknownTaskError } from '../../postgres/tasks.js';
import { createIdempotencyStore } from '../../postgres/idempotency.js';

describe('postgres tasks', () => {
	let client;
	let runner;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockPgClient();
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0
		});
	});

	afterEach(() => {
		runner.destroy();
	});

	describe('createTaskRunner', () => {
		it('returns a runner with the expected API', () => {
			expect(typeof runner.register).toBe('function');
			expect(typeof runner.run).toBe('function');
			expect(typeof runner.destroy).toBe('function');
		});

		it('throws on invalid table name', () => {
			expect(() => createTaskRunner(client, { table: '123bad', recoveryInterval: 0, cleanupInterval: 0 })).toThrow('invalid table name');
			expect(() => createTaskRunner(client, { table: 'drop;--', recoveryInterval: 0, cleanupInterval: 0 })).toThrow('invalid table name');
		});

		it('throws on non-positive fenceTtl', () => {
			expect(() => createTaskRunner(client, { fenceTtl: 0, recoveryInterval: 0, cleanupInterval: 0 })).toThrow('fenceTtl');
			expect(() => createTaskRunner(client, { fenceTtl: -1, recoveryInterval: 0, cleanupInterval: 0 })).toThrow('fenceTtl');
		});

		it('throws on negative recoveryInterval', () => {
			expect(() => createTaskRunner(client, { recoveryInterval: -1, cleanupInterval: 0 })).toThrow('recoveryInterval');
		});

		it('throws on non-positive recoveryBatchSize', () => {
			expect(() => createTaskRunner(client, { recoveryBatchSize: 0, recoveryInterval: 0, cleanupInterval: 0 })).toThrow('recoveryBatchSize');
		});
	});

	describe('register', () => {
		it('throws on invalid task name', () => {
			expect(() => runner.register('123bad', () => {})).toThrow('invalid task name');
			expect(() => runner.register('with spaces', () => {})).toThrow('invalid task name');
			expect(() => runner.register('', () => {})).toThrow('invalid task name');
		});

		it('throws on non-function handler', () => {
			expect(() => runner.register('foo', 'not a function')).toThrow('must be a function');
			expect(() => runner.register('foo', null)).toThrow('must be a function');
		});

		it('throws when re-registering an existing name', () => {
			runner.register('foo', async () => 1);
			expect(() => runner.register('foo', async () => 2)).toThrow('already registered');
		});

		it('throws on invalid retry.maxAttempts', () => {
			expect(() => runner.register('foo', async () => 1, { retry: { maxAttempts: 0 } })).toThrow('maxAttempts');
			expect(() => runner.register('foo', async () => 1, { retry: { maxAttempts: -1 } })).toThrow('maxAttempts');
		});

		it('throws on non-function retry.backoff or retry.on', () => {
			expect(() => runner.register('foo', async () => 1, { retry: { maxAttempts: 3, backoff: 'no' } })).toThrow('backoff');
			expect(() => runner.register('bar', async () => 1, { retry: { maxAttempts: 3, on: 'no' } })).toThrow('on');
		});
	});

	describe('run - basic flow', () => {
		it('runs a registered task and returns the result', async () => {
			runner.register('add', async ({ input }) => input.a + input.b);
			const result = await runner.run('add', { input: { a: 2, b: 3 } });
			expect(result).toBe(5);
		});

		it('throws UnknownTaskError when no handler is registered', async () => {
			await expect(runner.run('missing', { input: null })).rejects.toThrow(UnknownTaskError);
		});

		it('passes idempotencyKey through to the handler context', async () => {
			let seenKey;
			runner.register('echo', async ({ idempotencyKey }) => {
				seenKey = idempotencyKey;
				return 'ok';
			});
			await runner.run('echo', { input: null, idempotencyKey: 'abc' });
			expect(seenKey).toBe('abc');
		});

		it('passes a fence and abort signal in the handler context', async () => {
			let ctx;
			runner.register('echo', async (c) => {
				ctx = c;
				return 'ok';
			});
			await runner.run('echo', { input: null });
			expect(typeof ctx.fence).toBe('string');
			expect(ctx.fence.length).toBeGreaterThan(0);
			expect(ctx.signal).toBeInstanceOf(AbortSignal);
			expect(ctx.attempt).toBe(1);
		});

		it('writes a row with status=running then commits to status=committed', async () => {
			runner.register('add', async ({ input }) => input.a + input.b);
			await runner.run('add', { input: { a: 1, b: 1 } });

			const rows = [...client._getTaskRows().values()];
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('committed');
			expect(rows[0].result).toBe(2);
		});

		it('marks the row as failed when the handler throws and no retry is configured', async () => {
			runner.register('boom', async () => {
				throw new Error('handler exploded');
			});

			await expect(runner.run('boom', { input: null })).rejects.toThrow('handler exploded');

			const rows = [...client._getTaskRows().values()];
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('failed');
			expect(rows[0].error.message).toBe('handler exploded');
			expect(rows[0].error.name).toBe('Error');
		});
	});

	describe('idempotency integration', () => {
		it('returns the cached result on second run with the same key', async () => {
			const idempotency = createIdempotencyStore(client, { cleanupInterval: 0 });
			const r = createTaskRunner(client, {
				idempotency,
				recoveryInterval: 0,
				cleanupInterval: 0
			});

			let calls = 0;
			r.register('count', async () => {
				calls += 1;
				return calls;
			});

			const a = await r.run('count', { input: null, idempotencyKey: 'k' });
			const b = await r.run('count', { input: null, idempotencyKey: 'k' });
			expect(a).toBe(1);
			expect(b).toBe(1);
			expect(calls).toBe(1);

			r.destroy();
		});

		it('throws TaskInFlightError when the idempotency slot is pending', async () => {
			const fakeStore = {
				async acquire() { return { acquired: false, pending: true }; },
				async purge() {},
				async clear() {}
			};
			const r = createTaskRunner(client, {
				idempotency: fakeStore,
				recoveryInterval: 0,
				cleanupInterval: 0
			});
			r.register('echo', async () => 'ok');

			await expect(r.run('echo', { input: null, idempotencyKey: 'k' }))
				.rejects.toBeInstanceOf(TaskInFlightError);

			r.destroy();
		});

		it('skips the cache when idempotencyKey is omitted', async () => {
			const idempotency = createIdempotencyStore(client, { cleanupInterval: 0 });
			const r = createTaskRunner(client, {
				idempotency,
				recoveryInterval: 0,
				cleanupInterval: 0
			});

			let calls = 0;
			r.register('count', async () => {
				calls += 1;
				return calls;
			});

			await r.run('count', { input: null });
			await r.run('count', { input: null });
			expect(calls).toBe(2);

			r.destroy();
		});
	});

	describe('retry policy', () => {
		it('retries up to maxAttempts when the handler throws', async () => {
			let attempts = 0;
			runner.register('flaky', async ({ attempt }) => {
				attempts = attempt;
				if (attempt < 3) throw new Error('try again');
				return 'finally';
			}, {
				retry: { maxAttempts: 3, backoff: () => 0 }
			});

			const result = await runner.run('flaky', { input: null });
			expect(result).toBe('finally');
			expect(attempts).toBe(3);
		});

		it('respects retry.on predicate to skip retries for non-matching errors', async () => {
			let calls = 0;
			runner.register('selective', async () => {
				calls += 1;
				const err = new Error('nope');
				err.code = 'permanent';
				throw err;
			}, {
				retry: {
					maxAttempts: 5,
					backoff: () => 0,
					on: (err) => err.code === 'transient'
				}
			});

			await expect(runner.run('selective', { input: null })).rejects.toThrow('nope');
			expect(calls).toBe(1);
		});

		it('passes the attempt number and error to backoff', async () => {
			const seen = [];
			runner.register('flaky', async ({ attempt }) => {
				if (attempt < 3) throw new Error('again');
				return 'done';
			}, {
				retry: {
					maxAttempts: 3,
					backoff: (attempt, err) => {
						seen.push({ attempt, msg: err.message });
						return 0;
					}
				}
			});

			await runner.run('flaky', { input: null });
			expect(seen).toEqual([
				{ attempt: 1, msg: 'again' },
				{ attempt: 2, msg: 'again' }
			]);
		});

		it('marks the row as failed once retries exhaust', async () => {
			runner.register('always', async () => {
				throw new Error('persistent');
			}, {
				retry: { maxAttempts: 2, backoff: () => 0 }
			});

			await expect(runner.run('always', { input: null })).rejects.toThrow('persistent');

			const rows = [...client._getTaskRows().values()];
			expect(rows[0].status).toBe('failed');
			expect(rows[0].attempts).toBe(2);
		});
	});

	describe('recovery', () => {
		it('reclaims rows whose fence_expires_at is in the past', async () => {
			const r = createTaskRunner(client, {
				fenceTtl: 1,
				recoveryInterval: 0,
				cleanupInterval: 0
			});

			let calls = 0;
			r.register('worker', async () => {
				calls += 1;
				return 'done';
			});

			// Plant a stuck row directly: status='running', fence_expires_at in the past
			const taskRows = client._getTaskRows();
			taskRows.set('stuck-1', {
				task_id: 'stuck-1',
				name: 'worker',
				input: null,
				idempotency_key: null,
				status: 'running',
				result: null,
				error: null,
				fence: 'old-fence',
				fence_expires_at: Date.now() - 10000,
				attempts: 1,
				created_at: Date.now() - 60000,
				updated_at: Date.now() - 60000
			});

			// Manually trigger one recovery sweep via the internal path: we
			// can drive it by creating a runner with a tiny interval and
			// waiting one tick.
			const r2 = createTaskRunner(client, {
				recoveryInterval: 30,
				cleanupInterval: 0
			});
			r2.register('worker', async () => {
				calls += 1;
				return 'done';
			});

			await new Promise((resolve) => setTimeout(resolve, 80));

			const row = client._getTaskRows().get('stuck-1');
			expect(row.status).toBe('committed');
			expect(row.attempts).toBeGreaterThan(1);
			expect(row.fence).not.toBe('old-fence');
			expect(calls).toBeGreaterThanOrEqual(1);

			r.destroy();
			r2.destroy();
		});

		it('does not reclaim rows whose fence_expires_at is in the future', async () => {
			const taskRows = client._getTaskRows();
			taskRows.set('fresh-1', {
				task_id: 'fresh-1',
				name: 'worker',
				input: null,
				idempotency_key: null,
				status: 'running',
				result: null,
				error: null,
				fence: 'fresh-fence',
				fence_expires_at: Date.now() + 60000,
				attempts: 1,
				created_at: Date.now(),
				updated_at: Date.now()
			});

			const r = createTaskRunner(client, {
				recoveryInterval: 30,
				cleanupInterval: 0
			});
			r.register('worker', async () => 'done');

			await new Promise((resolve) => setTimeout(resolve, 80));

			const row = client._getTaskRows().get('fresh-1');
			expect(row.status).toBe('running');
			expect(row.fence).toBe('fresh-fence');

			r.destroy();
		});
	});

	describe('cleanup', () => {
		it('deletes terminal rows older than rowTtl', async () => {
			const r = createTaskRunner(client, {
				rowTtl: 1,
				recoveryInterval: 0,
				cleanupInterval: 30
			});
			r.register('echo', async () => 'ok');

			await r.run('echo', { input: null });

			// Age the row past rowTtl
			for (const row of client._getTaskRows().values()) {
				row.updated_at = Date.now() - 5000;
			}

			await new Promise((resolve) => setTimeout(resolve, 80));

			expect(client._getTaskRows().size).toBe(0);

			r.destroy();
		});

		it('keeps fresh terminal rows', async () => {
			const r = createTaskRunner(client, {
				rowTtl: 7 * 24 * 3600,
				recoveryInterval: 0,
				cleanupInterval: 30
			});
			r.register('echo', async () => 'ok');

			await r.run('echo', { input: null });

			await new Promise((resolve) => setTimeout(resolve, 80));

			expect(client._getTaskRows().size).toBe(1);

			r.destroy();
		});
	});

	describe('error serialisation', () => {
		it('preserves error name, message, stack, and code', async () => {
			runner.register('boom', async () => {
				const err = new Error('something');
				err.code = 'E_FAIL';
				throw err;
			});

			await expect(runner.run('boom', { input: null })).rejects.toThrow('something');

			const row = [...client._getTaskRows().values()][0];
			expect(row.error.message).toBe('something');
			expect(row.error.code).toBe('E_FAIL');
			expect(typeof row.error.stack).toBe('string');
		});
	});
});
