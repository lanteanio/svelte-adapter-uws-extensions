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

	describe('requestId threading', () => {
		it('exposes ctx.requestId = null when no requestId is provided', async () => {
			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; });
			await runner.run('echo', { input: null });
			expect(seen).toBeNull();
		});

		it('persists explicit requestId on the row and exposes it via ctx.requestId', async () => {
			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; return 'ok'; });
			await runner.run('echo', { input: null, requestId: 'req-abc-123' });

			expect(seen).toBe('req-abc-123');
			const rows = [...client._getTaskRows().values()];
			expect(rows[0].request_id).toBe('req-abc-123');
		});

		it('extracts requestId from a passed platform.requestId', async () => {
			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; });
			await runner.run('echo', { input: null, platform: { requestId: 'req-from-platform' } });

			expect(seen).toBe('req-from-platform');
		});

		it('explicit requestId wins over platform.requestId when both are given', async () => {
			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; });
			await runner.run('echo', {
				input: null,
				requestId: 'req-explicit',
				platform: { requestId: 'req-from-platform' }
			});
			expect(seen).toBe('req-explicit');
		});

		it('treats empty-string requestId as null', async () => {
			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; });
			await runner.run('echo', { input: null, requestId: '' });
			expect(seen).toBeNull();
		});

		it('persists request_id through retry attempts', async () => {
			let attempts = 0;
			let lastRequestId;
			runner.register('flaky', async ({ requestId }) => {
				attempts++;
				lastRequestId = requestId;
				if (attempts < 2) throw new Error('not yet');
				return 'ok';
			}, {
				retry: { maxAttempts: 3, backoff: () => 0 }
			});

			const result = await runner.run('flaky', { input: null, requestId: 'req-retry' });

			expect(result).toBe('ok');
			expect(attempts).toBe(2);
			expect(lastRequestId).toBe('req-retry');
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
				id: 'stuck-1',
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
				id: 'fresh-1',
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

	describe('ready()', () => {
		it('resolves once construction completes', async () => {
			const r = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
			await expect(r.ready()).resolves.toBeUndefined();
			r.destroy();
		});

		it('is idempotent (returns the same resolved promise)', async () => {
			const r = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0 });
			await r.ready();
			await r.ready();
			await r.ready();
			r.destroy();
		});

		it('resolves immediately when autoMigrate is false', async () => {
			const r = createTaskRunner(client, { autoMigrate: false, recoveryInterval: 0, cleanupInterval: 0 });
			await expect(r.ready()).resolves.toBeUndefined();
			r.destroy();
		});
	});

	describe('list()', () => {
		beforeEach(() => {
			runner.register('alpha', async (ctx) => ctx.input);
			runner.register('beta', async (ctx) => ctx.input);
		});

		it('returns rows shaped for the public API', async () => {
			await runner.run('alpha', { input: { x: 1 } });
			const rows = await runner.list();
			expect(rows).toHaveLength(1);
			const row = rows[0];
			expect(row.name).toBe('alpha');
			expect(row.status).toBe('committed');
			expect(row.result).toEqual({ x: 1 });
			expect(row.attempts).toBe(1);
			expect(row.error).toBe(null);
			expect(row.createdAt).toBeInstanceOf(Date);
			expect(row.updatedAt).toBeInstanceOf(Date);
			expect(row.fenceExpiresAt).toBeInstanceOf(Date);
			expect('fence' in row).toBe(false); // internal UUID is not exposed
		});

		it('newest first by createdAt', async () => {
			await runner.run('alpha', { input: 1 });
			await new Promise((r) => setTimeout(r, 5));
			await runner.run('alpha', { input: 2 });
			const rows = await runner.list();
			expect(rows.map((r) => r.result)).toEqual([2, 1]);
		});

		it('filters by name', async () => {
			await runner.run('alpha', { input: 'a' });
			await runner.run('beta', { input: 'b' });
			const rows = await runner.list({ name: 'alpha' });
			expect(rows.map((r) => r.name)).toEqual(['alpha']);
		});

		it('filters by status', async () => {
			runner.register('boom', async () => { throw new Error('x'); });
			await runner.run('alpha', { input: 1 });
			await runner.run('boom', { input: null }).catch(() => {});
			const ok = await runner.list({ status: 'committed' });
			const bad = await runner.list({ status: 'failed' });
			expect(ok.every((r) => r.status === 'committed')).toBe(true);
			expect(bad.every((r) => r.status === 'failed')).toBe(true);
		});

		it('honors limit and offset', async () => {
			for (let i = 0; i < 5; i++) {
				await runner.run('alpha', { input: i });
			}
			const page1 = await runner.list({ limit: 2 });
			const page2 = await runner.list({ limit: 2, offset: 2 });
			expect(page1).toHaveLength(2);
			expect(page2).toHaveLength(2);
			expect(page1[0].id).not.toBe(page2[0].id);
		});

		it('rejects invalid filters', async () => {
			await expect(runner.list({ name: '' })).rejects.toThrow(/name/);
			await expect(runner.list({ status: 'bogus' })).rejects.toThrow(/status/);
			await expect(runner.list({ limit: 0 })).rejects.toThrow(/limit/);
			await expect(runner.list({ limit: 5000 })).rejects.toThrow(/limit/);
			await expect(runner.list({ offset: -1 })).rejects.toThrow(/offset/);
		});
	});

	describe('counts()', () => {
		beforeEach(() => {
			runner.register('alpha', async (ctx) => ctx.input);
			runner.register('boom', async () => { throw new Error('x'); });
		});

		it('returns the full bucket set including zeros', async () => {
			await runner.run('alpha', { input: 1 });
			const c = await runner.counts();
			expect(c).toEqual({
				pending: 0,
				running: 0,
				committed: 1,
				failed: 0,
				total: 1
			});
		});

		it('aggregates across statuses', async () => {
			await runner.run('alpha', { input: 1 });
			await runner.run('alpha', { input: 2 });
			await runner.run('boom').catch(() => {});
			const c = await runner.counts();
			expect(c.committed).toBe(2);
			expect(c.failed).toBe(1);
			expect(c.total).toBe(3);
		});

		it('filters by name', async () => {
			await runner.run('alpha', { input: 1 });
			await runner.run('boom').catch(() => {});
			const c = await runner.counts({ name: 'alpha' });
			expect(c.committed).toBe(1);
			expect(c.failed).toBe(0);
			expect(c.total).toBe(1);
		});
	});

	describe('takeover()', () => {
		it('returns false if the row is not running', async () => {
			runner.register('alpha', async (ctx) => ctx.input);
			await runner.run('alpha', { input: 1 });
			const taskId = [...client._getTaskRows().keys()][0];
			expect(await runner.takeover(taskId)).toBe(false); // already committed
		});

		it('returns false for an unknown taskId', async () => {
			expect(await runner.takeover('does-not-exist')).toBe(false);
		});

		it('expires the fence on a running row and returns true', async () => {
			let release;
			runner.register('slow', async () => new Promise((r) => { release = r; }));
			const promise = runner.run('slow', { input: null });

			// Wait for the row to be inserted in 'running' state.
			await new Promise((r) => setTimeout(r, 10));
			const taskId = [...client._getTaskRows().keys()][0];

			expect(await runner.takeover(taskId)).toBe(true);
			const row = client._getTaskRows().get(taskId);
			expect(row.fence_expires_at).toBeLessThan(Date.now());

			// Let the handler finish so the test exits cleanly.
			release();
			await promise.catch(() => {});
		});

		it('also releases the external fence when configured', async () => {
			const released = [];
			const fenceProvider = {
				acquire: vi.fn(async () => true),
				heartbeat: vi.fn(async () => true),
				release: vi.fn(async (taskId, fence) => { released.push({ taskId, fence }); })
			};
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				fence: fenceProvider
			});
			r.register('slow', async () => new Promise((res) => setTimeout(res, 1000)));
			const promise = r.run('slow', { input: null });

			await new Promise((res) => setTimeout(res, 10));
			const taskId = [...client._getTaskRows().keys()][0];
			const fenceBefore = client._getTaskRows().get(taskId).fence;

			expect(await r.takeover(taskId)).toBe(true);
			expect(released).toHaveLength(1);
			expect(released[0]).toEqual({ taskId, fence: fenceBefore });

			// Don't await `promise` -- the simulated long handler is just a setTimeout
			// and will resolve cleanly; vi.restoreAllMocks() in beforeEach handles
			// cleanup. r.destroy() stops the timers we created.
			r.destroy();
		});

		it('rejects empty taskId', async () => {
			await expect(runner.takeover('')).rejects.toThrow(/taskId/);
		});
	});

	describe('onStateChange', () => {
		it('fires for every state-machine transition on a successful run', async () => {
			const events = [];
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				onStateChange: (e) => events.push(e)
			});
			r.register('alpha', async (ctx) => ({ ok: ctx.input }));

			await r.run('alpha', { input: 'hi' });

			// Expect: null -> running, then running -> committed.
			expect(events).toHaveLength(2);
			expect(events[0]).toMatchObject({ oldStatus: null, newStatus: 'running', name: 'alpha', attempt: 1 });
			expect(events[1]).toMatchObject({ oldStatus: 'running', newStatus: 'committed', name: 'alpha', attempt: 1 });
			expect(events[1].result).toEqual({ ok: 'hi' });
			expect(events[0].taskId).toBe(events[1].taskId);

			r.destroy();
		});

		it('fires for failed transitions with serialised error', async () => {
			const events = [];
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				onStateChange: (e) => events.push(e)
			});
			r.register('boom', async () => { throw new Error('nope'); });

			await r.run('boom').catch(() => {});

			const failed = events.find((e) => e.newStatus === 'failed');
			expect(failed).toBeDefined();
			expect(failed.error).toMatchObject({ message: 'nope', name: 'Error' });
			expect(typeof failed.error.stack).toBe('string');

			r.destroy();
		});

		it('fires for pending->running on dispatch claim', async () => {
			const events = [];
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				dispatchInterval: 0, // don't auto-tick
				cleanupInterval: 0,
				onStateChange: (e) => events.push(e)
			});
			r.register('alpha', async (ctx) => ctx.input);

			const taskId = await r.enqueue('alpha', { input: 1 });
			expect(events.find((e) => e.taskId === taskId && e.oldStatus === null && e.newStatus === 'pending')).toBeDefined();
			events.length = 0;

			// Manually trigger a dispatch tick (the runner's claim path is
			// public via the timer; here we use the mock's internal state to
			// confirm the call wires through). The simplest way is to call
			// run() on a different instance pointing at the same store, but
			// for this unit test we exercise the public path: enqueue + a
			// short dispatchInterval.
			const r2 = createTaskRunner(client, {
				recoveryInterval: 0,
				dispatchInterval: 5,
				cleanupInterval: 0,
				onStateChange: (e) => events.push(e)
			});
			r2.register('alpha', async (ctx) => ctx.input);

			// Wait for dispatch to claim the row.
			await new Promise((res) => setTimeout(res, 50));

			const transition = events.find((e) => e.taskId === taskId && e.oldStatus === 'pending' && e.newStatus === 'running');
			expect(transition).toBeDefined();
			expect(transition.attempt).toBeGreaterThanOrEqual(1);

			r.destroy();
			r2.destroy();
		});

		it('listener errors are caught and do not break the state machine', async () => {
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				onStateChange: () => { throw new Error('listener exploded'); }
			});
			r.register('alpha', async (ctx) => ctx.input);

			// Run completes successfully despite the listener throwing on
			// every transition.
			await expect(r.run('alpha', { input: 42 })).resolves.toBe(42);
			expect(warn).toHaveBeenCalled();

			r.destroy();
			warn.mockRestore();
		});

		it('async listener rejections are isolated', async () => {
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				onStateChange: async () => { throw new Error('async listener exploded'); }
			});
			r.register('alpha', async (ctx) => ctx.input);

			await expect(r.run('alpha', { input: 1 })).resolves.toBe(1);
			// Allow microtasks to drain so the rejection is observed.
			await new Promise((res) => setTimeout(res, 5));
			expect(warn).toHaveBeenCalled();

			r.destroy();
			warn.mockRestore();
		});

		it('rejects non-function onStateChange', () => {
			expect(() => createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				onStateChange: 'not a function'
			})).toThrow(/onStateChange/);
		});
	});
});
