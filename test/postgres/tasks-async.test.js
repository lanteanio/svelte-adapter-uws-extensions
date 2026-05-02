import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner } from '../../postgres/tasks.js';

describe('postgres tasks (enqueue / await / dispatch)', () => {
	let client;
	let runner;

	beforeEach(() => {
		client = mockPgClient();
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
	});

	afterEach(() => {
		runner.destroy();
	});

	describe('enqueue', () => {
		it('returns a fresh taskId', async () => {
			runner.register('echo', async ({ input }) => input);
			const id = await runner.enqueue('echo', { input: { x: 1 } });
			expect(typeof id).toBe('string');
			expect(id.length).toBeGreaterThan(0);
		});

		it('inserts a row with status=pending and attempts=0', async () => {
			runner.register('echo', async ({ input }) => input);
			const id = await runner.enqueue('echo', { input: 'hi' });

			const row = client._getTaskRows().get(id);
			expect(row).toBeDefined();
			expect(row.status).toBe('pending');
			expect(row.attempts).toBe(0);
			expect(row.input).toBe('hi');
			expect(row.name).toBe('echo');
		});

		it('throws on invalid task name', async () => {
			await expect(runner.enqueue('with spaces', { input: null })).rejects.toThrow('invalid task name');
			await expect(runner.enqueue('', { input: null })).rejects.toThrow('invalid task name');
		});

		it('throws on empty idempotencyKey', async () => {
			runner.register('echo', async () => 'ok');
			await expect(runner.enqueue('echo', { input: null, idempotencyKey: '' })).rejects.toThrow('idempotencyKey');
		});

		it('does not require the handler to be registered (cross-deployment scenario)', async () => {
			// enqueue accepts any name; the dispatch loop in any live instance
			// with the handler picks it up.  This test confirms enqueue does
			// not validate the registration locally.
			const id = await runner.enqueue('not-yet-registered', { input: null });
			expect(typeof id).toBe('string');

			const row = client._getTaskRows().get(id);
			expect(row.status).toBe('pending');
		});

		it('persists requestId on the pending row', async () => {
			runner.register('echo', async () => 'ok');
			const id = await runner.enqueue('echo', { input: null, requestId: 'req-enq-1' });
			const row = client._getTaskRows().get(id);
			expect(row.request_id).toBe('req-enq-1');
		});

		it('extracts requestId from platform.requestId', async () => {
			runner.register('echo', async () => 'ok');
			const id = await runner.enqueue('echo', {
				input: null,
				platform: { requestId: 'req-enq-platform' }
			});
			const row = client._getTaskRows().get(id);
			expect(row.request_id).toBe('req-enq-platform');
		});

		it('explicit requestId wins over platform.requestId on enqueue', async () => {
			runner.register('echo', async () => 'ok');
			const id = await runner.enqueue('echo', {
				input: null,
				requestId: 'req-explicit',
				platform: { requestId: 'req-from-platform' }
			});
			const row = client._getTaskRows().get(id);
			expect(row.request_id).toBe('req-explicit');
		});
	});

	describe('await', () => {
		it('returns the cached result for a committed task', async () => {
			runner.register('echo', async ({ input }) => `result: ${input}`);
			// Run synchronously to populate a committed row
			const result = await runner.run('echo', { input: 'sync' });
			expect(result).toBe('result: sync');

			// Now use await on the committed row
			const taskId = [...client._getTaskRows().keys()][0];
			const awaited = await runner.await(taskId);
			expect(awaited).toBe('result: sync');
		});

		it('throws the stored error for a failed task', async () => {
			runner.register('boom', async () => {
				throw new Error('handler exploded');
			});
			await expect(runner.run('boom', { input: null })).rejects.toThrow('handler exploded');

			const taskId = [...client._getTaskRows().keys()][0];
			await expect(runner.await(taskId)).rejects.toThrow('handler exploded');
		});

		it('throws when the task does not exist', async () => {
			await expect(runner.await('00000000-0000-0000-0000-000000000000', {
				timeout: 100, pollInterval: 10
			})).rejects.toThrow('not found');
		});

		it('rejects with timeout when the task never completes', async () => {
			runner.register('echo', async () => 'ok');
			const id = await runner.enqueue('echo', { input: null });

			// dispatchInterval=0 in beforeEach, so the pending row never gets
			// claimed.  await should time out.
			await expect(runner.await(id, { timeout: 200, pollInterval: 10 }))
				.rejects.toThrow('await timeout');
		});

		it('throws on empty taskId', async () => {
			await expect(runner.await('')).rejects.toThrow('non-empty');
		});
	});

	describe('dispatch loop', () => {
		it('claims pending rows and runs them in the background', async () => {
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 30
			});

			let calls = 0;
			r.register('echo', async ({ input }) => {
				calls += 1;
				return { result: input };
			});

			const id = await r.enqueue('echo', { input: 'pending-1' });
			expect(client._getTaskRows().get(id).status).toBe('pending');

			// Wait for the dispatch tick + handler to complete
			await new Promise((resolve) => setTimeout(resolve, 100));

			const row = client._getTaskRows().get(id);
			expect(row.status).toBe('committed');
			expect(row.result).toEqual({ result: 'pending-1' });
			expect(calls).toBe(1);

			r.destroy();
		});

		it('respects dispatchBatchSize per sweep', async () => {
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 30,
				dispatchBatchSize: 2
			});

			r.register('echo', async ({ input }) => input);

			// Enqueue 5 rows; batchSize of 2 means at most 2 are claimed per
			// dispatch tick.  Across 3 ticks (90ms) we should claim all 5.
			for (let i = 0; i < 5; i++) {
				await r.enqueue('echo', { input: i });
			}

			await new Promise((resolve) => setTimeout(resolve, 200));

			const rows = [...client._getTaskRows().values()];
			const committed = rows.filter((r) => r.status === 'committed');
			expect(committed).toHaveLength(5);

			r.destroy();
		});

		it('skips pending rows whose handler is not registered locally', async () => {
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 30
			});

			// No handler registered for 'remote-task'
			const id = await r.enqueue('remote-task', { input: null });

			await new Promise((resolve) => setTimeout(resolve, 100));

			// The dispatch CTE claimed the row (transitioned to running) but
			// the runner found no handler and left it in 'running' state.
			// Another instance with the handler can pick it up later via
			// the recovery sweep when its fence expires.
			const row = client._getTaskRows().get(id);
			expect(row.status).toBe('running');
			expect(row.attempts).toBe(1);

			r.destroy();
		});
	});

	describe('enqueue + await round trip', () => {
		it('await resolves once dispatch picks up and runs the handler', async () => {
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 30
			});

			r.register('echo', async ({ input }) => `done: ${input}`);

			const id = await r.enqueue('echo', { input: 'round-trip' });
			const result = await r.await(id, { timeout: 1000, pollInterval: 10 });

			expect(result).toBe('done: round-trip');

			r.destroy();
		});

		it('await surfaces handler-thrown errors after dispatch', async () => {
			const r = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 30
			});

			r.register('boom', async () => {
				const err = new Error('async failure');
				err.code = 'E_ASYNC';
				throw err;
			});

			const id = await r.enqueue('boom', { input: null });
			let caught;
			try {
				await r.await(id, { timeout: 1000, pollInterval: 10 });
			} catch (err) {
				caught = err;
			}

			expect(caught).toBeDefined();
			expect(caught.message).toBe('async failure');
			expect(caught.code).toBe('E_ASYNC');

			r.destroy();
		});
	});

	describe('option validation', () => {
		it('throws on negative dispatchInterval', () => {
			expect(() => createTaskRunner(client, {
				dispatchInterval: -1, recoveryInterval: 0, cleanupInterval: 0
			})).toThrow('dispatchInterval');
		});

		it('throws on non-positive dispatchBatchSize', () => {
			expect(() => createTaskRunner(client, {
				dispatchBatchSize: 0, recoveryInterval: 0, cleanupInterval: 0
			})).toThrow('dispatchBatchSize');
		});

		it('throws on non-positive awaitPollInterval', () => {
			expect(() => createTaskRunner(client, {
				awaitPollInterval: 0, recoveryInterval: 0, cleanupInterval: 0
			})).toThrow('awaitPollInterval');
		});

		it('throws on negative awaitTimeout', () => {
			expect(() => createTaskRunner(client, {
				awaitTimeout: -1, recoveryInterval: 0, cleanupInterval: 0
			})).toThrow('awaitTimeout');
		});
	});
});
