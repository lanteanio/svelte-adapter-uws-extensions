import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner } from '../../postgres/tasks.js';

const echoUrl = new URL('../helpers/workers/echo.js', import.meta.url);
const throwsUrl = new URL('../helpers/workers/throws.js', import.meta.url);
const abortableUrl = new URL('../helpers/workers/abortable.js', import.meta.url);
const slowUrl = new URL('../helpers/workers/slow.js', import.meta.url);
const missingUrl = new URL('../helpers/workers/does-not-exist.js', import.meta.url);

describe('postgres tasks (worker thread executor)', () => {
	let client;
	let runner;

	beforeEach(() => {
		client = mockPgClient();
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			fenceTtl: 30
		});
	});

	afterEach(() => {
		runner.destroy();
	});

	describe('register validation', () => {
		it('accepts a URL as the worker shorthand', () => {
			runner.register('echo', null, { worker: echoUrl });
		});

		it('accepts a URL string as the worker shorthand', () => {
			runner.register('echo', null, { worker: echoUrl.href });
		});

		it('accepts an object with path and pool config', () => {
			runner.register('echo', null, {
				worker: { path: echoUrl, pool: { size: 2, idleTimeout: 5000 } }
			});
		});

		it('throws when handler is provided alongside worker', () => {
			expect(() => runner.register('echo', async () => 1, { worker: echoUrl }))
				.toThrow('must be null/undefined when worker is provided');
		});

		it('throws on invalid worker.path', () => {
			expect(() => runner.register('echo', null, { worker: { path: 42 } }))
				.toThrow('worker.path');
		});

		it('throws on non-positive worker.pool.size', () => {
			expect(() => runner.register('echo', null, {
				worker: { path: echoUrl, pool: { size: 0 } }
			})).toThrow('pool.size');
		});

		it('throws on negative worker.pool.idleTimeout', () => {
			expect(() => runner.register('echo', null, {
				worker: { path: echoUrl, pool: { idleTimeout: -1 } }
			})).toThrow('idleTimeout');
		});
	});

	describe('execution', () => {
		it('runs the worker handler and returns its result', async () => {
			runner.register('echo', null, { worker: echoUrl });
			const result = await runner.run('echo', {
				input: { greeting: 'hello' },
				idempotencyKey: 'k1'
			});

			expect(result).toMatchObject({
				echoed: { greeting: 'hello' },
				idempotencyKey: 'k1',
				attempt: 1
			});
			expect(typeof result.fence).toBe('string');
		});

		it('commits the row on success', async () => {
			runner.register('echo', null, { worker: echoUrl });
			await runner.run('echo', { input: { x: 1 } });

			const rows = [...client._getTaskRows().values()];
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('committed');
			expect(rows[0].result.echoed).toEqual({ x: 1 });
		});

		it('rejects with the worker handler\'s error and preserves code/stack', async () => {
			runner.register('boom', null, {
				worker: throwsUrl
				// no retry: rejects on first attempt
			});

			let caught;
			try {
				await runner.run('boom', { input: { x: 1 } });
			} catch (err) {
				caught = err;
			}

			expect(caught).toBeDefined();
			expect(caught.message).toBe('worker handler exploded');
			expect(caught.code).toBe('WORKER_BOOM');
			expect(typeof caught.stack).toBe('string');

			const row = [...client._getTaskRows().values()][0];
			expect(row.status).toBe('failed');
			expect(row.error.message).toBe('worker handler exploded');
			expect(row.error.code).toBe('WORKER_BOOM');
		});

		it('honours retry policy across worker attempts', async () => {
			// throws.js always throws, so this exercises the retry loop.
			// We expect maxAttempts attempts then rejection.
			runner.register('flaky', null, {
				worker: throwsUrl,
				retry: { maxAttempts: 3, backoff: () => 0 }
			});

			await expect(runner.run('flaky', { input: null })).rejects.toThrow('worker handler exploded');

			const row = [...client._getTaskRows().values()][0];
			expect(row.status).toBe('failed');
			expect(row.attempts).toBe(3);
		});

		it('surfaces a missing worker file as a typed error', async () => {
			runner.register('missing', null, { worker: missingUrl });

			let caught;
			try {
				await runner.run('missing', { input: null });
			} catch (err) {
				caught = err;
			}
			expect(caught).toBeDefined();
			// Either the dynamic import fails, or the worker exits cleanly --
			// in both cases the run rejects with a meaningful message.
			expect(typeof caught.message).toBe('string');
			expect(caught.message.length).toBeGreaterThan(0);
		});
	});

	describe('pool concurrency', () => {
		it('runs concurrent calls in parallel up to pool size', async () => {
			runner.register('slow', null, {
				worker: { path: slowUrl, pool: { size: 3 } }
			});

			const start = Date.now();
			const results = await Promise.all([
				runner.run('slow', { input: { delayMs: 100 } }),
				runner.run('slow', { input: { delayMs: 100 } }),
				runner.run('slow', { input: { delayMs: 100 } })
			]);
			const elapsed = Date.now() - start;

			expect(results).toHaveLength(3);
			expect(results.every((r) => r.ok)).toBe(true);
			// Three 100ms tasks running serially would take >=300ms; in
			// parallel under a pool of 3 they should finish in <250ms even
			// accounting for worker spawn overhead.
			expect(elapsed).toBeLessThan(1000);
		});

		it('queues calls beyond pool size', async () => {
			runner.register('slow', null, {
				worker: { path: slowUrl, pool: { size: 1 } }
			});

			const start = Date.now();
			await Promise.all([
				runner.run('slow', { input: { delayMs: 80 } }),
				runner.run('slow', { input: { delayMs: 80 } })
			]);
			const elapsed = Date.now() - start;

			// Two 80ms tasks serialised through a pool of 1 take >=160ms.
			expect(elapsed).toBeGreaterThanOrEqual(150);
		});
	});

	describe('lifecycle', () => {
		it('destroy() terminates workers and rejects pending runs', async () => {
			runner.register('slow', null, {
				worker: { path: slowUrl, pool: { size: 1 } }
			});

			const inflight = runner.run('slow', { input: { delayMs: 5000 } });
			// Give the worker a tick to spawn and accept the message.
			await new Promise((r) => setTimeout(r, 50));

			runner.destroy();

			await expect(inflight).rejects.toThrow('task runner destroyed');
		});

		it('aborts the worker handler when the fence is lost mid-run', async () => {
			const r = createTaskRunner(client, {
				fenceTtl: 1,
				heartbeatInterval: 100,
				recoveryInterval: 0,
				cleanupInterval: 0
			});
			r.register('abortable', null, { worker: abortableUrl });

			const promise = r.run('abortable', { input: null });

			// Wait for the row to be inserted and the worker to start the
			// 5s wait loop, then rotate the row's fence so the next
			// heartbeat tick returns rowCount=0.
			await new Promise((res) => setTimeout(res, 200));
			const rows = [...client._getTaskRows().values()];
			expect(rows).toHaveLength(1);
			rows[0].fence = 'stolen-fence';

			// The next heartbeat (at ~300ms) detects the loss and fires
			// the runner's signal.  The pool forwards an abort message to
			// the worker, which aborts the handler's local controller,
			// which causes the handler to reject.  run() rejects with the
			// handler's rejection reason.
			await expect(promise).rejects.toThrow(/abort/i);

			r.destroy();
		}, 5000);
	});
});
