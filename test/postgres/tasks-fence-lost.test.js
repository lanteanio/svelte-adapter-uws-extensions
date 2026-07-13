import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { createIdempotencyStore } from '../../src/postgres/idempotency.js';

// A fenced-out caller (its fence was superseded by another worker before it
// could record its outcome) must report the CANONICAL durable result, never
// the stale local attempt. These tests simulate the takeover by rotating the
// row's fence from inside the handler so the runner's commit/fail loses.
describe('postgres tasks - fenced-out caller reports the canonical result', () => {
	let pg;
	let runner;

	beforeEach(() => {
		pg = mockPgClient();
	});

	afterEach(() => {
		runner?.destroy();
	});

	function rowByFence(fence) {
		return [...pg._getTaskRows().values()].find((r) => r.fence === fence);
	}

	it('commit-lost while still running under a new fence: polls for and returns the canonical result, not the stale local one', async () => {
		runner = createTaskRunner(pg, {
			recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 2000
		});
		runner.register('t', async ({ fence }) => {
			const row = rowByFence(fence);
			// Another worker takes over: rotate the fence so our commit loses,
			// leaving the row still running under the new fence.
			row.fence = 'successor-fence';
			// The successor commits the canonical result a little later.
			setTimeout(() => { row.status = 'committed'; row.result = { canonical: true }; }, 20);
			return { stale: true };
		});

		const result = await runner.run('t', { input: null });
		expect(result).toEqual({ canonical: true });
	});

	it('commit-lost where the successor already committed: returns the canonical result immediately', async () => {
		runner = createTaskRunner(pg, {
			recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 2000
		});
		runner.register('t', async ({ fence }) => {
			const row = rowByFence(fence);
			row.fence = 'successor-fence';
			row.status = 'committed';
			row.result = { canonical: true };
			return { stale: true };
		});

		expect(await runner.run('t', { input: null })).toEqual({ canonical: true });
	});

	it('fail-lost where the successor committed a success: returns the canonical success, not the local error', async () => {
		runner = createTaskRunner(pg, {
			recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 2000
		});
		runner.register('t', async ({ fence }) => {
			const row = rowByFence(fence);
			// The successor took over and committed a success before we (about to
			// throw) could record anything.
			row.fence = 'successor-fence';
			row.status = 'committed';
			row.result = { canonical: 'success' };
			throw new Error('stale local failure');
		});

		expect(await runner.run('t', { input: null })).toEqual({ canonical: 'success' });
	});

	it('fail-lost where the successor recorded a failure: throws the canonical error, not the local one', async () => {
		runner = createTaskRunner(pg, {
			recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 2000
		});
		runner.register('t', async ({ fence }) => {
			const row = rowByFence(fence);
			row.fence = 'successor-fence';
			row.status = 'failed';
			row.error = { name: 'Error', message: 'canonical failure' };
			throw new Error('stale local failure');
		});

		await expect(runner.run('t', { input: null })).rejects.toThrow('canonical failure');
	});

	it('throws TaskFenceLostError when the canonical outcome never becomes terminal within awaitTimeout', async () => {
		runner = createTaskRunner(pg, {
			recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 60
		});
		runner.register('t', async ({ fence }) => {
			const row = rowByFence(fence);
			// Our commit loses and the row stays running under the new fence forever.
			row.fence = 'successor-fence';
			return { stale: true };
		});

		await expect(runner.run('t', { input: null })).rejects.toMatchObject({
			name: 'TaskFenceLostError',
			code: 'TASK_FENCE_LOST'
		});
	});

	it('idempotency caches the CANONICAL result of a fenced-out run, not the stale local one', async () => {
		const idempotency = createIdempotencyStore(pg, { cleanupInterval: 0 });
		runner = createTaskRunner(pg, {
			idempotency, recoveryInterval: 0, cleanupInterval: 0,
			awaitPollInterval: 5, awaitTimeout: 2000
		});
		let calls = 0;
		runner.register('t', async ({ fence }) => {
			calls++;
			const row = rowByFence(fence);
			row.fence = 'successor-fence';
			setTimeout(() => { row.status = 'committed'; row.result = { canonical: true }; }, 20);
			return { stale: true };
		});

		const first = await runner.run('t', { idempotencyKey: 'k1', input: null });
		expect(first).toEqual({ canonical: true });

		// A retry with the same key returns the cached CANONICAL value and does
		// not re-run the handler.
		const second = await runner.run('t', { idempotencyKey: 'k1', input: null });
		expect(second).toEqual({ canonical: true });
		expect(calls).toBe(1);

		idempotency.destroy();
	});
});
