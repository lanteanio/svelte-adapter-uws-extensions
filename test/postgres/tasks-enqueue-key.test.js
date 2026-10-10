import { afterEach, describe, expect, it, vi } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';

const runners = [];
afterEach(() => { for (const runner of runners.splice(0)) runner.destroy(); vi.useRealTimers(); });
function create(client, options = {}) {
	const runner = createTaskRunner(client, { recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0, ...options }); runners.push(runner); return runner;
}

describe('queued task idempotency keys', () => {
	it('persists independent submissions and forwards the same key without using the run result cache', async () => {
		const client = mockPgClient();
		const idempotency = { acquire: vi.fn(async () => { throw new Error('enqueue and dispatch must not consult the run cache'); }) };
		const first = create(client, { idempotency }), second = create(client, { idempotency });
		await Promise.all([first.ready(), second.ready()]);
		vi.useFakeTimers();
		const ids = await Promise.all([
			first.enqueue('send', { input: { submission: 1 }, idempotencyKey: 'same-operation' }),
			second.enqueue('send', { input: { submission: 2 }, idempotencyKey: 'same-operation' })
		]);
		expect(new Set(ids).size).toBe(2); expect(await first.counts()).toMatchObject({ pending: 2, total: 2 });
		const seen = []; const worker = create(client, { idempotency, dispatchInterval: 10 });
		worker.register('send', async ({ input, idempotencyKey }) => { seen.push({ input, idempotencyKey }); return input.submission; });
		await worker.ready(); await vi.advanceTimersByTimeAsync(10);
		expect(await Promise.all(ids.map(id => worker.await(id)))).toEqual([1, 2]);
		expect(seen).toEqual(expect.arrayContaining(ids.map((_, i) => ({ input: { submission: i + 1 }, idempotencyKey: 'same-operation' }))));
		expect(seen).toHaveLength(2); expect(idempotency.acquire).not.toHaveBeenCalled();
	});
});
