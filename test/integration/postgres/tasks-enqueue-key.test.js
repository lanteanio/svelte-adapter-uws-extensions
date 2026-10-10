import { describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { createIdempotencyStore } from '../../../src/postgres/idempotency.js';

describe('queued idempotency keys on PostgreSQL', () => {
	it('creates two durable receipts for repeated keys and forwards both despite a cached run result', async () => {
		const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
		const store = createIdempotencyStore(client, { table: 'svti_enqueue_cache', cleanupInterval: 0 });
		const runners = [];
		const create = (options = {}) => {
			const runner = createTaskRunner(client, { table: 'svti_enqueue_contract', idempotency: store, recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0, ...options });
			runners.push(runner); return runner;
		};
		try {
			const slot = await store.acquire('task:send:same-operation'); expect(slot.acquired).toBe(true); await slot.commit('cached-run-result');
			const first = create(), second = create(); await Promise.all([first.ready(), second.ready()]);
			const ids = await Promise.all([
				first.enqueue('send', { input: { submission: 1 }, idempotencyKey: 'same-operation' }),
				second.enqueue('send', { input: { submission: 2 }, idempotencyKey: 'same-operation' })
			]);
			expect(new Set(ids).size).toBe(2);
			const rows = (await client.query('SELECT svti_tasks_id::text AS id, svti_idempotency_key AS key FROM svti_enqueue_contract')).rows;
			expect(rows).toHaveLength(2); expect(rows).toEqual(expect.arrayContaining(ids.map(id => ({ id, key: 'same-operation' }))));
			const seen = []; const worker = create({ dispatchInterval: 10 });
			worker.register('send', async ({ input, idempotencyKey }) => { seen.push({ input, idempotencyKey }); return input.submission; });
			await worker.ready();
			expect(await Promise.all(ids.map(id => worker.await(id, { pollInterval: 10, timeout: 2000 })))).toEqual([1, 2]);
			expect(seen).toEqual(expect.arrayContaining(ids.map((_, i) => ({ input: { submission: i + 1 }, idempotencyKey: 'same-operation' }))));
			expect(seen).toHaveLength(2);
			first.register('send', async () => { throw new Error('run must return its existing cached result'); });
			await expect(first.run('send', { idempotencyKey: 'same-operation' })).resolves.toBe('cached-run-result');
			expect(await first.counts()).toMatchObject({ committed: 2, total: 2 });
		} finally {
			for (const runner of runners) runner.destroy(); store.destroy();
			await client.query('DROP TABLE IF EXISTS svti_enqueue_contract');
			await client.query('DROP TABLE IF EXISTS svti_enqueue_cache');
			await client.end();
		}
	});
});
