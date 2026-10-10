import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';

const table = 'svti_tasks_handler_selection';
let client;
const runners = [];
beforeAll(() => {
	client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
});
afterEach(async () => {
	for (const runner of runners.splice(0)) runner.destroy();
	resetRuntimeEnv();
	await client.query(`DROP TABLE IF EXISTS ${table}`);
});
afterAll(async () => { await client.end(); });

describe('task handler selection against Postgres', () => {
	it.each(['pending', 'running'])('does not claim incompatible %s rows before compatible work', async (status) => {
		const ticks = new Map();
		setRuntimeEnv({ timers: {
			setInterval: (fn, ms) => { ticks.set(ms, fn); return { unref() {} }; },
			clearInterval: () => {}
		} });
		const runner = createTaskRunner(client, {
			table, dispatchInterval: 1000, recoveryInterval: 2000, cleanupInterval: 0,
			dispatchBatchSize: 1, recoveryBatchSize: 1
		});
		runners.push(runner);
		runner.register('local', async ({ input }) => input);
		const unknown = [];
		for (let i = 0; i < 12; i++) unknown.push(await runner.enqueue('other-tier', { input: i }));
		const known = await runner.enqueue('local', { input: 'done' });
		await client.query(`UPDATE ${table} SET created_at = now() - interval '1 hour' WHERE name = 'other-tier'`);
		if (status === 'running') {
			await client.query(`UPDATE ${table} SET status = 'running', attempts = 3, fence_expires_at = created_at`);
		}
		const readUnknown = async () => (await client.query(`SELECT svti_tasks_id, status, fence, fence_expires_at, attempts, updated_at FROM ${table} WHERE name = 'other-tier' ORDER BY svti_tasks_id`)).rows;
		const before = await readUnknown();
		const sweep = ticks.get(status === 'pending' ? 1000 : 2000);
		for (let i = 0; i < 4; i++) await sweep();
		expect(await runner.await(known)).toBe('done');
		expect(await readUnknown()).toEqual(before);
		expect(await runner.list({ name: 'other-tier', status })).toHaveLength(12);

		// A compatible worker restores service for these same task IDs.
		const peer = createTaskRunner(client, {
			table, dispatchInterval: 3000, recoveryInterval: 4000, cleanupInterval: 0,
			dispatchBatchSize: 12, recoveryBatchSize: 12
		});
		runners.push(peer);
		peer.register('other-tier', async ({ input }) => input);
		await ticks.get(status === 'pending' ? 3000 : 4000)();
		await vi.waitFor(async () => {
			const rows = await readUnknown();
			expect(rows.every(row => row.status === 'committed')).toBe(true);
			expect(rows.every(row => row.attempts === (status === 'pending' ? 1 : 4))).toBe(true);
		});
	});
});
