import { expect, it, vi } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';

it('leaves default-sized durable rows pending beyond the local queue deadline until a thread returns', async () => {
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	const ticks = new Map();
	setRuntimeEnv({ timers: { setInterval: (fn, ms) => { ticks.set(ms, fn); return { unref() {} }; }, clearInterval: () => {} } });
	const table = 'svti_tasks_worker_admission';
	const runner = createTaskRunner(client, { table, dispatchInterval: 1000, recoveryInterval: 0, heartbeatInterval: 0, cleanupInterval: 0 });
	runner.register('work', null, { worker: { path: new URL('../../helpers/workers/count-runs.js', import.meta.url), pool: { idleTimeout: 0 } } });
	try {
		const first = await runner.enqueue('work', { input: { value: 'first', delay: 31000 } });
		const second = await runner.enqueue('work', { input: { value: 'second' } });
		const third = await runner.enqueue('work', { input: { value: 'third' } });
		await ticks.get(1000)(); await ticks.get(1000)();
		await vi.waitFor(async () => expect((await runner.list({ status: 'committed' })).some(row => row.id === first)).toBe(true), { timeout: 45000, interval: 200 });
		const waiting = await runner.list({ status: 'pending' });
		for (const id of [second, third]) expect(waiting.find(row => row.id === id)).toMatchObject({ status: 'pending', attempts: 0 });
		expect(await runner.await(first, { pollInterval: 20 })).toEqual({ count: 1, value: 'first' });
		await ticks.get(1000)();
		expect(await runner.await(second, { pollInterval: 20 })).toEqual({ count: 2, value: 'second' });
		await ticks.get(1000)();
		expect(await runner.await(third, { pollInterval: 20 })).toEqual({ count: 3, value: 'third' });
	} finally { runner.destroy(); resetRuntimeEnv(); await client.query(`DROP TABLE IF EXISTS ${table}`); await client.end(); }
}, 60000);
