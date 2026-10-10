import { expect, it, vi } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';

it('leaves durable worker rows pending until local admission capacity returns', async () => {
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
	const ticks = new Map();
	setRuntimeEnv({ timers: { setInterval: (fn, ms) => { ticks.set(ms, fn); return { unref() {} }; }, clearInterval: () => {} } });
	const table = 'svti_tasks_worker_admission';
	const runner = createTaskRunner(client, { table, dispatchInterval: 1000, recoveryInterval: 0, heartbeatInterval: 0, cleanupInterval: 0, maxPayloadBytes: 8 * 1024 * 1024 });
	runner.register('work', null, { worker: { path: new URL('../../helpers/workers/count-runs.js', import.meta.url), pool: { idleTimeout: 0 } } });
	try {
		const first = await runner.enqueue('work', { input: { value: 'first', delay: 300 } });
		const second = await runner.enqueue('work', { input: { value: 'second' } });
		await ticks.get(1000)(); await ticks.get(1000)();
		const waiting = (await runner.list({ status: 'pending' })).find(row => row.id === second);
		expect(waiting).toMatchObject({ status: 'pending', attempts: 0 });
		expect(await runner.await(first, { pollInterval: 20 })).toEqual({ count: 1, value: 'first' });
		await ticks.get(1000)();
		expect(await runner.await(second, { pollInterval: 20 })).toEqual({ count: 2, value: 'second' });
	} finally { runner.destroy(); resetRuntimeEnv(); await client.query(`DROP TABLE IF EXISTS ${table}`); await client.end(); }
});
