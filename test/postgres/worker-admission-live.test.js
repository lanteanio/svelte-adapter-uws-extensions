import { expect, it } from 'vitest';
import { createWorkerPool } from '../../src/postgres/_tasks-worker-pool.js';

it('does not execute an aborted queued job on a real worker thread', async () => {
	const pool = createWorkerPool({ path: new URL('../helpers/workers/count-runs.js', import.meta.url), pool: { idleTimeout: 0 } }, 'count');
	const controller = new AbortController();
	try {
		const first = pool.run({ input: { delay: 100, value: 'first' } });
		const cancelled = pool.run({ input: { value: 'cancelled' }, signal: controller.signal }).catch(error => error);
		const reason = new Error('cancel queued'); controller.abort(reason);
		expect(await cancelled).toBe(reason);
		expect(await first).toEqual({ count: 1, value: 'first' });
		expect(await pool.run({ input: { value: 'last' } })).toEqual({ count: 2, value: 'last' });
	} finally { pool.destroy(); }
});
