import { afterEach, describe, expect, it, vi } from 'vitest';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockPgClient } from '../helpers/mock-pg.js';

const runners = [];
afterEach(() => { for (const runner of runners.splice(0)) runner.destroy(); resetRuntimeEnv(); });
function setup() {
	const ticks = new Map();
	setRuntimeEnv({ timers: {
		setInterval: (fn, ms) => { ticks.set(ms, fn); return { unref() {} }; },
		clearInterval: () => {}
	} });
	const client = mockPgClient();
	const runner = createTaskRunner(client, {
		dispatchInterval: 1000, recoveryInterval: 2000, cleanupInterval: 0,
		dispatchBatchSize: 1, recoveryBatchSize: 1
	});
	runners.push(runner);
	return { client, runner, ticks };
}

describe('task claims select compatible handlers before their batch limit', () => {
	it('does no dispatch or recovery query when no handlers are registered', async () => {
		const { client, runner, ticks } = setup();
		// Construction starts schema readiness independently of sweeps.
		await runner.ready();
		const query = vi.spyOn(client, 'query');
		await ticks.get(1000)();
		await ticks.get(2000)();
		expect(query).not.toHaveBeenCalled();
	});

	it.each(['pending', 'running'])('leaves unsupported %s rows unchanged and runs compatible work behind them', async (status) => {
		const { client, runner, ticks } = setup();
		runner.register('local', async ({ input }) => input);
		const unknown = [];
		for (let i = 0; i < 30; i++) unknown.push(await runner.enqueue('other-tier', { input: i }));
		const known = await runner.enqueue('local', { input: 'done' });
		const rows = client._getTaskRows();
		for (const [i, id] of [...unknown, known].entries()) {
			const row = rows.get(id);
			row.created_at = Date.now() - 60000 + i;
			if (status === 'running') {
				row.status = status;
				row.attempts = 3;
				row.fence_expires_at = Date.now() - 60000 + i;
			}
		}
		const before = unknown.map(id => structuredClone(rows.get(id)));
		const sweep = ticks.get(status === 'pending' ? 1000 : 2000);
		for (let i = 0; i < 4; i++) await sweep();
		await vi.waitFor(() => expect(rows.get(known).status).toBe('committed'));
		expect(rows.get(known).result).toBe('done');
		expect(unknown.map(id => rows.get(id))).toEqual(before);
		const listed = await runner.list({ name: 'other-tier', status, limit: 100 });
		expect(listed).toHaveLength(30);
		expect(listed.every(row => row.attempts === (status === 'pending' ? 0 : 3))).toBe(true);

		// Restoring this handler makes its existing rows eligible without
		// recreating input, changing task IDs, or another worker expiring a fence.
		runner.register('other-tier', async ({ input }) => input);
		await sweep();
		await vi.waitFor(() => expect(rows.get(unknown[0]).status).toBe('committed'));
		expect(rows.get(unknown[0]).attempts).toBe(status === 'pending' ? 1 : 4);
	});
});
