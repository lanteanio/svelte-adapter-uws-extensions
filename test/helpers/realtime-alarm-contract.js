import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { mockPlatform } from '../../src/testing/mock-platform.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const realtimeDir = process.env.SVELTE_REALTIME_DIR || resolve(root, '..', 'svelte-realtime');
const realtimeAlarm = resolve(realtimeDir, 'src', 'server', 'alarm.js');

/** Whether the svelte-realtime source is checked out beside this package or at SVELTE_REALTIME_DIR. */
export const realtimeAvailable = existsSync(realtimeAlarm);

/**
 * Drive svelte-realtime's own alarm runtime (`configureAlarm`, the precise
 * timer and the recovery poll) against a durable store. `createStore()` returns
 * a fresh, empty store; it may be async. `acceptOnly` runs only the
 * configuration check, for a store double that executes no alarm operations.
 */
export function realtimeAlarmContract(name, createStore, { acceptOnly = false } = {}) {
	describe.skipIf(!realtimeAvailable)(`${name} store against the svelte-realtime source`, () => {
		let alarm, state, registry, savedPlatform;
		const load = async () => {
			alarm = await import(pathToFileURL(realtimeAlarm).href);
			({ state, registry } = await import(pathToFileURL(resolve(realtimeDir, 'src', 'server', 'state.js')).href));
			savedPlatform = state.cronPlatform;
		};
		afterEach(() => {
			if (!alarm) return;
			alarm._resetAlarms();
			alarm.configureAlarm(null);
			state.cronPlatform = savedPlatform;
			registry.delete('rooms/retained');
		});

		it('configureAlarm accepts the store', async () => {
			await load();
			const store = await createStore();
			expect(() => alarm.configureAlarm({ store, pollMs: 100000 })).not.toThrow();
		});

		it.skipIf(acceptOnly)('retains a failed precise fire, then dispatches its explicit retry on the recovery poll', async () => {
			await load();
			const store = await createStore();
			const seen = [];
			const onAlarm = async ctx => {
				seen.push(ctx.alarm);
				if (seen.length === 1) throw new Error('handler failure');
			};
			registry.set('rooms/retained', { __streamOptions: { alarm: { id: 'retained', onAlarm } } });
			state.cronPlatform = mockPlatform();
			alarm.configureAlarm({ store, pollMs: 100000 });
			const ctx = {};
			alarm._bindAlarmCtx(ctx, { wireTopic: 'room:retained', onAlarm, alarmId: 'retained' });
			await ctx.setAlarm(Date.now() + 5);
			for (let waited = 0; waited < 5000 && !(await store.executions()).items.some(item => item.status === 'failed'); waited += 10) {
				await new Promise(done => setTimeout(done, 10));
			}
			const [failed] = (await store.executions()).items;
			expect(failed).toMatchObject({ topic: 'room:retained', attempt: 1, status: 'failed', errorClass: 'handler', meta: { alarmId: 'retained' } });
			expect(seen[0]).toMatchObject({ attempt: 1, generation: failed.generation, idempotencyKey: JSON.stringify(['room:retained', failed.generation]) });
			expect(await store.requeue('room:retained', failed.generation, 1, { at: Date.now() })).toBe(true);
			await alarm._pollAlarms();
			expect(seen[1]).toMatchObject({ attempt: 2, generation: failed.generation, recovered: true });
			expect(await store.executions()).toEqual({ items: [], next: null });
		});
	});
}
