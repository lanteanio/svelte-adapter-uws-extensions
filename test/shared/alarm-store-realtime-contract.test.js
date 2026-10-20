import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { createAlarmStore as createRedisAlarmStore } from '../../src/redis/alarm-store.js';
import { createAlarmStore as createPgAlarmStore } from '../../src/postgres/alarm-store.js';

// The members svelte-realtime's configureAlarm requires of a durable store
// (svelte-realtime src/server/alarm.js, configureAlarm store validation).
// `due` is optional there; both stores provide it for restart recovery.
const REQUIRED = ['nextGeneration', 'get', 'set', 'delete', 'claim', 'complete', 'fail', 'getExecution', 'executions', 'requeue'];

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const realtimeDir = process.env.SVELTE_REALTIME_DIR || resolve(root, '..', 'svelte-realtime');
const realtimeAlarm = resolve(realtimeDir, 'src', 'server', 'alarm.js');
const realtime = existsSync(realtimeAlarm);

const stores = {
	redis: () => createRedisAlarmStore(mockRedisClient('app:')),
	postgres: () => createPgAlarmStore(mockPgClient(), { autoMigrate: false })
};

describe('durable alarm stores satisfy the realtime store contract', () => {
	it.each(Object.keys(stores))('%s implements every required member and due()', name => {
		const store = stores[name]();
		for (const member of [...REQUIRED, 'due']) expect(typeof store[member], member).toBe('function');
	});

	describe.skipIf(!realtime)('against the svelte-realtime source', () => {
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

		it.each(Object.keys(stores))('configureAlarm accepts the %s store', async name => {
			await load();
			expect(() => alarm.configureAlarm({ store: stores[name](), pollMs: 100000 })).not.toThrow();
		});

		it('retains a failed precise fire, then dispatches its explicit retry on the recovery poll', async () => {
			await load();
			const store = stores.redis();
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
			for (let waited = 0; waited < 2000 && !(await store.executions()).items.some(item => item.status === 'failed'); waited += 10) {
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
});
