import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
describe('task heartbeat bounds', () => {
	let client, runner;
	beforeEach(() => {
		vi.useFakeTimers();
		setRuntimeEnv({ clock: { monotonic: () => Date.now(), now: () => Date.now() } });
		client = mockPgClient();
	});
	afterEach(() => { runner?.destroy(); resetRuntimeEnv(); vi.useRealTimers(); });
	function make(options = {}) {
		runner = createTaskRunner(client, { fenceTtl: 1, heartbeatInterval: 100, dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0, ...options });
		return runner;
	}
	it('coalesces slow renewals and drops the queued tick when the handler finishes', async () => {
		const blocked = deferred(), entered = deferred(), done = deferred();
		const query = client.query.bind(client);
		const renewals = [];
		client.query = (config, values) => {
			if (config?.name?.startsWith('tasks_heartbeat_')) {
				renewals.push(config);
				return blocked.promise.then(() => query(config, values));
			}
			return query(config, values);
		};
		make().register('work', async () => { entered.resolve(); return done.promise; });
		const result = runner.run('work', { input: null });
		await entered.promise;
		await vi.advanceTimersByTimeAsync(350);
		expect(renewals).toHaveLength(1);
		expect(renewals[0].query_timeout).toBe(100);
		done.resolve('finished');
		await expect(result).resolves.toBe('finished');
		blocked.resolve();
		await vi.advanceTimersByTimeAsync(1000);
		expect(renewals).toHaveLength(1);
	});
	it('runs at most one queued renewal after a slow successful reply', async () => {
		const blocked = deferred(), entered = deferred(), done = deferred();
		const query = client.query.bind(client);
		let calls = 0;
		client.query = (config, values) => {
			if (config?.name?.startsWith('tasks_heartbeat_') && ++calls === 1) return blocked.promise.then(() => query(config, values));
			return query(config, values);
		};
		make().register('work', async () => { entered.resolve(); return done.promise; });
		const result = runner.run('work', { input: null });
		await entered.promise;
		await vi.advanceTimersByTimeAsync(350);
		expect(calls).toBe(1);
		blocked.resolve();
		await vi.advanceTimersByTimeAsync(0);
		expect(calls).toBe(2);
		done.resolve('finished'); await result;
	});
	it('aborts before expiry when an external renewal never settles and suppresses its late SQL continuation', async () => {
		const blocked = deferred(), entered = deferred();
		const fence = { acquire: async () => true, release: async () => {}, heartbeat: vi.fn(() => blocked.promise) };
		const query = vi.spyOn(client, 'query');
		let signal;
		make({ fence }).register('work', async context => {
			signal = context.signal; entered.resolve();
			return new Promise((resolve, reject) => signal.addEventListener('abort', () => reject(new Error('handler cancelled')), { once: true }));
		});
		const outcome = runner.run('work', { input: null }).catch(error => error);
		await entered.promise;
		await vi.advanceTimersByTimeAsync(900);
		expect(signal.aborted).toBe(true);
		expect(fence.heartbeat).toHaveBeenCalledTimes(1);
		expect((await outcome).message).toBe('handler cancelled');
		blocked.resolve(true);
		await vi.advanceTimersByTimeAsync(2000);
		expect(query.mock.calls.filter(([config]) => config?.name?.startsWith('tasks_heartbeat_'))).toHaveLength(0);
	});
	it('cannot commit success when a handler ignores the lost-renewal signal', async () => {
		const entered = deferred(), done = deferred();
		const query = client.query.bind(client);
		client.query = (config, values) => config?.name?.startsWith('tasks_heartbeat_') ? new Promise(() => {}) : query(config, values);
		let signal;
		make().register('work', async context => { signal = context.signal; entered.resolve(); return done.promise; });
		const outcome = runner.run('work', { input: null }).catch(error => error);
		await entered.promise;
		await vi.advanceTimersByTimeAsync(900);
		expect(signal.aborted).toBe(true);
		done.resolve('stale success');
		expect((await outcome).message).toBe('fence lost');
		expect([...client._getTaskRows().values()][0].status).toBe('failed');
	});
	it.each([0, 800])('keeps a healthy lease with interval %s', async interval => {
		const entered = deferred(), done = deferred();
		let signal;
		make({ heartbeatInterval: interval }).register('work', async context => { signal = context.signal; entered.resolve(); return done.promise; });
		const result = runner.run('work', { input: null });
		await entered.promise;
		await vi.advanceTimersByTimeAsync(1700);
		expect(signal.aborted).toBe(false);
		done.resolve('ok'); await expect(result).resolves.toBe('ok');
	});
	it.each(['lost', 'never'])('withholds success while an external renewal is %s', async mode => {
		const entered = deferred(), done = deferred(), blocked = deferred();
		const fence = { acquire: async () => true, release: async () => {}, heartbeat: () => blocked.promise };
		make({ fence }).register('work', async () => { entered.resolve(); return done.promise; });
		let settled = false;
		const outcome = runner.run('work', { input: null }).catch(error => error).then(value => { settled = true; return value; });
		await entered.promise;
		await vi.advanceTimersByTimeAsync(100);
		done.resolve('stale success');
		await vi.advanceTimersByTimeAsync(1);
		expect(settled).toBe(false);
		if (mode === 'lost') blocked.resolve(false);
		else await vi.advanceTimersByTimeAsync(800);
		expect((await outcome).message).toBe('fence lost');
		expect([...client._getTaskRows().values()][0].status).toBe('failed');
		blocked.resolve(true);
	});
	it('does not execute a handler after acquiring the external fence consumed the confidence budget', async () => {
		const acquiring = deferred(), blocked = deferred();
		const fence = { acquire: () => { acquiring.resolve(); return blocked.promise; }, release: async () => {}, heartbeat: async () => true };
		const handler = vi.fn(async () => 'unsafe');
		make({ fence }).register('work', handler);
		const outcome = runner.run('work', { input: null }).catch(error => error);
		await acquiring.promise;
		await vi.advanceTimersByTimeAsync(950);
		blocked.resolve(true);
		expect((await outcome).message).toBe('fence lost');
		expect(handler).not.toHaveBeenCalled();
	});
	it('checks elapsed confidence when a returning handler starves the timer callback', async () => {
		make().register('work', async () => {
			vi.setSystemTime(Date.now() + 1000);
			return 'stale success';
		});
		await expect(runner.run('work', { input: null })).rejects.toThrow('fence lost');
		expect([...client._getTaskRows().values()][0].status).toBe('failed');
	});
	it.each([-1, NaN, Infinity, 1000, 1.5])('rejects unsafe heartbeat interval %s', interval => {
		expect(() => make({ heartbeatInterval: interval })).toThrow('heartbeatInterval');
	});
	it('renews short leases before their expiry with the default interval', async () => {
		const entered = deferred(), done = deferred();
		const query = vi.spyOn(client, 'query');
		runner = createTaskRunner(client, { fenceTtl: 1, dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0 });
		runner.register('work', async () => { entered.resolve(); return done.promise; });
		const result = runner.run('work', { input: null });
		await entered.promise;
		await vi.advanceTimersByTimeAsync(999);
		expect(query.mock.calls.filter(([config]) => config?.name?.startsWith('tasks_heartbeat_')).length).toBeGreaterThanOrEqual(2);
		done.resolve('ok'); await expect(result).resolves.toBe('ok');
	});
});
