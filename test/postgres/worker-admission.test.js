import { afterEach, expect, it, vi } from 'vitest';
import { createMetrics } from '../../src/prometheus/index.js';
import { createTaskRunner } from '../../src/postgres/tasks.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
vi.mock('node:worker_threads', () => {
	const instances = [];
	class Worker {
		constructor() { this.handlers = new Map(); this.dispatched = []; this.terminated = false; instances.push(this); }
		on(name, fn) { this.handlers.set(name, fn); return this; }
		postMessage(message) { if (message.type === 'abort' && this.failAbort) throw new Error('abort send failed'); if (message.input?.uncloneable) throw new Error('clone failed'); this.dispatched.push(message); }
		emit(name, value) { this.handlers.get(name)?.(value); }
		terminate() { this.terminated = true; if (this.failTermination) return Promise.reject(new Error('termination failed')); if (this.deferTermination) return new Promise(resolve => { this.finishTermination = () => { this.emit('exit', 0); resolve(); }; }); this.emit('exit', 0); return Promise.resolve(); }
		unref() {}
	}
	Worker.instances = instances;
	return { Worker };
});
const { Worker } = await import('node:worker_threads');
const { createWorkerPool } = await import('../../src/postgres/_tasks-worker-pool.js');
const owned = [];
const path = 'file:///test/handler.js';
const flush = () => new Promise(resolve => setImmediate(resolve));
afterEach(() => { for (const owner of owned.splice(0)) owner.destroy(); Worker.instances.length = 0; vi.useRealTimers(); resetRuntimeEnv(); });
function pool(options = {}) { const p = createWorkerPool({ path, pool: { idleTimeout: 0 } }, 'work', options); owned.push(p); return p; }
function finish(worker, result = 'ok') { const message = worker.dispatched.findLast(message => message.type === 'run'); worker.emit('message', { type: 'result', id: message.id, result }); }

it('admits a finite number of jobs and terminates active threads on destroy', async () => {
	const p = pool();
	const results = Array.from({ length: 1000 }, () => p.run({ input: 'x' }).catch(error => error));
	await flush();
	expect(Worker.instances).toHaveLength(1);
	expect(await results[257]).toBeInstanceOf(RangeError);
	expect(p.capacity(1)).toBe(0);
	p.destroy();
	expect(Worker.instances[0].terminated).toBe(true);
	expect((await Promise.all(results)).filter(result => result instanceof RangeError)).toHaveLength(743);
});

it('bounds input bytes independently of the job count and releases the reservation', async () => {
	const p = pool();
	const permit = p.reserve(8 * 1024 * 1024);
	expect(() => p.reserve(1)).toThrow(RangeError);
	permit.release(); permit.release();
	expect(p.capacity(1)).toBe(257);
});

it('removes aborted queued jobs before the worker can execute them', async () => {
	const p = pool();
	const first = p.run({ input: 'first' });
	const controller = new AbortController();
	const second = p.run({ input: 'cancelled', signal: controller.signal }).catch(error => error);
	const reason = new Error('cancel queued'); controller.abort(reason);
	expect(await second).toBe(reason);
	finish(Worker.instances[0]); await first;
	expect(Worker.instances[0].dispatched.map(message => message.input)).toEqual(['first']);
	expect(p.capacity(1)).toBe(257);
});

it('expires queued work without replaying it when capacity returns', async () => {
	vi.useFakeTimers();
	const p = pool();
	const first = p.run({ input: 'first' });
	const second = p.run({ input: 'expired' }).catch(error => error);
	await vi.advanceTimersByTimeAsync(30000);
	expect(await second).toMatchObject({ name: 'TimeoutError', service: 'postgres tasks', ms: 30000 });
	finish(Worker.instances[0]); await first;
	expect(Worker.instances[0].dispatched).toHaveLength(1);
});

it('settles running and queued work when the last worker exits without an error event', async () => {
	const p = pool();
	const first = p.run({ input: 1 }).catch(error => error);
	const second = p.run({ input: 2 }).catch(error => error);
	Worker.instances[0].emit('exit', 0);
	expect((await first).message).toContain('worker exited');
	expect((await second).message).toContain('worker exited');
	expect(p.capacity(1)).toBe(257);
	const third = p.run({ input: 3 }); finish(Worker.instances[1]); await third;
});

it('recovers capacity after synchronous postMessage failure and ignores unknown reply types', async () => {
	const p = pool();
	await expect(p.run({ input: { uncloneable: true } })).rejects.toThrow('clone failed');
	const next = p.run({ input: 2 });
	const worker = Worker.instances[0];
	worker.emit('message', { type: 'other', id: worker.dispatched[0].id });
	expect(p.capacity(1)).toBe(256);
	finish(worker); await next;
	expect(p.capacity(1)).toBe(257);
});

function runnerSetup(options = {}) {
	const ticks = new Map();
	setRuntimeEnv({ timers: { setInterval: (fn, ms) => { ticks.set(ms, fn); return { unref() {} }; }, clearInterval: () => {} } });
	const client = mockPgClient(), metrics = createMetrics();
	const runner = createTaskRunner(client, { dispatchInterval: 1000, recoveryInterval: 2000, heartbeatInterval: 0, cleanupInterval: 0, metrics, ...options });
	runner.register('work', null, { worker: { path, pool: { idleTimeout: 0 } } });
	owned.push(runner);
	return { runner, client, metrics, ticks };
}

it('reserves before SQL so concurrent sweeps and inline callers cannot consume the same worker capacity', async () => {
	const { runner, client, ticks } = runnerSetup({ maxPayloadBytes: 8 * 1024 * 1024 });
	const pending = await runner.enqueue('work', { input: 1 });
	const stuck = await runner.enqueue('work', { input: 2 });
	Object.assign(client._getTaskRows().get(stuck), { status: 'running', attempts: 2, fence_expires_at: Date.now() - 1000 });
	const query = client.query.bind(client); let release, entered;
	const held = new Promise(resolve => { entered = resolve; });
	client.query = async (...args) => {
		if (args[0]?.name?.startsWith('tasks_claim_registered_')) { entered(); await new Promise(resolve => { release = resolve; }); }
		return query(...args);
	};
	const dispatch = ticks.get(1000)(); await held;
	await ticks.get(2000)();
	await expect(runner.run('work', { input: 'no capacity' })).rejects.toBeInstanceOf(RangeError);
	expect(client._getTaskRows().get(stuck).attempts).toBe(2);
	expect(client._getTaskRows().size).toBe(2);
	release(); await dispatch;
	await vi.waitFor(() => expect(Worker.instances[0]?.dispatched).toHaveLength(1));
	finish(Worker.instances[0]);
	await vi.waitFor(() => expect(client._getTaskRows().get(pending).status).toBe('committed'));
	await ticks.get(2000)();
	await vi.waitFor(() => expect(Worker.instances[0].dispatched).toHaveLength(2));
	finish(Worker.instances[0]);
	await vi.waitFor(() => expect(client._getTaskRows().get(stuck).status).toBe('committed'));
});

it('a nearly full worker pool leaves the complete inline sweep budget available', async () => {
	const { runner, client, ticks } = runnerSetup({ maxPayloadBytes: 8 * 1024 * 1024, dispatchBatchSize: 5 });
	runner.register('inline', async ({ input }) => input);
	const workerId = await runner.enqueue('work', { input: 1 });
	const inlineIds = [];
	for (let i = 0; i < 4; i++) inlineIds.push(await runner.enqueue('inline', { input: i }));
	await ticks.get(1000)();
	await vi.waitFor(() => expect(inlineIds.every(id => client._getTaskRows().get(id).status === 'committed')).toBe(true));
	expect(client._getTaskRows().get(workerId).status).toBe('running');
	finish(Worker.instances[0]);
	await vi.waitFor(() => expect(client._getTaskRows().get(workerId).status).toBe('committed'));
});

it('releases admission after SQL insert failure and after a retried worker task settles', async () => {
	const { runner, client, metrics } = runnerSetup();
	const query = client.query.bind(client); let fail = true;
	client.query = (...args) => {
		if (fail && args[0]?.text?.includes('INSERT INTO') && args[0].text.includes("'running'")) { fail = false; return Promise.reject(new Error('insert unavailable')); }
		return query(...args);
	};
	await expect(runner.run('work', { input: 1 })).rejects.toThrow('insert unavailable');
	expect(metrics.serialize()).toContain('tasks_worker_reserved_bytes 0');
	runner.register('retry', null, { worker: { path, pool: { idleTimeout: 0 } }, retry: { maxAttempts: 2, backoff: () => 0 } });
	const pending = runner.run('retry', { input: 'payload' });
	await vi.waitFor(() => expect(Worker.instances[0]?.dispatched).toHaveLength(1));
	const worker = Worker.instances[0]; worker.emit('message', { type: 'error', id: worker.dispatched[0].id, error: { message: 'retry me' } });
	await vi.waitFor(() => expect(worker.dispatched).toHaveLength(2));
	expect(metrics.serialize()).not.toContain('tasks_worker_reserved_bytes 0');
	finish(worker, 'done'); expect(await pending).toBe('done');
	expect(metrics.serialize()).toContain('tasks_worker_reserved_bytes 0');
	expect(metrics.serialize()).toContain('tasks_worker_active 0');
});

it('retains non-cooperative active work in admission until it actually settles', async () => {
	const p = pool(); const controller = new AbortController();
	const active = p.run({ input: 'busy', signal: controller.signal });
	controller.abort(new Error('cancel'));
	expect(p.capacity(1)).toBe(256);
	expect(Worker.instances[0].dispatched.map(message => message.type)).toEqual(['run', 'abort']);
	finish(Worker.instances[0]); await active;
	expect(p.capacity(1)).toBe(257);
});

it('retires a worker whose abort message fails without leaking admission or throwing from the listener', async () => {
	const p = pool(); const controller = new AbortController();
	const active = p.run({ input: 'busy', signal: controller.signal }).catch(error => error);
	const queued = p.run({ input: 'next' }).catch(error => error);
	Worker.instances[0].failAbort = true;
	Worker.instances[0].deferTermination = true;
	controller.abort(new Error('cancel'));
	expect(p.capacity(1)).toBe(255);
	Worker.instances[0].finishTermination();
	expect((await active).message).toBe('abort send failed');
	expect((await queued).message).toBe('abort send failed');
	expect(Worker.instances[0].terminated).toBe(true);
	expect(p.capacity(1)).toBe(257);
});

it('executes the bounded persisted input snapshot instead of a mutated caller object', async () => {
	const { runner, client } = runnerSetup();
	const input = { value: 'before' };
	const pending = runner.run('work', { input });
	input.value = 'after';
	await vi.waitFor(() => expect(Worker.instances[0]?.dispatched).toHaveLength(1));
	expect(Worker.instances[0].dispatched[0].input).toEqual({ value: 'before' });
	expect([...client._getTaskRows().values()][0].input).toEqual({ value: 'before' });
	finish(Worker.instances[0]); await pending;
});

it('releases sweep reservations after a failed claim without touching pending rows', async () => {
	const { runner, client, metrics, ticks } = runnerSetup({ maxPayloadBytes: 8 * 1024 * 1024 });
	const id = await runner.enqueue('work', { input: 1 });
	const query = client.query.bind(client); let fail = true;
	client.query = (...args) => {
		if (fail && args[0]?.name?.startsWith('tasks_claim_registered_')) { fail = false; return Promise.reject(new Error('claim unavailable')); }
		return query(...args);
	};
	await ticks.get(1000)();
	expect(metrics.serialize()).toContain('tasks_worker_reserved_bytes 0');
	expect(client._getTaskRows().get(id)).toMatchObject({ status: 'pending', attempts: 0 });
	await ticks.get(1000)();
	await vi.waitFor(() => expect(Worker.instances[0]?.dispatched).toHaveLength(1));
	finish(Worker.instances[0]);
	await vi.waitFor(() => expect(client._getTaskRows().get(id).status).toBe('committed'));
	expect(metrics.serialize()).toContain('tasks_worker_reserved_bytes 0');
});

it('retains a worker whose termination request fails until its exit event', async () => {
	const p = pool(); const controller = new AbortController();
	const active = p.run({ input: 'busy', signal: controller.signal }).catch(error => error);
	const worker = Worker.instances[0]; worker.failAbort = true; worker.failTermination = true;
	controller.abort(new Error('cancel')); await flush();
	expect(p.capacity(1)).toBe(256);
	worker.emit('exit', 1);
	expect((await active).message).toBe('abort send failed');
	expect(p.capacity(1)).toBe(257);
});
