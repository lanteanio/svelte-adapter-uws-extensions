import { afterEach, beforeEach, expect, it } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createDistributedLock, LockLostError } from '../../src/redis/lock.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';

let now, tick, client;
const flush = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; }
beforeEach(() => {
	now = 0; client = mockRedisClient('lease-deadline:');
	setRuntimeEnv({ clock: { monotonic: () => now }, timers: {
		setInterval(fn) { tick = fn; return { unref() {} }; }, clearInterval() {},
		set(fn) { tick = fn; return { unref() {} }; }, clear() {}
	} });
});
afterEach(resetRuntimeEnv);

it('aborts and rejects after renewal errors outlast the confirmed lease', async () => {
	const metrics = createMetrics(), started = deferred(), finish = deferred(); let signal;
	const lock = createDistributedLock(client, { defaultTtlMs: 100, heartbeatMs: 20, metrics });
	client.redis.eval = async () => { throw new Error('partition'); };
	const pending = lock.withLock('key', async s => { signal = s; started.resolve(); await finish.promise; return 'done'; });
	const result = pending.then(value => ({ value }), error => ({ error }));
	await started.promise;
	for (now = 20; now <= 120; now += 20) { tick(); await flush(); }
	const aborted = signal.aborted; finish.resolve(); const outcome = await result;
	expect(aborted).toBe(true); expect(outcome.error).toBeInstanceOf(LockLostError);
	expect(signal.reason).toBe(outcome.error);
	expect(metrics.serialize()).toContain('lock_lost_total{key_class="all"} 1');
});

it('rejects an expired callback when no heartbeat tick ran and release cannot confirm ownership', async () => {
	const lock = createDistributedLock(client, { defaultTtlMs: 100 }); let signal;
	client.redis.eval = async () => { throw new Error('partition'); };
	await expect(lock.withLock('key', s => { signal = s; now = 120; return 'done'; })).rejects.toBeInstanceOf(LockLostError);
	expect(signal.aborted).toBe(true);
});

it('preserves callback errors after an expired uncertain release', async () => {
	const lock = createDistributedLock(client, { defaultTtlMs: 100 }); let signal;
	client.redis.eval = async () => { throw new Error('partition'); };
	const original = new Error('callback failure');
	await expect(lock.withLock('key', s => { signal = s; now = 120; throw original; })).rejects.toBe(original);
	expect(signal.reason).toBeInstanceOf(LockLostError);
});

it('allows best-effort release errors before the confirmed deadline', async () => {
	const lock = createDistributedLock(client, { defaultTtlMs: 100 });
	client.redis.eval = async () => { throw new Error('short failure'); };
	await expect(lock.withLock('key', () => { now = 50; return 'done'; })).resolves.toBe('done');
});

it('allows an authoritative release to confirm ownership after the local conservative deadline', async () => {
	const lock = createDistributedLock(client, { defaultTtlMs: 100 });
	await expect(lock.withLock('key', () => { now = 120; return 'done'; })).resolves.toBe('done');
});

it('aborts while a heartbeat reply is stuck and never revives on its late success', async () => {
	const started = deferred(), finish = deferred(), entered = deferred(), reply = deferred(); let signal, calls = 0;
	const realEval = client.redis.eval.bind(client.redis);
	client.redis.eval = async (script, ...args) => { if (script.includes("'PEXPIRE'")) { calls++; entered.resolve(); await reply.promise; return 1; } return realEval(script, ...args); };
	const lock = createDistributedLock(client, { defaultTtlMs: 100, heartbeatMs: 20 });
	const result = lock.withLock('key', async s => { signal = s; started.resolve(); await finish.promise; return 'done'; }).then(value => ({ value }), error => ({ error }));
	await started.promise; now = 20; tick(); await entered.promise;
	now = 120; tick(); await flush(); const aborted = signal.aborted;
	finish.resolve(); reply.resolve(); const outcome = await result;
	expect(aborted).toBe(true); expect(outcome.error).toBeInstanceOf(LockLostError); expect(calls).toBe(1);
});

it('anchors a successful renewal to its send time rather than reply time', async () => {
	const started = deferred(), finish = deferred(); let signal;
	client.redis.eval = async script => { if (script.includes("'PEXPIRE'")) { now = 70; return 1; } throw new Error('partition'); };
	const lock = createDistributedLock(client, { defaultTtlMs: 100, heartbeatMs: 20 });
	const result = lock.withLock('key', async s => { signal = s; started.resolve(); await finish.promise; return 'done'; }).then(value => ({ value }), error => ({ error }));
	await started.promise; now = 20; tick(); await flush(); now = 130; finish.resolve(); const outcome = await result;
	expect(outcome.error).toBeInstanceOf(LockLostError); expect(signal.aborted).toBe(true);
});

it('settles at expiry when the callback finished but its heartbeat never replied', async () => {
	const started = deferred(), finish = deferred(), entered = deferred(), reply = deferred();
	client.redis.eval = async () => { entered.resolve(); await reply.promise; return 1; };
	const lock = createDistributedLock(client, { defaultTtlMs: 100, heartbeatMs: 20 });
	const result = lock.withLock('key', async () => { started.resolve(); await finish.promise; return 'done'; }).then(value => ({ value }), error => ({ error }));
	await started.promise; now = 20; tick(); await entered.promise; finish.resolve(); await flush();
	now = 120; tick(); const outcome = await result; reply.resolve(); await flush();
	expect(outcome.error).toBeInstanceOf(LockLostError);
});

it('never starts a callback after a stale acquisition acknowledgement', async () => {
	const set = client.redis.set.bind(client.redis); let ran = false;
	client.redis.set = async (...args) => { const result = await set(...args); now = 120; return result; };
	const lock = createDistributedLock(client, { defaultTtlMs: 100 });
	await expect(lock.withLock('key', () => { ran = true; })).rejects.toBeInstanceOf(LockLostError);
	expect(ran).toBe(false); expect(await client.redis.get(client.key('lock:key'))).toBeNull();
});

it('coalesces synchronous renewal failures without wedging future retries', async () => {
	const started = deferred(), finish = deferred(); const define = client.redis.defineCommand.bind(client.redis); let definitions = 0;
	client.redis.defineCommand = (...args) => { if (++definitions === 1) throw new Error('registration failure'); return define(...args); };
	const lock = createDistributedLock(client, { defaultTtlMs: 100, heartbeatMs: 20 });
	const pending = lock.withLock('key', async () => { started.resolve(); await finish.promise; return 'done'; });
	await started.promise; now = 20; tick(); await flush(); now = 40; tick(); await flush(); finish.resolve();
	await expect(pending).resolves.toBe('done'); expect(definitions).toBe(3);
});


it('observes the exact lease deadline between non-divisible heartbeat intervals', async () => {
	let due = Infinity, delay, repeat = false;
	const handle = { unref() {} };
	setRuntimeEnv({ clock: { monotonic: () => now }, timers: {
		setInterval(fn, ms) { tick = fn; delay = ms; due = now + ms; repeat = true; return handle; },
		clearInterval() { due = Infinity; },
		set(fn, ms) { tick = fn; due = now + ms; repeat = false; return handle; },
		clear() { due = Infinity; }
	} });
	const advance = async target => { while (due <= target) { now = due; due = repeat ? due + delay : Infinity; tick(); await flush(); } now = target; };
	const started = deferred(), finish = deferred(), reply = deferred(); let signal;
	client.redis.eval = async () => { await reply.promise; return 1; };
	const lock = createDistributedLock(client, { defaultTtlMs: 1000, heartbeatMs: 300 });
	const result = lock.withLock('key', async s => { signal = s; started.resolve(); await finish.promise; }).then(value => ({ value }), error => ({ error }));
	await started.promise; await advance(1001); const aborted = signal.aborted;
	finish.resolve(); reply.resolve(); const outcome = await result;
	expect(aborted).toBe(true); expect(outcome.error).toBeInstanceOf(LockLostError);
});

it('rejects a renewal reply after the prior deadline even before its timer runs', async () => {
	const started = deferred(), finish = deferred(), entered = deferred(), reply = deferred(); let signal;
	client.redis.eval = async () => { entered.resolve(); await reply.promise; return 1; };
	const lock = createDistributedLock(client, { defaultTtlMs: 1000, heartbeatMs: 300 });
	const result = lock.withLock('key', async s => { signal = s; started.resolve(); await finish.promise; }).then(value => ({ value }), error => ({ error }));
	await started.promise; now = 300; tick(); await entered.promise;
	now = 1100; reply.resolve(); await flush(); const aborted = signal.aborted;
	finish.resolve(); const outcome = await result;
	expect(aborted).toBe(true); expect(outcome.error).toBeInstanceOf(LockLostError);
});
