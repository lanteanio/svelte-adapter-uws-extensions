import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPgClient } from '../../src/postgres/index.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const state = vi.hoisted(() => ({ clients: [], events: [], connect: null, query: null, end: null }));
vi.mock('pg', async () => {
	const { EventEmitter } = await import('node:events');
	class Pool extends EventEmitter {
		query() { return Promise.resolve({ rows: [] }); }
		end = vi.fn(async () => { state.events.push('pool.end'); });
	}
	class Client extends EventEmitter {
		constructor() { super(); this.endCall = this.end; this.queryCall = this.query; state.clients.push(this); }
		connect = vi.fn(() => state.connect?.(this) ?? Promise.resolve());
		query = vi.fn((sql) => state.query?.(this, sql) ?? Promise.resolve({ rows: [{ acquired: true }] }));
		end = vi.fn(async () => { state.events.push('client.end'); await state.end?.(this); this.emit('end'); });
	}
	return { default: { Pool, Client } };
});

function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
async function tick() { for (let i = 0; i < 30; i++) await Promise.resolve(); }

describe('Postgres notification session lifecycle', () => {
	let owners, bridges, diagnostics;
	beforeEach(() => {
		vi.useFakeTimers(); state.clients = []; state.events = []; state.connect = state.query = state.end = null;
		setRuntimeEnv({ rng: { float: () => 0.5 } }); owners = []; bridges = []; diagnostics = captureRegistered();
	});
	afterEach(async () => {
		for (const bridge of bridges) await bridge.deactivate();
		for (const owner of owners) await owner.end();
		diagnostics.stop(); resetRuntimeEnv(); vi.useRealTimers();
	});
	function setup(options = {}, clientOptions = {}) {
		const owner = createPgClient({ connectionString: 'postgres://localhost/test', autoShutdown: false, ...clientOptions }); owners.push(owner);
		const bridge = createNotifyBridge(owner, { channel: 'lifecycle', reconnectInterval: 100, ...options }); bridges.push(bridge);
		return { owner, bridge, platform: mockPlatform() };
	}
	it('awaits dedicated session cleanup before closing its owned pool and stops reconnects', async () => {
		const { owner, bridge, platform } = setup(); await bridge.activate(platform);
		const gate = deferred(); state.end = () => gate.promise;
		const closing = owner.end(); expect(owner.end()).toBe(closing); await tick();
		expect(owner.pool.end).not.toHaveBeenCalled(); expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
		gate.resolve(); await closing;
		expect(state.events).toEqual(['client.end', 'pool.end']);
		await vi.advanceTimersByTimeAsync(10000); expect(state.clients).toHaveLength(1);
		expect(() => owner.createClient()).toThrow('closing'); await expect(bridge.activate(platform)).rejects.toThrow('closing');
	});
	it('closes standalone clients without a bridge and preserves callback end semantics', async () => {
		const { owner } = setup(); const first = owner.createClient(); const second = owner.createClient();
		const callback = vi.fn(); expect(first.end(callback)).toBeUndefined(); await tick(); expect(callback).toHaveBeenCalledWith();
		await owner.end(); expect(first.end).not.toBeUndefined(); expect(second.listenerCount('end')).toBe(0);
		expect(state.events).toEqual(['client.end', 'client.end', 'pool.end']);
	});
	it('uses the existing shutdown event to stop notification sessions before the pool', async () => {
		const { owner, bridge, platform } = setup({}, { autoShutdown: true }); await bridge.activate(platform);
		const listener = process.listeners('sveltekit:shutdown').find((fn) => fn === owner.end);
		expect(listener).toBeDefined(); await listener();
		expect(state.events).toEqual(['client.end', 'pool.end']); expect(process.listeners('sveltekit:shutdown')).not.toContain(listener);
	});
	it('keeps externally supplied pools and sessions caller-owned', async () => {
		const pool = { query: vi.fn(), end: vi.fn() };
		const { owner, bridge, platform } = setup({}, { pool }); await bridge.activate(platform); await owner.end();
		expect(pool.end).not.toHaveBeenCalled(); expect(state.clients[0].endCall).not.toHaveBeenCalled();
		state.clients[0].emit('notification', { channel: 'lifecycle', payload: '{"topic":"x","event":"y"}' });
		expect(platform.published).toHaveLength(1); await bridge.deactivate(); expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
	});
	it('coalesces concurrent activation and does not LISTEN before connect settles', async () => {
		const gate = deferred(); state.connect = () => gate.promise;
		const { bridge, platform } = setup(); const first = bridge.activate(platform); const second = bridge.activate(platform); await tick();
		expect(state.clients).toHaveLength(1); expect(state.clients[0].queryCall).not.toHaveBeenCalled();
		gate.resolve(); await Promise.all([first, second]); expect(state.clients[0].queryCall).toHaveBeenCalledTimes(1);
	});
	it('stops during a never-settling connect without resurrecting a late LISTEN', async () => {
		const gate = deferred(); state.connect = () => gate.promise;
		const { bridge, platform } = setup(); const activation = bridge.activate(platform); await tick();
		await bridge.deactivate(); await activation; expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
		gate.resolve(); await tick(); expect(state.clients[0].queryCall).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(10000); expect(state.clients).toHaveLength(1);
	});
	it('stops an in-flight LISTEN and handles a later rejection without reconnecting', async () => {
		const gate = deferred(); state.query = () => gate.promise;
		const { bridge, platform } = setup(); const activation = bridge.activate(platform); await tick();
		await bridge.deactivate(); await activation; gate.reject(new Error('late query failure')); await tick();
		expect(state.clients[0].endCall).toHaveBeenCalledTimes(1); await vi.advanceTimersByTimeAsync(10000); expect(state.clients).toHaveLength(1);
	});
	it.each(['all', 'advisory'])('waits for old session closure and jitter before replacing a %s connection', async (multiListener) => {
		const { bridge, platform } = setup({ multiListener, lockId: 7, pollInterval: 10 }); await bridge.activate(platform); const gate = deferred(); state.end = () => gate.promise;
		state.clients[0].emit('error', new Error('lost')); await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(1);
		gate.resolve(); await tick(); state.end = null;
		await vi.advanceTimersByTimeAsync(149); expect(state.clients).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1); expect(state.clients).toHaveLength(2);
	});
	it.each(['all', 'advisory'])('jittered reconnect is single flight in %s mode', async (multiListener) => {
		const { bridge, platform } = setup({ multiListener, lockId: 7, pollInterval: 10 }); await bridge.activate(platform);
		state.clients[0].emit('end'); await tick();
		await vi.advanceTimersByTimeAsync(149); expect(state.clients).toHaveLength(1);
		const gate = deferred(); state.connect = () => gate.promise;
		await vi.advanceTimersByTimeAsync(1); expect(state.clients).toHaveLength(2);
		await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(2);
		gate.resolve(); await tick();
	});
	it('does not overlap slow advisory polls or reacquire an already-held lock', async () => {
		const { bridge, platform } = setup({ multiListener: 'advisory', lockId: 7, pollInterval: 10 }); await bridge.activate(platform);
		const gate = deferred(); state.query = () => gate.promise; await vi.advanceTimersByTimeAsync(100);
		expect(state.clients[0].queryCall.mock.calls.map(([sql]) => sql)).toEqual(['SELECT pg_try_advisory_lock($1) AS acquired', 'LISTEN "lifecycle"', 'SELECT 1']);
		gate.resolve({ rows: [] }); await tick(); state.query = null;
	});
	it.each(['all', 'advisory'])('allows explicit retry with autoReconnect disabled in %s mode', async (multiListener) => {
		const { bridge, platform } = setup({ multiListener, lockId: 7, pollInterval: 10, autoReconnect: false }); await bridge.activate(platform);
		state.clients[0].emit('error', new Error('lost')); await tick(); await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(1);
		await bridge.activate(platform); expect(state.clients).toHaveLength(2);
	});
	it('serializes reactivation behind deactivation and does not reuse a closed session', async () => {
		const { bridge, platform } = setup(); await bridge.activate(platform); const gate = deferred(); state.end = () => gate.promise;
		const stop = bridge.deactivate(); const start = bridge.activate(platform); await tick(); expect(state.clients).toHaveLength(1);
		gate.resolve(); state.end = null; await Promise.all([stop, start]); expect(state.clients).toHaveLength(2);
	});
	it.each(['all', 'advisory'])('rejects awaited setup failures in %s mode', async (multiListener) => {
		state.connect = () => Promise.reject(new Error('connection refused'));
		const { bridge, platform } = setup({ multiListener, lockId: 7, autoReconnect: false });
		await expect(bridge.activate(platform)).rejects.toThrow('connection refused');
		expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
		state.connect = null; await bridge.activate(platform); expect(state.clients).toHaveLength(2);
	});

	it.each(['connect', 'listen', 'advisory'])('rejects an error event that precedes the %s startup promise rejection', async (stage) => {
		const failure = new Error('socket failed during ' + stage), late = deferred();
		const fail = connection => { connection.emit('error', failure); return late.promise; };
		if (stage === 'connect') state.connect = fail;
		else state.query = (connection, sql) => (stage === 'listen' ? sql.startsWith('LISTEN') : sql.includes('pg_try_advisory_lock')) ? fail(connection) : Promise.resolve({ rows: [{ acquired: true }] });
		const breaker = createCircuitBreaker({ failureThreshold: 3 });
		const report = vi.spyOn(breaker, 'failure');
		const { bridge, platform } = setup({ multiListener: stage === 'advisory' ? 'advisory' : 'all', lockId: 7, autoReconnect: false, breaker });
		try {
			await expect(bridge.activate(platform)).rejects.toBe(failure);
			expect(report).toHaveBeenCalledTimes(1);
			late.reject(new Error('later driver rejection')); await tick();
			await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(1);
			state.connect = state.query = null;
			await bridge.activate(platform); expect(state.clients).toHaveLength(2);
			expect(state.clients[1].queryCall).toHaveBeenCalledWith('LISTEN "lifecycle"');
		} finally { late.resolve(); breaker.destroy(); }
	});

	it('rejects the interrupted startup and reconnects only after the old session closes', async () => {
		const failure = new Error('startup disconnected'), close = deferred(), late = deferred();
		state.query = connection => { connection.emit('error', failure); return late.promise; };
		state.end = () => close.promise;
		const { bridge, platform } = setup();
		const activation = bridge.activate(platform), rejected = expect(activation).rejects.toBe(failure);
		await tick(); await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(1);
		state.query = state.end = null; close.resolve(); await rejected;
		late.reject(new Error('late query rejection')); await tick();
		await vi.advanceTimersByTimeAsync(149); expect(state.clients).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1); expect(state.clients).toHaveLength(2);
		expect(state.clients[1].queryCall).toHaveBeenCalledWith('LISTEN "lifecycle"');
	});

	it('starts follower election polling after an initial connection failure recovers', async () => {
		state.connect = () => Promise.reject(new Error('initial failure'));
		state.query = () => Promise.resolve({ rows: [{ acquired: false }] });
		const { bridge, platform } = setup({ multiListener: 'advisory', lockId: 7, pollInterval: 20 });
		await expect(bridge.activate(platform)).rejects.toThrow('initial failure');
		state.connect = null; await vi.advanceTimersByTimeAsync(150);
		expect(state.clients).toHaveLength(2);
		expect(state.clients[1].queryCall).toHaveBeenCalledTimes(1);
		state.query = null; await vi.advanceTimersByTimeAsync(20);
		expect(state.clients[1].queryCall.mock.calls.map(([sql]) => sql)).toEqual(['SELECT pg_try_advisory_lock($1) AS acquired', 'SELECT pg_try_advisory_lock($1) AS acquired', 'LISTEN "lifecycle"']);
	});

	it('caps jitter at the native timer limit instead of overflowing into immediate retries', async () => {
		const delays = [];
		setRuntimeEnv({ rng: { float: () => 0.9 }, timers: { set: (callback, ms) => { delays.push(ms); return setTimeout(callback, ms); } } });
		const { bridge, platform } = setup({ reconnectInterval: 2147483647 }); await bridge.activate(platform);
		state.clients[0].emit('error', new Error('lost')); await tick();
		expect(delays).toEqual([2147483647]);
		await vi.advanceTimersByTimeAsync(1000); expect(state.clients).toHaveLength(1);
	});
	it('rejects timer values that the native runtime cannot represent', () => {
		expect(() => setup({ reconnectInterval: 2147483648 })).toThrow('2147483647');
		expect(() => setup({ pollInterval: 2147483648 })).toThrow('2147483647');
	});

	it.each(['connect', 'listen'])('returns a half-open breaker probe when shutdown abandons %s', async (stage) => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 20 });
		breaker.failure(new Error('offline')); await vi.advanceTimersByTimeAsync(20);
		const gate = deferred();
		if (stage === 'connect') state.connect = () => gate.promise;
		else state.query = () => gate.promise;
		const { bridge, platform } = setup({ breaker }); const activation = bridge.activate(platform); await tick();
		try {
			expect(breaker.state).toBe('probing');
			await bridge.deactivate(); await activation; expect(breaker.state).toBe('probing');
			state.connect = state.query = null;
			await bridge.activate(platform); expect(breaker.state).toBe('healthy');
			gate.resolve({ rows: [] }); await tick();
		} finally { gate.resolve({ rows: [] }); breaker.destroy(); }
	});

});
