import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPgClient } from '../../src/postgres/index.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const state = vi.hoisted(() => ({ clients: [], events: [], connect: null, query: null, end: null, probe: null, route: null }));
vi.mock('pg', async () => {
	const { EventEmitter } = await import('node:events');
	// A notification session confirms itself with a probe sent through the
	// pool; deliver it to every open session LISTENing on that channel.
	// state.route replaces that delivery, as a transaction-pooling proxy would.
	state.probe = (text, values) => {
		if (/pg_notify/.test(text)) {
			if (state.route) return state.route(values);
			for (const client of state.clients) if (client.listening.has(values[0])) queueMicrotask(() => client.emit('notification', { channel: values[0], payload: values[1] }));
		}
		return Promise.resolve({ rows: [] });
	};
	class Pool extends EventEmitter {
		query(text, values) { return state.probe(text, values); }
		end = vi.fn(async () => { state.events.push('pool.end'); });
	}
	class Client extends EventEmitter {
		constructor() { super(); this.endCall = this.end; this.queryCall = this.query; state.clients.push(this); }
		listening = new Set();
		connect = vi.fn(() => state.connect?.(this) ?? Promise.resolve());
		query = vi.fn((sql) => {
			const match = /^(UN)?LISTEN "(.*)"$/.exec(sql);
			if (match) match[1] ? this.listening.delete(match[2]) : this.listening.add(match[2]);
			return state.query?.(this, sql) ?? Promise.resolve({ rows: [{ acquired: true }] });
		});
		end = vi.fn(async () => { state.events.push('client.end'); await state.end?.(this); this.emit('end'); });
	}
	return { default: { Pool, Client } };
});

function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
async function tick() { for (let i = 0; i < 30; i++) await Promise.resolve(); }
// Session statements without the startup probe's private LISTEN/UNLISTEN.
const calls = (client) => client.queryCall.mock.calls.map(([sql]) => sql).filter((sql) => !sql.includes('svti_notify_probe_'));

describe('Postgres notification session lifecycle', () => {
	let owners, bridges, diagnostics;
	beforeEach(() => {
		vi.useFakeTimers(); state.clients = []; state.events = []; state.connect = state.query = state.end = state.route = null;
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
		const pool = { query: vi.fn((text, values) => state.probe(text, values)), end: vi.fn() };
		const { owner, bridge, platform } = setup({}, { pool }); await bridge.activate(platform); await owner.end();
		expect(pool.end).not.toHaveBeenCalled(); expect(state.clients[0].endCall).not.toHaveBeenCalled();
		state.clients[0].emit('notification', { channel: 'lifecycle', payload: '{"topic":"x","event":"y"}' });
		expect(platform.published).toHaveLength(1); await bridge.deactivate(); expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
	});
	it('coalesces concurrent activation and does not LISTEN before connect settles', async () => {
		const gate = deferred(); state.connect = () => gate.promise;
		const { bridge, platform } = setup(); const first = bridge.activate(platform); const second = bridge.activate(platform); await tick();
		expect(state.clients).toHaveLength(1); expect(state.clients[0].queryCall).not.toHaveBeenCalled();
		gate.resolve(); await Promise.all([first, second]); expect(calls(state.clients[0])).toEqual(['LISTEN "lifecycle"']);
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
		expect(calls(state.clients[0])).toEqual(['SELECT pg_try_advisory_lock($1) AS acquired', 'LISTEN "lifecycle"', 'SELECT 1']);
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
		else state.query = (connection, sql) => (stage === 'listen' ? sql === 'LISTEN "lifecycle"' : sql.includes('pg_try_advisory_lock')) ? fail(connection) : Promise.resolve({ rows: [{ acquired: true }] });
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
		expect(calls(state.clients[1])).toHaveLength(1);
		state.query = null; await vi.advanceTimersByTimeAsync(20);
		expect(calls(state.clients[1])).toEqual(['SELECT pg_try_advisory_lock($1) AS acquired', 'SELECT pg_try_advisory_lock($1) AS acquired', 'LISTEN "lifecycle"']);
	});

	it('caps jitter at the native timer limit instead of overflowing into immediate retries', async () => {
		const delays = [];
		setRuntimeEnv({ rng: { float: () => 0.9 }, timers: { set: (callback, ms) => { delays.push(ms); return setTimeout(callback, ms); } } });
		const { bridge, platform } = setup({ reconnectInterval: 2147483647 }); await bridge.activate(platform); delays.length = 0;
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

	it.each(['all', 'advisory'])('rejects a %s session whose probe never returns and takes no advisory lock', async (multiListener) => {
		// A transaction-pooling proxy hands the LISTEN backend back to its pool,
		// so a notification sent while the session is idle never reaches it.
		state.route = () => Promise.resolve({ rows: [] });
		const breaker = createCircuitBreaker({ failureThreshold: 5 });
		const { bridge, platform } = setup({ multiListener, lockId: 7, autoReconnect: false, breaker });
		try {
			const activation = bridge.activate(platform), rejected = expect(activation).rejects.toMatchObject({ name: 'ConnectionError', code: 'PG_NOTIFY_SESSION_UNSTABLE' });
			await vi.advanceTimersByTimeAsync(4999); expect(state.clients[0].endCall).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(1); await rejected;
			expect(calls(state.clients[0])).toEqual([]);
			expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
			state.route = null; await bridge.activate(platform);
			expect(calls(state.clients[1])).toEqual(multiListener === 'advisory' ? ['SELECT pg_try_advisory_lock($1) AS acquired', 'LISTEN "lifecycle"'] : ['LISTEN "lifecycle"']);
		} finally { breaker.destroy(); }
	});
	it.each([
		['an unconfirmed session', () => Promise.resolve({ rows: [] }), 'PG_NOTIFY_SESSION_UNSTABLE'],
		['a read-only query route', () => Promise.reject(Object.assign(new Error('cannot execute NOTIFY during recovery'), { code: '25006' })), 'PG_NOTIFY_SESSION_UNSTABLE']
	])('does not count %s as a dependency failure', async (_, route, code) => {
		state.route = route;
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const report = vi.spyOn(breaker, 'failure');
		const { bridge, platform } = setup({ autoReconnect: false, breaker });
		try {
			const rejected = expect(bridge.activate(platform)).rejects.toMatchObject({ name: 'ConnectionError', code });
			await vi.advanceTimersByTimeAsync(5000); await rejected;
			expect(report).not.toHaveBeenCalled(); expect(breaker.state).toBe('healthy');
		} finally { breaker.destroy(); }
	});
	it('names the read-only query route and keeps the driver error as the cause', async () => {
		const failure = Object.assign(new Error('cannot execute NOTIFY during recovery'), { code: '25006' });
		state.route = () => Promise.reject(failure);
		const { bridge, platform } = setup({ autoReconnect: false });
		await expect(bridge.activate(platform)).rejects.toMatchObject({ code: 'PG_NOTIFY_SESSION_UNSTABLE', message: expect.stringContaining('read-only'), cause: expect.objectContaining({ code: '25006' }) });
	});
	it('names a different database as a cause of a probe that never arrives', async () => {
		state.route = () => Promise.resolve({ rows: [] });
		const { bridge, platform } = setup({ autoReconnect: false });
		const rejected = expect(bridge.activate(platform)).rejects.toMatchObject({ message: expect.stringContaining('different database or server') });
		await vi.advanceTimersByTimeAsync(5000); await rejected;
	});
	it('releases a half-open breaker probe when local pool admission refuses the probe', async () => {
		const held = deferred();
		state.route = ([channel, payload]) => {
			if (channel === 'hold') return held.promise;
			for (const client of state.clients) if (client.listening.has(channel)) queueMicrotask(() => client.emit('notification', { channel, payload }));
			return Promise.resolve({ rows: [] });
		};
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 20 });
		const report = vi.spyOn(breaker, 'failure');
		const { owner, bridge, platform } = setup({ autoReconnect: false, breaker }, { maxInFlight: 1 });
		try {
			breaker.failure(new Error('offline')); report.mockClear(); await vi.advanceTimersByTimeAsync(20);
			const holding = owner.query('SELECT pg_notify($1, $2)', ['hold', 'x']);
			await expect(bridge.activate(platform)).rejects.toMatchObject({ code: 'PG_CAPACITY_EXCEEDED' });
			expect(report).not.toHaveBeenCalled();
			held.resolve({ rows: [] }); await holding;
			await bridge.activate(platform); expect(breaker.state).toBe('healthy');
		} finally { held.resolve({ rows: [] }); breaker.destroy(); }
	});
	it('bounds sending the probe through a stalled query route by the same deadline', async () => {
		let abandoned = null;
		state.route = () => new Promise(() => {});
		const breaker = createCircuitBreaker({ failureThreshold: 5 });
		const report = vi.spyOn(breaker, 'failure');
		const { owner, bridge, platform } = setup({ autoReconnect: false, breaker });
		const query = owner.query;
		owner.query = (text, values, options) => { abandoned = options?.signal; return query(text, values, options); };
		try {
			const activation = bridge.activate(platform);
			const rejected = expect(activation).rejects.toMatchObject({ name: 'ConnectionError', message: expect.stringContaining('could not be sent through the query route within 5 seconds') });
			await vi.advanceTimersByTimeAsync(4999); expect(state.clients[0].endCall).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(1); await rejected;
			expect(abandoned?.aborted).toBe(true);
			expect(report).toHaveBeenCalledTimes(1);
			expect(state.clients[0].endCall).toHaveBeenCalledTimes(1);
		} finally { breaker.destroy(); }
	});
	it('ignores a probe payload that does not carry this session nonce', async () => {
		state.route = ([channel]) => { for (const client of state.clients) client.emit('notification', { channel, payload: 'forged' }); return Promise.resolve({ rows: [] }); };
		const { bridge, platform } = setup({ autoReconnect: false });
		const rejected = expect(bridge.activate(platform)).rejects.toMatchObject({ code: 'PG_NOTIFY_SESSION_UNSTABLE' });
		await vi.advanceTimersByTimeAsync(5000); await rejected;
	});
	it('confirms a reconnected session again before resuming LISTEN', async () => {
		const { bridge, platform } = setup(); await bridge.activate(platform);
		state.route = () => Promise.resolve({ rows: [] });
		state.clients[0].emit('error', new Error('lost')); await tick();
		await vi.advanceTimersByTimeAsync(150); expect(state.clients).toHaveLength(2);
		await vi.advanceTimersByTimeAsync(5000); expect(calls(state.clients[1])).toEqual([]);
		expect(state.clients[1].endCall).toHaveBeenCalledTimes(1);
		state.route = null; await vi.advanceTimersByTimeAsync(300);
		expect(calls(state.clients[2])).toEqual(['LISTEN "lifecycle"']);
	});

});
