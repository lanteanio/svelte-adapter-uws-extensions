import { afterEach, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { keySlot, coalescedByOwner } from '../../src/shared/cluster.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

const flush = () => new Promise(resolve => setImmediate(resolve));
afterEach(resetRuntimeEnv);

// A Cluster double over the in-memory mock: every key belongs to owner
// 'healthy' except the named ones, and refresh pipelines that touch an owner
// in `stalled` wait until released.
function ownedCluster(client, slowKeys, { stalled = new Set(['slow']), error = null } = {}) {
	const redis = client.redis;
	redis.nodes = () => [redis];
	redis.slots = Array.from({ length: 16384 }, () => ['healthy']);
	const slowSlots = new Set(slowKeys.map(key => keySlot(key)));
	for (const slot of slowSlots) redis.slots[slot] = ['slow'];
	const held = [];
	const execs = { slow: 0, healthy: 0 };
	const pipeline = redis.pipeline.bind(redis);
	redis.pipeline = () => {
		const pipe = pipeline();
		const keys = [];
		let refresh = true;
		return new Proxy(pipe, { get(target, name) {
			if (name === 'exec') return () => {
				if (!refresh) return pipe.exec();
				const owner = keys.some(key => slowSlots.has(keySlot(key))) ? 'slow' : 'healthy';
				execs[owner]++;
				if (stalled.has(owner)) return new Promise(resolve => held.push(async () => resolve(await pipe.exec())));
				if (error && owner === 'healthy') return Promise.resolve(keys.map(() => [error, null]));
				return pipe.exec();
			};
			if (typeof target[name] === 'function') return (...args) => {
				refresh &&= REFRESH_METHODS.has(name);
				if (typeof args[0] === 'string') keys.push(args[0]);
				return target[name](...args);
			};
			return target[name];
		} });
	};
	return { held, execs };
}
const REFRESH_METHODS = new Set(['expire', 'hpexpire', 'set']);
function captureHeartbeat(clock = {}) {
	const intervals = new Set();
	setRuntimeEnv({ clock, timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.add(timer); return timer; },
		clearInterval(timer) { intervals.delete(timer); }
	} });
	return () => [...intervals].find(timer => timer.delay === 17).fn;
}
const recordingBreaker = () => ({ state: 'healthy', isHealthy: true, guard() {}, success: vi.fn(), failure: vi.fn(), subscribe() { return () => {}; } });
const openBreaker = breaker => Object.assign(breaker, {
	state: 'broken', isHealthy: false, guard() { throw new Error('circuit broken'); }
});
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
// The key that places a tracker's member `slow` on the slow owner.
const slowKey = (kind, client) => kind === 'presence' ? client.key('presence:topic:{slow}') : client.key('conns:slow');
function heartbeatTracker(kind, client, options = {}) {
	return kind === 'presence' ? createPresence(client, { key: 'id', heartbeat: 17, ...options })
		: createConnectionRegistry(client, { identify: ws => ws.getUserData().id, heartbeat: 17, ...options });
}
async function joinTracker(kind, tracker, id, topic, platform) {
	const ws = mockWs({ id, [WS_SESSION_ID]: id + ':' + topic });
	if (kind === 'presence') await tracker.join(ws, topic, platform);
	else await tracker.hooks.open(ws, { platform });
	return ws;
}

it.each([false, true])('completes each cycle from the owners it started while another owner is busy (failure %s)', async failure => {
	let now = 0;
	setRuntimeEnv({ clock: { monotonic: () => now } });
	const client = mockRedisClient();
	const error = failure ? new Error('refresh failed') : null;
	const { held } = ownedCluster(client, ['slow'], { error });
	const completed = [];
	const onError = vi.fn();
	const refresh = coalescedByOwner(client.redis, {
		key: key => key,
		commands: (key, out) => { out.push(['expire', key, 30]); },
		onError,
		onComplete: cycle => completed.push({ ...cycle })
	});
	try {
		refresh(['slow', 'healthy'], 1); await flush();
		expect(completed).toEqual([]);
		now = 40;
		refresh(['slow', 'healthy'], 2); await flush();
		expect(completed).toMatchObject([{ context: 2, deferred: 1, total: 1, failed: failure ? 1 : 0, error }]);
		expect(refresh.busySince()).toBe(0);
		expect(onError).toHaveBeenCalledTimes(failure ? 2 : 0);
		now = 50;
		await held.shift()(); await flush();
		// The busy owner answers its first refresh, then runs the newest items once.
		expect(completed.slice(1)).toMatchObject([{ context: 1, deferred: 0, total: 2, failed: failure ? 1 : 0, error }]);
		expect(held).toHaveLength(1);
		expect(refresh.busySince()).toBe(50);
		await held.shift()(); await flush();
		expect(completed.slice(2)).toMatchObject([{ context: 2, deferred: 0, total: 1, failed: 0, error: null }]);
		expect(refresh.busySince()).toBe(Infinity);
		expect(onError).toHaveBeenCalledTimes(failure ? 2 : 0);
	} finally { refresh.stop(); }
});

it('completes an empty submit at once and drops refreshes queued behind a busy owner', async () => {
	const client = mockRedisClient();
	const { held, execs } = ownedCluster(client, ['slow']);
	const completed = [];
	const refresh = coalescedByOwner(client.redis, {
		key: key => key,
		commands: (key, out) => { out.push(['expire', key, 30]); },
		onComplete: cycle => completed.push(cycle.context)
	});
	try {
		refresh(['slow'], 'first'); await flush();
		refresh(['slow'], 'queued'); await flush();
		expect(completed).toEqual(['queued']);
		refresh([], 'empty');
		expect(completed).toEqual(['queued', 'empty']);
		await held.shift()(); await flush();
		expect(completed).toEqual(['queued', 'empty', 'first']);
		expect(execs.slow).toBe(1);
	} finally { refresh.stop(); }
});

it('keeps an owner refreshing when its observers throw', async () => {
	const client = mockRedisClient();
	const { execs } = ownedCluster(client, [], { stalled: new Set(), error: new Error('refused') });
	const unhandled = [];
	const onUnhandled = error => unhandled.push(error);
	process.on('unhandledRejection', onUnhandled);
	const observer = () => { throw new Error('observer failed'); };
	const refresh = coalescedByOwner(client.redis, {
		key: key => key,
		commands: (key, out) => { out.push(['expire', key, 30]); },
		onError: observer, onSettled: observer, onComplete: observer
	});
	try {
		refresh(['healthy'], 1); await flush(); await flush();
		expect(refresh.busySince()).toBe(Infinity);
		refresh(['healthy'], 2); await flush(); await flush();
		expect(execs.healthy).toBe(2);
		expect(refresh.busySince()).toBe(Infinity);
		expect(unhandled).toEqual([]);
	} finally {
		process.off('unhandledRejection', onUnhandled);
		refresh.stop();
	}
});

it('routes each item once and sends its commands without grouping them again', async () => {
	const client = mockRedisClient();
	const { execs } = ownedCluster(client, ['{slow}:a'], { stalled: new Set() });
	let lookups = 0;
	const slots = client.redis.slots;
	client.redis.slots = new Proxy(slots, { get(target, name) {
		if (typeof name === 'string' && /^\d+$/.test(name)) lookups++;
		return target[name];
	} });
	const completed = [];
	const refresh = coalescedByOwner(client.redis, {
		key: tag => '{' + tag + '}:a',
		commands: (tag, out) => { for (const key of ['a', 'b', 'c']) out.push(['set', '{' + tag + '}:' + key, '1']); },
		onComplete: cycle => completed.push(cycle)
	});
	try {
		refresh(['slow', 'x', 'y'], 1); await flush();
		expect(lookups).toBe(3);
		expect(completed).toMatchObject([{ total: 9, failed: 0 }]);
		expect(execs.slow).toBe(1);
		expect(await client.redis.get('{slow}:c')).toBe('1');
	} finally { refresh.stop(); }
});

it.each(['presence', 'registry'])('coalesces slow %s ticks and stops queued work on destroy', async kind => {
	let now = 0;
	const intervals = new Set();
	setRuntimeEnv({ clock: { monotonic: () => now }, timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.add(timer); return timer; },
		clearInterval(timer) { intervals.delete(timer); }
	} });
	const client = mockRedisClient();
	const platform = mockPlatform();
	const tracker = kind === 'presence' ? createPresence(client, { key: 'id', heartbeat: 17 })
		: createConnectionRegistry(client, { identify: () => 'user', heartbeat: 17 });
	const ws = mockWs({ id: 'user', [WS_SESSION_ID]: 'session' });
	if (kind === 'presence') await tracker.join(ws, 'room', platform);
	else await tracker.hooks.open(ws, { platform });
	await flush();
	const pipeline = client.redis.pipeline.bind(client.redis);
	const pending = [];
	let calls = 0;
	client.redis.pipeline = () => {
		const pipe = pipeline();
		const exec = pipe.exec.bind(pipe);
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return () => { calls++; return new Promise(resolve => pending.push(async () => resolve(await exec()))); };
			return target[key];
		} });
	};
	const tick = [...intervals].find(timer => timer.delay === 17).fn;
	try {
		tick(); await flush();
		expect(calls).toBe(1);
		for (let i = 0; i < 20; i++) tick();
		await flush();
		expect(calls).toBe(1);
		now = 77;
		await pending.shift()(); await flush();
		expect(calls).toBe(2);
		if (kind === 'presence') expect(tracker.metrics().heartbeatLatencyMs).toBe(77);
		for (let i = 0; i < 20; i++) tick();
		await tracker.destroy();
		await pending.shift()(); await flush();
		expect(calls).toBe(2);
		tick(); await flush();
		expect(calls).toBe(2);
	} finally { await tracker.destroy(); }
});

it.each([['presence', false], ['registry', false], ['presence', true], ['registry', true]])('refreshes healthy %s owners across stalled ticks (departure %s)', async (kind, departure) => {
	const intervals = new Set();
	setRuntimeEnv({ timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.add(timer); return timer; },
		clearInterval(timer) { intervals.delete(timer); }
	} });
	const client = mockRedisClient();
	const platform = mockPlatform();
	const tracker = kind === 'presence' ? createPresence(client, { key: 'id', heartbeat: 17 })
		: createConnectionRegistry(client, { identify: ws => ws.getUserData().id, heartbeat: 17 });
	let slowWs;
	for (const owner of ['slow', 'healthy']) {
		const ws = mockWs({ id: owner, [WS_SESSION_ID]: owner });
		if (owner === 'slow') slowWs = ws;
		if (kind === 'presence') await tracker.join(ws, owner, platform);
		else await tracker.hooks.open(ws, { platform });
	}
	await flush();
	client.redis.nodes = () => [client.redis];
	client.redis.slots = Array.from({ length: 16384 }, () => ['healthy']);
	for (const key of client._hashes.keys()) {
		if (key.includes('slow')) client.redis.slots[keySlot(key)] = ['slow'];
	}
	const pipeline = client.redis.pipeline.bind(client.redis);
	const pending = [];
	let slow = 0;
	let healthy = 0;
	client.redis.pipeline = () => {
		const pipe = pipeline();
		const commands = [];
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return () => {
				if (!commands.every(command => command[0] === 'expire' || command[0] === 'hpexpire')) return pipe.exec();
				if (commands.some(command => command[1].includes('slow'))) {
					slow++;
					return new Promise(resolve => pending.push(async () => resolve(await pipe.exec())));
				}
				healthy++;
				return pipe.exec();
			};
			if (typeof target[key] === 'function') return (...args) => {
				commands.push([key, ...args]);
				return target[key](...args);
			};
			return target[key];
		} });
	};
	const tick = [...intervals].find(timer => timer.delay === 17).fn;
	try {
		for (let i = 0; i < 5; i++) { tick(); await flush(); }
		expect(healthy).toBe(5);
		expect(slow).toBe(1);
		if (departure) {
			if (kind === 'presence') await tracker.leave(slowWs, platform);
			else await tracker.hooks.close(slowWs, { platform });
		}
		await pending.shift()(); await flush();
		expect(slow).toBe(departure ? 1 : 2);
		await tracker.destroy();
		if (pending.length) await pending.shift()();
		await flush();
		expect(slow).toBe(departure ? 1 : 2);
	} finally { await tracker.destroy(); }
});

it.each(['presence', 'registry'])('%s reports healthy owners while another owner stalls', async kind => {
	let now = 0;
	const tickFn = captureHeartbeat({ monotonic: () => now });
	const client = mockRedisClient();
	const platform = mockPlatform();
	const breaker = recordingBreaker();
	const tracker = heartbeatTracker(kind, client, { breaker });
	for (const id of ['slow', 'healthy']) await joinTracker(kind, tracker, id, id, platform);
	await flush();
	const { held, execs } = ownedCluster(client, [kind === 'presence' ? client.key('presence:topic:{slow}') : client.key('conns:slow')]);
	const tick = tickFn();
	breaker.success.mockClear();
	try {
		tick(); await flush();
		expect(breaker.success).not.toHaveBeenCalled();
		for (const at of [100, 250]) {
			now = at;
			tick(); await flush();
			// The tick reads the age of the refresh still waiting on the stalled owner.
			if (kind === 'presence') expect(tracker.metrics().heartbeatLatencyMs).toBe(at);
		}
		expect(breaker.success).toHaveBeenCalledTimes(2);
		expect(breaker.failure).not.toHaveBeenCalled();
		expect(execs).toEqual({ slow: 1, healthy: 3 });
		now = 300;
		await held.shift()(); await flush();
		expect(breaker.success).toHaveBeenCalledTimes(3);
		if (kind === 'presence') expect(tracker.metrics().heartbeatLatencyMs).toBe(300);
		expect(execs.slow).toBe(2);
	} finally { await tracker.destroy(); }
});

it('reports the latency of an empty presence tick after the last member leaves', async () => {
	let now = 0;
	const tickFn = captureHeartbeat({ monotonic: () => now });
	const client = mockRedisClient();
	const platform = mockPlatform();
	const presence = heartbeatTracker('presence', client);
	const ws = await joinTracker('presence', presence, 'user', 'room', platform);
	const pipeline = client.redis.pipeline.bind(client.redis);
	client.redis.pipeline = () => {
		const pipe = pipeline();
		const exec = pipe.exec.bind(pipe);
		return new Proxy(pipe, { get(target, name) {
			if (name === 'exec') return async () => { now += 5000; return exec(); };
			return target[name];
		} });
	};
	const tick = tickFn();
	try {
		tick(); await flush();
		expect(presence.metrics().heartbeatLatencyMs).toBe(5000);
		client.redis.pipeline = pipeline;
		await presence.leave(ws, platform); await flush();
		tick(); await flush();
		expect(presence.metrics().heartbeatLatencyMs).toBe(0);
	} finally { await presence.destroy(); }
});

it.each([['presence', 3], ['registry', 12]])('%s routes each heartbeat unit once per Cluster tick', async (kind, units) => {
	const tickFn = captureHeartbeat();
	const client = mockRedisClient();
	const platform = mockPlatform();
	const tracker = heartbeatTracker(kind, client);
	for (let user = 0; user < 12; user++) await joinTracker(kind, tracker, 'u' + user, 'room' + (user % 3), platform);
	await flush();
	const { execs } = ownedCluster(client, [], { stalled: new Set() });
	let lookups = 0;
	const slots = client.redis.slots;
	client.redis.slots = new Proxy(slots, { get(target, name) {
		if (typeof name === 'string' && /^\d+$/.test(name)) lookups++;
		return target[name];
	} });
	const tick = tickFn();
	try {
		tick(); await flush();
		expect(execs.healthy).toBe(1);
		expect(lookups).toBe(units);
	} finally { await tracker.destroy(); }
});

it.each(['presence', 'registry'])('%s leaves a probing breaker its probe when a tick reaches no owner', async kind => {
	const tickFn = captureHeartbeat();
	const client = mockRedisClient();
	const platform = mockPlatform();
	const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
	const tracker = heartbeatTracker(kind, client, { breaker });
	await joinTracker(kind, tracker, 'slow', 'slow', platform);
	await flush();
	const { held, execs } = ownedCluster(client, [slowKey(kind, client)]);
	const tick = tickFn();
	try {
		tick(); await flush();
		expect(execs.slow).toBe(1);
		// Something else trips the shared breaker, which then probes.
		breaker.failure(new Error('unrelated failure'));
		await sleep(5);
		expect(breaker.state).toBe('probing');
		tick(); await flush();
		expect(execs.slow).toBe(1);
		// Nothing answered, so nothing closed the breaker, and its probe is left.
		expect(breaker.state).toBe('probing');
		expect(() => breaker.guard()).not.toThrow();
	} finally {
		for (const release of held.splice(0)) await release();
		await tracker.destroy();
		breaker.destroy();
	}
});

it.each(['presence', 'registry'])('%s reports no breaker success for a tick that reached no owner', async kind => {
	const tickFn = captureHeartbeat();
	const client = mockRedisClient();
	const platform = mockPlatform();
	const breaker = recordingBreaker();
	const tracker = heartbeatTracker(kind, client, { breaker });
	await joinTracker(kind, tracker, 'slow', 'slow', platform);
	await flush();
	const { held } = ownedCluster(client, [slowKey(kind, client)]);
	const tick = tickFn();
	try {
		tick(); await flush();
		breaker.success.mockClear();
		tick(); await flush();
		expect(breaker.success).not.toHaveBeenCalled();
		await held.shift()(); await flush();
		// The stalled refresh answers for its own tick; the newest items rerun.
		expect(breaker.success).toHaveBeenCalledTimes(1);
		await held.shift()(); await flush();
		expect(breaker.success).toHaveBeenCalledTimes(2);
	} finally {
		for (const release of held.splice(0)) await release();
		await tracker.destroy();
	}
});

it.each(['presence', 'registry'])('%s skips a refresh that waited for a busy owner while the breaker is open', async kind => {
	const tickFn = captureHeartbeat();
	const client = mockRedisClient();
	const platform = mockPlatform();
	const breaker = recordingBreaker();
	const tracker = heartbeatTracker(kind, client, { breaker });
	await joinTracker(kind, tracker, 'slow', 'slow', platform);
	await flush();
	const { held, execs } = ownedCluster(client, [slowKey(kind, client)]);
	const tick = tickFn();
	try {
		tick(); await flush();
		tick(); await flush();
		expect(execs.slow).toBe(1);
		openBreaker(breaker);
		await held.shift()(); await flush();
		expect(execs.slow).toBe(1);
		expect(held).toHaveLength(0);
	} finally {
		for (const release of held.splice(0)) await release();
		await tracker.destroy();
	}
});
