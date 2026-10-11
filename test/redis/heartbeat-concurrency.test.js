import { afterEach, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { keySlot, coalescedMultiSlot } from '../../src/shared/cluster.js';

const flush = () => new Promise(resolve => setImmediate(resolve));
afterEach(resetRuntimeEnv);

it.each([false, true])('reports completion after every owner and errors while a newer tick waits (failure %s)', async failure => {
	const client = mockRedisClient();
	const redis = client.redis;
	redis.nodes = () => [redis];
	redis.slots = Array.from({ length: 16384 }, () => ['healthy']);
	redis.slots[keySlot('slow')] = ['slow'];
	let release;
	const error = failure ? new Error('refresh failed') : null;
	const pipeline = redis.pipeline.bind(redis);
	redis.pipeline = () => {
		const pipe = pipeline();
		let slow = false;
		return new Proxy(pipe, { get(target, key) {
			if (key === 'expire') return (name, ttl) => { slow ||= name === 'slow'; return pipe.expire(name, ttl); };
			if (key === 'exec') return () => slow
				? new Promise(resolve => { release = async () => resolve(await pipe.exec()); })
				: failure ? Promise.resolve([[error, null]]) : pipe.exec();
			return target[key];
		} });
	};
	const complete = vi.fn();
	const onError = vi.fn();
	const refresh = coalescedMultiSlot(redis, complete, onError);
	refresh([['expire', 'slow', 30], ['expire', 'healthy', 30]], 42);
	await flush();
	expect(complete).not.toHaveBeenCalled();
	if (failure) {
		refresh([['expire', 'slow', 30], ['expire', 'healthy', 30]], 43);
		await flush();
		refresh([['expire', 'slow', 30], ['expire', 'healthy', 30]], 44);
		await flush();
		expect(onError).toHaveBeenCalledTimes(3);
	}
	await release(); await flush();
	expect(complete).toHaveBeenCalledExactlyOnceWith(error, 42);
	if (failure) { await release(); await flush(); expect(onError).toHaveBeenCalledTimes(3); }
	refresh.stop();
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
