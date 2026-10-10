import { afterEach, expect, it } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

const flush = () => new Promise(resolve => setImmediate(resolve));
afterEach(resetRuntimeEnv);
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
