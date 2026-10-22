import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

let client, registries, intervals;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-registry-routing:' }); });
beforeEach(async () => {
	await resetBackendKeys(client);
	registries = [];
	intervals = [];
	setRuntimeEnv({ timers: {
		setInterval(cb, delay) { const handle = { cb, unref() {} }; if (delay === 100) intervals.push(handle); return handle; },
		clearInterval(handle) { handle.cb = () => {}; }
	} });
});
afterEach(async () => { for (const r of registries) await r.destroy(); resetRuntimeEnv(); });
afterAll(async () => { await client.quit(); });
function registry(options = {}) {
	const r = createConnectionRegistry(client, { identify: ws => ws.getUserData().userId, heartbeat: 100, ...options });
	registries.push(r);
	return r;
}
function socket(sessionId) { return mockWs({ userId: 'user', [WS_SESSION_ID]: sessionId }); }
async function restored(r, sessionId) {
	await expect.poll(async () => (await r.lookup('user'))?.sessionId).toBe(sessionId);
}
async function reached(peer, platform, ws) {
	await peer.send('user', 'updates', 'changed', { n: 1 });
	await expect.poll(() => platform.sent.length).toBe(1);
	expect(platform.sent[0]).toMatchObject({ ws, event: 'changed', data: { n: 1 } });
	await peer.request('user', 'read', { n: 2 });
	expect(platform.requested[0]).toMatchObject({ ws, event: 'read', data: { n: 2 } });
}
it('restores no-attributes routing to the surviving owner after the latest owner closes', async () => {
	const a = registry(), b = registry(), peer = registry(), wa = socket('a'), wb = socket('b'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await b.hooks.open(wb, { platform: mockPlatform() });
	await peer.hooks.open(mockWs({}), { platform: mockPlatform() });
	await b.hooks.close(wb);
	expect(await peer.lookup('user')).toBeNull();
	intervals[0].cb();
	await restored(peer, 'a');
	expect(await client.redis.ttl(client.key('conns:user'))).toBeGreaterThan(0);
	await reached(peer, pa, wa);
});
it('lets a newer remote owner expire instead of renewing it, then recovers the surviving socket', async () => {
	const a = registry(), b = registry(), peer = registry(), wa = socket('a'), wb = socket('b'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await b.hooks.open(wb, { platform: mockPlatform() });
	await peer.hooks.open(mockWs({}), { platform: mockPlatform() });
	await client.redis.pexpire(client.key('conns:user'), 100);
	intervals[0].cb();
	await waitRedisMs(client, 200);
	expect(await peer.lookup('user')).toBeNull();
	intervals[0].cb();
	await restored(peer, 'a');
	await reached(peer, pa, wa);
});
it('preserves a foreign owner that arrives immediately before the repair reaches Redis', async () => {
	const a = registry(), b = registry(), wa = socket('a'), wb = socket('b'), pb = mockPlatform();
	await a.hooks.open(wa, { platform: mockPlatform() });
	await b.hooks.open(wb, { platform: pb });
	await client.redis.del(client.key('conns:user'));
	const pipeline = client.redis.pipeline.bind(client.redis);
	let complete;
	const completed = new Promise(resolve => { complete = resolve; });
	client.redis.pipeline = () => {
		const pipe = pipeline();
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return async () => {
				await client.redis.hset(client.key('conns:user'), 'instanceId', b.instanceId, 'sessionId', 'b', 'ts', 1);
				await client.redis.expire(client.key('conns:user'), 90);
				try { return await target.exec(); } finally { complete(); }
			};
			return target[key];
		} });
	};
	try { intervals[0].cb(); await completed; await restored(a, 'b'); }
	finally { client.redis.pipeline = pipeline; }
	await reached(a, pb, wb);
});
it('restores captured attributes from an absent row without replacing an existing newer transport', async () => {
	const a = registry({ attributes: () => ({ role: 'reader' }) }), wa = socket('a');
	await a.hooks.open(wa, { platform: mockPlatform() });
	await client.redis.del(client.key('conns:user'));
	intervals[0].cb();
	await restored(a, 'a');
	expect(await client.redis.hget(client.key('conns:user'), 'attrs')).toBe('{"role":"reader"}');
	await client.redis.hset(client.key('conns:user'), 'sessionId', 'newer');
	await client.redis.pexpire(client.key('conns:user'), 100);
	intervals[0].cb();
	await waitRedisMs(client, 200);
	expect(await a.lookup('user')).toBeNull();
});

it('reports a refused repair command and retries after the backend row becomes usable', async () => {
	const errors = [];
	const breaker = { state: 'healthy', isHealthy: true, guard() {}, success() {}, failure(error) { errors.push(error); }, subscribe() { return () => {}; } };
	const a = registry({ breaker }), peer = registry(), wa = socket('a'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await peer.hooks.open(mockWs({}), { platform: mockPlatform() });
	await client.redis.del(client.key('conns:user'));
	await client.redis.set(client.key('conns:user'), 'wrong type');
	intervals[0].cb();
	await expect.poll(() => errors.length).toBe(1);
	expect(errors[0].message).toContain('WRONGTYPE');
	await client.redis.del(client.key('conns:user'));
	intervals[0].cb();
	await restored(peer, 'a');
	await reached(peer, pa, wa);
});

it('announces a row restored after a peer close from the script, so peers route sendTo to the surviving socket', async () => {
	const attributes = () => ({ role: 'reader' });
	const a = registry({ attributes }), b = registry({ attributes }), peer = registry({ attributes });
	const wa = socket('a'), wb = socket('b'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await b.hooks.open(wb, { platform: mockPlatform() });
	await peer.hooks.open(mockWs({}), { platform: mockPlatform() });
	const sub = client.duplicate();
	const messages = [];
	try {
		await sub.subscribe(client.key('__registry-events'));
		sub.on('message', (_channel, message) => messages.push(message));
		await b.hooks.close(wb);
		await restored(peer, 'a');
		await expect.poll(() => messages.some(m => JSON.parse(m).type === 'open')).toBe(true);
		expect(messages.filter(m => JSON.parse(m).type === 'open')).toEqual([
			JSON.stringify({ type: 'open', userId: 'user', instanceId: a.instanceId, attrs: { role: 'reader' } })
		]);
		await expect.poll(async () => {
			await peer.sendTo({ role: 'reader' }, 'room', 'ping', 1);
			return pa.sent.some(f => f.ws === wa && f.event === 'ping');
		}).toBe(true);
	} finally {
		await sub.quit();
	}
});
