import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { execMultiSlot } from '../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const flush = () => new Promise(resolve => setImmediate(resolve));
let client, intervals, registries;
beforeEach(() => {
	intervals = [];
	registries = [];
	client = mockRedisClient('route-recovery:');
	setRuntimeEnv({ timers: {
		setInterval(cb) { const h = { cb, unref() {} }; intervals.push(h); return h; },
		clearInterval(h) { h.cb = () => {}; }
	} });
});
afterEach(async () => { for (const r of registries) await r.destroy(); resetRuntimeEnv(); });
function registry(options = {}) {
	const r = createConnectionRegistry(client, { identify: ws => ws.getUserData().userId, heartbeat: 100, ...options });
	registries.push(r);
	return r;
}
function socket(sessionId, attrs = {}) { return mockWs({ userId: 'user', [WS_SESSION_ID]: sessionId, attrs }); }
async function tick(index = 0) { intervals[index].cb(); await flush(); }
async function reach(r, platform, ws) {
	await r.send('user', 'updates', 'changed', { n: 1 });
	await r.request('user', 'read', { n: 2 });
	expect(platform.sent.at(-1)).toMatchObject({ ws, event: 'changed', data: { n: 1 } });
	expect(platform.requested.at(-1)).toMatchObject({ ws, event: 'read', data: { n: 2 } });
}

it('restores the surviving no-attributes owner and routes peer sends and requests after latest owner closes', async () => {
	const a = registry(), b = registry(), peer = registry();
	const wa = socket('a'), wb = socket('b'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await b.hooks.open(wb, { platform: mockPlatform() });
	await peer.hooks.open(mockWs({}), { platform: mockPlatform() });
	await b.hooks.close(wb);
	expect(await peer.lookup('user')).toBeNull();
	await tick();
	expect(await peer.lookup('user')).toMatchObject({ instanceId: a.instanceId, sessionId: 'a' });
	await reach(peer, pa, wa);
});

it('repairs a failed initial registration without requiring another open', async () => {
	const a = registry(), wa = socket('a'), pa = mockPlatform();
	const multi = client.redis.multi.bind(client.redis);
	client.redis.multi = () => { throw new Error('offline'); };
	await a.hooks.open(wa, { platform: pa });
	client.redis.multi = multi;
	await tick();
	await reach(a, pa, wa);
});

it('does not overwrite or extend the TTL of a newer remote owner during heartbeat', async () => {
	const a = registry(), b = registry(), wa = socket('a'), wb = socket('b');
	await a.hooks.open(wa, { platform: mockPlatform() });
	const pb = mockPlatform();
	await b.hooks.open(wb, { platform: pb });
	const expire = vi.spyOn(client.redis, 'expire');
	await tick();
	expect(expire).not.toHaveBeenCalled();
	expect(await a.lookup('user')).toMatchObject({ instanceId: b.instanceId, sessionId: 'b' });
	await reach(a, pb, wb);
});

it('restores the captured attributes and current local transport after reconnect', async () => {
	const a = registry({ attributes: ws => ws.getUserData().attrs });
	const first = socket('first', { role: 'old' }), latest = socket('latest', { role: 'new' }), pa = mockPlatform();
	await a.hooks.open(first, { platform: pa });
	await a.hooks.open(latest, { platform: pa });
	latest.getUserData().attrs.role = 'mutated';
	await client.redis.del(client.key('conns:user'));
	await a.hooks.close(first);
	await tick();
	expect(await client.redis.hgetall(client.key('conns:user'))).toMatchObject({ sessionId: 'latest', attrs: '{"role":"new"}' });
	await reach(a, pa, latest);
});

it('retries a failed restoration on the next heartbeat', async () => {
	const a = registry(), wa = socket('a'), pa = mockPlatform();
	await a.hooks.open(wa, { platform: pa });
	await client.redis.del(client.key('conns:user'));
	const pipeline = client.redis.pipeline.bind(client.redis);
	client.redis.pipeline = () => { throw new Error('offline'); };
	await tick();
	client.redis.pipeline = pipeline;
	await tick();
	await reach(a, pa, wa);
});

for (const action of ['close', 'purge', 'replace', 'destroy']) {
	it(`a heartbeat waiting for the shared Redis budget cannot restore a stale socket after ${action}`, async () => {
		const a = registry(), wa = socket('a'), pa = mockPlatform();
		await a.hooks.open(wa, { platform: pa });
		const pipeline = client.redis.pipeline.bind(client.redis);
		const releases = [];
		client.redis.pipeline = () => {
			const pipe = pipeline();
			const exec = pipe.exec.bind(pipe);
			return new Proxy(pipe, { get(target, key) {
				if (key === 'exec') return () => new Promise(resolve => releases.push(async () => resolve(await exec())));
				return target[key];
			} });
		};
		const busy = Array.from({ length: 8 }, (_, i) => execMultiSlot(client.redis, [['get', 'hold:' + i]]));
		await tick();
		expect(releases).toHaveLength(8);
		if (action === 'close') await a.hooks.close(wa);
		else if (action === 'purge') await a.purgeUser(null, 'user');
		else if (action === 'destroy') { await a.destroy(); await client.redis.del(client.key('conns:user')); }
		else { await a.hooks.open(socket('latest'), { platform: pa }); await client.redis.del(client.key('conns:user')); }
		client.redis.pipeline = pipeline;
		await Promise.all(releases.map(release => release()));
		await Promise.all(busy);
		await flush();
		expect(await a.lookup('user')).toBeNull();
		if (action === 'replace') {
			await tick();
			expect(await a.lookup('user')).toMatchObject({ sessionId: 'latest' });
		}
	});
}


it('cannot restore a replaced transport when the new connection attributes reject registration', async () => {
	const a = registry({ attributes: ws => { if (ws.getUserData().attrs.reject) throw new Error('invalid attributes'); return { role: 'reader' }; } });
	await a.hooks.open(socket('first'), { platform: mockPlatform() });
	await expect(a.hooks.open(socket('replacement', { reject: true }), { platform: mockPlatform() })).rejects.toThrow('invalid attributes');
	await client.redis.del(client.key('conns:user'));
	await tick();
	expect(await a.lookup('user')).toBeNull();
});
