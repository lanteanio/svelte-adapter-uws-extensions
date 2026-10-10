import { afterEach, beforeEach, expect, it } from 'vitest';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const owner = '0123456789abcdef';
const email = 'private@example.test';
const flush = () => new Promise(resolve => setImmediate(resolve));
let now, intervals, client, registry, platform, publishes;
beforeEach(async () => {
	now = 0;
	intervals = new Set();
	setRuntimeEnv({ clock: { monotonic: () => now }, timers: {
		setInterval(cb) { const handle = { cb, unref() {} }; intervals.add(handle); return handle; },
		clearInterval(handle) { intervals.delete(handle); }
	} });
	client = mockRedisClient('expiry:');
	platform = mockPlatform();
	registry = createConnectionRegistry(client, {
		identify: ws => ws.getUserData().userId,
		attributes: ws => ws.getUserData().attrs,
		ttl: 1, heartbeat: 100
	});
	await registry.hooks.open(mockWs({}), { platform });
	await flush();
	publishes = [];
	const publish = client.redis.publish.bind(client.redis);
	client.redis.publish = async (channel, raw) => {
		if (channel.includes('__push:')) publishes.push({ channel, envelope: JSON.parse(raw) });
		return publish(channel, raw);
	};
});
afterEach(async () => { await registry.destroy(); resetRuntimeEnv(); });
async function openRemote(userId = 'remote', attrs = { email }, instanceId = owner) {
	await client.redis.hset(client.key('conns:' + userId), 'instanceId', instanceId, 'attrs', JSON.stringify(attrs));
	await client.redis.publish(client.key('__registry-events'), JSON.stringify({ type: 'open', userId, instanceId, attrs }));
}
async function tick() { for (const h of intervals) h.cb(); await flush(); }
async function send(criteria = { email }) { await registry.sendTo(criteria, 'updates', 'changed', {}); }

it('a confirmed absent lookup removes the sibling attributes and routing index immediately', async () => {
	await openRemote();
	await send();
	expect(publishes).toHaveLength(1);
	await client.redis.del(client.key('conns:remote'));
	expect(await registry.lookup('remote')).toBeNull();
	await send();
	expect(publishes).toHaveLength(1);
});
it('expires dead owners in a sibling with no local users and without a lookup', async () => {
	await openRemote();
	await client.redis.del(client.key('conns:remote'));
	now = 1001;
	await tick();
	await send();
	expect(publishes).toEqual([]);
});
it('checks expired matches before sendTo even when the heartbeat has not fired', async () => {
	await openRemote();
	await client.redis.del(client.key('conns:remote'));
	now = 1001;
	await send();
	expect(publishes).toEqual([]);
});
it('retains a living peer and reconciles migrated attributes from authoritative rows', async () => {
	await openRemote();
	now = 1001;
	await send();
	expect(publishes).toHaveLength(1);
	await client.redis.hset(client.key('conns:remote'), 'instanceId', 'fedcba9876543210', 'attrs', JSON.stringify({ role: 'editor' }));
	now = 2002;
	await send();
	expect(publishes).toHaveLength(1);
	await send({ role: 'editor' });
	expect(publishes[1].channel).toContain('fedcba9876543210');
});
it.each(['close', 'forget', 'open', 'destroy'])('a pending reconciliation cannot overwrite a later %s', async action => {
	await openRemote();
	const pipeline = client.redis.pipeline.bind(client.redis);
	let finish;
	client.redis.pipeline = () => {
		const pipe = pipeline();
		const exec = pipe.exec.bind(pipe);
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return async () => { const result = await exec(); return new Promise(resolve => { finish = () => resolve(result); }); };
			return target[key];
		} });
	};
	now = 1001;
	const sending = send();
	await flush();
	if (action === 'destroy') await registry.destroy();
	else if (action === 'open') await openRemote('remote', { role: 'new' }, 'fedcba9876543210');
	else await client.redis.publish(client.key('__registry-events'), JSON.stringify({ type: action, userId: 'remote', instanceId: owner }));
	client.redis.pipeline = pipeline;
	finish();
	await sending;
	expect(publishes).toEqual([]);
	if (action === 'open') { await send({ role: 'new' }); expect(publishes).toHaveLength(1); }
});
it('drops expired attributes on read failure and recovers living peers by a later bootstrap', async () => {
	await openRemote();
	const pipeline = client.redis.pipeline.bind(client.redis);
	client.redis.pipeline = () => { const pipe = pipeline(); return new Proxy(pipe, { get(target, key) {
		if (key === 'exec') return async () => { throw new Error('offline'); }; return target[key];
	} }); };
	now = 1001;
	await send();
	expect(publishes).toEqual([]);
	client.redis.pipeline = pipeline;
	await tick();
	await send();
	expect(publishes).toHaveLength(1);
});
it('never expires locally connected users when Redis is absent', async () => {
	const ws = mockWs({ userId: 'local', sessionId: 'session', attrs: { email } });
	await registry.hooks.open(ws, { platform });
	await client.redis.del(client.key('conns:local'));
	now = 1001;
	await tick();
	await send();
	expect(platform.sent).toHaveLength(1);
});

it('bounds refresh batches and shares one refresh across concurrent sends', async () => {
	for (let i = 0; i < 205; i++) await openRemote('remote-' + i);
	const pipeline = client.redis.pipeline.bind(client.redis);
	const sizes = [];
	client.redis.pipeline = () => {
		const pipe = pipeline();
		let size = 0;
		return new Proxy(pipe, { get(target, key) {
			if (key === 'hgetall') return (...args) => { size++; return target.hgetall(...args); };
			if (key === 'exec') return async () => { sizes.push(size); return target.exec(); };
			return target[key];
		} });
	};
	now = 1001;
	await Promise.all([send(), send()]);
	expect(sizes).toEqual([100, 100, 5]);
	expect(publishes).toHaveLength(2);
});
it('a late bootstrap cannot resurrect an erased remote attribute', async () => {
	await registry.destroy();
	await client.redis.hset(client.key('conns:remote'), 'instanceId', owner, 'attrs', JSON.stringify({ email }));
	const pipeline = client.redis.pipeline.bind(client.redis);
	let finish;
	client.redis.pipeline = () => {
		const pipe = pipeline();
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return async () => { const rows = await target.exec(); return new Promise(resolve => { finish = () => resolve(rows); }); };
			return target[key];
		} });
	};
	registry = createConnectionRegistry(client, { identify: () => null, attributes: () => ({}), ttl: 1 });
	await registry.hooks.open(mockWs({}), { platform });
	await flush();
	await client.redis.publish(client.key('__registry-events'), JSON.stringify({ type: 'forget', userId: 'remote', instanceId: owner }));
	client.redis.pipeline = pipeline;
	finish();
	await flush();
	await send();
	expect(publishes).toEqual([]);
});

it('expires a cached row naming this instance when no local socket owns it', async () => {
	await openRemote('orphan', { email }, registry.instanceId);
	now = 1001;
	const hgetall = client.redis.hgetall.bind(client.redis);
	const reads = [];
	client.redis.hgetall = async key => { reads.push(key); return hgetall(key); };
	await client.redis.del(client.key('conns:orphan'));
	await tick();
	expect(reads).toContain(client.key('conns:orphan'));
	reads.length = 0;
	now = 2002;
	await tick();
	expect(reads).toEqual([]);
});
