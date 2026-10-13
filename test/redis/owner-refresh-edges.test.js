import { afterEach, expect, it } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createRedisIo } from '../../src/redis/cursor/redis-io.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { keySlot, coalescedByOwner } from '../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const flush = async () => { for (let i = 0; i < 6; i++) await new Promise(resolve => setImmediate(resolve)); };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const ownerOfSlot = slot => slot < 5461 ? 'A' : slot < 10922 ? 'B' : 'C';
afterEach(resetRuntimeEnv);

// A three-owner Cluster double over the in-memory mock. Pipelines to an owner
// in `refuse` answer every command with an error, owners in `hold` park until
// released, and an owner in `nullOnce` resolves its next exec() with null,
// which fails the whole call.
function owners(redis) {
	redis.nodes = () => [redis];
	redis.slots = Array.from({ length: 16384 }, (_, slot) => [ownerOfSlot(slot)]);
	const hold = new Set(), nullOnce = new Set(), refuse = new Set(), held = [], execs = { A: 0, B: 0, C: 0 };
	const pipeline = redis.pipeline.bind(redis);
	redis.pipeline = () => {
		const pipe = pipeline();
		const keys = [];
		const proxy = new Proxy(pipe, { get(target, name) {
			if (name === 'exec') return () => {
				const owner = redis.slots[keySlot(keys[0])][0];
				execs[owner]++;
				if (nullOnce.has(owner)) { nullOnce.delete(owner); return Promise.resolve(null); }
				const answer = async () => refuse.has(owner) ? keys.map(() => [new Error('READONLY refused'), null]) : pipe.exec();
				if (hold.has(owner)) return new Promise(resolve => held.push(async () => resolve(await answer())));
				return answer();
			};
			if (typeof target[name] === 'function') return (...args) => {
				keys.push(typeof args[0] === 'number' ? args[1] : args[0]);
				target[name](...args);
				return proxy;
			};
			return target[name];
		} });
		return proxy;
	};
	return { hold, nullOnce, refuse, held, execs };
}

it('probes a presence breaker opened by heartbeat failures closed again from the heartbeat', async () => {
	const intervals = new Set();
	setRuntimeEnv({ timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.add(timer); return timer; },
		clearInterval(timer) { intervals.delete(timer); }
	} });
	const client = mockRedisClient('test:');
	const breaker = createCircuitBreaker({ failureThreshold: 5, resetTimeout: 20 });
	const presence = createPresence(client, { key: 'id', heartbeat: 17, ttl: 90, breaker });
	const topicOn = owner => { for (let i = 0; ; i++) if (ownerOfSlot(keySlot(client.key('presence:topic:{t' + i + '}'))) === owner) return 't' + i; };
	const platform = mockPlatform();
	await presence.join(mockWs({ id: 'ua' }), topicOn('A'), platform);
	await presence.join(mockWs({ id: 'ub' }), topicOn('B'), platform);
	await flush();
	const o = owners(client.redis);
	o.refuse.add('A');
	const tick = [...intervals].find(timer => timer.delay === 17).fn;
	try {
		// Owner A refuses until the breaker opens, then recovers while nothing
		// else on this instance touches Redis.
		for (let i = 0; i < 6; i++) { tick(); await flush(); }
		expect(breaker.state).not.toBe('healthy');
		o.refuse.clear();
		await sleep(40);
		const before = o.execs.B;
		for (let i = 0; i < 3; i++) { tick(); await flush(); }
		expect(breaker.state).toBe('healthy');
		expect(o.execs.B).toBeGreaterThan(before);
	} finally { await presence.destroy(); breaker.destroy(); }
});

it('writes a cursor topic again after its previous snapshot call failed as a whole', async () => {
	const client = mockRedisClient('test:');
	const io = createRedisIo({ client, redis: client.redis, cursorTtl: 30, snapshotIntervalMs: 100, activeTopics: new Set() });
	let topic;
	for (let i = 0; !topic; i++) if (ownerOfSlot(keySlot(io.hashKey('t' + i))) === 'A') topic = 't' + i;
	const o = owners(client.redis);
	try {
		o.nullOnce.add('A');
		io.queueSnapshot(topic, 'k', {}, { x: 1 });
		io.flushSnapshot(); await flush();
		expect(o.execs.A).toBe(1);
		io.queueSnapshot(topic, 'k', {}, { x: 2 });
		io.flushSnapshot(); await flush();
		expect(o.execs.A).toBe(2);
		expect(JSON.parse((await client.redis.hgetall(io.hashKey(topic))).k).data).toEqual({ x: 2 });
	} finally { io.dispose(); }
});

it('reads admit when waiting work starts, and drops the work when admit throws', async () => {
	const client = mockRedisClient();
	const o = owners(client.redis);
	o.hold.add('A');
	let ok = true;
	const done = [];
	let keyA;
	for (let i = 0; !keyA; i++) if (ownerOfSlot(keySlot('k' + i)) === 'A') keyA = 'k' + i;
	const submit = coalescedByOwner(client.redis, {
		key: k => k,
		commands: (k, out) => out.push(['expire', k, 30]),
		admit: () => { if (ok === 'throw') throw new Error('admission failed'); return ok; },
		onComplete: cycle => done.push(cycle.context)
	});
	try {
		submit([keyA], 1); await flush();
		submit([keyA], 2); await flush();
		ok = 'throw';
		await o.held.shift()(); await flush();
		expect(o.execs.A).toBe(1);
		expect(done).toEqual([2, 1]);
		expect(submit.busySince()).toBe(Infinity);
		submit([keyA], 3); await flush();
		expect(o.execs.A).toBe(2);
	} finally {
		submit.stop();
		for (const release of o.held.splice(0)) await release();
	}
});
