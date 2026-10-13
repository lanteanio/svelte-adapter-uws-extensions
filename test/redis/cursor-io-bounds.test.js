import { afterEach, expect, it, vi } from 'vitest';
import { createRedisIo } from '../../src/redis/cursor/redis-io.js';
import { keySlot } from '../../src/shared/cluster.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const flush = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
let io;
afterEach(() => { io?.dispose(); releaseRuntimeClock(); vi.useRealTimers(); });
function setup(activeTopics, snapshotIntervalMs = 100, extra = {}) {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const client = mockRedisClient('test:');
	const waiting = [], batches = [];
	const pipeline = client.redis.pipeline.bind(client.redis);
	client.redis.pipeline = () => {
		const pipe = pipeline();
		const exec = pipe.exec.bind(pipe);
		return new Proxy(pipe, { get(target, key) {
			if (key === 'exec') return () => { batches.push(pipe); return new Promise(resolve => waiting.push(async () => resolve(await exec()))); };
			return target[key];
		} });
	};
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 1, snapshotIntervalMs, activeTopics, ...extra });
	return { client, waiting, batches };
}

it('bounds cleanup pipelines and coalesces overlapping ticks until a sweep settles', async () => {
	const { waiting, batches } = setup(new Set(Array.from({ length: 600 }, (_, i) => 'topic' + i)));
	io.startCleanupTimer();
	await vi.advanceTimersByTimeAsync(10_000);
	expect(batches).toHaveLength(3);
	await vi.advanceTimersByTimeAsync(60_000);
	expect(batches).toHaveLength(3);
	await Promise.all(waiting.splice(0).map(release => release())); await flush();
	expect(batches).toHaveLength(6);
	io.dispose();
	await Promise.all(waiting.splice(0).map(release => release())); await flush();
	await vi.advanceTimersByTimeAsync(60_000);
	expect(batches).toHaveLength(6);
});

it('keeps the latest pending snapshot while a previous write is stalled', async () => {
	const { client, waiting, batches } = setup(new Set(['topic']));
	io.startSnapshotTimer();
	io.queueSnapshot('topic', 'key', {}, { x: 1 });
	await vi.advanceTimersByTimeAsync(100);
	expect(batches).toHaveLength(1);
	for (let x = 2; x <= 10; x++) { io.queueSnapshot('topic', 'key', {}, { x }); await vi.advanceTimersByTimeAsync(100); }
	expect(batches).toHaveLength(1);
	await waiting.shift()(); await flush();
	expect(batches).toHaveLength(2);
	await waiting.shift()(); await flush();
	const saved = await client.redis.hgetall(io.hashKey('topic'));
	expect(JSON.parse(saved.key).data).toEqual({ x: 10 });
});

// A three-owner Cluster double: pipelines for the stalled owner never answer.
const ownerOfSlot = slot => slot < 5461 ? 'A' : slot < 10922 ? 'B' : 'C';
function stallOwner(redis, stalled) {
	redis.nodes = () => [redis];
	redis.slots = Array.from({ length: 16384 }, (_, slot) => [ownerOfSlot(slot)]);
	const execs = { A: 0, B: 0, C: 0 };
	const pipeline = redis.pipeline.bind(redis);
	redis.pipeline = () => {
		const pipe = pipeline();
		const keys = [];
		const proxy = new Proxy(pipe, { get(target, name) {
			if (name === 'exec') return () => {
				const owners = new Set(keys.map(key => ownerOfSlot(keySlot(key))));
				if (owners.size !== 1) return Promise.reject(new Error('pipeline spans owners'));
				const [owner] = owners;
				execs[owner]++;
				return owner === stalled ? new Promise(() => {}) : pipe.exec();
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
	return execs;
}
function topicsOn(owners) {
	return owners.map(owner => {
		for (let i = 0; ; i++) if (ownerOfSlot(keySlot(io.hashKey('t' + i))) === owner) return 't' + i;
	});
}

it('keeps writing snapshots on healthy owners while another owner stalls', async () => {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const client = mockRedisClient('test:');
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 30, snapshotIntervalMs: 100, activeTopics: new Set() });
	const [stalledTopic, healthyTopic] = topicsOn(['A', 'B']);
	const execs = stallOwner(client.redis, 'A');
	io.startSnapshotTimer();
	for (let x = 1; x <= 10; x++) {
		io.queueSnapshot(stalledTopic, 'k', {}, { x });
		io.queueSnapshot(healthyTopic, 'k', {}, { x });
		await vi.advanceTimersByTimeAsync(100);
		const saved = await client.redis.hgetall(io.hashKey(healthyTopic));
		expect(JSON.parse(saved.k).data).toEqual({ x });
	}
	expect(execs).toMatchObject({ A: 1, B: 10 });
	// The stalled owner keeps only its newest value, still readable locally.
	expect(io.getTopicPending(stalledTopic).get('k').data).toEqual({ x: 10 });
	expect(io.getTopicPending(healthyTopic)).toBeUndefined();
});

it('keeps sweeping topics on healthy owners while another owner stalls', async () => {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const client = mockRedisClient('test:');
	const activeTopics = new Set();
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 1, snapshotIntervalMs: 0, activeTopics });
	const [stalledTopic, healthyTopic] = topicsOn(['A', 'B']);
	activeTopics.add(stalledTopic).add(healthyTopic);
	const execs = stallOwner(client.redis, 'A');
	io.startCleanupTimer();
	for (let sweep = 1; sweep <= 5; sweep++) {
		await client.redis.hset(io.hashKey(healthyTopic), 'stale', JSON.stringify({ user: 'u', data: {}, ts: Date.now() - 5000 }));
		await vi.advanceTimersByTimeAsync(10_000);
		expect(await client.redis.hgetall(io.hashKey(healthyTopic))).toEqual({});
	}
	expect(execs).toMatchObject({ A: 1, B: 5 });
});

it('holds a snapshot that waited for its owner while the breaker is open', async () => {
	const breaker = { state: 'healthy', isHealthy: true, guard: vi.fn(), success: vi.fn(), failure: vi.fn() };
	const inputDropped = vi.fn();
	const { waiting, batches } = setup(new Set(['topic']), 100, { b: breaker, inputDropped });
	io.startSnapshotTimer();
	io.queueSnapshot('topic', 'key', {}, { x: 1 });
	await vi.advanceTimersByTimeAsync(100);
	io.queueSnapshot('topic', 'key', {}, { x: 2 });
	await vi.advanceTimersByTimeAsync(100);
	expect(batches).toHaveLength(1);
	Object.assign(breaker, { state: 'broken', isHealthy: false });
	breaker.guard.mockImplementation(() => { throw new Error('circuit broken'); });
	await waiting.shift()(); await flush();
	expect(batches).toHaveLength(1);
	expect(io.getTopicPending('topic').get('key').data).toEqual({ x: 2 });
	await vi.advanceTimersByTimeAsync(100);
	expect(io.getTopicPending('topic')).toBeUndefined();
	expect(inputDropped).toHaveBeenCalledWith('breaker_open', 'snapshot_write');
	expect(batches).toHaveLength(1);
});

// A Cluster double whose owners can be told to refuse every command or to
// hold their pipelines until released; a refusal applies at release time too.
function scriptOwners(redis) {
	redis.nodes = () => [redis];
	redis.slots = Array.from({ length: 16384 }, (_, slot) => [ownerOfSlot(slot)]);
	const refuse = new Set(), hold = new Set(), held = [];
	const pipeline = redis.pipeline.bind(redis);
	redis.pipeline = () => {
		const pipe = pipeline();
		const keys = [];
		const proxy = new Proxy(pipe, { get(target, name) {
			if (name === 'exec') return () => {
				const owner = redis.slots[keySlot(keys[0])][0];
				const answer = async () => refuse.has(owner) ? keys.map(() => [new Error('NOPERM refused'), null]) : pipe.exec();
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
	return { refuse, hold, held };
}

it('ends a cleanup failure streak only on a sweep that reached every owner', async () => {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const client = mockRedisClient('test:');
	const activeTopics = new Set();
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 1, snapshotIntervalMs: 0, activeTopics });
	const [topicA, topicB] = topicsOn(['A', 'B']);
	activeTopics.add(topicA).add(topicB);
	const owners = scriptOwners(client.redis);
	const sink = captureRegistered();
	const reports = () => sink.records.filter(record => record.event === 'redis.cursor.cleanup.failed');
	const sweep = () => vi.advanceTimersByTimeAsync(10_000);
	try {
		io.startCleanupTimer();
		owners.refuse.add('A');
		await sweep();
		expect(reports()).toHaveLength(1);
		owners.refuse.clear();
		owners.hold.add('B');
		await sweep();
		// Every answer succeeds, but owner B is still busy: the streak goes on.
		await sweep();
		owners.refuse.add('A');
		await sweep();
		expect(reports()).toHaveLength(1);
		// B's held sweep fails, and its waiting rerun finds nothing left to sweep.
		activeTopics.delete(topicB);
		owners.refuse.add('B');
		owners.hold.clear();
		await owners.held.shift()(); await flush();
		await sweep();
		expect(reports()).toHaveLength(1);
		// A sweep that reaches every owner cleanly ends it.
		owners.refuse.clear();
		await sweep();
		owners.refuse.add('A');
		await sweep();
		expect(reports()).toHaveLength(2);
	} finally { sink.stop(); }
});

it('writes a topic again only after its previous write answered, across an owner change', async () => {
	vi.useFakeTimers(); installFakeRuntimeClock();
	const client = mockRedisClient('test:');
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 30, snapshotIntervalMs: 100, activeTopics: new Set() });
	const [topic] = topicsOn(['A']);
	const owners = scriptOwners(client.redis);
	owners.hold.add('A');
	io.queueSnapshot(topic, 'k', {}, { x: 1 });
	io.flushSnapshot(); await flush();
	expect(owners.held).toHaveLength(1);
	// The slot moves to another node while the first write is on its way.
	const moved = ['D'];
	for (let slot = 0; slot < 16384; slot++) if (ownerOfSlot(slot) === 'A') client.redis.slots[slot] = moved;
	io.queueSnapshot(topic, 'k', {}, { x: 2 });
	io.flushSnapshot(); await flush();
	expect(io.getTopicPending(topic).get('k').data).toEqual({ x: 2 });
	await owners.held.shift()(); await flush();
	io.flushSnapshot(); await flush();
	expect(JSON.parse((await client.redis.hgetall(io.hashKey(topic))).k).data).toEqual({ x: 2 });
});

it('leaves a probing breaker its probe when a flush reaches no owner', async () => {
	const client = mockRedisClient('test:');
	const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
	io = createRedisIo({ client, redis: client.redis, b: breaker, cursorTtl: 30, snapshotIntervalMs: 100, activeTopics: new Set() });
	const [topic] = topicsOn(['A']);
	const owners = scriptOwners(client.redis);
	owners.hold.add('A');
	try {
		io.queueSnapshot(topic, 'k', {}, { x: 1 });
		io.flushSnapshot(); await flush();
		breaker.failure(new Error('unrelated failure'));
		await new Promise(resolve => setTimeout(resolve, 5));
		expect(breaker.state).toBe('probing');
		io.queueSnapshot(topic, 'k', {}, { x: 2 });
		io.flushSnapshot(); await flush();
		expect(owners.held).toHaveLength(1);
		expect(breaker.state).toBe('probing');
		expect(() => breaker.guard()).not.toThrow();
	} finally {
		for (const release of owners.held.splice(0)) await release();
		breaker.destroy();
	}
});
