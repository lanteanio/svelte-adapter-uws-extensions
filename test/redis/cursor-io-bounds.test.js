import { afterEach, expect, it, vi } from 'vitest';
import { createRedisIo } from '../../src/redis/cursor/redis-io.js';
import { keySlot } from '../../src/shared/cluster.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
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
