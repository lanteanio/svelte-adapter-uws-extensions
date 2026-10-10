import { afterEach, expect, it, vi } from 'vitest';
import { createRedisIo } from '../../src/redis/cursor/redis-io.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const flush = async () => { for (let i = 0; i < 30; i++) await Promise.resolve(); };
let io;
afterEach(() => { io?.dispose(); releaseRuntimeClock(); vi.useRealTimers(); });
function setup(activeTopics, snapshotIntervalMs = 100) {
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
	io = createRedisIo({ client, redis: client.redis, cursorTtl: 1, snapshotIntervalMs, activeTopics });
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