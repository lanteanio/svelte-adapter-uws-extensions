import { describe, it, expect, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createCursor } from '../../redis/cursor.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const opts = { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id }) };

describe('cursor snapshot during a Redis outage', () => {
	const rejections = [];
	const onRejection = (err) => rejections.push(err);

	afterEach(() => {
		process.off('unhandledRejection', onRejection);
		rejections.length = 0;
	});

	it('does not turn a snapshot frame into an unhandled rejection', async () => {
		process.on('unhandledRejection', onRejection);

		const client = mockRedisClient('out1:');
		const cursors = createCursor(client, opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);

		// Redis goes away. `list()` rethrows, and hooks.message dispatches the
		// snapshot fire-and-forget - so without an owner for the rejection the
		// worker exits. The shipped client sends this frame on every reconnect,
		// which makes the crash self-amplifying: the reconnect storm re-fires
		// it on each replacement worker while Redis is still down.
		client.redis.hgetall = async () => { throw new Error('redis down'); };

		cursors.hooks.message(ws, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(30);

		expect(rejections).toHaveLength(0);
		cursors.destroy();
	});

	it('does not turn a snapshot frame into an unhandled rejection while the breaker is open', async () => {
		process.on('unhandledRejection', onRejection);

		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const cursors = createCursor(mockRedisClient('out2:'), { ...opts, breaker });
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);

		// An open circuit makes `list()` throw from `guard()` before any Redis call.
		breaker.failure();
		expect(breaker.state).toBe('broken');

		cursors.hooks.message(ws, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(30);

		expect(rejections).toHaveLength(0);
		cursors.destroy();
		breaker.destroy();
	});

	it('still serves a snapshot once Redis recovers', async () => {
		const client = mockRedisClient('out3:');
		const cursors = createCursor(client, opts);
		const platform = mockPlatform();
		const ws = mockWs({ id: 'alice' });
		await cursors.attach(ws, 'board', platform);
		cursors.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 1 } }, platform });
		await sleep(20);

		platform.reset();
		cursors.hooks.message(ws, { data: { type: 'cursor-snapshot', topic: 'board' }, platform });
		await sleep(20);
		// Swallowing the outage must not have swallowed the healthy path.
		expect(platform.sent.some((s) => s.topic === '__cursor:board')).toBe(true);
		cursors.destroy();
	});
});
