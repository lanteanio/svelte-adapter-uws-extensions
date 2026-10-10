import { it, expect, afterEach, vi } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const schedulerStates = vi.hoisted(() => []);
vi.mock('../../src/redis/cursor/scheduler.js', async (importOriginal) => {
	const original = await importOriginal();
	return { ...original, createScheduler(deps) {
		schedulerStates.push(deps);
		return original.createScheduler(deps);
	} };
});
let cursor;
afterEach(() => cursor?.destroy());
it('keeps watcher-only flush state bounded by locally watched topics', async () => {
	const client = mockRedisClient();
	const platform = mockPlatform();
	const ws = mockWs({ id: 'watcher' });
	cursor = createCursor(client);
	await cursor.attach(ws, 'board', platform);
	for (let i = 0; i < 500; i++) {
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({
			instanceId: 'peer', topic: 'other-' + i, event: 'update', payload: { key: 'peer:1', data: { x: i } }
		}));
	}
	expect(cursor.stats().dirtyTopicsCurrent).toBe(0);
	await client.redis.publish(client.key('cursor:events'), JSON.stringify({
		instanceId: 'peer', topic: 'board', event: 'update', payload: { key: 'peer:1', data: { x: 7 } }
	}));
	expect(cursor.stats().dirtyTopicsCurrent).toBe(1);
	cursor.detach(ws, 'board', platform);
	expect(cursor.stats().dirtyTopicsCurrent).toBe(0);
	await client.redis.publish(client.key('cursor:events'), JSON.stringify({
		instanceId: 'peer', topic: 'board', event: 'update', payload: { key: 'peer:1', data: { x: 8 } }
	}));
	expect(cursor.stats().dirtyTopicsCurrent).toBe(0);
});


it('does not retain observer interest after the last membership is detached and the socket closes', async () => {
	const client = mockRedisClient(), platform = mockPlatform();
	cursor = createCursor(client);
	const keepAlive = mockWs({ id: 'keeper' });
	await cursor.attach(keepAlive, 'keeper', platform);
	const state = schedulerStates.at(-1);
	for (let i = 0; i < 300; i++) {
		const ws = mockWs({ id: 'observer-' + i });
		await cursor.attach(ws, 'board', platform);
		await cursor.snapshot(ws, 'other-' + i, platform);
		cursor.detach(ws, 'board', platform);
		await cursor.hooks.close(ws, { platform });
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({
			instanceId: 'peer', topic: 'other-' + i, event: 'update', payload: { key: 'peer:1', data: { x: i } }
		}));
	}
	expect(cursor.stats().dirtyTopicsCurrent).toBe(0);
	expect(state.topicFlush.size).toBe(0);
	expect(state.wsState.size).toBe(1);
});

it('does not retain interest when snapshot user projection fails before state allocation', async () => {
	const client = mockRedisClient(), platform = mockPlatform();
	cursor = createCursor(client, { select: user => user.id === 'bad' ? 1n : user });
	await cursor.attach(mockWs({ id: 'keeper' }), 'keeper', platform);
	const ws = mockWs({ id: 'bad' });
	await expect(cursor.snapshot(ws, 'other', platform)).rejects.toThrow('JSON-serializable');
	await cursor.hooks.close(ws, { platform });
	await client.redis.publish(client.key('cursor:events'), JSON.stringify({
		instanceId: 'peer', topic: 'other', event: 'update', payload: { key: 'peer:1', data: { x: 1 } }
	}));
	expect(schedulerStates.at(-1).topicFlush.size).toBe(0);
});
