import { it, expect, afterEach } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

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
