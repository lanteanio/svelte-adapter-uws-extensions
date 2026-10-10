import { describe, it, expect } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

describe('topic-scoped presence and cursor erasure', () => {
	it.each(['a', 'a_b', null])('erases only the requested tenant %s through the composite store', async (tenantId) => {
		const client = mockRedisClient('tenant-erasure:');
		const platform = mockPlatform();
		const presence = createPresence(client, { key: 'id', select: (u) => ({ id: u.id }), heartbeat: 60000, ttl: 180 });
		const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id }) });
		const store = createForgetStore({ presence, cursor });
		const topics = ['@t/a/room', '@t/a/other', '@t/ab/room', '@t/a_b/room', '@t/axb/room', '@t/b/room', 'room'];
		try {
			for (const topic of topics) {
				for (const id of ['shared-user', 'survivor']) {
					const ws = mockWs({ id });
					await presence.join(ws, topic, platform);
					cursor.update(ws, topic, { x: 1 }, platform);
				}
			}
			await new Promise((resolve) => setTimeout(resolve, 0));
			const expectedTopics = tenantId === null ? ['room'] : topics.filter((topic) => topic.startsWith('@t/' + tenantId + '/'));
			expect(await store.purgeUser(tenantId, 'shared-user')).toEqual({ presence: expectedTopics.length, cursor: expectedTopics.length });
			for (const topic of topics) {
				const expected = expectedTopics.includes(topic) ? ['survivor'] : ['shared-user', 'survivor'];
				expect((await presence.list(topic)).map((entry) => entry.id).sort(), topic).toEqual(expected);
				expect((await cursor.list(topic)).map((entry) => entry.user.id).sort(), topic).toEqual(expected);
			}
			// A second erasure neither widens the scope nor counts already-erased entries.
			expect(await store.purgeUser(tenantId, 'shared-user')).toEqual({ presence: 0, cursor: 0 });
		} finally {
			presence.destroy();
			cursor.destroy();
		}
	});
});
