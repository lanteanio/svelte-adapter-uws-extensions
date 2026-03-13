import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../redis/presence.js';

describe('redis presence', () => {
	let client;
	let platform;
	let presence;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		presence = createPresence(client, {
			key: 'id',
			select: (userData) => ({ id: userData.id, name: userData.name }),
			heartbeat: 60000,
			ttl: 180
		});
	});

	afterEach(() => {
		presence.destroy();
	});

	describe('createPresence', () => {
		it('returns a tracker with the expected API', () => {
			expect(typeof presence.join).toBe('function');
			expect(typeof presence.leave).toBe('function');
			expect(typeof presence.sync).toBe('function');
			expect(typeof presence.list).toBe('function');
			expect(typeof presence.count).toBe('function');
			expect(typeof presence.clear).toBe('function');
			expect(typeof presence.destroy).toBe('function');
		});
	});

	describe('join', () => {
		it('adds user to presence and sends list to joining client', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			// Should send full list to the joining client
			const sendEvents = platform.sent.filter((s) => s.event === 'list');
			expect(sendEvents).toHaveLength(1);
			expect(sendEvents[0].topic).toBe('__presence:room');
			expect(sendEvents[0].data).toEqual([
				{ key: '1', data: { id: '1', name: 'Alice' } }
			]);
		});

		it('subscribes ws to the internal presence topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			expect(ws.isSubscribed('__presence:room')).toBe(true);
		});

		it('broadcasts join event for new users', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			await presence.join(ws1, 'room', platform);
			platform.reset();

			await presence.join(ws2, 'room', platform);

			const joins = platform.published.filter((p) => p.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data).toEqual({ key: '2', data: { id: '2', name: 'Bob' } });
		});

		it('is idempotent', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const publishCount = platform.published.length;
			const sentCount = platform.sent.length;

			await presence.join(ws, 'room', platform);

			expect(platform.published.length).toBe(publishCount);
			expect(platform.sent.length).toBe(sentCount);
		});

		it('ignores __-prefixed topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, '__presence:room', platform);

			expect(platform.published).toHaveLength(0);
			expect(platform.sent).toHaveLength(0);
		});

		it('tracks multiple topics independently', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			expect(await presence.count('room-a')).toBe(1);
			expect(await presence.count('room-b')).toBe(1);
		});

		it('uses select function to filter userData', async () => {
			const p2 = createPresence(client, {
				key: 'id',
				select: (userData) => ({ id: userData.id })
			});
			const ws = mockWs({ id: '1', name: 'Alice', secret: 'token123' });
			await p2.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data).toEqual({ id: '1' });
			expect(listData[0].data.secret).toBeUndefined();
			p2.destroy();
		});
	});

	describe('multi-tab dedup', () => {
		it('same key, two connections = one presence entry', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '1', name: 'Alice' });

			await presence.join(ws1, 'room', platform);
			const joinsBefore = platform.published.filter((p) => p.event === 'join').length;

			await presence.join(ws2, 'room', platform);

			const joinsAfter = platform.published.filter((p) => p.event === 'join').length;
			// Should NOT publish a second join
			expect(joinsAfter).toBe(joinsBefore);
			expect(await presence.count('room')).toBe(1);
		});

		it('closing one tab keeps user present', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '1', name: 'Alice' });

			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);
			platform.reset();

			await presence.leave(ws1, platform);

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('closing last tab publishes leave and removes from Redis', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '1', name: 'Alice' });

			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);
			platform.reset();

			await presence.leave(ws1, platform);
			await presence.leave(ws2, platform);

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(1);
			expect(await presence.count('room')).toBe(0);
		});
	});

	describe('leave', () => {
		it('removes user from all topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			await presence.leave(ws, platform);

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(0);
		});

		it('broadcasts leave for each topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			platform.reset();

			await presence.leave(ws, platform);

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(2);
			expect(leaves.map((l) => l.topic).sort()).toEqual([
				'__presence:room-a',
				'__presence:room-b'
			]);
		});

		it('is safe to call for unknown ws', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.leave(ws, platform);
			expect(platform.published).toHaveLength(0);
		});

		it('cleans up empty topic state', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			await presence.leave(ws, platform);

			expect(await presence.list('room')).toEqual([]);
		});
	});

	describe('sync', () => {
		it('sends current list without joining', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });

			await presence.join(ws1, 'room', platform);
			platform.reset();

			await presence.sync(wsObserver, 'room', platform);

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);
			expect(lists[0].data).toEqual([
				{ key: '1', data: { id: '1', name: 'Alice' } }
			]);

			expect(wsObserver.isSubscribed('__presence:room')).toBe(true);
			// Observer should NOT be in the count
			expect(await presence.count('room')).toBe(1);
		});

		it('sends empty list for unknown topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.sync(ws, 'nonexistent', platform);

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);
			expect(lists[0].data).toEqual([]);
		});
	});

	describe('list / count', () => {
		it('returns current users', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);

			const list = await presence.list('room');
			expect(list).toHaveLength(2);
			expect(list).toContainEqual({ id: '1', name: 'Alice' });
			expect(list).toContainEqual({ id: '2', name: 'Bob' });
			expect(await presence.count('room')).toBe(2);
		});

		it('returns empty for unknown topics', async () => {
			expect(await presence.list('nonexistent')).toEqual([]);
			expect(await presence.count('nonexistent')).toBe(0);
		});
	});

	describe('clear', () => {
		it('resets all state', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			await presence.clear();

			expect(await presence.count('room')).toBe(0);
			expect(await presence.list('room')).toEqual([]);
		});
	});

	describe('no key field in data', () => {
		it('generates unique ID per connection', async () => {
			const p2 = createPresence(client, {
				select: (userData) => ({ name: userData.name })
			});
			const ws1 = mockWs({ name: 'Alice' });
			const ws2 = mockWs({ name: 'Bob' });

			await p2.join(ws1, 'room', platform);
			await p2.join(ws2, 'room', platform);

			expect(await p2.count('room')).toBe(2);
			p2.destroy();
		});
	});
});
