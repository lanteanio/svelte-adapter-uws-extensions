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

	describe('leave (per-topic)', () => {
		it('removes user from only the specified topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			await presence.leave(ws, platform, 'room-a');

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(1);
		});

		it('broadcasts leave only for the specified topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			platform.reset();

			await presence.leave(ws, platform, 'room-a');

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(1);
			expect(leaves[0].topic).toBe('__presence:room-a');
		});

		it('unsubscribes ws from the specified topic only', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			await presence.leave(ws, platform, 'room-a');

			expect(ws.isSubscribed('__presence:room-a')).toBe(false);
			expect(ws.isSubscribed('__presence:room-b')).toBe(true);
		});

		it('is safe to call for a topic the ws never joined', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			platform.reset();

			await presence.leave(ws, platform, 'nonexistent');
			expect(platform.published).toHaveLength(0);
			expect(await presence.count('room-a')).toBe(1);
		});

		it('allows subsequent leave-all after per-topic leave', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			await presence.leave(ws, platform, 'room-a');
			await presence.leave(ws, platform);

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(0);
		});

		it('respects multi-tab dedup for per-topic leave', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '1', name: 'Alice' });

			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);
			platform.reset();

			await presence.leave(ws1, platform, 'room');

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('leaves sync-only observer from specific topic', async () => {
			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });
			const ws1 = mockWs({ id: '1', name: 'Alice' });

			await presence.join(ws1, 'room-a', platform);
			await presence.sync(wsObserver, 'room-a', platform);
			await presence.sync(wsObserver, 'room-b', platform);

			await presence.leave(wsObserver, platform, 'room-a');

			expect(wsObserver.isSubscribed('__presence:room-a')).toBe(false);
			expect(wsObserver.isSubscribed('__presence:room-b')).toBe(true);
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

		it('subscribes to Redis channel so remote events are received with relay: false', async () => {
			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });
			await presence.sync(wsObserver, 'room', platform);

			platform.reset();

			// Simulate a remote join event via Redis pub/sub
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob', name: 'Bob' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			// The observer should have received the remote join
			const joins = platform.published.filter((p) => p.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data.key).toBe('bob');
			expect(joins[0].options).toEqual({ relay: false });
		});
	});

	describe('sync observer cleanup', () => {
		it('leave() cleans up Redis subscriptions for sync-only observers', async () => {
			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });
			await presence.sync(wsObserver, 'room', platform);

			// Observer is subscribed to the Redis channel
			// Now leave() should clean it up
			await presence.leave(wsObserver, platform);

			platform.reset();

			// Simulate a remote event -- should NOT be received since we unsubscribed
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'charlie', data: { id: 'charlie', name: 'Charlie' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			// No events should have been forwarded
			expect(platform.published).toHaveLength(0);
		});

		it('leave() does not unsubscribe channel if joined users remain', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });

			await presence.join(ws1, 'room', platform);
			await presence.sync(wsObserver, 'room', platform);

			// Observer leaves, but Alice is still joined
			await presence.leave(wsObserver, platform);
			platform.reset();

			// Remote events should still arrive because Alice's join keeps the channel alive
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob', name: 'Bob' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);
			expect(platform.published.filter((p) => p.event === 'join')).toHaveLength(1);
		});
	});

	describe('platform update', () => {
		it('uses the latest platform for remote event forwarding', async () => {
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			await presence.join(ws1, 'room', platform1);
			platform1.reset();

			// Second join with a different platform -- should update the subscriber
			await presence.join(ws2, 'room', platform2);
			platform2.reset();

			// Simulate a remote event
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'charlie', data: { id: 'charlie', name: 'Charlie' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			// Should have been forwarded via platform2 (latest), not platform1
			expect(platform2.published.filter((p) => p.event === 'join')).toHaveLength(1);
			expect(platform1.published.filter((p) => p.event === 'join')).toHaveLength(0);
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

		it('unsubscribes ws from presence topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			expect(ws.isSubscribed('__presence:room')).toBe(true);

			await presence.clear();

			expect(ws.isSubscribed('__presence:room')).toBe(false);
		});

		it('unsubscribes sync-only observers from presence topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.sync(ws, 'room', platform);
			expect(ws.isSubscribed('__presence:room')).toBe(true);

			await presence.clear();

			expect(ws.isSubscribed('__presence:room')).toBe(false);
		});

		it('stops forwarding remote events after clear', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			platform.reset();

			await presence.clear();
			platform.reset();

			// Simulate a remote join -- should NOT be forwarded
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob', name: 'Bob' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			expect(platform.published).toHaveLength(0);
		});

		it('allows re-joining after clear', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws1, 'room', platform);

			await presence.clear();

			const ws2 = mockWs({ id: '2', name: 'Bob' });
			await presence.join(ws2, 'room', platform);

			expect(await presence.count('room')).toBe(1);
			expect(ws2.isSubscribed('__presence:room')).toBe(true);
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

	describe('hooks', () => {
		it('API shape includes hooks with subscribe and close', () => {
			expect(typeof presence.hooks.subscribe).toBe('function');
			expect(typeof presence.hooks.close).toBe('function');
		});

		it('hooks.subscribe calls join for regular topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.hooks.subscribe(ws, 'room', { platform });

			expect(await presence.count('room')).toBe(1);
			expect(ws.isSubscribed('__presence:room')).toBe(true);
		});

		it('hooks.subscribe sends current list for __presence:* topics', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws1, 'room', platform);
			platform.reset();

			const wsObserver = mockWs({ id: 'admin', name: 'Admin' });
			await presence.hooks.subscribe(wsObserver, '__presence:room', { platform });

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);
			expect(lists[0].data).toEqual([
				{ key: '1', data: { id: '1', name: 'Alice' } }
			]);
			// Observer should NOT be counted
			expect(await presence.count('room')).toBe(1);
		});

		it('hooks.subscribe sends empty list when no users exist', async () => {
			const ws = mockWs({ id: 'admin', name: 'Admin' });
			await presence.hooks.subscribe(ws, '__presence:empty', { platform });

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);
			expect(lists[0].data).toEqual([]);
		});

		it('hooks.subscribe passes through other __-prefixed topics without error', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.hooks.subscribe(ws, '__other:topic', { platform });

			// join ignores __ topics, so no presence state should exist
			expect(platform.published).toHaveLength(0);
		});

		it('hooks.close calls leave', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			platform.reset();

			await presence.hooks.close(ws, { platform });

			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(1);
			expect(await presence.count('room')).toBe(0);
		});

		it('destructured hooks work correctly', async () => {
			const { subscribe, close } = presence.hooks;

			const ws = mockWs({ id: '1', name: 'Alice' });
			await subscribe(ws, 'room', { platform });
			expect(await presence.count('room')).toBe(1);

			platform.reset();
			await close(ws, { platform });
			expect(await presence.count('room')).toBe(0);
		});
	});

	describe('cross-instance join dedup', () => {
		it('same user joining on second instance does not broadcast a second join', async () => {
			const platform2 = mockPlatform();
			const instance1 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});
			const instance2 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: 'alice', name: 'Alice' });
			const ws2 = mockWs({ id: 'alice', name: 'Alice' });

			// Alice joins on instance1 -- should broadcast join
			await instance1.join(ws1, 'room', platform);
			const joins1 = platform.published.filter((p) => p.event === 'join');
			expect(joins1).toHaveLength(1);

			// Alice joins on instance2 -- should NOT broadcast join (already present globally)
			await instance2.join(ws2, 'room', platform2);
			const joins2 = platform2.published.filter((p) => p.event === 'join');
			expect(joins2).toHaveLength(0);

			instance1.destroy();
			instance2.destroy();
		});
	});

	describe('stale field leave handling', () => {
		it('stale field from dead instance does not suppress leave broadcast', async () => {
			const instance1 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: 'alice', name: 'Alice' });
			await instance1.join(ws1, 'room', platform);

			// Manually insert a stale field pretending to be from a dead instance
			const staleData = JSON.stringify({
				data: { id: 'alice', name: 'Alice' },
				ts: Date.now() - 200_000 // way past 180s TTL
			});
			await client.redis.hset(
				client.key('presence:room'),
				'dead-instance|alice',
				staleData
			);

			platform.reset();

			// Alice leaves instance1 -- the stale field should NOT suppress the leave
			await instance1.leave(ws1, platform);
			const leaves = platform.published.filter((p) => p.event === 'leave');
			expect(leaves).toHaveLength(1);

			instance1.destroy();
		});
	});

	describe('cross-instance safety', () => {
		it('leave on one instance does not remove user present on another', async () => {
			// Simulate two instances sharing the same Redis
			const platform2 = mockPlatform();
			const instance1 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});
			const instance2 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: 'alice', name: 'Alice' });
			const ws2 = mockWs({ id: 'alice', name: 'Alice' });

			// Alice joins on both instances
			await instance1.join(ws1, 'room', platform);
			await instance2.join(ws2, 'room', platform2);

			// Alice leaves instance1
			await instance1.leave(ws1, platform);

			// Alice should still be present via instance2
			const list = await instance2.list('room');
			expect(list).toHaveLength(1);
			expect(list[0]).toEqual({ id: 'alice', name: 'Alice' });

			const count = await instance2.count('room');
			expect(count).toBe(1);

			instance1.destroy();
			instance2.destroy();
		});

		it('leave broadcasts only after last instance disconnects', async () => {
			const platform2 = mockPlatform();
			const instance1 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});
			const instance2 = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: 'alice', name: 'Alice' });
			const ws2 = mockWs({ id: 'alice', name: 'Alice' });

			await instance1.join(ws1, 'room', platform);
			await instance2.join(ws2, 'room', platform2);
			platform.reset();
			platform2.reset();

			// Leave on instance1 -- should NOT broadcast leave
			await instance1.leave(ws1, platform);
			const leaves1 = platform.published.filter((p) => p.event === 'leave');
			expect(leaves1).toHaveLength(0);

			// Leave on instance2 -- NOW should broadcast leave
			await instance2.leave(ws2, platform2);
			const leaves2 = platform2.published.filter((p) => p.event === 'leave');
			expect(leaves2).toHaveLength(1);

			instance1.destroy();
			instance2.destroy();
		});
	});

	describe('dead connection cleanup (#4)', () => {
		it('heartbeat detects dead ws and cleans up presence', async () => {
			// Use a short heartbeat so the test does not wait long
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 200,
				ttl: 10
			});

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			await p.join(ws1, 'room', platform);
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(2);

			// Simulate ws1 dying without close handler firing
			ws1.close();

			// Wait for the heartbeat to fire and detect the dead connection
			await new Promise((r) => setTimeout(r, 350));

			// ws1 should have been cleaned up by the heartbeat probe
			expect(await p.count('room')).toBe(1);
			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0]).toEqual({ id: '2', name: 'Bob' });

			p.destroy();
		});

		it('heartbeat leave for dead ws broadcasts leave event', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 200,
				ttl: 10
			});

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await p.join(ws1, 'room', platform);
			platform.reset();

			ws1.close();
			await new Promise((r) => setTimeout(r, 350));

			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(leaves).toHaveLength(1);
			expect(leaves[0].data.key).toBe('1');

			p.destroy();
		});
	});

	describe('pipeline batch leave (#1)', () => {
		it('mass disconnect cleans up all entries', async () => {
			const connections = [];
			for (let i = 0; i < 20; i++) {
				const ws = mockWs({ id: String(i), name: 'User' + i });
				await presence.join(ws, 'room', platform);
				connections.push(ws);
			}
			expect(await presence.count('room')).toBe(20);

			// Disconnect all at once via leave-all
			await Promise.all(connections.map((ws) => presence.leave(ws, platform)));

			expect(await presence.count('room')).toBe(0);
			expect(await presence.list('room')).toEqual([]);
		});

		it('pipeline leave broadcasts correct leave events', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '1', name: 'Alice' }); // same user, different tab
			await presence.join(ws1, 'room-a', platform);
			await presence.join(ws1, 'room-b', platform);
			await presence.join(ws2, 'room-a', platform);
			platform.reset();

			// Leave-all for ws1 uses pipeline internally
			await presence.leave(ws1, platform);

			const leaves = platform.published.filter((p) => p.event === 'leave');
			// ws1 should leave room-b (only connection), but NOT room-a (ws2 still there with same key)
			expect(leaves).toHaveLength(1);
			expect(leaves[0].topic).toBe('__presence:room-b');
		});
	});
});
