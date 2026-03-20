import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../redis/presence.js';
import { createCircuitBreaker, CircuitBrokenError } from '../../shared/breaker.js';

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

	describe('userData sanitization', () => {
		it('strips __subscriptions and remoteAddress from userData before select', async () => {
			const ws = mockWs({
				id: '1',
				name: 'Alice',
				__subscriptions: new Set(['room']),
				remoteAddress: '127.0.0.1'
			});
			await presence.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.__subscriptions).toBeUndefined();
			expect(listData[0].data.remoteAddress).toBeUndefined();
			expect(listData[0].data.id).toBe('1');
		});
	});

	describe('default select strips __-prefixed keys', () => {
		it('default select strips __internal keys from broadcast data', async () => {
			const p = createPresence(client, { key: 'id', heartbeat: 60000, ttl: 180 });
			const ws = mockWs({ id: '1', name: 'Alice', __secret: 'hidden' });
			await p.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.id).toBe('1');
			expect(listData[0].data.name).toBe('Alice');
			expect(listData[0].data.__secret).toBeUndefined();

			p.destroy();
		});

		it('default select recursively strips nested __-prefixed keys', async () => {
			const p = createPresence(client, { key: 'id', heartbeat: 60000, ttl: 180 });
			const ws = mockWs({ id: '1', profile: { name: 'Alice', __secret: 'hidden' } });
			await p.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.profile.name).toBe('Alice');
			expect(listData[0].data.profile.__secret).toBeUndefined();

			p.destroy();
		});

		it('rejects non-function select option', () => {
			expect(() => createPresence(client, { select: 'nope' })).toThrow('select must be a function');
			expect(() => createPresence(client, { select: 42 })).toThrow('select must be a function');
		});

		it('default select strips sensitive-regex keys like password and token', async () => {
			const p = createPresence(client, { key: 'id', heartbeat: 60000, ttl: 180 });
			const ws = mockWs({ id: '1', name: 'Alice', password: 'secret', authToken: 'xyz' });
			await p.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.id).toBe('1');
			expect(listData[0].data.name).toBe('Alice');
			expect(listData[0].data.password).toBeUndefined();
			expect(listData[0].data.authToken).toBeUndefined();

			p.destroy();
		});

		it('default select handles circular references without crashing', async () => {
			const p = createPresence(client, { key: 'id', heartbeat: 60000, ttl: 180 });
			const userData = { id: '1', name: 'Alice' };
			userData.self = userData;
			const ws = mockWs(userData);

			await p.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.id).toBe('1');
			expect(listData[0].data.name).toBe('Alice');

			p.destroy();
		});
	});

	describe('key resolution before select', () => {
		it('resolves dedup key from raw data even when select strips it', async () => {
			const p = createPresence(client, {
				key: 'sessionId',
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ sessionId: 'abc', name: 'Alice' });
			const ws2 = mockWs({ sessionId: 'abc', name: 'Alice-tab2' });

			await p.join(ws1, 'room', platform);
			await p.join(ws2, 'room', platform);

			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('stripInternal shared references', () => {
		it('preserves shared objects referenced from multiple keys', async () => {
			const p = createPresence(client, {
				key: 'id',
				heartbeat: 60000,
				ttl: 180
			});

			const shared = { x: 1, y: 2 };
			const ws = mockWs({ id: '1', a: shared, b: shared });
			await p.join(ws, 'room', platform);

			const listData = platform.sent.find((s) => s.event === 'list').data;
			expect(listData[0].data.a).toEqual({ x: 1, y: 2 });
			expect(listData[0].data.b).toEqual({ x: 1, y: 2 });

			p.destroy();
		});
	});

	describe('timing option validation', () => {
		it('rejects invalid heartbeat', () => {
			expect(() => createPresence(client, { heartbeat: -1 })).toThrow('heartbeat must be a positive');
			expect(() => createPresence(client, { heartbeat: 0 })).toThrow('heartbeat must be a positive');
			expect(() => createPresence(client, { heartbeat: 'bad' })).toThrow('heartbeat must be a positive');
		});

		it('rejects invalid ttl', () => {
			expect(() => createPresence(client, { ttl: -1 })).toThrow('ttl must be a positive');
			expect(() => createPresence(client, { ttl: 0 })).toThrow('ttl must be a positive');
			expect(() => createPresence(client, { ttl: 'bad' })).toThrow('ttl must be a positive');
		});
	});

	describe('updated event', () => {
		it('publishes updated when same user re-joins with different data', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			const ws2 = mockWs({ id: '1', status: 'away' });

			await p.join(ws1, 'room', platform);
			platform.reset();

			await p.join(ws2, 'room', platform);

			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(1);
			expect(updated[0].data.data.status).toBe('away');

			p.destroy();
		});

		it('does not publish updated when data is unchanged', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			const ws2 = mockWs({ id: '1', status: 'active' });

			await p.join(ws1, 'room', platform);
			platform.reset();

			await p.join(ws2, 'room', platform);

			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(0);

			p.destroy();
		});
	});

	describe('hooks', () => {
		it('API shape includes hooks with subscribe, unsubscribe, and close', () => {
			expect(typeof presence.hooks.subscribe).toBe('function');
			expect(typeof presence.hooks.unsubscribe).toBe('function');
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

		it('hooks.unsubscribe removes presence from a single topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			platform.reset();

			await presence.hooks.unsubscribe(ws, 'room-a', { platform });

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(1);
		});

		it('hooks.unsubscribe ignores __-prefixed topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			platform.reset();

			await presence.hooks.unsubscribe(ws, '__presence:room', { platform });
			expect(await presence.count('room')).toBe(1);
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

	describe('cross-instance join behavior', () => {
		it('same user joining on second instance broadcasts join (client dedup)', async () => {
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

			// Alice joins on instance1 -- broadcasts join
			await instance1.join(ws1, 'room', platform);
			const joins1 = platform.published.filter((p) => p.event === 'join');
			expect(joins1).toHaveLength(1);

			// Alice joins on instance2 -- also broadcasts join.
			// Cross-instance dedup was removed because the O(N) scan per join
			// was the scalability bottleneck.  Duplicate join events are
			// harmless: the client does Map.set(key, data) which is idempotent.
			await instance2.join(ws2, 'room', platform2);
			const joins2 = platform2.published.filter((p) => p.event === 'join');
			expect(joins2).toHaveLength(1);

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

	describe('heartbeat publishes heartbeat event for client maxAge', () => {
		it('publishes heartbeat with active user keys on each tick', async () => {
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
			platform.reset();

			// Wait for one heartbeat tick
			await new Promise((r) => setTimeout(r, 350));

			const heartbeats = platform.published.filter((e) => e.event === 'heartbeat');
			expect(heartbeats.length).toBeGreaterThanOrEqual(1);

			const hb = heartbeats[0];
			expect(hb.topic).toBe('__presence:room');
			expect(hb.data).toContain('1');
			expect(hb.data).toContain('2');

			p.destroy();
		});

		it('does not publish heartbeat for topics with no active users', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 200,
				ttl: 10
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			await p.leave(ws, platform);
			platform.reset();

			await new Promise((r) => setTimeout(r, 350));

			const heartbeats = platform.published.filter((e) => e.event === 'heartbeat');
			expect(heartbeats).toHaveLength(0);

			p.destroy();
		});
	});

	describe('join rollback on breaker failure', () => {
		it('rolls back local state when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, name: ud.name }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			// Break the circuit
			breaker.failure();
			expect(breaker.state).toBe('broken');

			const ws = mockWs({ id: '1', name: 'Alice' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow(CircuitBrokenError);

			// Reset breaker so count() can execute
			breaker.reset();

			// Local state should be rolled back -- heartbeat must not
			// see any presence data for this topic.
			expect(await p.count('room')).toBe(0);

			// After breaker resets, joining should work normally
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			p.destroy();
		});

		it('rolls back local state when Redis eval throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			// Make the next eval throw
			const origEval = client.redis.eval;
			let evalCallCount = 0;
			client.redis.eval = async (...args) => {
				evalCallCount++;
				if (evalCallCount === 1) throw new Error('Redis connection lost');
				return origEval.apply(client.redis, args);
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('Redis connection lost');

			// Local state should be clean
			expect(await p.count('room')).toBe(0);

			// Restore and verify recovery
			client.redis.eval = origEval;
			breaker.reset();

			const ws2 = mockWs({ id: '2' });
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('soft-fail leave suppresses broadcast when Redis is unavailable', () => {
		it('per-topic leave suppresses broadcast when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			platform.reset();

			// Break the circuit so LEAVE_SCRIPT cannot execute
			breaker.failure();

			await p.leave(ws, platform, 'room');

			// No leave should be broadcast because we cannot confirm
			// the user is gone from other instances
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(leaves).toHaveLength(0);

			p.destroy();
		});

		it('leave-all suppresses broadcasts when pipeline fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			platform.reset();

			// Make pipeline exec throw by returning a proxy that
			// collects commands but throws on exec
			const origPipeline = client.redis.pipeline;
			client.redis.pipeline = () => {
				return new Proxy({}, {
					get(_, method) {
						if (method === 'exec') {
							return async () => { throw new Error('pipeline failed'); };
						}
						return () => new Proxy({}, { get: (_, m) => m === 'exec' ? async () => { throw new Error('pipeline failed'); } : () => {} });
					}
				});
			};

			await p.leave(ws, platform);

			// No leave should be broadcast
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(leaves).toHaveLength(0);

			client.redis.pipeline = origPipeline;
			p.destroy();
		});
	});

	describe('sensitive data warning for arrays', () => {
		it('warns about sensitive keys nested inside arrays', async () => {
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ud
			});

			const ws = mockWs({ id: '1', profiles: [{ authToken: 'secret' }] });
			await p.join(ws, 'room', platform);

			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining('authToken')
			);

			warn.mockRestore();
			p.destroy();
		});
	});

	describe('hgetall failure in join undoes join entirely', () => {
		it('throws without publishing join or leave when hgetall fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const origHgetall = client.redis.hgetall;
			let hgetallCallCount = 0;
			client.redis.hgetall = async (...args) => {
				hgetallCallCount++;
				if (hgetallCallCount === 1) throw new Error('hgetall failed');
				return origHgetall.apply(client.redis, args);
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('hgetall failed');

			// No join or leave events should have been published since
			// join events are deferred until the full operation commits.
			const joins = platform.published.filter((e) => e.event === 'join');
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(joins).toHaveLength(0);
			expect(leaves).toHaveLength(0);

			// Redis field cleaned up
			expect(await p.count('room')).toBe(0);

			// No stale list sent
			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(0);

			// Recovery: retry succeeds
			client.redis.hgetall = origHgetall;
			const ws2 = mockWs({ id: '2' });
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('dead-socket join rollback', () => {
		it('rolls back local state when ws is already closed before Redis write', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			ws.close();

			await presence.join(ws, 'room', platform);

			// Local state should be fully rolled back
			expect(await presence.count('room')).toBe(0);
			// No join event published
			expect(platform.published.filter((e) => e.event === 'join')).toHaveLength(0);
			// No list sent
			expect(platform.sent.filter((s) => s.event === 'list')).toHaveLength(0);
		});

		it('does not leak Redis subscription after dead-socket rollback', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			ws.close();

			await presence.join(ws, 'room', platform);
			platform.reset();

			// If the subscription leaked, a remote event would be forwarded
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob', name: 'Bob' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			expect(platform.published).toHaveLength(0);
		});

		it('ws.subscribe failure after Redis write does not publish join or leave', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);

			// Verify join was published
			const joins = platform.published.filter((e) => e.event === 'join');
			expect(joins).toHaveLength(1);

			platform.reset();

			// Create a ws that lets getUserData/getBufferedAmount pass but
			// subscribe throws (simulating ws death between HSET and ws.subscribe)
			const ws2 = mockWs({ id: '2' });
			const origSubscribe = ws2.subscribe;
			ws2.subscribe = (topic) => {
				if (topic === '__presence:room') throw new Error('ws closed');
				return origSubscribe.call(ws2, topic);
			};

			await p.join(ws2, 'room', platform);

			// No join or leave should be published since join is deferred
			// until after ws.subscribe succeeds
			const newJoins = platform.published.filter((e) => e.event === 'join');
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(newJoins).toHaveLength(0);
			expect(leaves).toHaveLength(0);

			// count should only include the first user
			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('failed-join subscription leak', () => {
		it('unsubscribes from Redis topic after breaker-guarded join failure', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow(CircuitBrokenError);

			breaker.reset();
			platform.reset();

			// The topic subscription should have been unwound.
			// A remote event for 'room' should NOT be forwarded.
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob' } }
			});
			await client.redis.publish(client.key('presence:events:room'), remoteMsg);

			expect(platform.published).toHaveLength(0);

			p.destroy();
		});
	});

	describe('subscribe failure rollback', () => {
		it('rolls back local state when Redis topic subscribe fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			// Make the subscriber's subscribe() fail
			const origDuplicate = client.duplicate.bind(client);
			let callCount = 0;
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				const origSub = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					callCount++;
					if (callCount === 1) throw new Error('subscribe failed');
					return origSub(ch);
				};
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('subscribe failed');

			// Local state should be fully rolled back
			// Reset breaker/client state so count() works
			expect(await p.count('room')).toBe(0);

			// After heartbeat, the ghost user should not appear
			expect(platform.published.filter((e) => e.event === 'join')).toHaveLength(0);

			p.destroy();
		});

		it('does not poison subscribedChannels when subscribe fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const origDuplicate = client.duplicate.bind(client);
			let failOnce = true;
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				const origSub = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					if (failOnce) {
						failOnce = false;
						throw new Error('subscribe failed');
					}
					return origSub(ch);
				};
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('subscribe failed');

			// Retry should succeed because the channel was not poisoned
			const ws2 = mockWs({ id: '2' });
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('sync snapshot-before-subscribe race', () => {
		it('subscribes to Redis channel before fetching snapshot', async () => {
			const callOrder = [];
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				const origSub = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					callOrder.push('subscribe');
					return origSub(ch);
				};
				return dup;
			};
			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async (...args) => {
				callOrder.push('hgetall');
				return origHgetall.apply(client.redis, args);
			};

			const ws = mockWs({ id: 'observer' });
			await presence.sync(ws, 'room', platform);

			// subscribe must happen before hgetall
			const subIdx = callOrder.indexOf('subscribe');
			const hgetIdx = callOrder.indexOf('hgetall');
			expect(subIdx).toBeLessThan(hgetIdx);

			client.redis.hgetall = origHgetall;
		});

		it('does not miss a remote join between subscribe and snapshot', async () => {
			// Pre-populate a user so the snapshot has something
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws1, 'room', platform);
			platform.reset();

			const observer = mockWs({ id: 'observer' });
			await presence.sync(observer, 'room', platform);

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);
			expect(lists[0].data).toHaveLength(1);
			expect(observer.isSubscribed('__presence:room')).toBe(true);
		});
	});

	describe('updated broadcast after Redis persistence', () => {
		it('does not broadcast updated when hset fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			const ws2 = mockWs({ id: '1', status: 'away' });

			await p.join(ws1, 'room', platform);
			platform.reset();

			const origHset = client.redis.hset;
			client.redis.hset = async () => { throw new Error('hset failed'); };

			await expect(p.join(ws2, 'room', platform)).rejects.toThrow('hset failed');

			// No updated event should have been broadcast
			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(0);

			client.redis.hset = origHset;

			// list() should still show old data
			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].status).toBe('active');

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

	describe('subscribe failure full rollback verification', () => {
		it('restores localCounts, localData, wsTopics, and activeTopics on subscribe failure', async () => {
			// Use a fresh client so the subscriber doesn't already exist
			const freshClient = mockRedisClient('test:');
			const p = createPresence(freshClient, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			// Force subscriber.subscribe() to fail by overriding the
			// duplicate before the subscriber is created
			const origDuplicate = freshClient.duplicate.bind(freshClient);
			freshClient.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				const origSub = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					throw new Error('subscribe failed');
				};
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room-fail', platform)).rejects.toThrow('subscribe failed');

			// The failed topic should have zero state
			expect(await p.count('room-fail')).toBe(0);
			expect(await p.list('room-fail')).toEqual([]);

			// The failed ws should not be in wsTopics (no leaked entries)
			// Verify by attempting leave -- should be a no-op
			platform.reset();
			await p.leave(ws, platform);
			expect(platform.published.filter((e) => e.event === 'leave')).toHaveLength(0);

			p.destroy();
		});

		it('existing topics are unaffected by failed subscribe on a new topic', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			// Join existing topic successfully
			const wsExisting = mockWs({ id: 'existing' });
			await p.join(wsExisting, 'other-room', platform);
			expect(await p.count('other-room')).toBe(1);

			// Now make Redis eval fail (subscribeToTopic won't recreate
			// the subscriber since it already exists, so we fail the HSET instead)
			const origEval = client.redis.eval;
			client.redis.eval = async (...args) => {
				const script = args[0];
				// Only fail the JOIN_SCRIPT
				if (script.includes('hset') && script.includes('expire') && !script.includes('hdel')) {
					throw new Error('Redis down');
				}
				return origEval.apply(client.redis, args);
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room-fail', platform)).rejects.toThrow('Redis down');

			client.redis.eval = origEval;

			// The existing topic should be unaffected
			expect(await p.count('other-room')).toBe(1);

			// The failed topic should have zero state
			expect(await p.count('room-fail')).toBe(0);

			p.destroy();
		});

		it('cleans up Redis hash field written before subscribe failure', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			// The subscribe call happens before the Redis eval (JOIN_SCRIPT).
			// subscribeToTopic is called before the HSET. So if subscribe fails,
			// there's no Redis field to clean up -- verify this is the case.
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('subscribe failed');

			// No hash fields should exist
			const all = await client.redis.hgetall(client.key('presence:room'));
			expect(Object.keys(all)).toHaveLength(0);

			p.destroy();
		});
	});

	describe('no ghost after heartbeat on failed join', () => {
		it('heartbeat does not resurrect a user whose join() threw', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 150,
				ttl: 10
			});

			const freshClient = mockRedisClient('test:');
			const p2 = createPresence(freshClient, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 150,
				ttl: 10
			});

			const origDuplicate = freshClient.duplicate.bind(freshClient);
			freshClient.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p2.join(ws, 'room', platform)).rejects.toThrow('subscribe failed');

			expect(await p2.count('room')).toBe(0);

			await new Promise((r) => setTimeout(r, 300));

			expect(await p2.count('room')).toBe(0);
			expect(await p2.list('room')).toEqual([]);

			const all = await freshClient.redis.hgetall(freshClient.key('presence:room'));
			expect(Object.keys(all)).toHaveLength(0);

			p.destroy();
			p2.destroy();
		});

		it('heartbeat does not resurrect a user whose HSET threw', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 150,
				ttl: 10
			});

			const origEval = client.redis.eval;
			let evalFail = true;
			client.redis.eval = async (...args) => {
				const script = args[0];
				if (evalFail && script.includes('hset') && script.includes('expire') && !script.includes('hdel')) {
					throw new Error('Redis down');
				}
				return origEval.apply(client.redis, args);
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('Redis down');

			evalFail = false;
			client.redis.eval = origEval;

			expect(await p.count('room')).toBe(0);

			await new Promise((r) => setTimeout(r, 300));

			expect(await p.count('room')).toBe(0);
			expect(await p.list('room')).toEqual([]);

			p.destroy();
		});
	});

	describe('swallowed presence relay failures', () => {
		it('join completes and sends list even when publishEvent fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			// Make redis.publish fail (used by publishEvent)
			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);

			// The join should have completed (local broadcast + list sent)
			const joins = platform.published.filter((e) => e.event === 'join');
			expect(joins).toHaveLength(1);

			const lists = platform.sent.filter((s) => s.event === 'list');
			expect(lists).toHaveLength(1);

			// count/list should show the user
			expect(await p.count('room')).toBe(1);

			client.redis.publish = origPublish;
			p.destroy();
		});

		it('leave completes even when publishEvent fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			platform.reset();

			// Make redis.publish fail
			const origPublish = client.redis.publish;
			client.redis.publish = async () => { throw new Error('publish failed'); };

			await p.leave(ws, platform, 'room');

			// The leave should complete locally
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(leaves).toHaveLength(1);

			expect(await p.count('room')).toBe(0);

			client.redis.publish = origPublish;
			p.destroy();
		});
	});

	describe('breaker accounting in presence', () => {
		it('join records success() after Redis eval succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			// Add a failure to check success() resets it
			breaker.failure();
			expect(breaker.failures).toBe(1);

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);

			expect(breaker.failures).toBe(0);

			p.destroy();
			breaker.destroy();
		});

		it('join records failure() when Redis eval throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const origEval = client.redis.eval;
			client.redis.eval = async () => { throw new Error('Redis down'); };

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('Redis down');
			expect(breaker.failures).toBe(1);

			client.redis.eval = origEval;
			p.destroy();
			breaker.destroy();
		});

		it('per-topic leave records success() after LEAVE_SCRIPT succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			breaker.failure();
			expect(breaker.failures).toBe(1);

			await p.leave(ws, platform, 'room');
			expect(breaker.failures).toBe(0);

			p.destroy();
			breaker.destroy();
		});

		it('per-topic leave records failure() when LEAVE_SCRIPT throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);

			const origEval = client.redis.eval;
			client.redis.eval = async () => { throw new Error('Redis down'); };

			await p.leave(ws, platform, 'room');
			expect(breaker.failures).toBe(1);

			// Leave should suppress broadcast when eval fails
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(leaves).toHaveLength(0);

			client.redis.eval = origEval;
			p.destroy();
			breaker.destroy();
		});

		it('subscribeToTopic failure records breaker failure', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const freshClient = mockRedisClient('test:');
			const p = createPresence(freshClient, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const origDuplicate = freshClient.duplicate.bind(freshClient);
			freshClient.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('subscribe failed');
			expect(breaker.failures).toBe(1);

			p.destroy();
			breaker.destroy();
		});

		it('join hgetall failure records failure() even on multi-tab path', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws1 = mockWs({ id: '1' });
			await p.join(ws1, 'room', platform);
			expect(breaker.failures).toBe(0);

			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };

			const ws2 = mockWs({ id: '2' });
			await expect(p.join(ws2, 'room', platform)).rejects.toThrow('hgetall failed');
			expect(breaker.failures).toBe(1);

			client.redis.hgetall = origHgetall;
			p.destroy();
			breaker.destroy();
		});

		it('join update-path hset failure records failure()', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			await p.join(ws1, 'room', platform);

			const origHset = client.redis.hset;
			client.redis.hset = async () => { throw new Error('hset failed'); };

			const ws2 = mockWs({ id: '1', status: 'away' });
			await expect(p.join(ws2, 'room', platform)).rejects.toThrow('hset failed');
			expect(breaker.failures).toBe(1);

			client.redis.hset = origHset;
			p.destroy();
			breaker.destroy();
		});

		it('broken breaker blocks multi-tab join update path', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			await p.join(ws1, 'room', platform);

			breaker.failure();

			const ws2 = mockWs({ id: '1', status: 'away' });
			await expect(p.join(ws2, 'room', platform)).rejects.toThrow(CircuitBrokenError);

			breaker.reset();
			p.destroy();
			breaker.destroy();
		});

		it('count() records success() so probe recovers breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();
			expect(breaker.state).toBe('broken');

			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			const count = await p.count('room');
			expect(count).toBe(0);
			expect(breaker.state).toBe('healthy');

			p.destroy();
			breaker.destroy();
		});

		it('list() records success() so probe recovers breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			await p.list('room');
			expect(breaker.state).toBe('healthy');

			p.destroy();
			breaker.destroy();
		});
	});

	describe('multi-tab data restoration on leave', () => {
		it('leaving the newer tab restores the older tabs data', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'active' });
			const wsB = mockWs({ id: '1', status: 'away' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			platform.reset();

			await p.leave(wsB, platform, 'room');

			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].status).toBe('active');

			p.destroy();
		});

		it('leaving the older tab keeps the newer tabs data', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'active' });
			const wsB = mockWs({ id: '1', status: 'away' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			platform.reset();

			await p.leave(wsA, platform, 'room');

			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].status).toBe('away');

			p.destroy();
		});

		it('three tabs: leaving newest restores second newest, not oldest', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'A' });
			const wsB = mockWs({ id: '1', status: 'B' });
			const wsC = mockWs({ id: '1', status: 'C' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			await p.join(wsC, 'room', platform);
			platform.reset();

			await p.leave(wsC, platform, 'room');

			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].status).toBe('B');

			p.destroy();
		});

		it('does not broadcast updated when corrective hset fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'active' });
			const wsB = mockWs({ id: '1', status: 'away' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			platform.reset();

			const origHset = client.redis.hset;
			client.redis.hset = async () => { throw new Error('hset failed'); };

			await p.leave(wsB, platform, 'room');

			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(0);

			client.redis.hset = origHset;
			p.destroy();
		});

		it('leave-all broadcasts updated when restoring remaining tab data', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'active' });
			const wsB = mockWs({ id: '1', status: 'away' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			platform.reset();

			await p.leave(wsB, platform);

			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(1);
			expect(updated[0].data.data.status).toBe('active');

			const list = await p.list('room');
			expect(list[0].status).toBe('active');

			p.destroy();
		});

		it('leave-all does not broadcast updated when corrective hset fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const wsA = mockWs({ id: '1', status: 'active' });
			const wsB = mockWs({ id: '1', status: 'away' });

			await p.join(wsA, 'room', platform);
			await p.join(wsB, 'room', platform);
			platform.reset();

			const origHset = client.redis.hset;
			client.redis.hset = async () => { throw new Error('hset failed'); };

			await p.leave(wsB, platform);

			const updated = platform.published.filter((e) => e.event === 'updated');
			expect(updated).toHaveLength(0);

			client.redis.hset = origHset;
			p.destroy();
		});
	});

	describe('sync breaker probe recovery', () => {
		it('sync() records success() so breaker recovers from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			const ws = mockWs({ id: 'obs' });
			await p.sync(ws, 'room', platform);
			expect(breaker.state).toBe('healthy');

			p.destroy();
			breaker.destroy();
		});
	});

	describe('multi-tab join rollback safety', () => {
		it('failed update-path hset rolls back local state completely', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			await p.join(ws1, 'room', platform);

			const origHset = client.redis.hset;
			client.redis.hset = async () => { throw new Error('hset failed'); };

			const ws2 = mockWs({ id: '1', status: 'away' });
			await expect(p.join(ws2, 'room', platform)).rejects.toThrow('hset failed');

			client.redis.hset = origHset;

			expect(await p.count('room')).toBe(1);
			const list = await p.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].status).toBe('active');

			p.destroy();
		});

		it('undoJoin restores previous Redis value when prevCount > 0', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			const ws2 = mockWs({ id: '1', status: 'away' });
			await p.join(ws1, 'room', platform);
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };

			const ws3 = mockWs({ id: '3' });
			await expect(p.join(ws3, 'room', platform)).rejects.toThrow('hgetall failed');

			client.redis.hgetall = origHgetall;

			expect(await p.count('room')).toBe(1);
			const list = await p.list('room');
			expect(list).toHaveLength(1);

			p.destroy();
		});

		it('undoJoin propagates restore failure when hset fails during rollback', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, status: ud.status }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1', status: 'active' });
			await p.join(ws1, 'room', platform);

			const ws2 = mockWs({ id: '1', status: 'away' });
			await p.join(ws2, 'room', platform);
			expect(await p.count('room')).toBe(1);

			const origHgetall = client.redis.hgetall;
			const origHset = client.redis.hset;
			let hgetallFails = true;
			let hsetFails = true;
			client.redis.hgetall = async (...args) => {
				if (hgetallFails) throw new Error('hgetall failed');
				return origHgetall.apply(client.redis, args);
			};
			client.redis.hset = async (...args) => {
				if (hsetFails) throw new Error('hset failed');
				return origHset.apply(client.redis, args);
			};

			const ws3 = mockWs({ id: '1', status: 'new' });
			await expect(p.join(ws3, 'room', platform)).rejects.toThrow('hset failed');

			client.redis.hgetall = origHgetall;
			client.redis.hset = origHset;
			hgetallFails = false;
			hsetFails = false;

			p.destroy();
		});

		it('undoJoin propagates hdel failure for new-user rollback', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const origHgetall = client.redis.hgetall;
			const origHdel = client.redis.hdel;
			let hgetallFails = true;
			let hdelFails = true;
			client.redis.hgetall = async (...args) => {
				if (hgetallFails) throw new Error('hgetall failed');
				return origHgetall.apply(client.redis, args);
			};
			client.redis.hdel = async (...args) => {
				if (hdelFails) throw new Error('hdel failed');
				return origHdel.apply(client.redis, args);
			};

			const ws = mockWs({ id: '1' });
			await expect(p.join(ws, 'room', platform)).rejects.toThrow('hdel failed');

			client.redis.hgetall = origHgetall;
			client.redis.hdel = origHdel;
			hgetallFails = false;
			hdelFails = false;

			p.destroy();
		});

		it('does not publish join or leave when snapshot fails', async () => {
			const p = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180
			});

			const ws1 = mockWs({ id: '1' });
			await p.join(ws1, 'room', platform);
			platform.reset();

			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall failed'); };

			const ws2 = mockWs({ id: '2' });
			await expect(p.join(ws2, 'room', platform)).rejects.toThrow('hgetall failed');

			client.redis.hgetall = origHgetall;

			const joins = platform.published.filter((e) => e.event === 'join');
			const leaves = platform.published.filter((e) => e.event === 'leave');
			expect(joins).toHaveLength(0);
			expect(leaves).toHaveLength(0);

			expect(await p.count('room')).toBe(1);

			p.destroy();
		});
	});

	describe('subscriber setup during breaker probing', () => {
		it('join() in probing state still receives cross-instance events', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const freshClient = mockRedisClient('test:');
			const p = createPresence(freshClient, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			const ws = mockWs({ id: '1' });
			await p.join(ws, 'room', platform);
			expect(breaker.state).toBe('healthy');
			platform.reset();

			const remoteMsg = JSON.stringify({
				instanceId: 'remote',
				topic: 'room',
				event: 'join',
				payload: { key: 'bob', data: { id: 'bob' } }
			});
			await freshClient.redis.publish(freshClient.key('presence:events:room'), remoteMsg);

			const joins = platform.published.filter((e) => e.event === 'join');
			expect(joins).toHaveLength(1);
			expect(joins[0].data.key).toBe('bob');

			p.destroy();
			breaker.destroy();
		});

		it('sync() in probing state still receives cross-instance events', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const freshClient = mockRedisClient('test:');
			const p = createPresence(freshClient, {
				key: 'id',
				select: (ud) => ({ id: ud.id }),
				heartbeat: 60000,
				ttl: 180,
				breaker
			});

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			const ws = mockWs({ id: 'obs' });
			await p.sync(ws, 'room', platform);
			expect(breaker.state).toBe('healthy');
			platform.reset();

			const remoteMsg = JSON.stringify({
				instanceId: 'remote',
				topic: 'room',
				event: 'join',
				payload: { key: 'alice', data: { id: 'alice' } }
			});
			await freshClient.redis.publish(freshClient.key('presence:events:room'), remoteMsg);

			const joins = platform.published.filter((e) => e.event === 'join');
			expect(joins).toHaveLength(1);

			p.destroy();
			breaker.destroy();
		});
	});
});
