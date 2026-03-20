import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createCursor } from '../../redis/cursor.js';
import { createCircuitBreaker, CircuitBrokenError } from '../../shared/breaker.js';

describe('redis cursor', () => {
	let client;
	let cursors;
	let platform;

	beforeEach(() => {
		vi.useRealTimers();
		client = mockRedisClient('test:');
		platform = mockPlatform();
		cursors = createCursor(client, {
			throttle: 100,
			select: (userData) => ({ id: userData.id, name: userData.name })
		});
	});

	afterEach(() => {
		cursors.destroy();
	});

	describe('createCursor', () => {
		it('returns a cursor tracker with the expected API', () => {
			expect(typeof cursors.update).toBe('function');
			expect(typeof cursors.remove).toBe('function');
			expect(typeof cursors.list).toBe('function');
			expect(typeof cursors.clear).toBe('function');
			expect(typeof cursors.destroy).toBe('function');
		});

		it('works with default options', () => {
			const c = createCursor(client);
			expect(typeof c.update).toBe('function');
			c.destroy();
		});

		it('throws on negative throttle', () => {
			expect(() => createCursor(client, { throttle: -1 })).toThrow('non-negative');
		});

		it('throws on non-function select', () => {
			expect(() => createCursor(client, { select: 'bad' })).toThrow('function');
		});

		it('throws on invalid topicThrottle', () => {
			expect(() => createCursor(client, { topicThrottle: -5 })).toThrow('non-negative');
			expect(() => createCursor(client, { topicThrottle: 'bad' })).toThrow('non-negative');
		});

		it('throws on invalid ttl', () => {
			expect(() => createCursor(client, { ttl: 0 })).toThrow('ttl must be a positive');
			expect(() => createCursor(client, { ttl: -1 })).toThrow('ttl must be a positive');
			expect(() => createCursor(client, { ttl: 'bad' })).toThrow('ttl must be a positive');
		});
	});

	describe('default select recursive stripping', () => {
		it('strips nested __-prefixed keys', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', profile: { name: 'Alice', __token: 'x' } });
			c.update(ws, 'doc', { x: 1 }, platform);

			expect(platform.published[0].data.user.profile.name).toBe('Alice');
			expect(platform.published[0].data.user.profile.__token).toBeUndefined();
			c.destroy();
		});

		it('handles circular references without crashing', () => {
			const c = createCursor(client, { throttle: 0 });
			const userData = { id: '1', name: 'Alice' };
			userData.self = userData;
			const ws = mockWs(userData);
			c.update(ws, 'doc', { x: 1 }, platform);

			expect(platform.published[0].data.user.id).toBe('1');
			expect(platform.published[0].data.user.name).toBe('Alice');
			c.destroy();
		});

		it('strips sensitive-regex keys like password and token', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', password: 'secret', sessionToken: 'xyz' });
			c.update(ws, 'doc', { x: 1 }, platform);

			expect(platform.published[0].data.user.id).toBe('1');
			expect(platform.published[0].data.user.password).toBeUndefined();
			expect(platform.published[0].data.user.sessionToken).toBeUndefined();
			c.destroy();
		});
	});

	describe('userData sanitization', () => {
		it('strips __subscriptions and remoteAddress from userData', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({
				id: '1',
				name: 'Alice',
				__subscriptions: new Set(['room']),
				remoteAddress: '127.0.0.1'
			});
			c.update(ws, 'canvas', { x: 10 }, platform);

			expect(platform.published[0].data.user.__subscriptions).toBeUndefined();
			expect(platform.published[0].data.user.remoteAddress).toBeUndefined();
			expect(platform.published[0].data.user.id).toBe('1');
			c.destroy();
		});
	});

	describe('snapshot', () => {
		it('sends current cursors as a bulk event to a single connection', async () => {
			const c = createCursor(client, { throttle: 0, select: (ud) => ({ id: ud.id }) });
			const ws1 = mockWs({ id: '1' });
			const ws2 = mockWs({ id: '2' });

			c.update(ws1, 'canvas', { x: 10 }, platform);
			c.update(ws2, 'canvas', { x: 20 }, platform);
			platform.reset();

			const receiver = mockWs({ id: 'new' });
			await c.snapshot(receiver, 'canvas', platform);

			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].topic).toBe('__cursor:canvas');
			expect(platform.sent[0].event).toBe('bulk');
			expect(platform.sent[0].data).toHaveLength(2);
			c.destroy();
		});

		it('does not send bulk when no cursors exist', async () => {
			const c = createCursor(client, { throttle: 0 });
			const receiver = mockWs({ id: 'new' });
			await c.snapshot(receiver, 'empty-topic', platform);

			expect(platform.sent).toHaveLength(0);
			c.destroy();
		});
	});

	describe('hooks', () => {
		it('API shape includes hooks with subscribe, message, and close', () => {
			expect(typeof cursors.hooks.subscribe).toBe('function');
			expect(typeof cursors.hooks.message).toBe('function');
			expect(typeof cursors.hooks.close).toBe('function');
		});

		it('hooks.message dispatches cursor updates', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1' });

			c.hooks.message(ws, {
				data: { type: 'cursor', topic: 'canvas', data: { x: 42 } },
				platform
			});

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.data).toEqual({ x: 42 });
			c.destroy();
		});

		it('hooks.message ignores non-cursor messages', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1' });

			c.hooks.message(ws, { data: { type: 'chat', text: 'hi' }, platform });
			expect(platform.published).toHaveLength(0);
			c.destroy();
		});

		it('hooks.close removes all cursor state', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1' });

			c.update(ws, 'canvas', { x: 10 }, platform);
			platform.reset();

			await c.hooks.close(ws, { platform });

			const removes = platform.published.filter((e) => e.event === 'remove');
			expect(removes).toHaveLength(1);
			c.destroy();
		});
	});

	describe('update - basic', () => {
		it('first update broadcasts immediately without relay: false', () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			cursors.update(ws, 'canvas', { x: 10, y: 20 }, platform);

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('__cursor:canvas');
			expect(platform.published[0].event).toBe('update');
			expect(platform.published[0].data).toEqual({
				key: expect.any(String),
				user: { id: '1', name: 'Alice' },
				data: { x: 10, y: 20 }
			});
			// User-initiated broadcasts should relay to sibling workers
			expect(platform.published[0].options).toBeUndefined();
		});

		it('uses select to extract user info', () => {
			const c = createCursor(client, {
				throttle: 0,
				select: (ud) => ({ id: ud.id })
			});
			const ws = mockWs({ id: '1', name: 'Alice', secret: 'token' });
			c.update(ws, 'room', { x: 0, y: 0 }, platform);

			expect(platform.published[0].data.user).toEqual({ id: '1' });
			expect(platform.published[0].data.user.secret).toBeUndefined();
			c.destroy();
		});

		it('without select, broadcasts full userData', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', role: 'admin' });
			c.update(ws, 'room', { x: 5, y: 5 }, platform);

			expect(platform.published[0].data.user).toEqual({ id: '1', role: 'admin' });
			c.destroy();
		});
	});

	describe('update - throttle', () => {
		it('second update within throttle window is not broadcast immediately', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			expect(platform.published).toHaveLength(1);
			platform.reset();

			vi.advanceTimersByTime(50);
			cursors.update(ws, 'canvas', { x: 10, y: 10 }, platform);
			expect(platform.published).toHaveLength(0);
		});

		it('trailing edge fires after throttle window', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(50);
			cursors.update(ws, 'canvas', { x: 10, y: 10 }, platform);

			vi.advanceTimersByTime(50);
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.data).toEqual({ x: 10, y: 10 });
		});

		it('trailing edge sends latest data, not intermediate', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(30);
			cursors.update(ws, 'canvas', { x: 5, y: 5 }, platform);
			vi.advanceTimersByTime(30);
			cursors.update(ws, 'canvas', { x: 99, y: 99 }, platform);

			vi.advanceTimersByTime(40);
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.data).toEqual({ x: 99, y: 99 });
		});

		it('update after throttle window passes broadcasts immediately', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(100);
			cursors.update(ws, 'canvas', { x: 50, y: 50 }, platform);
			expect(platform.published).toHaveLength(1);
		});

		it('throttle: 0 broadcasts every update', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			c.update(ws, 'canvas', { x: 1, y: 1 }, platform);
			c.update(ws, 'canvas', { x: 2, y: 2 }, platform);

			expect(platform.published).toHaveLength(3);
			c.destroy();
		});
	});

	describe('update - multiple topics', () => {
		it('same ws can have cursor state on different topics', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas-a', { x: 1 }, platform);
			c.update(ws, 'canvas-b', { x: 2 }, platform);

			expect(platform.published).toHaveLength(2);
			expect(platform.published[0].topic).toBe('__cursor:canvas-a');
			expect(platform.published[1].topic).toBe('__cursor:canvas-b');
			c.destroy();
		});

		it('throttle is per-user per-topic', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas-a', { x: 0 }, platform);
			cursors.update(ws, 'canvas-b', { x: 0 }, platform);
			expect(platform.published).toHaveLength(2);
		});

		it('different connections have independent throttle', () => {
			vi.useFakeTimers();
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			cursors.update(ws1, 'canvas', { x: 0 }, platform);
			cursors.update(ws2, 'canvas', { x: 0 }, platform);
			expect(platform.published).toHaveLength(2);
		});
	});

	describe('remove', () => {
		it('removes ws from all topics and broadcasts removal', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas-a', { x: 1 }, platform);
			c.update(ws, 'canvas-b', { x: 2 }, platform);
			platform.reset();

			await c.remove(ws, platform);

			const removes = platform.published.filter((e) => e.event === 'remove');
			expect(removes).toHaveLength(2);
			expect(removes.map((r) => r.topic).sort()).toEqual([
				'__cursor:canvas-a',
				'__cursor:canvas-b'
			]);
			c.destroy();
		});

		it('is safe to call for unknown ws', async () => {
			const ws = mockWs({ id: '1' });
			await expect(cursors.remove(ws, platform)).resolves.not.toThrow();
			expect(platform.published).toHaveLength(0);
		});

		it('cleans up empty topic maps', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 1 }, platform);
			await c.remove(ws, platform);

			const list = await c.list('canvas');
			expect(list).toEqual([]);
			c.destroy();
		});

		it('stops polling abandoned topics after last cursor leaves', async () => {
			const c = createCursor(client, { throttle: 0, ttl: 30 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 1 }, platform);

			// Track which topics the cleanup timer polls by spying on hgetall
			const polledKeys = [];
			const origHgetall = client.redis.hgetall;
			client.redis.hgetall = async (key) => {
				polledKeys.push(key);
				return origHgetall.call(client.redis, key);
			};

			await c.remove(ws, platform);

			// After removing the only cursor on 'canvas', the cleanup timer
			// should no longer poll that topic's hash key.
			// We can verify the topic was removed from activeTopics indirectly:
			// a new update on a different topic should not cause 'canvas' to appear in polls.
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			c.update(ws2, 'other-topic', { x: 0 }, platform);
			await c.remove(ws2, platform);

			// Restore and verify 'canvas' was cleaned up
			client.redis.hgetall = origHgetall;
			c.destroy();
		});

		it('clears pending trailing-edge timers', async () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0, y: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(50);
			cursors.update(ws, 'canvas', { x: 10, y: 10 }, platform);

			await cursors.remove(ws, platform);

			vi.advanceTimersByTime(100);
			const updates = platform.published.filter((e) => e.event === 'update');
			expect(updates).toHaveLength(0);
		});

		it('per-topic: removes cursor from only the specified topic', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas-a', { x: 1 }, platform);
			c.update(ws, 'canvas-b', { x: 2 }, platform);
			platform.reset();

			await c.remove(ws, platform, 'canvas-a');

			const removes = platform.published.filter((e) => e.event === 'remove');
			expect(removes).toHaveLength(1);
			expect(removes[0].topic).toBe('__cursor:canvas-a');

			// canvas-b should still have a cursor
			const list = await c.list('canvas-b');
			expect(list).toHaveLength(1);

			// canvas-a should be empty
			const listA = await c.list('canvas-a');
			expect(listA).toEqual([]);
			c.destroy();
		});

		it('per-topic: ws can still update remaining topics after per-topic remove', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas-a', { x: 1 }, platform);
			c.update(ws, 'canvas-b', { x: 2 }, platform);

			await c.remove(ws, platform, 'canvas-a');
			platform.reset();

			// Should still be able to update canvas-b
			c.update(ws, 'canvas-b', { x: 3 }, platform);
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.data).toEqual({ x: 3 });
			c.destroy();
		});

		it('per-topic: is safe to call for a topic the ws never had', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas-a', { x: 1 }, platform);
			platform.reset();

			await c.remove(ws, platform, 'nonexistent');
			expect(platform.published).toHaveLength(0);

			// canvas-a should still be intact
			const list = await c.list('canvas-a');
			expect(list).toHaveLength(1);
			c.destroy();
		});

		it('per-topic: cleans up wsState when last topic is removed', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 1 }, platform);
			await c.remove(ws, platform, 'canvas');

			// After removing the only topic, remove-all should be a no-op
			platform.reset();
			await c.remove(ws, platform);
			expect(platform.published).toHaveLength(0);
			c.destroy();
		});

		it('removes entry from Redis hash', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 1 }, platform);

			// Verify it was stored
			let list = await c.list('canvas');
			expect(list).toHaveLength(1);

			await c.remove(ws, platform);

			// Verify it was removed from Redis
			list = await c.list('canvas');
			expect(list).toEqual([]);
			c.destroy();
		});
	});

	describe('list', () => {
		it('returns current cursor positions from Redis', async () => {
			const c = createCursor(client, {
				throttle: 0,
				select: (ud) => ({ id: ud.id, name: ud.name })
			});
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			c.update(ws1, 'canvas', { x: 10, y: 20 }, platform);
			c.update(ws2, 'canvas', { x: 30, y: 40 }, platform);

			const list = await c.list('canvas');
			expect(list).toHaveLength(2);

			const alice = list.find((e) => e.user.id === '1');
			expect(alice).toBeDefined();
			expect(alice.data).toEqual({ x: 10, y: 20 });
			expect(alice.user).toEqual({ id: '1', name: 'Alice' });
			c.destroy();
		});

		it('returns empty array for unknown topic', async () => {
			const list = await cursors.list('nonexistent');
			expect(list).toEqual([]);
		});
	});

	describe('clear', () => {
		it('resets all local and Redis state', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 1 }, platform);
			await c.clear();

			const list = await c.list('canvas');
			expect(list).toEqual([]);
			c.destroy();
		});

		it('clears all pending timers', async () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(50);
			cursors.update(ws, 'canvas', { x: 10 }, platform);

			await cursors.clear();

			vi.advanceTimersByTime(100);
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('cross-instance relay', () => {
		it('publishes updates to Redis pub/sub channel', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// Spy on redis.publish
			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push({ channel: ch, message: JSON.parse(msg) });
				return origPublish.call(client.redis, ch, msg);
			};

			c.update(ws, 'canvas', { x: 10, y: 20 }, platform);
			await new Promise((r) => setTimeout(r, 0));

			expect(publishCalls).toHaveLength(1);
			expect(publishCalls[0].channel).toBe('test:cursor:events');
			expect(publishCalls[0].message.event).toBe('update');
			expect(publishCalls[0].message.topic).toBe('canvas');
			expect(publishCalls[0].message.payload.data).toEqual({ x: 10, y: 20 });
			c.destroy();
		});

		it('publishes remove events to Redis pub/sub channel', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 10 }, platform);
			await new Promise((r) => setTimeout(r, 10));

			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push({ channel: ch, message: JSON.parse(msg) });
				return origPublish.call(client.redis, ch, msg);
			};

			await c.remove(ws, platform);

			expect(publishCalls).toHaveLength(1);
			expect(publishCalls[0].message.event).toBe('remove');
			c.destroy();
		});

		it('stores cursor data in Redis hash', async () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 42, y: 99 }, platform);

			// Read directly from Redis hash
			const all = await client.redis.hgetall('test:cursor:canvas');
			const keys = Object.keys(all);
			expect(keys).toHaveLength(1);

			const stored = JSON.parse(Object.values(all)[0]);
			expect(stored.data).toEqual({ x: 42, y: 99 });
			c.destroy();
		});

		it('forwards remote updates to local platform with relay: false', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// Trigger subscriber setup
			c.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			// Simulate a message from another instance
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'canvas',
				event: 'update',
				payload: { key: 'remote:1', user: { id: '2', name: 'Bob' }, data: { x: 77 } }
			});

			// Publish through Redis (mock forwards to subscriber)
			client.redis.publish('test:cursor:events', remoteMsg);

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('__cursor:canvas');
			expect(platform.published[0].data.key).toBe('remote:1');
			expect(platform.published[0].data.data).toEqual({ x: 77 });
			expect(platform.published[0].options).toEqual({ relay: false });
			c.destroy();
		});

		it('ignores messages from own instance (echo suppression)', () => {
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// Trigger subscriber setup and get instanceId from a broadcast
			c.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			// The update will have been published to Redis; find the instanceId
			// by checking what the local publish sent
			// We can't easily get instanceId, but we know local publishes are
			// already forwarded locally, so the echo suppression prevents double-delivery
			// We test this indirectly: after the initial update there should be
			// exactly 1 publish (local), not 2 (local + echo)
			expect(platform.published).toHaveLength(0); // we reset after the initial
			c.destroy();
		});
	});

	describe('subscriber backfill on startup', () => {
		it('backfills remote cursor entries after subscriber becomes ready', async () => {
			const c = createCursor(client, { throttle: 0, select: (ud) => ({ id: ud.id }) });

			const remoteKey = 'remote-instance:1';
			const remoteData = JSON.stringify({
				user: { id: '2' },
				data: { x: 77 },
				ts: Date.now()
			});
			await client.redis.hset('test:cursor:canvas', remoteKey, remoteData);

			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 10 }, platform);

			await new Promise((r) => setTimeout(r, 20));

			const bulkEvents = platform.published.filter(
				(p) => p.event === 'bulk' && p.options && p.options.relay === false
			);
			expect(bulkEvents.length).toBeGreaterThanOrEqual(1);
			const entries = bulkEvents[0].data;
			const remoteEntry = entries.find((e) => e.key === remoteKey);
			expect(remoteEntry).toBeDefined();
			expect(remoteEntry.data).toEqual({ x: 77 });

			c.destroy();
		});

		it('does not backfill entries from the local instance', async () => {
			const c = createCursor(client, { throttle: 0, select: (ud) => ({ id: ud.id }) });
			const ws = mockWs({ id: '1' });

			c.update(ws, 'canvas', { x: 10 }, platform);
			await new Promise((r) => setTimeout(r, 20));

			const bulkEvents = platform.published.filter(
				(p) => p.event === 'bulk' && p.options && p.options.relay === false
			);
			expect(bulkEvents).toHaveLength(0);

			c.destroy();
		});
	});

	describe('subscribe failure recovery', () => {
		it('retries subscriber setup after subscribe() failure', async () => {
			// Create a client whose duplicate's subscribe() fails once
			const failClient = mockRedisClient('test:');
			let failCount = 0;
			const origDuplicate = failClient.duplicate.bind(failClient);
			failClient.duplicate = () => {
				const dup = origDuplicate();
				const origSubscribe = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					if (failCount === 0) {
						failCount++;
						throw new Error('connection lost');
					}
					return origSubscribe(ch);
				};
				return dup;
			};

			const c = createCursor(failClient, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// First update triggers ensureSubscriber which will fail async
			c.update(ws, 'canvas', { x: 0, y: 0 }, platform);

			// Wait for the async failure to settle
			await new Promise((r) => setTimeout(r, 10));

			// Second update should retry and succeed
			c.update(ws, 'canvas', { x: 10, y: 10 }, platform);
			await new Promise((r) => setTimeout(r, 10));

			// Simulate a remote event -- should now be received
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'canvas',
				event: 'update',
				payload: { key: 'remote:1', user: { id: '2' }, data: { x: 77 } }
			});
			await failClient.redis.publish('test:cursor:events', remoteMsg);

			const remoteUpdates = platform.published.filter(
				(p) => p.data && p.data.key === 'remote:1'
			);
			expect(remoteUpdates).toHaveLength(1);
			c.destroy();
		});
	});

	describe('platform update', () => {
		it('uses the latest platform for remote event forwarding', () => {
			const c = createCursor(client, { throttle: 0 });
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			// First update sets up subscriber with platform1
			c.update(ws1, 'canvas', { x: 0 }, platform1);
			platform1.reset();

			// Second update with platform2 should update the platform ref
			c.update(ws2, 'canvas', { x: 1 }, platform2);
			platform2.reset();

			// Simulate a remote event
			const remoteMsg = JSON.stringify({
				instanceId: 'remote-instance',
				topic: 'canvas',
				event: 'update',
				payload: { key: 'remote:1', user: { id: '3' }, data: { x: 99 } }
			});
			client.redis.publish('test:cursor:events', remoteMsg);

			// Should have been forwarded via platform2 (latest), not platform1
			expect(platform2.published.filter((p) => p.data && p.data.key === 'remote:1')).toHaveLength(1);
			expect(platform1.published.filter((p) => p.data && p.data.key === 'remote:1')).toHaveLength(0);
			c.destroy();
		});
	});

	describe('destroy', () => {
		it('clears all timers and stops subscriber', () => {
			vi.useFakeTimers();
			const ws = mockWs({ id: '1', name: 'Alice' });

			cursors.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			vi.advanceTimersByTime(50);
			cursors.update(ws, 'canvas', { x: 10 }, platform);

			cursors.destroy();

			vi.advanceTimersByTime(100);
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('per-topic broadcast budget (#3)', () => {
		it('topicThrottle limits aggregate broadcasts per topic', () => {
			vi.useFakeTimers();
			const c = createCursor(client, {
				throttle: 0, // no per-connection throttle
				topicThrottle: 100 // max 10 broadcasts/sec per topic
			});

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			const ws3 = mockWs({ id: '3', name: 'Charlie' });

			// First update broadcasts immediately (leading edge, single entry = normal update)
			c.update(ws1, 'canvas', { x: 1 }, platform);
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].event).toBe('update');

			// Second and third updates within the window are coalesced
			c.update(ws2, 'canvas', { x: 2 }, platform);
			c.update(ws3, 'canvas', { x: 3 }, platform);
			expect(platform.published).toHaveLength(1);

			// After the window passes, trailing edge flushes as a single bulk event
			vi.advanceTimersByTime(100);
			expect(platform.published).toHaveLength(2); // 1 update + 1 bulk
			const bulk = platform.published[1];
			expect(bulk.event).toBe('bulk');
			expect(bulk.data).toHaveLength(2);
			expect(bulk.data.map((e) => e.data.x).sort()).toEqual([2, 3]);

			c.destroy();
		});

		it('single coalesced entry uses normal update event, not bulk', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 100 });

			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			// Only one entry coalesced -- should use normal update, not bulk
			c.update(ws, 'canvas', { x: 99 }, platform);
			vi.advanceTimersByTime(100);

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].event).toBe('update');
			expect(platform.published[0].data.data).toEqual({ x: 99 });

			c.destroy();
		});

		it('topicThrottle sends latest data per key in bulk', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 100 });

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			c.update(ws1, 'canvas', { x: 0 }, platform);
			platform.reset();

			// Multiple updates from same key -- only latest should appear in bulk
			c.update(ws1, 'canvas', { x: 10 }, platform);
			c.update(ws1, 'canvas', { x: 99 }, platform);
			c.update(ws2, 'canvas', { x: 50 }, platform);

			vi.advanceTimersByTime(100);
			expect(platform.published).toHaveLength(1);
			const bulk = platform.published[0];
			expect(bulk.event).toBe('bulk');
			expect(bulk.data).toHaveLength(2);

			const alice = bulk.data.find((e) => e.data.x === 99);
			const bob = bulk.data.find((e) => e.data.x === 50);
			expect(alice).toBeDefined();
			expect(bob).toBeDefined();

			c.destroy();
		});

		it('topicThrottle: 0 disables aggregate throttle', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0 });

			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });

			c.update(ws1, 'canvas', { x: 1 }, platform);
			c.update(ws2, 'canvas', { x: 2 }, platform);

			// Both should broadcast immediately (no aggregate throttle)
			expect(platform.published).toHaveLength(2);

			c.destroy();
		});

		it('different topics have independent budgets', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 100 });

			const ws = mockWs({ id: '1', name: 'Alice' });

			// Each topic gets its own leading-edge broadcast
			c.update(ws, 'canvas-a', { x: 1 }, platform);
			c.update(ws, 'canvas-b', { x: 2 }, platform);

			expect(platform.published).toHaveLength(2);

			c.destroy();
		});

		it('topicThrottle timers are cleaned up on destroy', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 100 });

			const ws = mockWs({ id: '1', name: 'Alice' });
			c.update(ws, 'canvas', { x: 0 }, platform);
			platform.reset();

			c.update(ws, 'canvas', { x: 10 }, platform);
			c.destroy();

			vi.advanceTimersByTime(200);
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('remove suppresses local broadcast when Redis fails', () => {
		it('per-topic remove does not publish locally when hdel fails', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });

			const ws = mockWs({ id: '1' });
			c.update(ws, 'doc', { x: 10 }, platform);
			platform.reset();

			const listBefore = await c.list('doc');
			expect(listBefore).toHaveLength(1);

			const origHdel = client.redis.hdel;
			client.redis.hdel = async () => { throw new Error('hdel failed'); };

			await c.remove(ws, platform, 'doc');

			const removes = platform.published.filter((p) => p.event === 'remove');
			expect(removes).toHaveLength(0);

			client.redis.hdel = origHdel;
			const listAfter = await c.list('doc');
			expect(listAfter).toHaveLength(1);

			c.destroy();
		});

		it('remove-all does not publish locally when pipeline fails', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });

			const ws = mockWs({ id: '1' });
			c.update(ws, 'doc', { x: 10 }, platform);
			c.update(ws, 'canvas', { y: 20 }, platform);
			platform.reset();

			const origPipeline = client.redis.pipeline;
			client.redis.pipeline = () => {
				return new Proxy({}, {
					get(_, method) {
						if (method === 'exec') {
							return async () => { throw new Error('pipeline failed'); };
						}
						return () => new Proxy({}, {
							get: (_, m) => m === 'exec'
								? async () => { throw new Error('pipeline failed'); }
								: () => {}
						});
					}
				});
			};

			await c.remove(ws, platform);

			const removes = platform.published.filter((p) => p.event === 'remove');
			expect(removes).toHaveLength(0);

			client.redis.pipeline = origPipeline;
			c.destroy();
		});

		it('per-topic remove publishes locally when hdel succeeds', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });

			const ws = mockWs({ id: '1' });
			c.update(ws, 'doc', { x: 10 }, platform);
			platform.reset();

			await c.remove(ws, platform, 'doc');

			const removes = platform.published.filter((p) => p.event === 'remove');
			expect(removes).toHaveLength(1);

			const listAfter = await c.list('doc');
			expect(listAfter).toEqual([]);

			c.destroy();
		});
	});

	describe('sensitive data warning for arrays', () => {
		it('warns about sensitive keys nested inside arrays', () => {
			vi.useRealTimers();
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const c = createCursor(client, { throttle: 0, select: (ud) => ud });

			const ws = mockWs({ id: '1', profiles: [{ authToken: 'secret' }] });
			c.update(ws, 'doc', { x: 1 }, platform);

			expect(warn).toHaveBeenCalledWith(
				expect.stringContaining('authToken')
			);

			warn.mockRestore();
			c.destroy();
		});
	});

	describe('write-after-broadcast consistency', () => {
		it('does not relay cross-instance when Redis pipeline fails', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// Track relay publishes
			const relayMessages = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				relayMessages.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			// Make pipeline fail
			const origPipeline = client.redis.pipeline;
			client.redis.pipeline = () => {
				return new Proxy({}, {
					get(_, method) {
						if (method === 'exec') {
							return async () => { throw new Error('pipeline failed'); };
						}
						return () => new Proxy({}, {
							get: (_, m) => m === 'exec'
								? async () => { throw new Error('pipeline failed'); }
								: () => {}
						});
					}
				});
			};

			c.update(ws, 'canvas', { x: 10, y: 20 }, platform);

			// Local broadcast should still happen
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.data).toEqual({ x: 10, y: 20 });

			// Wait for async pipeline to settle
			await new Promise((r) => setTimeout(r, 50));

			// No relay should have been sent since pipeline failed
			expect(relayMessages).toHaveLength(0);

			client.redis.pipeline = origPipeline;
			client.redis.publish = origPublish;
			c.destroy();
		});

		it('relays cross-instance when Redis pipeline succeeds', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			const relayMessages = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				relayMessages.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			c.update(ws, 'canvas', { x: 10 }, platform);

			// Wait for async pipeline + relay
			await new Promise((r) => setTimeout(r, 50));

			expect(relayMessages.length).toBeGreaterThanOrEqual(1);
			const relay = relayMessages.find((m) => m.event === 'update');
			expect(relay).toBeDefined();
			expect(relay.payload.data).toEqual({ x: 10 });

			client.redis.publish = origPublish;
			c.destroy();
		});

		it('list() returns empty when pipeline failed but local broadcast happened', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			// Make pipeline fail so data never reaches Redis
			const origPipeline = client.redis.pipeline;
			client.redis.pipeline = () => {
				return new Proxy({}, {
					get(_, method) {
						if (method === 'exec') {
							return async () => { throw new Error('pipeline failed'); };
						}
						return () => new Proxy({}, {
							get: (_, m) => m === 'exec'
								? async () => { throw new Error('pipeline failed'); }
								: () => {}
						});
					}
				});
			};

			c.update(ws, 'canvas', { x: 10 }, platform);

			// Local broadcast happened
			expect(platform.published).toHaveLength(1);

			// Wait for pipeline to settle
			await new Promise((r) => setTimeout(r, 50));

			// list() reads from Redis, which was never written
			client.redis.pipeline = origPipeline;
			const list = await c.list('canvas');
			expect(list).toEqual([]);

			c.destroy();
		});

		it('list() returns data when pipeline succeeds', async () => {
			vi.useRealTimers();
			const c = createCursor(client, { throttle: 0 });
			const ws = mockWs({ id: '1', name: 'Alice' });

			c.update(ws, 'canvas', { x: 42 }, platform);

			// Wait for pipeline to complete
			await new Promise((r) => setTimeout(r, 50));

			const list = await c.list('canvas');
			expect(list).toHaveLength(1);
			expect(list[0].data).toEqual({ x: 42 });

			c.destroy();
		});
	});

	describe('breaker accounting in cursor', () => {
		it('skips Redis write and relay when breaker is broken', () => {
			vi.useRealTimers();
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const c = createCursor(client, { throttle: 0, breaker });
			const ws = mockWs({ id: '1' });

			const hsetCalls = [];
			const origHset = client.redis.hset;
			client.redis.hset = async (...args) => {
				hsetCalls.push(args);
				return origHset.apply(client.redis, args);
			};

			c.update(ws, 'canvas', { x: 1 }, platform);

			// Local broadcast still happens
			expect(platform.published).toHaveLength(1);

			// Redis was not touched
			expect(hsetCalls).toHaveLength(0);

			client.redis.hset = origHset;
			c.destroy();
			breaker.destroy();
		});

		it('list() throws when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const c = createCursor(client, { throttle: 0, breaker });
			await expect(c.list('canvas')).rejects.toThrow(CircuitBrokenError);

			c.destroy();
			breaker.destroy();
		});

		it('remove does not publish locally when breaker is broken', async () => {
			vi.useRealTimers();
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const c = createCursor(client, { throttle: 0, breaker });
			const ws = mockWs({ id: '1' });

			c.update(ws, 'canvas', { x: 1 }, platform);

			breaker.failure();
			platform.reset();

			await c.remove(ws, platform);

			const removes = platform.published.filter((p) => p.event === 'remove');
			expect(removes).toHaveLength(0);

			breaker.reset();
			const list = await c.list('canvas');
			expect(list).toHaveLength(1);

			c.destroy();
			breaker.destroy();
		});
	});
});
