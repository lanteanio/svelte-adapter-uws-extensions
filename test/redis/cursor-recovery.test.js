import { EventEmitter } from 'node:events';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const trackers = [];
const connections = [];
afterEach(() => {
	for (const cursor of trackers.splice(0)) cursor.destroy();
	for (const sub of connections.splice(0)) sub.emit('end');
	vi.useRealTimers();
});
function setup(options = {}) {
	const client = mockRedisClient();
	const duplicate = client.duplicate.bind(client);
	let sub;
	client.duplicate = () => {
		sub = duplicate();
		const events = new EventEmitter();
		sub.options = {};
		sub.status = 'ready';
		sub.on = events.on.bind(events);
		sub.emit = events.emit.bind(events);
		sub.removeListener = events.removeListener.bind(events);
		sub._listeners.set('message', (...args) => events.emit('message', ...args));
		connections.push(sub);
		queueMicrotask(() => sub.emit('ready'));
		return sub;
	};
	const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, ...options });
	trackers.push(cursor);
	return { cursor, client, platform: mockPlatform(), reconnect() { sub._subscribedChannels.clear(); sub.emit('ready'); } };
}

describe('cursor relay recovery', () => {
	it('recovers every watched topic once for multiple watchers without clock frames', async () => {
		const { cursor, client, platform, reconnect } = setup();
		const watchers = [mockWs({ id: 'one' }), mockWs({ id: 'two' }), mockWs({ id: 'three' })];
		await cursor.attach(watchers[0], 'board', platform);
		await cursor.attach(watchers[1], 'board', platform);
		await cursor.attach(watchers[2], 'other', platform);
		await client.redis.hset(client.key('cursor:{board}'), 'peer:1', JSON.stringify({ user: { id: 'peer' }, data: { x: 9 }, ts: Date.now() }));
		const reads = vi.spyOn(client.redis, 'hgetall');
		platform.reset();
		reconnect();
		await flush();
		expect(reads).toHaveBeenCalledTimes(2);
		for (const ws of watchers) expect(platform.sent.filter((frame) => frame.ws === ws).map((frame) => frame.event)).toEqual(['catalog', 'bulk']);
		for (const ws of watchers.slice(0, 2)) expect(platform.sent.find((frame) => frame.ws === ws && frame.event === 'catalog').data).toEqual([{ key: 'peer:1', user: { id: 'peer' } }]);
	});

	it('preserves live peers missing from the hash when a mover instance restores its relay', async () => {
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 10000 });
		const watcher = mockWs({ id: 'watcher' });
		await cursor.attach(watcher, 'board', platform);
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 1 }, platform);
		for (const [event, payload] of [
			['join', { key: 'peer:1', user: { id: 'peer' } }],
			['update', { key: 'peer:1', data: { x: 7 } }]
		]) await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event, payload }));
		expect(await client.redis.hgetall(client.key('cursor:{board}'))).toEqual({});
		platform.reset();
		reconnect();
		await flush();
		const catalog = platform.sent.find(frame => frame.ws === watcher && frame.event === 'catalog').data;
		expect(catalog.map(entry => entry.user.id).sort()).toEqual(['local', 'peer']);
		expect(platform.sent.find(frame => frame.ws === watcher && frame.event === 'bulk').data)
			.toContainEqual({ key: 'peer:1', data: { x: 7 } });
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event: 'update', payload: { key: 'peer:1', data: { x: 8 } } }));
		expect(platform.published.filter(frame => frame.event === 'update').at(-1).data).toEqual({ key: 'peer:1', data: { x: 8 } });
	});

	it('retains live removal tombstones across recovery of a stale hash', async () => {
		const { cursor, client, platform, reconnect } = setup();
		const watcher = mockWs({ id: 'watcher' });
		await cursor.attach(watcher, 'board', platform);
		await client.redis.hset(client.key('cursor:{board}'), 'peer:gone', JSON.stringify({ user: { id: 'gone' }, data: {}, ts: Date.now() }));
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event: 'remove', payload: { key: 'peer:gone' } }));
		platform.reset();
		reconnect();
		await flush();
		expect(platform.sent.find(frame => frame.ws === watcher && frame.event === 'catalog').data).toEqual([]);
	});

	it('supersedes a snapshot read begun before recovery instead of replacing the recovered roster with stale data', async () => {
		const { cursor, client, platform, reconnect } = setup();
		const watcher = mockWs({ id: 'watcher' });
		await cursor.attach(watcher, 'board', platform);
		let release;
		vi.spyOn(client.redis, 'hgetall').mockImplementationOnce(() => new Promise(resolve => { release = resolve; }));
		const snapshot = cursor.snapshot(watcher, 'board', platform);
		await flush();
		await client.redis.hset(client.key('cursor:{board}'), 'peer:new', JSON.stringify({ user: { id: 'new' }, data: { x: 9 }, ts: Date.now() }));
		platform.reset();
		reconnect();
		await flush();
		expect(platform.sent.filter(frame => frame.event === 'catalog').map(frame => frame.data))
			.toEqual([[{ key: 'peer:new', user: { id: 'new' } }]]);
		release({ 'peer:old': JSON.stringify({ user: { id: 'old' }, data: {}, ts: Date.now() }) });
		await snapshot;
		expect(platform.sent.filter(frame => frame.event === 'catalog').map(frame => frame.data))
			.toEqual([[{ key: 'peer:new', user: { id: 'new' } }]]);
		platform.reset();
		await cursor.snapshot(watcher, 'board', platform);
		expect(platform.sent.find(frame => frame.event === 'catalog').data)
			.toEqual([{ key: 'peer:new', user: { id: 'new' } }]);
	});

	it('retries a failed recovery read while the subscription stays open', async () => {
		const { cursor, client, platform, reconnect } = setup();
		await cursor.attach(mockWs({ id: 'watcher' }), 'board', platform);
		const reads = vi.spyOn(client.redis, 'hgetall');
		reads.mockRejectedValueOnce(new Error('unavailable'));
		platform.reset();
		reconnect();
		await flush();
		expect(platform.sent).toHaveLength(0);
		await new Promise((resolve) => setTimeout(resolve, 1100));
		expect(reads).toHaveBeenCalledTimes(2);
		expect(platform.sent.map((frame) => frame.event)).toEqual(['catalog', 'bulk']);
	});
	it('lets a relay publish finish the only breaker probe', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 1 });
		try {
			const { cursor, platform } = setup({ breaker, snapshotIntervalMs: 10000 });
			const ws = mockWs({ id: 'mover' });
			await cursor.attach(ws, 'board', platform);
			breaker.failure(new Error('unavailable'));
			await new Promise((resolve) => setTimeout(resolve, 10));
			expect(breaker.state).toBe('probing');
			cursor.update(ws, 'board', { x: 1 }, platform);
			await flush();
			expect(breaker.state).toBe('healthy');
			expect(() => breaker.guard()).not.toThrow();
		} finally { breaker.destroy(); }
	});
});
