import { EventEmitter } from 'node:events';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { WS_SUBSCRIPTIONS } from '../../src/shared/ws-subscriptions.js';
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
	resetRuntimeEnv();
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
	return { cursor, client, platform: mockPlatform(), pause() { sub._subscribedChannels.clear(); }, reconnect() { sub._subscribedChannels.clear(); sub.emit('ready'); } };
}

describe('cursor relay recovery', () => {
	function virtualClock() {
		vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
		setRuntimeEnv({ clock: { now: () => Date.now(), monotonic: () => Date.now() } });
	}
	const peer = (client, event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event, payload }));
	const peerRow = () => JSON.stringify({ user: { id: 'peer' }, data: { x: 7 }, ts: Date.now() });
	const removedPeer = platform => platform.published.some(frame => frame.event === 'remove' && frame.data.key === 'peer:1');

	it('removes a peer whose departure was lost during the relay gap and excludes it from later snapshots', async () => {
		virtualClock();
		const { cursor, client, platform, pause, reconnect } = setup({ snapshotIntervalMs: 20 });
		await cursor.attach(mockWs({ id: 'watcher' }), 'board', platform);
		await peer(client, 'join', { key: 'peer:1', user: { id: 'peer' } });
		await client.redis.hset(client.key('cursor:{board}'), 'peer:1', peerRow());
		pause();
		await client.redis.hdel(client.key('cursor:{board}'), 'peer:1');
		await peer(client, 'remove', { key: 'peer:1' });
		platform.reset();
		reconnect();
		await flush();
		await vi.advanceTimersByTimeAsync(41);
		expect(removedPeer(platform)).toBe(true);
		platform.reset();
		await cursor.attach(mockWs({ id: 'new-watcher' }), 'board', platform);
		expect(platform.sent.filter(frame => frame.event === 'catalog').flatMap(frame => frame.data)).toEqual([]);
	});

	it('keeps a peer whose delayed snapshot arrives during the confirmation window', async () => {
		virtualClock();
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 20 });
		const watcher = mockWs({ id: 'watcher' });
		await cursor.attach(watcher, 'board', platform);
		await peer(client, 'join', { key: 'peer:1', user: { id: 'peer' } });
		reconnect();
		await flush();
		await client.redis.hset(client.key('cursor:{board}'), 'peer:1', peerRow());
		await vi.advanceTimersByTimeAsync(41);
		expect(removedPeer(platform)).toBe(false);
		await cursor.snapshot(watcher, 'board', platform);
		expect(platform.sent.filter(frame => frame.event === 'catalog').at(-1).data).toEqual([{ key: 'peer:1', user: { id: 'peer' } }]);
	});
	it('does not spend a breaker probe when no provisional peer remains for confirmation', async () => {
		virtualClock();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 10 });
		try {
			const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 20, breaker });
			const watcher = mockWs({ id: 'watcher' });
			await cursor.attach(watcher, 'board', platform);
			await peer(client, 'join', { key: 'peer:1', user: { id: 'peer' } });
			reconnect();
			await flush();
			await client.redis.hset(client.key('cursor:{board}'), 'peer:1', peerRow());
			await cursor.snapshot(watcher, 'board', platform);
			const read = vi.spyOn(client.redis, 'hgetall');
			const guard = vi.spyOn(breaker, 'guard');
			breaker.failure(new Error('unavailable'));
			await vi.advanceTimersByTimeAsync(11);
			expect(breaker.state).toBe('probing');
			await vi.advanceTimersByTimeAsync(30);
			expect(read).not.toHaveBeenCalled();
			expect(guard).not.toHaveBeenCalled();
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
			expect(() => breaker.guard()).toThrow();
		} finally { breaker.destroy(); }
	});

	it('does not remove a peer that moves while its confirmation read is pending', async () => {
		virtualClock();
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 20 });
		await cursor.attach(mockWs({ id: 'watcher' }), 'board', platform);
		await peer(client, 'join', { key: 'peer:1', user: { id: 'peer' } });
		reconnect();
		await flush();
		let release;
		vi.spyOn(client.redis, 'hgetall').mockImplementationOnce(() => new Promise(resolve => { release = resolve; }));
		await vi.advanceTimersByTimeAsync(41);
		expect(release).toBeTypeOf('function');
		await peer(client, 'update', { key: 'peer:1', data: { x: 8 } });
		release({});
		await flush();
		await vi.advanceTimersByTimeAsync(1);
		expect(removedPeer(platform)).toBe(false);
		await client.redis.hset(client.key('cursor:{board}'), 'peer:1', peerRow());
		await vi.advanceTimersByTimeAsync(41);
		expect(removedPeer(platform)).toBe(false);
	});

	it('retries confirmation after Redis refuses a read without deleting the peer early', async () => {
		virtualClock();
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 20 });
		await cursor.attach(mockWs({ id: 'watcher' }), 'board', platform);
		await peer(client, 'join', { key: 'peer:1', user: { id: 'peer' } });
		reconnect();
		await flush();
		vi.spyOn(client.redis, 'hgetall').mockRejectedValueOnce(new Error('unavailable'));
		await vi.advanceTimersByTimeAsync(41);
		expect(removedPeer(platform)).toBe(false);
		await vi.advanceTimersByTimeAsync(1001);
		expect(removedPeer(platform)).toBe(true);
	});

	it.each(['subscribe', 'breaker'])('restores local snapshots when the first subscription succeeds after %s failure', async failure => {
		virtualClock();
		let blocked = failure === 'breaker';
		const breaker = { get state() { return blocked ? 'broken' : 'healthy'; }, guard() { if (blocked) throw new Error('unavailable'); }, success() {}, failure() {} };
		const { cursor, client, platform } = setup({ snapshotIntervalMs: 0, breaker });
		if (failure === 'subscribe') {
			const duplicate = client.duplicate;
			let first = true;
			client.duplicate = () => {
				const sub = duplicate();
				if (first) { first = false; sub.subscribe = async () => { throw new Error('unavailable'); }; }
				return sub;
			};
			vi.spyOn(client.redis, 'hset').mockRejectedValueOnce(new Error('unavailable'));
		}
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 3 }, platform);
		await flush();
		expect(await client.redis.hgetall(client.key('cursor:{board}'))).toEqual({});
		blocked = false;
		await vi.advanceTimersByTimeAsync(1001);
		await flush();
		expect(Object.values(await client.redis.hgetall(client.key('cursor:{board}'))).map(value => JSON.parse(value).data)).toEqual([{ x: 3 }]);
	});
	it.each([0, 20])('retries a refused restoration write without new movement with snapshot interval %s', async snapshotIntervalMs => {
		virtualClock();
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs });
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 5 }, platform);
		await flush();
		await vi.advanceTimersByTimeAsync(21);
		await client.redis.del(client.key('cursor:{board}'));
		const read = vi.spyOn(client.redis, 'hgetall');
		const write = vi.spyOn(client.redis, 'hset').mockRejectedValueOnce(new Error('unavailable'));
		reconnect();
		await flush();
		expect(read).toHaveBeenCalled();
		expect(write).toHaveBeenCalledTimes(1);
		expect(await client.redis.hgetall(client.key('cursor:{board}'))).toEqual({});
		await vi.advanceTimersByTimeAsync(1001);
		await flush();
		expect(Object.values(await client.redis.hgetall(client.key('cursor:{board}'))).map(value => JSON.parse(value).data)).toEqual([{ x: 5 }]);
	});

	it('restores local snapshots after the breaker delays a reconnect recovery', async () => {
		virtualClock();
		let blocked = false;
		const breaker = { get state() { return blocked ? 'broken' : 'healthy'; }, guard() { if (blocked) throw new Error('unavailable'); }, success() {}, failure() {} };
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 0, breaker });
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 4 }, platform);
		await flush();
		await client.redis.del(client.key('cursor:{board}'));
		blocked = true;
		reconnect();
		await flush();
		expect(await client.redis.hgetall(client.key('cursor:{board}'))).toEqual({});
		blocked = false;
		await vi.advanceTimersByTimeAsync(1001);
		await flush();
		expect(Object.values(await client.redis.hgetall(client.key('cursor:{board}'))).map(value => JSON.parse(value).data)).toEqual([{ x: 4 }]);
	});

	it('rewrites local movers and announces their identities when their own relay restores', async () => {
		virtualClock();
		const { cursor, client, platform, reconnect } = setup({ snapshotIntervalMs: 20 });
		const mover = mockWs({ id: 'local' });
		await cursor.attach(mover, 'board', platform);
		cursor.update(mover, 'board', { x: 2 }, platform);
		await vi.advanceTimersByTimeAsync(21);
		await client.redis.del(client.key('cursor:{board}'));
		const publishes = vi.spyOn(client.redis, 'publish');
		reconnect();
		await flush();
		await vi.advanceTimersByTimeAsync(21);
		const rows = Object.values(await client.redis.hgetall(client.key('cursor:{board}'))).map(value => JSON.parse(value));
		expect(rows).toEqual([{ user: { id: 'local' }, data: { x: 2 }, ts: expect.any(Number) }]);
		expect(publishes.mock.calls.map(([, value]) => JSON.parse(value)).some(frame => frame.event === 'join' && frame.payload.user.id === 'local')).toBe(true);
	});

	it('announces an idle mover again before its first resumed update even below the movement threshold', async () => {
		virtualClock();
		const { cursor, platform } = setup({ ttl: 1, minMove: 10 });
		const mover = mockWs({ id: 'local' });
		await cursor.attach(mover, 'board', platform);
		cursor.update(mover, 'board', { x: 1, y: 1 }, platform);
		await vi.advanceTimersByTimeAsync(1001);
		platform.reset();
		cursor.update(mover, 'board', { x: 2, y: 1 }, platform);
		expect(platform.published.map(frame => frame.event)).toEqual(['join', 'update']);
	});

	it('recovers a watcher subscribed through the connection registry without granting observer membership', async () => {
		const { cursor, client, platform, reconnect } = setup();
		const watcher = mockWs({ id: 'watcher', [WS_SUBSCRIPTIONS]: new Set(['__cursor:board']) });
		await cursor.snapshot(watcher, 'board', platform);
		await client.redis.hset(client.key('cursor:{board}'), 'peer:1', peerRow());
		platform.reset();
		reconnect();
		await flush();
		expect(platform.sent.map(frame => frame.event)).toEqual(['catalog', 'bulk']);
		expect(platform.sent[0].data).toEqual([{ key: 'peer:1', user: { id: 'peer' } }]);
		watcher.getUserData()[WS_SUBSCRIPTIONS].clear();
		platform.reset();
		reconnect();
		await flush();
		expect(platform.sent).toEqual([]);
	});

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
