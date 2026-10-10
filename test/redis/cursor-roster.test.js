import { describe, it, expect, afterEach } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const trackers = [];
afterEach(() => { for (const cursor of trackers.splice(0)) cursor.destroy(); });
function setup(options = {}) {
	const client = mockRedisClient();
	const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, ...options });
	trackers.push(cursor);
	const platform = mockPlatform();
	const ws = mockWs({ id: 'watcher' });
	const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'board', event, payload }));
	return { cursor, client, platform, ws, relay };
}

describe('cursor snapshot roster', () => {
	it('keeps a join that arrives before the persisted snapshot catches up', async () => {
		const { cursor, platform, ws, relay } = setup();
		await cursor.attach(ws, 'board', platform);
		await relay('join', { key: 'peer:1', user: { id: 'peer' } });
		await relay('update', { key: 'peer:1', data: { x: 7 } });
		platform.reset();
		await cursor.snapshot(ws, 'board', platform);
		expect(platform.sent.find((frame) => frame.event === 'catalog').data).toEqual([{ key: 'peer:1', user: { id: 'peer' } }]);
		expect(platform.sent.find((frame) => frame.event === 'bulk').data).toEqual([{ key: 'peer:1', data: { x: 7 } }]);
	});
	it('merges joins and removals arriving while a snapshot read is pending', async () => {
		const { cursor, client, platform, ws, relay } = setup();
		await cursor.attach(ws, 'board', platform);
		let release;
		const pending = new Promise((resolve) => { release = resolve; });
		client.redis.hgetall = () => pending;
		platform.reset();
		const snapshot = cursor.snapshot(ws, 'board', platform);
		await Promise.resolve(); await Promise.resolve();
		await relay('remove', { key: 'peer:gone' });
		await relay('join', { key: 'peer:new', user: { id: 'new' } });
		await relay('update', { key: 'peer:new', data: { x: 9 } });
		release({ 'peer:gone': JSON.stringify({ user: { id: 'gone' }, data: { x: 0 }, ts: Date.now() }) });
		await snapshot;
		expect(platform.sent.find((frame) => frame.event === 'catalog').data).toEqual([{ key: 'peer:new', user: { id: 'new' } }]);
	});
	it('includes a local mover before its first position flush', async () => {
		const { cursor, platform, ws } = setup({ topicThrottle: 100 });
		await cursor.attach(ws, 'board', platform);
		cursor.update(mockWs({ id: 'local' }), 'board', { x: 1 }, platform);
		platform.reset();
		await cursor.snapshot(ws, 'board', platform);
		expect(platform.sent.find((frame) => frame.event === 'catalog').data.map((entry) => entry.user)).toEqual([{ id: 'local' }]);
	});
	it('does not allocate state when a socket closes during authorization', async () => {
		const { cursor, platform, ws } = setup();
		let authorize;
		platform.checkSubscribe = () => new Promise((resolve) => { authorize = resolve; });
		const joining = cursor.hooks.subscribe(ws, '__cursor:board', { platform });
		ws.getBufferedAmount = () => { throw new Error('closed'); };
		authorize(null);
		await joining;
		expect(platform.sent).toHaveLength(0);
		cursor.hooks.message(ws, { data: { type: 'cursor', topic: 'board', data: { x: 7 } }, platform });
		expect(cursor.stats().activeTopicsTotal).toBe(0);
	});
});
