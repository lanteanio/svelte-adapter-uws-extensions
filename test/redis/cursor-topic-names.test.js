import { afterEach, describe, expect, it } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const trackers = [];
afterEach(() => { for (const t of trackers.splice(0)) t.destroy(); });
function cursorOf(options) {
	const c = createCursor(mockRedisClient(), options);
	trackers.push(c);
	return c;
}

describe('cursor maxTopicLength', () => {
	it('defaults to 256 and drops a longer update topic', async () => {
		const cursor = cursorOf();
		const platform = mockPlatform();
		cursor.update(mockWs({ id: 'a' }), 'x'.repeat(257), { x: 1 }, platform);
		cursor.update(mockWs({ id: 'b' }), 'y'.repeat(256), { x: 1 }, platform);
		await new Promise((r) => setTimeout(r, 50));
		expect(await cursor.list('x'.repeat(257))).toEqual([]);
		expect(await cursor.list('y'.repeat(256))).toHaveLength(1);
	});

	it('honours a configured cap', async () => {
		const cursor = cursorOf({ maxTopicLength: 4 });
		const platform = mockPlatform();
		cursor.update(mockWs({ id: 'a' }), 'room1', { x: 1 }, platform);
		cursor.update(mockWs({ id: 'b' }), 'room', { x: 1 }, platform);
		await new Promise((r) => setTimeout(r, 50));
		expect(await cursor.list('room1')).toEqual([]);
		expect(await cursor.list('room')).toHaveLength(1);
	});

	it.each([0, -1, 1.5, NaN, '256'])('rejects maxTopicLength %s at construction', (value) => {
		expect(() => createCursor(mockRedisClient(), { maxTopicLength: value })).toThrow('maxTopicLength must be a positive integer');
	});
});

describe('cursor topic validation runs before authorization', () => {
	const invalid = ['', 'x'.repeat(257), '__cursor:room', 'say "hi"', 'back\\slash', 'ctl\u0001'];

	it.each(invalid)('attach(%j) throws TypeError without consulting the platform', async (topic) => {
		const cursor = cursorOf();
		const platform = mockPlatform();
		const ws = mockWs({ id: 'a' });
		await expect(cursor.attach(ws, topic, platform)).rejects.toBeInstanceOf(TypeError);
		expect(platform.checkedSubscribe).toHaveLength(0);
		expect(ws.isSubscribed('__cursor:' + topic)).toBe(false);
	});

	it('attach rejects a non-string topic', async () => {
		const cursor = cursorOf();
		await expect(cursor.attach(mockWs({ id: 'a' }), 42, mockPlatform())).rejects.toBeInstanceOf(TypeError);
	});

	it.each(invalid)('hooks.subscribe(__cursor:%j) refuses with INVALID_TOPIC before authorizing', async (topic) => {
		const cursor = cursorOf();
		const platform = mockPlatform();
		expect(await cursor.hooks.subscribe(mockWs({ id: 'a' }), '__cursor:' + topic, { platform })).toBe('INVALID_TOPIC');
		expect(platform.checkedSubscribe).toHaveLength(0);
	});

	it('a valid name still authorizes and attaches', async () => {
		const cursor = cursorOf();
		const platform = mockPlatform();
		const ws = mockWs({ id: 'a' });
		await cursor.attach(ws, 'board', platform);
		expect(platform.checkedSubscribe.map((c) => c.topic)).toEqual(['board']);
		expect(ws.isSubscribed('__cursor:board')).toBe(true);
	});
});

describe('presence hooks.subscribe topic validation', () => {
	it.each(['', '__presence:room', 'say "hi"'])('refuses __presence:%j with INVALID_TOPIC before authorizing', async (topic) => {
		const presence = createPresence(mockRedisClient(), { key: 'id' });
		trackers.push(presence);
		const platform = mockPlatform();
		expect(await presence.hooks.subscribe(mockWs({ id: 'a' }), '__presence:' + topic, { platform })).toBe('INVALID_TOPIC');
		expect(platform.checkedSubscribe).toHaveLength(0);
	});
});
