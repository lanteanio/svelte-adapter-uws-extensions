import { describe, it, expect, afterEach } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const trackers = [];
afterEach(() => { for (const tracker of trackers.splice(0)) tracker.destroy(); });
function setup(options = {}) {
	const client = mockRedisClient();
	const tracker = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, ...options });
	trackers.push(tracker);
	return { tracker, client, platform: mockPlatform(), ws: mockWs({ id: 'mover' }) };
}

describe('cursor data limits', () => {
	it.each([0, -1, 1.5, NaN, Infinity, '8192'])('rejects invalid cap %s', (maxDataBytes) => {
		expect(() => setup({ maxDataBytes })).toThrow('maxDataBytes must be a positive integer');
	});
	it('counts UTF-8 bytes and accepts the exact limit', async () => {
		const { tracker, platform, ws } = setup({ maxDataBytes: 6 });
		tracker.update(ws, 'board', '\u00e9\u00e9', platform);
		tracker.update(ws, 'board', '\u00e9\u00e9x', platform);
		expect(platform.published.filter((frame) => frame.event === 'update').map((frame) => frame.data.data)).toEqual(['\u00e9\u00e9']);
		expect((await tracker.list('board'))[0].data).toBe('\u00e9\u00e9');
	});
	it('drops oversized, cyclic, unserializable and deep data before allocating state', async () => {
		const { tracker, platform, ws } = setup();
		const cyclic = {}; cyclic.self = cyclic;
		let deep = {}; for (let i = 0; i < 70; i++) deep = { child: deep };
		for (const data of ['x'.repeat(8192), cyclic, 1n, () => {}, deep]) {
			expect(() => tracker.update(ws, 'board', data, platform)).not.toThrow();
		}
		expect(platform.published).toHaveLength(0);
		expect(platform.sent).toHaveLength(0);
		expect(tracker.stats().activeTopicsTotal).toBe(0);
		expect(await tracker.list('board')).toEqual([]);
	});
});
