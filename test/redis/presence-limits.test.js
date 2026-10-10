import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { MAX_PROJECTION_DEPTH } from '../../src/shared/sensitive-names.js';

const trackers = [];
afterEach(() => { for (const t of trackers.splice(0)) t.destroy(); vi.useRealTimers(); });
async function setup(options = {}) {
	const client = mockRedisClient();
	const tracker = createPresence(client, { binary: false, ...options });
	trackers.push(tracker);
	const platform = mockPlatform();
	const ws = mockWs({ id: 'alice' });
	await tracker.join(ws, 'room', platform);
	tracker.flushDiffs();
	platform.reset();
	return { client, tracker, platform, ws };
}

describe('presence resource limits', () => {
	for (const name of ['maxFieldsBytes', 'maxTotalFieldsBytes', 'maxTopicsPerConnection']) {
		for (const value of [0, -1, 1.5, Infinity, '10']) {
			it(`rejects ${name}=${value}`, () => {
				expect(() => createPresence(mockRedisClient(), { [name]: value })).toThrow(`presence: ${name} must be a positive integer`);
			});
		}
	}
	for (const value of [-1, Infinity, '10']) {
		it(`rejects topicThrottle=${value}`, () => expect(() => createPresence(mockRedisClient(), { topicThrottle: value })).toThrow('presence: topicThrottle must be a non-negative number'));
	}
	for (const fromClient of [false, true]) {
		it(`drops oversized and over-deep updates before writes (${fromClient ? 'wire' : 'direct'})`, async () => {
			const { client, tracker, platform, ws } = await setup({ clientUpdateFields: ['value'], maxFieldsBytes: 1024 });
			const publish = vi.spyOn(client.redis, 'publish');
			const before = await client.redis.hgetall(client.key('presence:topic:{room}'));
			let nested = {};
			for (let i = 0; i <= MAX_PROJECTION_DEPTH; i++) nested = { x: nested };
			for (const fields of [{ value: 'x'.repeat(1024) }, { value: nested }]) {
				if (fromClient) {
					expect(tracker.hooks.message(ws, { msg: { type: 'presence-update', topic: 'room', fields }, data: new Uint8Array(), platform })).toBe(true);
					await Promise.resolve();
				} else await tracker.update(ws, 'room', fields, platform);
			}
			tracker.flushDiffs();
			expect(platform.published).toEqual([]);
			expect(publish).not.toHaveBeenCalled();
			expect(await client.redis.hgetall(client.key('presence:topic:{room}'))).toEqual(before);
			await tracker.update(ws, 'room', { value: 'ok' }, platform);
			expect(await tracker.list('room')).toEqual([{ id: 'alice', value: 'ok' }]);
		});
	}
	it('charges names and framing and rejects the whole cumulative update', async () => {
		const { tracker, platform, ws, client } = await setup({ maxTotalFieldsBytes: 15 });
		await tracker.update(ws, 'room', { color: 'red' }, platform); // 14 bytes
		tracker.flushDiffs();
		platform.reset();
		const publish = vi.spyOn(client.redis, 'publish');
		await tracker.update(ws, 'room', { color: 'blue', x: 1 }, platform);
		tracker.flushDiffs();
		expect(await tracker.list('room')).toEqual([{ id: 'alice', color: 'red' }]);
		expect(platform.published).toEqual([]);
		expect(publish).not.toHaveBeenCalled();
		await tracker.update(ws, 'room', { color: 'x' }, platform);
		expect(await tracker.list('room')).toEqual([{ id: 'alice', color: 'x' }]);
	});
	it('limits a connection before selecting or opening another topic', async () => {
		const select = vi.fn((u) => ({ id: u.id }));
		const { tracker, platform, ws } = await setup({ maxTopicsPerConnection: 1, select });
		await tracker.join(ws, 'other', platform);
		expect(select).toHaveBeenCalledTimes(1);
		expect(await tracker.count('other')).toBe(0);
		await tracker.leave(ws, platform, 'room');
		await tracker.join(ws, 'other', platform);
		expect(await tracker.count('other')).toBe(1);
	});
	it('uses the default 16 ms throttle between automatically flushed changes', async () => {
		vi.useFakeTimers();
		const { tracker, platform, ws } = await setup();
		await tracker.update(ws, 'room', { color: 'red' }, platform);
		await vi.advanceTimersByTimeAsync(15);
		expect(platform.published).toEqual([]);
		await vi.advanceTimersByTimeAsync(1);
		expect(platform.published.filter((f) => f.event === 'diff')).toHaveLength(1);
		await tracker.update(ws, 'room', { color: 'blue' }, platform);
		await vi.advanceTimersByTimeAsync(15);
		expect(platform.published.filter((f) => f.event === 'diff')).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1);
		expect(platform.published.filter((f) => f.event === 'diff')).toHaveLength(2);
	});
});
