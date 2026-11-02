import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const trackers = [];
beforeEach(() => { vi.useFakeTimers(); installFakeRuntimeClock(); });
afterEach(() => {
	for (const tracker of trackers.splice(0)) tracker.destroy();
	releaseRuntimeClock();
	vi.useRealTimers();
	vi.restoreAllMocks();
});

async function setup(count, options = {}) {
	const client = mockRedisClient();
	const platform = mockPlatform();
	const tracker = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0, binary: false, ...options });
	trackers.push(tracker);
	const sockets = Array.from({ length: count }, (_, id) => mockWs({ id }));
	for (const socket of sockets) await tracker.attach(socket, 'room', platform);
	await vi.advanceTimersByTimeAsync(0);
	platform.reset();
	return { client, platform, tracker, sockets };
}

function positions(platform) {
	return platform.published.filter(frame => frame.topic === '__cursor:room' && ['update', 'bulk'].includes(frame.event))
		.flatMap(frame => frame.event === 'bulk' ? frame.data : [frame.data]).map(entry => entry.data.x);
}

describe('cursor mover admission', () => {
	it.each([undefined, null])('defaults %s to 64 movers and drains the remainder on the next tick', async maxMovers => {
		const { platform, tracker, sockets } = await setup(66, { maxMovers });
		for (let index = 0; index < sockets.length; index++) tracker.update(sockets[index], 'room', { x: index, y: 0 }, platform);
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual(Array.from({ length: 64 }, (_, index) => index));
		expect(tracker.stats().moversDeferred).toBe(2);
		await vi.advanceTimersByTimeAsync(16);
		expect(positions(platform)).toEqual(Array.from({ length: 66 }, (_, index) => index));
		expect(tracker.stats().moversDeferred).toBe(2);
	});

	it('preserves first arrival across local and peer movers while retaining their latest positions', async () => {
		const { client, platform, tracker, sockets } = await setup(2, { maxMovers: 2 });
		tracker.update(sockets[0], 'room', { x: 0, y: 0 }, platform);
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({
			instanceId: 'peer', topic: 'room', event: 'bulk', payload: [{ key: 'peer:1', data: { x: 10 } }, { key: 'peer:2', data: { x: 11 } }]
		}));
		tracker.update(sockets[1], 'room', { x: 1, y: 0 }, platform);
		tracker.update(sockets[0], 'room', { x: 2, y: 0 }, platform);
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([2, 10]);
		expect(tracker.stats().moversDeferred).toBe(2);
		await vi.advanceTimersByTimeAsync(16);
		expect(positions(platform)).toEqual([2, 10, 11, 1]);
	});

	it('discards a deferred peer position when the peer leaves before its next tick', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event, payload }));
		await relay('update', { key: 'peer:1', data: { x: 10 } });
		await vi.advanceTimersByTimeAsync(0);
		await relay('remove', { key: 'peer:1' });
		await vi.advanceTimersByTimeAsync(16);
		expect(positions(platform)).toEqual([0]);
	});

	it.each(['topic', 'all'])('keeps deferred peer positions when the last local mover leaves %s while an observer remains', async scope => {
		const { client, platform, tracker, sockets } = await setup(2, { maxMovers: 1 });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event: 'update', payload: { key: 'peer:1', data: { x: 10 } } }));
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		await tracker.remove(sockets[0], platform, scope === 'topic' ? 'room' : undefined);
		await vi.advanceTimersByTimeAsync(16);
		expect(positions(platform)).toEqual([0, 10]);
	});

	it('discards a deferred position when snapshot confirmation proves the peer departed', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1, topicThrottle: 1000, snapshotIntervalMs: 50 });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event, payload }));
		await relay('join', { key: 'peer:1', user: { id: 'peer' } });
		await relay('update', { key: 'peer:1', data: { x: 10 } });
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		await tracker.snapshot(sockets[0], 'room', platform);
		await vi.advanceTimersByTimeAsync(101);
		expect(platform.published.some(frame => frame.event === 'remove' && frame.data.key === 'peer:1')).toBe(true);
		await vi.advanceTimersByTimeAsync(1000);
		expect(positions(platform)).toEqual([0]);
	});

	it('allows Infinity to deliver every pending mover in one tick', async () => {
		const { platform, tracker, sockets } = await setup(66, { maxMovers: Infinity });
		for (let index = 0; index < sockets.length; index++) tracker.update(sockets[index], 'room', { x: index }, platform);
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toHaveLength(66);
		expect(tracker.stats().moversDeferred).toBe(0);
	});

	it('erases deferred positions without broadcasting or persisting them on a later tick', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		const victim = mockWs({ id: 'erased' });
		await tracker.attach(victim, 'room', platform);
		tracker.update(victim, 'room', { x: 10 }, platform);
		await vi.advanceTimersByTimeAsync(0);
		const victimKey = (await tracker.list('room')).find(entry => entry.user.id === 'erased').key;
		platform.reset();
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		tracker.update(victim, 'room', { x: 11 }, platform);
		await vi.advanceTimersByTimeAsync(16);
		expect(positions(platform)).toEqual([0]);
		expect(await tracker.purgeUser(null, 'erased')).toBe(1);
		expect(await client.redis.hget(client.key('cursor:{room}'), victimKey)).toBeNull();
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0]);
		expect(await client.redis.hget(client.key('cursor:{room}'), victimKey)).toBeNull();
	});

	it('erases a peer position waiting in the combined delivery queue', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		await client.redis.hset(client.key('cursor:{room}'), 'peer:1', JSON.stringify({ user: { id: 'erased' }, data: { x: 10 }, ts: Date.now() }));
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event: 'update', payload: { key: 'peer:1', data: { x: 11 } } }));
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		expect(await tracker.purgeUser(null, 'erased')).toBe(1);
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0]);
		expect(await client.redis.hget(client.key('cursor:{room}'), 'peer:1')).toBeNull();
	});

	it('erases a peer known from JOIN before its first durable snapshot exists', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event, payload }));
		await relay('join', { key: 'peer:1', user: { id: 'erased' } });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		await relay('update', { key: 'peer:1', data: { x: 11 } });
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		expect(await client.redis.hget(client.key('cursor:{room}'), 'peer:1')).toBeNull();
		expect(await tracker.purgeUser(null, 'erased')).toBe(1);
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0]);
		expect(await client.redis.hget(client.key('cursor:{room}'), 'peer:1')).toBeNull();
	});

	it('leaves a different peer identity and unknown update metadata untouched', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: 'room', event, payload }));
		await relay('join', { key: 'peer:other', user: { id: 'other' } });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		await relay('update', { key: 'peer:other', data: { x: 11 } });
		await relay('update', { key: 'erased', data: { x: 12 } });
		await vi.advanceTimersByTimeAsync(0);
		expect(await tracker.purgeUser(null, 'erased')).toBe(0);
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0, 11, 12]);
	});

	it('restricts known peer erasure to the requested tenant', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		const topic = '@t/other/room'; await tracker.attach(sockets[0], topic, platform);
		const relay = (event, payload) => client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic, event, payload }));
		await relay('join', { key: 'peer:1', user: { id: 'erased' } });
		tracker.update(sockets[0], topic, { x: 0 }, platform); await relay('update', { key: 'peer:1', data: { x: 11 } });
		await vi.advanceTimersByTimeAsync(0);
		expect(await tracker.purgeUser(null, 'erased')).toBe(0);
		expect(await tracker.purgeUser('wanted', 'erased')).toBe(0);
		await vi.advanceTimersByTimeAsync(16);
		expect(platform.published.some(frame => frame.topic === '__cursor:' + topic && frame.event === 'update' && frame.data.data.x === 11)).toBe(true);
	});

	it.each(['clear', 'destroy'])('drops deferred delivery on %s', async operation => {
		const { client, platform, tracker, sockets } = await setup(2, { maxMovers: 1 });
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		tracker.update(sockets[1], 'room', { x: 11 }, platform);
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		await tracker[operation]();
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0]);
		expect(Object.values(await client.redis.hgetall(client.key('cursor:{room}'))).some(raw => JSON.parse(raw).data.x === 11)).toBe(false);
	});

	it('erases a local mover deferred before its first persisted position exists', async () => {
		const { client, platform, tracker, sockets } = await setup(1, { maxMovers: 1 });
		const victim = mockWs({ id: 'erased' });
		await tracker.attach(victim, 'room', platform);
		tracker.update(sockets[0], 'room', { x: 0 }, platform);
		tracker.update(victim, 'room', { x: 11 }, platform);
		await vi.advanceTimersByTimeAsync(0);
		expect(positions(platform)).toEqual([0]);
		expect(await tracker.purgeUser(null, 'erased')).toBe(1);
		await vi.advanceTimersByTimeAsync(32);
		expect(positions(platform)).toEqual([0]);
		expect((await tracker.list('room')).some(entry => entry.user.id === 'erased')).toBe(false);
		expect(Object.values(await client.redis.hgetall(client.key('cursor:{room}'))).some(raw => JSON.parse(raw).user.id === 'erased')).toBe(false);
	});

	it('leaves topicThrottle: 0 synchronous and warns once about an explicit finite bound', async () => {
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const { platform, tracker, sockets } = await setup(3, { maxMovers: 1, topicThrottle: 0 });
		for (let index = 0; index < sockets.length; index++) tracker.update(sockets[index], 'room', { x: index }, platform);
		expect(positions(platform)).toEqual([0, 1, 2]);
		expect(tracker.stats().moversDeferred).toBe(0);
		expect(warn.mock.calls.filter(call => String(call[0]).includes('maxMovers has no effect'))).toHaveLength(1);
	});

	it.each([0, -1, 1.5, NaN, '2', true])('refuses maxMovers=%s with the shared contract error', value => {
		expect(() => createCursor(mockRedisClient(), { maxMovers: value })).toThrow('cursor: maxMovers must be a positive integer or Infinity');
	});
});
