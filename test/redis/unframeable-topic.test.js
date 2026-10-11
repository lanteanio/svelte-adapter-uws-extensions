// A topic with a double quote, a backslash or a control character cannot be
// put into a frame: the adapter throws when it builds the envelope. Presence
// and cursor reject such a topic where it enters, and their timers keep going
// for every other topic, and report, when one topic's publish throws.

import { describe, it, expect, vi, afterEach } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { registeredRecord } from '../helpers/diagnostic-line.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const BAD = ['say "hi"', 'back\\slash', 'line\nbreak', 'tab\there', 'nul\u0000'];

async function until(check, timeoutMs = 2000) {
	const start = Date.now();
	while (!check()) {
		if (Date.now() - start > timeoutMs) throw new Error('condition never held');
		await sleep(5);
	}
}

/** A platform whose publish throws for one channel, quoting it, the way the adapter throws for a frame it cannot build. */
function platformFailingFor(channel, events) {
	const platform = mockPlatform();
	const publish = platform.publish.bind(platform);
	platform.publish = (topic, event, ...rest) => {
		if (topic === channel && (!events || events.includes(event))) throw new Error("cannot frame '" + topic + "'");
		return publish(topic, event, ...rest);
	};
	return platform;
}

/** Registered-failure lines for this event among the console.warn calls. */
function reports(warn, event) {
	return warn.mock.calls.map((c) => String(c[0])).filter((line) => line.includes('event=' + event + ' '));
}

afterEach(() => vi.restoreAllMocks());

describe('presence and an unframeable topic', () => {
	it('join() rejects it before selecting data or storing anything', async () => {
		const client = mockRedisClient('uf1:');
		const platform = mockPlatform();
		const select = vi.fn((u) => ({ id: u.id }));
		const presence = createPresence(client, { select });
		for (const topic of BAD) {
			const ws = mockWs({ id: 'alice' });
			await expect(presence.join(ws, topic, platform)).rejects.toThrow(TypeError);
			expect([...ws._topics]).toEqual([]);
		}
		const keys = [...client._hashes.keys()].filter((k) => k.startsWith('uf1:'));
		const online = presence.metrics().totalOnline;
		presence.destroy();
		expect(keys).toEqual([]);
		expect(online).toBe(0);
		expect(select).not.toHaveBeenCalled();
		expect(platform.published).toEqual([]);
		expect(platform.sent).toEqual([]);
	});

	it('sync() silently returns before authorization and subscribes nothing', async () => {
		const client = mockRedisClient('uf2:');
		const platform = mockPlatform();
		const presence = createPresence(client, { select: (u) => ({ id: u.id }) });
		const ws = mockWs({ id: 'observer' });
		const authorize = vi.spyOn(platform, 'checkSubscribe');
		for (const topic of BAD) await expect(presence.sync(ws, topic, platform)).resolves.toBeUndefined();
		presence.destroy();
		expect(authorize).not.toHaveBeenCalled();
		expect([...ws._topics]).toEqual([]);
		expect(platform.sent).toEqual([]);
	});

	it('continues flushing healthy diffs after an earlier topic fails', async () => {
		const client = mockRedisClient('uf-diff:');
		const platform = platformFailingFor('__presence:room-alpha', ['diff']);
		const presence = createPresence(client, { select: (u) => ({ id: u.id }), topicThrottle: 10000 });
		try {
			for (let round = 0; round < 2; round++) {
				await presence.join(mockWs({ id: 'alice-' + round }), 'room-alpha', platform);
				await presence.join(mockWs({ id: 'bob-' + round }), 'room-beta', platform);
				expect(() => presence.flushDiffs()).not.toThrow();
				const diff = platform.published.filter((frame) => frame.topic === '__presence:room-beta' && frame.event === 'diff').at(-1);
				expect(diff.data.joins['bob-' + round]).toEqual({ id: 'bob-' + round });
			}
		} finally { presence.destroy(); }
	});

	it('keeps beating and refreshing every TTL when one topic cannot be published, and reports it once', async () => {
		const client = mockRedisClient('uf3:');
		const platform = platformFailingFor('__presence:room-alpha', ['heartbeat']);
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const presence = createPresence(client, { select: (u) => ({ id: u.id }), heartbeat: 20 });
		// The failing topic first, so a throw there would end the loop.
		await presence.join(mockWs({ id: 'alice' }), 'room-alpha', platform);
		await presence.join(mockWs({ id: 'bob' }), 'room-beta', platform);
		const refreshed = vi.spyOn(client.redis, 'hpexpire');
		platform.reset();

		const beta = () => platform.published.filter((f) => f.topic === '__presence:room-beta' && f.event === 'heartbeat');
		await until(() => beta().length >= 3);
		const keys = new Set(refreshed.mock.calls.map((c) => c[0]));
		const lines = reports(warn, 'redis.presence.heartbeat.publish-failed');
		presence.destroy();

		expect(keys.has('uf3:presence:topic:{room-alpha}')).toBe(true);
		expect(keys.has('uf3:presence:topic:{room-beta}')).toBe(true);
		// Three failing heartbeats, one report.
		expect(lines).toHaveLength(1);
		expect(registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PRESENCE_HEARTBEAT_PUBLISH).attributes).toMatchObject({ failed: 1, topics: 2 });
		expect(lines[0]).not.toContain('room-alpha');
	});
});

describe('cursor and an unframeable topic', () => {
	it('update() rejects it without storing anything', () => {
		const client = mockRedisClient('uf4:');
		const platform = mockPlatform();
		const subscriber = vi.spyOn(client.redis, 'duplicate');
		const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		for (const topic of BAD) {
			expect(() => cursor.update(mockWs({ id: '1' }), topic, { x: 1 }, platform)).toThrow(TypeError);
		}
		for (const topic of ['', undefined, 7]) {
			expect(cursor.update(mockWs({ id: '1' }), topic, { x: 1 }, platform)).toBeUndefined();
		}
		for (const topic of BAD) {
			expect(cursor.update(mockWs({ id: '1' }), topic, { x: 1n }, platform)).toBeUndefined();
			expect(cursor.update(mockWs({ id: '1' }), topic, { text: 'x'.repeat(100000) }, platform)).toBeUndefined();
		}
		const active = cursor.stats().activeTopicsTotal;
		cursor.destroy();
		expect(active).toBe(0);
		expect(subscriber).not.toHaveBeenCalled();
		expect(platform.published).toEqual([]);
	});

	it('snapshot() silently rejects invalid names before authorization or retained state', async () => {
		const client = mockRedisClient('uf-snapshot:');
		const platform = mockPlatform();
		const authorize = vi.spyOn(platform, 'checkSubscribe');
		const subscriber = vi.spyOn(client.redis, 'duplicate');
		const cursor = createCursor(client);
		const ws = mockWs({ id: 'observer' });
		try {
			for (const topic of [...BAD, '__cursor:room', undefined, 7]) {
				await expect(cursor.snapshot(ws, topic, platform)).resolves.toBeUndefined();
			}
			expect(authorize).not.toHaveBeenCalled();
			expect(subscriber).not.toHaveBeenCalled();
			expect(cursor.stats().activeTopicsTotal).toBe(0);
			expect([...ws._topics]).toEqual([]);
			expect(platform.sent).toEqual([]);
		} finally { cursor.destroy(); }
	});

	it('reports a topic that keeps failing once, whatever phase the other topics flush on', async () => {
		vi.useFakeTimers();
		installFakeRuntimeClock();
		try {
			const client = mockRedisClient('uf7:');
			const platform = platformFailingFor('__cursor:room-alpha', ['update', 'bulk']);
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const cursor = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0 });
			const a = mockWs({ id: '1' });
			const b = mockWs({ id: '2' });
			// room-beta first moves half a cycle after room-alpha, so the two
			// flush on alternating ticks and every room-beta tick succeeds.
			cursor.update(a, 'room-alpha', { x: 0 }, platform);
			vi.advanceTimersByTime(8);
			cursor.update(b, 'room-beta', { x: 0 }, platform);
			for (let x = 1; x <= 20; x++) {
				cursor.update(a, 'room-alpha', { x }, platform);
				cursor.update(b, 'room-beta', { x }, platform);
				vi.advanceTimersByTime(8);
			}
			const beta = platform.published.filter((f) => f.topic === '__cursor:room-beta' && f.event !== 'join').length;
			const streak = reports(warn, 'redis.cursor.flush.failed').length;
			// A new streak after clear() is reported again.
			await cursor.clear();
			cursor.update(a, 'room-alpha', { x: 99 }, platform);
			vi.advanceTimersByTime(32);
			const afterClear = reports(warn, 'redis.cursor.flush.failed').length;
			cursor.destroy();
			expect(beta).toBeGreaterThan(5);
			expect(streak).toBe(1);
			expect(afterClear).toBe(2);
		} finally {
			releaseRuntimeClock();
			vi.useRealTimers();
		}
	});

	it('flushes the other topics when one topic cannot be published, and reports it once', async () => {
		const client = mockRedisClient('uf5:');
		const platform = platformFailingFor('__cursor:room-alpha', ['update', 'bulk']);
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const cursor = createCursor(client, { throttle: 0, topicThrottle: 5, snapshotIntervalMs: 0 });
		const a = mockWs({ id: '1' });
		const b = mockWs({ id: '2' });
		for (let x = 0; x < 3; x++) {
			cursor.update(a, 'room-alpha', { x }, platform);
			cursor.update(b, 'room-beta', { x }, platform);
			await until(() => platform.published.some((f) => f.topic === '__cursor:room-beta' && f.event !== 'join' && f.data?.data?.x === x));
		}
		const lines = reports(warn, 'redis.cursor.flush.failed');
		cursor.destroy();

		expect(lines).toHaveLength(1);
		expect(registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.CURSOR_FLUSH).attributes).toMatchObject({ failed: 1, flushed: 2 });
		expect(lines[0]).not.toContain('room-alpha');
	});

	it('does not keep a position that failed to flush, so it cannot block the topic', async () => {
		const client = mockRedisClient('uf6:');
		const platform = mockPlatform();
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		const cursor = createCursor(client, { throttle: 0, topicThrottle: 5, snapshotIntervalMs: 0 });
		// The snapshot write serializes the data, which a BigInt cannot be.
		cursor.update(mockWs({ id: '1' }), 'room', { x: 1n }, platform);
		await sleep(20);
		// A second connection's position on the same topic still goes out.
		cursor.update(mockWs({ id: '2' }), 'room', { x: 2 }, platform);
		await until(() => platform.published.some((f) => f.topic === '__cursor:room' && f.event !== 'join' && f.data?.data?.x === 2));
		cursor.destroy();
	});
});
