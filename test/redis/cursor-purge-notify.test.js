import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCursor } from '../../src/redis/cursor.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';

const trackers = [], clients = [];
beforeEach(() => { vi.useFakeTimers(); installFakeRuntimeClock(); });
afterEach(async () => {
	for (const tracker of trackers.splice(0)) tracker.destroy();
	for (const client of clients.splice(0)) await client.quit();
	releaseRuntimeClock(); vi.useRealTimers();
});

describe('cursor erasure notifications', () => {
	it.each(['local', 'peer', 'deferred'])('notifies local and remote observers once when erasing a %s mover', async origin => {
		const client = mockRedisClient(); clients.push(client);
		const make = () => { const t = createCursor(client, { binary: false, throttle: 0, topicThrottle: 16, maxMovers: 1, snapshotIntervalMs: 0 }); trackers.push(t); return t; };
		const a = make(), b = make();
		const p = mockPlatform(), q = mockPlatform();
		await a.attach(mockWs({ id: 'observer-a' }), 'room', p);
		await b.attach(mockWs({ id: 'observer-b' }), 'room', q);
		const victim = mockWs({ id: 'erased' });
		const source = origin === 'peer' ? b : a, output = origin === 'peer' ? q : p;
		await source.attach(victim, 'room', output);
		if (origin === 'deferred') {
			const other = mockWs({ id: 'other' }); await source.attach(other, 'room', output);
			source.update(other, 'room', { x: 0 }, output);
		}
		source.update(victim, 'room', { x: 10 }, output);
		await vi.advanceTimersByTimeAsync(0);
		const key = output.sent.find(frame => frame.ws === victim && frame.event === 'you').data.key;
		p.reset(); q.reset();
		expect(await a.purgeUser(null, 'erased')).toBe(1);
		await vi.advanceTimersByTimeAsync(1);
		for (const platform of [p, q]) expect(platform.published.filter(frame => frame.event === 'remove' && frame.data.key === key)).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(32);
		for (const platform of [p, q]) expect(platform.published.some(frame => frame.event === 'update' && frame.data.key === key)).toBe(false);
	});

	it('does not announce a durable erasure that Redis refused', async () => {
		const client = mockRedisClient(); clients.push(client);
		const t = createCursor(client, { binary: false, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 }); trackers.push(t);
		const p = mockPlatform(), victim = mockWs({ id: 'erased' });
		await t.attach(victim, 'room', p); t.update(victim, 'room', { x: 10 }, p); await vi.advanceTimersByTimeAsync(0); p.reset();
		vi.spyOn(client.redis, 'hdel').mockRejectedValue(new Error('denied'));
		expect(await t.purgeUser(null, 'erased')).toBe(0);
		await vi.advanceTimersByTimeAsync(1);
		expect(p.published.filter(frame => frame.event === 'remove')).toHaveLength(0);
	});
});
