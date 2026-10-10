import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';

const cleanup = [];
afterEach(() => { for (const x of cleanup.splice(0)) x.destroy(); vi.useRealTimers(); });
async function probing() {
	vi.useFakeTimers();
	const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 10 });
	cleanup.push(breaker);
	breaker.failure(new Error('offline'));
	await vi.advanceTimersByTimeAsync(10);
	expect(breaker.state).toBe('probing');
	return breaker;
}
describe('presence breaker admission', () => {
	for (const denial of [undefined, false, 'DENIED', 'throw']) {
		it(`does not consume a probe for authorization denial ${denial}`, async () => {
			const breaker = await probing();
			const tracker = createPresence(mockRedisClient(), { breaker });
			cleanup.push(tracker);
			const platform = denial === undefined ? {} : { checkSubscribe: async () => { if (denial === 'throw') throw new Error('denied'); return denial; } };
			expect(await tracker.sync(mockWs({}), 'room', platform)).toBe(denial === 'DENIED' ? 'DENIED' : 'FORBIDDEN');
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
		});
	}
	it('returns a probe when the socket dies after the subscriber opens', async () => {
		const breaker = await probing();
		const tracker = createPresence(mockRedisClient(), { breaker });
		cleanup.push(tracker);
		const ws = mockWs({ id: 'alice' });
		ws.getBufferedAmount = () => { throw new Error('closed'); };
		await expect(tracker.join(ws, 'room', mockPlatform())).rejects.toMatchObject({ code: 'WS_CLOSED' });
		expect(breaker.state).toBe('probing');
		expect(() => breaker.guard()).not.toThrow();
	});
	it('returns a probe when the close hook runs during the join write', async () => {
		const breaker = await probing();
		const client = mockRedisClient();
		const tracker = createPresence(client, { breaker });
		cleanup.push(tracker);
		const ws = mockWs({ id: 'alice' });
		const platform = mockPlatform();
		const original = client.redis.eval.bind(client.redis);
		let closed = false;
		client.redis.eval = async (...args) => {
			const result = await original(...args);
			if (!closed && args[0].includes('PRESENCE_JOIN')) {
				closed = true;
				await tracker.leave(ws, platform);
			}
			return result;
		};
		await expect(tracker.join(ws, 'room', platform)).rejects.toMatchObject({ code: 'WS_CLOSED' });
		expect(breaker.state).toBe('probing');
		expect(() => breaker.guard()).not.toThrow();
	});
	it('records a backend feature-probe failure and admits a later retry', async () => {
		const breaker = await probing();
		const client = mockRedisClient();
		const tracker = createPresence(client, { breaker });
		cleanup.push(tracker);
		vi.spyOn(client.redis, 'info').mockRejectedValueOnce(new Error('offline'));
		await expect(tracker.join(mockWs({ id: 'alice' }), 'room', mockPlatform())).rejects.toThrow('offline');
		expect(breaker.state).toBe('broken');
		await vi.advanceTimersByTimeAsync(10);
		await tracker.join(mockWs({ id: 'alice' }), 'room', mockPlatform());
		expect(breaker.state).toBe('healthy');
	});
});
