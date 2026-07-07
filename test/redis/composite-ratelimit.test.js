// Composite limiter peek(): a read-only, all-dimension check that reports the
// verdict a consume would return without writing to any dimension bucket and
// without moving any counter. (The composite consume behavior itself is covered
// in ratelimit-emergency.test.js; this file focuses on peek.)

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createCompositeRateLimit } from '../../src/redis/ratelimit.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

function mockWs(userData = {}) {
	return { getUserData: () => userData };
}

const DIMS = {
	account: { points: 2, interval: 60000, keyBy: (ws) => ws.getUserData().user },
	ip: { points: 3, interval: 60000, keyBy: 'ip' }
};

describe('composite ratelimit peek', () => {
	let client;
	let wallMs;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
		// Freeze the clock so `peek` and the following `consume` compute resetMs
		// against the same `now` (a real-clock tick between them would differ by 1ms).
		wallMs = 1_000_000;
		setRuntimeEnv({ clock: { now: () => wallMs, monotonic: () => wallMs, wallEpoch: () => wallMs } });
	});

	afterEach(() => {
		resetRuntimeEnv();
	});

	it('reports the same shape as consume without writing anything', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS });
		const ws = mockWs({ user: 'alice', ip: '9.9.9.9' });
		const peeked = await composite.peek(ws);
		expect(peeked).toMatchObject({ allowed: true, tripped: null, remaining: { account: 1, ip: 2 } });
		// No dimension bucket was created.
		expect([...client._hashes.keys()].filter((k) => k.includes('ratelimitc'))).toEqual([]);
	});

	it('verdict equals what the next consume returns', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS });
		const ws = mockWs({ user: 'bob', ip: '8.8.8.8' });
		await composite.consume(ws); // account 1, ip 2 left
		const peeked = await composite.peek(ws);
		const consumed = await composite.consume(ws);
		expect(peeked).toEqual(consumed);
	});

	it('does not mutate existing dimension buckets', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS });
		const ws = mockWs({ user: 'carol', ip: '7.7.7.7' });
		await composite.consume(ws);
		const snapshot = [...client._hashes.entries()]
			.filter(([k]) => k.includes('ratelimitc'))
			.map(([k, v]) => [k, new Map(v)]);
		await composite.peek(ws);
		for (const [k, before] of snapshot) {
			const after = client._hashes.get(k);
			expect(after.get('points')).toBe(before.get('points'));
			expect(after.get('bannedUntil')).toBe(before.get('bannedUntil'));
		}
	});

	it('names the tripped dimension without consuming from any bucket', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS });
		const ws = mockWs({ user: 'dave', ip: '6.6.6.6' });
		await composite.consume(ws);
		await composite.consume(ws); // account (2) now exhausted
		const peeked = await composite.peek(ws);
		expect(peeked.allowed).toBe(false);
		expect(peeked.tripped).toBe('account');
		// ip still holds what the two consumes left; the peek spent nothing.
		expect(peeked.remaining.ip).toBe(1);
		// A follow-up consume still finds ip at 1 (peek consumed nothing).
		const consumed = await composite.consume(ws);
		expect(consumed.remaining.ip).toBe(1);
	});

	it('reports a dimension ban without lifting or writing it', async () => {
		const composite = createCompositeRateLimit(client, {
			dimensions: {
				account: { points: 1, interval: 1000, blockDuration: 60000, keyBy: (ws) => ws.getUserData().user },
				ip: { points: 100, interval: 1000, keyBy: 'ip' }
			}
		});
		const ws = mockWs({ user: 'mallory', ip: '5.5.5.5' });
		await composite.consume(ws);
		await composite.consume(ws); // trips account -> auto-ban
		const peeked = await composite.peek(ws);
		expect(peeked).toMatchObject({ allowed: false, tripped: 'account' });
		expect(peeked.resetMs).toBeGreaterThan(0);
	});

	it('is tenant-scoped', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS, tenant: (ws) => ws.getUserData().org });
		await composite.consume(mockWs({ user: 'u', ip: '1.2.3.4', org: 'acme' }));
		await composite.consume(mockWs({ user: 'u', ip: '1.2.3.4', org: 'acme' })); // acme account exhausted
		expect((await composite.peek(mockWs({ user: 'u', ip: '1.2.3.4', org: 'acme' }))).tripped).toBe('account');
		// A different tenant on the same keys is untouched.
		expect((await composite.peek(mockWs({ user: 'u', ip: '1.2.3.4', org: 'other' }))).allowed).toBe(true);
	});

	it('does not move the composite counters', async () => {
		const metrics = createMetrics();
		const composite = createCompositeRateLimit(client, { dimensions: DIMS, metrics });
		const ws = mockWs({ user: 'frank', ip: '2.2.2.2' });
		await composite.consume(ws); // allowed_total 1
		await composite.peek(ws);    // must not move allowed/denied
		const out = metrics.serialize();
		expect(out).toContain('ratelimit_composite_allowed_total 1');
		expect(out).not.toContain('ratelimit_composite_allowed_total 2');
	});

	it('throws on an invalid cost', async () => {
		const composite = createCompositeRateLimit(client, { dimensions: DIMS });
		await expect(composite.peek(mockWs({ user: 'u', ip: '1.1.1.1' }), 0)).rejects.toThrow('positive integer');
	});

	describe('floor peek while the store is down', () => {
		it('decides read-only on the in-process floor without mutating it', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS, localFloorOnStorageFailure: true });
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			const ws = mockWs({ user: 'erin', ip: '4.4.4.4' });
			// Consume twice on the floor to exhaust the account budget (2).
			expect((await composite.consume(ws)).allowed).toBe(true);
			expect((await composite.consume(ws)).allowed).toBe(true);
			// A read-only peek reports the tripped account WITHOUT spending.
			const peeked = await composite.peek(ws);
			expect(peeked.allowed).toBe(false);
			expect(peeked.tripped).toBe('account');
			// Peeking again yields the same verdict (no mutation happened).
			const again = await composite.peek(ws);
			expect(again.allowed).toBe(false);
			expect(again.tripped).toBe('account');
		});

		it('rejects to the caller without a floor', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			await expect(composite.peek(mockWs({ user: 'x', ip: '3.3.3.3' }))).rejects.toThrow('down');
		});
	});
});
