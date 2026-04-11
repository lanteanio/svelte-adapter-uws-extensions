import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit } from '../../redis/ratelimit.js';

function mockWs(userData = {}) {
	return { getUserData: () => userData };
}

describe('redis ratelimit', () => {
	let client;
	let limiter;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
		limiter = createRateLimit(client, { points: 5, interval: 1000 });
	});

	describe('createRateLimit', () => {
		it('returns a limiter with the expected API', () => {
			expect(typeof limiter.consume).toBe('function');
			expect(typeof limiter.reset).toBe('function');
			expect(typeof limiter.ban).toBe('function');
			expect(typeof limiter.unban).toBe('function');
			expect(typeof limiter.clear).toBe('function');
		});

		it('throws on missing options', () => {
			expect(() => createRateLimit(client)).toThrow('options object is required');
		});

		it('throws on non-positive points', () => {
			expect(() => createRateLimit(client, { points: 0, interval: 1000 })).toThrow('positive integer');
			expect(() => createRateLimit(client, { points: -1, interval: 1000 })).toThrow('positive integer');
			expect(() => createRateLimit(client, { points: 1.5, interval: 1000 })).toThrow('positive integer');
		});

		it('throws on non-positive interval', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 0 })).toThrow('positive number');
			expect(() => createRateLimit(client, { points: 5, interval: -100 })).toThrow('positive number');
		});

		it('throws on negative blockDuration', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 1000, blockDuration: -1 })).toThrow('non-negative');
		});

		it('throws on invalid keyBy', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 1000, keyBy: 'bad' })).toThrow('keyBy');
		});
	});

	describe('key versioning', () => {
		it('uses versioned key prefix to isolate Lua script versions', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws);

			const allKeys = [...client._hashes.keys()];
			const rlKeys = allKeys.filter((k) => k.includes('ratelimit'));
			expect(rlKeys).toHaveLength(1);
			expect(rlKeys[0]).toMatch(/^test:v\d+:ratelimit:1\.2\.3\.4$/);
		});

		it('clear() only scans versioned keys', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws);

			// Plant an unversioned key that should NOT be cleared
			client._hashes.set('test:ratelimit:old', new Map([['points', '5']]));

			await limiter.clear();

			const allKeys = [...client._hashes.keys()];
			const rlKeys = allKeys.filter((k) => k.includes('ratelimit'));
			// The old unversioned key should survive
			expect(rlKeys).toEqual(['test:ratelimit:old']);
		});
	});

	describe('consume - basic token bucket', () => {
		it('first consume is allowed and decrements remaining', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const result = await limiter.consume(ws);

			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
			expect(result.resetMs).toBeGreaterThan(0);
		});

		it('consuming all points succeeds', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) {
				expect((await limiter.consume(ws)).allowed).toBe(true);
			}
			expect((await limiter.consume(ws)).remaining).toBe(0);
		});

		it('exceeding points is rejected', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) await limiter.consume(ws);

			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(false);
		});

		it('custom cost deducts multiple points', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const result = await limiter.consume(ws, 3);

			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(2);
		});

		it('cost exceeding remaining is rejected', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws, 4); // 1 left

			const result = await limiter.consume(ws, 2);
			expect(result.allowed).toBe(false);
		});
	});

	describe('consume - cost validation', () => {
		it('throws on negative cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, -2)).rejects.toThrow('positive integer');
		});

		it('throws on zero cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 0)).rejects.toThrow('positive integer');
		});

		it('throws on fractional cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 1.5)).rejects.toThrow('positive integer');
		});

		it('throws on non-number cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 'abc')).rejects.toThrow('positive integer');
		});
	});

	describe('consume - refill', () => {
		it('refills after interval passes', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			for (let i = 0; i < 5; i++) await limiter.consume(ws);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			Date.now.mockReturnValue(now + 1001);
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
		});

		it('partial interval does not refill', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			for (let i = 0; i < 5; i++) await limiter.consume(ws);

			Date.now.mockReturnValue(now + 500);
			expect((await limiter.consume(ws)).allowed).toBe(false);
		});
	});

	describe('consume - auto-ban', () => {
		it('bans when points exhausted and blockDuration set', async () => {
			const rl = createRateLimit(client, { points: 2, interval: 1000, blockDuration: 5000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws);
			const result = await rl.consume(ws);

			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBe(5000);
		});

		it('ban expires after blockDuration', async () => {
			const rl = createRateLimit(client, { points: 2, interval: 1000, blockDuration: 5000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws);
			await rl.consume(ws); // triggers ban

			Date.now.mockReturnValue(now + 5001);
			const result = await rl.consume(ws);
			expect(result.allowed).toBe(true);
		});

		it('during ban, resetMs reflects ban expiry', async () => {
			const rl = createRateLimit(client, { points: 1, interval: 1000, blockDuration: 3000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws); // triggers ban

			Date.now.mockReturnValue(now + 1000);
			const result = await rl.consume(ws);
			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBe(2000);
		});
	});

	describe('keyBy modes', () => {
		it('ip mode: same IP shares bucket', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '1.2.3.4' });

			await limiter.consume(ws1, 3);
			const result = await limiter.consume(ws2, 1);
			expect(result.remaining).toBe(1);
		});

		it('ip mode: different IPs get separate buckets', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '5.6.7.8' });

			await limiter.consume(ws1, 5);
			expect((await limiter.consume(ws1)).allowed).toBe(false);
			expect((await limiter.consume(ws2)).allowed).toBe(true);
		});

		it('connection mode: each ws gets its own bucket', async () => {
			const rl = createRateLimit(client, { points: 3, interval: 1000, keyBy: 'connection' });
			const ws1 = mockWs({});
			const ws2 = mockWs({});

			await rl.consume(ws1, 3);
			expect((await rl.consume(ws1)).allowed).toBe(false);
			expect((await rl.consume(ws2)).allowed).toBe(true);
		});

		it('custom function: uses return value as key', async () => {
			const rl = createRateLimit(client, {
				points: 3,
				interval: 1000,
				keyBy: (ws) => ws.getUserData().room
			});
			const ws1 = mockWs({ room: 'A' });
			const ws2 = mockWs({ room: 'A' });
			const ws3 = mockWs({ room: 'B' });

			await rl.consume(ws1, 3);
			expect((await rl.consume(ws2)).allowed).toBe(false);
			expect((await rl.consume(ws3)).allowed).toBe(true);
		});
	});

	describe('reset / ban / unban / clear', () => {
		it('reset clears a key bucket', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) await limiter.consume(ws);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			await limiter.reset('1.2.3.4');
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
		});

		it('ban makes consume return false', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.ban('1.2.3.4', 5000);

			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBeGreaterThan(0);
		});

		it('unban allows consume again', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws); // 4 remaining
			await limiter.ban('1.2.3.4', 60000);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			await limiter.unban('1.2.3.4');
			expect((await limiter.consume(ws)).allowed).toBe(true);
		});

		it('operations on unknown keys are safe', async () => {
			await limiter.reset('nope');
			await limiter.ban('nope');
			await limiter.unban('nope');
			// Should not throw
		});

		it('clear resets all state', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '5.6.7.8' });
			await limiter.consume(ws1, 5);
			await limiter.consume(ws2, 5);

			await limiter.clear();

			const r1 = await limiter.consume(ws1);
			expect(r1.allowed).toBe(true);
			expect(r1.remaining).toBe(4);
			expect((await limiter.consume(ws2)).allowed).toBe(true);
		});
	});
});
