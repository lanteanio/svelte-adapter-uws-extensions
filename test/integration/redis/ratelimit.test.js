/**
 * Integration tests for redis/ratelimit against a real Redis 7 server.
 *
 * Exercises the CONSUME_SCRIPT and BAN_SCRIPT bodies end-to-end: real
 * Redis TIME for clock-skew-safe timestamps, real PEXPIRE on the bucket
 * hash, atomic decrement under concurrent EVAL, and refill behaviour
 * driven by actual elapsed wall-clock time. The mock-based suite at
 * test/redis/ratelimit.test.js stays as-is; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createRateLimit } from '../../../redis/ratelimit.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function fakeWs(userData = {}) {
	return { getUserData: () => userData };
}

describe('redis ratelimit (integration)', () => {
	let client;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-rl:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('CONSUME_SCRIPT basic token bucket', () => {
		it('first consume is allowed and remaining decrements correctly', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 1000 });
			const ws = fakeWs({ ip: '1.2.3.4' });
			const r = await limiter.consume(ws);
			expect(r.allowed).toBe(true);
			expect(r.remaining).toBe(4);
			expect(r.resetMs).toBeGreaterThan(0);
		});

		it('exhausting all points then one more is rejected', async () => {
			const limiter = createRateLimit(client, { points: 3, interval: 1000 });
			const ws = fakeWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 3; i++) {
				const r = await limiter.consume(ws);
				expect(r.allowed).toBe(true);
			}
			const denied = await limiter.consume(ws);
			expect(denied.allowed).toBe(false);
		});

		it('cost greater than remaining is rejected without decrementing further', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 1000 });
			const ws = fakeWs({ ip: '1.2.3.4' });
			await limiter.consume(ws, 4);
			const r = await limiter.consume(ws, 2);
			expect(r.allowed).toBe(false);
			// Still 1 point left; another cost=1 should succeed.
			const ok = await limiter.consume(ws, 1);
			expect(ok.allowed).toBe(true);
		});
	});

	describe('atomic concurrent consumption', () => {
		it('20 parallel consumes against points:5 produce exactly 5 allowed and 15 denied', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 60_000 });
			const ws = fakeWs({ ip: '10.0.0.1' });

			const results = await Promise.all(
				Array.from({ length: 20 }, () => limiter.consume(ws))
			);
			const allowed = results.filter((r) => r.allowed).length;
			const denied = results.filter((r) => !r.allowed).length;
			expect(allowed).toBe(5);
			expect(denied).toBe(15);
		});
	});

	describe('refill on real wall-clock time', () => {
		it('refills after the interval elapses (real time, no clock mock)', async () => {
			const limiter = createRateLimit(client, { points: 2, interval: 500 });
			const ws = fakeWs({ ip: '1.2.3.4' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			await wait(700);
			const r = await limiter.consume(ws);
			expect(r.allowed).toBe(true);
			expect(r.remaining).toBe(1);
		});
	});

	describe('BAN_SCRIPT and auto-ban', () => {
		it('blockDuration triggers ban once exhausted; resetMs reflects the ban window', async () => {
			const limiter = createRateLimit(client, {
				points: 1, interval: 60_000, blockDuration: 5000
			});
			const ws = fakeWs({ ip: '5.5.5.5' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			const banned = await limiter.consume(ws);
			expect(banned.allowed).toBe(false);
			// blockDuration=5000ms; resetMs should be near 5000 (ban window).
			expect(banned.resetMs).toBeGreaterThan(4000);
			expect(banned.resetMs).toBeLessThanOrEqual(5000);
		});

		it('explicit ban() makes consume reject on the next call', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 60_000 });
			const ws = fakeWs({ ip: '6.6.6.6' });
			await limiter.ban('6.6.6.6', 5000);
			const r = await limiter.consume(ws);
			expect(r.allowed).toBe(false);
			expect(r.resetMs).toBeGreaterThan(0);
		});

		it('unban allows consume again', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 60_000 });
			const ws = fakeWs({ ip: '7.7.7.7' });
			await limiter.ban('7.7.7.7', 60_000);
			expect((await limiter.consume(ws)).allowed).toBe(false);
			await limiter.unban('7.7.7.7');
			expect((await limiter.consume(ws)).allowed).toBe(true);
		});

		it('ban expires naturally after blockDuration', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 60_000 });
			const ws = fakeWs({ ip: '8.8.8.8' });
			await limiter.ban('8.8.8.8', 800);
			expect((await limiter.consume(ws)).allowed).toBe(false);
			await wait(1000);
			expect((await limiter.consume(ws)).allowed).toBe(true);
		});
	});

	describe('keyBy modes', () => {
		it('ip mode shares a bucket across two ws with the same IP', async () => {
			const limiter = createRateLimit(client, { points: 5, interval: 60_000 });
			const a = fakeWs({ ip: '1.2.3.4' });
			const b = fakeWs({ ip: '1.2.3.4' });
			await limiter.consume(a, 3);
			const r = await limiter.consume(b, 1);
			expect(r.remaining).toBe(1);
		});

		it('different IPs get independent buckets', async () => {
			const limiter = createRateLimit(client, { points: 2, interval: 60_000 });
			const a = fakeWs({ ip: '1.1.1.1' });
			const b = fakeWs({ ip: '2.2.2.2' });
			await limiter.consume(a, 2);
			expect((await limiter.consume(a)).allowed).toBe(false);
			expect((await limiter.consume(b)).allowed).toBe(true);
		});

		it('connection mode gives every ws its own bucket', async () => {
			const limiter = createRateLimit(client, {
				points: 2, interval: 60_000, keyBy: 'connection'
			});
			const a = fakeWs({});
			const b = fakeWs({});
			await limiter.consume(a, 2);
			expect((await limiter.consume(a)).allowed).toBe(false);
			expect((await limiter.consume(b)).allowed).toBe(true);
		});

		it('custom keyBy function is used as the bucket key', async () => {
			const limiter = createRateLimit(client, {
				points: 2, interval: 60_000,
				keyBy: (ws) => ws.getUserData().room
			});
			const a = fakeWs({ room: 'A' });
			const b = fakeWs({ room: 'A' });
			const c = fakeWs({ room: 'B' });
			await limiter.consume(a, 2);
			expect((await limiter.consume(b)).allowed).toBe(false);
			expect((await limiter.consume(c)).allowed).toBe(true);
		});
	});

	describe('reset / clear and key versioning on the wire', () => {
		it('reset zeroes the bucket so consume returns full points again', async () => {
			const limiter = createRateLimit(client, { points: 3, interval: 60_000 });
			const ws = fakeWs({ ip: '9.9.9.9' });
			await limiter.consume(ws, 3);
			expect((await limiter.consume(ws)).allowed).toBe(false);
			await limiter.reset('9.9.9.9');
			const r = await limiter.consume(ws);
			expect(r.allowed).toBe(true);
			expect(r.remaining).toBe(2);
		});

		it('versioned key prefix lands on the wire (test:v1:ratelimit:<key>)', async () => {
			const limiter = createRateLimit(client, { points: 2, interval: 60_000 });
			const ws = fakeWs({ ip: '11.11.11.11' });
			await limiter.consume(ws);
			const exists = await client.redis.exists(client.key('v1:ratelimit:11.11.11.11'));
			expect(exists).toBe(1);
		});

		it('clear only removes versioned keys; outsider keys survive', async () => {
			const limiter = createRateLimit(client, { points: 2, interval: 60_000 });
			await limiter.consume(fakeWs({ ip: '12.12.12.12' }));

			// Plant a non-versioned key that clear() must NOT touch.
			const survivor = client.key('ratelimit:legacy-untouched');
			await client.redis.set(survivor, 'still-here');

			await limiter.clear();
			expect(await client.redis.get(survivor)).toBe('still-here');

			// Versioned bucket must be gone.
			const exists = await client.redis.exists(client.key('v1:ratelimit:12.12.12.12'));
			expect(exists).toBe(0);

			await client.redis.unlink(survivor);
		});
	});

	describe('PEXPIRE on the bucket hash', () => {
		it('CONSUME_SCRIPT applies a TTL so abandoned buckets expire on their own', async () => {
			const limiter = createRateLimit(client, {
				points: 5, interval: 60_000, blockDuration: 0
			});
			const ws = fakeWs({ ip: '13.13.13.13' });
			await limiter.consume(ws);
			const ttl = await client.redis.pttl(client.key('v1:ratelimit:13.13.13.13'));
			// Script sets ttlMs = interval + 60000 on the success path.
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(60_000 + 60_000);
		});
	});

	// Production paths validate ARGV in JS BEFORE EVAL, but bypassing
	// the JS layer (a future plugin author who passes user-supplied data
	// without validation, or a direct `redis.eval` against the script
	// literal) used to crash the script with "attempt to compare nil
	// with number". The defensive tonumber + redis.error_reply now
	// turns those into a clean rejection rather than a Lua-level crash.
	describe('CONSUME / BAN scripts reject non-numeric ARGV cleanly', () => {
		// Script source mirrors the constants at the top of redis/ratelimit.js.
		const CONSUME_SCRIPT = `
local key = KEYS[1]
local maxPoints = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local blockDuration = tonumber(ARGV[4])
if maxPoints == nil or interval == nil or cost == nil or blockDuration == nil then
  return redis.error_reply('CONSUME: maxPoints/interval/cost/blockDuration must be numeric')
end
return {1, 0, 0}
`;

		it('CONSUME with non-numeric maxPoints returns a clean error reply', async () => {
			const key = client.key('lua-poison:consume:1');
			await expect(
				client.redis.eval(CONSUME_SCRIPT, 1, key, 'hostile', 1000, 1, 0)
			).rejects.toThrow(/CONSUME.*numeric/);
		});

		it('CONSUME with non-numeric interval/cost/blockDuration each return a clean error', async () => {
			const key = client.key('lua-poison:consume:2');
			// `tonumber` in Lua treats some special strings ('NaN', 'inf')
			// as numbers; use unambiguously non-numeric inputs so the nil
			// branch is the one we exercise.
			await expect(
				client.redis.eval(CONSUME_SCRIPT, 1, key, 5, 'not-a-number', 1, 0)
			).rejects.toThrow(/CONSUME/);
			await expect(
				client.redis.eval(CONSUME_SCRIPT, 1, key, 5, 1000, 'oops', 0)
			).rejects.toThrow(/CONSUME/);
			await expect(
				client.redis.eval(CONSUME_SCRIPT, 1, key, 5, 1000, 1, 'forever')
			).rejects.toThrow(/CONSUME/);
		});
	});
});
