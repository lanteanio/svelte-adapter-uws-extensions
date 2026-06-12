/**
 * Lua parity guard: the in-memory Redis double's mirrored-JS evaluators must
 * agree with a real Redis `EVAL` of the same script on identical inputs. This is
 * the anti-drift oracle for the double - a mirrored-JS reimplementation can
 * silently diverge from the Lua it mirrors (it did once: the rate-limit path
 * read a raw wall clock instead of the script's `redis.call('TIME')`), and the
 * deterministic unit tests cannot catch that on their own. Runs against the real
 * Redis the integration harness starts; the double's clock is pinned to the real
 * server's TIME so both evaluate the same instant.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../shared/runtime.js';
import { mockRedisClient } from '../../../testing/mock-redis.js';
import { CONSUME_SCRIPT } from '../../../redis/token-bucket-script.js';

describe('mock-redis Lua parity against real Redis (integration)', () => {
	let client;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-parity:' });
	});

	beforeEach(async () => {
		await resetBackendKeys(client);
	});

	afterEach(() => resetRuntimeEnv());

	afterAll(async () => {
		await client.quit();
	});

	// Pin the double's clock to the real server's current TIME so the two evaluate
	// against the same `now`; the RNG sub-ms jitter is zeroed for a clean compare.
	async function pinDoubleToRealClock() {
		const t = await client.redis.time();
		const serverMs = Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
		setRuntimeEnv({ clock: { wallEpoch: () => serverMs, now: () => serverMs }, rng: { u32: () => 0 } }, { force: true });
		return serverMs;
	}

	// allowed + remaining must match exactly; the time-derived field (resetMs /
	// ban-remaining) can differ by the real round-trip elapsed, so it is compared
	// with a tolerance.
	function expectParity(dblRes, realRes, resetToleranceMs = 2000) {
		expect(Number(dblRes[0])).toBe(Number(realRes[0]));
		expect(Number(dblRes[1])).toBe(Number(realRes[1]));
		expect(Math.abs(Number(dblRes[2]) - Number(realRes[2]))).toBeLessThan(resetToleranceMs);
	}

	it('CONSUME: drain a bucket to exhaustion, double matches real step for step', async () => {
		await pinDoubleToRealClock();
		const dbl = mockRedisClient();
		const realKey = client.key('rl:drain');
		const args = [3, 60000, 1, 0]; // points=3, interval, cost=1, blockDuration=0
		for (let i = 0; i < 5; i++) {
			const realRes = await client.redis.eval(CONSUME_SCRIPT, 1, realKey, ...args);
			const dblRes = await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:drain', ...args);
			expectParity(dblRes, realRes);
		}
	});

	it('CONSUME: cost greater than the bucket is rejected without decrementing, on both', async () => {
		await pinDoubleToRealClock();
		const dbl = mockRedisClient();
		const realKey = client.key('rl:cost');
		// points=5, cost=10 -> rejected, remaining stays 5.
		const realRes = await client.redis.eval(CONSUME_SCRIPT, 1, realKey, 5, 60000, 10, 0);
		const dblRes = await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:cost', 5, 60000, 10, 0);
		expectParity(dblRes, realRes);
		expect(Number(dblRes[0])).toBe(0);
		expect(Number(dblRes[1])).toBe(5);
	});

	it('CONSUME: the ban path (blockDuration > 0) matches real on exhaustion and while banned', async () => {
		await pinDoubleToRealClock();
		const dbl = mockRedisClient();
		const realKey = client.key('rl:ban');
		const args = [2, 60000, 1, 5000]; // 2 points, ban 5s on exhaustion
		// Two allowed, the third exhausts and arms the ban, a fourth is still banned.
		for (let i = 0; i < 4; i++) {
			const realRes = await client.redis.eval(CONSUME_SCRIPT, 1, realKey, ...args);
			const dblRes = await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:ban', ...args);
			// Ban-remaining drifts by the real round-trip, so widen the tolerance a touch.
			expectParity(dblRes, realRes, 3000);
		}
	});

	it('CONSUME: refills after the interval elapses, on both', async () => {
		await pinDoubleToRealClock();
		let dbl = mockRedisClient();
		const realKey = client.key('rl:refill');
		const args = [2, 200, 1, 0]; // tiny 200ms interval so a real wait can elapse it
		// Exhaust both.
		for (let i = 0; i < 2; i++) {
			await client.redis.eval(CONSUME_SCRIPT, 1, realKey, ...args);
			await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:refill', ...args);
		}
		// Next call is denied on both (bucket empty, same interval window).
		expectParity(
			await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:refill', ...args),
			await client.redis.eval(CONSUME_SCRIPT, 1, realKey, ...args)
		);
		// Let the interval elapse on Redis's OWN clock (TIME) - the same clock
		// the script's refill window is stamped from, so this is immune to
		// host/VM clock drift - then re-pin the double to the new real clock
		// so both observe the refill.
		await waitRedisMs(client, 260);
		await pinDoubleToRealClock();
		const realRes = await client.redis.eval(CONSUME_SCRIPT, 1, realKey, ...args);
		const dblRes = await dbl.redis.eval(CONSUME_SCRIPT, 1, 'rl:refill', ...args);
		expect(Number(realRes[0])).toBe(1); // refilled -> allowed again
		expectParity(dblRes, realRes);
	});
});
