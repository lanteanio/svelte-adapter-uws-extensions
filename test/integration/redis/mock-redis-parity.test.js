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
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockRedisClient } from '../../../src/testing/mock-redis.js';
import { CONSUME_SCRIPT } from '../../../src/redis/token-bucket-script.js';

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

	// SCAN MATCH is the other place the double reimplements server behaviour,
	// and it is the one that purge-scope and tenant-isolation tests run
	// through. A double that matches LOOSELY reports those bugs as fixed, so
	// the glob matcher gets the same real-server oracle the Lua evaluators do.
	describe('SCAN MATCH glob', () => {
		// Keys that put every glob metacharacter on the SUBJECT side too, so a
		// pattern cannot pass by never meeting the character it mishandles.
		const KEYS = [
			'a', 'b', 'c', 'ab', 'abc', 'aXc', 'a-c', 'a]c', 'a^c', 'a\\c', 'a*c', 'a?c',
			'A', 'Z', 'z', ']', '-', '^', '*', '?', '[', 'abd', 'aa', 'aaa', 'aab',
			'x:1', 'x:2', 'user:1:name', 'a.c', 'a b', '_', '`', 'a_c', 'a`c'
		];

		// Every one of these was wrong under the previous regex translation, or
		// guards a rule that is easy to get wrong when porting.
		const PATTERNS = [
			'*', 'a', 'a*', '*c', 'a*c', '*a*', 'a?c', '??', '???',
			'[abc]', '[^abc]', '[a-c]', '[a-cx-z]', '[A-Za-z]', '[^a-c]',
			// Reversed ranges: Redis swaps the endpoints. A regex translation
			// THROWS `Range out of order`, turning a scan into a hard error.
			'[c-a]', '[z-a]', '[Z-A]', '[^c-a]',
			// A leading ']' closes the class in Redis - it is NOT a literal
			// member the way most glob dialects treat it.
			'[]abc]', '[]]', '[^]]', '[^]abc]', 'a[]c',
			// '[a-]' is the range a..']' with the endpoints swapped, not the
			// two members {a, -}.
			'[a-]', '[-a]', '[^-a]', '[--a]',
			// Escapes, unterminated classes, and metacharacters as literals.
			'[a\\-c]', '[\\]]', '[[]', '[a', '[a-c', '[\\\\]', 'a\\*c', 'a\\?c',
			'\\*', 'a\\\\c', 'a[b]c', '*[0-9]*', 'a**c', '**', 'x:*', '*:*', '?',
			// '!' is not a negation character here, unlike some shells.
			'[!abc]'
		];

		async function scanAll(redis, pattern) {
			const found = new Set();
			let cursor = '0';
			do {
				const [next, batch] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
				for (const k of batch) found.add(k);
				cursor = String(next);
			} while (cursor !== '0');
			return found;
		}

		it('agrees with the real server on every probed (pattern, key) pair', async () => {
			const prefix = client.key('glob:');
			const dbl = mockRedisClient(prefix);
			for (const k of KEYS) {
				await client.redis.set(prefix + k, '1');
				await dbl.redis.set(prefix + k, '1');
			}

			// Collected rather than asserted per pair, so a failure reports the
			// whole divergence set instead of only the first row.
			const divergences = [];
			for (const pattern of PATTERNS) {
				const real = await scanAll(client.redis, prefix + pattern);
				const mock = await scanAll(dbl.redis, prefix + pattern);
				for (const k of KEYS) {
					const r = real.has(prefix + k);
					const m = mock.has(prefix + k);
					if (r !== m) divergences.push(`${JSON.stringify(pattern)} x ${JSON.stringify(k)}: real=${r} mock=${m}`);
				}
			}
			expect(divergences).toEqual([]);
		});

		it('does not throw on a reversed range, where a regex translation did', async () => {
			// The sharpest of the three: this was not a wrong answer but an
			// exception out of the double, so a purge scan became a hard error.
			const prefix = client.key('rev:');
			const dbl = mockRedisClient(prefix);
			for (const k of ['a', 'b', 'c', 'd']) {
				await client.redis.set(prefix + k, '1');
				await dbl.redis.set(prefix + k, '1');
			}
			const real = await scanAll(client.redis, prefix + '[c-a]');
			const mock = await scanAll(dbl.redis, prefix + '[c-a]');
			expect(mock).toEqual(real);
			expect([...mock].sort()).toEqual([prefix + 'a', prefix + 'b', prefix + 'c']);
		});

		it('returns promptly on the shape that backtracked catastrophically', async () => {
			// `*a*a*...*b` against a long non-matching subject took 148s under
			// the regex form. The port's skip-longer-matches rule is what
			// bounds it; a wall-clock ceiling is the only way to pin that.
			const prefix = client.key('backtrack:');
			const dbl = mockRedisClient(prefix);
			const subject = 'a'.repeat(64);
			await client.redis.set(prefix + subject, '1');
			await dbl.redis.set(prefix + subject, '1');

			const pattern = prefix + '*a'.repeat(24) + '*b';
			const t0 = process.hrtime.bigint();
			const mock = await scanAll(dbl.redis, pattern);
			const ms = Number(process.hrtime.bigint() - t0) / 1e6;

			expect(mock.size).toBe(0);
			expect(await scanAll(client.redis, pattern)).toEqual(mock);
			expect(ms).toBeLessThan(1000);
		});
	});
});
