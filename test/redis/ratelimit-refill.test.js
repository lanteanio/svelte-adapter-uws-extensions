// Alternate refill algorithms: sliding-window-counter and GCRA / leaky-bucket.
//
// Both remove the classic ~2x fixed-window boundary burst that 'window' mode
// admits across an edge. The clock is driven through the injectable runtime seam
// (setRuntimeEnv), which the mock redis TIME command and the in-process floor
// both read, so a window edge is crossed deterministically without real waits.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

function mockWs(userData = {}) {
	return { getUserData: () => userData };
}

describe('ratelimit refill modes', () => {
	let client;
	let wallMs;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
		// Align the virtual clock to a window boundary so the arithmetic below is
		// exact (windowStart = now - (now % interval)).
		wallMs = 1_000_000; // a multiple of 1000
		setRuntimeEnv({ clock: { now: () => wallMs, monotonic: () => wallMs, wallEpoch: () => wallMs } });
	});

	afterEach(() => {
		resetRuntimeEnv();
	});

	async function countAllowed(limiter, ws, n, cost = 1) {
		let allowed = 0;
		for (let i = 0; i < n; i++) {
			if ((await limiter.consume(ws, cost)).allowed) allowed++;
		}
		return allowed;
	}

	describe('validation', () => {
		it("accepts 'window' | 'sliding' | 'gcra' and rejects anything else", () => {
			for (const refill of ['window', 'sliding', 'gcra']) {
				expect(() => createRateLimit(client, { points: 5, interval: 1000, refill })).not.toThrow();
			}
			expect(() => createRateLimit(client, { points: 5, interval: 1000, refill: 'leaky' })).toThrow('refill');
		});

		it('each mode uses its own key space (mode infix in the bucket key)', async () => {
			const sliding = createRateLimit(client, { points: 5, interval: 1000, refill: 'sliding' });
			const gcra = createRateLimit(client, { points: 5, interval: 1000, refill: 'gcra' });
			await sliding.consume(mockWs({ ip: '1.2.3.4' }));
			await gcra.consume(mockWs({ ip: '1.2.3.4' }));
			const keys = [...client._hashes.keys()].filter((k) => k.includes('ratelimit')).sort();
			expect(keys).toEqual(['test:v1:ratelimitg:1.2.3.4', 'test:v1:ratelimits:1.2.3.4']);
		});
	});

	describe('sliding-window-counter', () => {
		it('admits up to points within a window and denies beyond', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, refill: 'sliding' });
			const ws = mockWs({ ip: '1.1.1.1' });
			expect(await countAllowed(lim, ws, 5)).toBe(5);
			expect((await lim.consume(ws)).allowed).toBe(false);
		});

		it('does NOT admit a second full budget across a window edge (anti-boundary-burst)', async () => {
			const win = createRateLimit(client, { points: 5, interval: 1000 });
			const sliding = createRateLimit(client, { points: 5, interval: 1000, refill: 'sliding' });
			const wsW = mockWs({ ip: 'w' });
			const wsS = mockWs({ ip: 's' });

			// Prime both so the fixed window's reset lands at wallMs+1000, and the
			// sliding window fills its current window - all within window 1.
			await win.consume(wsW); // starts the fixed window: reset at +1000
			await sliding.consume(wsS);
			// Move to the very end of the window and use up the rest of the budget.
			wallMs += 990;
			expect(await countAllowed(win, wsW, 4)).toBe(4); // 5 total in window 1
			expect(await countAllowed(sliding, wsS, 4)).toBe(4); // 5 total in window 1

			// Cross the edge and immediately try another full budget.
			wallMs += 10; // now exactly on the boundary
			const winBurst = await countAllowed(win, wsW, 5);
			const slidingBurst = await countAllowed(sliding, wsS, 5);
			expect(winBurst).toBe(5); // fixed window refilled fully: the ~2x burst
			expect(slidingBurst).toBe(0); // previous window still fully weighted: no burst
		});

		it('smoothly releases budget as the previous window ages out', async () => {
			const lim = createRateLimit(client, { points: 10, interval: 1000, refill: 'sliding' });
			const ws = mockWs({ ip: '2.2.2.2' });
			expect(await countAllowed(lim, ws, 10)).toBe(10); // fill window 1
			wallMs += 1500; // halfway into window 2: previous window weighted 0.5
			// weighted = 10 * 0.5 = 5, so about 5 more admitted, not a full 10.
			const admitted = await countAllowed(lim, ws, 10);
			expect(admitted).toBeGreaterThanOrEqual(4);
			expect(admitted).toBeLessThanOrEqual(6);
		});

		it('honors a blockDuration ban and lifts it after the window', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 1000, blockDuration: 5000, refill: 'sliding' });
			const ws = mockWs({ ip: '3.3.3.3' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			const banned = await lim.consume(ws);
			expect(banned.allowed).toBe(false);
			expect(banned.resetMs).toBe(5000);
			wallMs += 5001;
			expect((await lim.consume(ws)).allowed).toBe(true);
		});

		it('peek is read-only and matches the next consume', async () => {
			const lim = createRateLimit(client, { points: 3, interval: 1000, refill: 'sliding' });
			const ws = mockWs({ ip: '4.4.4.4' });
			await lim.consume(ws, 2); // 1 slot left in the window
			const before = new Map(client._hashes.get('test:v1:ratelimits:4.4.4.4'));
			const peeked = await lim.peek(ws);
			const after = client._hashes.get('test:v1:ratelimits:4.4.4.4');
			expect(after.get('curr')).toBe(before.get('curr')); // no mutation
			const consumed = await lim.consume(ws);
			expect(peeked).toEqual(consumed);
		});
	});

	describe('gcra / leaky bucket', () => {
		it('admits a burst up to points then throttles', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, refill: 'gcra' });
			const ws = mockWs({ ip: '1.1.1.1' });
			expect(await countAllowed(lim, ws, 5)).toBe(5);
			expect((await lim.consume(ws)).allowed).toBe(false);
		});

		it('enforces smooth spacing: one token every interval/points ms', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, refill: 'gcra' }); // emission 200ms
			const ws = mockWs({ ip: '2.2.2.2' });
			expect(await countAllowed(lim, ws, 5)).toBe(5); // drain the burst
			expect((await lim.consume(ws)).allowed).toBe(false);
			wallMs += 199;
			expect((await lim.consume(ws)).allowed).toBe(false); // still one ms short
			wallMs += 1;
			expect((await lim.consume(ws)).allowed).toBe(true); // exactly one emission elapsed
			expect((await lim.consume(ws)).allowed).toBe(false); // and only one
		});

		it('does NOT admit a second full budget across a window edge (anti-boundary-burst)', async () => {
			const gcra = createRateLimit(client, { points: 5, interval: 1000, refill: 'gcra' });
			const ws = mockWs({ ip: '5.5.5.5' });
			await gcra.consume(ws);
			wallMs += 990;
			expect(await countAllowed(gcra, ws, 4)).toBe(4); // 5 total consumed
			wallMs += 10; // cross a 1000ms boundary
			// Only ~1 emission (10ms since the last consume span) has freed up, so a
			// full budget of 5 cannot pass at the edge.
			expect(await countAllowed(gcra, ws, 5)).toBeLessThan(3);
		});

		it('honors a blockDuration ban and lifts it after it expires', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 1000, blockDuration: 5000, refill: 'gcra' });
			const ws = mockWs({ ip: '3.3.3.3' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			const banned = await lim.consume(ws);
			expect(banned.allowed).toBe(false);
			expect(banned.resetMs).toBe(5000);
			wallMs += 2000;
			expect((await lim.consume(ws)).allowed).toBe(false); // still banned
			wallMs += 3001;
			expect((await lim.consume(ws)).allowed).toBe(true);
		});

		it('peek is read-only and matches the next consume', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, refill: 'gcra' });
			const ws = mockWs({ ip: '4.4.4.4' });
			await lim.consume(ws, 2);
			const before = new Map(client._hashes.get('test:v1:ratelimitg:4.4.4.4'));
			const peeked = await lim.peek(ws);
			const after = client._hashes.get('test:v1:ratelimitg:4.4.4.4');
			expect(after.get('tat')).toBe(before.get('tat')); // no mutation
			const consumed = await lim.consume(ws);
			expect(peeked).toEqual(consumed);
		});
	});

	describe('emergency scale applies to both modes', () => {
		// Warm the lazily-refreshed emergency reader: the first current() returns the
		// previous value and schedules the background GET; flush microtasks so the
		// cache holds the stored factor before the asserted consumes.
		async function warm(limiter, ws) {
			await limiter.consume(ws);
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
		}

		it('scale 0.2 on 10 points admits 2 under sliding', async () => {
			const lim = createRateLimit(client, { points: 10, interval: 60000, refill: 'sliding' });
			await lim.emergency.set(0.2);
			await warm(lim, mockWs({ ip: 'warm' }));
			const ws = mockWs({ ip: '1.2.3.4' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(false);
		});

		it('scale 0.2 on 10 points admits 2 under gcra', async () => {
			const lim = createRateLimit(client, { points: 10, interval: 60000, refill: 'gcra' });
			await lim.emergency.set(0.2);
			await warm(lim, mockWs({ ip: 'warm' }));
			const ws = mockWs({ ip: '9.9.9.9' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(false);
		});
	});

	describe('local floor mirrors the selected mode while the store is down', () => {
		function downClient() {
			const down = () => Promise.reject(new Error('redis down'));
			const redis = { eval: down };
			redis.defineCommand = (name) => { redis[name] = down; };
			return { redis, key: (s) => 'test:' + s };
		}

		it('window floor (the default mode) admits points then denies, and peek is read-only', async () => {
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			// No refill option -> default 'window'. The store-down window floor plus
			// its read-only peek path was otherwise exercised by no test.
			const lim = createRateLimit(downClient(), { points: 3, interval: 60000, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '3.3.3.3' });
			expect(await countAllowed(lim, ws, 3)).toBe(3);
			expect((await lim.consume(ws)).allowed).toBe(false);
			// A read-only peek on the window floor does not spend: still denied.
			expect((await lim.peek(ws)).allowed).toBe(false);
		});

		it('sliding floor admits points then denies, and peek is read-only', async () => {
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			const lim = createRateLimit(downClient(), { points: 3, interval: 60000, refill: 'sliding', localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			expect(await countAllowed(lim, ws, 3)).toBe(3);
			expect((await lim.consume(ws)).allowed).toBe(false);
			// A read-only peek on the floor does not spend: it still reports denied.
			expect((await lim.peek(ws)).allowed).toBe(false);
		});

		it('gcra floor enforces the burst then throttles', async () => {
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			const lim = createRateLimit(downClient(), { points: 5, interval: 1000, refill: 'gcra', localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '2.2.2.2' });
			expect(await countAllowed(lim, ws, 5)).toBe(5);
			expect((await lim.consume(ws)).allowed).toBe(false);
			wallMs += 200;
			expect((await lim.consume(ws)).allowed).toBe(true); // one emission later
		});
	});
});
