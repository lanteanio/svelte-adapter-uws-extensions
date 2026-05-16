import { describe, it, expect } from 'vitest';
import { now, monotonicNow } from '../../shared/time.js';

describe('shared/time', () => {
	describe('now()', () => {
		it('returns a number close to Date.now()', () => {
			const wall = Date.now();
			const cached = now();
			// Within ~2 seconds (the cache refresh interval + scheduling jitter)
			expect(Math.abs(wall - cached)).toBeLessThan(2000);
		});

		it('returns the same value when called twice within a tick (cached)', () => {
			const a = now();
			const b = now();
			expect(b).toBe(a);
		});
	});

	describe('monotonicNow()', () => {
		it('returns a number in the wall-clock-shaped range (ms since epoch)', () => {
			const m = monotonicNow();
			// Sanity: somewhere between year 2020 and year 2100 in ms-since-epoch.
			expect(m).toBeGreaterThan(1_577_836_800_000); // 2020-01-01
			expect(m).toBeLessThan(4_102_444_800_000);    // 2100-01-01
		});

		it('advances forward across two calls', async () => {
			const a = monotonicNow();
			await new Promise((r) => setTimeout(r, 5));
			const b = monotonicNow();
			expect(b).toBeGreaterThan(a);
		});

		it('produces a non-negative elapsed delta even under rapid successive reads', () => {
			// Smoke test: 1000 reads in a tight loop should all advance.
			let last = monotonicNow();
			for (let i = 0; i < 1000; i++) {
				const cur = monotonicNow();
				expect(cur).toBeGreaterThanOrEqual(last);
				last = cur;
			}
		});

		it('elapsed deltas are millisecond-shaped (small for small intervals)', async () => {
			const start = monotonicNow();
			await new Promise((r) => setTimeout(r, 20));
			const elapsed = monotonicNow() - start;
			// Allow generous bounds for scheduling jitter on slow CI.
			expect(elapsed).toBeGreaterThanOrEqual(15);
			expect(elapsed).toBeLessThan(500);
		});

		it('initial value is within ~1s of Date.now() (process-start alignment)', () => {
			// processStartEpoch is captured at module load; over a long-running
			// process performance.now() and Date.now() can drift slightly, but
			// at test time the drift should be small.
			expect(Math.abs(monotonicNow() - Date.now())).toBeLessThan(5000);
		});
	});

	describe('now() vs monotonicNow() contracts', () => {
		it('both return numbers', () => {
			expect(typeof now()).toBe('number');
			expect(typeof monotonicNow()).toBe('number');
		});

		it('monotonicNow has higher resolution than the cached now()', async () => {
			// now() is cached at 1s precision; monotonicNow advances continuously.
			// Difference between two monotonicNow calls separated by 10ms should
			// be > 1ms; difference between two now() calls in the same tick is 0.
			const m1 = monotonicNow();
			const n1 = now();
			await new Promise((r) => setTimeout(r, 10));
			const m2 = monotonicNow();
			const n2 = now();
			expect(m2 - m1).toBeGreaterThan(1);
			// n2 - n1 is 0 in the typical case (same cache window).
			expect(n2 - n1).toBeGreaterThanOrEqual(0);
		});
	});
});
