import { afterEach, describe, it, expect } from 'vitest';
import {
	now,
	monotonicNow,
	wallEpoch,
	randomFloat,
	randomU32,
	randomUuid,
	randomBytes,
	setTimer,
	clearTimer,
	microtask,
	effectiveTimeZone,
	setRuntimeEnv,
	resetRuntimeEnv,
	getRuntimeEnv
} from '../../src/shared/runtime.js';

// Always restore the native environment between cases so an override in one
// test can never leak into another.
afterEach(() => {
	resetRuntimeEnv();
});

describe('shared/runtime', () => {
	describe('default helpers (native binding)', () => {
		it('clock helpers return native-equivalent values', () => {
			const wall = Date.now();
			expect(Math.abs(wall - now())).toBeLessThan(2000);
			expect(Math.abs(wall - wallEpoch())).toBeLessThan(2000);
			// monotonic is wall-clock-shaped (ms since epoch) and within ~5s.
			expect(Math.abs(monotonicNow() - wall)).toBeLessThan(5000);
		});

		it('rng helpers return native-equivalent values', () => {
			const f = randomFloat();
			expect(f).toBeGreaterThanOrEqual(0);
			expect(f).toBeLessThan(1);

			const u = randomU32();
			expect(Number.isInteger(u)).toBe(true);
			expect(u).toBeGreaterThanOrEqual(0);
			expect(u).toBeLessThanOrEqual(0xffffffff);

			expect(randomUuid()).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

			const b = randomBytes(8);
			expect(b).toBeInstanceOf(Buffer);
			expect(b.length).toBe(8);
		});

		it('timer helpers schedule and clear against the native loop', async () => {
			await new Promise((resolve, reject) => {
				const h = setTimer(() => reject(new Error('cleared timer still fired')), 50);
				clearTimer(h);
				// A microtask + a later real timer confirms the cleared one did not run.
				microtask(() => {
					setTimer(resolve, 80);
				});
			});
		});

		it('effectiveTimeZone is undefined under the native default', () => {
			expect(effectiveTimeZone()).toBeUndefined();
		});
	});

	describe('setRuntimeEnv', () => {
		it('installs a partial env (fixed clock) reflected by the helpers', () => {
			setRuntimeEnv({ clock: { now: () => 123456 } });
			expect(now()).toBe(123456);
		});

		it('a partial env merges over native defaults (override clock leaves rng/timers native)', () => {
			setRuntimeEnv({ clock: { now: () => 777 } });

			// Overridden field reflects the fake.
			expect(now()).toBe(777);

			// Non-overridden clock fields stay native.
			expect(Math.abs(wallEpoch() - Date.now())).toBeLessThan(2000);

			// RNG stays native.
			const f = randomFloat();
			expect(f).toBeGreaterThanOrEqual(0);
			expect(f).toBeLessThan(1);
			expect(randomUuid()).toMatch(/^[0-9a-f-]{36}$/);

			// Timers stay native: setTimer returns a real handle that can be cleared.
			const env = getRuntimeEnv();
			expect(env.timers.set).toBe(getRuntimeEnv().timers.set);
			const h = setTimer(() => {}, 1000);
			clearTimer(h);
		});

		it('overrides rng independently of the clock', () => {
			setRuntimeEnv({ rng: { float: () => 0.5, u32: () => 42 } });
			expect(randomFloat()).toBe(0.5);
			expect(randomU32()).toBe(42);
			// Clock stays native.
			expect(Math.abs(now() - Date.now())).toBeLessThan(2000);
		});

		it('overrides tz only when the key is present', () => {
			setRuntimeEnv({ tz: 'UTC' });
			expect(effectiveTimeZone()).toBe('UTC');

			// An env without a tz key keeps the native default (undefined).
			setRuntimeEnv({ clock: { now: () => 1 } });
			expect(effectiveTimeZone()).toBeUndefined();
		});

		it('refuses in production without { force: true } and succeeds with it', () => {
			const prior = process.env.NODE_ENV;
			process.env.NODE_ENV = 'production';
			try {
				expect(() => setRuntimeEnv({ clock: { now: () => 0 } })).toThrow(/production/);
				// The refused call must not have swapped anything.
				expect(Math.abs(now() - Date.now())).toBeLessThan(2000);

				// With force the swap goes through.
				setRuntimeEnv({ clock: { now: () => 999 } }, { force: true });
				expect(now()).toBe(999);
			} finally {
				if (prior === undefined) delete process.env.NODE_ENV;
				else process.env.NODE_ENV = prior;
			}
		});
	});

	describe('resetRuntimeEnv', () => {
		it('restores native behavior after an override', () => {
			setRuntimeEnv({ clock: { now: () => 5 } });
			expect(now()).toBe(5);
			resetRuntimeEnv();
			expect(Math.abs(now() - Date.now())).toBeLessThan(2000);
		});
	});

	describe('getRuntimeEnv', () => {
		it('reflects the active env', () => {
			const before = getRuntimeEnv();
			expect(before.clock.now()).toBeTypeOf('number');

			const installed = setRuntimeEnv({ clock: { now: () => 314 } });
			expect(getRuntimeEnv()).toBe(installed);
			expect(getRuntimeEnv().clock.now()).toBe(314);
		});
	});

	describe('frozen monomorphic shape', () => {
		it('the active env stays a frozen object with the same key shape after a swap', () => {
			const before = getRuntimeEnv();
			const beforeKeys = Object.keys(before).sort();
			expect(Object.isFrozen(before)).toBe(true);
			expect(Object.isFrozen(before.clock)).toBe(true);
			expect(Object.isFrozen(before.rng)).toBe(true);
			expect(Object.isFrozen(before.timers)).toBe(true);

			setRuntimeEnv({ clock: { now: () => 1 } });
			const after = getRuntimeEnv();
			const afterKeys = Object.keys(after).sort();

			expect(Object.isFrozen(after)).toBe(true);
			expect(Object.isFrozen(after.clock)).toBe(true);
			expect(Object.isFrozen(after.rng)).toBe(true);
			expect(Object.isFrozen(after.timers)).toBe(true);

			// Same top-level key shape; the nested sub-objects keep their shape too.
			expect(afterKeys).toEqual(beforeKeys);
			expect(Object.keys(after.clock).sort()).toEqual(Object.keys(before.clock).sort());
			expect(Object.keys(after.rng).sort()).toEqual(Object.keys(before.rng).sort());
			expect(Object.keys(after.timers).sort()).toEqual(Object.keys(before.timers).sort());
		});

		it('a frozen env cannot be mutated in place', () => {
			const env = getRuntimeEnv();
			expect(() => {
				'use strict';
				env.clock = {};
			}).toThrow();
		});
	});
});
