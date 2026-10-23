/**
 * A virtual runtime clock that models the production clocks side by side.
 *
 * `wallEpoch()` and `monotonicNow()` read the exact virtual time. `now()`
 * reads the value the ~1 s cached wall clock would hold: it is refreshed to
 * the exact time at every refresh tick and lags it in between, never leads.
 * The refresh period defaults to 1010 ms, the step a real `setInterval(1000)`
 * produces on a loaded host. Intervals created through the runtime seam are
 * captured so a test runs them on demand; one-shot timers stay native.
 *
 * Install in a `beforeEach`, and call `resetRuntimeEnv()` in an `afterEach`.
 * Test-only.
 */
import { setRuntimeEnv } from '../../src/shared/runtime.js';

/**
 * @param {{ start?: number, refreshMs?: number, captureIntervals?: boolean }} [options]
 */
export function installSteppedClock(options = {}) {
	const start = options.start ?? 1_000_000;
	const refreshMs = options.refreshMs ?? 1010;
	/** @type {Array<{ cb: () => unknown, ms: number, cleared: boolean, unref(): void }>} */
	const intervals = [];
	const state = { t: start, cached: start, nextRefresh: start + refreshMs };
	/** @type {Record<string, unknown>} */
	const timers = {};
	if (options.captureIntervals !== false) {
		timers.setInterval = (cb, ms) => {
			const h = { cb, ms, cleared: false, unref() {} };
			intervals.push(h);
			return h;
		};
		timers.clearInterval = (h) => { if (h) h.cleared = true; };
	}
	setRuntimeEnv({
		clock: { now: () => state.cached, monotonic: () => state.t, wallEpoch: () => state.t },
		timers
	});
	return {
		intervals,
		get t() { return state.t; },
		get cached() { return state.cached; },
		/** Move exact time forward, refreshing the cached clock at each tick passed. */
		advance(ms) {
			state.t += ms;
			while (state.t >= state.nextRefresh) {
				state.cached = state.nextRefresh;
				state.nextRefresh += refreshMs;
			}
		},
		/** Run one round of every live captured interval. */
		tick() {
			for (const h of intervals) if (!h.cleared) h.cb();
		}
	};
}
