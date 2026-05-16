/**
 * Time primitives for hot paths and duration math.
 *
 * Two functions, two contracts:
 *
 * `now()` returns a cached `Date.now()`-shaped value updated every
 * second. Use for wall-clock-ish timestamps that go on the wire or
 * into log lines, and for staleness checks where ~1s precision is
 * fine. Cheap: a single variable read per call.
 *
 * `monotonicNow()` returns a high-resolution monotonic timestamp
 * (millisecond-aligned with `Date.now()` at process start, but
 * advances strictly forward regardless of NTP / DST / manual clock
 * adjustments). Use for lock / lease / timeout math where a backward
 * NTP step would otherwise extend a timeout or make an elapsed
 * measurement appear negative. Slightly more expensive than `now()`:
 * one `performance.now()` call per invocation, which is a syscall on
 * some platforms. Still negligible against the work it's measuring.
 *
 * Rule of thumb:
 * - "How long did this take?" -> `monotonicNow()` deltas.
 * - "What time is it?" -> `now()`.
 *
 * @module svelte-adapter-uws-extensions/shared/time
 */

import { performance } from 'node:perf_hooks';

let cachedNow = Date.now();

const timer = setInterval(() => { cachedNow = Date.now(); }, 1000);
if (timer.unref) timer.unref();

/** Returns a cached timestamp, accurate to within ~1 second. */
export function now() {
	return cachedNow;
}

/**
 * Snapshot taken at module load: the wall-clock time corresponding
 * to `performance.now() === 0`. Adding `performance.now()` to this
 * gives a monotonic timestamp in the same shape as `Date.now()` (ms
 * since epoch) without the susceptibility to clock steps. Computed
 * once at module load - any tiny drift between Date.now() and
 * `performance.now()` over the process lifetime is acceptable because
 * the value is only used for deltas, not for cross-process comparison.
 */
const processStartEpoch = Date.now() - performance.now();

/**
 * Returns a monotonically-increasing timestamp in milliseconds.
 *
 * Backed by `performance.now() + processStartEpoch`. The value is
 * shaped like a wall-clock timestamp (ms since epoch) but advances
 * strictly forward regardless of system-clock adjustments. Use this
 * for lock / lease / timeout math:
 *
 * ```js
 * const start = monotonicNow();
 * await doWork();
 * const elapsed = monotonicNow() - start; // always >= 0
 * ```
 *
 * With `Date.now()`, a backward NTP step between the two calls makes
 * `elapsed` appear negative; lock retry budgets and TTL checks built
 * on that subtraction misbehave silently.
 *
 * Cost: one `performance.now()` per call (a fast syscall on most
 * platforms, ~50-200ns). Negligible against the work being measured.
 * Do NOT use in tight per-message loops where `now()` would do.
 */
export function monotonicNow() {
	return processStartEpoch + performance.now();
}
