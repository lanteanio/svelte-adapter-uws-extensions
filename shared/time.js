/**
 * Cached Date.now() — updated every second via setInterval.
 *
 * Hot paths (heartbeat loops, throttle checks, stale detection) call
 * Date.now() thousands of times per tick. This module caches the value
 * and updates it once per second, which is more than precise enough
 * for TTL and staleness comparisons.
 *
 * @module svelte-adapter-uws-extensions/shared/time
 */

let cachedNow = Date.now();

const timer = setInterval(() => { cachedNow = Date.now(); }, 1000);
if (timer.unref) timer.unref();

/** Returns a cached timestamp, accurate to within ~1 second. */
export function now() {
	return cachedNow;
}
