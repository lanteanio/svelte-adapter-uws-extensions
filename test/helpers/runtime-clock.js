/**
 * Bind the injectable runtime clock to the global `Date.now()` for the
 * duration of a test that scripts time.
 *
 * The cluster source reads wall and duration time through the runtime module
 * (`shared/runtime.js`) rather than the global. Its production default is a
 * ~1s-cached wall read plus a `performance.now()`-backed monotonic source -
 * neither of which a synchronous test can advance. A test that moves time with
 * `vi.useFakeTimers()` / `vi.advanceTimersByTime()` or pins it with
 * `vi.spyOn(Date, 'now')` therefore must route those movements into the runtime
 * clock. Binding both the wall (`now`) and duration (`monotonic`) readers to
 * `Date.now()` makes the runtime clock follow the scripted time at full
 * precision. Mirrors the adapter's `installFakeRuntimeClock` test helper.
 *
 * Call in a `beforeEach` and pair with {@link releaseRuntimeClock} in an
 * `afterEach`. Test-only: never imported by production code.
 */
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

export function installFakeRuntimeClock() {
	setRuntimeEnv({ clock: { now: () => Date.now(), monotonic: () => Date.now() } });
}

/** Restore the native runtime clock. Pair with {@link installFakeRuntimeClock}. */
export function releaseRuntimeClock() {
	resetRuntimeEnv();
}
