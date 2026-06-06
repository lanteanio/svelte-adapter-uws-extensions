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
 * Both functions are backed by the injectable runtime environment in
 * `shared/runtime.js`, which owns the cached wall clock and the
 * monotonic source. They are re-exported here unchanged so the existing
 * callers and their contracts stay byte-identical.
 *
 * @module svelte-adapter-uws-extensions/shared/time
 */

export { now, monotonicNow } from './runtime.js';
