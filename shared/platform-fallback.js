/**
 * Native fallbacks for the adapter's projected platform clock/RNG/HLC surface.
 *
 * Recent adapters project an injectable clock and RNG onto their Platform: a
 * `now` / `monotonic` function pair, a `random` object, and an `hlc` function.
 * The bus wraps forward those live so a seeded harness clock reaches
 * cluster-side per-message handlers. But this package's peer-dependency floor
 * also admits older adapters that predate the projection, where those members
 * are `undefined`. A cluster handler that reads `wrapped.random.u32()` or
 * `wrapped.hlc()` off such a platform would throw.
 *
 * These fallbacks assemble the same shapes from this package's OWN runtime seam
 * so the wrapped surface is always populated and stays seedable in a simulation
 * harness (overriding the runtime env reseeds the fallback too). The `now` and
 * `monotonic` fallbacks are just the runtime `now` / `monotonicNow` helpers used
 * directly at the wrap site - only `random` (needs an object shape) and `hlc`
 * (needs stateful per-process assembly) are factored here.
 *
 * @module svelte-adapter-uws-extensions/shared/platform-fallback
 */

import { now, randomFloat, randomU32, randomUuid, randomBytes } from './runtime.js';

/**
 * Matches the adapter's `platform.random` object, assembled from the runtime
 * RNG seam so a seeded harness reproduces values. A frozen singleton, so the
 * fallback identity is stable across every wrap (the same contract the adapter's
 * own projected `random` object holds).
 *
 * @type {{ float: () => number, u32: () => number, uuid: () => string, bytes: (n: number) => Uint8Array }}
 */
export const fallbackRandom = Object.freeze({
	float: randomFloat,
	u32: randomU32,
	uuid: randomUuid,
	bytes: randomBytes
});

// Process-local hybrid logical clock state. The adapter's HLC is per process
// too (one stamp generator created once at init), so a single instance here is
// the right granularity: when the fallback is in play there is no adapter HLC,
// and a wrap reads this same generator. The nodeId is drawn once from the
// runtime RNG, so a seeded harness reproduces it.
const _nodeId = randomUuid().slice(0, 8);
let _lastWall = 0;
let _logical = 0;

/**
 * Matches the adapter's `platform.hlc()`: a non-decreasing `wall` (sourced from
 * the runtime clock) with a same-millisecond `logical` tiebreaker and a stable
 * per-process `nodeId`. A backward or same-millisecond clock read holds `wall`
 * and advances `logical`, so the `(wall, logical)` pair is a strict per-process
 * ordering.
 *
 * @returns {{ wall: number, logical: number, nodeId: string }}
 */
export function fallbackHlc() {
	const w = now();
	if (w > _lastWall) {
		_lastWall = w;
		_logical = 0;
	} else {
		// Same millisecond or a backward clock step: hold the wall value and
		// advance the tiebreaker so the pair still increases.
		_logical += 1;
	}
	return { wall: _lastWall, logical: _logical, nodeId: _nodeId };
}
