/**
 * Resolve + validate the public cursor options into the frozen internal
 * config the Redis-backed cursor tracker closes over.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor/options
 */

import { projectDefaultUserData } from '../../shared/default-projection.js';

/**
 * @typedef {Object} ResolvedCursorOptions
 * @property {number} throttleMs
 * @property {number} topicThrottleMs
 * @property {number} snapshotIntervalMs
 * @property {(userData: any) => any} select
 * @property {boolean} sanitizeSelected
 * @property {number} cursorTtl
 * @property {(data: any) => ({ x: number, y: number } | null)} position
 * @property {(data: any) => ({ x: number, y: number } | null)} finitePosition
 * @property {number} minMove
 * @property {number} settleMs
 * @property {boolean} bpEnabled
 * @property {number} bpMaxBufferedBytes
 * @property {boolean} viewportEnabled
 * @property {number} viewportPadding
 * @property {number} viewportCell
 * @property {boolean} perSubscriberWalk
 */

/**
 * Apply defaults, run every validation throw, and derive the position
 * extractors plus the per-subscriber-walk flag from the public cursor
 * options. Pure: no I/O and no shared state, so the same input always yields
 * the same frozen config. createCursor destructures the result back into the
 * same local names its body already used.
 *
 * @param {import('../cursor.js').RedisCursorOptions} [options]
 * @returns {ResolvedCursorOptions}
 */
export function resolveCursorOptions(options = {}) {
	const throttleMs = options.throttle ?? 16;
	const topicThrottleMs = options.topicThrottle ?? 16;
	const snapshotIntervalMs = options.snapshotIntervalMs ?? 100;
	if (options.select != null && typeof options.select !== 'function') {
		throw new Error('redis cursor: select must be a function');
	}
	const sanitizeSelected = options.select != null;
	const select = sanitizeSelected ? options.select : projectDefaultUserData;
	const cursorTtl = options.ttl ?? 30;

	// Read {x, y} out of the app's cursor `data` for the jitter filter. The
	// default reads finite `data.x` / `data.y`; an app whose payload nests
	// coordinates elsewhere overrides it. Returning null (or throwing) opts a
	// single frame out of filtering - it is always delivered - so a
	// coordinate-less or malformed frame is never silently dropped. Mirrors the
	// in-memory cursor plugin's `position` option.
	const position = typeof options.position === 'function'
		? options.position
		: (data) =>
				data && typeof data.x === 'number' && typeof data.y === 'number'
					&& Number.isFinite(data.x) && Number.isFinite(data.y)
					? { x: data.x, y: data.y }
					: null;

	// Extract a finite {x, y} for the jitter filter. Wraps `position` with a
	// finiteness guard so a non-finite or unextractable coordinate yields null
	// (treated as "always deliver") rather than letting a NaN comparison
	// silently fail open and corrupt the `lastSentPos` anchor. Only called when
	// minMove > 0.
	const finitePosition = (data) => {
		let p = null;
		try { p = position(data); } catch { p = null; }
		if (p && (typeof p.x !== 'number' || typeof p.y !== 'number'
			|| !Number.isFinite(p.x) || !Number.isFinite(p.y))) p = null;
		return p;
	};

	// Jitter filter (opt-in). Drop a cursor move at ingest when it has not moved
	// at least `minMove` (Chebyshev distance) from the LAST BROADCAST position,
	// so a burst of sub-threshold wobble around a point is never fanned out (and
	// never relayed across instances). The distance is in the units `position`
	// returns and is measured against what the subscriber last actually saw (the
	// last committed flush), so a slow drift still delivers every `minMove`
	// units. When movement then stops, a debounced settle delivers the final
	// resting position once - even if it is within `minMove` of the last
	// broadcast - so a still cursor is never left stranded at a stale point; an
	// exact repeat stays dropped because the settle sends nothing when the rest
	// position equals the last broadcast. 0 (default) disables it. For
	// integer-pixel cursor data, `minMove: 1` drops exact-repeat frames at no
	// visual cost; raise to 2-4 to suppress sub-pixel wobble from high-DPI
	// input. Off by default because the right threshold depends on the app's
	// coordinate scale (1 board unit can be many on-screen pixels when zoomed in).
	const minMove = options.minMove ?? 0;

	// Debounce delay before the jitter filter flushes a settled cursor's final
	// position. Tracks the per-cursor throttle cadence (the rate the app already
	// accepts); falls back to one ~60 Hz frame when throttling is off. Only used
	// when minMove > 0.
	const settleMs = throttleMs > 0 ? throttleMs : 16;

	if (typeof throttleMs !== 'number' || !Number.isFinite(throttleMs) || throttleMs < 0) {
		throw new Error('redis cursor: throttle must be a non-negative number');
	}
	if (typeof topicThrottleMs !== 'number' || !Number.isFinite(topicThrottleMs) || topicThrottleMs < 0) {
		throw new Error('redis cursor: topicThrottle must be a non-negative number');
	}
	if (typeof snapshotIntervalMs !== 'number' || !Number.isFinite(snapshotIntervalMs) || snapshotIntervalMs < 0) {
		throw new Error('redis cursor: snapshotIntervalMs must be a non-negative number');
	}
	if (typeof cursorTtl !== 'number' || !Number.isFinite(cursorTtl) || cursorTtl < 1) {
		throw new Error('redis cursor: ttl must be a positive number (seconds)');
	}
	if (options.position !== undefined && typeof options.position !== 'function') {
		throw new Error('redis cursor: position must be a function');
	}
	if (typeof minMove !== 'number' || !Number.isFinite(minMove) || minMove < 0) {
		throw new Error('redis cursor: minMove must be a non-negative number');
	}

	// Backpressure-aware per-subscriber drop (opt-in). When enabled, a topic's
	// flush switches from the shared combined frame to a per-subscriber walk over
	// THIS instance's local subscriber set, skipping any subscriber whose queued
	// bytes exceed `maxBufferedBytes` for the current flush. Cursors are
	// latest-value, so a skipped subscriber catches up on the next flush with the
	// latest coalesced positions - it renders one cadence later, never
	// accumulating a backlog. Off by default: the zero-config path keeps the
	// shared combined-frame fan-out and pays nothing.
	// `viewport: true` / `backpressure: true` are shorthand for `{ enabled: true }`
	// with defaults. A bare object with tuning keys but no `enabled` is almost
	// certainly a forgotten `enabled: true`, so it throws below rather than
	// silently culling nothing.
	const bp = options.backpressure === true ? { enabled: true } : (options.backpressure || {});
	const vp = options.viewport === true ? { enabled: true } : (options.viewport || {});

	const bpEnabled = bp.enabled === true;
	const bpMaxBufferedBytes = bp.maxBufferedBytes ?? 1024 * 1024;

	// Viewport culling (opt-in). When enabled, the per-subscriber walk sends each
	// reporting subscriber only the moving cursors inside its last reported
	// viewport rect (plus a padding overscan), culled against the COMBINED
	// local + peer cursor set this flush assembles. A subscriber that never
	// reports a rect is treated as whole-board and is never culled - the
	// per-subscriber opt-in that makes culling safe by construction. The reported
	// rect is in the board's own coordinate space; only the overscan is widened by
	// 1/zoom when zoomed out so it stays roughly constant on screen.
	const viewportEnabled = vp.enabled === true;
	const viewportPadding = vp.padding ?? 256;
	const viewportCell = vp.cell ?? 256;

	for (const name of ['viewport', 'backpressure']) {
		const val = options[name];
		if (val !== undefined && val !== null && typeof val !== 'boolean' && typeof val !== 'object') {
			throw new Error(`redis cursor: ${name} must be true or an options object`);
		}
	}
	if (bp.maxBufferedBytes !== undefined && bp.enabled === undefined) {
		throw new Error('redis cursor: backpressure.maxBufferedBytes is set but backpressure.enabled is not - did you mean { enabled: true }?');
	}
	if (
		bp.maxBufferedBytes !== undefined &&
		(!Number.isInteger(bpMaxBufferedBytes) || bpMaxBufferedBytes < 1)
	) {
		throw new Error('redis cursor: backpressure.maxBufferedBytes must be a positive integer');
	}
	if ((vp.padding !== undefined || vp.cell !== undefined) && vp.enabled === undefined) {
		throw new Error('redis cursor: viewport.padding/cell is set but viewport.enabled is not - did you mean { enabled: true }?');
	}
	if (
		vp.padding !== undefined &&
		(typeof viewportPadding !== 'number' || !Number.isFinite(viewportPadding) || viewportPadding < 0)
	) {
		throw new Error('redis cursor: viewport.padding must be a non-negative number');
	}
	if (
		vp.cell !== undefined &&
		(typeof viewportCell !== 'number' || !Number.isFinite(viewportCell) || viewportCell <= 0)
	) {
		throw new Error('redis cursor: viewport.cell must be a positive number');
	}

	// Single boolean the flush hot path branches on. When false, the flush takes
	// the unchanged shared combined-frame path; when true a topic with at least
	// one reporter (or any topic, under backpressure) takes the per-subscriber
	// walk instead.
	const perSubscriberWalk = bpEnabled || viewportEnabled;

	return Object.freeze({
		throttleMs,
		topicThrottleMs,
		snapshotIntervalMs,
		select,
		sanitizeSelected,
		cursorTtl,
		position,
		finitePosition,
		minMove,
		settleMs,
		bpEnabled,
		bpMaxBufferedBytes,
		viewportEnabled,
		viewportPadding,
		viewportCell,
		perSubscriberWalk
	});
}
