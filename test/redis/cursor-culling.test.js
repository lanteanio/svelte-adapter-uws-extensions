import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { walkPlatform } from '../helpers/walk-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createCursor as createCursorRaw } from '../../redis/cursor.js';

// Full topic the cursor plugin scopes every wire frame to.
const CURSOR = '__cursor:board';

// Trackers created in a test, registered so afterEach can tear them down even
// when the test throws before reaching its own destroy(). A leaked tracker
// keeps an async mock-redis subscriber alive; a leaked fake-timer test bleeds
// timers into the next test. The afterEach below closes both leak vectors so
// the suite is order-independent when run standalone, not just under vitest's
// per-file worker isolation.
let created = [];
function createCursor(...args) {
	const c = createCursorRaw(...args);
	created.push(c);
	return c;
}

// Positions delivered to one subscriber, as a sorted "x,y" set, across both
// the single-mover `update` and the coalesced `bulk` wire shapes. The redis
// variant keys its combined entries by connection key with `{ key, data }`,
// exactly like the bundled in-memory variant, so the position lives at
// `e.data.data` (update) or `it.data` (bulk item).
function deliveredPositions(p, ws) {
	const out = [];
	for (const e of p.sentTo(ws)) {
		if (e.event === 'update') out.push(`${e.data.data.x},${e.data.data.y}`);
		else if (e.event === 'bulk') for (const it of e.data) out.push(`${it.data.x},${it.data.y}`);
	}
	return out.sort();
}

describe('redis cursor viewport culling and backpressure', () => {
	let client;

	beforeEach(() => {
		vi.useRealTimers();
		created = [];
		client = mockRedisClient('test:');
	});

	afterEach(() => {
		// Restore real timers unconditionally so a fake-timer test that threw
		// before its own restore line cannot bleed into the next test, then
		// destroy every tracker created in the test (idempotent with the
		// per-test destroy() calls) so no async subscriber survives the test.
		vi.useRealTimers();
		for (const c of created) {
			try { c.destroy(); } catch { /* already torn down */ }
		}
		created = [];
	});

	describe('zero-config parity', () => {
		it('never walks subscribers and stays on the shared frame with no options', () => {
			const platform = walkPlatform();
			const walkSpy = vi.spyOn(platform, 'forEachSubscriber');
			const bufferedSpy = vi.spyOn(platform, 'bufferedAmount');
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (ud) => ({ id: ud.id }) });

			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			const mover = mockWs({ id: 'M' });
			c.update(mover, 'board', { x: 1, y: 2 }, platform);

			// Shared fan-out: join + update on published[]; nothing per-subscriber.
			expect(platform.published.map((e) => e.event)).toEqual(['join', 'update']);
			expect(platform.sent).toHaveLength(0);
			expect(walkSpy).not.toHaveBeenCalled();
			expect(bufferedSpy).not.toHaveBeenCalled();
			c.destroy();
		});

		it('reports perSubscriberFlushes 0 in the zero-config stats', () => {
			const platform = walkPlatform();
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
			const mover = mockWs({ id: 'M' });
			platform.addSubscriber(mockWs({ id: 'V' }), CURSOR);
			c.update(mover, 'board', { x: 5, y: 5 }, platform);
			expect(c.stats().perSubscriberFlushes).toBe(0);
			c.destroy();
		});
	});

	describe('option validation', () => {
		it('accepts the boolean shorthand for viewport and backpressure', () => {
			expect(() => createCursor(client, { viewport: true })).not.toThrow();
			expect(() => createCursor(client, { backpressure: true })).not.toThrow();
		});

		it('rejects a tuning key set without enabled', () => {
			expect(() => createCursor(client, { viewport: { padding: 512 } })).toThrow('enabled');
			expect(() => createCursor(client, { viewport: { cell: 128 } })).toThrow('enabled');
			expect(() => createCursor(client, { backpressure: { maxBufferedBytes: 4096 } })).toThrow('enabled');
		});

		it('rejects a non-boolean non-object value', () => {
			expect(() => createCursor(client, { viewport: 1 })).toThrow();
			expect(() => createCursor(client, { backpressure: 'x' })).toThrow();
		});

		it('validates viewport padding and cell', () => {
			expect(() => createCursor(client, { viewport: { enabled: true, padding: -1 } })).toThrow('non-negative');
			expect(() => createCursor(client, { viewport: { enabled: true, cell: 0 } })).toThrow('positive');
			expect(() => createCursor(client, { viewport: { enabled: true, cell: -5 } })).toThrow('positive');
			expect(() => createCursor(client, { viewport: { enabled: true } })).not.toThrow();
		});

		it('validates backpressure maxBufferedBytes', () => {
			expect(() => createCursor(client, { backpressure: { enabled: true, maxBufferedBytes: 0 } })).toThrow('positive integer');
			expect(() => createCursor(client, { backpressure: { enabled: true, maxBufferedBytes: 1.5 } })).toThrow('positive integer');
			expect(() => createCursor(client, { backpressure: { enabled: true, maxBufferedBytes: -1 } })).toThrow('positive integer');
			expect(() => createCursor(client, { backpressure: { enabled: true } })).not.toThrow();
		});

		it('rejects a non-function position', () => {
			expect(() => createCursor(client, { position: 'bad' })).toThrow('function');
			expect(() => createCursor(client, { position: (d) => d })).not.toThrow();
		});
	});

	describe('viewport ingress and lazy activation', () => {
		it('exposes viewport and viewportFor on the tracker', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			expect(typeof c.viewport).toBe('function');
			expect(typeof c.viewportFor).toBe('function');
			c.destroy();
		});

		it('records a reported rect and reads it back, defaulting zoom to 1', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			const ws = mockWs({ id: 'V' });
			c.viewport(ws, 'board', { x: 100, y: 200, w: 1920, h: 1080, zoom: 1 });
			expect(c.viewportFor(ws, 'board')).toEqual({ x: 100, y: 200, w: 1920, h: 1080, zoom: 1 });

			const ws2 = mockWs({ id: 'V2' });
			c.viewport(ws2, 'board', { x: 0, y: 0, w: 800, h: 600 });
			expect(c.viewportFor(ws2, 'board')).toEqual({ x: 0, y: 0, w: 800, h: 600, zoom: 1 });
			c.destroy();
		});

		it('returns null for a subscriber that never reported a rect', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			expect(c.viewportFor(mockWs({ id: 'V' }), 'board')).toBeNull();
			c.destroy();
		});

		it('drops a malformed or degenerate rect so the subscriber stays whole-board', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			const ws = mockWs({ id: 'V' });
			c.viewport(ws, 'board', { x: 1, y: 2, w: 3 });                 // missing h
			c.viewport(ws, 'board', { x: 1, y: 2, w: 3, h: 'nope' });      // non-number
			c.viewport(ws, 'board', null);                                 // non-object
			c.viewport(ws, 'board', { x: 0, y: 0, w: 0, h: 100 });         // zero width
			c.viewport(ws, 'board', { x: 0, y: 0, w: 100, h: -5 });        // negative height
			c.viewport(ws, 'board', { x: 0, y: 0, w: 100, h: 100, zoom: 0 }); // zero zoom
			expect(c.viewportFor(ws, 'board')).toBeNull();
			c.destroy();
		});

		it('keeps the shared frame when viewport is enabled but nobody reports a rect', () => {
			const platform = walkPlatform();
			const walkSpy = vi.spyOn(platform, 'forEachSubscriber');
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const viewer = mockWs({ id: 'V' }); // never reports a rect
			platform.addSubscriber(viewer, CURSOR);

			c.update(mockWs({ id: 'M' }), 'board', { x: 99999, y: 99999 }, platform);

			expect(platform.published.map((e) => e.event)).toEqual(['join', 'update']);
			expect(platform.sent).toHaveLength(0);
			expect(walkSpy).not.toHaveBeenCalled();
			expect(c.stats().perSubscriberFlushes).toBe(0);
			c.destroy();
		});

		it('flips a topic onto the walk on the first reported rect', () => {
			const platform = walkPlatform();
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const reporter = mockWs({ id: 'R' });
			platform.addSubscriber(reporter, CURSOR);
			c.viewport(reporter, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 1 });

			c.update(mockWs({ id: 'M' }), 'board', { x: 500, y: 500 }, platform);

			expect(c.stats().perSubscriberFlushes).toBe(1);
			c.destroy();
		});

		it('does not double-count a re-reported rect', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			const a = mockWs({ id: 'A' });
			c.viewport(a, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			c.viewport(a, 'board', { x: 5, y: 5, w: 10, h: 10, zoom: 1 }); // re-report
			c.viewport(a, 'board', { x: 9, y: 9, w: 10, h: 10, zoom: 1 }); // re-report again
			expect(c.stats().viewportsReported).toBe(1);
			c.destroy();
		});

		it('counts distinct reporting subscribers across topics', () => {
			const c = createCursor(client, { viewport: { enabled: true } });
			const a = mockWs({ id: 'A' });
			const b = mockWs({ id: 'B' });
			expect(c.stats().viewportsReported).toBe(0);
			c.viewport(a, 'board', { x: 0, y: 0, w: 1, h: 1 });
			c.viewport(b, 'board', { x: 0, y: 0, w: 1, h: 1 });
			c.viewport(a, 'other', { x: 0, y: 0, w: 1, h: 1 }); // same subscriber, second topic
			expect(c.stats().viewportsReported).toBe(2);
			c.destroy();
		});
	});

	describe('cull correctness', () => {
		function viewportTracker(extra = {}) {
			return createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				snapshotIntervalMs: 0,
				viewport: { enabled: true },
				select: (ud) => ({ id: ud.id }),
				...extra
			});
		}

		it('omits a mover outside a reporter rect and keeps the padding band', () => {
			const c = viewportTracker(); // default padding 256
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 1 });

			c.update(mockWs({ id: 'near' }), 'board', { x: 500, y: 500 }, platform); // inside
			c.update(mockWs({ id: 'band' }), 'board', { x: 1100, y: 500 }, platform); // in 256 padding band
			c.update(mockWs({ id: 'far' }), 'board', { x: 2000, y: 500 }, platform); // beyond padding

			expect(deliveredPositions(platform, viewer)).toEqual(['1100,500', '500,500']);
			expect(c.stats().culledEntriesDropped).toBe(1); // the far mover withheld from V
			c.destroy();
		});

		it('never culls a non-reporting subscriber even while the walk is active', () => {
			const c = viewportTracker();
			const platform = walkPlatform();
			const reporter = mockWs({ id: 'R' });
			const nonReporter = mockWs({ id: 'N' });
			platform.addSubscriber(reporter, CURSOR);
			platform.addSubscriber(nonReporter, CURSOR);
			c.viewport(reporter, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });

			c.update(mockWs({ id: 'M' }), 'board', { x: 99999, y: 99999 }, platform);

			expect(deliveredPositions(platform, reporter)).toEqual([]); // far outside -> culled
			expect(deliveredPositions(platform, nonReporter)).toEqual(['99999,99999']); // whole-board
			c.destroy();
		});

		it('always delivers a coordinate-less frame (null position)', () => {
			const c = viewportTracker();
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 1, h: 1, zoom: 1 }); // excludes nearly everything

			c.update(mockWs({ id: 'M' }), 'board', { stroke: 'abc' }, platform); // no x/y -> null position

			expect(platform.sentTo(viewer).filter((e) => e.event === 'update')).toHaveLength(1);
			expect(c.stats().culledEntriesDropped).toBe(0); // a null-position entry is never dropped
			c.destroy();
		});

		it('degrades a throwing position extractor to always-delivered without crashing the flush', () => {
			const c = viewportTracker({ position: () => { throw new Error('boom'); } });
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 1, h: 1, zoom: 1 });

			expect(() => c.update(mockWs({ id: 'M' }), 'board', { x: 5, y: 5 }, platform)).not.toThrow();
			expect(platform.sentTo(viewer).filter((e) => e.event === 'update')).toHaveLength(1);
			c.destroy();
		});

		it('widens the overscan for a zoomed-out subscriber', () => {
			const c = viewportTracker(); // padding 256
			const platform = walkPlatform();
			const z1 = mockWs({ id: 'Z1' });
			const zHalf = mockWs({ id: 'ZH' });
			platform.addSubscriber(z1, CURSOR);
			platform.addSubscriber(zHalf, CURSOR);
			c.viewport(z1, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 1 }); // band edge 1256
			c.viewport(zHalf, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 0.5 }); // pad 512 -> edge 1512

			c.update(mockWs({ id: 'M' }), 'board', { x: 1400, y: 500 }, platform);

			expect(deliveredPositions(platform, z1)).toEqual([]); // 1400 > 1256 -> culled
			expect(deliveredPositions(platform, zHalf)).toEqual(['1400,500']); // 1400 < 1512 -> delivered
			c.destroy();
		});

		it('sends one visible mover as update, several as bulk, and zero as no frame', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const one = mockWs({ id: 'one' });
			const many = mockWs({ id: 'many' });
			const none = mockWs({ id: 'none' });
			[one, many, none].forEach((ws) => platform.addSubscriber(ws, CURSOR));
			c.viewport(one, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			c.viewport(many, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 1 });
			c.viewport(none, 'board', { x: 50000, y: 50000, w: 10, h: 10, zoom: 1 });

			c.update(mockWs({ id: 'a' }), 'board', { x: 5, y: 5 }, platform);
			c.update(mockWs({ id: 'b' }), 'board', { x: 400, y: 400 }, platform);
			c.update(mockWs({ id: 'd' }), 'board', { x: 700, y: 700 }, platform);
			vi.advanceTimersByTime(16);

			expect(platform.sentTo(one).map((e) => e.event)).toEqual(['update']); // just (5,5)
			const manyEv = platform.sentTo(many);
			expect(manyEv).toHaveLength(1);
			expect(manyEv[0].event).toBe('bulk');
			expect(manyEv[0].data).toHaveLength(3);
			expect(platform.sentTo(none)).toHaveLength(0); // empty slice -> no frame
			vi.useRealTimers();
			c.destroy();
		});
	});

	describe('flat versus indexed split', () => {
		// Grid positions and a viewport rect; the visible set is whatever a flat
		// bounds test yields, independent of whether the transient index was built.
		function gridScenario(c, platform, count) {
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 2000, h: 2000, zoom: 1 });
			const pad = 256;
			const expected = [];
			for (let i = 0; i < count; i++) {
				const x = (i * 137) % 4000;
				const y = (i * 251) % 4000;
				c.update(mockWs({ id: 'm' + i }), 'board', { x, y }, platform);
				if (x >= -pad && x <= 2000 + pad && y >= -pad && y <= 2000 + pad) expected.push(`${x},${y}`);
			}
			return { viewer, expected: expected.sort() };
		}

		it('the flat path below the crossover matches a brute-force bounds test', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const { viewer, expected } = gridScenario(c, platform, 20); // < 512 -> flat
			vi.advanceTimersByTime(16);
			expect(deliveredPositions(platform, viewer)).toEqual(expected);
			vi.useRealTimers();
			c.destroy();
		});

		it('the indexed path above the crossover matches the same brute-force test', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const { viewer, expected } = gridScenario(c, platform, 600); // > 512 -> indexed
			vi.advanceTimersByTime(16);
			expect(deliveredPositions(platform, viewer)).toEqual(expected);
			vi.useRealTimers();
			c.destroy();
		});

		it('delivers the full set for a degenerate wide viewport (deliver-all clamp)', () => {
			vi.useFakeTimers();
			const c = createCursor(client, { throttle: 0, topicThrottle: 16, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			// A rect spanning far more cells than there are movers -> clamp to all.
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 100000, h: 100000, zoom: 1 });
			const COUNT = 600; // > 512 -> indexed path where the clamp lives
			for (let i = 0; i < COUNT; i++) c.update(mockWs({ id: 'm' + i }), 'board', { x: i, y: i }, platform);
			// A mover a precise cull would drop must still arrive under the clamp.
			c.update(mockWs({ id: 'far' }), 'board', { x: 9999999, y: 9999999 }, platform);
			vi.advanceTimersByTime(16);
			const bulks = platform.sentTo(viewer).filter((e) => e.event === 'bulk');
			expect(bulks).toHaveLength(1);
			expect(bulks[0].data).toHaveLength(COUNT + 1); // all delivered, including the far one
			vi.useRealTimers();
			c.destroy();
		});
	});

	describe('backpressure skip', () => {
		function bpTracker(maxBufferedBytes) {
			return createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				snapshotIntervalMs: 0,
				backpressure: { enabled: true, maxBufferedBytes },
				select: (ud) => ({ id: ud.id })
			});
		}

		it('skips a subscriber over the cap and lets it catch up on the next flush', () => {
			const c = bpTracker(1024);
			const platform = walkPlatform();
			const healthy = mockWs({ id: 'A' });
			const slow = mockWs({ id: 'S' });
			platform.addSubscriber(healthy, CURSOR);
			platform.addSubscriber(slow, CURSOR);
			platform.setBuffered(slow, 4096); // over the 1 KiB cap

			c.update(mockWs({ id: 'M' }), 'board', { x: 1, y: 1 }, platform);
			expect(platform.sentTo(slow)).toHaveLength(0); // skipped
			expect(platform.sentTo(healthy)).toHaveLength(1); // healthy subscriber unaffected
			expect(c.stats().bpSkips).toBe(1);

			// Next flush: the slow consumer has drained below the cap and receives
			// the latest position, not a replay of the skipped frame.
			platform.reset();
			platform.setBuffered(slow, 0);
			c.update(mockWs({ id: 'M2' }), 'board', { x: 9, y: 9 }, platform);
			const got = platform.sentTo(slow);
			expect(got).toHaveLength(1);
			expect(got[0].data.data).toEqual({ x: 9, y: 9 });
			expect(c.stats().bpSkips).toBe(1); // no new skip
			c.destroy();
		});

		it('never falsely skips a healthy or unknown subscriber that reads 0 buffered', () => {
			const c = bpTracker(1024);
			const platform = walkPlatform();
			const healthy = mockWs({ id: 'A' });
			platform.addSubscriber(healthy, CURSOR); // never setBuffered -> reads 0
			c.update(mockWs({ id: 'M' }), 'board', { x: 1, y: 1 }, platform);
			expect(platform.sentTo(healthy)).toHaveLength(1);
			expect(c.stats().bpSkips).toBe(0);
			c.destroy();
		});

		it('delivers to every healthy subscriber and skips only the over-cap ones', () => {
			const c = bpTracker(1024);
			const platform = walkPlatform();
			const healthy = [mockWs({ id: 'H1' }), mockWs({ id: 'H2' }), mockWs({ id: 'H3' })];
			const slow = [mockWs({ id: 'S1' }), mockWs({ id: 'S2' })];
			[...healthy, ...slow].forEach((ws) => platform.addSubscriber(ws, CURSOR));
			slow.forEach((ws) => platform.setBuffered(ws, 99999));

			c.update(mockWs({ id: 'M' }), 'board', { x: 7, y: 7 }, platform);

			expect(platform.sent.filter((e) => e.event === 'update')).toHaveLength(3); // 3 healthy
			slow.forEach((ws) => expect(platform.sentTo(ws)).toHaveLength(0));
			expect(c.stats().bpSkips).toBe(2);
			expect(c.stats().perSubscriberFlushes).toBe(1);
			c.destroy();
		});

		it('engages the walk via the boolean shorthand', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, backpressure: true, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const a = mockWs({ id: 'A' });
			platform.addSubscriber(a, CURSOR);
			c.update(a, 'board', { x: 1, y: 1 }, platform);
			expect(c.stats().perSubscriberFlushes).toBe(1);
			c.destroy();
		});

		it('degrades to the shared frame on a platform without forEachSubscriber', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, backpressure: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = mockPlatform(); // shared mock: forEachSubscriber is a no-op
			delete platform.forEachSubscriber;
			const a = mockWs({ id: 'A' });
			expect(() => c.update(a, 'board', { x: 1, y: 1 }, platform)).not.toThrow();
			// Fell back to the shared publish fan-out (join + update on published[]).
			expect(platform.published.map((e) => e.event)).toEqual(['join', 'update']);
			expect(platform.sent).toHaveLength(0);
			c.destroy();
		});
	});

	describe('immediate path forces the coalesced walk', () => {
		it('still culls with topicThrottle 0 (the immediate broadcast routes through the walk)', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });

			c.update(mockWs({ id: 'M' }), 'board', { x: 99999, y: 99999 }, platform); // far outside

			expect(deliveredPositions(platform, viewer)).toEqual([]); // culled on the immediate path
			expect(platform.published.filter((e) => e.event === 'update' || e.event === 'bulk')).toHaveLength(0); // no shared position frame
			expect(c.stats().perSubscriberFlushes).toBe(1);
			expect(c.stats().culledEntriesDropped).toBe(1);
			c.destroy();
		});

		it('still skips an over-cap subscriber with topicThrottle 0', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, backpressure: { enabled: true, maxBufferedBytes: 1024 }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const slow = mockWs({ id: 'S' });
			platform.addSubscriber(slow, CURSOR);
			platform.setBuffered(slow, 8192);

			c.update(mockWs({ id: 'M' }), 'board', { x: 1, y: 1 }, platform);

			expect(platform.sentTo(slow)).toHaveLength(0);
			expect(c.stats().bpSkips).toBe(1);
			c.destroy();
		});
	});

	describe('cross-replica cull', () => {
		// A cursor relayed from a peer instance must be culled per local
		// subscriber exactly like a locally-originated one: it has to be a
		// first-class member of the combined entries the per-subscriber walk
		// indexes, not an un-culled emit that reaches every local subscriber.
		// The peer cursor is injected through the same pub/sub message handler
		// the real subscriber registers, so this runs without a live Redis.
		function injectPeer(c, client, platform, key, data) {
			const handler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			const onMessage = handler.listeners.get('message');
			const ch = client.key('cursor:events');
			onMessage(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'board', event: 'update', payload: { key, data }
			}));
		}

		it('culls a far peer-relayed cursor from a tight reporter and delivers it whole-board', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const reporter = mockWs({ id: 'R' });
			const whole = mockWs({ id: 'W' });
			platform.addSubscriber(reporter, CURSOR);
			platform.addSubscriber(whole, CURSOR);
			c.viewport(reporter, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });

			// Prime so the relay subscriber and its message handler are registered.
			c.update(mockWs({ id: 'prime' }), 'board', { x: 5, y: 5 }, platform);
			platform.reset();
			const droppedBefore = c.stats().culledEntriesDropped;

			injectPeer(c, client, platform, 'OTHER:0', { x: 99999, y: 99999 });

			expect(deliveredPositions(platform, reporter)).toEqual([]); // far peer outside the rect -> culled
			expect(deliveredPositions(platform, whole)).toEqual(['99999,99999']); // non-reporter -> whole-board
			expect(c.stats().culledEntriesDropped).toBe(droppedBefore + 1);
			c.destroy();
		});

		it('delivers a peer-relayed cursor that falls inside a reporter rect', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const reporter = mockWs({ id: 'R' });
			platform.addSubscriber(reporter, CURSOR);
			c.viewport(reporter, 'board', { x: 0, y: 0, w: 1000, h: 1000, zoom: 1 });

			c.update(mockWs({ id: 'prime' }), 'board', { x: 5, y: 5 }, platform);
			platform.reset();

			injectPeer(c, client, platform, 'OTHER:0', { x: 500, y: 500 }); // inside the rect

			expect(deliveredPositions(platform, reporter)).toEqual(['500,500']);
			expect(c.stats().culledEntriesDropped).toBe(0);
			c.destroy();
		});
	});

	describe('viewport frames never relay', () => {
		it('does not relay a viewport report to the events channel', () => {
			const relayMessages = [];
			const origPublish = client.redis.publish;
			client.redis.publish = (ch, msg) => {
				try { relayMessages.push(JSON.parse(msg)); } catch { /* ignore */ }
				return origPublish.call(client.redis, ch, msg);
			};

			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const ws = mockWs({ id: 'V' });
			c.viewport(ws, 'board', { x: 0, y: 0, w: 640, h: 480, zoom: 1 });

			// Reporting a viewport publishes nothing to the cross-instance events
			// channel: the rect stays on the local replica. This fails if viewport()
			// ever grows a relay path (the property cull correctness rests on).
			expect(relayMessages).toHaveLength(0);
			expect(relayMessages.find((m) => m && m.event === 'viewport')).toBeUndefined();
			for (const m of relayMessages) {
				const payload = m && m.payload;
				const hasRectShape = payload && typeof payload === 'object' &&
					'w' in payload && 'h' in payload && 'zoom' in payload;
				expect(hasRectShape).toBeFalsy();
			}

			client.redis.publish = origPublish;
			c.destroy();
		});

		it('hooks.message routes a cursor-viewport frame to tracker.viewport for a subscribed ws and does not relay', () => {
			const relayMessages = [];
			const origPublish = client.redis.publish;
			client.redis.publish = (ch, msg) => {
				try { relayMessages.push(JSON.parse(msg)); } catch { /* ignore */ }
				return origPublish.call(client.redis, ch, msg);
			};

			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const ws = mockWs({ id: 'V' });
			ws.subscribe(CURSOR);

			c.hooks.message(ws, {
				data: { type: 'cursor-viewport', topic: 'board', rect: { x: 10, y: 20, w: 640, h: 480, zoom: 2 } },
				platform
			});

			expect(c.viewportFor(ws, 'board')).toEqual({ x: 10, y: 20, w: 640, h: 480, zoom: 2 });
			expect(platform.published).toHaveLength(0);
			expect(relayMessages.find((m) => m && m.payload && typeof m.payload === 'object' && 'w' in m.payload)).toBeUndefined();

			client.redis.publish = origPublish;
			c.destroy();
		});
	});

	describe('teardown and no leak', () => {
		it('drops the reporter and viewport on a single-topic remove', async () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const ws = mockWs({ id: 'V' });
			platform.addSubscriber(ws, CURSOR);
			c.update(ws, 'board', { x: 1, y: 1 }, platform);
			c.viewport(ws, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			expect(c.viewportFor(ws, 'board')).not.toBeNull();
			expect(c.stats().viewportsReported).toBe(1);

			await c.remove(ws, platform, 'board');

			expect(c.viewportFor(ws, 'board')).toBeNull();
			expect(c.stats().viewportsReported).toBe(0);
			c.destroy();
		});

		it('drops the reporter and viewport for a pure spectator that never moved a cursor', async () => {
			// A read-only spectator reports a viewport for a topic it never moves
			// a cursor on, so the topic is absent from its cursor-topic set. A
			// single-topic remove must still drop the rect and its reporter, or the
			// stale rect keeps culling live movers from a viewer that has gone.
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const spectator = mockWs({ id: 'S' });
			platform.addSubscriber(spectator, CURSOR);
			// Report a rect WITHOUT ever calling update() for this connection.
			c.viewport(spectator, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			expect(c.viewportFor(spectator, 'board')).not.toBeNull();
			expect(c.stats().viewportsReported).toBe(1);

			await c.remove(spectator, platform, 'board');

			expect(c.viewportFor(spectator, 'board')).toBeNull();
			expect(c.stats().viewportsReported).toBe(0);

			// A second subscriber still reports a rect, so the topic stays on the
			// walk. The removed spectator is now a non-reporter and must receive a
			// far mover whole-board, not have it withheld by the dropped rect.
			const keeper = mockWs({ id: 'K' });
			platform.addSubscriber(keeper, CURSOR);
			c.viewport(keeper, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			platform.reset();
			c.update(mockWs({ id: 'M' }), 'board', { x: 99999, y: 99999 }, platform);
			expect(deliveredPositions(platform, spectator)).toEqual(['99999,99999']); // non-reporter -> whole-board
			expect(deliveredPositions(platform, keeper)).toEqual([]); // still reporting a tight rect -> culled
			c.destroy();
		});

		it('drops every reporter and viewport on a remove-all', async () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const ws = mockWs({ id: 'V' });
			platform.addSubscriber(ws, '__cursor:board');
			platform.addSubscriber(ws, '__cursor:other');
			c.update(ws, 'board', { x: 1, y: 1 }, platform);
			c.update(ws, 'other', { x: 2, y: 2 }, platform);
			c.viewport(ws, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			c.viewport(ws, 'other', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			expect(c.stats().viewportsReported).toBe(1); // one subscriber, two topics

			await c.remove(ws, platform);

			expect(c.viewportFor(ws, 'board')).toBeNull();
			expect(c.viewportFor(ws, 'other')).toBeNull();
			expect(c.stats().viewportsReported).toBe(0);
			c.destroy();
		});

		it('clears all viewports on clear without resetting lifetime counters', async () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			c.update(mockWs({ id: 'M' }), 'board', { x: 99999, y: 99999 }, platform); // one culled flush
			const droppedBefore = c.stats().culledEntriesDropped;
			const flushesBefore = c.stats().perSubscriberFlushes;
			expect(droppedBefore).toBeGreaterThan(0);
			expect(flushesBefore).toBeGreaterThan(0);

			await c.clear();

			expect(c.viewportFor(viewer, 'board')).toBeNull();
			expect(c.stats().viewportsReported).toBe(0);
			// Lifetime counters survive clear (matching flushes).
			expect(c.stats().culledEntriesDropped).toBe(droppedBefore);
			expect(c.stats().perSubscriberFlushes).toBe(flushesBefore);
			c.destroy();
		});

		it('clears all viewports on destroy', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const viewer = mockWs({ id: 'V' });
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 10, h: 10, zoom: 1 });
			expect(c.stats().viewportsReported).toBe(1);
			c.destroy();
			expect(c.viewportFor(viewer, 'board')).toBeNull();
			expect(c.stats().viewportsReported).toBe(0);
		});
	});

	describe('stats fields', () => {
		it('exposes the per-subscriber-walk counters alongside the scheduler fields', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true } });
			const s = c.stats();
			expect(s).toMatchObject({
				flushes: 0,
				dirtyTopicsCurrent: 0,
				activeTopicsTotal: 0,
				viewportsReported: 0,
				perSubscriberFlushes: 0,
				bpSkips: 0,
				culledEntriesDropped: 0,
				jitterDropped: 0
			});
			c.destroy();
		});

		it('tallies viewportsReported, perSubscriberFlushes, and culledEntriesDropped from a culled flush', () => {
			const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, viewport: { enabled: true }, select: (ud) => ({ id: ud.id }) });
			const platform = walkPlatform();
			const viewer = mockWs({ id: 'V' });
			platform.addSubscriber(viewer, CURSOR);
			c.viewport(viewer, 'board', { x: 0, y: 0, w: 100, h: 100, zoom: 1 });

			c.update(mockWs({ id: 'in' }), 'board', { x: 50, y: 50 }, platform); // inside
			c.update(mockWs({ id: 'out' }), 'board', { x: 99999, y: 99999 }, platform); // culled

			const s = c.stats();
			expect(s.viewportsReported).toBe(1);
			expect(s.perSubscriberFlushes).toBe(2); // two immediate-path walks
			expect(s.culledEntriesDropped).toBe(1);
			c.destroy();
		});
	});
});
