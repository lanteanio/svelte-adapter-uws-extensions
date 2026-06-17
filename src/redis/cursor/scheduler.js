/**
 * Per-topic coalescing flush engine for the Redis-backed cursor tracker: the
 * single-timer scheduler that batches local + peer cursor moves into one wire
 * frame per topic per cycle, runs the per-subscriber viewport/backpressure walk,
 * and relays the local-origin slice to peers. Owns the per-flush scratch, the
 * viewport culler, the tracker-wide tick timer, and the health counters.
 *
 * Cluster-neutral: the cross-instance hop is the injected relay (redis-io); this
 * module only decides WHAT each local subscriber sees and WHEN.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor/scheduler
 */

import { monotonicNow, setTimer, clearTimer } from '../../shared/runtime.js';
import { EVENTS } from './events.js';
import { INDEX_CROSSOVER, createViewportCuller } from './spatial.js';

/**
 * Create the cursor flush scheduler. The shared collections (wsState /
 * topicFlush / dirtyTopics / subViewport / topicReporters) are owned by the
 * tracker and passed in as refs (mutated in place by both sides); emit/emitTo
 * are the shared wire primitives; relay/queueSnapshot come from redis-io. The
 * health counters live here so the hot flush path writes them with no indirection.
 *
 * @param {Object} deps
 */
export function createScheduler({
	emit,
	emitTo,
	relay,
	queueSnapshot,
	mBroadcasts,
	mt,
	perSubscriberWalk,
	viewportEnabled,
	bpEnabled,
	bpMaxBufferedBytes,
	topicThrottleMs,
	position,
	viewportCell,
	viewportPadding,
	wsState,
	topicFlush,
	dirtyTopics,
	subViewport,
	topicReporters
}) {

	/** Record that a subscriber started reporting a viewport for a topic. */
	function addReporter(topic) {
		topicReporters.set(topic, (topicReporters.get(topic) || 0) + 1);
	}
	/** Record that a subscriber stopped reporting a viewport for a topic. */
	function dropReporter(topic) {
		const n = topicReporters.get(topic);
		if (n === undefined) return;
		if (n <= 1) topicReporters.delete(topic);
		else topicReporters.set(topic, n - 1);
	}
	/**
	 * Whether a flush for `topic` must take the per-subscriber walk: always when
	 * backpressure is on (it needs per-socket queue checks), and when culling is
	 * on only once at least one subscriber has reported a viewport for the topic.
	 * @param {string} topic
	 */
	function topicNeedsWalk(topic) {
		return bpEnabled || topicReporters.get(topic) > 0;
	}

	/**
	 * Read a subscriber's last reported viewport rect for a topic, or null if it
	 * never reported one. The null return is the per-subscriber opt-in that keeps
	 * culling safe by construction. Does not create `wsState`. Keyed by
	 * `state.key` (`instanceId:counter`) because `subViewport` is state-key
	 * indexed, so the userData passed to `forEachSubscriber` is not consulted.
	 * @param {any} ws
	 * @param {string} topic
	 * @returns {{ x: number, y: number, w: number, h: number, zoom: number } | null}
	 */
	function lookupViewport(ws, topic) {
		const state = wsState.get(ws);
		if (!state) return null;
		const byTopic = subViewport.get(state.key);
		return byTopic ? (byTopic.get(topic) ?? null) : null;
	}

	// Per-flush scratch for the per-subscriber walk, reused every flush so the
	// walk allocates no new collections. `flushEntries` is the single
	// materialization of a flush's COMBINED entries (local dirty + inbound peer
	// dirty); `flushPos[i]` is entry i's resolved { x, y } (or null when the
	// extractor could not place it - such an entry is always delivered);
	// `alwaysVisible` holds those null-position indices; `inDeliver` is the
	// re-entrancy guard. The viewport spatial index and the cull math live in
	// the culler (see ./cursor/spatial.js).
	/** @type {Array<{ key: string, data: any }>} */
	const flushEntries = [];
	/** @type {Array<{ x: number, y: number } | null>} */
	const flushPos = [];
	/** @type {number[]} */
	const alwaysVisible = [];
	let inDeliver = false;

	// Per-subscriber viewport culler: owns the transient spatial index, the cell
	// pool, the cull-output buffer, and the padded-bounds scratch. Given the
	// combined frame (flushEntries/flushPos/alwaysVisible) and a subscriber rect
	// it returns the visible slice. Cluster-neutral - culling runs per replica.
	const culler = createViewportCuller({ viewportCell, viewportPadding });

	/**
	 * Single timer for the whole tracker. Always points at the next earliest
	 * topic deadline (or null when idle). Replaces the previous per-topic
	 * setTimeout pattern: N pending timers -> 1 pending timer regardless of
	 * topic count. Scheduling overhead is O(dirty topics), not O(active
	 * topics), and a single late fire affects exactly one cycle (target-
	 * anchored, no drift compounding).
	 *
	 * @type {ReturnType<typeof setTimeout> | null}
	 */
	let tickTimer = null;

	/**
	 * Drift accounting for observability. Updated on every flush in `tick()`.
	 * Exposed via the `stats()` accessor; optional `metrics` integration
	 * (Prometheus histogram) is wired separately.
	 */
	let driftSum = 0;
	let driftCount = 0;
	let driftMax = 0;
	let flushCount = 0;

	// Cursor moves dropped by the jitter filter (minMove) before they reached
	// the flush. Always 0 when minMove is 0 (the default). Exposed via stats().
	let jitterDropped = 0;

	// Per-subscriber-walk observability (lifetime counters, like `flushCount`):
	// `perSubscriberFlushes` is how many flushes took the per-subscriber walk
	// rather than the shared combined frame; `bpSkips` is how often the
	// backpressure cap bit; `culledEntriesDropped` is how many combined entries
	// a viewport withheld from a subscriber. Counts only - no topic names, keys,
	// or coordinates. NOT reset by clear()/destroy() (match `flushCount`).
	let perSubscriberFlushes = 0;
	let bpSkips = 0;
	let culledEntriesDropped = 0;

	// Cursor frames go out via emit()/emitTo() (above): the binary publishWire
	// path (0x03 frames) for binary-capable clients, JSON publish/send with
	// { compress: false } otherwise. Cursor is the 60Hz hot path, so it stays
	// UNCOMPRESSED on both paths (binary publishWire defaults off; the JSON
	// fallback opts out): uWS permessage-deflate runs once per recipient, so
	// compressing here would cost ~1-2.5 CPU cores per topic at high subscriber
	// counts. REMOVE stays on the JSON publishBatched path - mass-disconnect OOM
	// safety has no binary batch equivalent, and a {key} frame saves nothing binary.
	function emitJoin(topic, key, user, platform) {
		const payload = { key, user };
		emit('__cursor:' + topic, EVENTS.JOIN, payload, platform);
		relay(topic, EVENTS.JOIN, payload);
	}

	function doBroadcast(topic, key, user, data, platform) {
		mBroadcasts?.inc({ topic: mt(topic) });
		const payload = { key, data };
		emit('__cursor:' + topic, EVENTS.UPDATE, payload, platform);
		queueSnapshot(topic, key, user, data);
		relay(topic, EVENTS.UPDATE, payload);
	}

	/**
	 * Flush a topic's `dirty` + `inboundDirty` maps as a single wire frame to
	 * local subscribers, then relay the local-origin slice to peers.
	 *
	 * - Local subscribers see one combined frame per cycle covering this
	 *   worker's own cursors PLUS any cursors received from peers since the
	 *   last flush. Pre-change, peer-relayed cursors emitted as a separate
	 *   frame immediately on receive, producing tight doublets at subscribers.
	 * - Peers receive only the local-origin slice (relay payload is built
	 *   from `dirty`, not from `inboundDirty`). Re-relaying inbound cursors
	 *   would loop: filtered at the receiver via `instanceId`, but still
	 *   wastes Redis pub/sub bandwidth.
	 * - `queueSnapshot` runs for local-origin only. The originating worker
	 *   owns the Redis HSET for its cursors; receivers must not re-write
	 *   what the origin already wrote (would double the HSET storm).
	 *
	 * Single-entry vs. multi-entry choice mirrors the existing wire shape:
	 * one cursor -> `update {key, data}`, many -> `bulk [{key, data}, ...]`.
	 * Subscribers handle both as cursor-position frames.
	 */
	function flushBoth(topic, state) {
		const entries = [];
		let flushPlatform = null;
		let localCount = 0;

		// Local-origin slice first so we can take a prefix for the relay.
		for (const [k, v] of state.dirty) {
			entries.push({ key: k, data: v.data });
			flushPlatform = v.platform;
			queueSnapshot(topic, k, v.user, v.data);
			localCount++;
		}
		for (const [k, v] of state.inboundDirty) {
			entries.push({ key: k, data: v.data });
			flushPlatform ||= v.platform;
		}

		state.dirty.clear();
		state.inboundDirty.clear();

		if (!flushPlatform || entries.length === 0) return;

		mBroadcasts?.inc({ topic: mt(topic) });
		flushCount++;

		// Per-subscriber walk: engaged only when a per-subscriber reducer is on AND
		// this topic needs the walk (backpressure always, viewport once a subscriber
		// has reported a rect) AND the platform exposes the local subscriber walk
		// (a minimal host or the unit mock without an iterating forEachSubscriber
		// falls through). Each reporting subscriber gets only the combined entries
		// inside its viewport (plus overscan); a non-reporter gets the full combined
		// frame; a backpressured socket is skipped for this flush and catches up next
		// cycle. Otherwise the shared combined-frame fan-out below is byte-identical
		// to the zero-config path.
		const walk =
			perSubscriberWalk &&
			topicNeedsWalk(topic) &&
			typeof flushPlatform.forEachSubscriber === 'function' &&
			!inDeliver;

		if (walk) {
			inDeliver = true;
			try {
				// Single materialization of the combined entries into shared scratch.
				flushEntries.length = 0;
				if (viewportEnabled) { flushPos.length = 0; alwaysVisible.length = 0; }
				for (let i = 0; i < entries.length; i++) {
					const e = entries[i];
					flushEntries.push(e);
					if (viewportEnabled) {
						let pos = null;
						// A buggy or slow app extractor must not crash the flush.
						try { pos = position(e.data); } catch { pos = null; }
						if (pos && (typeof pos.x !== 'number' || typeof pos.y !== 'number'
							|| !Number.isFinite(pos.x) || !Number.isFinite(pos.y))) {
							pos = null;
						}
						flushPos.push(pos);
						if (pos === null) alwaysVisible.push(flushEntries.length - 1);
					}
				}
				const n = flushEntries.length;
				const fullTopic = '__cursor:' + topic;
				const indexed = viewportEnabled && n >= INDEX_CROSSOVER;
				if (indexed) culler.buildFlushCells(flushPos, n);
				flushPlatform.forEachSubscriber(fullTopic, (ws) => {
					if (bpEnabled && flushPlatform.bufferedAmount(ws) > bpMaxBufferedBytes) {
						bpSkips++;
						return;
					}
					let slice = flushEntries;
					if (viewportEnabled) {
						const rect = lookupViewport(ws, topic);
						// A non-reporter (null rect) is whole-board and never culled.
						if (rect !== null) slice = indexed ? culler.cullIndexed(rect, flushEntries, flushPos, alwaysVisible) : culler.cullDirect(rect, flushEntries, flushPos);
					}
					const len = slice.length;
					// Count entries withheld by the cull, including a fully culled slice.
					if (slice !== flushEntries) culledEntriesDropped += n - len;
					if (len === 0) return; // nothing visible to this subscriber this flush
					if (len === 1) {
						emitTo(ws, fullTopic, EVENTS.UPDATE, slice[0], flushPlatform);
					} else {
						emitTo(ws, fullTopic, EVENTS.BULK, slice, flushPlatform);
					}
				});
				perSubscriberFlushes++;
				if (indexed) culler.releaseFlushCells();
			} finally {
				inDeliver = false;
			}
		} else if (entries.length === 1) {
			// Shared combined frame (local + inbound) - unchanged zero-config path.
			emit('__cursor:' + topic, EVENTS.UPDATE, entries[0], flushPlatform);
		} else {
			emit('__cursor:' + topic, EVENTS.BULK, entries, flushPlatform);
		}

		// Relay LOCAL-ORIGIN slice only; never re-relay what came from peers.
		if (localCount > 0) {
			if (localCount === 1) {
				relay(topic, EVENTS.UPDATE, entries[0]);
			} else {
				relay(topic, EVENTS.BULK, entries.slice(0, localCount));
			}
		}
	}

	/**
	 * Scheduler tick. Walks `dirtyTopics`, flushes any topic whose deadline
	 * (`lastFlush + topicThrottleMs`) has passed, and re-arms `tickTimer`
	 * for the next earliest pending deadline. Topics whose deadline has not
	 * yet passed stay in `dirtyTopics` for the next tick.
	 *
	 * Target-anchored advance: on flush, `lastFlush` is set to the deadline
	 * (not the actual fire time) so a single late tick does not compound
	 * drift on subsequent cycles. If we fell behind by more than one cycle
	 * (event loop saturation > `topicThrottleMs`), `lastFlush` resets to
	 * `now` to avoid queueing phantom catch-up fires that would all hit the
	 * next event loop turn.
	 */
	function tick() {
		tickTimer = null;
		const nowTs = monotonicNow();
		let nextDeadline = Infinity;

		for (const topic of dirtyTopics) {
			const state = topicFlush.get(topic);
			if (!state) { dirtyTopics.delete(topic); continue; }
			if (state.dirty.size === 0 && state.inboundDirty.size === 0) {
				dirtyTopics.delete(topic);
				continue;
			}
			const deadline = state.lastFlush + topicThrottleMs;
			if (deadline <= nowTs) {
				const drift = nowTs - deadline;
				driftSum += drift;
				driftCount++;
				if (drift > driftMax) driftMax = drift;

				flushBoth(topic, state);
				dirtyTopics.delete(topic);

				// Target-anchored: advance lastFlush by the cadence amount.
				// Multi-cycle backlog collapse to `nowTs` so the next
				// broadcast's `monotonicNow() - lastFlush >= topicThrottleMs`
				// delay computation does not fire every queued cycle on
				// this turn.
				state.lastFlush = drift < topicThrottleMs ? deadline : nowTs;
			} else if (deadline < nextDeadline) {
				nextDeadline = deadline;
			}
		}

		if (nextDeadline !== Infinity) {
			tickTimer = setTimer(tick, Math.max(0, nextDeadline - monotonicNow()));
		}
		// else: scheduler goes idle until next broadcast() / enqueueInbound().
	}

	function armTick(delay) {
		if (tickTimer !== null) return;
		tickTimer = setTimer(tick, delay);
	}

	/**
	 * Schedule a local cursor for the next coalesced flush. Always-tick: every
	 * call appends to `state.dirty`, adds the topic to `dirtyTopics`, and arms
	 * the tracker-wide tick timer. NO leading-edge synchronous fire and NO
	 * microtask defer.
	 *
	 * Why: uWS dispatches each WS message as its own JS task, and N-API
	 * drains microtasks at the C++/JS boundary between dispatches. A
	 * `queueMicrotask`-deferred flush fires BEFORE the next socket's message
	 * handler runs, so cross-socket coalescing is impossible at the microtask
	 * level. `setTimeout(0)` is in libuv's timers phase and fires only after
	 * the poll phase processes every ready message on every socket - so all
	 * broadcasts dispatched in the same loop iteration end up in one flush
	 * regardless of how many task boundaries separate them.
	 *
	 * 0.5.5/0.5.6 shipped a `queueMicrotask` + `pendingMicroflush` variant
	 * built on the wrong dispatch-model assumption (that co-arriving
	 * broadcasts share a JS task). Demo measured ~99% single-cursor UPDATE /
	 * ~1% BULK at 1000-mover load on the deployed 0.5.6 - essentially the
	 * pre-fix shape. The bench validated the assumed input shape (all
	 * broadcasts in one synchronous task) instead of the input shape uWS
	 * actually produces (per-message tasks separated by microtask drains).
	 *
	 * First-cursor latency cost of always-tick: up to `topicThrottleMs` (16ms
	 * default, one frame budget) before fanout. Below the perceptual floor
	 * for cursors. Same trade-off the adapter's bundled cursor plugin makes.
	 */
	function broadcast(topic, key, user, data, platform) {
		// The immediate path (topicThrottle: 0) bypasses the coalesced flush, where
		// the per-subscriber walk lives. When no reducer is engaged for this topic,
		// keep the legacy immediate single-mover emit. When a reducer IS engaged,
		// the move must instead go through the combined `flushBoth` walk so culling
		// / backpressure cannot silently no-op for a `topicThrottle: 0` app. Route
		// it through the dirty map and flush synchronously so the immediate path's
		// synchronous-delivery contract holds while still coalescing whatever
		// co-arrived in this loop turn into one combined frame and one walk.
		// `&&` short-circuits, so the throttled path never runs topicNeedsWalk.
		if (topicThrottleMs <= 0 && !(perSubscriberWalk && topicNeedsWalk(topic))) {
			doBroadcast(topic, key, user, data, platform);
			return;
		}

		let state = topicFlush.get(topic);
		if (!state) {
			// Anchor `lastFlush` one cycle in the past so the first broadcast
			// is treated as "cycle ready" with zero drift on the very first
			// tick. Without this, `Date.now() - 0` would be a huge "lateness"
			// that pollutes the drift stats forever.
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: monotonicNow() - topicThrottleMs };
			topicFlush.set(topic, state);
		}
		state.dirty.set(key, { user, data, platform });
		dirtyTopics.add(topic);

		if (topicThrottleMs <= 0) {
			// Immediate path with a reducer engaged: flush the combined entries now.
			dirtyTopics.delete(topic);
			flushBoth(topic, state);
			return;
		}

		const elapsed = monotonicNow() - state.lastFlush;
		const delay = elapsed >= topicThrottleMs ? 0 : topicThrottleMs - elapsed;
		armTick(delay);
	}

	/**
	 * Schedule a peer-relayed cursor for the next coalesced flush. Symmetric
	 * to `broadcast()`: same leading/trailing edge semantics, but inbound
	 * entries route through `state.inboundDirty` so they are visible to
	 * local subscribers on the next flush WITHOUT being re-relayed to peers
	 * (which would loop) and WITHOUT being written to Redis (origin owns
	 * the HSET).
	 *
	 * The peer's cross-replica end-to-end latency gains up to one
	 * `topicThrottleMs` of coalescing delay on the receiver side. Cursors
	 * are already throttled in the 8-16ms range; adding 8-16ms is well
	 * below the ~50-100ms human perception threshold for cursor lag. The
	 * smoothness win (one frame per subscriber per cycle instead of two)
	 * is the structural benefit.
	 */
	function enqueueInbound(topic, key, data, platform) {
		// Mirror broadcast(): the immediate path bypasses the coalesced flush where
		// the per-subscriber walk lives, so force coalescing when a reducer is
		// engaged. A peer-origin cursor must be a first-class member of the combined
		// entries the local walk culls against, not an un-culled immediate emit that
		// would reach every local subscriber regardless of its viewport.
		// `&&` short-circuits, so the throttled path never runs topicNeedsWalk.
		if (topicThrottleMs <= 0 && !(perSubscriberWalk && topicNeedsWalk(topic))) {
			// Legacy immediate mode (matches old receiver behavior).
			emit('__cursor:' + topic, EVENTS.UPDATE, { key, data }, platform, { relay: false });
			return;
		}

		let state = topicFlush.get(topic);
		if (!state) {
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: monotonicNow() - topicThrottleMs };
			topicFlush.set(topic, state);
		}
		state.inboundDirty.set(key, { data, platform });
		dirtyTopics.add(topic);

		if (topicThrottleMs <= 0) {
			// Immediate path with a reducer engaged: flush the combined entries now,
			// so the peer cursor is culled per local subscriber rather than emitted
			// un-culled to everyone.
			dirtyTopics.delete(topic);
			flushBoth(topic, state);
			return;
		}

		// Symmetric with `broadcast()` always-tick: both source paths share
		// the same `state` and the same tracker-wide tick timer, so a local
		// broadcast and a peer-relayed inbound landing in the same loop
		// iteration ship together as one combined frame at the next tick.
		const elapsed = monotonicNow() - state.lastFlush;
		const delay = elapsed >= topicThrottleMs ? 0 : topicThrottleMs - elapsed;
		armTick(delay);
	}

	/** Record a move dropped by the update() jitter filter (minMove). */
	function recordJitterDrop() {
		jitterDropped++;
	}

	/** Health snapshot for tracker.stats() (it adds activeTopicsTotal = topics.size). */
	function statsSnapshot() {
		return {
			flushes: flushCount,
			driftMeanMs: driftCount > 0 ? driftSum / driftCount : 0,
			driftMaxMs: driftMax,
			dirtyTopicsCurrent: dirtyTopics.size,
			jitterDropped,
			viewportsReported: subViewport.size,
			perSubscriberFlushes,
			bpSkips,
			culledEntriesDropped
		};
	}

	/**
	 * Reset the scheduler-private flush state on tracker clear()/destroy(): stop
	 * the tick timer, release the per-flush scratch + culler index, drop the
	 * re-entrancy guard. The lifetime counters are intentionally left alone
	 * (they match flushCount semantics); the shared collections are cleared by
	 * the tracker.
	 */
	function reset() {
		if (tickTimer !== null) { clearTimer(tickTimer); tickTimer = null; }
		flushEntries.length = 0;
		flushPos.length = 0;
		alwaysVisible.length = 0;
		culler.reset();
		inDeliver = false;
	}

	return {
		emitJoin,
		broadcast,
		enqueueInbound,
		addReporter,
		dropReporter,
		lookupViewport,
		recordJitterDrop,
		statsSnapshot,
		reset
	};
}
