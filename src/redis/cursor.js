/**
 * Redis-backed cursor / ephemeral state plugin for svelte-adapter-uws.
 *
 * Same API as the core createCursor plugin, but cursor positions are shared
 * across instances via Redis. Each instance throttles locally (same
 * leading/trailing edge logic as the core), then relays broadcasts through
 * Redis pub/sub so subscribers on other instances see cursor updates.
 *
 * Wire shape (channel `__cursor:{topic}`):
 *   - `catalog`  [{key, user}, ...]   - sent on attach + on subscriber-startup
 *                                       reconcile. Roster of users on this topic.
 *   - `join`     {key, user}          - emitted once per (ws, topic) the first time
 *                                       that ws updates on the topic. Cross-replica.
 *   - `update`   {key, data}          - single-mover position update.
 *   - `bulk`     [{key, data}, ...]   - coalesced multi-mover positions.
 *   - `remove`   {key}                - user is gone (catalog + positions cleared).
 *   - `time`     {t}                  - server wall clock, leads every snapshot reply.
 *                                       Single-target; never relayed.
 *   - `you`      {key}                - the receiving connection's own roster key,
 *                                       sent once per (connection, topic) before its
 *                                       first join and in every snapshot reply between
 *                                       `time` and `catalog`. Single-target; never
 *                                       relayed (each replica names its own sockets).
 *
 * Separating user metadata (catalog) from per-frame positions cuts the per-flush
 * wire payload from ~100 bytes per cursor to ~16 bytes per cursor, and cuts the
 * Redis pub/sub relay envelope by the same factor. Catalog churn is O(joins +
 * leaves), not O(active-cursor count x rate).
 *
 * Storage layout:
 *   - Hash `{prefix}cursor:{topic}` - field = connectionKey, value = JSON
 *     `{user, data, ts}`. Writes are coalesced onto a `snapshotIntervalMs`
 *     timer; the broadcast path does not write to Redis directly. New joiners
 *     reading the hash see at most `snapshotIntervalMs`-stale data, which is
 *     fine for cursor reconcile.
 *   - Channel `{prefix}cursor:events` - pub/sub for join/update/bulk/remove relay.
 *
 * Hash entries expire via TTL so stale cursors from crashed instances
 * get cleaned up automatically.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor
 */

import {
	randomBytes,
	now,
	monotonicNow,
	wallEpoch,
	setTimer,
	clearTimer,
} from '../shared/runtime.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink, scanKeys } from '../shared/redis-scan.js';
import { MAX_CURSOR_WS, MAX_CURSOR_TOPICS } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { WsClosedError } from '../shared/errors.js';
import { addWsSubscription } from '../shared/ws-subscriptions.js';
import { createCursorWireCodec } from 'svelte-adapter-uws/plugins/cursor';
import { EVENTS } from './cursor/events.js';
import { _warnCursorHooksMessageShape } from './cursor/diagnostics.js';
import { resolveCursorOptions } from './cursor/options.js';
import { createRemoveBuffer } from './cursor/remove-buffer.js';
import { createRedisIo } from './cursor/redis-io.js';
import { createScheduler } from './cursor/scheduler.js';

export { WsClosedError };


/**
 * @typedef {Object} RedisCursorOptions
 * @property {number} [throttle=16] - Minimum ms between broadcasts per user per topic.
 *   Trailing-edge timer fires to ensure the final position is always sent.
 *   Default 16 (60Hz) matches the world-state tick rate so an individual cursor's
 *   motion stays smooth at the per-peer wire rate set by `topicThrottle`.
 * @property {number} [topicThrottle=16] - World-state tick rate, in ms.
 *   Per-topic aggregate cap on broadcasts: each topic emits at most one frame
 *   per window, carrying the latest position for every cursor that moved.
 *   Bandwidth per peer scales with active-mover count, not with mover-count
 *   times per-mover rate. Default 16 (60Hz) suits typical small-to-medium
 *   rooms; raise to 33 (30Hz) for high-density rooms where wire bytes matter.
 *   0 disables the tick; per-cursor `throttle` then governs broadcast rate.
 * @property {number} [snapshotIntervalMs=100] - How often to flush coalesced
 *   cursor positions to Redis HSET. The wire/relay path runs on the per-flush
 *   cadence (above) and does not wait for HSET; this timer only governs the
 *   Redis snapshot used for new-joiner reconcile and cross-instance startup
 *   reconcile. 100ms staleness on the reconcile path is fine for cursors.
 *   0 disables coalescing and reverts to per-flush HSET (legacy behavior).
 * @property {(userData: any) => any} [select] - Extract user-identifying data from userData.
 *   Defaults to the full userData.
 * @property {number} [ttl=30] - TTL in seconds for hash entries. Should be longer than
 *   the expected gap between updates. Entries are refreshed on every snapshot tick.
 * @property {(data: any) => ({ x: number, y: number } | null)} [position] - Extract a
 *   `{x, y}` coordinate from cursor `data` for the `minMove` jitter filter. Defaults to
 *   reading finite `data.x` / `data.y`. Return null (or throw) to opt a frame out of the
 *   filter (it is always delivered). Only consulted when `minMove > 0`.
 * @property {number} [minMove=0] - Minimum movement (Chebyshev distance in the units
 *   `position` returns) from the last broadcast position before a cursor move is fanned
 *   out. A burst of sub-threshold wobble is dropped at ingest, then a debounced settle
 *   delivers the final resting position once movement stops. 0 (default) disables the
 *   filter, broadcasting every accepted move. For integer-pixel data, `minMove: 1` drops
 *   exact-repeat frames at no visual cost. Mirrors the in-memory cursor plugin's `minMove`.
 * @property {boolean | { enabled?: boolean, padding?: number, cell?: number }} [viewport] - Per-subscriber
 *   viewport culling (opt-in, default off). When enabled, each subscriber that reports a
 *   viewport rect (via a `{type:'cursor-viewport', topic, rect}` frame or `tracker.viewport`)
 *   receives only the moving cursors inside that rect plus a `padding` overscan; a subscriber
 *   that never reports a rect is treated as whole-board and is never culled. Culling runs
 *   against the combined local + peer cursor set on each replica, so a peer-origin cursor is
 *   culled identically to a local one and the rect never crosses Redis. `true` is shorthand
 *   for `{ enabled: true }`. `padding` (default 256) is the board-unit overscan, widened by
 *   `1/zoom` when zoomed out; `cell` (default 256) is the spatial-index cell size.
 * @property {boolean | { enabled?: boolean, maxBufferedBytes?: number }} [backpressure] - Per-subscriber
 *   backpressure drop (opt-in, default off). When enabled, a subscriber whose queued bytes
 *   exceed `maxBufferedBytes` (default 1048576) is skipped for the current flush and catches
 *   up on the next flush with the latest coalesced positions - cursors are latest-value, so a
 *   skipped subscriber never accumulates a backlog. `true` is shorthand for `{ enabled: true }`.
 */

/**
 * @typedef {Object} CursorEntry
 * @property {string} key - Unique connection key.
 * @property {any} user - Selected user data.
 * @property {any} data - Latest cursor/position data.
 */

/**
 * @typedef {Object} RedisCursorTracker
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => Promise<void>} attach
 * @property {(ws: any, topic: string, platform: import('svelte-adapter-uws').Platform) => void} detach
 * @property {(ws: any, topic: string, data: any, platform: import('svelte-adapter-uws').Platform) => void} update
 * @property {(ws: any, topic: string, rect: any) => void} viewport - Record a subscriber's viewport rect for a topic (viewport culling)
 * @property {(ws: any, topic: string) => ({ x: number, y: number, w: number, h: number, zoom: number } | null)} viewportFor - The subscriber's last reported viewport rect for a topic, or null
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, topic?: string) => Promise<void>} remove
 * @property {(topic: string) => Promise<CursorEntry[]>} list
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop the Redis subscriber
 * @property {() => { flushes: number, driftMeanMs: number, driftMaxMs: number, dirtyTopicsCurrent: number, activeTopicsTotal: number, jitterDropped: number, viewportsReported: number, perSubscriberFlushes: number, bpSkips: number, culledEntriesDropped: number }} stats - Scheduler health snapshot
 */

/**
 * Create a Redis-backed cursor tracker.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisCursorOptions} [options]
 * @returns {RedisCursorTracker}
 */
export function createCursor(client, options = {}) {
	const {
		throttleMs,
		topicThrottleMs,
		snapshotIntervalMs,
		select,
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
	} = resolveCursorOptions(options);

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const channel = client.key('cursor:events');

	// Binary wire codec (cursor.protocol:2 full-string / :3 short-id dict), built
	// by the adapter's shared factory so the cluster variant speaks the IDENTICAL
	// wire to the bundled in-memory cursor plugin. null when `binary: false`. The
	// per-connection dictionary state lives in the framework (publishWire encodes
	// per-subscriber against it), so we just hand the codec to publishWire.
	const wireCodec = createCursorWireCodec(options);

	/**
	 * Broadcast a cursor wire event to local subscribers. Prefers the binary
	 * publishWire (0x03 frames, uncompressed - the 60Hz hot path) and falls back
	 * to JSON publish ({ compress: false }) when binary is off or the platform
	 * lacks the wire methods (e.g. the unit-test mock). `opts` carries `relay`
	 * (the cross-instance fan-out is this plugin's own Redis relay).
	 * @param {string} fullTopic
	 * @param {string} event
	 * @param {any} data
	 * @param {import('svelte-adapter-uws').Platform} platform
	 * @param {{ relay?: boolean }} [opts]
	 */
	function emit(fullTopic, event, data, platform, opts) {
		if (wireCodec && typeof platform.publishWire === 'function') {
			platform.publishWire(fullTopic, event, data, wireCodec, opts);
		} else {
			platform.publish(fullTopic, event, data, opts ? { ...opts, compress: false } : { compress: false });
		}
	}

	/**
	 * Single-target variant of {@link emit} (snapshot catalog + positions).
	 * @param {any} ws
	 * @param {string} fullTopic
	 * @param {string} event
	 * @param {any} data
	 * @param {import('svelte-adapter-uws').Platform} platform
	 */
	function emitTo(ws, fullTopic, event, data, platform) {
		if (wireCodec && typeof platform.sendWire === 'function') {
			platform.sendWire(ws, fullTopic, event, data, wireCodec);
		} else {
			platform.send(ws, fullTopic, event, data, { compress: false });
		}
	}

	const validator = createBusValidator({
		maxBytes: options.maxEnvelopeBytes,
		allowSystemTopics: false,
		allowedSystemTopics: []
	});

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mUpdates = m?.counter('cursor_updates_total', 'Cursor update calls', ['topic']);
	const mBroadcasts = m?.counter('cursor_broadcasts_total', 'Cursor broadcasts sent', ['topic']);
	const mThrottled = m?.counter('cursor_throttled_total', 'Cursor updates deferred by throttle', ['topic']);
	const mAttachesAborted = m?.counter('cursor_attaches_aborted_total', 'Cursor attach calls that aborted because the websocket closed before `ws.subscribe` could complete. Symmetric with `presence_joins_aborted_total`; same `WS_CLOSED` cause.', ['topic', 'reason']);

	const warnSensitive = createSensitiveWarner('redis/cursor');

	let connCounter = 0;

	function safeUserData(ws) {
		// Closed-WS race: getWsState (called from `update`) may reach here
		// after an `await` that outlasted the socket; `ws.getUserData()`
		// throws on a freed native handle. Fall back to an empty userData
		// rather than crashing the worker. Matches adapter 0.5.5's
		// `plugins/cursor/server.js getWsState` guard.
		let raw = {};
		if (typeof ws.getUserData === 'function') {
			try { raw = ws.getUserData(); } catch { raw = {}; }
		}
		if (!raw || typeof raw !== 'object') return {};
		const { __subscriptions, remoteAddress, ...safeData } = raw;
		return safeData;
	}

	/**
	 * Per-ws state: connection key, selected user data, and which topics this ws
	 * has already announced (`join` emitted). `topics` doubles as the
	 * already-announced set - presence in the set means a join has fired.
	 * @type {Map<any, { key: string, user: any, topics: Set<string> }>}
	 */
	const wsState = new Map();

	/**
	 * Per-topic local cursor state. Drives the per-(ws,topic) throttle and the
	 * post-disconnect timer cleanup. The Redis snapshot is the cross-replica
	 * source of truth; this map is the local-replica cache.
	 * @type {Map<string, Map<string, { user: any, data: any, lastBroadcast: number, timer: any, lastSentPos?: { x: number, y: number }, settleTimer?: any }>>}
	 */
	const topics = new Map();

	/** @type {Set<string>} */
	const activeTopics = new Set();

	function getWsState(ws) {
		let state = wsState.get(ws);
		if (!state) {
			if (wsState.size >= MAX_CURSOR_WS) {
				throw new Error(
					`redis cursor: local ws count exceeded ${MAX_CURSOR_WS} on this instance`
				);
			}
			const selected = select(safeUserData(ws));
			warnSensitive(selected);
			const user = stripInternal(selected);
			try { JSON.stringify(user); } catch {
				throw new Error('redis cursor: select() must return JSON-serializable data');
			}
			state = {
				key: instanceId + ':' + (++connCounter),
				user,
				topics: new Set()
			};
			wsState.set(ws, state);
		}
		return state;
	}

	/**
	 * Per-topic aggregate flush state.
	 *
	 * - `dirty`: locally-originated cursors. Flushed locally AND relayed.
	 * - `inboundDirty`: cursors received from peer instances via Redis pub/sub.
	 *   Flushed locally ONLY (re-relaying would loop). Kept separate from
	 *   `dirty` so the relay payload is structurally a subset of the local
	 *   flush, not a per-entry origin check.
	 * - `lastFlush`: target-anchored timestamp of the most recent flush.
	 *   Advanced by `topicThrottleMs` per cycle (not to actual fire time) so
	 *   a single late tick does not compound drift on subsequent cycles.
	 *   Initialized to `monotonicNow() - topicThrottleMs` so the first broadcast
	 *   on a new topic is "cycle ready" without polluting drift stats with
	 *   the full `monotonicNow()` lateness an init of 0 would imply.
	 *
	 * @type {Map<string, { dirty: Map<string, { user: any, data: any, platform: any }>, inboundDirty: Map<string, { data: any, platform: any }>, lastFlush: number }>}
	 */
	const topicFlush = new Map();

	/**
	 * Single scheduler-driven set: topics with at least one dirty entry
	 * awaiting flush. Bounded by mover count, not topic count, so the
	 * per-tick walk does not scan idle topics. Updated synchronously on
	 * `broadcast()` / `enqueueInbound()` and on every tick.
	 *
	 * @type {Set<string>}
	 */
	const dirtyTopics = new Set();

	/**
	 * Per-(subscriber, topic) viewport rect, recorded from the inbound
	 * `cursor-viewport` frame. Outer key is the subscriber's `wsState` key
	 * (`instanceId:counter`); inner key is the topic. Read by the per-subscriber
	 * walk, which never culls a subscriber that has not reported a rect (a
	 * non-reporter is whole-board). Subscriber-keyed: torn down only with the
	 * subscriber on `remove()` / `clear()` / `destroy()`, never by topic eviction
	 * or an empty topic.
	 * @type {Map<string, Map<string, { x: number, y: number, w: number, h: number, zoom: number }>>}
	 */
	const subViewport = new Map();

	/**
	 * Count of distinct subscribers currently reporting a viewport per topic.
	 * Lets a viewport-enabled topic with zero reporters keep the shared
	 * combined-frame fan-out instead of paying the O(local connections)
	 * per-subscriber walk for the same bytes. Maintained alongside `subViewport`:
	 * incremented when a `(subscriber, topic)` rect is first recorded, decremented
	 * when the subscriber is removed, cleared in `clear()` / `destroy()`.
	 * @type {Map<string, number>}
	 */
	const topicReporters = new Map();

	// Coalesced REMOVE buffer: mass-disconnect O(N^2) fan-out guard (see
	// ./cursor/remove-buffer.js for the setTimeout(0) cross-socket coalescing).
	const { queueRemove, clear: clearRemoveBuffer } = createRemoveBuffer();

	function onPeerMessage(parsed, platform) {
		// Receiver-side coalescing for high-frequency cursor-position
		// events. UPDATE / BULK enqueue into the local topic's
		// inboundDirty map so the NEXT local flush emits one combined
		// frame covering local + peer cursors. Pre-change, peer-
		// relayed frames published immediately on receive, producing
		// tight doublets at subscribers (one frame per worker per
		// cycle, ms apart). Now one frame per subscriber per cycle
		// regardless of worker count.
		//
		// CATALOG / JOIN / REMOVE stay immediate: low-frequency
		// roster events where coalescing would add latency without
		// smoothness benefit.
		if (parsed.event === EVENTS.UPDATE && parsed.payload && typeof parsed.payload.key === 'string') {
			enqueueInbound(parsed.topic, parsed.payload.key, parsed.payload.data, platform);
		} else if (parsed.event === EVENTS.BULK && Array.isArray(parsed.payload)) {
			for (const entry of parsed.payload) {
				if (entry && typeof entry.key === 'string') {
					enqueueInbound(parsed.topic, entry.key, entry.data, platform);
				}
			}
		} else if (parsed.event === EVENTS.REMOVE && parsed.payload && typeof parsed.payload.key === 'string') {
			// Peer-relayed cursor removes are coalesced through the
			// same tick buffer the local close path uses. Without
			// this, a mass-disconnect on one instance produces an
			// equally-sized O(N) immediate-publish storm on every
			// other instance, propagating the OOM risk cluster-wide.
			queueRemove(parsed.topic, parsed.payload.key, platform);
		} else {
			emit(
				'__cursor:' + parsed.topic,
				parsed.event,
				parsed.payload,
				platform,
				{ relay: false }
			);
		}
	}

	const redisIo = createRedisIo({
		client,
		redis,
		channel,
		instanceId,
		b,
		validator,
		cursorTtl,
		snapshotIntervalMs,
		activeTopics,
		emit,
		onMessage: onPeerMessage,
		queueRemove
	});
	const {
		ensureSubscriber,
		startCleanupTimer,
		startSnapshotTimer,
		stopCleanupTimer,
		relay,
		queueSnapshot,
		hashKey,
		broadcastRemove,
		getTopicPending,
		dropTopicPending,
		dropKeyPending,
		removeKeysBatch,
		resetPending,
		dispose
	} = redisIo;

	const scheduler = createScheduler({
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
	});
	const {
		emitJoin,
		broadcast,
		enqueueInbound,
		addReporter,
		dropReporter,
		lookupViewport,
		recordJitterDrop,
		statsSnapshot,
		reset: resetScheduler
	} = scheduler;

	/** @type {RedisCursorTracker} */
	const tracker = {
		async attach(ws, topic, platform) {
			// Raw `ws.subscribe` (uWS-native) NOT `platform.subscribe`. As of
			// adapter 0.5.5 `platform.subscribe` swallows uWS's "closed
			// websocket" throw and returns the same `null` sentinel it returns
			// on success, so a try/catch around `platform.subscribe` cannot
			// distinguish closed-ws from success without racing on
			// `platform.closedWsAborts`. uWS-native `ws.subscribe` still throws
			// on closed-ws, so the throw-WsClosedError contract holds. Mirrors
			// what `presence.join` already does with `ws.subscribe('__presence:...')`.
			try {
				ws.subscribe('__cursor:' + topic);
			} catch {
				// No state to roll back (the throw fires before snapshot()
				// could allocate this connection's identity). Throw so the
				// caller can distinguish a no-op-and-rollback from a successful attach;
				// without this the RPC metric reports `status=ok` for
				// connections that never received cursor frames.
				mAttachesAborted?.inc({ topic: mt(topic), reason: 'ws_closed' });
				throw new WsClosedError('cursor.attach', topic);
			}
			// Mirror into the subscription registry so the adapter's binary
			// publish walk delivers to this member (native membership alone is
			// invisible to it). detach() routes through platform.unsubscribe,
			// which is already registry-aware.
			addWsSubscription(ws, '__cursor:' + topic);
			// snapshot() itself swallows ws-closed during platform.send (the
			// state is already committed; clients recover via the next bulk
			// frame). Intentional asymmetry with subscribe failure above.
			await tracker.snapshot(ws, topic, platform);
		},

		detach(ws, topic, platform) {
			try {
				platform.unsubscribe(ws, '__cursor:' + topic);
			} catch { /* closed */ }
		},

		update(ws, topic, data, platform) {
			mUpdates?.inc({ topic: mt(topic) });
			ensureSubscriber(platform);

			const state = getWsState(ws);
			const isFirstOnTopic = !state.topics.has(topic);
			state.topics.add(topic);
			if (!activeTopics.has(topic) && activeTopics.size === 0) {
				activeTopics.add(topic);
				startCleanupTimer();
				startSnapshotTimer();
			} else {
				activeTopics.add(topic);
				startSnapshotTimer();
			}

			let topicMap = topics.get(topic);
			if (!topicMap) {
				if (topics.size >= MAX_CURSOR_TOPICS) {
					throw new Error(
						`redis cursor: local topic count exceeded ${MAX_CURSOR_TOPICS} on this instance`
					);
				}
				topicMap = new Map();
				topics.set(topic, topicMap);
			}

			if (isFirstOnTopic) {
				// Tell the mover which roster key is its own BEFORE the join
				// broadcast announces that key to everyone (the mover included),
				// so the client can attribute the join - and every later frame -
				// to itself. Single-target via emitTo, so the event stays on
				// this replica by construction and never enters the Redis relay:
				// the key only means something to the connection it names. The
				// binary codec declines the event, so it rides the JSON fallback
				// even on a binary-capable connection, and an older client's
				// merge ignores it as an unknown event. `state.topics` is the
				// once-per-(ws, topic) gate, the same one that gates the join.
				emitTo(ws, '__cursor:' + topic, EVENTS.YOU, { key: state.key }, platform);
				emitJoin(topic, state.key, state.user, platform);
			}

			let entry = topicMap.get(state.key);
			const nowTs = monotonicNow();

			if (!entry) {
				entry = { user: state.user, data, lastBroadcast: 0, timer: null };
				topicMap.set(state.key, entry);
			}

			// Jitter filter: drop a sub-threshold wobble before it reaches the
			// flush scheduler (and before any cross-instance relay). Measured
			// against the last BROADCAST position (`lastSentPos`, set only on a
			// real broadcast below) so repeated small moves never accumulate into
			// a delivered jump. A dropped move stays as entry.data (so
			// list()/snapshot() see the true position) and arms a debounced settle
			// timer so the final resting position is delivered once movement stops
			// - the cursor is never left stranded at a stale point. pos === null
			// (no usable coordinate) always passes through.
			let pos = null;
			if (minMove > 0) {
				// Re-arm point for the debounced settle: clear any pending timer; a
				// drop below re-arms it, a real broadcast leaves it cleared.
				if (entry.settleTimer) { clearTimer(entry.settleTimer); entry.settleTimer = null; }
				pos = finitePosition(data);
				if (
					pos && entry.lastSentPos &&
					Math.max(Math.abs(pos.x - entry.lastSentPos.x), Math.abs(pos.y - entry.lastSentPos.y)) < minMove
				) {
					entry.data = data; // keep latest for a real move later + snapshot
					entry.user = state.user;
					recordJitterDrop();
					// Deliver the settled position once movement quiesces (debounced:
					// each drop re-armed the timer above). Skipped while a trailing
					// throttle broadcast is pending - that already sends the latest
					// entry.data at the window end. On fire, send only if the rest
					// position differs from the last broadcast, so an exact repeat
					// (minMove: 1) stays dropped.
					if (!entry.timer) {
						const key = state.key;
						entry.settleTimer = setTimer(() => {
							const e = topicMap.get(key);
							if (!e) return;
							e.settleTimer = null;
							const p = finitePosition(e.data);
							if (p && (!e.lastSentPos || p.x !== e.lastSentPos.x || p.y !== e.lastSentPos.y)) {
								e.lastBroadcast = monotonicNow();
								e.lastSentPos = p;
								broadcast(topic, key, e.user, e.data, platform);
							}
						}, settleMs);
					}
					return;
				}
			}

			entry.data = data;
			entry.user = state.user;

			if (nowTs - entry.lastBroadcast >= throttleMs) {
				if (entry.timer) {
					clearTimer(entry.timer);
					entry.timer = null;
				}
				entry.lastBroadcast = nowTs;
				// Anchor the jitter filter against this committed flush. The
				// broadcast is deferred through topicThrottle, but this is the
				// position the subscriber will next see, so the next move is
				// measured against it. Set only on a real broadcast.
				if (pos) entry.lastSentPos = pos;
				broadcast(topic, state.key, state.user, data, platform);
				return;
			}

			mThrottled?.inc({ topic: mt(topic) });
			if (!entry.timer) {
				const key = state.key;
				const user = state.user;
				entry.timer = setTimer(() => {
					const e = topicMap.get(key);
					if (e) {
						e.lastBroadcast = monotonicNow();
						e.timer = null;
						// Record the position actually broadcast (the latest stored
						// data, which may be newer than this call's), never a dropped
						// one. Set only on a real broadcast.
						if (minMove > 0) {
							const p = finitePosition(e.data);
							if (p) e.lastSentPos = p;
						}
						broadcast(topic, key, user, e.data, platform);
					}
				}, throttleMs - (nowTs - entry.lastBroadcast));
			}
		},

		async remove(ws, platform, topic) {
			const state = wsState.get(ws);
			if (!state) return;

			if (topic !== undefined) {
				// Drop this subscriber's viewport for the topic first, decrementing
				// the reporter count only if a rect actually existed. A subscriber
				// can report a viewport for a topic it never moved a cursor on (a
				// read-only spectator), so the rect lives in `subViewport` even when
				// the topic is absent from `state.topics`. Running this before the
				// cursor-entry guard below keeps the rect and its reporter from
				// outliving an explicit single-topic remove. Subscriber-keyed:
				// removing one topic never touches the subscriber's other topics.
				const byTopic = subViewport.get(state.key);
				if (byTopic && byTopic.delete(topic)) {
					dropReporter(topic);
					if (byTopic.size === 0) subViewport.delete(state.key);
				}

				if (!state.topics.has(topic)) {
					if (state.topics.size === 0) wsState.delete(ws);
					return;
				}

				const topicMap = topics.get(topic);
				if (topicMap) {
					const entry = topicMap.get(state.key);
					if (entry) {
						if (entry.timer) clearTimer(entry.timer);
						if (entry.settleTimer) clearTimer(entry.settleTimer);
						const removed = await broadcastRemove(topic, state.key, platform);
						if (!removed) return;
						topicMap.delete(state.key);
						state.topics.delete(topic);
						if (topicMap.size === 0) {
							topics.delete(topic);
							activeTopics.delete(topic);
							topicFlush.delete(topic);
							dirtyTopics.delete(topic);
							dropTopicPending(topic);
							stopCleanupTimer();
						}
						const flushState = topicFlush.get(topic);
						if (flushState) {
							flushState.dirty.delete(state.key);
						}
					} else {
						state.topics.delete(topic);
					}
				} else {
					state.topics.delete(topic);
				}

				if (state.topics.size === 0) wsState.delete(ws);
				return;
			}

			if (b) { try { b.guard(); } catch { return; } }

			const removedTopics = [];
			for (const t of state.topics) {
				const topicMap = topics.get(t);
				if (!topicMap) continue;
				const entry = topicMap.get(state.key);
				if (entry) {
					if (entry.timer) clearTimer(entry.timer);
					entry.timer = null;
					if (entry.settleTimer) { clearTimer(entry.settleTimer); entry.settleTimer = null; }
					removedTopics.push(t);
				}
			}

			if (!(await removeKeysBatch(removedTopics, state.key))) return;

			for (const t of removedTopics) {
				queueRemove(t, state.key, platform);
				const topicMap = topics.get(t);
				if (topicMap) {
					topicMap.delete(state.key);
					if (topicMap.size === 0) {
						topics.delete(t);
						activeTopics.delete(t);
						topicFlush.delete(t);
						dirtyTopics.delete(t);
						dropTopicPending(t);
					}
				}
				const flushState = topicFlush.get(t);
				if (flushState) {
					flushState.dirty.delete(state.key);
				}
				dropKeyPending(t, state.key);
			}
			const byTopic = subViewport.get(state.key);
			if (byTopic) {
				for (const t of byTopic.keys()) dropReporter(t);
				subViewport.delete(state.key);
			}
			wsState.delete(ws);
			stopCleanupTimer();
		},

		async snapshot(ws, topic, platform) {
			// Authorize against the REAL topic before emitting the roster/positions
			// to this socket. The cursor-snapshot message reaches this directly
			// (hooks.message), so without this gate it is an un-authorized read of
			// __cursor:{topic} state, around the wire-level `__`-subscribe block.
			// The authorized subscribe path (attach -> here) re-checks harmlessly.
			// Optional-chained (checkSubscribe was added to the platform later);
			// the snapshot is low-frequency so the await is off the hot path.
			if (platform && typeof platform.checkSubscribe === 'function') {
				let denial;
				try { denial = await platform.checkSubscribe(ws, topic); } catch { return; }
				if (denial) return;
			}
			// Server time first - even for an empty board - so the requester's
			// smoothing clock is seeded before the first stamped position frame
			// and the request/reply round trip is measurable. Rides the codec's
			// JSON fallback (the codec declines the event), an additive envelope
			// an older client's merge ignores as an unknown event. This replica
			// stamps with ITS clock, the same clock that stamps the position
			// frames it re-encodes, so the client's time axis is consistent
			// regardless of which replica originated a move.
			try {
				emitTo(ws, '__cursor:' + topic, EVENTS.TIME, { t: wallEpoch() }, platform);
			} catch {
				// WebSocket closed before send
			}
			// The requester's own roster key, ahead of the roster it appears in
			// (or will appear in on its first move) - sent even for an empty
			// board. getWsState only allocates the connection key - the join
			// broadcast (and its Redis relay) still keys off `state.topics` on
			// the first move - so a pure viewer is never announced to others by
			// snapshotting. Snapshot-then-move keeps one identity: the key
			// handed out here is the instance-scoped key the later join
			// broadcasts. Single-target via emitTo; never relayed.
			const requesterKey = getWsState(ws).key;
			try {
				emitTo(ws, '__cursor:' + topic, EVENTS.YOU, { key: requesterKey }, platform);
			} catch {
				// WebSocket closed before send
			}
			const cursors = await this.list(topic);
			if (cursors.length === 0) return;
			const catalog = cursors.map((c) => ({ key: c.key, user: c.user }));
			const positions = cursors.map((c) => ({ key: c.key, data: c.data }));
			try {
				emitTo(ws, '__cursor:' + topic, EVENTS.CATALOG, catalog, platform);
				emitTo(ws, '__cursor:' + topic, EVENTS.BULK, positions, platform);
			} catch {
				// WebSocket closed before send
			}
		},

		/**
		 * Record this subscriber's viewport rect for a topic, from the inbound
		 * `cursor-viewport` frame. The rect bounds which cursors the subscriber
		 * receives once viewport culling is enabled; a subscriber that never
		 * reports one is treated as whole-board and is never culled. Best-effort:
		 * a malformed rect (missing or non-finite x/y/w/h, or a non-positive
		 * w/h/zoom) is dropped silently so the subscriber stays whole-board
		 * rather than recording a rect that culls to nothing. x/y may be negative
		 * (board coordinates). `zoom` is optional and defaults to 1. The rect is
		 * never relayed across Redis - it is local-replica state only.
		 * @param {any} ws
		 * @param {string} topic
		 * @param {any} rect
		 */
		viewport(ws, topic, rect) {
			if (!rect || typeof rect !== 'object') return;
			const { x, y, w, h } = rect;
			const zoom = rect.zoom === undefined ? 1 : rect.zoom;
			if (![x, y, w, h, zoom].every((n) => typeof n === 'number' && Number.isFinite(n))) return;
			if (w <= 0 || h <= 0 || zoom <= 0) return;
			const state = getWsState(ws);
			let byTopic = subViewport.get(state.key);
			if (!byTopic) { byTopic = new Map(); subViewport.set(state.key, byTopic); }
			// First rect this subscriber reports for the topic flips it onto the
			// per-subscriber walk; a re-report of an existing topic does not.
			if (!byTopic.has(topic)) addReporter(topic);
			byTopic.set(topic, { x, y, w, h, zoom });
		},

		/**
		 * The last viewport rect this subscriber reported for a topic, or `null`
		 * if it never reported one. Read by the per-subscriber walk; does not
		 * create `wsState`.
		 * @param {any} ws
		 * @param {string} topic
		 * @returns {{ x: number, y: number, w: number, h: number, zoom: number } | null}
		 */
		viewportFor(ws, topic) {
			return lookupViewport(ws, topic);
		},

		async list(topic) {
			if (b) b.guard();
			let all;
			try {
				all = await redis.hgetall(hashKey(topic));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}
			const result = [];
			const nowTs = now();
			const ttlMs = cursorTtl * 1000;
			for (const key of Object.keys(all)) {
				try {
					const parsed = JSON.parse(all[key]);
					if (!parsed.ts || (nowTs - parsed.ts) > ttlMs) continue;
					result.push({ key, user: parsed.user, data: parsed.data });
				} catch { /* corrupted entry */ }
			}
			// Include locally-pending entries that have not yet been flushed
			// to Redis. Without this, a list() call between a broadcast and
			// the next snapshot tick under-reports the cursor we just saw.
			const topicPending = getTopicPending(topic);
			if (topicPending) {
				const seen = new Set(result.map((r) => r.key));
				for (const [key, entry] of topicPending) {
					if (seen.has(key)) continue;
					if (!entry.ts || (nowTs - entry.ts) > ttlMs) continue;
					result.push({ key, user: entry.user, data: entry.data });
				}
			}
			return result;
		},

		/**
		 * Right-to-erasure (`live.forget`): remove a user's cursors from every
		 * topic across the cluster. The cursor hash FIELD is an ephemeral
		 * connection key (not the userId), so this scans `cursor:{*}`, HGETALLs
		 * each topic hash, and matches the userId against `value.user` (the
		 * select() output): a bare string equal to the userId, or an object whose
		 * `id`/`userId` equals it. Matched fields are HDELeted and a REMOVE is
		 * broadcast (via removeKeysBatch, cluster-correct) so subscribers drop the
		 * cursor. A select() output that does not surface the userId as a string or
		 * an id/userId field is not addressable here (document your select shape).
		 * `tenantId` is accepted for the uniform contract; cursor keys carry no
		 * tenant segment.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} cursor entries removed
		 */
		async purgeUser(tenantId, userId) {
			if (typeof userId !== 'string' || userId.length === 0) return 0;
			const prefix = client.key('cursor:{');
			let keys;
			try { keys = await scanKeys(redis, hashKey('*')); } catch { return 0; }
			let n = 0;
			for (const fullKey of keys) {
				if (!fullKey.startsWith(prefix) || !fullKey.endsWith('}')) continue;
				const topic = fullKey.slice(prefix.length, fullKey.length - 1);
				let all;
				try { all = await redis.hgetall(fullKey); } catch { continue; }
				if (!all) continue;
				for (const field of Object.keys(all)) {
					let parsed;
					try { parsed = JSON.parse(all[field]); } catch { continue; }
					const u = parsed && parsed.user;
					const matches = typeof u === 'string'
						? u === userId
						: !!(u && typeof u === 'object' && (u.id === userId || u.userId === userId));
					if (!matches) continue;
					// Drop any pending snapshot + local entry first so a flush cannot
					// resurrect the cursor, then HDEL + broadcast REMOVE cluster-wide.
					dropKeyPending(topic, field);
					const tm = topics.get(topic);
					if (tm) {
						const e = tm.get(field);
						if (e) {
							if (e.timer) clearTimer(e.timer);
							if (e.settleTimer) clearTimer(e.settleTimer);
							tm.delete(field);
						}
					}
					if (await removeKeysBatch([topic], field)) n++;
				}
			}
			return n;
		},

		async clear() {
			b?.guard();
			try {
				await scanAndUnlink(redis, client.key('cursor:*'));
				b?.success();
			} catch (err) {
				b?.failure(err);
				throw err;
			}

			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimer(entry.timer);
					if (entry.settleTimer) clearTimer(entry.settleTimer);
				}
			}
			resetScheduler();
			clearRemoveBuffer();
			dirtyTopics.clear();
			topics.clear();
			topicFlush.clear();
			wsState.clear();
			subViewport.clear();
			topicReporters.clear();
			activeTopics.clear();
			resetPending();
			stopCleanupTimer();
			connCounter = 0;
		},

		destroy() {
			dispose();
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimer(entry.timer);
					if (entry.settleTimer) clearTimer(entry.settleTimer);
				}
			}
			resetScheduler();
			clearRemoveBuffer();
			dirtyTopics.clear();
			topicFlush.clear();
			subViewport.clear();
			topicReporters.clear();
		},

		/**
		 * Snapshot of scheduler health. Always available, near-zero cost.
		 *
		 * - `flushes`: total tick-driven flushes since tracker creation.
		 * - `driftMeanMs`: mean (target_deadline - actual_fire_time) across
		 *   all tick-driven flushes. 0 means perfect cadence; values >
		 *   `topicThrottle` indicate sustained event-loop saturation or
		 *   CPU contention.
		 * - `driftMaxMs`: largest single observed late fire. Useful for
		 *   spotting one-off GC pauses vs. sustained drift.
		 * - `dirtyTopicsCurrent`: topics with pending coalesced entries
		 *   right now. Should hover near zero in healthy operation; growth
		 *   means tick is falling behind.
		 * - `activeTopicsTotal`: topics with at least one local cursor.
		 * - `jitterDropped`: cursor moves dropped by the `minMove` jitter
		 *   filter before they reached the flush scheduler. Always 0 when
		 *   `minMove` is 0 (the default).
		 * - `viewportsReported`: current count of subscribers with a recorded
		 *   viewport rect. 0 when viewport culling is off or no subscriber has
		 *   reported a rect yet.
		 * - `perSubscriberFlushes`: lifetime count of flushes that took the
		 *   per-subscriber walk (viewport culling / backpressure) instead of the
		 *   shared combined frame. NOT reset by clear()/destroy().
		 * - `bpSkips`: lifetime count of per-flush subscriber skips because the
		 *   socket's queued bytes exceeded `backpressure.maxBufferedBytes`. NOT
		 *   reset by clear()/destroy().
		 * - `culledEntriesDropped`: lifetime count of combined entries a
		 *   subscriber's viewport withheld from it across all flushes. NOT reset
		 *   by clear()/destroy().
		 *
		 * Leading-edge synchronous flushes (first call on an idle topic)
		 * are not counted in drift stats - they fire on the call thread,
		 * not via the scheduler.
		 *
		 * @returns {{ flushes: number, driftMeanMs: number, driftMaxMs: number, dirtyTopicsCurrent: number, activeTopicsTotal: number, jitterDropped: number, viewportsReported: number, perSubscriberFlushes: number, bpSkips: number, culledEntriesDropped: number }}
		 */
		stats() {
			return { ...statsSnapshot(), activeTopicsTotal: topics.size };
		},

		hooks: {
			subscribe(ws, topic, { platform }) {
				if (topic.startsWith('__cursor:')) {
					const realTopic = topic.slice('__cursor:'.length);
					return tracker.snapshot(ws, realTopic, platform);
				}
			},
			message(ws, { data, platform }) {
				if (data && data.type === 'cursor' && data.topic && data.data !== undefined) {
					tracker.update(ws, data.topic, data.data, platform);
					return;
				}
				// Client-initiated reconnect-snapshot. The cursor plugin client
				// sends `{type:'cursor-snapshot', topic}` on every status==='open'
				// (initial connect + reconnect). Pre-fix, this text frame had no
				// server handler and was a dead wire frame; the snapshot path
				// only fired through `hooks.subscribe` -> `tracker.snapshot` when
				// the ws subscribed to the `__cursor:{topic}` channel. With this
				// branch, the snapshot also re-emits on the explicit frame so a
				// reconnecting tab that resubscribes via `subscribe-batch` (which
				// the adapter dedups when the topic is already in the user data
				// set) still gets a fresh catalog + bulk.
				if (data && data.type === 'cursor-snapshot' && typeof data.topic === 'string') {
					tracker.snapshot(ws, data.topic, platform);
					return;
				}
				// Client-reported viewport rect for culling. Routes ONLY to local
				// per-replica state (`tracker.viewport`); never relayed across Redis -
				// each replica culls its own local subscribers against the combined
				// local + peer cursor set, so a peer never needs the rect.
				if (data && data.type === 'cursor-viewport' && typeof data.topic === 'string') {
					tracker.viewport(ws, data.topic, data.rect);
					return;
				}
				// Silent no-op when the caller dispatches a parsed object whose
				// `type` is not ours. The dispatch-to-all pattern (e.g. forwarding
				// every unhandled frame to cursor.hooks.message AND
				// presence.hooks.message and letting each ignore frames not
				// addressed to it) is legitimate. Only warn on shapes that
				// indicate the wrong wiring (raw bytes, non-object) where the
				// developer almost certainly intended a parsed envelope.
				if (data && typeof data === 'object' && !Array.isArray(data) && typeof data.type === 'string') {
					return;
				}
				_warnCursorHooksMessageShape(data);
			},
			close(ws, { platform }) {
				return tracker.remove(ws, platform);
			}
		}
	};

	return tracker;
}


