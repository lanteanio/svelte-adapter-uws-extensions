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
	setTimer,
	clearTimer,
	setIntervalTimer,
	clearIntervalTimer
} from '../shared/runtime.js';
import { CLEANUP_SCRIPT } from '../shared/scripts.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { execMultiSlot } from '../shared/cluster.js';
import { MAX_CURSOR_WS, MAX_CURSOR_TOPICS } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { WsClosedError } from '../shared/errors.js';
import { createCursorWireCodec } from 'svelte-adapter-uws/plugins/cursor';

export { WsClosedError };

/** Wire-protocol event names this module emits. */
const EVENTS = Object.freeze({
	CATALOG: 'catalog',
	JOIN: 'join',
	UPDATE: 'update',
	REMOVE: 'remove',
	BULK: 'bulk'
});

/**
 * Mover count past which a flush builds the transient spatial index instead of
 * bounds-testing every entry per subscriber. Below it a flat scan is cheaper
 * than per-cell Map probes; above it the index earns a multiple-x CPU win at
 * the thousands-of-simultaneous-movers tail. Measured against the per-instance
 * combined mover count (this replica's local + inbound entries).
 */
const INDEX_CROSSOVER = 512;

/**
 * Pack a grid cell coordinate pair into one numeric key. Covers +-32k cells per
 * axis (at the default 256-unit cell, +-8.3M board units). A key collision can
 * only over-deliver - every pulled entry is re-tested against the exact bounds -
 * never blank a region.
 * @param {number} cx
 * @param {number} cy
 */
function packCell(cx, cy) {
	return ((cx & 0xffff) << 16) | (cy & 0xffff);
}

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
	const throttleMs = options.throttle ?? 16;
	const topicThrottleMs = options.topicThrottle ?? 16;
	const snapshotIntervalMs = options.snapshotIntervalMs ?? 100;
	if (options.select != null && typeof options.select !== 'function') {
		throw new Error('redis cursor: select must be a function');
	}
	const select = options.select || stripInternal;
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

	/**
	 * Coalesced HSET writes. Latest-wins per (topic, key). Drained on the
	 * `snapshotIntervalMs` timer into a single `pipe.hset(topic, f1, v1, ...)`
	 * per topic per tick. The broadcast/relay path queues entries here but
	 * does not await the write.
	 * @type {Map<string, Map<string, { user: any, data: any, ts: number }>>}
	 */
	let redisPending = new Map();

	/** @type {import('ioredis').Redis | null} */
	let subscriber = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let activePlatform = null;
	let subscriberReady = null;

	function ensureSubscriber(platform) {
		activePlatform = platform;
		if (subscriber) return;
		if (b && b.state === 'broken') return;
		const sub = client.duplicate({ enableReadyCheck: false });
		subscriber = sub;
		sub.on('error', (err) => {
			console.error('cursor subscriber error:', err.message);
		});
		sub.on('message', (ch, message) => {
			if (ch !== channel) return;
			if (!validator.acceptRaw(message)) return;
			try {
				const parsed = JSON.parse(message);
				if (parsed.instanceId === instanceId) return;
				if (!validator.acceptEnvelope(parsed.topic, parsed.event)) return;
				if (!activePlatform) return;

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
					enqueueInbound(parsed.topic, parsed.payload.key, parsed.payload.data, activePlatform);
				} else if (parsed.event === EVENTS.BULK && Array.isArray(parsed.payload)) {
					for (const entry of parsed.payload) {
						if (entry && typeof entry.key === 'string') {
							enqueueInbound(parsed.topic, entry.key, entry.data, activePlatform);
						}
					}
				} else if (parsed.event === EVENTS.REMOVE && parsed.payload && typeof parsed.payload.key === 'string') {
					// Peer-relayed cursor removes are coalesced through the
					// same tick buffer the local close path uses. Without
					// this, a mass-disconnect on one instance produces an
					// equally-sized O(N) immediate-publish storm on every
					// other instance, propagating the OOM risk cluster-wide.
					queueRemove(parsed.topic, parsed.payload.key, activePlatform);
				} else {
					emit(
						'__cursor:' + parsed.topic,
						parsed.event,
						parsed.payload,
						activePlatform,
						{ relay: false }
					);
				}
			} catch {
				// Malformed, skip
			}
		});
		subscriberReady = sub.subscribe(channel).then(async () => {
			if (!activePlatform || activeTopics.size === 0) return;
			const topicList = [...activeTopics];
			const commands = topicList.map((topic) => ['hgetall', hashKey(topic)]);
			let results;
			try {
				results = await execMultiSlot(redis, commands);
			} catch { return; }
			if (!activePlatform) return;
			const nowTs = now();
			for (let i = 0; i < topicList.length; i++) {
				const [, all] = results[i];
				if (!all) continue;
				const topic = topicList[i];
				const catalogEntries = [];
				const positionEntries = [];
				for (const key of Object.keys(all)) {
					if (key.startsWith(instanceId + ':')) continue;
					try {
						const parsed = JSON.parse(all[key]);
						if (parsed.ts && (nowTs - parsed.ts) <= cursorTtlMs) {
							catalogEntries.push({ key, user: parsed.user });
							positionEntries.push({ key, data: parsed.data });
						}
					} catch { /* skip */ }
				}
				if (catalogEntries.length > 0 && activePlatform) {
					emit('__cursor:' + topic, EVENTS.CATALOG, catalogEntries, activePlatform, { relay: false });
					emit('__cursor:' + topic, EVENTS.BULK, positionEntries, activePlatform, { relay: false });
				}
			}
		}).catch(() => {
			sub.quit().catch(() => sub.disconnect());
			if (subscriber === sub) {
				subscriber = null;
			}
		}).finally(() => {
			subscriberReady = null;
		});
	}

	const cursorTtlMs = cursorTtl * 1000;

	/** @type {Set<string>} */
	const activeTopics = new Set();

	const cleanupInterval = Math.max(cursorTtlMs, 10000);
	let cleanupTimer = null;
	let snapshotTimer = null;

	function startCleanupTimer() {
		if (cleanupTimer) return;
		cleanupTimer = setIntervalTimer(() => {
			const nowTs = now();
			for (const topic of activeTopics) {
				redis.eval(CLEANUP_SCRIPT, 1, hashKey(topic), nowTs, cursorTtlMs).catch((err) => {
					console.warn('cursor cleanup: stale removal failed for topic "' + topic + '":', err.message);
				});
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	function stopCleanupTimer() {
		if (cleanupTimer && activeTopics.size === 0) {
			clearIntervalTimer(cleanupTimer);
			cleanupTimer = null;
		}
		if (snapshotTimer && activeTopics.size === 0) {
			clearIntervalTimer(snapshotTimer);
			snapshotTimer = null;
		}
	}

	function startSnapshotTimer() {
		if (snapshotTimer || snapshotIntervalMs === 0) return;
		snapshotTimer = setIntervalTimer(flushSnapshot, snapshotIntervalMs);
		if (snapshotTimer.unref) snapshotTimer.unref();
	}

	function flushSnapshot() {
		if (redisPending.size === 0) return;
		if (b) { try { b.guard(); } catch { redisPending = new Map(); return; } }
		const pending = redisPending;
		redisPending = new Map();
		const commands = [];
		let queued = 0;
		for (const [topic, entries] of pending) {
			if (entries.size === 0) continue;
			const args = [];
			for (const [key, entry] of entries) {
				args.push(key, JSON.stringify({ user: entry.user, data: entry.data, ts: entry.ts }));
			}
			commands.push(['hset', hashKey(topic), ...args]);
			commands.push(['expire', hashKey(topic), cursorTtl]);
			queued += entries.size;
		}
		if (queued === 0) return;
		execMultiSlot(redis, commands).then(() => b?.success()).catch((err) => b?.failure(err));
	}

	function queueSnapshot(topic, key, user, data) {
		if (snapshotIntervalMs === 0) {
			if (b) { try { b.guard(); } catch { return; } }
			const pipe = redis.pipeline();
			pipe.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: now() }));
			pipe.expire(hashKey(topic), cursorTtl);
			pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
			return;
		}
		let topicPending = redisPending.get(topic);
		if (!topicPending) {
			topicPending = new Map();
			redisPending.set(topic, topicPending);
		}
		topicPending.set(key, { user, data, ts: now() });
	}

	function hashKey(topic) {
		return client.key('cursor:{' + topic + '}');
	}

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
	// `alwaysVisible` holds those null-position indices; `flushCells` is the
	// transient spatial index (packed cell key -> entry indices), built only past
	// INDEX_CROSSOVER and emptied back into `cellPool` after the walk; `cullOut`
	// is the per-subscriber slice handed to the wire; `bounds` is the reused
	// padded-bounds object; `inDeliver` is the re-entrancy guard.
	/** @type {Array<{ key: string, data: any }>} */
	const flushEntries = [];
	/** @type {Array<{ x: number, y: number } | null>} */
	const flushPos = [];
	/** @type {number[]} */
	const alwaysVisible = [];
	/** @type {Map<number, number[]>} */
	const flushCells = new Map();
	/** @type {number[][]} */
	const cellPool = [];
	/** @type {Array<{ key: string, data: any }>} */
	const cullOut = [];
	const bounds = { minX: 0, minY: 0, maxX: 0, maxY: 0 };
	let inDeliver = false;

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

	// Pending REMOVE keys per topic, coalesced into one wire frame per
	// subscriber per event-loop iteration. Mass-disconnect scenarios (e.g.
	// 5K cursors closing in one tick during a stress test or browser-tab
	// teardown of a packed board) used to fire 5K immediate publishes per
	// topic, each fanning out to every remaining subscriber. The resulting
	// O(N^2) uWS send queue allocations OOM-killed workers ~18s into the
	// cleanup cascade. With this buffer, every subscriber sees one frame
	// per topic per tick containing the full leave list, regardless of
	// how many sockets dropped together. Symmetric to the presence
	// `pendingDiffs` buffer.
	//
	// Why `setTimeout(0)` over `queueMicrotask`: uWS dispatches each WS
	// close as its own JS task, and N-API drains microtasks at the C++/JS
	// boundary between tasks. A microtask-deferred flush fires BEFORE the
	// next socket's close handler runs, so cross-socket coalescing is
	// impossible at the microtask level. `setTimeout(0)` lands in libuv's
	// timers phase, which fires only after the poll phase has dispatched
	// every ready socket event in the current iteration.
	/** @type {Map<string, Set<string>>} */
	const pendingRemoves = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} */
	let removeFlushTimer = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let removeFlushPlatform = null;

	function queueRemove(topic, key, platform) {
		let set = pendingRemoves.get(topic);
		if (!set) {
			set = new Set();
			pendingRemoves.set(topic, set);
		}
		set.add(key);
		removeFlushPlatform = platform;
		if (removeFlushTimer === null) {
			removeFlushTimer = setTimer(flushPendingRemoves, 0);
			if (removeFlushTimer.unref) removeFlushTimer.unref();
		}
	}

	function flushPendingRemoves() {
		removeFlushTimer = null;
		const platform = removeFlushPlatform;
		removeFlushPlatform = null;
		if (!platform) {
			pendingRemoves.clear();
			return;
		}
		for (const [topic, keys] of pendingRemoves) {
			if (keys.size === 0) continue;
			// publishBatched (adapter >= 0.5.0-next.5) bundles N events into
			// one wire frame per subscriber. Each subscriber decodes the
			// individual REMOVE events from the frame, so no client change
			// is required. Fall back to per-event publishes when the
			// adapter does not expose publishBatched. publishBatched has no
			// per-frame compress seam and always sends uncompressed, so the
			// fallback passes { compress: false } to keep both REMOVE paths
			// off the compressor (see emitJoin for the cursor-wide policy).
			if (typeof platform.publishBatched === 'function') {
				const messages = [];
				for (const key of keys) {
					messages.push({ topic: '__cursor:' + topic, event: EVENTS.REMOVE, data: { key } });
				}
				try { platform.publishBatched(messages); } catch { /* platform unavailable mid-flight */ }
			} else {
				for (const key of keys) {
					try { platform.publish('__cursor:' + topic, EVENTS.REMOVE, { key }, { compress: false }); } catch { /* swallow */ }
				}
			}
		}
		pendingRemoves.clear();
	}

	function relay(topic, event, payload) {
		if (b) { try { b.guard(); } catch { return; } }
		const msg = JSON.stringify({ instanceId, topic, event, payload });
		if (subscriberReady) {
			subscriberReady.then(() => redis.publish(channel, msg).catch(() => {}));
		} else {
			redis.publish(channel, msg).catch(() => {});
		}
	}

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
				if (indexed) buildFlushCells(n);
				flushPlatform.forEachSubscriber(fullTopic, (ws) => {
					if (bpEnabled && flushPlatform.bufferedAmount(ws) > bpMaxBufferedBytes) {
						bpSkips++;
						return;
					}
					let slice = flushEntries;
					if (viewportEnabled) {
						const rect = lookupViewport(ws, topic);
						// A non-reporter (null rect) is whole-board and never culled.
						if (rect !== null) slice = indexed ? cullIndexed(rect) : cullDirect(rect);
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
				if (indexed) releaseFlushCells();
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
	 * Build the transient spatial index over this flush's positioned movers.
	 * Bucket arrays are drawn from `cellPool` and returned by `releaseFlushCells`
	 * after the walk, so a dense flush recycles them.
	 * @param {number} n - flushEntries.length
	 */
	function buildFlushCells(n) {
		releaseFlushCells();
		for (let i = 0; i < n; i++) {
			const pos = flushPos[i];
			if (pos === null) continue; // null-pos delivered via alwaysVisible
			const ck = packCell(Math.floor(pos.x / viewportCell), Math.floor(pos.y / viewportCell));
			let bucket = flushCells.get(ck);
			if (!bucket) {
				bucket = cellPool.pop() || [];
				bucket.length = 0;
				flushCells.set(ck, bucket);
			}
			bucket.push(i);
		}
	}

	/** Return this flush's bucket arrays to the pool and empty the index. */
	function releaseFlushCells() {
		for (const bucket of flushCells.values()) cellPool.push(bucket);
		flushCells.clear();
	}

	/**
	 * Resolve a reported rect's padded board bounds. Width/height are board units
	 * already (the client reports the visible board region), so only the overscan
	 * is widened by 1/zoom when zoomed out, keeping it roughly constant on screen.
	 * Writes into the shared `bounds` object to avoid per-call alloc.
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 */
	function rectBounds(rect) {
		const pad = rect.zoom < 1 ? viewportPadding / rect.zoom : viewportPadding;
		bounds.minX = rect.x - pad;
		bounds.minY = rect.y - pad;
		bounds.maxX = rect.x + rect.w + pad;
		bounds.maxY = rect.y + rect.h + pad;
		return bounds;
	}

	/**
	 * Flat bounds test over every mover this flush. Used below INDEX_CROSSOVER,
	 * where the combined set is small enough that building an index does not pay.
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 * @returns {Array<{ key: string, data: any }>}
	 */
	function cullDirect(rect) {
		const bb = rectBounds(rect);
		cullOut.length = 0;
		for (let i = 0; i < flushEntries.length; i++) {
			const pos = flushPos[i];
			if (pos === null) { cullOut.push(flushEntries[i]); continue; }
			if (pos.x >= bb.minX && pos.x <= bb.maxX && pos.y >= bb.minY && pos.y <= bb.maxY) {
				cullOut.push(flushEntries[i]);
			}
		}
		return cullOut;
	}

	/**
	 * Spatial-index cull: walk only the cells the viewport covers and bounds-test
	 * their movers. Per-subscriber cost is O(visible cells + movers in them), not
	 * O(all movers). A viewport spanning more cells than the flush has movers sees
	 * ~the whole board, so it delivers everything (the deliver-all clamp),
	 * bounding worst-case cost at O(movers).
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 * @returns {Array<{ key: string, data: any }>}
	 */
	function cullIndexed(rect) {
		const bb = rectBounds(rect);
		const cx0 = Math.floor(bb.minX / viewportCell);
		const cy0 = Math.floor(bb.minY / viewportCell);
		const cx1 = Math.floor(bb.maxX / viewportCell);
		const cy1 = Math.floor(bb.maxY / viewportCell);
		if ((cx1 - cx0 + 1) * (cy1 - cy0 + 1) > flushEntries.length) return flushEntries;
		cullOut.length = 0;
		for (let a = 0; a < alwaysVisible.length; a++) cullOut.push(flushEntries[alwaysVisible[a]]);
		for (let cy = cy0; cy <= cy1; cy++) {
			for (let cx = cx0; cx <= cx1; cx++) {
				const bucket = flushCells.get(packCell(cx, cy));
				if (!bucket) continue;
				for (let bi = 0; bi < bucket.length; bi++) {
					const i = bucket[bi];
					const pos = flushPos[i];
					if (pos.x >= bb.minX && pos.x <= bb.maxX && pos.y >= bb.minY && pos.y <= bb.maxY) {
						cullOut.push(flushEntries[i]);
					}
				}
			}
		}
		return cullOut;
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

	async function broadcastRemove(topic, key, platform) {
		if (b) { try { b.guard(); } catch { return false; } }

		try {
			await redis.hdel(hashKey(topic), key);
			b?.success();
		} catch (err) {
			b?.failure(err);
			return false;
		}

		// Drop any pending snapshot write for this key so we do not
		// resurrect a removed cursor on the next snapshot tick.
		const topicPending = redisPending.get(topic);
		if (topicPending) {
			topicPending.delete(key);
			if (topicPending.size === 0) redisPending.delete(topic);
		}

		queueRemove(topic, key, platform);
		relay(topic, EVENTS.REMOVE, { key });
		return true;
	}

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
				// No state to roll back (no `wsState` entry exists yet; that
				// is only created on `update`). Throw so the caller can
				// distinguish a no-op-and-rollback from a successful attach;
				// without this the RPC metric reports `status=ok` for
				// connections that never received cursor frames.
				mAttachesAborted?.inc({ topic: mt(topic), reason: 'ws_closed' });
				throw new WsClosedError('cursor.attach', topic);
			}
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
					jitterDropped++;
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
							redisPending.delete(topic);
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

			const commands = [];
			for (const t of removedTopics) {
				commands.push(['hdel', hashKey(t), state.key]);
				commands.push(['publish', channel, JSON.stringify({
					instanceId, topic: t, event: EVENTS.REMOVE, payload: { key: state.key }
				})]);
			}

			try {
				await execMultiSlot(redis, commands);
				b?.success();
			} catch (err) {
				b?.failure(err);
				return;
			}

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
						redisPending.delete(t);
					}
				}
				const flushState = topicFlush.get(t);
				if (flushState) {
					flushState.dirty.delete(state.key);
				}
				const topicPending = redisPending.get(t);
				if (topicPending) {
					topicPending.delete(state.key);
					if (topicPending.size === 0) redisPending.delete(t);
				}
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
			const topicPending = redisPending.get(topic);
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
			// Tracker-level scheduler timer + dirty-topic set.
			if (tickTimer !== null) { clearTimer(tickTimer); tickTimer = null; }
			if (removeFlushTimer !== null) { clearTimer(removeFlushTimer); removeFlushTimer = null; }
			removeFlushPlatform = null;
			pendingRemoves.clear();
			dirtyTopics.clear();
			topics.clear();
			topicFlush.clear();
			wsState.clear();
			subViewport.clear();
			topicReporters.clear();
			// Release per-flush walk scratch so a reset reclaims the last flush's
			// references. The lifetime walk counters (perSubscriberFlushes/bpSkips/
			// culledEntriesDropped) are intentionally left alone, matching flushCount.
			flushEntries.length = 0;
			flushPos.length = 0;
			alwaysVisible.length = 0;
			cullOut.length = 0;
			cellPool.length = 0;
			flushCells.clear();
			inDeliver = false;
			activeTopics.clear();
			redisPending = new Map();
			stopCleanupTimer();
			connCounter = 0;
		},

		destroy() {
			if (cleanupTimer) clearIntervalTimer(cleanupTimer);
			cleanupTimer = null;
			if (snapshotTimer) clearIntervalTimer(snapshotTimer);
			snapshotTimer = null;
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimer(entry.timer);
					if (entry.settleTimer) clearTimer(entry.settleTimer);
				}
			}
			if (tickTimer !== null) { clearTimer(tickTimer); tickTimer = null; }
			if (removeFlushTimer !== null) { clearTimer(removeFlushTimer); removeFlushTimer = null; }
			removeFlushPlatform = null;
			pendingRemoves.clear();
			dirtyTopics.clear();
			topicFlush.clear();
			subViewport.clear();
			topicReporters.clear();
			flushEntries.length = 0;
			flushPos.length = 0;
			alwaysVisible.length = 0;
			cullOut.length = 0;
			cellPool.length = 0;
			flushCells.clear();
			inDeliver = false;
			if (subscriber) {
				subscriber.quit().catch(() => subscriber.disconnect());
				subscriber = null;
			}
			activePlatform = null;
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
			return {
				flushes: flushCount,
				driftMeanMs: driftCount > 0 ? driftSum / driftCount : 0,
				driftMaxMs: driftMax,
				dirtyTopicsCurrent: dirtyTopics.size,
				activeTopicsTotal: topics.size,
				jitterDropped,
				viewportsReported: subViewport.size,
				perSubscriberFlushes,
				bpSkips,
				culledEntriesDropped
			};
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

/**
 * One-time dev-warn dedup for `cursor.hooks.message` shape misuse. The most
 * common cause is wiring the hook against `createMessage({ onUnhandled })`
 * which passes raw bytes, not a parsed envelope. The fix is to switch to
 * `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` (svelte-
 * realtime >= 0.5.9 + svelte-adapter-uws >= 0.5.3), which forwards the
 * parsed object directly.
 */
let _cursorHooksMessageBadShapeWarned = false;

/**
 * @param {any} data
 */
function _warnCursorHooksMessageShape(data) {
	if (_cursorHooksMessageBadShapeWarned) return;
	_cursorHooksMessageBadShapeWarned = true;
	const got = data instanceof ArrayBuffer
		? 'ArrayBuffer (raw bytes -- did you wire this from createMessage({onUnhandled}) ?)'
		: Array.isArray(data)
			? 'Array'
			: data === null
				? 'null'
				: typeof data === 'object'
					? 'object with data.type=' + String(data.type)
					: typeof data;
	console.warn(
		'[redis/cursor] hooks.message called with unexpected shape (' + got + '). ' +
		'Expected a parsed object {type:"cursor", topic, data} or ' +
		'{type:"cursor-snapshot", topic}. ' +
		'If you wired this from `createMessage({ onUnhandled })` and got raw bytes, ' +
		'switch to `createMessage({ onJsonMessage(ws, msg, platform) { ... } })` ' +
		'which forwards the parsed JSON envelope. ' +
		'This warning fires once per process.\n' +
		'  See: https://svti.me/cursor-hooks-message'
	);
}
