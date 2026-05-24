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

import { randomBytes } from 'node:crypto';
import { CLEANUP_SCRIPT } from '../shared/scripts.js';
import { stripInternal, createSensitiveWarner } from '../shared/sensitive.js';
import { scanAndUnlink } from '../shared/redis-scan.js';
import { MAX_CURSOR_WS, MAX_CURSOR_TOPICS } from '../shared/caps.js';
import { createBusValidator } from '../shared/bus-validate.js';
import { WsClosedError } from '../shared/errors.js';

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
 * @property {(ws: any, platform: import('svelte-adapter-uws').Platform, topic?: string) => Promise<void>} remove
 * @property {(topic: string) => Promise<CursorEntry[]>} list
 * @property {() => Promise<void>} clear
 * @property {() => void} destroy - Stop the Redis subscriber
 * @property {() => { flushes: number, driftMeanMs: number, driftMaxMs: number, dirtyTopicsCurrent: number, activeTopicsTotal: number }} stats - Scheduler health snapshot
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

	const instanceId = randomBytes(8).toString('hex');
	const redis = client.redis;
	const channel = client.key('cursor:events');

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
	 * @type {Map<string, Map<string, { user: any, data: any, lastBroadcast: number, timer: any }>>}
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
					activePlatform.publish(
						'__cursor:' + parsed.topic,
						parsed.event,
						parsed.payload,
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
			const pipe = redis.pipeline();
			for (const topic of topicList) pipe.hgetall(hashKey(topic));
			let results;
			try {
				results = await pipe.exec();
			} catch { return; }
			if (!activePlatform) return;
			const now = Date.now();
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
						if (parsed.ts && (now - parsed.ts) <= cursorTtlMs) {
							catalogEntries.push({ key, user: parsed.user });
							positionEntries.push({ key, data: parsed.data });
						}
					} catch { /* skip */ }
				}
				if (catalogEntries.length > 0 && activePlatform) {
					activePlatform.publish('__cursor:' + topic, EVENTS.CATALOG, catalogEntries, { relay: false });
					activePlatform.publish('__cursor:' + topic, EVENTS.BULK, positionEntries, { relay: false });
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
		cleanupTimer = setInterval(() => {
			const now = Date.now();
			for (const topic of activeTopics) {
				redis.eval(CLEANUP_SCRIPT, 1, hashKey(topic), now, cursorTtlMs).catch((err) => {
					console.warn('cursor cleanup: stale removal failed for topic "' + topic + '":', err.message);
				});
			}
		}, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	function stopCleanupTimer() {
		if (cleanupTimer && activeTopics.size === 0) {
			clearInterval(cleanupTimer);
			cleanupTimer = null;
		}
		if (snapshotTimer && activeTopics.size === 0) {
			clearInterval(snapshotTimer);
			snapshotTimer = null;
		}
	}

	function startSnapshotTimer() {
		if (snapshotTimer || snapshotIntervalMs === 0) return;
		snapshotTimer = setInterval(flushSnapshot, snapshotIntervalMs);
		if (snapshotTimer.unref) snapshotTimer.unref();
	}

	function flushSnapshot() {
		if (redisPending.size === 0) return;
		if (b) { try { b.guard(); } catch { redisPending = new Map(); return; } }
		const pending = redisPending;
		redisPending = new Map();
		const pipe = redis.pipeline();
		let queued = 0;
		for (const [topic, entries] of pending) {
			if (entries.size === 0) continue;
			const args = [];
			for (const [key, entry] of entries) {
				args.push(key, JSON.stringify({ user: entry.user, data: entry.data, ts: entry.ts }));
			}
			pipe.hset(hashKey(topic), ...args);
			pipe.expire(hashKey(topic), cursorTtl);
			queued += entries.size;
		}
		if (queued === 0) return;
		pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
	}

	function queueSnapshot(topic, key, user, data) {
		if (snapshotIntervalMs === 0) {
			if (b) { try { b.guard(); } catch { return; } }
			const pipe = redis.pipeline();
			pipe.hset(hashKey(topic), key, JSON.stringify({ user, data, ts: Date.now() }));
			pipe.expire(hashKey(topic), cursorTtl);
			pipe.exec().then(() => b?.success()).catch((err) => b?.failure(err));
			return;
		}
		let topicPending = redisPending.get(topic);
		if (!topicPending) {
			topicPending = new Map();
			redisPending.set(topic, topicPending);
		}
		topicPending.set(key, { user, data, ts: Date.now() });
	}

	function hashKey(topic) {
		return client.key('cursor:' + topic);
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
	 *   Initialized to `Date.now() - topicThrottleMs` so the first broadcast
	 *   on a new topic is "cycle ready" without polluting drift stats with
	 *   the full `Date.now()` lateness an init of 0 would imply.
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
			removeFlushTimer = setTimeout(flushPendingRemoves, 0);
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
			// adapter does not expose publishBatched.
			if (typeof platform.publishBatched === 'function') {
				const messages = [];
				for (const key of keys) {
					messages.push({ topic: '__cursor:' + topic, event: EVENTS.REMOVE, data: { key } });
				}
				try { platform.publishBatched(messages); } catch { /* platform unavailable mid-flight */ }
			} else {
				for (const key of keys) {
					try { platform.publish('__cursor:' + topic, EVENTS.REMOVE, { key }); } catch { /* swallow */ }
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

	function emitJoin(topic, key, user, platform) {
		const payload = { key, user };
		platform.publish('__cursor:' + topic, EVENTS.JOIN, payload);
		relay(topic, EVENTS.JOIN, payload);
	}

	function doBroadcast(topic, key, user, data, platform) {
		mBroadcasts?.inc({ topic: mt(topic) });
		const payload = { key, data };
		platform.publish('__cursor:' + topic, EVENTS.UPDATE, payload);
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

		// Single local publish covering all entries (local + inbound).
		if (entries.length === 1) {
			flushPlatform.publish('__cursor:' + topic, EVENTS.UPDATE, entries[0]);
		} else {
			flushPlatform.publish('__cursor:' + topic, EVENTS.BULK, entries);
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
		const now = Date.now();
		let nextDeadline = Infinity;

		for (const topic of dirtyTopics) {
			const state = topicFlush.get(topic);
			if (!state) { dirtyTopics.delete(topic); continue; }
			if (state.dirty.size === 0 && state.inboundDirty.size === 0) {
				dirtyTopics.delete(topic);
				continue;
			}
			const deadline = state.lastFlush + topicThrottleMs;
			if (deadline <= now) {
				const drift = now - deadline;
				driftSum += drift;
				driftCount++;
				if (drift > driftMax) driftMax = drift;

				flushBoth(topic, state);
				dirtyTopics.delete(topic);

				// Target-anchored: advance lastFlush by the cadence amount.
				// Multi-cycle backlog collapse to `now` so the next
				// broadcast's `Date.now() - lastFlush >= topicThrottleMs`
				// delay computation does not fire every queued cycle on
				// this turn.
				state.lastFlush = drift < topicThrottleMs ? deadline : now;
			} else if (deadline < nextDeadline) {
				nextDeadline = deadline;
			}
		}

		if (nextDeadline !== Infinity) {
			tickTimer = setTimeout(tick, Math.max(0, nextDeadline - Date.now()));
		}
		// else: scheduler goes idle until next broadcast() / enqueueInbound().
	}

	function armTick(delay) {
		if (tickTimer !== null) return;
		tickTimer = setTimeout(tick, delay);
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
		if (topicThrottleMs <= 0) {
			doBroadcast(topic, key, user, data, platform);
			return;
		}

		let state = topicFlush.get(topic);
		if (!state) {
			// Anchor `lastFlush` one cycle in the past so the first broadcast
			// is treated as "cycle ready" with zero drift on the very first
			// tick. Without this, `Date.now() - 0` would be a huge "lateness"
			// that pollutes the drift stats forever.
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: Date.now() - topicThrottleMs };
			topicFlush.set(topic, state);
		}
		state.dirty.set(key, { user, data, platform });
		dirtyTopics.add(topic);

		const elapsed = Date.now() - state.lastFlush;
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
		if (topicThrottleMs <= 0) {
			// Legacy immediate mode (matches old receiver behavior).
			platform.publish('__cursor:' + topic, EVENTS.UPDATE, { key, data }, { relay: false });
			return;
		}

		let state = topicFlush.get(topic);
		if (!state) {
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: Date.now() - topicThrottleMs };
			topicFlush.set(topic, state);
		}
		state.inboundDirty.set(key, { data, platform });
		dirtyTopics.add(topic);

		// Symmetric with `broadcast()` always-tick: both source paths share
		// the same `state` and the same tracker-wide tick timer, so a local
		// broadcast and a peer-relayed inbound landing in the same loop
		// iteration ship together as one combined frame at the next tick.
		const elapsed = Date.now() - state.lastFlush;
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
			const now = Date.now();

			if (!entry) {
				entry = { user: state.user, data, lastBroadcast: 0, timer: null };
				topicMap.set(state.key, entry);
			}

			entry.data = data;
			entry.user = state.user;

			if (now - entry.lastBroadcast >= throttleMs) {
				if (entry.timer) {
					clearTimeout(entry.timer);
					entry.timer = null;
				}
				entry.lastBroadcast = now;
				broadcast(topic, state.key, state.user, data, platform);
				return;
			}

			mThrottled?.inc({ topic: mt(topic) });
			if (!entry.timer) {
				const key = state.key;
				const user = state.user;
				entry.timer = setTimeout(() => {
					const e = topicMap.get(key);
					if (e) {
						e.lastBroadcast = Date.now();
						e.timer = null;
						broadcast(topic, key, user, e.data, platform);
					}
				}, throttleMs - (now - entry.lastBroadcast));
			}
		},

		async remove(ws, platform, topic) {
			const state = wsState.get(ws);
			if (!state) return;

			if (topic !== undefined) {
				if (!state.topics.has(topic)) return;

				const topicMap = topics.get(topic);
				if (topicMap) {
					const entry = topicMap.get(state.key);
					if (entry) {
						if (entry.timer) clearTimeout(entry.timer);
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
					if (entry.timer) clearTimeout(entry.timer);
					entry.timer = null;
					removedTopics.push(t);
				}
			}

			const pipe = redis.pipeline();
			for (const t of removedTopics) {
				pipe.hdel(hashKey(t), state.key);
				pipe.publish(channel, JSON.stringify({
					instanceId, topic: t, event: EVENTS.REMOVE, payload: { key: state.key }
				}));
			}

			try {
				await pipe.exec();
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
			wsState.delete(ws);
			stopCleanupTimer();
		},

		async snapshot(ws, topic, platform) {
			const cursors = await this.list(topic);
			if (cursors.length === 0) return;
			const catalog = cursors.map((c) => ({ key: c.key, user: c.user }));
			const positions = cursors.map((c) => ({ key: c.key, data: c.data }));
			try {
				platform.send(ws, '__cursor:' + topic, EVENTS.CATALOG, catalog);
				platform.send(ws, '__cursor:' + topic, EVENTS.BULK, positions);
			} catch {
				// WebSocket closed before send
			}
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
			const now = Date.now();
			const ttlMs = cursorTtl * 1000;
			for (const key of Object.keys(all)) {
				try {
					const parsed = JSON.parse(all[key]);
					if (!parsed.ts || (now - parsed.ts) > ttlMs) continue;
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
					if (!entry.ts || (now - entry.ts) > ttlMs) continue;
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
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			// Tracker-level scheduler timer + dirty-topic set.
			if (tickTimer !== null) { clearTimeout(tickTimer); tickTimer = null; }
			if (removeFlushTimer !== null) { clearTimeout(removeFlushTimer); removeFlushTimer = null; }
			removeFlushPlatform = null;
			pendingRemoves.clear();
			dirtyTopics.clear();
			topics.clear();
			topicFlush.clear();
			wsState.clear();
			activeTopics.clear();
			redisPending = new Map();
			stopCleanupTimer();
			connCounter = 0;
		},

		destroy() {
			if (cleanupTimer) clearInterval(cleanupTimer);
			cleanupTimer = null;
			if (snapshotTimer) clearInterval(snapshotTimer);
			snapshotTimer = null;
			for (const [, topicMap] of topics) {
				for (const [, entry] of topicMap) {
					if (entry.timer) clearTimeout(entry.timer);
				}
			}
			if (tickTimer !== null) { clearTimeout(tickTimer); tickTimer = null; }
			if (removeFlushTimer !== null) { clearTimeout(removeFlushTimer); removeFlushTimer = null; }
			removeFlushPlatform = null;
			pendingRemoves.clear();
			dirtyTopics.clear();
			topicFlush.clear();
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
		 *
		 * Leading-edge synchronous flushes (first call on an idle topic)
		 * are not counted in drift stats - they fire on the call thread,
		 * not via the scheduler.
		 *
		 * @returns {{ flushes: number, driftMeanMs: number, driftMaxMs: number, dirtyTopicsCurrent: number, activeTopicsTotal: number }}
		 */
		stats() {
			return {
				flushes: flushCount,
				driftMeanMs: driftCount > 0 ? driftSum / driftCount : 0,
				driftMaxMs: driftMax,
				dirtyTopicsCurrent: dirtyTopics.size,
				activeTopicsTotal: topics.size
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
