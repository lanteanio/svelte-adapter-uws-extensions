/**
 * Per-iteration diff buffer for the Redis-backed presence tracker.
 *
 * Coalesces per-topic join/leave/update ops within one event-loop iteration so
 * the wire only sees the net change, then flushes once per iteration. No Redis,
 * no slot concerns - pure local batching over injected shared state (the emit
 * broadcast, the localData cache, the publicData projection, the metric
 * handles). Owns its pending map and the single deferred flush timer; dispose()
 * is the teardown the tracker's clear()/destroy() call.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/diff-buffer
 */

import { setTimer, clearTimer } from '../../shared/runtime.js';

/**
 * Create the per-topic pending-diff buffer.
 *
 * @param {{
 *   emit: (fullTopic: string, event: string, data: any, platform: any, opts?: any) => void,
 *   localData: Map<string, Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>>,
 *   publicData: (entry: { data: Record<string, any>, fields?: Record<string, any> | null }) => Record<string, any>,
 *   mt: ((topic: string) => string) | undefined,
 *   mDiffCoalesced: { inc: (labels: Record<string, string>) => void } | null | undefined,
 *   mDiffFrames: { inc: (labels: Record<string, string>) => void } | null | undefined
 * }} deps
 */
export function createDiffBuffer({ emit, localData, publicData, mt, mDiffCoalesced, mDiffFrames }) {
	/**
	 * Per-topic pending diff buffer: latest op per key wins. Joins and
	 * leaves on the same key in one event-loop iteration collapse so the
	 * wire only sees the net change. Flushed once per iteration via
	 * `setTimeout(flushPendingDiffs, 0)` armed when the first dirty entry
	 * lands. Mirrors the buffer model the adapter's bundled presence
	 * plugin uses, so a single client decoder handles both.
	 *
	 * Why `setTimeout(0)` and not `queueMicrotask`: uWS dispatches each WS
	 * message as its own JS task, and N-API drains microtasks at the C++/JS
	 * boundary between tasks. A microtask-deferred flush fires BEFORE the
	 * next socket's handler runs, so cross-socket coalescing is impossible
	 * at the microtask level - a mass-join into a populated topic produces
	 * O(N) one-entry publishes instead of one batched diff. `setTimeout(0)`
	 * lands in libuv's timers phase, which fires only after the poll phase
	 * has dispatched every ready socket message in the current iteration -
	 * so all joins arriving together end up in one flush regardless of how
	 * many task boundaries separate them. Same structural choice the
	 * 0.5.7 cursor always-tick rewrite locked in.
	 *
	 * @type {Map<string, Map<string, { op: 'join' | 'leave', data: Record<string, any> }>>}
	 */
	const pendingDiffs = new Map();
	/** @type {ReturnType<typeof setTimeout> | null} */
	let diffFlushTimer = null;
	/** @type {import('svelte-adapter-uws').Platform | null} */
	let diffFlushPlatform = null;

	function armDiffFlush(platform) {
		diffFlushPlatform = platform;
		if (diffFlushTimer === null) {
			diffFlushTimer = setTimer(flushPendingDiffs, 0);
			if (diffFlushTimer.unref) diffFlushTimer.unref();
		}
	}

	function bufferDiff(topic, op, key, data, platform) {
		let entries = pendingDiffs.get(topic);
		if (!entries) {
			entries = new Map();
			pendingDiffs.set(topic, entries);
		}
		if (entries.has(key)) {
			mDiffCoalesced?.inc({ topic: mt(topic) });
		}
		// A join/leave supersedes any pending field-level update for the key:
		// the join roster re-reads publicData (durable fields included) and a
		// leave drops the user entirely, so a buffered update is moot.
		entries.set(key, { op, data });
		armDiffFlush(platform);
	}

	/**
	 * Buffer a field-level update for the next flush, collapsing against any op
	 * already pending for the key, exactly like the in-memory plugin:
	 *   - pending leave  -> drop (the user leaves this flush; the update is moot)
	 *   - pending join   -> drop (the join roster carries durable fields via
	 *     publicData; a transient change is correctly excluded on a fresh join)
	 *   - pending update -> accumulate the changed fields
	 * @param {string} topic
	 * @param {string} key
	 * @param {Record<string, any>} changed - durable + transient changed fields
	 * @param {import('svelte-adapter-uws').Platform} platform
	 */
	function bufferUpdate(topic, key, changed, platform) {
		let entries = pendingDiffs.get(topic);
		if (!entries) {
			entries = new Map();
			pendingDiffs.set(topic, entries);
		}
		const prev = entries.get(key);
		if (prev) {
			// A pending leave wins: the user is gone this flush, so the update is moot.
			if (prev.op === 'leave') return;
			if (prev.op === 'join') {
				// A LOCAL join re-reads publicData(localData) at flush, so its durable
				// fields are already current and the update is redundant (transient is
				// excluded on a fresh local join, matching the in-memory plugin). A
				// RELAYED join (the user is not presented on this instance, so flush
				// uses the buffered payload verbatim) must absorb the change, or a
				// cross-instance field update landing in the same tick as the relayed
				// join / updated event for that user is silently lost.
				if (!localData.get(topic)?.get(key) && prev.data && typeof prev.data === 'object') {
					Object.assign(prev.data, changed);
					armDiffFlush(platform);
				}
				return;
			}
			Object.assign(prev.changed, changed);
			armDiffFlush(platform);
			return;
		}
		entries.set(key, { op: 'update', changed: { ...changed } });
		armDiffFlush(platform);
	}

	function flushPendingDiffs() {
		if (diffFlushTimer !== null) {
			clearTimer(diffFlushTimer);
			diffFlushTimer = null;
		}
		const platform = diffFlushPlatform;
		diffFlushPlatform = null;
		if (!platform) {
			pendingDiffs.clear();
			return;
		}
		for (const [topic, entries] of pendingDiffs) {
			/** @type {Record<string, Record<string, any>>} */
			const joins = {};
			/** @type {Record<string, Record<string, any>>} */
			const leaves = {};
			/** @type {Record<string, Record<string, any>> | null} */
			let updates = null;
			const localUsers = localData.get(topic);
			for (const [key, e] of entries) {
				if (e.op === 'join') {
					// Re-read the live local entry so the join roster carries the
					// user's latest durable fields (publicData strips transient).
					// A relayed join for a user this instance does not present has
					// no local entry and falls back to the relayed payload.
					const localEntry = localUsers && localUsers.get(key);
					joins[key] = localEntry ? publicData(localEntry) : e.data;
				} else if (e.op === 'leave') {
					leaves[key] = e.data;
				} else {
					if (!updates) updates = {};
					updates[key] = e.changed;
				}
			}
			// Keep the common `{ joins, leaves }` shape byte-identical when no
			// field-level update is pending, so a deployment that never calls
			// update() sees an unchanged wire. `updates` is additive: an old
			// client ignores it.
			const diff = updates ? { joins, leaves, updates } : { joins, leaves };
			try {
				// Presence WS frames opt INTO compression. They are low-frequency -
				// diffs coalesce per tick, heartbeat is periodic, state is on-attach -
				// so per-subscriber deflate CPU is amortized and the roster JSON
				// compresses well. This is the deliberate counterpart to the cursor
				// plugin's compress:false 60Hz hot path, and matches the bundled
				// in-memory presence plugin. No-op while websocket.compression is off
				// (the default): the adapter resolves the flag to false regardless.
				emit('__presence:' + topic, 'diff', diff, platform, { relay: false });
				mDiffFrames?.inc({ topic: mt(topic) });
			} catch { /* platform unavailable mid-flight */ }
		}
		pendingDiffs.clear();
	}

	function dispose() {
		pendingDiffs.clear();
		if (diffFlushTimer !== null) {
			clearTimer(diffFlushTimer);
			diffFlushTimer = null;
		}
		diffFlushPlatform = null;
	}

	return { bufferDiff, bufferUpdate, flushPendingDiffs, dispose };
}
