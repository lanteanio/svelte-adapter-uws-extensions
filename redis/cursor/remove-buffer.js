/**
 * Coalesced REMOVE buffer for the Redis-backed cursor tracker: batches the
 * per-(topic) leave keys that pile up during a mass disconnect into one wire
 * frame per subscriber per event-loop iteration, instead of one immediate
 * publish per leaving socket. Cluster-neutral local fan-out only - the Redis
 * hash cleanup for removed cursors runs in the tracker teardown, not here.
 *
 * @module svelte-adapter-uws-extensions/redis/cursor/remove-buffer
 */

import { setTimer, clearTimer } from '../../shared/runtime.js';
import { EVENTS } from './events.js';

/**
 * @typedef {Object} RemoveBuffer
 * @property {(topic: string, key: string, platform: import('svelte-adapter-uws').Platform) => void} queueRemove
 * @property {() => void} clear - Cancel any pending flush and drop all buffered removes (teardown).
 */

/**
 * Create a coalesced REMOVE buffer. State (the pending map + flush timer +
 * last platform) is private to the returned closure; the tracker drives it
 * through queueRemove and resets it through clear.
 *
 * @returns {RemoveBuffer}
 */
export function createRemoveBuffer() {
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

	function clear() {
		if (removeFlushTimer !== null) { clearTimer(removeFlushTimer); removeFlushTimer = null; }
		removeFlushPlatform = null;
		pendingRemoves.clear();
	}

	return { queueRemove, clear };
}
