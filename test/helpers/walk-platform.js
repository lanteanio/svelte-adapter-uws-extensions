// Mock platform variant that supports the per-subscriber walk used by the
// cursor viewport-cull and backpressure-skip delivery paths. The shared
// mockPlatform() exposes a no-op forEachSubscriber and a constant-zero
// bufferedAmount on purpose (the zero-config fan-out never touches either),
// which means any cull / skip assertion run against it would pass vacuously:
// the walk would iterate nothing and no subscriber could ever be over the cap.
//
// This helper starts from mockPlatform() (so it keeps every recorded array and
// the full member surface the rest of the suite relies on) and overrides
// forEachSubscriber / bufferedAmount with real semantics:
//
//   - forEachSubscriber(fullTopic, fn) iterates the subscribers registered for
//     that exact full topic, passing fn(ws, userData) so the delivery walk can
//     read each connection's viewport rect from the same ws it recorded.
//   - bufferedAmount(ws) returns the scripted queued-byte count for a ws (0 for
//     an unknown or never-scripted ws, matching uWS for a closed/unknown handle).
//
// Per-subscriber sends land in `sent[]` (via the inherited send recorder);
// shared-frame fan-out (join / catalog / bulk fallback / remove) lands in
// `published[]`. The subscriber set is snapshotted before each walk so a
// callback that disconnects a peer mid-walk does not skip survivors.

import { mockPlatform } from './mock-platform.js';

/**
 * @returns {ReturnType<typeof mockPlatform> & {
 *   addSubscriber(ws: any, fullTopic: string): void,
 *   removeSubscriber(ws: any, fullTopic: string): void,
 *   setBuffered(ws: any, bytes: number): void,
 *   sentTo(ws: any): Array<any>
 * }}
 */
export function walkPlatform() {
	const p = mockPlatform();
	const subscribers = new Map(); // fullTopic -> Set<ws>
	const buffered = new Map(); // ws -> queued bytes

	// The per-subscriber walk hands the wire a reused scratch array for the BULK
	// slice (zero-alloc by design: a real platform serializes the array to bytes
	// synchronously inside send/sendWire, so reuse across subscribers is safe).
	// The mock records the call by reference, so without a snapshot a later
	// subscriber's cull would mutate an already-recorded frame. Snapshot array
	// payloads on record so `sentTo`/`sent` reflect the bytes each subscriber
	// actually received, matching a real platform's synchronous serialization.
	const baseSend = p.send;
	p.send = (ws, topic, event, data, options) => {
		const snap = Array.isArray(data) ? data.slice() : data;
		return baseSend(ws, topic, event, snap, options);
	};

	p.forEachSubscriber = (fullTopic, fn) => {
		const set = subscribers.get(fullTopic);
		if (!set) return;
		for (const ws of [...set]) {
			let ud = {};
			if (typeof ws.getUserData === 'function') {
				try { ud = ws.getUserData(); } catch { ud = {}; }
			}
			fn(ws, ud);
		}
	};
	p.bufferedAmount = (ws) => buffered.get(ws) || 0;
	p.subscribers = (fullTopic) => {
		const set = subscribers.get(fullTopic);
		return set ? set.size : 0;
	};

	p.addSubscriber = (ws, fullTopic) => {
		let set = subscribers.get(fullTopic);
		if (!set) { set = new Set(); subscribers.set(fullTopic, set); }
		set.add(ws);
		if (typeof ws.subscribe === 'function') {
			try { ws.subscribe(fullTopic); } catch { /* closed ws */ }
		}
	};
	p.removeSubscriber = (ws, fullTopic) => {
		const set = subscribers.get(fullTopic);
		if (set) set.delete(ws);
	};
	p.setBuffered = (ws, bytes) => { buffered.set(ws, bytes); };
	p.sentTo = (ws) => p.sent.filter((e) => e.ws === ws);

	return p;
}
