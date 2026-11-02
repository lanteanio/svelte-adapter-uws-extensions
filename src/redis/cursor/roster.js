import { wallEpoch, monotonicNow, setTimer, clearTimer } from '../../shared/runtime.js';
import { EVENTS } from './events.js';

/** Merge persisted cursors with live relay changes and this worker's movers. */
export function createRoster({ topics, instanceId, cursorTtl, snapshotIntervalMs, revalidate, removeMissing, inputDropped = () => {} }) {
	const live = new Map();
	const provisional = new Map();
	const confirmationMs = Math.max(1, snapshotIntervalMs * 2 || 100);
	let confirmationTimer = null;
	let confirming = false;
	let disposed = false;
	let version = 0;

	function confirmLater(minDelay = 1) {
		if (disposed || confirming || confirmationTimer !== null || provisional.size === 0) return;
		let deadline = Infinity;
		for (const entries of provisional.values()) for (const entry of entries.values()) deadline = Math.min(deadline, entry.deadline);
		confirmationTimer = setTimer(async () => {
			confirmationTimer = null;
			confirming = true;
			let succeeded = false;
			try { succeeded = await revalidate([...provisional.keys()]); }
			catch { inputDropped('delivery_failed', 'catch_up'); }
			finally { confirming = false; confirmLater(succeeded ? 1 : 1000); }
		}, Math.max(minDelay, deadline - monotonicNow()));
		confirmationTimer?.unref?.();
	}

	// Relay overlay entries are stamped and aged on the monotonic clock: only
	// this instance reads them. Persisted entries carry the writer's exact
	// wall-clock stamp and are judged on this instance's exact wall clock.
	// The cached now() steps about once a second, which at a one-second TTL
	// would drop a cursor that moved a few ms earlier.
	const ttlMs = cursorTtl * 1000;
	let timer = null;
	function prune() {
		timer = null;
		const cutoff = monotonicNow() - ttlMs;
		for (const [topic, entries] of live) {
			for (const [key, entry] of entries) if (entry.ts < cutoff) entries.delete(key);
			if (entries.size === 0) live.delete(topic);
		}
		if (live.size) arm();
	}
	function arm() {
		if (timer !== null) return;
		timer = setTimer(prune, ttlMs);
		timer?.unref?.();
	}
	function recordOne(item, event, entries, ts) {
		if (!item || typeof item.key !== 'string') return;
		let entry = entries.get(item.key);
		if (!entry) entries.set(item.key, entry = {});
		entry.removed = event === EVENTS.REMOVE;
		if (entry.removed) { delete entry.user; delete entry.data; }
		if (event === EVENTS.JOIN) entry.user = item.user;
		if (event === EVENTS.UPDATE || event === EVENTS.BULK) entry.data = item.data;
		entry.ts = ts;
		entry.version = ++version;
	}
	function record(topic, event, payload) {
		if (event !== EVENTS.JOIN && event !== EVENTS.UPDATE && event !== EVENTS.BULK && event !== EVENTS.REMOVE) return;
		if (event === EVENTS.BULK && !Array.isArray(payload)) return;
		let entries = live.get(topic);
		if (!entries) live.set(topic, entries = new Map());
		const ts = monotonicNow();

		if (event === EVENTS.BULK) for (const item of payload) recordOne(item, event, entries, ts);
		else recordOne(payload, event, entries, ts);
		arm();
	}
	function merge(topic, all, platform) {
		const result = new Map();
		const cutoff = wallEpoch() - ttlMs;
		for (const [key, value] of Object.entries(all)) {
			if (key.startsWith(instanceId + ':')) continue;
			try {
				const entry = JSON.parse(value);
				if (!entry || !Number.isFinite(entry.ts) || !entry.ts) { inputDropped('corrupt_entry', 'snapshot_read'); continue; }
				if (entry.ts >= cutoff) result.set(key, { key, user: entry.user, data: entry.data });
			} catch { inputDropped('corrupt_entry', 'snapshot_read'); }
		}
		let candidates = provisional.get(topic);
		const current = monotonicNow();
		const liveCutoff = current - ttlMs;
		for (const [key, entry] of live.get(topic) || []) {
			if (entry.ts < liveCutoff) continue;
			if (entry.removed) { result.delete(key); candidates?.delete(key); continue; }
			if (result.has(key)) candidates?.delete(key);
			else if ('user' in entry) {
				if (!candidates) provisional.set(topic, candidates = new Map());
				const candidate = candidates.get(key);
				if (candidate?.version === entry.version && candidate.deadline <= current) {
					entry.removed = true;
					entry.ts = current;
					delete entry.user;
					delete entry.data;
					candidates.delete(key);
					removeMissing(topic, key, platform);
					continue;
				}
				if (!candidate || candidate.version !== entry.version) candidates.set(key, { version: entry.version, deadline: current + confirmationMs });
			}
			const merged = { ...result.get(key), key };
			if ('user' in entry) merged.user = entry.user;
			if ('data' in entry) merged.data = entry.data;
			if ('user' in merged) result.set(key, merged);
		}
		// An overlay may expire while its confirming read is in flight.
		// Its absence still needs to reach clients that saw the earlier join.
		if (candidates) {
			for (const [key, candidate] of candidates) {
				const entry = live.get(topic)?.get(key);
				if (entry && entry.ts >= liveCutoff) continue;
				if (!result.has(key) && candidate.deadline <= current) removeMissing(topic, key, platform);
				candidates.delete(key);
			}
			if (candidates.size === 0) provisional.delete(topic);
		}
		confirmLater();
		for (const [key, entry] of topics.get(topic) || []) {
			result.set(key, { key, user: entry.user, data: entry.data });
		}
		return [...result.values()];
	}
	function clear() {
		live.clear();
		provisional.clear();
		if (confirmationTimer !== null) clearTimer(confirmationTimer);
		confirmationTimer = null;
		if (timer !== null) clearTimer(timer);
		timer = null;
	}
	function visitKnown(visit) {
		const cutoff = monotonicNow() - ttlMs;
		for (const [topic, entries] of live) for (const [key, entry] of entries) {
			if (!entry.removed && entry.ts >= cutoff && 'user' in entry) visit(topic, key, entry.user);
		}
	}
	return { record, merge, visitKnown, drop(topic) { live.delete(topic); provisional.delete(topic); }, clear, dispose() { disposed = true; clear(); } };
}
