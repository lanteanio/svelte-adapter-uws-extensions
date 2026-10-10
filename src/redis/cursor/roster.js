import { now, setTimer, clearTimer } from '../../shared/runtime.js';
import { EVENTS } from './events.js';

/** Merge persisted cursors with live relay changes and this worker's movers. */
export function createRoster({ topics, instanceId, cursorTtl }) {
	const live = new Map();
	const ttlMs = cursorTtl * 1000;
	let timer = null;
	function prune() {
		timer = null;
		const cutoff = now() - ttlMs;
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
	function record(topic, event, payload) {
		const incoming = event === EVENTS.BULK ? payload : [payload];
		if (![EVENTS.JOIN, EVENTS.UPDATE, EVENTS.BULK, EVENTS.REMOVE].includes(event) || !Array.isArray(incoming)) return;
		let entries = live.get(topic);
		if (!entries) live.set(topic, entries = new Map());
		for (const item of incoming) {
			if (!item || typeof item.key !== 'string') continue;
			const entry = event === EVENTS.REMOVE ? { removed: true } : (entries.get(item.key) || {});
			if (event !== EVENTS.REMOVE) entry.removed = false;
			if (event === EVENTS.JOIN) entry.user = item.user;
			if (event === EVENTS.UPDATE || event === EVENTS.BULK) entry.data = item.data;
			entry.ts = now();
			entries.set(item.key, entry);
		}
		arm();
	}
	function merge(topic, all) {
		const result = new Map();
		const cutoff = now() - ttlMs;
		for (const [key, value] of Object.entries(all)) {
			if (key.startsWith(instanceId + ':')) continue;
			try {
				const entry = JSON.parse(value);
				if (entry.ts && entry.ts >= cutoff) result.set(key, { key, user: entry.user, data: entry.data });
			} catch { /* corrupted entry */ }
		}
		for (const [key, entry] of live.get(topic) || []) {
			if (entry.ts < cutoff) continue;
			if (entry.removed) { result.delete(key); continue; }
			const merged = { ...result.get(key), key };
			if ('user' in entry) merged.user = entry.user;
			if ('data' in entry) merged.data = entry.data;
			if ('user' in merged) result.set(key, merged);
		}
		for (const [key, entry] of topics.get(topic) || []) {
			result.set(key, { key, user: entry.user, data: entry.data });
		}
		return [...result.values()];
	}
	function clear() {
		live.clear();
		if (timer !== null) clearTimer(timer);
		timer = null;
	}
	return { record, merge, drop: (topic) => live.delete(topic), clear };
}
