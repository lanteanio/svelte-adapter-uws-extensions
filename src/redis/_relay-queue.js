import { clearTimer, monotonicNow, setTimer } from '../shared/runtime.js';

const MAX_ENTRIES = 8192;
const MAX_BYTES = 8 * 1024 * 1024;
const CHUNK_ENTRIES = 256;
const CHUNK_BYTES = 1024 * 1024;
const registries = new WeakMap();
const collectedQueues = new FinalizationRegistry(({ refs, ref }) => refs.delete(ref));

// FIFO admission includes the submitted chunk until its real promise settles.
// One submission at a time preserves every channel's order through slot routing.
export function createRelayQueue({ submit, dropped, overflow, metrics, prefix, maxEnvelopeBytes = 0 }) {
	// An idle queue admits one call up to the larger of its byte budget and
	// one envelope at the configured cap, so the retained ceiling stays
	// max(MAX_BYTES, maxEnvelopeBytes) on both admission paths.
	const idleBytes = Math.max(MAX_BYTES, maxEnvelopeBytes);
	let entries = [];
	let head = 0, normalizationHead = 0, retainedBytes = 0, retained = 0;
	let timer = null, flight = null, warned = false;
	const state = {
		snapshot() {
			normalizeBytes();
			return { queued: entries.length - head, queuedBytes: retainedBytes - (flight?.bytes ?? 0), retained, retainedBytes,
				oldest: flight?.oldest ?? entries[head]?.enqueued ?? null };
		}
	};
	if (metrics) registerQueue(metrics, prefix, state);
	// Three bytes per UTF-16 code unit safely bounds UTF-8 without scanning
	// every small publish. Resolve exact lengths before any byte refusal and
	// at scrape time, including payloads still owned by the submitted chunk.
	function normalizeBytes() {
		for (let i = Math.max(head, normalizationHead); i < entries.length; i++) {
			const entry = entries[i];
			if (entry.exact) continue;
			const bytes = Buffer.byteLength(entry.msg);
			const difference = entry.bytes - bytes;
			entry.bytes = bytes; entry.exact = true;
			retainedBytes -= difference;
		}
		normalizationHead = entries.length;
		if (flight && !flight.normalized) for (const entry of flight.batch) {
			if (entry.exact) continue;
			const bytes = Buffer.byteLength(entry.msg);
			const difference = entry.bytes - bytes;
			entry.bytes = bytes; entry.exact = true;
			flight.bytes -= difference; retainedBytes -= difference;
		}
		if (flight) flight.normalized = true;
	}
	function schedule() {
		if (timer !== null || flight || head === entries.length) return;
		timer = setTimer(drain, 0);
		timer?.unref?.();
	}
	function settled() {
		retained -= flight.batch.length;
		retainedBytes -= flight.bytes;
		flight = null;
		// One burst report per overload episode: only an empty queue re-arms it.
		if (retained === 0) warned = false;
		schedule();
	}
	function drain() {
		timer = null;
		if (flight || head === entries.length) return;
		let batch, bytes;
		const oldest = entries[head].enqueued;
		// The usual coalesced tick already fits one chunk. Transfer its array
		// directly instead of copying every entry before submission.
		if (head === 0 && entries.length <= CHUNK_ENTRIES && (retainedBytes <= CHUNK_BYTES || entries.length === 1)) {
			batch = entries; bytes = retainedBytes;
			entries = []; normalizationHead = 0;
		} else {
			batch = []; bytes = 0;
			while (head < entries.length && batch.length < CHUNK_ENTRIES) {
				const entry = entries[head];
				if (batch.length && bytes + entry.bytes > CHUNK_BYTES) break;
				batch.push(entry);
				bytes += entry.bytes;
				entries[head++] = undefined;
			}
			if (head === entries.length) { entries.length = 0; head = 0; normalizationHead = 0; }
			else if (head >= MAX_ENTRIES / 2) { entries.splice(0, head); normalizationHead = Math.max(0, normalizationHead - head); head = 0; }
		}
		flight = { oldest, batch, bytes, normalized: false };
		try { Promise.resolve(submit(batch)).then(settled, settled); }
		catch { settled(); }
	}
	function enqueue(msg, count, channel, topics, bytes, exact) {
		const entry = { msg, count, channel, topics, bytes, exact, enqueued: metrics ? monotonicNow() : 0 };
		entries.push(entry);
		retained++; retainedBytes += bytes;
		return entry;
	}
	return {
		state,
		push(msg, count, channel, topics) {
			let bytes = msg.length * 3, exact = false;
			if (retained < MAX_ENTRIES && bytes > MAX_BYTES - retainedBytes) {
				normalizeBytes(); bytes = Buffer.byteLength(msg); exact = true;
			}
			// A configured envelope above the normal byte budget occupies the
			// queue alone. Its capacity stays charged until submission settles.
			if (retained === MAX_ENTRIES || (bytes > MAX_BYTES - retainedBytes && retained > 0)) {
				dropped('queue_full', 1, count);
				if (!warned) { warned = true; normalizeBytes(); overflow({ entries: retained, bytes: retainedBytes, maxEntries: MAX_ENTRIES, maxBytes: MAX_BYTES }); }
				return false;
			}
			enqueue(msg, count, channel, topics, bytes, exact);
			schedule();
			return true;
		},
		// Admit every envelope of one call or none of them, before the caller
		// delivers anything locally. `list` holds [msg, count, channel, topics]
		// groups of four. Returns null without charging anything when the call
		// does not fit, otherwise the call's first queued entry, which
		// withdraw() takes back with the rest of the call if local delivery
		// throws.
		admit(list) {
			const n = list.length / 4;
			if (retained + n > MAX_ENTRIES) return null;
			const room = retained === 0 ? idleBytes : MAX_BYTES - retainedBytes;
			let bytes = 0, exact = false;
			for (let i = 0; i < list.length; i += 4) bytes += list[i].length * 3;
			if (bytes > room) {
				normalizeBytes(); exact = true; bytes = 0;
				for (let i = 0; i < list.length; i += 4) bytes += Buffer.byteLength(list[i]);
				if (bytes > (retained === 0 ? idleBytes : MAX_BYTES - retainedBytes)) return null;
			}
			let first = null;
			for (let i = 0; i < list.length; i += 4) {
				const msg = list[i];
				const entry = enqueue(msg, list[i + 1], list[i + 2], list[i + 3], exact ? Buffer.byteLength(msg) : msg.length * 3, exact);
				if (first === null) first = entry;
			}
			schedule();
			return first;
		},
		// admit() for a call with one envelope, without building a list.
		admitOne(msg, count, channel, topics) {
			if (retained === MAX_ENTRIES) return null;
			let bytes = msg.length * 3, exact = false;
			if (retained > 0 && bytes > MAX_BYTES - retainedBytes) {
				normalizeBytes(); bytes = Buffer.byteLength(msg); exact = true;
				if (bytes > MAX_BYTES - retainedBytes) return null;
			}
			const entry = enqueue(msg, count, channel, topics, bytes, exact);
			schedule();
			return entry;
		},
		// Take back the `n` entries one admission queued from `first` on. They
		// are still queued and contiguous: drains run on a timer, entries are
		// only appended, and a nested call's entries, admitted later, follow
		// them. After clear() they are gone and this does nothing.
		withdraw(first, n) {
			if (first === null) return;
			const index = entries[entries.length - n] === first ? entries.length - n : entries.indexOf(first, head);
			if (index === -1) return;
			for (let i = index; i < index + n; i++) { retained--; retainedBytes -= entries[i].bytes; }
			entries.splice(index, n);
			if (normalizationHead > index) normalizationHead = Math.max(index, normalizationHead - n);
			if (head === entries.length) {
				entries.length = 0; head = 0; normalizationHead = 0;
				if (timer !== null) { clearTimer(timer); timer = null; }
			}
			if (retained === 0) warned = false;
		},
		clear() {
			if (timer !== null) { clearTimer(timer); timer = null; }
			let messages = 0;
			for (let i = head; i < entries.length; i++) messages += entries[i].count;
			const count = entries.length - head;
			if (count) dropped('deactivated', count, messages);
			retained -= count; retainedBytes = flight?.bytes ?? 0;
			entries.length = 0; head = 0; normalizationHead = 0;
			if (retained === 0) warned = false;
		}
	};
}

function registerQueue(metrics, prefix, state) {
	let registry = registries.get(metrics);
	if (!registry) { registry = new Map(); registries.set(metrics, registry); }
	let refs = registry.get(prefix);
	if (!refs) {
		refs = new Set(); registry.set(prefix, refs);
		installCollector(metrics, prefix, refs);
	}
	const ref = new WeakRef(state);
	refs.add(ref);
	collectedQueues.register(state, { refs, ref });
}

// A separate context keeps the registry from retaining any bus or payload.
function installCollector(metrics, prefix, refs) {
	const depth = metrics.gauge(prefix + '_relay_queue_envelopes', 'Retained Redis relay envelopes', ['state']);
	const bytes = metrics.gauge(prefix + '_relay_queue_bytes', 'Retained Redis relay encoded UTF-8 bytes', ['state']);
	const age = metrics.gauge(prefix + '_relay_oldest_age_seconds', 'Age of the oldest retained Redis relay envelope');
	function snapshot() {
		let queued = 0, queuedBytes = 0, retained = 0, retainedBytes = 0, oldest = null;
		for (const ref of refs) {
			const state = ref.deref();
			if (!state) { refs.delete(ref); continue; }
			const value = state.snapshot();
			queued += value.queued; queuedBytes += value.queuedBytes;
			retained += value.retained; retainedBytes += value.retainedBytes;
			if (value.oldest !== null && (oldest === null || value.oldest < oldest)) oldest = value.oldest;
		}
		depth.set({ state: 'queued' }, queued); depth.set({ state: 'in_flight' }, retained - queued);
		bytes.set({ state: 'queued' }, queuedBytes); bytes.set({ state: 'in_flight' }, retainedBytes - queuedBytes);
		age.set(oldest === null ? 0 : Math.max(0, monotonicNow() - oldest) / 1000);
	}
	depth.collect?.(snapshot); bytes.collect?.(snapshot); age.collect?.(snapshot);
}
