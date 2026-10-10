import { clearTimer, monotonicNow, setTimer } from '../shared/runtime.js';

const MAX_ENTRIES = 8192;
const MAX_BYTES = 8 * 1024 * 1024;
const CHUNK_ENTRIES = 256;
const CHUNK_BYTES = 1024 * 1024;
const registries = new WeakMap();
const collectedQueues = new FinalizationRegistry(({ refs, ref }) => refs.delete(ref));

// FIFO admission includes the submitted chunk until its real promise settles.
// One submission at a time preserves every channel's order through slot routing.
export function createRelayQueue({ submit, dropped, overflow, metrics, prefix }) {
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
	return {
		state,
		push(msg, count, channel, topics) {
			let bytes = msg.length * 3, exact = false;
			if (retained < MAX_ENTRIES && bytes > MAX_BYTES - retainedBytes) {
				normalizeBytes(); bytes = Buffer.byteLength(msg); exact = true;
			}
			if (retained === MAX_ENTRIES || bytes > MAX_BYTES - retainedBytes) {
				dropped('queue_full', 1, count);
				if (!warned) { warned = true; normalizeBytes(); overflow({ entries: retained, bytes: retainedBytes, maxEntries: MAX_ENTRIES, maxBytes: MAX_BYTES }); }
				return false;
			}
			entries.push({ msg, count, channel, topics, bytes, exact, enqueued: metrics ? monotonicNow() : 0 });
			retained++; retainedBytes += bytes;
			schedule();
			return true;
		},
		clear() {
			if (timer !== null) { clearTimer(timer); timer = null; }
			let messages = 0;
			for (let i = head; i < entries.length; i++) messages += entries[i].count;
			const count = entries.length - head;
			if (count) dropped('deactivated', count, messages);
			retained -= count; retainedBytes = flight?.bytes ?? 0;
			entries.length = 0; head = 0; normalizationHead = 0;
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
