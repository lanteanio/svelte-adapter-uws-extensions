// A/B bench for the leading-edge sync-fire vs microtask-defer patterns in
// redis/cursor.js broadcast() and enqueueInbound(). Ported from the adapter's
// `bench/micro-cursor-microtask-defer.mjs`, extended with peer-relay (Redis
// pub/sub message replay) traffic so the bench exercises both source paths
// the cluster cursor module has - local-origin via broadcast() and
// peer-relayed via enqueueInbound() - through the same shared
// pendingMicroflush per topic state.
//
// Pre-fix shape: each leading-edge broadcast fires synchronously with
// whatever single cursor is in `dirty`. Co-arriving broadcasts in the same
// JS pass take the trailing path (next tick), so a 250-mover burst
// produces 1 immediate UPDATE + 249 trailing BULK entries one cycle later.
// Worse: every co-arriving peer-relayed cursor in the same pass also takes
// its OWN leading edge, fragmenting further.
//
// Post-fix shape: leading-edge claims the cadence slot synchronously
// (state.lastFlush = now) and defers the flush by one microtask via a
// shared state.pendingMicroflush. All co-arriving broadcasts AND
// enqueueInbound calls in the same JS pass append to dirty / inboundDirty
// before the microtask fires; one combined BULK ships per cycle.
//
// Expected result on demo load profile (250 movers per worker x 8 workers,
// topicThrottle: 8ms): single UPDATE rate collapses toward zero, BULK rate
// matches cycle rate, mean BULK size approaches mover-count.
//
// Usage:
//   node bench/03-cursor-microtask-defer.mjs [movers] [bursts] [peerRatio]
//
// Defaults: 250 movers/burst, 1000 bursts, peerRatio=0.2 (20% of cursors
// per burst arrive via the inbound peer-relay path).

const MOVERS_PER_BURST = parseInt(process.argv[2] || '250', 10);
const BURST_COUNT = parseInt(process.argv[3] || '1000', 10);
const PEER_RATIO = parseFloat(process.argv[4] || '0.2');
const TOPIC_THROTTLE_MS = 8;

function makeRecordingPlatform() {
	const events = { update: 0, bulk: 0, bulkSizes: [] };
	return {
		events,
		publish(_topic, event, data) {
			if (event === 'update') events.update++;
			else if (event === 'bulk') {
				events.bulk++;
				events.bulkSizes.push(data.length);
			}
		}
	};
}

// ----- Variant A: sync leading-edge (pre-fix) ---------------------------

function makeVariantSync() {
	const topicFlush = new Map();

	function flushBoth(topic, state, platform) {
		const entries = [];
		for (const [k, v] of state.dirty) entries.push({ key: k, data: v.data });
		for (const [k, v] of state.inboundDirty) entries.push({ key: k, data: v.data });
		state.dirty.clear();
		state.inboundDirty.clear();
		if (entries.length === 0) return;
		if (entries.length === 1) {
			platform.publish(topic, 'update', entries[0]);
		} else {
			platform.publish(topic, 'bulk', entries);
		}
	}

	return {
		broadcast(topic, key, data, platform) {
			let state = topicFlush.get(topic);
			if (!state) {
				state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0 };
				topicFlush.set(topic, state);
			}
			state.dirty.set(key, { data });
			const now = Date.now();
			if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
				state.lastFlush = now;
				flushBoth(topic, state, platform);
			}
		},
		enqueueInbound(topic, key, data, platform) {
			let state = topicFlush.get(topic);
			if (!state) {
				state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0 };
				topicFlush.set(topic, state);
			}
			state.inboundDirty.set(key, { data });
			const now = Date.now();
			if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
				state.lastFlush = now;
				flushBoth(topic, state, platform);
			}
		}
	};
}

// ----- Variant B: microtask-deferred leading-edge (post-fix) -------------

function makeVariantDeferred() {
	const topicFlush = new Map();

	function flushBoth(topic, state, platform) {
		const entries = [];
		for (const [k, v] of state.dirty) entries.push({ key: k, data: v.data });
		for (const [k, v] of state.inboundDirty) entries.push({ key: k, data: v.data });
		state.dirty.clear();
		state.inboundDirty.clear();
		if (entries.length === 0) return;
		if (entries.length === 1) {
			platform.publish(topic, 'update', entries[0]);
		} else {
			platform.publish(topic, 'bulk', entries);
		}
	}

	function scheduleMicroflush(topic, state, platform) {
		if (state.pendingMicroflush) return;
		state.pendingMicroflush = true;
		queueMicrotask(() => {
			state.pendingMicroflush = false;
			flushBoth(topic, state, platform);
		});
	}

	return {
		broadcast(topic, key, data, platform) {
			let state = topicFlush.get(topic);
			if (!state) {
				state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0, pendingMicroflush: false };
				topicFlush.set(topic, state);
			}
			state.dirty.set(key, { data });
			const now = Date.now();
			if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
				state.lastFlush = now;
				scheduleMicroflush(topic, state, platform);
			}
		},
		enqueueInbound(topic, key, data, platform) {
			let state = topicFlush.get(topic);
			if (!state) {
				state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0, pendingMicroflush: false };
				topicFlush.set(topic, state);
			}
			state.inboundDirty.set(key, { data });
			const now = Date.now();
			if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
				state.lastFlush = now;
				scheduleMicroflush(topic, state, platform);
			}
		}
	};
}

// ----- Test driver -------------------------------------------------------

async function runBursts(variant) {
	const platform = makeRecordingPlatform();
	const peerCount = Math.floor(MOVERS_PER_BURST * PEER_RATIO);
	const localCount = MOVERS_PER_BURST - peerCount;

	for (let burst = 0; burst < BURST_COUNT; burst++) {
		// Consume real wall-clock so the lastFlush comparison takes the
		// leading-edge branch every burst. Both variants read Date.now()
		// the same way; this is fair.
		const target = Date.now() + TOPIC_THROTTLE_MS + 2;
		while (Date.now() < target) { /* spin */ }

		// Single JS pass: local cursors and peer-relayed cursors arrive
		// interleaved. The shared pendingMicroflush in variant B should
		// merge them into ONE bulk per cycle.
		for (let i = 0; i < localCount; i++) {
			variant.broadcast('canvas', 'L' + i, { x: burst, y: i }, platform);
		}
		for (let i = 0; i < peerCount; i++) {
			variant.enqueueInbound('canvas', 'P' + i, { x: burst, y: i }, platform);
		}

		// Yield for microtasks before the next burst.
		await Promise.resolve();
	}

	// Tail microtask drain.
	await Promise.resolve();
	return platform.events;
}

console.log(`Node ${process.version}`);
console.log(`Bench: ${BURST_COUNT} bursts x ${MOVERS_PER_BURST} cursors (${(PEER_RATIO * 100).toFixed(0)}% peer-relay), topicThrottle=${TOPIC_THROTTLE_MS}ms`);
console.log('Simulates demo profile: local + peer-relayed cursors co-arriving in the same sync pass.\n');

console.log('--- Variant A: sync leading-edge (pre-fix shape) ---');
const sync = await runBursts(makeVariantSync());
console.log(`  UPDATE frames: ${sync.update.toLocaleString()}`);
console.log(`  BULK frames:   ${sync.bulk.toLocaleString()}`);
console.log(`  ratio U:B:     ${sync.bulk ? (sync.update / sync.bulk).toFixed(2) : 'inf'}`);
if (sync.bulkSizes.length > 0) {
	const meanBulk = sync.bulkSizes.reduce((a, b) => a + b, 0) / sync.bulkSizes.length;
	console.log(`  bulk size:     mean=${meanBulk.toFixed(1)}, target=${MOVERS_PER_BURST}`);
}

console.log('\n--- Variant B: microtask-deferred leading-edge (post-fix shape) ---');
const deferred = await runBursts(makeVariantDeferred());
console.log(`  UPDATE frames: ${deferred.update.toLocaleString()}`);
console.log(`  BULK frames:   ${deferred.bulk.toLocaleString()}`);
console.log(`  ratio U:B:     ${deferred.bulk ? (deferred.update / deferred.bulk).toFixed(2) : 'inf'}`);
if (deferred.bulkSizes.length > 0) {
	const meanBulk = deferred.bulkSizes.reduce((a, b) => a + b, 0) / deferred.bulkSizes.length;
	const maxBulk = Math.max(...deferred.bulkSizes);
	console.log(`  bulk size:     mean=${meanBulk.toFixed(1)}, max=${maxBulk}, target=${MOVERS_PER_BURST}`);
}

const totalFramesA = sync.update + sync.bulk;
const totalFramesB = deferred.update + deferred.bulk;
console.log(`\nTotal frames: ${totalFramesA.toLocaleString()} -> ${totalFramesB.toLocaleString()}  (${(totalFramesA / totalFramesB).toFixed(1)}x reduction)`);
console.log(`UPDATE fragmentation: ${sync.update.toLocaleString()} -> ${deferred.update.toLocaleString()}  (${sync.update > 0 ? ((1 - deferred.update / sync.update) * 100).toFixed(1) : 'inf'}% reduction)`);
