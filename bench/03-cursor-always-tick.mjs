// A/B/C bench for the cursor flush dispatch model in redis/cursor.js
// broadcast() and enqueueInbound(). Drives broadcasts ACROSS task
// boundaries (await Promise.resolve() between each) - the input shape uWS
// actually produces, because each WS message is dispatched as its own JS
// task with a microtask drain at the C++/JS boundary between dispatches.
//
// Three variants:
//   A. sync leading-edge   (the pre-0.5.5 shape)
//   B. queueMicrotask defer (the 0.5.5/0.5.6 shape - the wrong fix)
//   C. always-tick         (the 0.5.7 shape - this release)
//
// Both A and B should fragment under cross-task driving (every broadcast
// fires its own UPDATE since the previous task's queued microtask has
// already drained). C should produce ONE bulk per cadence cycle covering
// every broadcast that landed before the timers phase.
//
// Why A and B both fail under cross-task driving but the 0.5.6 bench
// showed B "working": the 0.5.6 bench drove all broadcasts in a single
// synchronous loop with no `await` between them, so the queueMicrotask
// callback fired ONCE at the end of that loop with all entries in
// `dirty`. Real uWS dispatches each message as its own task. setTimeout(0)
// is the only callback that lands AFTER the poll phase processes every
// ready message on every socket, so always-tick is structurally correct
// independent of dispatch model.
//
// Usage:
//   node bench/03-cursor-always-tick.mjs [movers] [bursts] [peerRatio]
//
// Defaults: 50 movers per burst, 60 bursts, 20% peer-relay.

const MOVERS_PER_BURST = parseInt(process.argv[2] || '50', 10);
const BURST_COUNT = parseInt(process.argv[3] || '60', 10);
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

function flushNow(topic, state, platform) {
	const entries = [];
	for (const [k, v] of state.dirty) entries.push({ key: k, data: v.data });
	for (const [k, v] of state.inboundDirty) entries.push({ key: k, data: v.data });
	state.dirty.clear();
	state.inboundDirty.clear();
	if (entries.length === 0) return;
	if (entries.length === 1) platform.publish(topic, 'update', entries[0]);
	else platform.publish(topic, 'bulk', entries);
}

// ----- Variant A: sync leading-edge ------------------------------------

function makeVariantSync() {
	const topicFlush = new Map();
	function call(map, topic, key, data, platform) {
		let state = topicFlush.get(topic);
		if (!state) {
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0 };
			topicFlush.set(topic, state);
		}
		state[map].set(key, { data });
		const now = Date.now();
		if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
			state.lastFlush = now;
			flushNow(topic, state, platform);
		}
	}
	return {
		broadcast: (t, k, d, p) => call('dirty', t, k, d, p),
		enqueueInbound: (t, k, d, p) => call('inboundDirty', t, k, d, p)
	};
}

// ----- Variant B: queueMicrotask defer ---------------------------------

function makeVariantMicrotask() {
	const topicFlush = new Map();
	function schedule(topic, state, platform) {
		if (state.pendingMicroflush) return;
		state.pendingMicroflush = true;
		queueMicrotask(() => {
			state.pendingMicroflush = false;
			flushNow(topic, state, platform);
		});
	}
	function call(map, topic, key, data, platform) {
		let state = topicFlush.get(topic);
		if (!state) {
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: 0, pendingMicroflush: false };
			topicFlush.set(topic, state);
		}
		state[map].set(key, { data });
		const now = Date.now();
		if (now - state.lastFlush >= TOPIC_THROTTLE_MS) {
			state.lastFlush = now;
			schedule(topic, state, platform);
		}
	}
	return {
		broadcast: (t, k, d, p) => call('dirty', t, k, d, p),
		enqueueInbound: (t, k, d, p) => call('inboundDirty', t, k, d, p)
	};
}

// ----- Variant C: always-tick ------------------------------------------

function makeVariantAlwaysTick() {
	const topicFlush = new Map();
	const dirtyTopics = new Set();
	let tickTimer = null;

	function tick(platform) {
		tickTimer = null;
		const now = Date.now();
		let nextDeadline = Infinity;
		for (const topic of dirtyTopics) {
			const state = topicFlush.get(topic);
			if (!state) { dirtyTopics.delete(topic); continue; }
			const deadline = state.lastFlush + TOPIC_THROTTLE_MS;
			if (deadline <= now) {
				flushNow(topic, state, platform);
				dirtyTopics.delete(topic);
				state.lastFlush = now - (now - deadline);
			} else if (deadline < nextDeadline) {
				nextDeadline = deadline;
			}
		}
		if (nextDeadline !== Infinity) {
			tickTimer = setTimeout(() => tick(platform), Math.max(0, nextDeadline - Date.now()));
		}
	}

	function armTick(platform, delay) {
		if (tickTimer !== null) return;
		tickTimer = setTimeout(() => tick(platform), delay);
	}

	function call(map, topic, key, data, platform) {
		let state = topicFlush.get(topic);
		if (!state) {
			state = { dirty: new Map(), inboundDirty: new Map(), lastFlush: Date.now() - TOPIC_THROTTLE_MS };
			topicFlush.set(topic, state);
		}
		state[map].set(key, { data });
		dirtyTopics.add(topic);
		const elapsed = Date.now() - state.lastFlush;
		const delay = elapsed >= TOPIC_THROTTLE_MS ? 0 : TOPIC_THROTTLE_MS - elapsed;
		armTick(platform, delay);
	}

	return {
		broadcast: (t, k, d, p) => call('dirty', t, k, d, p),
		enqueueInbound: (t, k, d, p) => call('inboundDirty', t, k, d, p)
	};
}

// ----- Test driver: cross-task dispatch --------------------------------

async function runBursts(variant) {
	const platform = makeRecordingPlatform();
	const peerCount = Math.floor(MOVERS_PER_BURST * PEER_RATIO);
	const localCount = MOVERS_PER_BURST - peerCount;
	const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

	for (let burst = 0; burst < BURST_COUNT; burst++) {
		// Each broadcast lives in its own task (microtask boundary
		// between calls). This mirrors uWS's per-message JS dispatch.
		for (let i = 0; i < localCount; i++) {
			variant.broadcast('canvas', 'L' + i, { x: burst, y: i }, platform);
			await Promise.resolve();
		}
		for (let i = 0; i < peerCount; i++) {
			variant.enqueueInbound('canvas', 'P' + i, { x: burst, y: i }, platform);
			await Promise.resolve();
		}
		// Wait one cadence cycle so the always-tick variant's timer
		// fires before the next burst.
		await sleep(TOPIC_THROTTLE_MS + 2);
	}
	// Tail drain.
	await sleep(TOPIC_THROTTLE_MS + 5);
	return platform.events;
}

function report(name, events) {
	console.log(`\n--- ${name} ---`);
	console.log(`  UPDATE frames: ${events.update.toLocaleString()}`);
	console.log(`  BULK frames:   ${events.bulk.toLocaleString()}`);
	const ratio = events.bulk ? (events.update / events.bulk).toFixed(2) : 'inf';
	console.log(`  ratio U:B:     ${ratio}`);
	if (events.bulkSizes.length > 0) {
		const meanBulk = events.bulkSizes.reduce((a, b) => a + b, 0) / events.bulkSizes.length;
		const maxBulk = Math.max(...events.bulkSizes);
		console.log(`  bulk size:     mean=${meanBulk.toFixed(1)}, max=${maxBulk}, target=${MOVERS_PER_BURST}`);
	}
}

console.log(`Node ${process.version}`);
console.log(`Bench: ${BURST_COUNT} bursts x ${MOVERS_PER_BURST} cursors (${(PEER_RATIO * 100).toFixed(0)}% peer-relay), topicThrottle=${TOPIC_THROTTLE_MS}ms`);
console.log('Each broadcast crosses a microtask boundary (await Promise.resolve()) - models uWS per-message dispatch.');
console.log('Target: variant C produces ~BURST_COUNT bulks of MOVERS_PER_BURST entries each; variants A and B fragment.');

report('Variant A: sync leading-edge', await runBursts(makeVariantSync()));
report('Variant B: queueMicrotask defer (the 0.5.5/0.5.6 shape, WRONG under cross-task)', await runBursts(makeVariantMicrotask()));
report('Variant C: always-tick (the 0.5.7 shape, structurally coalesces)', await runBursts(makeVariantAlwaysTick()));
