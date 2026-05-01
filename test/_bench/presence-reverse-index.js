// Bench: presence reverse-index proposal.
//
// Current code (presence.js:558, 657) does a linear scan across every
// ws on the instance to find another connection for the same (topic, key).
// Reverse-index variant maintains Map<topic|key, Set<ws>> on every
// join/leave so the lookup is O(K) where K is connections-per-user-per-
// topic instead of O(N) over all wsTopics.
//
// The trade is: extra Map/Set maintenance on every join (a hot path).
// For the change to be net positive, leave-time savings have to outweigh
// the per-join overhead at realistic workloads.

import { bench, compare } from './harness.js';

function buildWorld(numWs, topicsPerWs, usersPerTopic) {
	// Assign each ws a stable user key cycling through usersPerTopic.
	// Each ws joins topicsPerWs random-ish topics from a pool.
	const wsTopics = new Map();
	const wsList = [];
	for (let i = 0; i < numWs; i++) {
		const ws = { id: i };
		const topics = new Map();
		const userKey = 'u' + (i % (numWs / usersPerTopic | 0 || 1));
		for (let t = 0; t < topicsPerWs; t++) {
			const topic = 'room-' + ((i + t * 7) % 200);
			topics.set(topic, { key: userKey, data: { v: i } });
		}
		wsTopics.set(ws, topics);
		wsList.push({ ws, userKey, topicsPerWs });
	}
	return { wsTopics, wsList };
}

// Current implementation: linear scan over wsTopics.
function findNewestLinear(wsTopics, leavingWs, topic, userKey) {
	let newest = null;
	for (const [otherWs, otherTopics] of wsTopics) {
		if (otherWs === leavingWs) continue;
		const entry = otherTopics.get(topic);
		if (entry && entry.key === userKey) newest = entry.data;
	}
	return newest;
}

// Reverse-index implementation: O(K) lookup.
function buildReverseIndex(wsTopics) {
	const index = new Map(); // topic|key -> Set<ws>
	for (const [ws, topics] of wsTopics) {
		for (const [topic, { key }] of topics) {
			const k = topic + '|' + key;
			let set = index.get(k);
			if (!set) {
				set = new Set();
				index.set(k, set);
			}
			set.add(ws);
		}
	}
	return index;
}

function findNewestIndexed(index, wsTopics, leavingWs, topic, userKey) {
	const set = index.get(topic + '|' + userKey);
	if (!set) return null;
	let newest = null;
	for (const ws of set) {
		if (ws === leavingWs) continue;
		const entry = wsTopics.get(ws).get(topic);
		if (entry && entry.key === userKey) newest = entry.data;
	}
	return newest;
}

// Maintenance cost of the reverse index: per-join add and per-leave remove.
function indexAdd(index, ws, topic, key) {
	const k = topic + '|' + key;
	let set = index.get(k);
	if (!set) {
		set = new Set();
		index.set(k, set);
	}
	set.add(ws);
}

function indexRemove(index, ws, topic, key) {
	const k = topic + '|' + key;
	const set = index.get(k);
	if (!set) return;
	set.delete(ws);
	if (set.size === 0) index.delete(k);
}

function runScenario(numWs, topicsPerWs, usersPerTopic) {
	console.log(`\nScenario: ${numWs} ws × ${topicsPerWs} topics, ~${usersPerTopic} ws per (topic, user)`);
	const { wsTopics, wsList } = buildWorld(numWs, topicsPerWs, usersPerTopic);
	const index = buildReverseIndex(wsTopics);

	// 1. leave-time: per-call cost of finding the newest other connection.
	//    Use the worst-case "scan all wsTopics" pattern.
	const sample = wsList[Math.floor(numWs / 2)];
	const sampleTopic = wsTopics.get(sample.ws).keys().next().value;

	const linearLeave = bench(
		`  linear scan (leave)`,
		() => findNewestLinear(wsTopics, sample.ws, sampleTopic, sample.userKey),
		{ iters: numWs >= 5000 ? 1000 : 5000 }
	);
	const indexedLeave = bench(
		`  indexed lookup (leave)`,
		() => findNewestIndexed(index, wsTopics, sample.ws, sampleTopic, sample.userKey),
		{ iters: 50000 }
	);
	compare('  leave: indexed vs linear', linearLeave, indexedLeave);

	// 2. join-time maintenance: cost of one indexAdd per join.
	// Use a pre-populated set to avoid measuring Set construction/teardown
	// (the realistic case is adding to an already-populated index entry).
	indexAdd(index, { id: 'sentinel-A' }, 'bench-topic', 'bench-user');
	indexAdd(index, { id: 'sentinel-B' }, 'bench-topic', 'bench-user');
	const joinWs = { id: 'join-ws' };
	const joinAdd = bench(
		`  index add (per join, populated set)`,
		() => {
			indexAdd(index, joinWs, 'bench-topic', 'bench-user');
			indexRemove(index, joinWs, 'bench-topic', 'bench-user');
		},
		{ iters: 200000 }
	);
	console.log(`  per-join overhead: ${(joinAdd.nsPerOp / 2).toFixed(0)} ns (one add to populated set)`);

	// 3. break-even: how many joins per leave can we afford before the
	//    overhead eats the savings?
	const saved = linearLeave.nsPerOp - indexedLeave.nsPerOp;
	const cost = joinAdd.nsPerOp / 2;
	if (saved > 0 && cost > 0) {
		console.log(`  break-even: ${(saved / cost).toFixed(0)} joins per leave (= leave saves ${saved.toFixed(0)} ns, join costs ${cost.toFixed(0)} ns)`);
	}
}

runScenario(100, 5, 1);
runScenario(1000, 5, 1);
runScenario(5000, 10, 2);
runScenario(10000, 20, 3);
