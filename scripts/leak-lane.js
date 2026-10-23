// The standing leak lane for a Redis-backed deployment: does any instance's
// memory, any instance's handle or listener count, or the shared Redis keyspace
// grow under a workload the deployment should sustain indefinitely?
//
// The adapter's own leak lane covers one process. What this package adds lives
// BETWEEN processes: the pub/sub relay, presence, group membership and replay
// buffers, all held in Redis on behalf of several instances at once. So this
// lane boots a real Redis (or a real Redis Cluster), forks several server
// instances that reach each other only through it, churns real WebSocket
// clients against every instance at a fixed rate, and judges each process and
// the keyspace separately.
//
// The verdict rules are the adapter lane's, consumed through its public trend
// kernel (`svelte-adapter-uws/sim`), with its thresholds spelled the same way.
// The reasons for each rule are the same too:
//
//   A SLOPE THROUGH A CLOUD IS NOT A TREND. The fit must explain the samples
//   (r-squared at or above 0.5) before a slope counts.
//
//   A FORCED COLLECTION MANUFACTURES THE SIGNATURE IT LOOKS FOR. The baseline
//   is collected, the workload keeps running through an unsampled resettle
//   window, and the window opens only once every instance has stopped moving.
//
//   A GATE THAT CANNOT FAIL IS NOT A GATE. One instance of the deployment is a
//   self-check: once the window opens it leaks a buffer, a Redis listener and
//   a Redis key with no expiry per closed connection, and the lane requires
//   the growth gate, the listener gate and the key gate to each catch their
//   own planted leak. A lane that has gone blind says so instead of passing.
//
// What this lane adds over the single-process one, and why:
//
//   THE KEYSPACE IS NOT IN ANY PROCESS. Replay, presence and group keys that
//   never expire under churn are invisible to a resident-set probe, so the
//   lane samples the deployment's key count across the window and, after the
//   cooldown, requires the keys without an expiry to be no more than the
//   window opened with and the membership left behind by closed clients to
//   be zero.
//
//   HANDLES AND LISTENERS. A Redis subscriber that adds a listener per
//   connection, or a timer per topic that is never cleared, costs almost no
//   memory per unit and takes hours to show in a resident set. Both are
//   counted directly, trended across the window and compared after the
//   cooldown.
//
// EXIT CODES. 0 clean; 1 a verdict failed (a leak, an error rate, a latency
// creep, or a self-check whose planted leak went undetected); 2 the lane could
// not reach a verdict it trusts (an instance never became ready, a window never
// settled, the cross-instance relay carried nothing).
//
// Usage:
//   node scripts/leak-lane.js                      standalone Redis, default window
//   node scripts/leak-lane.js --backend cluster    a Redis Cluster instead
//   node scripts/leak-lane.js --minutes 2          shorter window (1 through 60 minutes)
//   node scripts/leak-lane.js --out <dir>          where the record is written (default .leak-lane/)
//
// The Redis stack is brought up and torn down through the integration
// setups, so the isolation variables in CONTRIBUTING.md apply unchanged.

import { fork } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { detectGrowth } from 'svelte-adapter-uws/sim';
import { isCluster } from '../src/shared/cluster.js';

const require = createRequire(import.meta.url);
const instanceEntry = fileURLToPath(new URL('./fixtures/leak-lane-instance.mjs', import.meta.url));

/** Where a run leaves its record unless --out says otherwise: .leak-lane/ in this repo, gitignored. */
const DEFAULT_OUT = fileURLToPath(new URL('../.leak-lane/', import.meta.url));

// ---------------------------------------------------------------------------
// Verdict rules. Pure, exported, and unit-tested in test/leak-lane-rules.test.js
// so the rules can be driven in milliseconds while the lane itself spends
// minutes. The thresholds shared with the adapter's lane carry its names.
// ---------------------------------------------------------------------------

/** Fit quality below which a slope is not evidence of anything. */
export const MIN_R_SQUARED = 0.5;
/** Share of requests allowed to fail across a window. */
export const MAX_ERROR_RATE = 0.005;
/** How much the window's p95 may exceed the warmed baseline's. */
export const MAX_P95_CREEP = 0.5;
/** Bytes of resident growth across the window that count as flat. */
export const RSS_TOLERANCE_BYTES = 16 * 1024 * 1024;
/** Bytes per sample below which a slope is not worth escalating. */
export const RSS_MIN_SLOPE_BYTES = 256 * 1024;
/** Settled retention growth allowed across the run, as a share AND in bytes: both must be exceeded. */
export const MAX_RETAINED_GROWTH = 0.3;
export const RETAINED_TOLERANCE_BYTES = 2 * 1024 * 1024;
/** Load applied before anything is measured, so the working set is filled first. */
export const WARMUP_MS = 60000;
/** Worked but unsampled after the forced collection, before the settle probe begins. */
export const RESETTLE_MS = 20000;
/** Idle time after the window before the final reading, so every client has finished closing. */
export const COOLDOWN_MS = 3000;
/**
 * Four-second settle rounds the deployment may take before the window opens
 * anyway, reported unsettled. Every instance must hold still for eight rounds
 * in a row at the same time, and after the baseline collection each one climbs
 * back to its working set in steps that can take a few minutes; a cap that
 * fits one process cuts several off mid-climb.
 */
export const SETTLE_MAX_ROUNDS = 60;
/** Forced collections a settled reading may take before it is reported unsettled. */
export const SETTLED_PROBE_ROUNDS = 8;
/** Two consecutive settled readings agreeing within this share are one reading. */
export const SETTLED_PROBE_TOLERANCE = 0.02;

/**
 * Handles an instance may hold after the cooldown beyond what it held when the
 * window opened. The baseline is read under load, so it already carries the
 * few in-flight sockets and timers of a healthy instance.
 */
export const HANDLE_TOLERANCE = 16;
/** Redis connection listeners allowed beyond the baseline, by the same reading. */
export const LISTENER_TOLERANCE = 8;
/**
 * Keys the deployment's key count may move across the window without counting
 * as growth: the per-connection presence keys of the clients in flight.
 */
export const KEY_TOLERANCE = 64;
/** Keys without an expiry allowed after the cooldown beyond the baseline. */
export const PERSISTENT_KEY_TOLERANCE = 8;

/** Server instances in the deployment, the self-check among them. */
export const INSTANCES = 3;
/** Connections churned per second against each instance. */
export const CHURN_RATE_PER_INSTANCE = 20;
/** Held connections per instance, subscribed to every topic, that receive the relay. */
export const HELD_PER_INSTANCE = 2;
/**
 * A small fixed topic set: a unique topic per connection grows the per-topic
 * replay buffers toward a bound by design, which reads like a leak until the
 * bound is reached.
 */
export const CHURN_TOPICS = Array.from({ length: 16 }, (_, i) => `churn-${i}`);
/** Share of the other instances' publishes the held clients must see for the relay to count as exercised. */
export const MIN_RELAY_SHARE = 0.5;

/**
 * @param {number[]} values
 * @param {number} p in [0,1]
 */
export function quantile(values, p) {
	if (values.length === 0) return 0;
	const sorted = values.slice().sort((a, b) => a - b);
	return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}

/**
 * What the retention gate compares: heap PLUS external. A Buffer lives outside
 * the JS heap, and buffers are what a server retains.
 *
 * @param {{ heapUsed: number, external?: number }} p
 */
export const retainedBytes = (p) => p.heapUsed + (p.external || 0);

/** The kernel options for a count that should hold flat: a structural series. */
function countGrowth(samples, tolerance) {
	return detectGrowth(samples, { warmup: 0, tolerance, minSlope: 0, minRSquared: MIN_R_SQUARED });
}

/**
 * Turn one instance's measurements into a verdict.
 *
 * The memory, error and latency gates are the adapter lane's. Two more read
 * what a resident set cannot: the handle count and the Redis listener count,
 * each trended across the window and compared again after the cooldown.
 *
 * @param {{
 *   rss: number[],
 *   handles?: number[],
 *   listeners?: number[],
 *   errors: number,
 *   requests: number,
 *   baselineP95: number,
 *   windowP95: number,
 *   settled: boolean,
 *   retained?: { baseline: number, final: number, settled: boolean },
 *   connectionsAfter?: number,
 *   handlesBefore?: number, handlesAfter?: number,
 *   listenersBefore?: number, listenersAfter?: number
 * }} m
 */
export function judge(m) {
	const growth = detectGrowth(m.rss, {
		warmup: 0,
		tolerance: RSS_TOLERANCE_BYTES,
		minSlope: RSS_MIN_SLOPE_BYTES,
		minRSquared: MIN_R_SQUARED,
		// A resident set wanders by several MiB between samples while V8
		// collects, so a leaking process still steps backwards often. The fit
		// quality carries the noise judgment instead of the monotonic vote.
		minMonotonicFraction: 0
	});
	const handleGrowth = m.handles ? countGrowth(m.handles, HANDLE_TOLERANCE) : null;
	const listenerGrowth = m.listeners ? countGrowth(m.listeners, LISTENER_TOLERANCE) : null;

	const errorRate = m.requests > 0 ? m.errors / m.requests : 0;
	const p95Creep = m.baselineP95 > 0 ? (m.windowP95 - m.baselineP95) / m.baselineP95 : null;

	let retained = null;
	if (m.retained) {
		const delta = m.retained.final - m.retained.baseline;
		const ratio = m.retained.baseline > 0 ? delta / m.retained.baseline : 0;
		retained = {
			baseline: m.retained.baseline, final: m.retained.final, delta, ratio,
			leaking: ratio > MAX_RETAINED_GROWTH && delta > RETAINED_TOLERANCE_BYTES
		};
	}

	const handlesKept = Number.isInteger(m.handlesBefore) && Number.isInteger(m.handlesAfter)
		? m.handlesAfter - m.handlesBefore : null;
	const listenersKept = Number.isInteger(m.listenersBefore) && Number.isInteger(m.listenersAfter)
		? m.listenersAfter - m.listenersBefore : null;
	const handlesLeaking = (handleGrowth !== null && handleGrowth.leaking) || (handlesKept !== null && handlesKept > HANDLE_TOLERANCE);
	const listenersLeaking = (listenerGrowth !== null && listenerGrowth.leaking) || (listenersKept !== null && listenersKept > LISTENER_TOLERANCE);

	/** @type {string[]} */
	const failures = [];
	/** @type {string[]} */
	const health = [];

	if (!m.settled) health.push('the resident set never settled before the window opened, so nothing measured here is a baseline');
	if (m.rss.length < 8) health.push(`only ${m.rss.length} resident-set sample(s): the growth fit needs 8, so it could not judge`);
	if (p95Creep === null) health.push('no warmed baseline latency, so the creep gate could not run');
	if (m.retained && !m.retained.settled) health.push('retention never settled at one end of the run, so the retained comparison is noise');
	if ('connectionsAfter' in m && !(Number.isInteger(m.connectionsAfter) && m.connectionsAfter >= 0)) {
		health.push('the probe could not read the live connection count, so the connections gate did not run');
	}

	if (growth.leaking) {
		failures.push(
			`resident set grew ${(growth.delta / 1048576).toFixed(1)} MiB over ${growth.n} samples ` +
			`(slope ${(growth.slope / 1024).toFixed(0)} KiB/sample, r-squared ${growth.rSquared.toFixed(2)})`
		);
	}
	if (retained && retained.leaking) {
		failures.push(
			`retained (heap+external) grew ${(retained.ratio * 100).toFixed(1)}% across the run ` +
			`(${(retained.baseline / 1048576).toFixed(1)} MiB settled to ${(retained.final / 1048576).toFixed(1)} MiB)`
		);
	}
	if (Number.isInteger(m.connectionsAfter) && m.connectionsAfter > 0) {
		failures.push(`${m.connectionsAfter} WebSocket connection(s) still registered after cooldown; every client had closed`);
	}
	if (handlesLeaking) {
		failures.push(`active handles grew from ${m.handlesBefore} to ${m.handlesAfter}` +
			(handleGrowth && handleGrowth.leaking ? ` (trend ${handleGrowth.reason}, r-squared ${handleGrowth.rSquared.toFixed(2)})` : ''));
	}
	if (listenersLeaking) {
		failures.push(`Redis connection listeners grew from ${m.listenersBefore} to ${m.listenersAfter}` +
			(listenerGrowth && listenerGrowth.leaking ? ` (trend ${listenerGrowth.reason}, r-squared ${listenerGrowth.rSquared.toFixed(2)})` : ''));
	}
	if (errorRate > MAX_ERROR_RATE) {
		failures.push(`${(errorRate * 100).toFixed(2)}% of ${m.requests} requests failed, over the ${(MAX_ERROR_RATE * 100).toFixed(2)}% ceiling`);
	}
	if (p95Creep !== null && p95Creep > MAX_P95_CREEP) {
		failures.push(`p95 latency crept ${(p95Creep * 100).toFixed(0)}% (${m.baselineP95.toFixed(1)}ms warmed to ${m.windowP95.toFixed(1)}ms)`);
	}

	return {
		growth,
		...(retained ? { retained } : {}),
		handles: { trend: handleGrowth, kept: handlesKept, leaking: handlesLeaking },
		listeners: { trend: listenerGrowth, kept: listenersKept, leaking: listenersLeaking },
		errorRate, p95Creep, failures, health, failing: failures.length > 0
	};
}

/**
 * The verdict on the Redis side, which belongs to the deployment rather than
 * to any one instance.
 *
 * @param {{
 *   keys: number[],
 *   persistentBefore: number,
 *   persistentAfter: number,
 *   membersAfter: number,
 *   relayShare: number | null
 * }} m
 */
export function judgeKeyspace(m) {
	const growth = countGrowth(m.keys, KEY_TOLERANCE);
	const persistentKept = m.persistentAfter - m.persistentBefore;
	/** @type {string[]} */
	const failures = [];
	/** @type {string[]} */
	const health = [];
	if (m.keys.length < 8) health.push(`only ${m.keys.length} key-count sample(s): the growth fit needs 8, so it could not judge`);
	if (m.relayShare === null || m.relayShare < MIN_RELAY_SHARE) {
		health.push('the held clients saw ' + (m.relayShare === null ? 'no' : `${(m.relayShare * 100).toFixed(0)}% of the`) +
			' publishes made on other instances, so the cross-instance relay was not exercised');
	}
	if (growth.leaking) {
		failures.push(`the deployment's Redis keys grew from ${growth.first} to ${growth.last} over ${growth.n} samples ` +
			`(slope ${growth.slope.toFixed(1)} keys/sample, r-squared ${growth.rSquared.toFixed(2)})`);
	}
	if (persistentKept > PERSISTENT_KEY_TOLERANCE) {
		failures.push(`${m.persistentAfter} Redis keys without an expiry after cooldown, ${persistentKept} more than when the window opened`);
	}
	if (m.membersAfter > 0) {
		failures.push(`${m.membersAfter} presence or group member entries left in Redis after every client had closed`);
	}
	return { growth, persistentKept, failures, health, failing: failures.length > 0 };
}

/**
 * The self-check's own verdict, typed on the gate under test: each planted
 * leak must be caught by the gate that exists for it. A failing verdict on
 * some other gate is not detection, and accepting it would let the lane pass
 * its self-check while the gate it plants for has gone blind.
 *
 * @param {{ growth?: { leaking?: boolean }, listeners?: { leaking?: boolean } }} instance
 * @param {{ leaking?: boolean }} plantedKeys the key gate's report on the planted keys
 * @returns {{ pass: boolean, reasons: string[] }}
 */
export function selfCheckVerdict(instance, plantedKeys) {
	/** @type {string[]} */
	const reasons = [];
	if (!(instance.growth && instance.growth.leaking === true)) reasons.push('the planted memory growth went undetected by the growth gate');
	if (!(instance.listeners && instance.listeners.leaking === true)) reasons.push('the planted Redis listeners went undetected by the listener gate');
	if (!(plantedKeys && plantedKeys.leaking === true)) reasons.push('the planted Redis keys went undetected by the key gate');
	return { pass: reasons.length === 0, reasons };
}

/** Read either --name value or --name=value without silently dropping a value. */
export function readFlag(argv, name, fallback) {
	const key = `--${name}`;
	const at = argv.findIndex((arg) => arg === key || arg.startsWith(key + '='));
	if (at < 0) return fallback;
	const inline = argv[at].startsWith(key + '=');
	const value = inline ? argv[at].slice(key.length + 1) : argv[at + 1];
	if (value === undefined || value === '' || (!inline && value.startsWith('--'))) {
		throw new Error(`${key} requires a value`);
	}
	return value;
}

// ---------------------------------------------------------------------------
// The lane itself.
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * The young generation pinned at one size in every instance.
 *
 * V8 starts it at 1 MiB and grows it under allocation pressure, and a forced
 * collection shrinks it again. An instance driven by this workload grows it to
 * 128 MiB in steps minutes apart, so after the baseline collection the
 * resident set climbs by some 45 MiB on a staircase that the fit reads as a
 * leak at r-squared 0.6. Pinned, that climb is gone, and nothing a leak can
 * occupy is pinned: the old generation, large objects and external memory
 * still grow freely.
 */
const YOUNG_GENERATION_FLAGS = ['--min-semi-space-size=16', '--max-semi-space-size=16'];

/**
 * Fork one instance and wait for it to listen.
 *
 * @param {'instance' | 'selfcheck'} role
 * @param {{ keyPrefix: string, plantedPrefix: string }} prefixes
 */
async function startInstance(role, prefixes) {
	const child = fork(instanceEntry, [], {
		execArgv: ['--expose-gc', ...YOUNG_GENERATION_FLAGS],
		stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
		env: { ...process.env, LEAK_LANE_ROLE: role, LEAK_LANE_KEY_PREFIX: prefixes.keyPrefix, LEAK_LANE_PLANTED_PREFIX: prefixes.plantedPrefix }
	});
	let output = '';
	const keep = (buf) => { output = (output + buf.toString()).slice(-8192); };
	child.stdout.on('data', keep);
	child.stderr.on('data', keep);
	/** @type {Map<number, (body: any) => void>} */
	const pending = new Map();
	let nextId = 1;
	const ready = await new Promise((resolve) => {
		const timer = setTimeout(() => resolve(null), 60000);
		child.on('message', (msg) => {
			if (msg && msg.ready) { clearTimeout(timer); resolve(msg); return; }
			const settle = msg && pending.get(msg.id);
			if (settle) { pending.delete(msg.id); settle(msg); }
		});
		child.on('exit', () => { clearTimeout(timer); resolve(null); });
	});
	if (!ready) {
		child.kill('SIGKILL');
		throw new Error(`a ${role} never became ready\n${output}`);
	}
	/** @param {string} op */
	const call = (op) => new Promise((resolve, reject) => {
		const id = nextId++;
		const timer = setTimeout(() => { pending.delete(id); reject(new Error(`${role} did not answer "${op}"\n${output}`)); }, 30000);
		pending.set(id, (body) => { clearTimeout(timer); resolve(body); });
		child.send({ op, id });
	});
	return { role, port: ready.port, child, call, output: () => output };
}

/**
 * A heap reading that has stopped moving, for the retention gate: forced
 * collections until two consecutive readings of heap plus external agree.
 */
async function settledProbe(instance) {
	const gc = await instance.call('gc');
	if (gc.collected !== true) throw new Error(`${instance.role} could not force a collection (${gc.reason || 'no reason given'})`);
	let last = await instance.call('mem');
	for (let round = 2; round <= SETTLED_PROBE_ROUNDS; round++) {
		await sleep(200);
		await instance.call('gc');
		const next = await instance.call('mem');
		const agreed = Math.abs(retainedBytes(next) - retainedBytes(last)) <= retainedBytes(last) * SETTLED_PROBE_TOLERANCE;
		last = next;
		if (agreed) return { ...last, rounds: round, unsettled: false };
	}
	return { ...last, rounds: SETTLED_PROBE_ROUNDS, unsettled: true };
}

/**
 * Connect as a fresh user, subscribe, publish one event through the replay
 * buffer and wait for it to come back, then close. Presence, group membership
 * and the bus all see a connection arrive and leave.
 *
 * @param {number} port @param {string} user @param {number} n
 * @returns {Promise<{ ms: number, ok: boolean }>}
 */
function churnOnce(port, user, n) {
	const { WebSocket } = require('ws');
	const started = performance.now();
	const topic = CHURN_TOPICS[n % CHURN_TOPICS.length];
	return new Promise((resolve) => {
		const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?u=${user}`);
		let done = false;
		const finish = (ok) => {
			if (done) return;
			done = true;
			clearTimeout(timer);
			try { ws.close(); } catch { /* already closing */ }
			resolve({ ms: performance.now() - started, ok });
		};
		const timer = setTimeout(() => finish(false), 5000);
		ws.on('error', () => finish(false));
		ws.on('open', () => ws.send(JSON.stringify({ type: 'subscribe', topic, ref: 1 })));
		ws.on('message', (data) => {
			let msg = null;
			try { msg = JSON.parse(data.toString()); } catch { /* not a JSON frame */ }
			if (!msg) return;
			if (msg.type === 'subscribe-denied') finish(false);
			else if (msg.type === 'subscribed' && msg.topic === topic) ws.send(JSON.stringify({ type: 'publish', topic, data: { user, n } }));
			else if (msg.topic === topic && msg.event === 'tick' && msg.data && msg.data.user === user) finish(true);
		});
	});
}

/**
 * A held client subscribed to every churn topic, counting the publishes it
 * receives by the instance they were made on.
 *
 * @param {number} port @param {string} user
 */
async function holdClient(port, user) {
	const { WebSocket } = require('ws');
	const ws = new WebSocket(`ws://127.0.0.1:${port}/ws?u=${user}`);
	/** @type {Map<string, number>} */
	const seen = new Map();
	let subscribed = 0;
	ws.on('message', (data) => {
		let msg = null;
		try { msg = JSON.parse(data.toString()); } catch { return; }
		if (msg && msg.type === 'subscribed') subscribed++;
		else if (msg && msg.event === 'tick' && msg.data && typeof msg.data.user === 'string') {
			const origin = msg.data.user.split('-')[1];
			seen.set(origin, (seen.get(origin) || 0) + 1);
		}
	});
	await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
	for (const topic of CHURN_TOPICS) ws.send(JSON.stringify({ type: 'subscribe', topic, ref: topic }));
	const deadline = Date.now() + 10000;
	while (subscribed < CHURN_TOPICS.length) {
		if (Date.now() > deadline) throw new Error(`a held client on :${port} was never subscribed to every topic`);
		await sleep(20);
	}
	return { ws, seen };
}

/**
 * Drive the workload at a FIXED rate for `ms`: a closed loop slows down when
 * the server does, which hides the latency creep the gate is looking for.
 * Issued in 25 ms ticks against an absolute schedule, so timer granularity
 * cannot lower the rate.
 *
 * @param {{ ms: number, ratePerSecond: number, drive: (i: number) => Promise<{ ms: number, ok: boolean, instance: number }>, startIndex: number }} spec
 */
async function driveAtRate({ ms, ratePerSecond, drive, startIndex }) {
	const tickMs = 25;
	const began = performance.now();
	const ticks = Math.max(1, Math.round(ms / tickMs));
	/** @type {Promise<{ ms: number, ok: boolean, instance: number }>[]} */
	const inFlight = [];
	let issued = startIndex;
	for (let tick = 0; tick < ticks; tick++) {
		const dueCount = Math.floor(((tick + 1) * tickMs * ratePerSecond) / 1000);
		while (inFlight.length < dueCount) inFlight.push(drive(issued++));
		const wait = began + (tick + 1) * tickMs - performance.now();
		if (wait > 0) await sleep(wait);
	}
	const results = await Promise.all(inFlight);
	const elapsedMs = performance.now() - began;
	return { results, achievedRatePerSecond: elapsedMs > 0 ? (results.length * 1000) / elapsedMs : 0, nextIndex: issued };
}

/** Count the keys under a pattern that carry no expiry, on every master of a cluster. */
async function countPersistentKeys(client, pattern) {
	const redis = client.redis;
	const nodes = isCluster(redis) ? redis.nodes('master') : [redis];
	let count = 0;
	for (const node of nodes) {
		let cursor = '0';
		do {
			const [next, keys] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 200);
			cursor = next;
			for (const key of keys) {
				// The node SCAN answered from owns the key, so the PTTL needs no redirect.
				if ((await node.pttl(key)) === -1) count++;
			}
		} while (cursor !== '0');
	}
	return count;
}

/**
 * The membership closed clients left behind: presence entries per user and
 * topic, and group member fields. Named, so a failure says what was kept.
 */
async function listMembers(client, keyPrefix) {
	const redis = client.redis;
	const nodes = isCluster(redis) ? redis.nodes('master') : [redis];
	/** @type {string[]} */
	const left = [];
	for (const node of nodes) {
		let cursor = '0';
		do {
			const [next, keys] = await node.scan(cursor, 'MATCH', keyPrefix + 'presence:user:*', 'COUNT', 200);
			cursor = next;
			left.push(...keys);
		} while (cursor !== '0');
	}
	for (const field of await redis.hkeys(keyPrefix + 'group:{room}:members')) left.push('group member ' + field);
	return left;
}

/**
 * @param {{ minutes: number, backend: 'redis' | 'cluster' }} opts
 */
async function runLane({ minutes, backend }) {
	const { createBackendClient, countBackendKeys } = await import('../test/integration/helpers/backend.js');
	const run = Date.now().toString(36);
	const prefixes = { keyPrefix: `leak-lane-${run}:`, plantedPrefix: `leak-lane-planted-${run}:` };
	const admin = createBackendClient({ keyPrefix: prefixes.keyPrefix });
	/** @type {Awaited<ReturnType<typeof startInstance>>[]} */
	const instances = [];
	/** @type {{ ws: import('ws').WebSocket, seen: Map<string, number> }[][]} */
	const held = [];
	try {
		for (let i = 0; i < INSTANCES; i++) {
			instances.push(await startInstance(i === INSTANCES - 1 ? 'selfcheck' : 'instance', prefixes));
		}
		for (let i = 0; i < INSTANCES; i++) {
			const clients = [];
			for (let h = 0; h < HELD_PER_INSTANCE; h++) clients.push(await holdClient(instances[i].port, `held-${i}-${h}-${run}`));
			held.push(clients);
		}

		/** Issued and completed publishes per instance, for the relay share. */
		const published = new Array(INSTANCES).fill(0);
		/** @param {number} i */
		const drive = async (i) => {
			const instance = i % INSTANCES;
			const res = await churnOnce(instances[instance].port, `u-${instance}-${i}-${run}`, i);
			if (res.ok) published[instance]++;
			return { ...res, instance };
		};
		const ratePerSecond = CHURN_RATE_PER_INSTANCE * INSTANCES;
		const perInstance = (results) => instances.map((_, k) => results.filter((r) => r.instance === k));

		// WARMUP: fills the working set and gives the creep gate its baseline.
		let nextIndex = 0;
		const warm = await driveAtRate({ ms: WARMUP_MS, ratePerSecond, drive, startIndex: nextIndex });
		nextIndex = warm.nextIndex;
		const warmByInstance = perInstance(warm.results);

		// SETTLE: collect, keep working unsampled, then wait for every instance's
		// resident set to stop moving in either direction.
		const baselines = [];
		for (const instance of instances) baselines.push(await settledProbe(instance));
		nextIndex = (await driveAtRate({ ms: RESETTLE_MS, ratePerSecond, drive, startIndex: nextIndex })).nextIndex;
		const previous = new Array(INSTANCES).fill(null);
		const stableRounds = new Array(INSTANCES).fill(0);
		const settleReadings = instances.map(() => []);
		let settled = false;
		for (let round = 0; round < SETTLE_MAX_ROUNDS && !settled; round++) {
			nextIndex = (await driveAtRate({ ms: 4000, ratePerSecond, drive, startIndex: nextIndex })).nextIndex;
			for (let k = 0; k < INSTANCES; k++) {
				const { rss } = await instances[k].call('mem');
				settleReadings[k].push(rss);
				if (previous[k] !== null) {
					const band = Math.max(2 * 1048576, previous[k] * 0.01);
					stableRounds[k] = Math.abs(rss - previous[k]) <= band ? stableRounds[k] + 1 : 0;
				}
				previous[k] = rss;
			}
			settled = stableRounds.every((n) => n >= 8);
		}

		// WINDOW: the self-check starts leaking, every instance and the keyspace
		// are sampled every 2s while the same workload continues.
		const before = [];
		for (const instance of instances) before.push(await instance.call('mem'));
		const persistentBefore = await countPersistentKeys(admin, prefixes.keyPrefix + '*');
		for (const instance of instances) await instance.call('arm');
		const publishedBefore = published.slice();
		const seenBefore = held.map((clients) => clients.map((c) => new Map(c.seen)));

		const samples = instances.map(() => ({ rss: [], handles: [], listeners: [] }));
		const keys = [];
		const plantedKeys = [];
		const windowResults = [];
		const achieved = [];
		for (let elapsed = 0; elapsed < minutes * 60000; elapsed += 2000) {
			const slice = await driveAtRate({ ms: 2000, ratePerSecond, drive, startIndex: nextIndex });
			nextIndex = slice.nextIndex;
			achieved.push(slice.achievedRatePerSecond);
			windowResults.push(...slice.results);
			for (let k = 0; k < INSTANCES; k++) {
				const mem = await instances[k].call('mem');
				samples[k].rss.push(mem.rss);
				samples[k].handles.push(mem.handles);
				samples[k].listeners.push(mem.listeners);
			}
			keys.push(await countBackendKeys(admin, prefixes.keyPrefix + '*'));
			plantedKeys.push(await countBackendKeys(admin, prefixes.plantedPrefix + '*'));
		}

		// Relay share: of what the OTHER instances published during the window,
		// how much did each instance's held clients see?
		let relayShare = null;
		for (let k = 0; k < INSTANCES; k++) {
			for (let h = 0; h < HELD_PER_INSTANCE; h++) {
				let expected = 0;
				let got = 0;
				for (let o = 0; o < INSTANCES; o++) {
					if (o === k) continue;
					expected += published[o] - publishedBefore[o];
					got += (held[k][h].seen.get(String(o)) || 0) - (seenBefore[k][h].get(String(o)) || 0);
				}
				const share = expected > 0 ? got / expected : 0;
				relayShare = relayShare === null ? share : Math.min(relayShare, share);
			}
		}

		// FINAL: every client closes, the cooldown lets each close finish, then
		// every instance and the keyspace are read again.
		for (const clients of held) for (const c of clients) c.ws.close();
		await sleep(COOLDOWN_MS);
		const finals = [];
		for (const instance of instances) finals.push(await settledProbe(instance));
		const persistentAfter = await countPersistentKeys(admin, prefixes.keyPrefix + '*');
		const membersLeft = await listMembers(admin, prefixes.keyPrefix);
		const membersAfter = membersLeft.length;

		const windowByInstance = perInstance(windowResults);
		const records = instances.map((instance, k) => {
			const results = windowByInstance[k];
			const warmResults = warmByInstance[k];
			const verdict = judge({
				rss: samples[k].rss,
				handles: samples[k].handles,
				listeners: samples[k].listeners,
				errors: results.filter((r) => !r.ok).length,
				requests: results.length,
				baselineP95: quantile(warmResults.map((r) => r.ms), 0.95),
				windowP95: quantile(results.map((r) => r.ms), 0.95),
				settled,
				retained: { baseline: retainedBytes(baselines[k]), final: retainedBytes(finals[k]), settled: !baselines[k].unsettled && !finals[k].unsettled },
				connectionsAfter: finals[k].connections,
				handlesBefore: before[k].handles, handlesAfter: finals[k].handles,
				listenersBefore: before[k].listeners, listenersAfter: finals[k].listeners
			});
			const warmErrors = warmResults.filter((r) => !r.ok).length;
			if (warmResults.length > 0 && warmErrors / warmResults.length > MAX_ERROR_RATE) {
				verdict.health.push(`the warmup itself lost ${((warmErrors / warmResults.length) * 100).toFixed(2)}% of ${warmResults.length} requests`);
			}
			return {
				role: instance.role, port: instance.port,
				requests: results.length, errors: results.filter((r) => !r.ok).length,
				rss: samples[k].rss, handles: samples[k].handles, listeners: samples[k].listeners,
				settleReadings: settleReadings[k],
				settledRounds: { baseline: baselines[k].rounds, final: finals[k].rounds },
				connectionsAfter: finals[k].connections,
				...verdict
			};
		});
		const keyspace = judgeKeyspace({ keys, persistentBefore, persistentAfter, membersAfter, relayShare });
		const planted = countGrowth(plantedKeys, KEY_TOLERANCE);
		return {
			backend, minutes, ratePerSecond, settled,
			achievedRatePerSecond: achieved.length > 0 ? achieved.reduce((a, b) => a + b, 0) / achieved.length : 0,
			instances: records,
			keyspace: { keys, persistentBefore, persistentAfter, membersAfter, membersLeft: membersLeft.slice(0, 20), relayShare, ...keyspace },
			plantedKeys: { keys: plantedKeys, ...planted }
		};
	} finally {
		for (const clients of held) for (const c of clients) { try { c.ws.terminate(); } catch { /* gone */ } }
		for (const instance of instances) {
			await Promise.race([instance.call('stop').catch(() => {}), sleep(10000)]);
			instance.child.kill('SIGKILL');
		}
		const { resetBackendKeys } = await import('../test/integration/helpers/backend.js');
		await resetBackendKeys(admin, prefixes.keyPrefix + '*').catch(() => {});
		await resetBackendKeys(admin, prefixes.plantedPrefix + '*').catch(() => {});
		await admin.quit().catch(() => {});
	}
}

/** Bring up the integration stack for a backend; returns its teardown. */
async function provision(backend) {
	const setup = backend === 'cluster'
		? (await import('../test/integration/global-setup-cluster-mirror.js')).default
		: (await import('../test/integration/global-setup-redis.js')).default;
	return setup();
}

function report(record) {
	let exit = 0;
	const mib = (b) => (b / 1048576).toFixed(0);
	console.log(`[leak-lane] ${record.backend}: ${record.instances.length} instances at ${record.achievedRatePerSecond.toFixed(0)}/s ` +
		`(asked ${record.ratePerSecond}/s)${record.settled ? '' : ', NEVER STEADY before the window'}`);
	for (const r of record.instances) {
		const name = `${r.role}:${r.port}`;
		console.log(`[leak-lane] ${name}: ${r.requests} churns, ${r.errors} failed; rss MiB ${r.rss.map(mib).join(' ')}`);
		console.log(`[leak-lane] ${name}: fit r-squared ${r.growth.rSquared.toFixed(2)} (${r.growth.reason}); ` +
			`handles ${r.handles.kept === null ? '?' : (r.handles.kept >= 0 ? '+' : '') + r.handles.kept}, ` +
			`listeners ${r.listeners.kept === null ? '?' : (r.listeners.kept >= 0 ? '+' : '') + r.listeners.kept}, ` +
			`connections after cooldown ${r.connectionsAfter}`);
		if (r.role === 'selfcheck') {
			const check = selfCheckVerdict(r, record.plantedKeys);
			if (check.pass) console.log(`[leak-lane] ${name}: selfcheck PASS - every planted leak was caught by its own gate`);
			else {
				for (const reason of check.reasons) console.error(`[leak-lane] ${name}: selfcheck FAIL - ${reason}`);
				exit = Math.max(exit, 1);
			}
		} else if (r.failing) {
			for (const line of r.failures) console.error(`[leak-lane] ${name}: FAIL - ${line}`);
			exit = Math.max(exit, 1);
		} else console.log(`[leak-lane] ${name}: PASS`);
		for (const line of r.health) {
			console.error(`[leak-lane] ${name}: HEALTH - ${line}`);
			exit = Math.max(exit, 2);
		}
	}
	const ks = record.keyspace;
	console.log(`[leak-lane] keyspace: keys ${ks.keys.join(' ')}; without expiry ${ks.persistentBefore} -> ${ks.persistentAfter}; ` +
		`members after cooldown ${ks.membersAfter}; relay share ${ks.relayShare === null ? '?' : (ks.relayShare * 100).toFixed(0) + '%'}`);
	if (ks.failing) {
		for (const line of ks.failures) console.error(`[leak-lane] keyspace: FAIL - ${line}`);
		exit = Math.max(exit, 1);
	} else console.log('[leak-lane] keyspace: PASS');
	for (const line of ks.health) {
		console.error(`[leak-lane] keyspace: HEALTH - ${line}`);
		exit = Math.max(exit, 2);
	}
	return exit;
}

async function main() {
	const argv = process.argv.slice(2);
	const minutesText = readFlag(argv, 'minutes', '3');
	const minutes = Number(minutesText);
	if (!Number.isFinite(minutes) || minutes < 1 || minutes > 60) {
		console.error(`--minutes must be a number from 1 to 60, got "${minutesText}"`);
		process.exit(2);
	}
	const backend = readFlag(argv, 'backend', 'redis');
	if (backend !== 'redis' && backend !== 'cluster') {
		console.error(`--backend must be redis or cluster, got "${backend}"`);
		process.exit(2);
	}
	const outDir = readFlag(argv, 'out', DEFAULT_OUT);

	console.log(`[leak-lane] ${backend}: warmup ${WARMUP_MS / 1000}s, collect, resettle ${RESETTLE_MS / 1000}s unsampled, ` +
		`settle until steady, then a ${minutes} minute window`);
	const started = Date.now();
	const teardown = await provision(backend);
	let exit = 0;
	let record;
	try {
		record = await runLane({ minutes, backend });
		exit = report(record);
	} catch (err) {
		console.error(`[leak-lane] HEALTH - ${err.message}`);
		record = { backend, minutes, health: [err.message] };
		exit = 2;
	} finally {
		await teardown();
	}
	console.log(`[leak-lane] finished in ${((Date.now() - started) / 1000).toFixed(0)}s`);

	try {
		mkdirSync(outDir, { recursive: true });
		const file = path.join(outDir, `leak-lane-${backend}-${process.pid}.json`);
		writeFileSync(file, JSON.stringify({ node: process.version, platform: process.platform, record }, null, '\t'));
		console.log(`[leak-lane] record written to ${file}`);
	} catch (err) {
		console.error(`[leak-lane] could not write the record: ${err.message}`);
	}
	process.exit(exit);
}

// Importing this module for its rules must not start anything.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	main().catch((err) => {
		console.error(`[leak-lane] ${err.stack || err.message}`);
		process.exit(2);
	});
}
