// Deterministic simulation backends for the redis/postgres extensions.
//
// The adapter's `svelte-adapter-uws/sim` drives the createTestServer wire
// dispatch over an in-memory app under a virtual clock + seeded fault engine.
// These runners extend that to a redis/postgres-BACKED deployment: N in-memory
// server instances share ONE virtual clock and ONE shared store double, and the
// cross-instance relay flows through the real plugin code (the pub/sub bus, the
// replay ring, presence, LISTEN/NOTIFY) against the mock client - so a seed
// reproduces a multi-instance store-backed run bit-for-bit.
//
// The load-bearing wiring: there are TWO runtime seams - the adapter's
// (`svelte-adapter-uws/files/runtime.js`, read by the dispatch) and this
// package's (`shared/runtime.js`, read by the mocks + plugins). They are
// distinct module instances, so the same scheduler env is installed on BOTH,
// backed by ONE scheduler heap; otherwise the relay timer arms on a heap the
// run loop never drains and inbound relay never fires.
//
// Public subpath: `svelte-adapter-uws-extensions/sim`.

import {
	createScheduler, createSeededRng, createFaultEngine, createInMemoryApp,
	createInMemoryUwsHelpers, setRuntimeEnv as adapterSetRuntimeEnv,
	resetRuntimeEnv as adapterResetRuntimeEnv, resetProcessEpoch, FIXED_EPOCH
} from 'svelte-adapter-uws/sim';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { setRuntimeEnv as extSetRuntimeEnv, resetRuntimeEnv as extResetRuntimeEnv } from './shared/runtime.js';
import { checkSubscriptionBookkeeping, checkRedisReplaySeqRegression, checkSharedStoreConvergence } from './shared/invariants.js';
import { mockRedisClient } from './testing/mock-redis.js';
import { mockPgClient } from './testing/mock-pg.js';
import { createPubSubBus } from './redis/pubsub.js';
import { createReplay as createRedisReplay } from './redis/replay.js';
import { createPresence as createRedisPresence } from './redis/presence.js';
import { createNotifyBridge } from './postgres/notify.js';
import { createReplay as createPgReplay } from './postgres/replay.js';

export { mockRedisClient, mockPgClient };

/** Default seed so the zero-config run is itself reproducible. */
export const DEFAULT_REDIS_SEED = 'svti-redis-sim-0';
export const DEFAULT_PG_SEED = 'svti-pg-sim-0';

/**
 * Install the SAME scheduler env on both seams: the adapter seam (read by the
 * dispatch) carries the dispatch rng; this package's seam (read by the mocks +
 * plugins) carries a derived store rng, so a fault-config change never perturbs
 * the dispatch uuid stream. Both share the one scheduler heap. The store seam's
 * `bytes` is Buffer-wrapped because plugin code calls `randomBytes(n).toString('hex')`
 * (the adapter rng yields a Uint8Array, whose hex toString is malformed).
 *
 * @param {ReturnType<typeof createScheduler>} scheduler
 * @param {ReturnType<typeof createSeededRng>} dispatchRng
 * @param {ReturnType<typeof createSeededRng>} storeRng
 */
function installBothSeams(scheduler, dispatchRng, storeRng) {
	adapterSetRuntimeEnv(scheduler.buildEnv(dispatchRng), { force: true });
	const base = scheduler.buildEnv(storeRng);
	const storeEnv = { ...base, rng: { ...base.rng, bytes: (n) => Buffer.from(base.rng.bytes(n)) } };
	extSetRuntimeEnv(storeEnv, { force: true });
	resetProcessEpoch();
}

/** Tear both seams down (once per run). */
function resetBothSeams() {
	adapterResetRuntimeEnv();
	extResetRuntimeEnv();
	resetProcessEpoch();
}

/**
 * Fill the few production Platform helpers `createTestServer` does not implement
 * (`sendCoalesced` / `onPressure` / `onPublishRate`), so a plugin's `bus.wrap`
 * (which binds the full Platform surface) can wrap it. None are on the
 * cross-instance relay path: the sim exercises neither coalesced sends nor the
 * live pressure / publish-rate samplers, so the defaults are faithful. (A future
 * adapter patch can add these to createTestServer's platform for full parity.)
 * @param {any} platform
 */
function completePlatform(platform) {
	if (typeof platform.sendCoalesced === 'function'
		&& typeof platform.onPressure === 'function'
		&& typeof platform.onPublishRate === 'function') return platform;
	return Object.assign(Object.create(platform), {
		sendCoalesced: typeof platform.sendCoalesced === 'function'
			? platform.sendCoalesced.bind(platform)
			: (ws, topic, event, data, options) => platform.send(ws, topic, event, data, options),
		onPressure: typeof platform.onPressure === 'function' ? platform.onPressure.bind(platform) : () => () => {},
		onPublishRate: typeof platform.onPublishRate === 'function' ? platform.onPublishRate.bind(platform) : () => () => {}
	});
}

/**
 * Construct the requested stateful redis plugins against the ONE shared client
 * and compose their createTestServer handler hooks. `replay` adds a `resume`
 * hook (gap-fill from the shared ring); `presence` ships subscribe/unsubscribe/
 * close/message hooks. Every plugin reads/writes the shared store, so a publish/join on
 * one instance is visible to a resume/roster read on another. Plugin hooks take
 * precedence over a user-supplied `config.handler` for the same key.
 *
 * @param {any} client the shared mockRedisClient
 * @param {string[]} names e.g. ['replay', 'presence']
 * @param {Record<string, any>} opts per-plugin options keyed by name
 * @param {object} userHandler config.handler
 */
function buildRedisInstance(client, names, opts, userHandler) {
	const plugins = {};
	const handler = Object.assign({}, userHandler);
	if (names.includes('replay')) {
		plugins.replay = createRedisReplay(client, opts.replay || { size: 1000 });
		handler.resume = plugins.replay.resumeHook();
	}
	if (names.includes('presence')) {
		plugins.presence = createRedisPresence(client, opts.presence || { key: 'id', select: (ud) => ud, heartbeat: 60000, ttl: 180 });
		const ph = plugins.presence.hooks;
		if (ph.subscribe) handler.subscribe = ph.subscribe;
		if (ph.unsubscribe) handler.unsubscribe = ph.unsubscribe;
		if (ph.close) handler.close = ph.close;
		if (ph.message) handler.message = ph.message;
	}
	return { plugins, handler };
}

// The per-connection subscription registry slot the adapter dispatch stamps.
// `Symbol.for` so this resolves the SAME registry across the bundled module
// instances (the cross-package slot-key convention).
const WS_SUBSCRIPTIONS = Symbol.for('adapter-uws.ws.subscriptions');

/**
 * Build the structure-only invariant snapshot for one instance from its live
 * in-memory connections: per-connection subscribed set (the fan-out set) and
 * cap-counted bookkeeping set, so `checkSubscriptionBookkeeping` reads them.
 * Carries NO payload or user data. `bookkeeping` is null when the registry slot
 * is not a Set, so the shape check fires identically to production.
 * @param {any} server the createTestServer result
 * @returns {{ connections: Array<{ id: unknown, subscribed: string[], bookkeeping: string[] | null }> }}
 */
function instanceInvariantSnapshot(server) {
	const connections = [];
	for (const ws of server.wsConnections) {
		let subscribed = [];
		try { subscribed = ws.getTopics(); } catch { subscribed = []; }
		let bookkeeping = null;
		try {
			const subs = ws.getUserData()[WS_SUBSCRIPTIONS];
			if (subs instanceof Set) bookkeeping = [...subs];
		} catch { bookkeeping = null; }
		connections.push({ id: ws._simId, subscribed, bookkeeping });
	}
	return { connections };
}

/**
 * Project one instance's delivered per-topic max seq from the decoded frames its
 * clients received. The convergent observable is the monotonic the originator
 * stamps into the envelope body: the pub/sub scenario stamps `data.n` and the
 * replay scenario stamps `data.seq`, so the projection reads whichever numeric
 * monotonic the body carries (seq preferred), grouped on the UNcorrupted routing
 * topic (a corrupt fault mangles the body but never the routing key, so it cannot
 * move the projection; a body that fails to decode or carries no numeric monotonic
 * simply does not advance its topic's max). Structure only - the seq integer and
 * the topic string, never the payload data.
 *
 * @param {Array<{ raw: Array<{ routingTopic?: string | null }>, decoded: any[] }>} clientFrames
 * @returns {Record<string, number>} topic -> highest delivered monotonic
 */
function deliveredTopicSeqs(clientFrames) {
	/** @type {Record<string, number>} */
	const topicSeqs = {};
	for (const c of clientFrames) {
		const raw = c.raw || [];
		const decoded = c.decoded || [];
		for (let i = 0; i < raw.length; i++) {
			const f = raw[i];
			if (!f || f.routingTopic == null) continue; // only a topic publish carries a routing key
			const body = decoded[i];
			if (!body) continue;
			const data = body.data;
			let seq = null;
			if (data && typeof data === 'object') {
				if (typeof data.seq === 'number') seq = data.seq;
				else if (typeof data.n === 'number') seq = data.n;
			}
			if (seq == null) continue;
			const t = f.routingTopic;
			if (!(t in topicSeqs) || seq > topicSeqs[t]) topicSeqs[t] = seq;
		}
	}
	return topicSeqs;
}

/**
 * Per-instance replay seq-ordering check. For each instance running the replay
 * plugin, read the shared ring head per topic (through the live plugin, which
 * queries the shared store) and the highest seq that instance actually delivered
 * on each `__replay:{topic}` channel, then feed the pair to the seq-regression
 * predicate. The delivered seq comes from the instance's OWN frames and the head
 * from the SHARED store, so the comparison crosses the local view against the
 * shared authority - never a same-source tautology. Records a violation through
 * the supplied sink. No-op for an instance without a replay plugin.
 *
 * @param {Array<{ id: number, plugins: any }>} instancesArr
 * @param {Array<{ instanceId: number, facade: any }>} allClients
 * @param {(v: { category: string, context: any } | null) => void} record
 */
async function collectReplaySeqRegression(instancesArr, allClients, record) {
	for (const inst of instancesArr) {
		const replay = inst.plugins && inst.plugins.replay;
		if (!replay || typeof replay.seq !== 'function') continue;

		// Highest seq this instance delivered per replay topic, parsed from the
		// `__replay:{topic}` 'msg' frames its clients received. Structure only -
		// the seq integer and the derived topic, never the payload data.
		/** @type {Map<string, number>} */
		const deliveredByTopic = new Map();
		for (const c of allClients) {
			if (c.instanceId !== inst.id) continue;
			for (const body of c.facade.json()) {
				if (!body || body.event !== 'msg' || typeof body.topic !== 'string') continue;
				if (!body.topic.startsWith('__replay:')) continue;
				const data = body.data;
				if (!data || typeof data.seq !== 'number') continue;
				const topic = body.topic.slice('__replay:'.length);
				const prev = deliveredByTopic.get(topic);
				if (prev === undefined || data.seq > prev) deliveredByTopic.set(topic, data.seq);
			}
		}
		if (deliveredByTopic.size === 0) continue;

		const replaySeqs = [];
		for (const [topic, deliveredSeq] of deliveredByTopic) {
			const ringHeadSeq = await replay.seq(topic);
			replaySeqs.push({ topic, deliveredSeq, ringHeadSeq });
		}
		// Sort for a deterministic, byte-stable evaluation order across runs.
		replaySeqs.sort((a, b) => (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
		record(checkRedisReplaySeqRegression({ replaySeqs }));
	}
}

/**
 * A deterministic, sorted snapshot of one instance's structural state: per-topic
 * subscriber counts. Sorted so two runs of a seed produce byte-identical output.
 * @param {any} server the createTestServer result
 */
function instanceSnapshot(server) {
	/** @type {Record<string, number>} */
	const topicCounts = {};
	let open = 0;
	for (const ws of server.wsConnections) {
		open++;
		let topics = [];
		try { topics = ws.getTopics(); } catch { topics = []; }
		for (const t of topics) topicCounts[t] = (topicCounts[t] || 0) + 1;
	}
	const sorted = {};
	for (const t of Object.keys(topicCounts).sort()) sorted[t] = topicCounts[t];
	return { topicCounts: sorted, openConnections: open };
}

/**
 * The default redis scenario: connect `clients` clients on every instance,
 * subscribe each to every topic, then publish a few events per topic FROM
 * instance 0, so the pub/sub relay carries them cross-instance.
 */
async function defaultRedisScenario(api, opts) {
	for (let i = 0; i < opts.instances; i++) {
		for (let c = 0; c < opts.clients; c++) api.instance(i).connect();
	}
	await api.advance();
	for (let i = 0; i < opts.instances; i++) {
		for (const c of api.instance(i).clients()) for (const t of opts.topics) c.subscribe(t);
	}
	await api.advance();
	for (const t of opts.topics) for (let n = 0; n < 3; n++) api.instance(0).publish(t, 'tick', { n });
	await api.advance();
}

/**
 * Run one redis-backed simulation. N in-memory instances share ONE shared
 * `mockRedisClient` (so their `duplicate()` subscribers connect) and ONE virtual
 * clock; cross-instance delivery flows through the real `createPubSubBus`
 * wrap/activate against the mock, fault-gated by `relayFaults`.
 *
 * @param {{
 *   seed?: string, instances?: number, clients?: number, topics?: string[],
 *   steps?: number, startEpoch?: number, tz?: string, channel?: string,
 *   faults?: object, relayFaults?: object, handler?: object,
 *   scenario?: (api: any, opts: { instances: number, clients: number, topics: string[] }) => void | Promise<void>,
 *   gitCommit?: string
 * }} [config]
 */
export async function runRedisSim(config = {}) {
	const seed = config.seed ?? DEFAULT_REDIS_SEED;
	const instances = config.instances ?? 2;
	const clients = config.clients ?? 1;
	const topics = config.topics ?? ['room'];
	const maxSteps = config.steps ?? 100000;
	const startEpoch = config.startEpoch ?? FIXED_EPOCH;
	const channel = config.channel || 'uws:sim';

	const dispatchRng = createSeededRng(seed);
	const storeRng = createSeededRng(seed + ':redis');
	const scheduler = createScheduler({ startEpoch, tz: config.tz });
	const relayFaultEngine = createFaultEngine({ rng: createSeededRng(seed + ':relay'), faults: config.relayFaults || {} });

	try {
		// Inside the try so a mid-install throw (e.g. between the two seam installs)
		// is still torn down by the finally - the adapter seam is a worker-global,
		// and a half-install would freeze the clock/RNG for every later test.
		installBothSeams(scheduler, dispatchRng, storeRng);
		const sharedClient = mockRedisClient('', { faultEngine: relayFaultEngine });
		/** @type {Array<{ id: number, app: any, server: any, bus: any, wrapped: any, plugins: any, clients: any[] }>} */
		const instancesArr = [];
		/** @type {Array<{ instanceId: number, facade: any, subTopics: Set<string> }>} */
		const allClients = [];
		let totalSteps = 0;

		const pluginNames = config.plugins || [];
		for (let i = 0; i < instances; i++) {
			const wsFaultEngine = createFaultEngine({ rng: createSeededRng(seed + ':ws:' + i), faults: config.faults || {} });
			const app = createInMemoryApp({ scheduler, faultEngine: wsFaultEngine });
			const uws = createInMemoryUwsHelpers(app);
			const { plugins, handler } = buildRedisInstance(sharedClient, pluginNames, config.pluginOptions || {}, config.handler || {});
			const server = await createTestServer({ handler, __app: app, __uws: uws });
			// Route the subscribe-ack epoch + replay-discovery through the replay
			// tracker so a resume that presents a matching epoch gap-fills (rather
			// than unconditionally cold-rehydrating), mirroring a real deployment.
			if (plugins.replay) {
				server.platform.replay = plugins.replay;
				server.platform.topicEpoch = (t) => plugins.replay.cachedEpoch(t);
			}
			const platform = completePlatform(server.platform);
			const bus = createPubSubBus(sharedClient, { channel, systemChannel: false });
			const wrapped = bus.wrap(platform);
			await bus.activate(platform);
			instancesArr.push({ id: i, app, server, bus, wrapped, plugins, clients: [] });
		}

		/** @type {Array<{ category: string, context: any }>} */
		const violations = [];
		const seen = new Set();
		function recordViolation(v) {
			if (!v) return;
			const key = v.category + ':' + JSON.stringify(v.context);
			if (!seen.has(key)) { seen.add(key); violations.push(v); }
		}
		// Per-step self-consistency: each instance's fan-out subscription set must
		// agree with its cap-counted bookkeeping set. Run after every scheduler step
		// so the earliest interleaving that breaks it is the one recorded.
		function checkInvariants() {
			for (const inst of instancesArr) {
				recordViolation(checkSubscriptionBookkeeping(instanceInvariantSnapshot(inst.server)));
			}
		}

		const api = {
			now: () => scheduler.now(),
			instances,
			instance(i) {
				const inst = instancesArr[i];
				return {
					connect: (o) => {
						const facade = inst.app.connect(o);
						const subTopics = new Set();
						const origSub = facade.subscribe.bind(facade);
						facade.subscribe = (topic, ref) => { subTopics.add(topic); return origSub(topic, ref); };
						inst.clients.push(facade);
						allClients.push({ instanceId: i, facade, subTopics });
						return facade;
					},
					clients: () => inst.clients.slice(),
					publish: (t, e, d, o) => inst.wrapped.publish(t, e, d, o),
					publishBatched: (m) => inst.wrapped.publishBatched(m),
					// The bus-wrapped platform (publishing through it relays cross-instance)
					// and the stateful plugins constructed against the shared store, so a
					// scenario can drive replay.publish / presence directly.
					platform: inst.wrapped,
					rawPlatform: inst.server.platform,
					replay: inst.plugins.replay,
					presence: inst.plugins.presence
				};
			},
			async advance(rounds) {
				totalSteps += await scheduler.run({ maxSteps: rounds ?? maxSteps, onStep: checkInvariants });
			}
		};

		const scenario = config.scenario || defaultRedisScenario;
		await scenario(api, { instances, clients, topics });
		totalSteps += await scheduler.run({ maxSteps, onStep: checkInvariants });
		checkInvariants();

		// Quiescent cross-instance convergence: every instance that subscribed to a
		// topic backed by the shared store should have received the same delivered
		// seq run for it, so their per-topic delivered-seq projections hash
		// identically. A relay drop/dup/reorder that shorts one instance moves only
		// that instance's projection - a real, seed-reproducible signal, not a
		// tautology over the one shared store object.
		const redisProjections = instancesArr.map((inst) => ({
			id: inst.id,
			topicSeqs: deliveredTopicSeqs(
				allClients.filter((c) => c.instanceId === inst.id)
					.map((c) => ({ raw: c.facade.frames(), decoded: c.facade.json() }))
			)
		}));
		recordViolation(checkSharedStoreConvergence(redisProjections));

		// Quiescent replay seq-ordering: no instance may have delivered a replay-ring
		// seq past the shared ring head. Read the ring head per topic from the shared
		// store (through the live plugin, before teardown) and compare against the max
		// seq the instance delivered on each `__replay:{topic}` channel.
		await collectReplaySeqRegression(instancesArr, allClients, recordViolation);

		const clusterFrames = instancesArr.map((inst) => ({
			instance: inst.id,
			clients: allClients.filter((c) => c.instanceId === inst.id).map((c) => c.facade.json())
		}));
		const finalState = instancesArr.map((inst) => ({ instance: inst.id, ...instanceSnapshot(inst.server) }));
		const totalFrames = allClients.reduce((s, c) => s + c.facade.frames().length, 0);

		for (const inst of instancesArr) {
			try { inst.plugins.presence && inst.plugins.presence.destroy && inst.plugins.presence.destroy(); } catch { /* best effort */ }
			try { inst.plugins.replay && inst.plugins.replay.destroy && inst.plugins.replay.destroy(); } catch { /* best effort */ }
			try { await inst.bus.deactivate(); } catch { /* best effort */ }
		}
		for (const inst of instancesArr) { try { await inst.server.close(); } catch { /* best effort */ } }
		totalSteps += await scheduler.run({ maxSteps, onStep: checkInvariants });

		return {
			seed,
			gitCommit: config.gitCommit ?? (typeof process !== 'undefined' ? process.env.GIT_COMMIT : null) ?? null,
			config: {
				instances, clients, topics, steps: maxSteps, channel,
				faults: config.faults || {}, relayFaults: config.relayFaults || {},
				tz: config.tz ?? null, startEpoch
			},
			steps: totalSteps,
			virtualTimeMs: scheduler.now() - startEpoch,
			invariantViolations: violations,
			metrics: { instances, clients: allClients.length, framesDelivered: totalFrames },
			clusterFrames,
			finalState,
			_handler: config.handler,
			_scenario: config.scenario,
			_seedConfig: config
		};
	} finally {
		resetBothSeams();
	}
}

/**
 * Re-run a redis reproducer and assert the same outcome. The determinism
 * self-gate: a deterministic run reproduces its per-instance frames, structural
 * state, invariant violations, and metrics exactly.
 * @param {any} reproducer a result from runRedisSim
 */
export async function replayRedisSim(reproducer) {
	const cfg = { ...(reproducer._seedConfig || {}), seed: reproducer.seed, handler: reproducer._handler, scenario: reproducer._scenario, gitCommit: reproducer.gitCommit };
	const result = await runRedisSim(cfg);
	result.reproduced =
		JSON.stringify(result.clusterFrames) === JSON.stringify(reproducer.clusterFrames) &&
		JSON.stringify(result.finalState) === JSON.stringify(reproducer.finalState) &&
		JSON.stringify(result.invariantViolations) === JSON.stringify(reproducer.invariantViolations) &&
		JSON.stringify(result.metrics) === JSON.stringify(reproducer.metrics) &&
		result.virtualTimeMs === reproducer.virtualTimeMs &&
		result.steps === reproducer.steps;
	return result;
}

/**
 * The default postgres scenario: connect `clients` clients on every instance,
 * subscribe each to every topic, then emit a few changes per topic via `pg_notify`
 * so the LISTEN/NOTIFY bridge fans them out to every instance ('all' mode).
 */
async function defaultPgScenario(api, opts) {
	for (let i = 0; i < opts.instances; i++) {
		for (let c = 0; c < opts.clients; c++) api.instance(i).connect();
	}
	await api.advance();
	for (let i = 0; i < opts.instances; i++) {
		for (const c of api.instance(i).clients()) for (const t of opts.topics) c.subscribe(t);
	}
	await api.advance();
	for (const t of opts.topics) for (let n = 0; n < 3; n++) await api.notify(t, 'tick', { n });
	await api.advance();
}

/**
 * Run one postgres-backed simulation. N in-memory instances share ONE
 * `mockPgClient` and ONE virtual clock; cross-instance delivery flows through the
 * real `createNotifyBridge` LISTEN/NOTIFY relay ('all' mode: every instance
 * LISTENs, a `pg_notify` fans to all), fault-gated by `relayFaults`. The runner
 * exposes `api.client` (the shared mock) so a scenario can drive advisory-lock
 * leader election + `FOR UPDATE SKIP LOCKED` claims directly.
 *
 * @param {{
 *   seed?: string, instances?: number, clients?: number, topics?: string[],
 *   steps?: number, startEpoch?: number, tz?: string, channel?: string,
 *   faults?: object, relayFaults?: object, handler?: object,
 *   scenario?: (api: any, opts: { instances: number, clients: number, topics: string[] }) => void | Promise<void>,
 *   gitCommit?: string
 * }} [config]
 */
export async function runPgSim(config = {}) {
	const seed = config.seed ?? DEFAULT_PG_SEED;
	const instances = config.instances ?? 2;
	const clients = config.clients ?? 1;
	const topics = config.topics ?? ['room'];
	const maxSteps = config.steps ?? 100000;
	const startEpoch = config.startEpoch ?? FIXED_EPOCH;
	const channel = config.channel || 'sim_changes';

	const dispatchRng = createSeededRng(seed);
	const storeRng = createSeededRng(seed + ':pg');
	const scheduler = createScheduler({ startEpoch, tz: config.tz });
	const relayFaultEngine = createFaultEngine({ rng: createSeededRng(seed + ':relay'), faults: config.relayFaults || {} });

	try {
		// Inside the try so a mid-install throw (e.g. between the two seam installs)
		// is still torn down by the finally - the adapter seam is a worker-global,
		// and a half-install would freeze the clock/RNG for every later test.
		installBothSeams(scheduler, dispatchRng, storeRng);
		const sharedClient = mockPgClient({ faultEngine: relayFaultEngine });
		/** @type {Array<{ id: number, app: any, server: any, bridge: any, platform: any, plugins: any, clients: any[] }>} */
		const instancesArr = [];
		/** @type {Array<{ instanceId: number, facade: any, subTopics: Set<string> }>} */
		const allClients = [];
		let totalSteps = 0;

		const pluginNames = config.plugins || [];
		for (let i = 0; i < instances; i++) {
			const wsFaultEngine = createFaultEngine({ rng: createSeededRng(seed + ':ws:' + i), faults: config.faults || {} });
			const app = createInMemoryApp({ scheduler, faultEngine: wsFaultEngine });
			const uws = createInMemoryUwsHelpers(app);
			const plugins = {};
			const handler = Object.assign({}, config.handler || {});
			if (pluginNames.includes('replay')) {
				// cleanupInterval:0 disables the periodic trim timer; the durable ring
				// lives in the ONE shared mock-pg, so a publish on instance A is
				// resumable from instance B.
				plugins.replay = createPgReplay(sharedClient, (config.pluginOptions && config.pluginOptions.replay) || { size: 1000, cleanupInterval: 0 });
				handler.resume = plugins.replay.resumeHook();
			}
			const server = await createTestServer({ handler, __app: app, __uws: uws });
			if (plugins.replay) {
				server.platform.replay = plugins.replay;
				server.platform.topicEpoch = (t) => plugins.replay.cachedEpoch(t);
			}
			const platform = completePlatform(server.platform);
			const bridge = createNotifyBridge(sharedClient, { channel, multiListener: 'all', autoReconnect: false });
			await bridge.activate(platform);
			instancesArr.push({ id: i, app, server, bridge, platform, plugins, clients: [] });
		}

		/** @type {Array<{ category: string, context: any }>} */
		const violations = [];
		const seen = new Set();
		function recordViolation(v) {
			if (!v) return;
			const key = v.category + ':' + JSON.stringify(v.context);
			if (!seen.has(key)) { seen.add(key); violations.push(v); }
		}
		// Per-step self-consistency: each instance's fan-out subscription set must
		// agree with its cap-counted bookkeeping set. Run after every scheduler step
		// so the earliest interleaving that breaks it is the one recorded.
		function checkInvariants() {
			for (const inst of instancesArr) {
				recordViolation(checkSubscriptionBookkeeping(instanceInvariantSnapshot(inst.server)));
			}
		}

		const api = {
			now: () => scheduler.now(),
			instances,
			client: sharedClient,
			instance(i) {
				const inst = instancesArr[i];
				return {
					connect: (o) => {
						const facade = inst.app.connect(o);
						const subTopics = new Set();
						const origSub = facade.subscribe.bind(facade);
						facade.subscribe = (topic, ref) => { subTopics.add(topic); return origSub(topic, ref); };
						inst.clients.push(facade);
						allClients.push({ instanceId: i, facade, subTopics });
						return facade;
					},
					clients: () => inst.clients.slice(),
					platform: inst.platform,
					server: inst.server,
					replay: inst.plugins.replay
				};
			},
			/** Emit a change across the cluster via pg_notify (every instance's bridge relays it). */
			async notify(topic, event, data) {
				await sharedClient.query('SELECT pg_notify($1, $2)', [channel, JSON.stringify({ topic, event, data })]);
			},
			async advance(rounds) {
				totalSteps += await scheduler.run({ maxSteps: rounds ?? maxSteps, onStep: checkInvariants });
			},
			async advanceTime(ms) {
				// Match the scheduler's own clamping (Number, no 32-bit truncation), so a
				// large or fractional advance is not silently corrupted into the virtual
				// clock (which the replay gate compares).
				scheduler._scheduleTimer(() => {}, Math.max(0, Math.floor(Number(ms)) || 0), [], false);
				totalSteps += await scheduler.run({ maxSteps, onStep: checkInvariants });
			}
		};

		const scenario = config.scenario || defaultPgScenario;
		await scenario(api, { instances, clients, topics });
		totalSteps += await scheduler.run({ maxSteps, onStep: checkInvariants });
		checkInvariants();

		// Quiescent cross-instance convergence: every instance that subscribed to a
		// topic fed by the shared LISTEN/NOTIFY bridge should have received the same
		// delivered seq run for it, so their per-topic delivered-seq projections hash
		// identically. A NOTIFY drop/dup/reorder that shorts one instance moves only
		// that instance's projection - a real, seed-reproducible signal, not a
		// tautology over the one shared store object.
		const pgProjections = instancesArr.map((inst) => ({
			id: inst.id,
			topicSeqs: deliveredTopicSeqs(
				allClients.filter((c) => c.instanceId === inst.id)
					.map((c) => ({ raw: c.facade.frames(), decoded: c.facade.json() }))
			)
		}));
		recordViolation(checkSharedStoreConvergence(pgProjections));

		// Quiescent replay seq-ordering: no instance may have delivered a replay seq
		// past the shared durable ring head (read through the live plugin before
		// teardown), compared against the max seq delivered on `__replay:{topic}`.
		await collectReplaySeqRegression(instancesArr, allClients, recordViolation);

		const clusterFrames = instancesArr.map((inst) => ({
			instance: inst.id,
			clients: allClients.filter((c) => c.instanceId === inst.id).map((c) => c.facade.json())
		}));
		const finalState = instancesArr.map((inst) => ({ instance: inst.id, ...instanceSnapshot(inst.server) }));
		const totalFrames = allClients.reduce((s, c) => s + c.facade.frames().length, 0);

		for (const inst of instancesArr) {
			try { inst.plugins.replay && inst.plugins.replay.destroy && inst.plugins.replay.destroy(); } catch { /* best effort */ }
			try { await inst.bridge.deactivate(); } catch { /* best effort */ }
		}
		for (const inst of instancesArr) { try { await inst.server.close(); } catch { /* best effort */ } }
		totalSteps += await scheduler.run({ maxSteps, onStep: checkInvariants });

		return {
			seed,
			gitCommit: config.gitCommit ?? (typeof process !== 'undefined' ? process.env.GIT_COMMIT : null) ?? null,
			config: {
				instances, clients, topics, steps: maxSteps, channel,
				faults: config.faults || {}, relayFaults: config.relayFaults || {},
				tz: config.tz ?? null, startEpoch
			},
			steps: totalSteps,
			virtualTimeMs: scheduler.now() - startEpoch,
			invariantViolations: violations,
			metrics: { instances, clients: allClients.length, framesDelivered: totalFrames },
			clusterFrames,
			finalState,
			_handler: config.handler,
			_scenario: config.scenario,
			_seedConfig: config
		};
	} finally {
		resetBothSeams();
	}
}

/**
 * Re-run a pg reproducer and assert the same outcome (the determinism self-gate).
 * @param {any} reproducer a result from runPgSim
 */
export async function replayPgSim(reproducer) {
	const cfg = { ...(reproducer._seedConfig || {}), seed: reproducer.seed, handler: reproducer._handler, scenario: reproducer._scenario, gitCommit: reproducer.gitCommit };
	const result = await runPgSim(cfg);
	result.reproduced =
		JSON.stringify(result.clusterFrames) === JSON.stringify(reproducer.clusterFrames) &&
		JSON.stringify(result.finalState) === JSON.stringify(reproducer.finalState) &&
		JSON.stringify(result.invariantViolations) === JSON.stringify(reproducer.invariantViolations) &&
		JSON.stringify(result.metrics) === JSON.stringify(reproducer.metrics) &&
		result.virtualTimeMs === reproducer.virtualTimeMs &&
		result.steps === reproducer.steps;
	return result;
}
