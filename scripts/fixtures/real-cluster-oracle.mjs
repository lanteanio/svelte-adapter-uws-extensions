// Scenario half of scripts/real-cluster-oracle.js. It runs inside a throwaway
// consumer that installed the packed packages, against a real Redis or Valkey
// Cluster named by ORACLE_CLUSTER_NODES, and prints one JSON report on stdout.
// It provisions nothing and never falls back to an in-memory store.
//
// Run it in a dedicated process: realtime's setBus() is process-global. The
// file must sit in the consumer's root: every package has to load from the
// node_modules next to it.
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import Module from 'node:module';
import { dirname, isAbsolute, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const DEADLINE_MS = 60000;
const SINGLES = 6;
const BATCH = 6;
// How long every socket stays watched after the last barrier arrived, so a
// late duplicate still counts against the exact totals.
const DRAIN_MS = 500;
const CONSUMER = dirname(fileURLToPath(import.meta.url));

// Every import and require of every loaded package must resolve inside the
// consumer. Node would otherwise climb into any node_modules above it (a
// checkout's own, for instance), and a missing transitive dependency would be
// served from there unnoticed. A refusal carries MODULE_NOT_FOUND, so an
// optional require() treats the module as absent.
const refused = [];
const confined = typeof Module.registerHooks === 'function';
// The hooks reach neither worker threads nor child processes, so the packages
// may start neither: every way to start one throws, is recorded, and fails the
// run in cleanup even when a package catches the error.
const blocked = [];
const requireBuiltin = Module.createRequire(import.meta.url);
function forbid(moduleName, names) {
	const api = requireBuiltin(moduleName);
	for (const name of names) {
		if (typeof api[name] !== 'function') continue;
		api[name] = function blockedStart() {
			blocked.push(moduleName + '.' + name);
			throw new Error('The scenario refuses ' + moduleName + '.' + name + ': module resolution cannot be confined outside its own thread');
		};
	}
}
forbid('node:child_process', ['spawn', 'spawnSync', 'exec', 'execSync', 'execFile', 'execFileSync', 'fork']);
forbid('node:worker_threads', ['Worker']);
Module.syncBuiltinESMExports();
if (confined) {
	Module.registerHooks({
		resolve(specifier, context, nextResolve) {
			const resolved = nextResolve(specifier, context);
			if (resolved.url?.startsWith('file:')) {
				const path = fileURLToPath(resolved.url);
				const where = relative(CONSUMER, path);
				if (where === '..' || where.startsWith('..' + sep) || isAbsolute(where)) {
					refused.push({ specifier, url: resolved.url });
					const error = new Error(specifier + ' resolved outside the consumer node_modules: ' + path);
					error.code = 'MODULE_NOT_FOUND';
					throw error;
				}
			}
			return resolved;
		}
	});
}

// Two client pairs, each on its own topic. Plain clients send no hello, so
// publishBatched reaches them through the adapter's per-member fallback.
// Batch-capable clients announce the 'batch' capability, so the adapter sends
// them shared {type:'batch'} frames. Mixing both on one topic would push every
// subscriber onto the fallback.
const PAIRS = [
	{ name: 'plain', topic: 'cluster-oracle', caps: null },
	{ name: 'batch-capable', topic: 'cluster-oracle-batch-capable', caps: ['batch'] }
];

const hash = (value) => createHash('sha256').update(value).digest('hex');
const cases = ['setup', 'cluster-topology', 'single-fanout', 'batch-fanout', 'cleanup'].map((name) => ({ name, status: 'not_run' }));
const report = {
	schemaVersion: 1,
	oracle: 'real-cluster-fanout',
	startedAt: new Date().toISOString(),
	seed: null,
	faultMode: 'none',
	provenance: { node: process.version, platform: process.platform + '-' + process.arch, scenarioSha256: hash(readFileSync(fileURLToPath(import.meta.url))), packages: {}, resolution: { confined, refused, blocked } },
	backend: null,
	cases
};

let emitted = false;
function emit() {
	if (emitted) return;
	emitted = true;
	report.finishedAt = new Date().toISOString();
	const count = (status) => cases.filter((c) => c.status === status).length;
	report.counts = {
		planned: cases.length,
		executed: count('passed') + count('failed'),
		passed: count('passed'),
		failed: count('failed'),
		notRun: cases.length - count('passed') - count('failed')
	};
	report.status = report.counts.passed === cases.length ? 'passed' : 'failed';
	process.stdout.write(JSON.stringify(report) + '\n');
	process.exitCode = report.status === 'passed' ? 0 : 1;
}

function fail(record, error) {
	record.status = 'failed';
	record.error = { name: error?.name || 'Error', message: String(error?.message || error).slice(0, 2000) };
}

async function runCase(name, run) {
	const record = cases.find((c) => c.name === name);
	record.status = 'running';
	const started = Date.now();
	try {
		await run(record);
		record.status = 'passed';
	} catch (error) {
		fail(record, error);
		throw error;
	} finally {
		record.durationMs = Date.now() - started;
	}
}

const watchdog = setTimeout(() => {
	const active = cases.find((record) => record.status === 'running');
	if (active) fail(active, new Error('Scenario exceeded its ' + DEADLINE_MS / 1000 + '-second deadline'));
	emit();
	process.exit(1);
}, DEADLINE_MS);

// Walk up from the resolved entry file to the package root that owns it, and
// refuse a package that resolved from outside the consumer's own node_modules
// (a parent directory's install would otherwise stand in for it silently).
function identity(specifier, expectedName) {
	const entry = fileURLToPath(import.meta.resolve(specifier));
	let root = dirname(entry);
	let found = null;
	while (!found) {
		try {
			const bytes = readFileSync(join(root, 'package.json'));
			const pkg = JSON.parse(bytes);
			if (pkg.name === expectedName) found = { pkg, bytes };
		} catch { /* the entry may sit below its package root */ }
		if (found) break;
		const parent = dirname(root);
		if (parent === root) throw new Error('Cannot locate the package root of ' + expectedName);
		root = parent;
	}
	const where = relative(CONSUMER, root);
	if (isAbsolute(where) || !where.startsWith('node_modules' + sep)) {
		throw new Error(expectedName + ' resolved outside the consumer node_modules: ' + root);
	}
	return { version: found.pkg.version, root: where.split(sep).join('/'), entrySha256: hash(readFileSync(entry)), packageJsonSha256: hash(found.bytes) };
}

async function waitFor(read, description) {
	const deadline = Date.now() + 5000;
	while (!read()) {
		if (Date.now() >= deadline) throw new Error('Timed out waiting for ' + description);
		await delay(5);
	}
}

const sockets = [], servers = [], buses = [], connections = [];
const namespace = 'oracle:' + randomUUID() + ':';
let setBus, sourceClient;

try {
	await runCase('setup', async () => {
		assert.ok(confined, 'The scenario needs module.registerHooks (Node 22.15 or later) to keep module resolution inside the consumer');
		const nodes = JSON.parse(process.env.ORACLE_CLUSTER_NODES || 'null');
		assert.ok(Array.isArray(nodes) && nodes.length > 0, 'ORACLE_CLUSTER_NODES must be a JSON array of cluster seed nodes');
		for (const node of nodes) {
			assert.ok(node && typeof node.host === 'string' && Number.isInteger(node.port) && node.port > 0 && node.port <= 65535, 'Every seed node needs a host and an integer port');
		}
		const natMap = JSON.parse(process.env.ORACLE_CLUSTER_NAT_MAP || 'null');
		const [{ default: Redis }, { WebSocket }, { createTestServer }, { createPubSubBus }, realtime] = await Promise.all([
			import('ioredis'),
			import('ws'),
			import('svelte-adapter-uws/testing'),
			import('svelte-adapter-uws-extensions/redis/pubsub'),
			import('svelte-realtime/server')
		]);
		for (const [specifier, name] of [
			['svelte-adapter-uws/testing', 'svelte-adapter-uws'],
			['svelte-adapter-uws-extensions/redis/pubsub', 'svelte-adapter-uws-extensions'],
			['svelte-realtime/server', 'svelte-realtime'],
			['ioredis', 'ioredis'],
			['ws', 'ws'],
			['uWebSockets.js', 'uWebSockets.js']
		]) report.provenance.packages[name] = identity(specifier, name);

		const auth = {
			...(process.env.ORACLE_REDIS_USERNAME ? { username: process.env.ORACLE_REDIS_USERNAME } : {}),
			...(process.env.ORACLE_REDIS_PASSWORD ? { password: process.env.ORACLE_REDIS_PASSWORD } : {})
		};
		const clusterOptions = {
			...(natMap ? { natMap } : {}),
			enableReadyCheck: true,
			maxRedirections: 16,
			slotsRefreshTimeout: 5000,
			clusterRetryStrategy: (times) => (times < 3 ? 100 : null),
			redisOptions: { commandTimeout: 5000, connectTimeout: 5000, maxRetriesPerRequest: 1, ...auth }
		};
		const open = (overrides = {}) => {
			const connection = new Redis.Cluster(nodes, { ...clusterOptions, ...overrides });
			connection.on('error', () => {});
			connections.push(connection);
			return connection;
		};
		// The RedisClient shape the extensions accept, backed by a Cluster.
		const client = () => {
			const redis = open({ lazyConnect: true });
			return { redis, keyPrefix: namespace, key: (key) => namespace + key, duplicate: (overrides) => open(overrides), async quit() { redis.disconnect(); } };
		};
		sourceClient = client();
		const targetClient = client();
		await Promise.all([sourceClient.redis.connect(), targetClient.redis.connect()]);

		const sender = createPubSubBus(sourceClient, { channel: namespace + 'events', systemChannel: false });
		const receiver = createPubSubBus(targetClient, { channel: namespace + 'events', systemChannel: false });
		buses.push(sender, receiver);
		setBus = realtime.setBus;
		setBus(sender);

		const topics = new Set(PAIRS.map((pair) => pair.topic));
		const subscribe = (_ws, name) => (topics.has(name) ? null : 'FORBIDDEN');
		const source = await createTestServer({ handler: {
			subscribe,
			message: realtime.createMessage({ onJsonMessage(_ws, message, platform) {
				if (!topics.has(message.topic)) return;
				if (message.type === 'oracle-single') platform.publish(message.topic, 'changed', message.data);
				else if (message.type === 'oracle-batch') platform.publishBatched(message.data.map((data) => ({ topic: message.topic, event: 'changed', data })));
			} })
		} });
		servers.push(source);
		const target = await createTestServer({ handler: { subscribe } });
		servers.push(target);
		await sender.activate(source.platform);
		await receiver.activate(target.platform);

		async function connect(server, pair, side) {
			const socket = new WebSocket(server.wsUrl);
			sockets.push(socket);
			// Control frames (acknowledgements and the like) and deliveries, which
			// are the frames that carry an event.
			const messages = [];
			let batchFrames = 0;
			let failure = null;
			socket.on('error', (error) => { failure = error; });
			socket.on('message', (bytes, binary) => {
				try {
					assert.equal(binary, false, 'The scenario expects JSON frames');
					const frame = JSON.parse(bytes.toString());
					if (frame.type === 'batch') {
						assert.ok(Array.isArray(frame.events), 'A batch frame carries an events array');
						batchFrames++;
						messages.push(...frame.events);
					} else messages.push(frame);
				} catch (error) { failure = error; }
			});
			const check = () => { if (failure) throw failure; };
			await waitFor(() => { check(); return socket.readyState === WebSocket.OPEN; }, 'the WebSocket to open');
			if (pair.caps) socket.send(JSON.stringify({ type: 'hello', caps: pair.caps }));
			socket.send(JSON.stringify({ type: 'subscribe', topic: pair.topic, ref: 'ready' }));
			await waitFor(() => { check(); return messages.some((m) => m.type === 'subscribed' && m.ref === 'ready'); }, 'the subscription acknowledgement');
			return {
				send(message) { check(); socket.send(JSON.stringify(message)); },
				// Every delivery counts: one for another topic or event fails the
				// case instead of being filtered out.
				values() {
					check();
					const deliveries = messages.filter((m) => m && typeof m === 'object' && 'event' in m);
					const stray = deliveries.filter((m) => m.topic !== pair.topic || m.event !== 'changed');
					if (stray.length) throw new Error('The ' + side + ' ' + pair.name + ' socket received ' + stray.length + ' frame(s) for another topic or event, first: ' + JSON.stringify(stray[0]).slice(0, 200));
					return deliveries.map((m) => m.data);
				},
				batchFrames: () => batchFrames
			};
		}
		for (const pair of PAIRS) {
			pair.expected = [];
			pair.local = await connect(source, pair, 'local');
			pair.remote = await connect(target, pair, 'remote');
		}
	});

	await runCase('cluster-topology', async () => {
		const masters = sourceClient.redis.nodes('master');
		assert.ok(masters.length >= 3, 'A real cluster with at least three masters is required, found ' + masters.length);
		const info = await sourceClient.redis.cluster('INFO');
		assert.match(info, /cluster_state:ok/, 'cluster_state must be ok');
		assert.match(info, /cluster_slots_ok:16384/, 'every slot must be served');
		const servers = await Promise.all(masters.map(async (node) => {
			const text = await node.info('server');
			return {
				engine: /valkey_version:/.test(text) ? 'valkey' : 'redis',
				version: /(?:valkey_version|redis_version):([^\r\n]+)/.exec(text)?.[1] || null
			};
		}));
		report.backend = { kind: 'real-cluster', masters: masters.length, replicas: sourceClient.redis.nodes('slave').length, slots: 16384, servers };
	});

	async function assertDelivered() {
		for (const pair of PAIRS) {
			const { local, remote, expected } = pair;
			await waitFor(() => local.values().length >= expected.length && remote.values().length >= expected.length, 'ordered local and remote delivery to ' + pair.name + ' clients');
			assert.deepEqual(local.values(), expected, 'Local ' + pair.name + ' values, order or count differ');
			assert.deepEqual(remote.values(), expected, 'Remote ' + pair.name + ' values, order or count differ');
		}
	}

	await runCase('single-fanout', async () => {
		for (const pair of PAIRS) {
			for (let index = 0; index < SINGLES; index++) {
				const data = { index, nested: { text: 'single-' + index } };
				pair.expected.push(data);
				pair.local.send({ type: 'oracle-single', topic: pair.topic, data });
			}
		}
		await assertDelivered();
	});

	await runCase('batch-fanout', async () => {
		for (const pair of PAIRS) {
			const data = Array.from({ length: BATCH }, (_, offset) => ({ index: pair.expected.length + offset, nested: { text: 'batch-' + offset } }));
			pair.expected.push(...data);
			pair.local.send({ type: 'oracle-batch', topic: pair.topic, data });
		}
		await assertDelivered();
		// One more ordered message per topic is the barrier, followed by a drain
		// window. On the remote socket the barrier travels the same Redis relay
		// path as every earlier message, so a remote duplicate arrives before
		// it. On the local socket the barrier is delivered directly, so an echo
		// of an earlier member through the bus could still arrive after it:
		// the window starts once the barrier has crossed the relay to the
		// remote server and keeps every socket watched for DRAIN_MS more, and
		// the totals are compared again after it.
		for (const pair of PAIRS) {
			const barrier = { index: pair.expected.length, barrier: true };
			pair.expected.push(barrier);
			pair.local.send({ type: 'oracle-single', topic: pair.topic, data: barrier });
		}
		await assertDelivered();
		await delay(DRAIN_MS);
		await assertDelivered();
		// Shared batch frames go only to clients that announced the capability,
		// on both sides of the relay.
		for (const pair of PAIRS) {
			for (const side of ['local', 'remote']) {
				const frames = pair[side].batchFrames();
				if (pair.caps) assert.ok(frames > 0, 'The ' + side + ' batch-capable client received no batch frame, so the shared-frame path was not exercised');
				else assert.equal(frames, 0, 'The ' + side + ' plain client received ' + frames + ' batch frame(s) without announcing the capability');
			}
		}
		report.deliveries = Object.fromEntries(PAIRS.map((pair) => [pair.name, {
			expectedPerSocket: pair.expected.length,
			local: pair.local.values().length,
			remote: pair.remote.values().length,
			localBatchFrames: pair.local.batchFrames(),
			remoteBatchFrames: pair.remote.batchFrames()
		}]));
	});
} catch {
	// The failing case holds the verdict; the cases after it stay not_run.
} finally {
	try {
		await runCase('cleanup', async () => {
			setBus?.(null);
			for (const socket of sockets) socket.terminate();
			const results = await Promise.allSettled([...buses.map((bus) => bus.deactivate()), ...servers.map((server) => server.close())]);
			for (const connection of connections) connection.disconnect();
			const rejected = results.find((result) => result.status === 'rejected');
			if (rejected) throw rejected.reason;
			assert.deepEqual(blocked, [], 'A loaded package tried to start a worker thread or child process');
		});
	} catch {
		// A cleanup failure fails the report too.
	}
	clearTimeout(watchdog);
	emit();
}
