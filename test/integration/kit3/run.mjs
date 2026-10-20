import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { cp, mkdir, mkdtemp, readFile, readdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { Redis } from 'ioredis';
import pg from 'pg';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const fixture = path.join(root, 'test/fixtures/kit3');
const withMutations = process.argv.includes('--mutations');
const unknown = process.argv.slice(2).filter((arg) => arg !== '--mutations');
if (unknown.length) throw new Error('Unknown arguments: ' + unknown.join(' '));

const npmCommand = process.platform === 'win32' ? process.execPath : 'npm';
const npmPrefix = process.platform === 'win32'
	? [process.env.npm_execpath || path.join(path.dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js')]
	: [];

async function command(executable, args, cwd, env = process.env, timeout = 180000) {
	const child = spawn(executable, args, { cwd, env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
	let output = '';
	child.stdout.on('data', (data) => { output += data; });
	child.stderr.on('data', (data) => { output += data; });
	const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
	try {
		const [code, signal] = await once(child, 'exit');
		if (code !== 0) throw new Error(args.join(' ') + ' exited with ' + (code ?? signal) + '\n' + output);
		return output;
	} finally {
		clearTimeout(timer);
	}
}

const npm = (args, cwd, env) => command(npmCommand, [...npmPrefix, ...args], cwd, env);

async function freePort() {
	const server = createServer();
	server.listen(0, '127.0.0.1');
	await once(server, 'listening');
	const port = server.address().port;
	await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
	return port;
}

async function poll(read, accept, label, timeout = 10000) {
	const deadline = Date.now() + timeout;
	let value;
	while (Date.now() < deadline) {
		value = await read();
		if (accept(value)) return value;
		await delay(30);
	}
	assert.fail(label + ': ' + JSON.stringify(value));
}

function inbox(socket) {
	const frames = [];
	const waiting = new Set();
	let failure;
	function fail(error) {
		failure = error;
		for (const waiter of waiting) waiter.reject(error);
		waiting.clear();
	}
	socket.on('message', (raw) => {
		try {
			const value = JSON.parse(raw.toString());
			for (const waiter of waiting) {
				if (waiter.predicate(value)) {
					waiting.delete(waiter);
					waiter.resolve(value);
					return;
				}
			}
			frames.push(value);
		} catch (error) { fail(error); }
	});
	socket.on('error', fail);
	socket.on('close', (code) => fail(new Error('WebSocket closed with ' + code)));
	const next = async (predicate, label) => {
		const index = frames.findIndex(predicate);
		if (index !== -1) return frames.splice(index, 1)[0];
		if (failure) throw failure;
		let waiter;
		let timer;
		try {
			return await new Promise((resolve, reject) => {
				waiter = { predicate, resolve, reject };
				waiting.add(waiter);
				timer = setTimeout(() => reject(new Error(label + ' timed out')), 5000);
			});
		} finally {
			clearTimeout(timer);
			waiting.delete(waiter);
		}
	};
	// Frames that arrived without a matching wait.
	next.pending = () => [...frames];
	return next;
}

function closeCode(socket, label, timeout = 30000) {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error(label + ' timed out')), timeout);
		socket.once('close', (code) => {
			clearTimeout(timer);
			resolve(code);
		});
	});
}

async function startApp(directory, mode, environment) {
	const port = await freePort();
	const origin = 'http://127.0.0.1:' + port;
	const child = spawn(process.execPath, [mode === 'dev' ? 'dev.mjs' : 'prod.mjs'], {
		cwd: directory,
		env: { ...process.env, ...environment, KIT3_INSTANCE: String(port), HOST: '127.0.0.1', PORT: String(port), ORIGIN: origin },
		stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
		windowsHide: true
	});
	let output = '';
	let spawnError;
	child.on('error', (error) => { spawnError = error; });
	child.stdout.on('data', (data) => { output += data; });
	child.stderr.on('data', (data) => { output += data; });
	const app = {
		origin,
		output: () => output,
		exitCode: () => child.exitCode,
		// Both entry scripts shut down gracefully on an IPC stop message.
		async stop() {
			if (child.exitCode !== null || child.signalCode !== null || spawnError) return;
			const exited = once(child, 'exit');
			if (child.connected) child.send('stop');
			else child.kill('SIGTERM');
			const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
			try { await exited; } finally { clearTimeout(timer); }
		},
		async api(body) {
			const response = await fetch(origin + '/api', {
				method: body ? 'POST' : 'GET',
				headers: body ? { 'content-type': 'application/json', origin } : {},
				body: body ? JSON.stringify(body) : undefined,
				signal: AbortSignal.timeout(10000)
			});
			assert.equal(response.status, 200, 'HTTP API: ' + await response.clone().text());
			return response.json();
		}
	};
	try {
		await poll(async () => {
			if (spawnError) throw spawnError;
			if (child.exitCode !== null || child.signalCode !== null) {
				throw new Error('App exited before readiness\n' + output);
			}
			try { return await fetch(origin, { signal: AbortSignal.timeout(1000) }); }
			catch { return null; }
		}, (response) => response?.ok, 'HTTP readiness\n' + output, 30000);
		return app;
	} catch (error) {
		await app.stop();
		throw new Error(error.message + '\n' + output, { cause: error });
	}
}

const CHECKS = ['bus', 'redis', 'postgres', 'registry'];
const alice = { userId: 'alice', preference: 'blue' };
const bob = { userId: 'bob', preference: 'green' };

function environmentFor(namespace) {
	return {
		KIT3_PRIVATE_VALUE: 'runtime-value',
		REDIS_URL: process.env.INTEGRATION_REDIS_URL,
		DATABASE_URL: process.env.INTEGRATION_POSTGRES_URL,
		TEST_NAMESPACE: namespace
	};
}

async function connect(WebSocket, sockets, app, token) {
	const socket = new WebSocket(app.origin.replace('http:', 'ws:') + '/ws', {
		headers: { origin: app.origin, cookie: 'token=' + token }
	});
	sockets.push(socket);
	const next = inbox(socket);
	await once(socket, 'open', { signal: AbortSignal.timeout(10000) });
	const ready = await next((frame) => frame.event === 'ready', 'WebSocket initialization');
	const http = await app.api();
	assert.equal(ready.data.environment, 'runtime-value', 'WebSocket module environment');
	assert.equal(ready.data.graph, http.graph, 'HTTP and WebSocket share the server module graph');
	assert.equal(ready.data.initialized, true, 'WebSocket init hook ran');
	assert.equal(http.initialized, true, 'HTTP sees WebSocket initialization');
	return { socket, next, ready: ready.data };
}

async function subscribe(client, topic) {
	client.socket.send(JSON.stringify({ type: 'subscribe', topic, ref: topic }));
	await client.next((frame) => frame.type === 'subscribed' && frame.ref === topic, 'Subscription acknowledgement');
}

async function send(client, operation) {
	const ref = operation.operation + ':' + operation.topic;
	client.socket.send(JSON.stringify({ ...operation, ref }));
	await client.next((frame) => frame.event === 'handled' && frame.data.ref === ref, 'Message handler completion');
}

// Sends from the left client on the first instance to the right client on
// the second instance through one backend path.
async function deliver(a, b, left, right, check, topic, label) {
	const value = { source: label, check, value: 42, nested: ['stored', true, null] };
	await subscribe(right, topic);
	if (check === 'registry') {
		const entry = await a.api({ operation: 'lookup', userId: 'bob' });
		assert.equal(entry?.instanceId, right.ready.registry, 'registry registration');
		assert.ok(entry.sessionId, 'registry uses the adapter connection identifier');
		await send(left, { operation: 'send', userId: 'bob', topic, value });
		assert.deepEqual((await right.next((frame) => frame.event === 'direct' && frame.topic === topic,
			'registry delivery')).data, value);
		return;
	}
	await send(left, { operation: check === 'bus' ? 'publish' : check, topic, value });
	assert.deepEqual((await right.next((frame) => frame.event === 'message' && frame.topic === topic,
		check + ' delivery')).data, value);
	if (check !== 'bus') {
		const history = await b.api({ operation: 'history', backend: check, topic });
		assert.equal(history.length, 1, check + ' persistence');
		assert.deepEqual(history[0], { seq: 1, topic, event: 'message', data: value }, check + ' readback');
	}
}

async function verify(directory, WebSocket, mode, namespace, checks = CHECKS) {
	const apps = [];
	const sockets = [];
	const full = checks === CHECKS;
	const label = mode + (full ? '' : '/' + (checks.join('+') || 'connect'));
	const environment = environmentFor(namespace);
	try {
		// Start separately so cleanup also owns the first server if the second fails.
		apps.push(await startApp(directory, mode, environment));
		apps.push(await startApp(directory, mode, environment));
		const [a, b] = apps;
		for (const app of apps) {
			const html = await (await fetch(app.origin, { signal: AbortSignal.timeout(10000) })).text();
			assert.match(html, /Extensions on SvelteKit 3/);
			assert.match(html, /Server environment: runtime-value/);
		}
		await a.api({ operation: 'session-set', token: 'alice-token', value: alice });
		await b.api({ operation: 'session-set', token: 'bob-token', value: bob });
		assert.deepEqual(await b.api({ operation: 'session-get', token: 'alice-token' }), alice);
		assert.deepEqual(await a.api({ operation: 'session-get', token: 'bob-token' }), bob);

		const left = await connect(WebSocket, sockets, a, 'alice-token');
		const right = await connect(WebSocket, sockets, b, 'bob-token');
		assert.equal(left.ready.userId, 'alice');
		assert.equal(right.ready.userId, 'bob');
		assert.notEqual(left.ready.registry, right.ready.registry, 'Separate registry instances');
		for (const check of checks) await deliver(a, b, left, right, check, namespace + check, label);
		assert.equal((await a.api()).messages, checks.length, 'HTTP sees WebSocket message updates');
		if (full) {
			const closed = once(right.socket, 'close');
			right.socket.close();
			await closed;
			await poll(() => a.api({ operation: 'lookup', userId: 'bob' }),
				(entry) => entry === null, 'registry removal on close');
			assert.equal(await a.api({ operation: 'session-delete', token: 'bob-token' }), true);
			assert.equal(await b.api({ operation: 'session-get', token: 'bob-token' }), null);
			const denied = new WebSocket(b.origin.replace('http:', 'ws:') + '/ws', {
				headers: { origin: b.origin, cookie: 'token=bob-token' }
			});
			sockets.push(denied);
			denied.on('error', () => {});
			const rejected = await new Promise((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error('Deleted token was not rejected')), 5000);
				denied.once('open', () => { clearTimeout(timer); reject(new Error('Deleted token opened a WebSocket')); });
				denied.once('unexpected-response', (_request, response) => {
					clearTimeout(timer);
					response.resume();
					denied.terminate();
					resolve(response.statusCode);
				});
			});
			assert.equal(rejected, 401, 'Deleted shared credentials reject upgrades');
			if (mode === 'production') {
				// Graceful shutdown closes connections before autoShutdown releases
				// the clients, so close hooks still remove their registry entries.
				const carol = { userId: 'carol', preference: 'red' };
				await b.api({ operation: 'session-set', token: 'carol-token', value: carol });
				const leaving = await connect(WebSocket, sockets, b, 'carol-token');
				assert.equal((await a.api({ operation: 'lookup', userId: 'carol' }))?.instanceId, leaving.ready.registry,
					'registry registration before shutdown');
				const closed = closeCode(leaving.socket, 'WebSocket close on graceful shutdown');
				await b.stop();
				const code = await closed;
				assert.equal(b.exitCode(), 0, 'Graceful shutdown completes');
				// Well inside the 90 second registry TTL, so expiry cannot pass it.
				await poll(() => a.api({ operation: 'lookup', userId: 'carol' }),
					(entry) => entry === null, 'registry removal during graceful shutdown', 5000);
				assert.equal((await a.api({ operation: 'lookup', userId: 'alice' }))?.instanceId, left.ready.registry,
					'registry lookups still work after the shutdown');
				console.log('Graceful shutdown closed the WebSocket with code ' + code);
			}
		}
		console.log('PASS ' + label + ': HTTP, private environment, shared modules, real backends and WebSockets');
	} catch (error) {
		for (const app of apps) {
			const output = app.output();
			if (output) console.error(output);
		}
		throw error;
	} finally {
		for (const socket of sockets) socket.terminate();
		await Promise.all(apps.map((app) => app.stop()));
	}
}

// Counts the backend connections the fixture opens under its namespace.
async function connectionMonitor(name) {
	const redis = new Redis(process.env.INTEGRATION_REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 1 });
	const postgres = new pg.Client({ connectionString: process.env.INTEGRATION_POSTGRES_URL });
	// A lost backend fails the next count instead of crashing the runner.
	redis.on('error', () => {});
	postgres.on('error', () => {});
	try {
		await redis.connect();
		await postgres.connect();
	} catch (error) {
		redis.disconnect();
		await postgres.end().catch(() => {});
		throw error;
	}
	return {
		async count() {
			const clients = String(await redis.client('LIST')).split('\n')
				.filter((line) => line.split(' ').includes('name=' + name));
			const { rows } = await postgres.query(
				'SELECT count(*)::int AS count FROM pg_stat_activity WHERE application_name = $1', [name]);
			return { redis: clients.length, postgres: rows[0].count };
		},
		async close() {
			redis.disconnect();
			await postgres.end().catch(() => {});
		}
	};
}

// Kit 3 replaces the server module graph for route and server module edits.
const EDITS = [
	{
		name: 'route', file: 'src/routes/api/+server.js', field: 'route',
		from: "const route = 'initial';", to: "const route = 'edited';"
	},
	{
		name: 'module', file: 'src/lib/server/services.js', field: 'revision',
		from: "export const revision = 'initial';", to: "export const revision = 'edited';"
	}
];

async function verifyDevEdits(directory, WebSocket, namespace) {
	const apps = [];
	const sockets = [];
	const originals = new Map();
	const environment = environmentFor(namespace);
	const monitor = await connectionMonitor(namespace + 'client');
	try {
		apps.push(await startApp(directory, 'dev', environment));
		apps.push(await startApp(directory, 'dev', environment));
		const [a, b] = apps;
		await a.api({ operation: 'session-set', token: 'alice-token', value: alice });
		await b.api({ operation: 'session-set', token: 'bob-token', value: bob });
		let left = await connect(WebSocket, sockets, a, 'alice-token');
		let right = await connect(WebSocket, sockets, b, 'bob-token');
		async function exercise(stage) {
			for (const check of CHECKS) {
				await deliver(a, b, left, right, check, namespace + stage + '_' + check, 'dev/' + stage);
			}
			// A handler instance that kept its bus relays each message again.
			await delay(500);
			const extra = right.next.pending().filter((frame) => frame.event === 'message' || frame.event === 'direct');
			assert.deepEqual(extra, [], 'no duplicate deliveries after the ' + stage + ' stage');
		}
		await exercise('initial');
		const baseline = await monitor.count();
		assert.ok(baseline.redis > 0 && baseline.postgres > 0, 'Named backend connections: ' + JSON.stringify(baseline));
		const listeners = await Promise.all([a.api(), b.api()].map((status) => status.then((value) => value.listeners)));
		assert.ok(listeners.every((count) => count > 0), 'Clients listen for sveltekit:shutdown: ' + listeners);
		for (const edit of EDITS) {
			const file = path.join(directory, edit.file);
			const source = await readFile(file, 'utf8');
			assert.equal(source.split(edit.from).length, 2, 'Edit has exactly one target');
			if (!originals.has(file)) originals.set(file, source);
			const previous = [left, right];
			const restarted = previous.map((client) => closeCode(client.socket, 'WebSocket restart'));
			await writeFile(file, source.replace(edit.from, edit.to));
			assert.deepEqual(await Promise.all(restarted), [1012, 1012],
				'The ' + edit.name + ' edit restarts WebSocket connections');
			for (const [index, app] of [a, b].entries()) {
				await poll(() => app.api().catch(() => null),
					(status) => status?.[edit.field] === 'edited' && status.graph !== previous[index].ready.graph,
					'The ' + edit.name + ' edit replaces the server module graph', 30000);
			}
			left = await connect(WebSocket, sockets, a, 'alice-token');
			right = await connect(WebSocket, sockets, b, 'bob-token');
			if (edit.field === 'revision') assert.equal(right.ready.revision, 'edited', 'WebSocket sees the edited module');
			await exercise(edit.name);
			await poll(() => monitor.count(),
				(count) => count.redis === baseline.redis && count.postgres === baseline.postgres,
				'The ' + edit.name + ' edit releases the previous backend connections (baseline '
					+ JSON.stringify(baseline) + ')', 15000);
			for (const [index, app] of [a, b].entries()) {
				assert.equal((await app.api()).listeners, listeners[index],
					'The ' + edit.name + ' edit removes the previous sveltekit:shutdown listeners');
			}
		}
		console.log('PASS dev/edits: route and server module edits restart WebSockets on one shared graph and release backends');
	} catch (error) {
		for (const app of apps) {
			const output = app.output();
			if (output) console.error(output);
		}
		throw error;
	} finally {
		try {
			for (const socket of sockets) socket.terminate();
			await Promise.allSettled(apps.map((app) => app.stop()));
		} finally {
			try {
				for (const [file, source] of originals) await writeFile(file, source);
			} finally {
				await monitor.close();
			}
		}
	}
}

await mkdir(path.join(fixture, '.runtime'), { recursive: true });
const directory = await mkdtemp(path.join(fixture, '.runtime/run-'));
for (const entry of await readdir(fixture, { withFileTypes: true })) {
	if (entry.name === '.runtime' || entry.name === '.gitignore') continue;
	await cp(path.join(fixture, entry.name), path.join(directory, entry.name), { recursive: true });
}
console.log('Consumer artifacts: ' + directory);
const packOutput = await npm(['pack', '--json', '--pack-destination', directory], root);
const packed = JSON.parse(packOutput);
const extensionsTarball = path.join(directory, packed[0].filename);
console.log('Extensions: ' + packed[0].filename + ' ' + packed[0].shasum + ' ' + packed[0].integrity);
const adapterTarball = process.env.KIT3_ADAPTER_TARBALL;
if (adapterTarball && !path.isAbsolute(adapterTarball)) throw new Error('KIT3_ADAPTER_TARBALL must be an absolute path');
const manifestPath = path.join(directory, 'package.json');
const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
manifest.dependencies['svelte-adapter-uws-extensions'] = 'file:' + extensionsTarball.replaceAll('\\', '/');
if (adapterTarball) manifest.dependencies['svelte-adapter-uws'] = 'file:' + adapterTarball.replaceAll('\\', '/');
await writeFile(manifestPath, JSON.stringify(manifest, null, 2) + '\n');
console.log('Installing the packed consumer...');
await npm(['install', '--no-audit', '--no-fund'], directory);
for (const [name, expected] of [
	['@sveltejs/kit', '3.0.1'], ['svelte-adapter-uws', '0.5.11'],
	['svelte-adapter-uws-extensions', packed[0].version]
]) {
	const installed = JSON.parse(await readFile(path.join(directory, 'node_modules', name, 'package.json'), 'utf8'));
	assert.equal(installed.version, expected, name + ' installed version');
}
const installedAdapter = JSON.parse(await readFile(path.join(directory, 'package-lock.json'), 'utf8'))
	.packages['node_modules/svelte-adapter-uws'];
console.log('Adapter: ' + installedAdapter.resolved + ' ' + installedAdapter.integrity);
const require = createRequire(path.join(directory, 'package.json'));
const { WebSocket } = require('ws');
const vite = path.join(directory, 'node_modules/vite/bin/vite.js');
const buildEnvironment = {
	...process.env,
	KIT3_PRIVATE_VALUE: 'build-value',
	REDIS_URL: 'redis://127.0.0.1:1',
	DATABASE_URL: 'postgres://test:test@127.0.0.1:1/test',
	TEST_NAMESPACE: 'kit3_build_'
};
const build = () => command(process.execPath, [vite, 'build'], directory, buildEnvironment);
process.env.INTEGRATION_REDIS_HOST_PORT ||= String(await freePort());
process.env.INTEGRATION_POSTGRES_HOST_PORT ||= String(await freePort());
// The standalone test suite reads the same name, so an exported value gets a suffix.
process.env.INTEGRATION_COMPOSE_PROJECT = process.env.INTEGRATION_COMPOSE_PROJECT
	? process.env.INTEGRATION_COMPOSE_PROJECT + '-kit3'
	: 'extensions-kit3-' + path.basename(directory).toLowerCase();
let teardown;
try {
	// The backend setup reads its port constants when imported.
	// Import the setup after choosing isolated host ports.
	const { default: isolatedSetup } = await import('../global-setup.js');
	teardown = await isolatedSetup();
	console.log('Building with unreachable backend URLs...');
	await build();
	for (const mode of ['production', 'dev']) {
		await verify(directory, WebSocket, mode, 'kit3_' + mode + '_');
	}
	await verifyDevEdits(directory, WebSocket, 'kit3_dev_edit_');
	if (withMutations) {
		const hooks = 'src/hooks.ws.js';
		const mutations = [
			{
				name: 'bus', file: hooks, from: 'const distributed = services.bus.wrap(platform);',
				to: 'const distributed = platform;', failure: /bus delivery timed out/, checks: ['bus']
			},
			{
				name: 'registry', file: hooks, from: 'await services.registry.hooks.open(ws, ctx);',
				to: '', failure: /registry registration/, checks: ['registry']
			},
			{
				name: 'redis', file: hooks,
				from: "await services.redisReplay.publish(distributed, command.topic, 'message', command.value);",
				to: "distributed.publish(command.topic, 'message', command.value);",
				failure: /redis persistence/, checks: ['redis']
			},
			{
				name: 'postgres', file: hooks,
				from: "await services.postgresReplay.publish(distributed, command.topic, 'message', command.value);",
				to: "distributed.publish(command.topic, 'message', command.value);",
				failure: /postgres persistence/, checks: ['postgres']
			},
			{
				// A second module instance for the WebSocket hooks splits the graph.
				name: 'graph', file: hooks, from: "from '#lib/server/services.js';",
				to: "from './lib/server/services.js?instance=hooks';",
				failure: /share the server module graph/, checks: []
			},
			{
				// Without shutdown, a replaced handler keeps relaying through its bus.
				name: 'shutdown', file: hooks, from: 'export { shutdown };', to: '',
				failure: /no duplicate deliveries/, edits: true
			},
			{
				// Releasing in production runs before connections close and drain.
				name: 'production-release', file: 'src/lib/server/services.js',
				from: 'if (!dev || !services) return;', to: 'if (!services) return;',
				failure: /registry removal during graceful shutdown/, modes: ['production']
			},
			{
				// Without the client release, a replaced handler keeps its connections.
				name: 'release', file: 'src/lib/server/services.js',
				from: 'await Promise.all([services.redis.quit(), services.pg.end()]);', to: '',
				failure: /releases the previous backend connections/, edits: true
			}
		];
		for (const mutation of mutations) {
			const file = path.join(directory, mutation.file);
			const original = await readFile(file, 'utf8');
			assert.equal(original.split(mutation.from).length, 2, 'Mutation has exactly one target');
			try {
				await writeFile(file, original.replace(mutation.from, mutation.to));
				await build();
				const runs = mutation.edits
					? [['dev', (namespace) => verifyDevEdits(directory, WebSocket, namespace)]]
					: (mutation.modes ?? ['production', 'dev']).map((mode) => [mode,
						(namespace) => verify(directory, WebSocket, mode, namespace, mutation.checks ?? CHECKS)]);
				for (const [mode, run] of runs) {
					const failure = await run('kit3_mut_' + mutation.name + '_' + mode + '_').then(() => null, (error) => error);
					assert.ok(failure, 'The ' + mutation.name + ' mutation (' + mode + ') must fail a behavior assertion');
					const reason = String(failure).split('\n')[0];
					assert.match(reason, mutation.failure,
						'The ' + mutation.name + ' mutation (' + mode + ') failed elsewhere: ' + reason);
					console.log('KILLED ' + mutation.name + ' (' + mode + '): ' + reason);
				}
			} finally {
				await writeFile(file, original);
			}
		}
		await build();
		for (const mode of ['production', 'dev']) {
			await verify(directory, WebSocket, mode, 'kit3_restored_' + mode + '_');
		}
		await verifyDevEdits(directory, WebSocket, 'kit3_restored_dev_edit_');
	}
} finally {
	await teardown?.();
}
