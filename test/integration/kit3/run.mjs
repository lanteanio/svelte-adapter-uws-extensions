import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { cp, mkdir, mkdtemp, readFile, readdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

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
	return async (predicate, label) => {
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
}

async function startApp(directory, mode, environment) {
	const port = await freePort();
	const origin = 'http://127.0.0.1:' + port;
	const child = spawn(process.execPath, [mode === 'dev' ? 'dev.mjs' : 'build/index.js'], {
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
		async stop() {
			if (child.exitCode !== null || child.signalCode !== null || spawnError) return;
			const exited = once(child, 'exit');
			if (mode === 'dev' && child.connected) child.send('stop');
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

async function verify(directory, WebSocket, mode, namespace, only) {
	const apps = [];
	const sockets = [];
	const label = mode + (only ? '/' + only : '');
	const environment = {
		KIT3_PRIVATE_VALUE: 'runtime-value',
		REDIS_URL: process.env.INTEGRATION_REDIS_URL,
		DATABASE_URL: process.env.INTEGRATION_POSTGRES_URL,
		TEST_NAMESPACE: namespace
	};
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
		const alice = { userId: 'alice', preference: 'blue' };
		const bob = { userId: 'bob', preference: 'green' };
		await a.api({ operation: 'session-set', token: 'alice-token', value: alice });
		await b.api({ operation: 'session-set', token: 'bob-token', value: bob });
		assert.deepEqual(await b.api({ operation: 'session-get', token: 'alice-token' }), alice);
		assert.deepEqual(await a.api({ operation: 'session-get', token: 'bob-token' }), bob);

		async function connect(app, token) {
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
		const left = await connect(a, 'alice-token');
		const right = await connect(b, 'bob-token');
		assert.equal(left.ready.userId, 'alice');
		assert.equal(right.ready.userId, 'bob');
		assert.notEqual(left.ready.registry, right.ready.registry, 'Separate registry instances');

		async function subscribe(client, topic) {
			client.socket.send(JSON.stringify({ type: 'subscribe', topic, ref: topic }));
			await client.next((frame) => frame.type === 'subscribed' && frame.ref === topic, 'Subscription acknowledgement');
		}
		async function send(client, operation) {
			const ref = operation.operation + ':' + operation.topic;
			client.socket.send(JSON.stringify({ ...operation, ref }));
			await client.next((frame) => frame.event === 'handled' && frame.data.ref === ref, 'Message handler completion');
		}
		const modes = only ? [only] : ['bus', 'redis', 'postgres', 'registry'];
		for (const check of modes) {
			const topic = namespace + check;
			const value = { source: label, check, value: 42, nested: ['stored', true, null] };
			await subscribe(right, topic);
			if (check === 'registry') {
				const entry = await a.api({ operation: 'lookup', userId: 'bob' });
				assert.equal(entry?.instanceId, right.ready.registry, 'registry registration');
				assert.ok(entry.sessionId, 'registry uses the adapter connection identifier');
				await send(left, { operation: 'send', userId: 'bob', topic, value });
				assert.deepEqual((await right.next((frame) => frame.event === 'direct' && frame.topic === topic,
					'registry delivery')).data, value);
			} else {
				await send(left, { operation: check === 'bus' ? 'publish' : check, topic, value });
				assert.deepEqual((await right.next((frame) => frame.event === 'message' && frame.topic === topic,
					check + ' delivery')).data, value);
				if (check !== 'bus') {
					const history = await b.api({ operation: 'history', backend: check, topic });
					assert.equal(history.length, 1, check + ' persistence');
					assert.deepEqual(history[0], { seq: 1, topic, event: 'message', data: value }, check + ' readback');
				}
			}
		}
		assert.equal((await a.api()).messages, modes.length, 'HTTP sees WebSocket message updates');
		if (!only) {
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
process.env.INTEGRATION_COMPOSE_PROJECT ||= 'extensions-kit3-' + path.basename(directory).toLowerCase();
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
	if (withMutations) {
		const hooksPath = path.join(directory, 'src/hooks.ws.js');
		const original = await readFile(hooksPath, 'utf8');
		const mutations = [
			{
				name: 'bus', from: 'const distributed = services.bus.wrap(platform);',
				to: 'const distributed = platform;', failure: /bus delivery timed out/
			},
			{
				name: 'registry', from: 'await services.registry.hooks.open(ws, ctx);',
				to: '', failure: /registry registration/
			},
			{
				name: 'redis',
				from: "await services.redisReplay.publish(distributed, command.topic, 'message', command.value);",
				to: "distributed.publish(command.topic, 'message', command.value);",
				failure: /redis persistence/
			},
			{
				name: 'postgres',
				from: "await services.postgresReplay.publish(distributed, command.topic, 'message', command.value);",
				to: "distributed.publish(command.topic, 'message', command.value);",
				failure: /postgres persistence/
			}
		];
		try {
			for (const mutation of mutations) {
				assert.equal(original.split(mutation.from).length, 2, 'Mutation has exactly one target');
				await writeFile(hooksPath, original.replace(mutation.from, mutation.to));
				await build();
				for (const mode of ['production', 'dev']) {
					await assert.rejects(
						verify(directory, WebSocket, mode, 'kit3_mut_' + mutation.name + '_' + mode + '_', mutation.name),
						mutation.failure,
						'The ' + mutation.name + ' mutation must fail its behavior assertion'
					);
					console.log('KILLED ' + mutation.name + ' (' + mode + ')');
				}
			}
		} finally {
			await writeFile(hooksPath, original);
		}
		await build();
		for (const mode of ['production', 'dev']) {
			await verify(directory, WebSocket, mode, 'kit3_restored_' + mode + '_');
		}
	}
} finally {
	await teardown?.();
}
