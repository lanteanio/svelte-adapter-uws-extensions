import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Client } from 'pg';
import { WebSocket } from 'ws';

for (const key of ['INTEGRATION_REDIS_URL', 'INTEGRATION_POSTGRES_URL']) {
	assert.ok(process.env[key], key + ' must explicitly name an isolated test service');
	const url = new URL(process.env[key]);
	assert.ok(['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname), key + ' must use loopback');
}
const schema = 'delivery_' + randomUUID().replaceAll('-', '');
const db = new Client({ connectionString: process.env.INTEGRATION_POSTGRES_URL, connectionTimeoutMillis: 5000, query_timeout: 6000, statement_timeout: 5000 });
const children = [], sockets = [];
const servers = [];
let failure;
const watchdog = setTimeout(() => { failure = new Error('Packed delivery contract timed out'); }, 80000);
function check() {
	if (failure) throw failure;
	for (const server of servers) server.check();
}
async function waitFor(read, label) {
	const deadline = Date.now() + 10000;
	while (true) {
		check();
		const value = read();
		if (value) { check(); return value; }
		if (Date.now() > deadline) throw new Error('Timed out waiting for ' + label);
		await delay(5);
	}
}
async function start(backend, topic, mode) {
	const child = fork(new URL('./cross-repo-delivery-worker.mjs', import.meta.url), [], {
		env: { ...process.env, DELIVERY_SCHEMA: schema, DELIVERY_CHANNEL: topic, DELIVERY_BACKEND: backend, DELIVERY_MODE: mode },
		stdio: ['ignore', 'pipe', 'pipe', 'ipc']
	});
	children.push(child);
	const messages = [];
	let diagnostics = '';
	let stopping = false, closed = false;
	child.stdout.on('data', data => { diagnostics += data; });
	child.stderr.on('data', data => { diagnostics += data; });
	child.on('message', message => messages.push(message));
	child.once('close', () => { closed = true; });
	child.on('error', error => messages.push({ type: 'failure', message: error.message }));
	function check() {
		const failure = messages.find(message => message.type === 'failure');
		if (failure) throw new Error(failure.message);
		if (closed || child.exitCode !== null) {
			if (!stopping || child.exitCode !== 0) throw new Error('Child exited: ' + diagnostics);
		}
	}
	const ready = await waitFor(() => { check(); return messages.find(message => message.type === 'ready'); }, 'native server readiness');
	const server = { child, messages, check, wsUrl: ready.wsUrl, get closed() { return closed; }, stop() { stopping = true; child.send({ type: 'stop' }); } };
	servers.push(server);
	return server;
}
async function connect(server, subject, topic, rpc) {
	const socket = new WebSocket(server.wsUrl, { headers: { Cookie: 'subject=' + subject + '; expectedRpc=' + (rpc || '') } });
	sockets.push(socket);
	const frames = [];
	let failure;
	socket.on('error', error => { failure = error; });
	socket.on('message', data => frames.push(data.toString()));
	await waitFor(() => { server.check(); if (failure) throw failure; return socket.readyState === WebSocket.OPEN; }, 'native connection');
	const id = randomUUID();
	let deliveryTopic = topic;
	socket.send(JSON.stringify(rpc ? { rpc, args: [topic], id, stream: true } : { type: 'subscribe', topic, ref: id }));
	await waitFor(() => frames.some(raw => {
		const frame = JSON.parse(raw);
		if (!rpc) return frame.type === 'subscribed' && frame.ref === id;
		if (frame.topic !== '__rpc' || frame.event !== id || frame.data?.ok !== true || typeof frame.data.topic !== 'string' || frame.data.data?.nonce !== 'snapshot') return false;
		deliveryTopic = frame.data.topic;
		return true;
	}), 'admitted subscription and snapshot');
	return { socket, frames, deliveryTopic };
}
function received(client, nonce) {
	return client.frames.some(raw => {
		const frame = JSON.parse(raw);
		return frame.topic === client.deliveryTopic && frame.event === 'set' && frame.data?.nonce === nonce;
	});
}
function leaked(client, nonce) { return client.frames.some(raw => raw.includes(nonce)); }
let connected = false;
try {
	await db.connect();
	connected = true;
	await db.query('CREATE SCHEMA ' + schema);
	await db.query('CREATE TABLE ' + schema + '.permission(subject text primary key, allowed boolean not null)');
	await db.query('CREATE TABLE ' + schema + '.payload(value jsonb not null)');
	for (const backend of ['redis', 'postgres']) {
		for (const mode of ['raw', 'realtime']) {
			const topic = schema + '_' + backend + '_' + mode;
			await db.query('TRUNCATE ' + schema + '.permission, ' + schema + '.payload');
			await db.query('INSERT INTO ' + schema + ".permission VALUES ('revoked',true),('control',true)");
			await db.query('INSERT INTO ' + schema + '.payload VALUES ($1)', [{ nonce: 'snapshot' }]);
			const source = await start(backend, topic, 'raw');
			const target = await start(backend, topic, mode);
			const revoked = [], controls = [];
			for (const rpc of mode === 'realtime' ? ['security/feed', 'security/stats'] : [null]) {
				revoked.push(await connect(target, 'revoked', topic, rpc));
				controls.push(await connect(target, 'control', topic, rpc));
			}
			const before = randomUUID();
			await db.query('UPDATE ' + schema + '.payload SET value=$1', [{ nonce: before }]);
			source.child.send({ type: 'publish', data: { nonce: before } });
			try {
				await waitFor(() => { source.check(); target.check(); return [...revoked, ...controls].every(client => received(client, before)); }, 'authorized cross-instance delivery ' + backend + '/' + mode);
			} catch (error) {
				console.error(JSON.stringify({ backend, mode, received: [...revoked, ...controls].map(client => received(client, before)), attempts: target.messages.filter(message => message.type === 'attempt' || message.type === 'policy' || message.type === 'failure') }));
				throw error;
			}
			const secret = randomUUID();
			await db.query('UPDATE ' + schema + '.payload SET value=$1', [{ nonce: secret, pending: true }]);
			source.child.send({ type: 'publish', data: { nonce: secret, pending: true } });
			await waitFor(() => target.messages.filter(message => message.type === 'pending').length === revoked.length, 'pending recipient authorization');
			await db.query('UPDATE ' + schema + ".permission SET allowed=false WHERE subject='revoked'");
			target.child.send({ type: 'release' });
			await waitFor(() => revoked.every(client => client.socket.readyState === WebSocket.CLOSED), 'revoked socket closure');
			await waitFor(() => controls.every(client => received(client, secret)), 'unaffected control recipients');
			assert.ok(revoked.every(client => !leaked(client, secret)), 'Pending payload leaked after committed revocation');
			assert.equal(target.messages.filter(message => message.type === 'policy' && message.nonce === secret && !message.allowed).length, revoked.length);
			const after = randomUUID();
			await db.query('UPDATE ' + schema + '.payload SET value=$1', [{ nonce: after }]);
			source.child.send({ type: 'publish', data: { nonce: after } });
			await waitFor(() => controls.every(client => received(client, after)), 'continued control delivery');
			assert.ok(revoked.every(client => !leaked(client, after)));
			for (const client of [...revoked, ...controls]) client.socket.terminate();
			for (const server of [source, target]) server.stop();
			await waitFor(() => [source, target].every(server => server.closed && server.child.exitCode === 0), 'clean native process shutdown');
			check();
			console.log('Packed delivery case passed: ' + backend + '/' + mode);
		}
	}
	check();
	console.log('Packed delivery contract passed: Redis and PostgreSQL, native cross-process revocation, realtime stream and opaque derived identity');
} finally {
	for (const socket of sockets) socket.terminate();
	try {
		for (const child of children) if (child.exitCode === null && child.connected) child.send({ type: 'stop' });
		const deadline = Date.now() + 10000;
		while (children.some(child => child.exitCode === null) && Date.now() < deadline) await delay(5);
		for (const child of children) if (child.exitCode === null) child.kill();
		if (connected) await db.query('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE');
	} finally {
		await db.end();
		clearTimeout(watchdog);
	}
}
