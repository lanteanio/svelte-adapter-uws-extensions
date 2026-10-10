import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import uWS from 'uWebSockets.js';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
import { mockRedisClient } from 'svelte-adapter-uws-extensions/testing';
import { createMessage, setBus } from 'svelte-realtime/server';

assert.equal(typeof uWS.App, 'function', 'The canonical native uWebSockets.js module must load');
const topic = 'packed-contract';
const sockets = [], servers = [], buses = [];
const redis = mockRedisClient('packed-contract:');
const watchdog = setTimeout(() => {
	console.error('Packed runtime contract exceeded its 20-second deadline');
	process.exit(1);
}, 20000);

async function waitFor(read, description) {
	const deadline = Date.now() + 3000;
	while (!read()) {
		if (Date.now() >= deadline) throw new Error('Timed out waiting for ' + description);
		await delay(5);
	}
}

async function connect(server) {
	const socket = new WebSocket(server.wsUrl);
	sockets.push(socket);
	const messages = [];
	let failure = null;
	socket.on('error', error => { failure = error; });
	socket.on('message', (data, binary) => {
		try {
			assert.equal(binary, false);
			messages.push(JSON.parse(data.toString()));
		} catch (error) { failure = error; }
	});
	await waitFor(() => { if (failure) throw failure; return socket.readyState === WebSocket.OPEN; }, 'WebSocket connection');
	return {
		send(message) { if (failure) throw failure; socket.send(JSON.stringify(message)); },
		messages() { if (failure) throw failure; return messages; }
	};
}

async function subscribe(client) {
	client.send({ type: 'subscribe', topic, ref: 'ready' });
	await waitFor(() => client.messages().some(message => message.type === 'subscribed' && message.ref === 'ready'), 'subscription acknowledgement');
}

async function main() {
	const senderBus = createPubSubBus(redis), receiverBus = createPubSubBus(redis);
	buses.push(senderBus, receiverBus);
	setBus(senderBus);
	const source = await createTestServer({ handler: {
		subscribe(_ws, name) { return name === topic ? null : 'FORBIDDEN'; },
		message: createMessage({ onJsonMessage(_ws, message, platform) {
			if (message.type === 'contract-publish') platform.publish(topic, 'changed', message.data);
		} })
	} });
	servers.push(source);
	const target = await createTestServer({ handler: { subscribe(_ws, name) { return name === topic ? null : 'FORBIDDEN'; } } });
	servers.push(target);
	await senderBus.activate(source.platform);
	await receiverBus.activate(target.platform);
	const local = await connect(source), remote = await connect(target);
	await subscribe(local); await subscribe(remote);
	const expected = [];
	const values = client => client.messages().filter(message => message.topic === topic && message.event === 'changed').map(message => message.data);
	for (let burst = 0; burst < 2; burst++) {
		for (let offset = 0; offset < 3; offset++) {
			const data = { index: expected.length, text: 'message-' + expected.length, nested: { value: true } };
			expected.push(data);
			local.send({ type: 'contract-publish', data });
		}
		await waitFor(() => values(local).length >= expected.length && values(remote).length >= expected.length, 'local and remote relayed values');
		assert.deepEqual(values(local), expected, 'Local delivery changed values, order or count');
		assert.deepEqual(values(remote), expected, 'Packed realtime bus wiring did not preserve remote values, order or count');
	}
	// Let queued relay echoes reach the clients before checking the final count.
	await delay(100);
	assert.deepEqual(values(local), expected, 'Local delivery changed values, order or count after settling');
	assert.deepEqual(values(remote), expected, 'Remote delivery changed values, order or count after settling');
	console.log('Packed runtime contract passed: native WebSocket -> realtime message -> extension relay -> remote WebSocket; 6 ordered values per client');
}

try {
	await main();
} finally {
	setBus(null);
	for (const socket of sockets) socket.terminate();
	const cleanup = await Promise.allSettled([...buses.map(bus => bus.deactivate()), ...servers.map(server => server.close())]);
	await redis.quit();
	clearTimeout(watchdog);
	const failure = cleanup.find(result => result.status === 'rejected');
	if (failure) throw failure.reason;
}
