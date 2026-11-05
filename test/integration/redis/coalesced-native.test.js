import { afterEach, describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';

const servers = [];
const buses = [];
const backends = [];
const sockets = [];

afterEach(async () => {
	for (const socket of sockets.splice(0)) {
		socket.resume();
		socket.terminate();
	}
	const results = await Promise.allSettled(buses.splice(0).map(bus => bus.deactivate()));
	results.push(...await Promise.allSettled(servers.splice(0).map(server => server.close())));
	results.push(...await Promise.allSettled(backends.splice(0).map(client => client.quit())));
	const failures = results.filter(result => result.status === 'rejected');
	if (failures.length) throw new AggregateError(failures.map(result => result.reason), 'Coalesced transport cleanup failed');
});

async function waitFor(read, description, clients = [], timeoutMs = 10000) {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		for (const client of clients) client.check();
		if (read()) return;
		if (Date.now() >= deadline) throw new Error('Timed out waiting for ' + description);
		await new Promise(resolve => setTimeout(resolve, 5));
	}
}

async function connect(server) {
	const socket = server.track(new WebSocket(server.wsUrl));
	sockets.push(socket);
	const frames = [];
	const state = { binaryBytes: 0, failure: null };
	socket.on('error', error => { state.failure = error; });
	socket.on('message', (data, binary) => {
		if (binary) { state.binaryBytes += data.length; return; }
		try { frames.push(JSON.parse(data.toString())); }
		catch (error) { state.failure = error; }
	});
	const client = {
		socket, frames, state,
		check() {
			if (state.failure) throw state.failure;
			if (socket.readyState === WebSocket.CLOSED) throw new Error('WebSocket closed before the transport assertions finished');
		}
	};
	await waitFor(() => socket.readyState === WebSocket.OPEN, 'WebSocket connection', [client]);
	return client;
}

async function subscribe(client, topic) {
	const ref = randomUUID();
	client.socket.send(JSON.stringify({ type: 'subscribe', topic, ref }));
	await waitFor(() => client.frames.some(frame => frame.type === 'subscribed' && frame.topic === topic && frame.ref === ref), 'subscription acknowledgement', [client]);
}

function framesFor(client, topic, event) {
	client.check();
	return client.frames.filter(frame => frame.topic === topic && frame.event === event);
}

for (const mode of ['classic', 'sharded']) describe(`${mode} coalesced relay over Redis and native WebSockets`, () => {
	it('returns true, avoids local delivery, and delivers only the latest value after receiver backpressure drains', async () => {
		const identity = randomUUID();
		const topic = 'coalesced-' + identity;
		const channel = 'integration-coalesced:' + identity;
		const senderClient = createBackendClient();
		backends.push(senderClient);
		const receiverClient = createBackendClient();
		backends.push(receiverClient);
		const handler = { subscribe(_ws, name) { return name === topic ? null : 'FORBIDDEN'; } };
		const source = await createTestServer({ handler });
		servers.push(source);
		const target = await createTestServer({ handler });
		servers.push(target);
		const factory = mode === 'classic'
			? client => createPubSubBus(client, { channel, systemChannel: false })
			: client => createShardedBus(client, { channelPrefix: channel + ':' });
		const sender = factory(senderClient);
		buses.push(sender);
		const receiver = factory(receiverClient);
		buses.push(receiver);
		await sender.activate(source.platform);
		await receiver.activate(target.platform);
		if (mode === 'sharded') {
			await sender.follow(topic);
			await receiver.follow(topic);
		}
		const local = await connect(source);
		const slow = await connect(target);
		expect(target.wsConnections.size).toBe(1);
		const nativeSlow = [...target.wsConnections][0];
		const observer = await connect(target);
		await Promise.all([subscribe(local, topic), subscribe(slow, topic), subscribe(observer, topic)]);
		const wrapped = sender.wrap(source.platform);
		wrapped.publish(topic, 'ready', { ready: true }, { seq: false });
		const clients = [local, slow, observer];
		await waitFor(() => clients.every(client => framesFor(client, topic, 'ready').length === 1), 'Redis round trip', clients);

		// Pause one client and fill its uWS output buffer, at most 64 MiB, until
		// a send is refused after a settling turn. The coalesced values and the
		// drain callbacks go through the unmodified platform.
		slow.socket.pause();
		expect(nativeSlow).toBeDefined();
		const chunk = new Uint8Array(1024 * 1024);
		let accepted = 0;
		let backpressured = false;
		for (let attempt = 0; attempt < 64 && accepted < 64; attempt++) {
			while (accepted < 64) {
				if (nativeSlow.send(chunk, true) === 2) break;
				accepted++;
			}
			if (accepted === 64) break;
			await new Promise(resolve => setTimeout(resolve, 25));
			if (nativeSlow.send(chunk, true) === 2) { backpressured = true; break; }
			accepted++;
		}
		expect(backpressured, 'paused receiver must report backpressure before the burst').toBe(true);
		expect(nativeSlow.getBufferedAmount()).toBeGreaterThan(0);
		const count = 8;
		for (let index = 0; index < count; index++) {
			expect(wrapped.relayCoalesced(topic, 'position', { index }, 'cursor')).toBe(true);
		}
		// The observer is on the same server and Redis topic, so its fence frame
		// arrives only after the server processed the whole burst.
		wrapped.publish(topic, 'burst-fence', { identity }, { seq: false });
		await waitFor(() => framesFor(observer, topic, 'burst-fence').length === 1, 'receiver-side Redis burst fence', [local, observer]);
		expect(framesFor(observer, topic, 'position').map(frame => frame.data)).toEqual(Array.from({ length: count }, (_, index) => ({ index })));
		expect(framesFor(local, topic, 'position')).toEqual([]);
		expect(slow.frames.filter(frame => frame.topic === topic && frame.event === 'position')).toEqual([]);
		expect(nativeSlow.getBufferedAmount()).toBeGreaterThan(0);

		slow.socket.resume();
		await waitFor(() => framesFor(slow, topic, 'position').length > 0, 'latest coalesced value after the native drain', clients);
		wrapped.publish(topic, 'drained-fence', { identity }, { seq: false });
		await waitFor(() => clients.every(client => framesFor(client, topic, 'drained-fence').length === 1), 'post-drain delivery fence', clients);
		expect(slow.state.binaryBytes).toBeGreaterThanOrEqual(chunk.length);
		expect(framesFor(slow, topic, 'position').map(frame => frame.data)).toEqual([{ index: count - 1 }]);
		expect(framesFor(local, topic, 'position')).toEqual([]);
	});
});
