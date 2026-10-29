// Where the sharded bus activates under the real adapter. The adapter closes a
// connection with 1011 when its `open` hook rejects, and the sharded bus does
// not retry a failed activation on its own, so activating in `open` closes
// every connection for as long as activation fails. `init` runs once at
// startup and activates the bus for every connection after it.
import { describe, it, expect, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}
const describeUWS = uWS ? describe : describe.skip;

const TOPIC = 'chat';
const REMOTE = JSON.stringify({ instanceId: 'other-instance', topic: TOPIC, event: 'remote', data: 1 });

/** @type {Array<() => unknown>} */
const cleanups = [];
afterEach(async () => {
	for (const fn of cleanups.splice(0).reverse()) {
		try { await fn(); } catch { /* already closed */ }
	}
	vi.restoreAllMocks();
});

/** Wait for `check` to hold, polling, and fail after `ms`. */
async function until(check, ms = 3000) {
	const deadline = Date.now() + ms;
	while (!check()) {
		if (Date.now() > deadline) throw new Error('condition not reached');
		await new Promise((r) => setTimeout(r, 5));
	}
}

/** A client that records every JSON frame it receives and how it closed. */
async function connect(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const state = { messages: [], closed: null, ws };
	ws.on('message', (raw, isBinary) => {
		if (isBinary) return;
		try { state.messages.push(JSON.parse(raw.toString())); } catch { /* not JSON */ }
	});
	ws.on('close', (code) => { state.closed = code; });
	await new Promise((resolve, reject) => {
		ws.on('open', resolve);
		ws.on('error', reject);
	});
	cleanups.push(() => ws.close());
	return state;
}

/** Subscribe a connection to `topic` and wait for the server's ack. */
async function subscribe(c, topic) {
	c.ws.send(JSON.stringify({ type: 'subscribe', topic, ref: topic }));
	await until(() => c.messages.some((m) => m.type === 'subscribed' && m.topic === topic));
}

/** A sharded bus whose INFO probe rejects while `refuse.on`, counting probes. */
function probedBus(refuse) {
	const client = mockRedisClient('sharded-init:');
	const probes = { count: 0 };
	const info = client.redis.info.bind(client.redis);
	client.redis.info = (...args) => {
		probes.count++;
		return refuse.on ? Promise.reject(new Error('INFO refused')) : info(...args);
	};
	const bus = createShardedBus(client);
	cleanups.push(() => bus.deactivate());
	return { client, bus, probes };
}

async function serve(handler) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const server = await createTestServer({ handler });
	cleanups.push(() => server.close());
	return server;
}

describeUWS('sharded bus activation under the real adapter', () => {
	it('closes every connection with 1011 while activation in the open hook fails', async () => {
		vi.spyOn(console, 'error').mockImplementation(() => {});
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		const refuse = { on: true };
		const { bus, probes } = probedBus(refuse);
		const server = await serve({
			async open(ws, { platform }) {
				await bus.activate(platform);
			}
		});

		const first = await connect(server.wsUrl);
		const second = await connect(server.wsUrl);
		await until(() => first.closed !== null && second.closed !== null);
		expect(first.closed).toBe(1011);
		expect(second.closed).toBe(1011);
		// One attempt per connection and none between them.
		await new Promise((r) => setTimeout(r, 50));
		expect(probes.count).toBe(2);
	});

	it('activates once in init and delivers remote messages to every connection', async () => {
		const { client, bus, probes } = probedBus({ on: false });
		const server = await serve({
			async init({ platform }) {
				await bus.activate(platform);
				await bus.follow(TOPIC);
			},
			subscribe() {}
		});

		const first = await connect(server.wsUrl);
		const second = await connect(server.wsUrl);
		await subscribe(first, TOPIC);
		await subscribe(second, TOPIC);
		await client.redis.spublish('uws:sharded:' + TOPIC, REMOTE);
		const remote = (c) => c.messages.filter((m) => m.topic === TOPIC && m.event === 'remote');
		await until(() => remote(first).length === 1 && remote(second).length === 1);
		expect(first.closed).toBe(null);
		expect(second.closed).toBe(null);
		expect(probes.count).toBe(1);
	});
});
