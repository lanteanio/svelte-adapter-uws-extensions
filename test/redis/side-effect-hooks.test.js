import { afterEach, describe, expect, it } from 'vitest';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createGroup } from '../../src/redis/groups.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';

let uWS;
try { uWS = (await import('uWebSockets.js')).default; } catch { uWS = null; }
if (!uWS && process.env.REQUIRE_UWS === '1') throw new Error('uWebSockets.js is required');
const describeUWS = uWS ? describe : describe.skip;

const cleanups = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function connect(url) {
	const { WebSocket } = await import('ws');
	const ws = new WebSocket(url);
	const messages = [];
	ws.on('message', (raw, binary) => {
		if (!binary) {
			try { messages.push(JSON.parse(raw.toString())); } catch { /* not JSON */ }
		}
	});
	await new Promise((resolve, reject) => {
		ws.once('open', resolve);
		ws.once('error', reject);
	});
	cleanups.push(() => ws.close());
	return {
		send: (message) => ws.send(JSON.stringify(message)),
		async ack(ref) {
			await expect.poll(() => messages.find((m) => m.ref === ref), { timeout: 3000 }).toBeDefined();
			return messages.find((m) => m.ref === ref);
		}
	};
}

const plugins = {
	presence: (redis) => createPresence(redis, { key: 'id', topicThrottle: 0, binary: false }),
	cursor: (redis) => createCursor(redis),
	groups: (redis) => createGroup(redis, 'lobby')
};

async function start(plugin, wrap, options = {}) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const subscribe = wrap ? (ws, topic, ctx) => plugin.hooks.subscribe(ws, topic, ctx) : plugin.hooks.subscribe;
	const server = await createTestServer({
		authorizeWireSubscribe: true,
		...options,
		handler: {
			upgrade() { return { id: 'u1' }; },
			subscribe,
			close: plugin.hooks.close
		}
	});
	cleanups.push(() => server.close());
	return server;
}

describeUWS('plugin subscribe hooks keep the server-grant gate armed', () => {
	for (const [name, make] of Object.entries(plugins)) {
		it(`${name}: exporting hooks.subscribe as-is refuses an ungranted application topic`, async () => {
			const plugin = make(mockRedisClient(name + '-se:'));
			cleanups.push(() => plugin.destroy?.());
			const server = await start(plugin, false);
			const client = await connect(server.wsUrl);
			client.send({ type: 'subscribe', topic: 'private-room', ref: 1 });
			expect(await client.ack(1)).toMatchObject({ type: 'subscribe-denied', topic: 'private-room' });
		});

		it(`${name}: a wrapper function is app code and the gate steps aside`, async () => {
			const plugin = make(mockRedisClient(name + '-wr:'));
			cleanups.push(() => plugin.destroy?.());
			const server = await start(plugin, true);
			const client = await connect(server.wsUrl);
			client.send({ type: 'subscribe', topic: 'private-room', ref: 1 });
			expect(await client.ack(1)).toMatchObject({ type: 'subscribed', topic: 'private-room' });
		});
	}
});

describeUWS('plugin channels under an armed server-grant gate', () => {
	for (const allowSystemTopicSubscribe of [false, true]) {
		it(`groups: a client joins through __group:<name> with the exported hook (allowSystemTopicSubscribe: ${allowSystemTopicSubscribe})`, async () => {
			const group = createGroup(mockRedisClient('groups-own-' + allowSystemTopicSubscribe + ':'), 'lobby');
			cleanups.push(() => group.destroy());
			const server = await start(group, false, { allowSystemTopicSubscribe });
			const client = await connect(server.wsUrl);
			client.send({ type: 'subscribe', topic: '__group:lobby', ref: 1 });
			expect(await client.ack(1)).toMatchObject({ type: 'subscribed', topic: '__group:lobby' });
			expect(await group.count()).toBe(1);
		});
	}

	it('groups: a full group refuses the subscribe', async () => {
		const group = createGroup(mockRedisClient('groups-full:'), 'lobby', { maxMembers: 1 });
		cleanups.push(() => group.destroy());
		const server = await start(group, false);
		const first = await connect(server.wsUrl);
		first.send({ type: 'subscribe', topic: '__group:lobby', ref: 1 });
		expect(await first.ack(1)).toMatchObject({ type: 'subscribed' });
		const second = await connect(server.wsUrl);
		second.send({ type: 'subscribe', topic: '__group:lobby', ref: 2 });
		expect(await second.ack(2)).toMatchObject({ type: 'subscribe-denied', topic: '__group:lobby' });
		expect(await group.count()).toBe(1);
	});

	it('groups: a __group: topic no group handles is refused', async () => {
		const group = createGroup(mockRedisClient('groups-other:'), 'lobby');
		cleanups.push(() => group.destroy());
		const server = await start(group, false);
		const client = await connect(server.wsUrl);
		client.send({ type: 'subscribe', topic: '__group:elsewhere', ref: 1 });
		expect(await client.ack(1)).toMatchObject({ type: 'subscribe-denied', topic: '__group:elsewhere' });
	});

	// Presence and cursor taps are not plugin-owned namespaces, as for the
	// adapter's own presence and cursor plugins: with the gate armed the
	// server joins a socket to them (join / attach / a granted topic).
	for (const [name, topic] of [['presence', '__presence:room'], ['cursor', '__cursor:room']]) {
		it(`${name}: a client subscribe to ${topic} is refused`, async () => {
			const plugin = plugins[name](mockRedisClient(name + '-tap:'));
			cleanups.push(() => plugin.destroy?.());
			const server = await start(plugin, false, { allowSystemTopicSubscribe: true });
			const client = await connect(server.wsUrl);
			client.send({ type: 'subscribe', topic, ref: 1 });
			expect(await client.ack(1)).toMatchObject({ type: 'subscribe-denied', topic });
		});
	}
});
