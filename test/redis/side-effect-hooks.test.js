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

async function start(plugin, wrap) {
	const { createTestServer } = await import('svelte-adapter-uws/testing');
	const subscribe = wrap ? (ws, topic, ctx) => plugin.hooks.subscribe(ws, topic, ctx) : plugin.hooks.subscribe;
	const server = await createTestServer({
		authorizeWireSubscribe: true,
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
