// One server instance of the leak lane's Redis-backed deployment.
//
// Forked by scripts/leak-lane.js, one process per instance, so each instance
// has its own resident set, handles and Redis connections to measure. The
// instances reach each other only through Redis, exactly as separate hosts or
// separate workers do: the pub/sub bus relays every publish, presence and the
// group keep their membership in Redis, and replay keeps one buffer per topic.
//
// The parent drives the WebSocket side and talks to this process over IPC:
//
//   { op: 'mem' }    resident set, heap, live connections, handles, listeners
//   { op: 'gc' }     a forced collection (the child runs with --expose-gc)
//   { op: 'arm' }    the self-check instance starts leaking (see below)
//   { op: 'stop' }   tear everything down and exit
//
// The self-check instance (LEAK_LANE_ROLE=selfcheck) is an ordinary member of
// the deployment until it is armed. From then on every connection it closes
// leaves three things behind: a 64 KiB buffer, a listener on its Redis
// connection and a Redis key with no expiry. Each is what one of the lane's
// gates exists to catch, and the lane requires each gate to catch it.

import { createTestServer } from 'svelte-adapter-uws/testing';
import { createBackendClient } from '../../test/integration/helpers/backend.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createPresence } from '../../src/redis/presence.js';
import { createReplay } from '../../src/redis/replay.js';
import { createGroup } from '../../src/redis/groups.js';

const role = process.env.LEAK_LANE_ROLE === 'selfcheck' ? 'selfcheck' : 'instance';
const keyPrefix = process.env.LEAK_LANE_KEY_PREFIX || 'leak-lane:';
const plantedPrefix = process.env.LEAK_LANE_PLANTED_PREFIX || 'leak-lane-planted:';
const channel = keyPrefix + 'bus';

const client = createBackendClient({ keyPrefix });
/** Every Redis connection this instance opens, so the listener probe covers the subscribers too. */
const connections = new Set();
connections.add(client.redis);
const duplicate = client.duplicate.bind(client);
client.duplicate = (...args) => {
	const conn = duplicate(...args);
	connections.add(conn);
	conn.once('end', () => connections.delete(conn));
	return conn;
};

const bus = createPubSubBus(client, { channel });
const presence = createPresence(client, { ttl: 90 });
const replay = createReplay(client, { size: 64 });
const group = createGroup(client, 'room');

let armed = false;
/** @type {Buffer[]} */
const retained = [];
let planted = 0;
const noop = () => {};

const decoder = new TextDecoder();

/** @type {import('svelte-adapter-uws/testing').TestServer} */
let server;
/** @type {import('svelte-adapter-uws').Platform} */
let relayed;

server = await createTestServer({
	handler: {
		upgrade({ url }) {
			const id = new URL(url, 'http://local').searchParams.get('u');
			return id ? { id } : false;
		},
		async open(ws, ctx) {
			await bus.hooks.open(ws, ctx);
			await group.join(ws, ctx.platform);
		},
		async subscribe(ws, topic, ctx) {
			return presence.hooks.subscribe(ws, topic, ctx);
		},
		async unsubscribe(ws, topic, ctx) {
			await presence.hooks.unsubscribe(ws, topic, ctx);
		},
		message(ws, ctx) {
			if (presence.hooks.message(ws, ctx)) return;
			let msg;
			try {
				msg = JSON.parse(typeof ctx.data === 'string' ? ctx.data : decoder.decode(ctx.data));
			} catch {
				return;
			}
			if (msg && msg.type === 'publish' && typeof msg.topic === 'string') {
				replay.publish(relayed, msg.topic, 'tick', msg.data).catch(noop);
			}
		},
		async close(ws, ctx) {
			await presence.hooks.close(ws, ctx);
			await group.leave(ws, ctx.platform);
			if (armed) {
				retained.push(Buffer.alloc(64 * 1024, 1));
				client.redis.on('ready', noop);
				const key = plantedPrefix + process.pid + ':' + planted++;
				client.redis.set(key, '1').catch(noop);
			}
		}
	}
});
relayed = bus.wrap(server.platform);
await bus.activate(server.platform);

/** Listeners registered on every Redis connection this instance holds. */
function listenerCount() {
	let total = 0;
	for (const conn of connections) {
		for (const name of conn.eventNames()) total += conn.listenerCount(name);
	}
	return total;
}

function mem() {
	const usage = process.memoryUsage();
	return {
		rss: usage.rss,
		heapUsed: usage.heapUsed,
		external: usage.external,
		connections: server.wsConnections.size,
		handles: process.getActiveResourcesInfo().length,
		listeners: listenerCount(),
		retainedBuffers: retained.length
	};
}

async function stop() {
	await bus.deactivate().catch(noop);
	presence.destroy();
	group.destroy();
	await server.close().catch(noop);
	await client.quit().catch(noop);
}

process.on('message', async (msg) => {
	if (!msg || typeof msg !== 'object') return;
	const reply = (body) => process.send({ id: msg.id, ...body });
	try {
		if (msg.op === 'mem') reply(mem());
		else if (msg.op === 'gc') {
			if (typeof globalThis.gc !== 'function') reply({ collected: false, reason: 'the instance was started without --expose-gc' });
			else {
				globalThis.gc();
				reply({ collected: true });
			}
		} else if (msg.op === 'arm') {
			armed = role === 'selfcheck';
			reply({ armed });
		} else if (msg.op === 'stop') {
			await stop();
			reply({ stopped: true });
			process.disconnect();
		} else reply({ error: 'unknown op ' + msg.op });
	} catch (err) {
		reply({ error: err && err.message ? err.message : String(err) });
	}
});

process.send({ ready: true, port: server.port, role, pid: process.pid });
