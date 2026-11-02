import assert from 'node:assert/strict';
import { Client } from 'pg';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';
import { createNotifyBridge } from 'svelte-adapter-uws-extensions/postgres/notify';
import { live, realtime, __register, __registerDerived } from 'svelte-realtime/server';

const { DELIVERY_SCHEMA: schema, DELIVERY_CHANNEL: topic, DELIVERY_BACKEND: backend, DELIVERY_MODE: mode } = process.env;
assert.match(schema, /^delivery_[a-f0-9]+$/);
const authority = new Client({ connectionString: process.env.INTEGRATION_POSTGRES_URL, connectionTimeoutMillis: 5000, query_timeout: 6000, statement_timeout: 5000 });
const pending = [];
let client, bus, published, server;
let parentGone = false, stopRequested = false, initialized = false, stopping;
function send(message) { if (process.connected) process.send(message, () => {}); }
process.once('disconnect', () => {
	parentGone = true;
	setTimeout(() => process.exit(1), 9000).unref();
	void stop(true, 1);
});
const readPayload = async () => (await authority.query('SELECT value FROM ' + schema + '.payload')).rows[0].value;
async function authorize(subject, delivery, expectedRpc) {
	send({ type: 'attempt', subject, kind: delivery.kind, rpc: delivery.rpc, topic: delivery.topic, args: delivery.args, nonce: delivery.data?.nonce });
	if (mode === 'realtime') {
		try {
			assert.equal(delivery.rpc, expectedRpc, 'Delivery must retain this socket\'s admitted RPC identity');
			assert.deepEqual(delivery.args, [topic], 'Delivery must retain its admitted arguments');
			if (expectedRpc === 'security/stats') assert.notEqual(delivery.topic, topic, 'Derived delivery must use an opaque topic');
		} catch (error) {
			send({ type: 'failure', message: error.message, rpc: delivery.rpc, topic: delivery.topic });
			throw error;
		}
	}
	if (delivery.data?.pending && subject === 'revoked') {
		send({ type: 'pending', rpc: delivery.rpc });
		await new Promise(resolve => pending.push(resolve));
	}
	const result = await authority.query('SELECT allowed FROM ' + schema + '.permission WHERE subject=$1', [subject]);
	const allowed = result.rows[0]?.allowed === true;
	send({ type: 'policy', subject, allowed, nonce: delivery.data?.nonce, rpc: delivery.rpc });
	return allowed;
}
let handler = { upgrade: ({ cookies }) => ({ id: cookies.subject }), subscribe() {} };
if (mode === 'realtime') {
	const stream = live.stream((_ctx, name) => name, () => readPayload());
	const derived = live.derived(name => [name], () => readPayload());
	__register('security/feed', stream);
	__register('security/stats', derived);
	__registerDerived('security/stats', derived);
	handler = realtime({
		upgrade: ({ cookies }) => ({ id: cookies.subject, expectedRpc: cookies.expectedRpc }),
		authorizeDelivery: (ctx, delivery) => authorize(ctx.user.id, delivery, ctx.user.expectedRpc)
	});
}
async function initialize() {
	await authority.connect();
	if (parentGone || stopRequested) return;
	server = await createTestServer({ handler, host: '127.0.0.1', upgradeRateLimit: 100 });
	if (parentGone || stopRequested) return;
	assert.equal(typeof server.platform.authorizeDelivery, 'function', 'Packed adapter must provide native delivery authorization');
	if (backend === 'redis') {
		client = createRedisClient({ url: process.env.INTEGRATION_REDIS_URL, autoShutdown: false });
		bus = createPubSubBus(client, { channel: topic, systemChannel: false });
		published = bus.wrap(server.platform);
		assert.equal(typeof published.authorizeDelivery, 'function', 'Redis wrap must forward the capability');
	} else {
		client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false });
		bus = createNotifyBridge(client, { channel: topic });
	}
	if (mode === 'raw') (published || server.platform).authorizeDelivery((ws, delivery) => authorize(ws.getUserData().id, delivery));
	await bus.activate(server.platform);
}
async function stop(removeSchema, code = 0) {
	if (stopping) return stopping;
	stopping = (async () => {
		const watchdog = setTimeout(() => process.exit(1), 9000);
		for (const resolve of pending.splice(0)) resolve();
		const close = async operation => {
			try { await operation(); }
			catch (error) { code = 1; send({ type: 'failure', message: error.message }); }
		};
		await Promise.all([
			(async () => {
				await close(() => bus?.deactivate());
				await close(() => backend === 'redis' ? client?.quit() : client?.end());
			})(),
			close(() => server?.close()),
			(async () => {
				if (removeSchema || parentGone) await close(() => authority.query('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE'));
				await close(() => authority.end());
			})()
		]);
		clearTimeout(watchdog);
		process.exit(code);
	})();
	return stopping;
}
process.on('message', async message => {
	try {
		if (message.type === 'publish') {
			if (backend === 'redis') published.publish(topic, 'set', message.data);
			else await authority.query('SELECT pg_notify($1,$2)', [topic, JSON.stringify({ topic, event: 'set', data: message.data })]);
		} else if (message.type === 'release') {
			for (const resolve of pending.splice(0)) resolve();
		} else if (message.type === 'stop') {
			stopRequested = true;
			if (initialized) await stop(false);
		}
	} catch (error) {
		send({ type: 'failure', message: error.message });
	}
});
try {
	await initialize();
	initialized = true;
	if (parentGone || stopRequested) await stop(parentGone);
	else send({ type: 'ready', wsUrl: server.wsUrl });
} catch (error) {
	send({ type: 'failure', message: error.message });
	await stop(parentGone, 1);
}
