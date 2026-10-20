import { createServer, connect } from 'node:net';
import { it, expect, vi } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createRedisClient } from '../../../src/redis/index.js';
import { MAX_REDIS_QUEUED_COMMANDS } from '../../../src/shared/caps.js';
import { createRateLimit } from '../../../src/redis/ratelimit.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/**
 * The server `admin` reaches that holds `key`: on a Cluster the master of the
 * key's slot, so a proxied write lands where it is applied instead of being
 * answered with MOVED; without a key, any master.
 */
function serverOf(admin, key) {
	if (typeof admin.redis.nodes !== 'function') return { host: admin.redis.options.host, port: admin.redis.options.port };
	if (key === undefined) return admin.redis.nodes('master')[0].options;
	const owner = admin.redis.slots[keySlot(key)][0];
	const at = owner.lastIndexOf(':');
	return { host: owner.slice(0, at), port: Number(owner.slice(at + 1)) };
}
async function ready(redis) {
	for (let i = 0; i < 100; i++) { if (redis.status === 'ready') return; await wait(20); }
	throw new Error('Redis did not become ready');
}

it('expires a startup write without sending it after recovery, then fails fast during an outage', async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const key = 'inttest-outage:expired-startup-write';
	const source = serverOf(admin, key);
	const sockets = new Set();
	let blocked = true;
	const buffered = [];
	const server = createServer((socket) => {
		const upstream = connect({ host: source.host, port: source.port });
		sockets.add(socket); sockets.add(upstream);
		upstream.on('error', () => socket.destroy());
		socket.on('error', () => upstream.destroy());
		socket.on('close', () => { sockets.delete(socket); upstream.destroy(); });
		upstream.on('close', () => sockets.delete(upstream));
		upstream.pipe(socket);
		socket.on('data', (chunk) => { if (blocked) buffered.push([upstream, chunk]); else upstream.write(chunk); });
	});
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const sink = captureRegistered();
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false });
	try {
		await admin.redis.del(key);
		const start = Date.now();
		await expect(client.redis.set(key, 'must-not-arrive')).rejects.toThrow(/timed out|retries/i);
		expect(Date.now() - start).toBeLessThan(6500);
		// A reconnect handshake may queue INFO after the expired user write.
		// Only the unsent write must disappear, and Redis must never apply it.
		expect(client.redis.offlineQueue.toArray().some(entry => entry.command.name === 'set' && entry.command.args[0] === key)).toBe(false);
		blocked = false;
		for (const [upstream, chunk] of buffered) if (!upstream.destroyed) upstream.write(chunk);
		await ready(client.redis);
		expect(await admin.redis.get(key)).toBe(null);
		expect(client.redis.options.enableOfflineQueue).toBe(false);
		const closed = new Promise((resolve) => client.redis.once('close', resolve));
		blocked = true;
		for (const socket of sockets) socket.destroy();
		await closed;
		expect(client.redis.status).not.toBe('ready');
		const rejectedAt = Date.now();
		await expect(client.redis.set(key, 'also-not-arriving')).rejects.toThrow();
		expect(Date.now() - rejectedAt).toBeLessThan(500);
		expect(client.redis.offlineQueue.toArray().some(entry => entry.command.name === 'set' && entry.command.args[0] === key)).toBe(false);
	} finally {
		client.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise((resolve) => server.close(resolve));
		await admin.quit();
		sink.stop();
	}
});

it('reaches the configured rate-limit floor and breaker within the retry budget', async () => {
	const server = createServer();
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const port = server.address().port;
	await new Promise((resolve) => server.close(resolve));
	const sink = captureRegistered();
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + port, autoShutdown: false });
	const breaker = createCircuitBreaker({ failureThreshold: 1 });
	try {
		const limiter = createRateLimit(client, { points: 1, interval: 60000, breaker, localFloorOnStorageFailure: true });
		const start = Date.now();
		expect((await limiter.consume({ ip: '127.0.0.1' })).allowed).toBe(true);
		expect(Date.now() - start).toBeLessThan(1000);
		expect(breaker.isHealthy).toBe(false);
		expect((await limiter.consume({ ip: '127.0.0.1' })).allowed).toBe(false);
		expect(client.redis.offlineQueue.length).toBe(0);
	} finally { client.redis.disconnect(); breaker.destroy(); sink.stop(); }
});

it('keeps an individual command deadline despite unrelated pubsub traffic', async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = typeof admin.redis.nodes === 'function' ? admin.redis.nodes('master')[0] : admin.redis;
	const client = createRedisClient({ url: 'redis://' + source.options.host + ':' + source.options.port, autoShutdown: false, options: { commandTimeout: 150 } });
	const topic = 'inttest-outage:deadline-traffic';
	let timer;
	try {
		await client.redis.subscribe(topic);
		let messages = 0;
		client.redis.on('message', () => { messages++; });
		const write = client.redis.stream.write.bind(client.redis.stream);
		client.redis.stream.write = (chunk, ...args) => chunk.toString().toLowerCase().includes('\r\nping\r\n') ? true : write(chunk, ...args);
		timer = setInterval(() => { void admin.redis.publish(topic, 'traffic'); }, 10);
		const started = Date.now();
		const closed = new Promise(resolve => client.redis.once('close', resolve));
		await expect(client.redis.ping()).rejects.toThrow(/timed out/);
		await closed;
		expect(Date.now() - started).toBeLessThan(500);
		expect(messages).toBeGreaterThan(2);
		expect(client.redis.status).not.toBe('ready');
		expect(client.redis.commandQueue.length).toBe(0);
	} finally {
		clearInterval(timer);
		client.redis.disconnect();
		await admin.quit();
	}
});


it('preserves explicit buffering and dedicated subscriber overrides', async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = typeof admin.redis.nodes === 'function' ? admin.redis.nodes('master')[0] : admin.redis;
	const client = createRedisClient({ url: 'redis://' + source.options.host + ':' + source.options.port, autoShutdown: false, options: { enableOfflineQueue: true } });
	let subscriber;
	try {
		await client.redis.ping();
		expect(client.redis.options.enableOfflineQueue).toBe(true);
		subscriber = client.duplicate({ maxRetriesPerRequest: null, commandTimeout: undefined, socketTimeout: undefined });
		await subscriber.subscribe('inttest-outage:override');
		expect(subscriber.options).toMatchObject({ enableOfflineQueue: true, maxRetriesPerRequest: null });
		expect(subscriber.options.commandTimeout).toBeUndefined();
		expect(subscriber.options.socketTimeout).toBeUndefined();
	} finally { subscriber?.disconnect(); await client.quit(); await admin.quit(); }
});


async function proxyClient(options = {}, key) {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = serverOf(admin, key);
	const sockets = new Set(), buffered = [];
	const gate = { outbound: false, inbound: false };
	const server = createServer(socket => {
		const upstream = connect({ host: source.host, port: source.port });
		sockets.add(socket); sockets.add(upstream);
		upstream.on('error', () => socket.destroy()); socket.on('error', () => upstream.destroy());
		socket.on('close', () => { sockets.delete(socket); upstream.destroy(); });
		upstream.on('close', () => sockets.delete(upstream));
		socket.on('data', data => { if (gate.outbound) buffered.push([upstream, data]); else upstream.write(data); });
		upstream.on('data', data => { if (!gate.inbound) socket.write(data); });
	});
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options });
	return { admin, client, gate,
		unblock() { gate.outbound = false; for (const [socket, data] of buffered.splice(0)) if (!socket.destroyed) socket.write(data); },
		async close() { client.redis.disconnect(); for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); await admin.quit(); }
	};
}

it('refuses an entire offline pipeline that straddles capacity, preserving later reply alignment', async () => {
	const keys = ['inttest-pipeline:{capacity}:a', 'inttest-pipeline:{capacity}:b', 'inttest-pipeline:{capacity}:c'];
	const fixture = await proxyClient({ lazyConnect: true }, keys[0]);
	const { client, admin, gate } = fixture;
	gate.outbound = true;
	const callbacks = keys.map(() => vi.fn());
	try {
		for (const key of keys) await admin.redis.del(key);
		const singles = Array.from({ length: MAX_REDIS_QUEUED_COMMANDS - 1 }, () => client.redis.ping());
		const pipeline = client.redis.pipeline();
		keys.forEach((key, i) => pipeline.set(key, 'never', callbacks[i]));
		const result = await pipeline.exec();
		expect(result.every(([error]) => error?.name === 'ConnectionError')).toBe(true);
		expect(client.redis.offlineQueue.length).toBe(MAX_REDIS_QUEUED_COMMANDS - 1);
		callbacks.forEach(callback => expect(callback).toHaveBeenCalledTimes(1));
		fixture.unblock();
		expect(await Promise.all(singles)).toEqual(singles.map(() => 'PONG'));
		for (const key of keys) expect(await admin.redis.get(key)).toBeNull();
		expect(await client.redis.echo('after-pipeline')).toBe('after-pipeline');
	} finally { await fixture.close(); }
});

it('cancels all unsent pipeline members when one native deadline expires', async () => {
	const keys = ['inttest-pipeline:{expiry}:a', 'inttest-pipeline:{expiry}:b'];
	const fixture = await proxyClient({ lazyConnect: true }, keys[0]);
	const { client, admin, gate } = fixture;
	gate.outbound = true;
	const callbacks = keys.map(() => vi.fn());
	try {
		for (const key of keys) await admin.redis.del(key);
		const pipeline = client.redis.pipeline();
		keys.forEach((key, i) => pipeline.set(key, 'never', callbacks[i]));
		pipeline._queue[0].setTimeout(50);
		const started = Date.now();
		const result = await pipeline.exec();
		expect(Date.now() - started).toBeLessThan(1000);
		expect(result.every(([error]) => /timed out/i.test(error?.message))).toBe(true);
		expect(client.redis.offlineQueue.length).toBe(0);
		callbacks.forEach(callback => expect(callback).toHaveBeenCalledTimes(1));
		fixture.unblock(); await ready(client.redis);
		for (const key of keys) expect(await admin.redis.get(key)).toBeNull();
		expect(await client.redis.echo('after-expiry')).toBe('after-expiry');
	} finally { await fixture.close(); }
});

it('retires a timed-out WAIT without replaying it on reconnect', async () => {
	const fixture = await proxyClient({ commandTimeout: 150, socketTimeout: undefined });
	const { client } = fixture;
	try {
		await client.redis.ping();
		let retired = false;
		const closed = new Promise(resolve => client.redis.once('close', () => { retired = true; resolve(); }));
		await expect(client.redis.wait(99, 0)).rejects.toMatchObject({ code: 'OPERATION_TIMEOUT', ms: 150 });
		await vi.waitFor(() => expect(retired).toBe(true), { timeout: 500 });
		await closed; await ready(client.redis);
		expect(await client.redis.ping()).toBe('PONG');
	} finally { await fixture.close(); }
});

it.each([false, true])('does not resend a timed-out applied write (already reconnecting: %s)', async reconnecting => {
	const key = 'inttest-outage:ambiguous-write';
	const fixture = await proxyClient({ commandTimeout: 150, socketTimeout: undefined, retryStrategy: () => 300 }, key);
	const { client, admin, gate } = fixture;
	try {
		await admin.redis.del(key); await client.redis.ping();
		gate.inbound = true;
		let retired = false;
		const closed = new Promise(resolve => client.redis.once('close', () => { retired = true; resolve(); }));
		const result = client.redis.incr(key).catch(error => error);
		await vi.waitFor(async () => expect(await admin.redis.get(key)).toBe('1'));
		if (reconnecting) { client.redis.stream.destroy(); await closed; }
		expect(await result).toMatchObject(reconnecting ? { name: 'ConnectionError' } : { code: 'OPERATION_TIMEOUT', ms: 150 });
		await vi.waitFor(() => expect(retired).toBe(true), { timeout: 500 });
		gate.inbound = false;
		await closed; await ready(client.redis);
		expect(await client.redis.ping()).toBe('PONG');
		expect(await admin.redis.get(key)).toBe('1');
	} finally { await admin.redis.del(key); await fixture.close(); }
});

it.each(['reset', 'socket deadline'])('does not resend an applied write after %s', async cause => {
	const key = 'inttest-outage:reset-applied-write';
	const fixture = await proxyClient(cause === 'reset' ? {} : { commandTimeout: 1000, socketTimeout: 100 }, key);
	const { client, admin, gate } = fixture;
	try {
		await admin.redis.del(key);
		await client.redis.ping();
		gate.inbound = true;
		const result = client.redis.incr(key).catch(error => error);
		await vi.waitFor(async () => expect(await admin.redis.get(key)).toBe('1'));
		if (cause === 'reset') client.redis.stream.destroy();
		expect(await result).toMatchObject({ name: 'ConnectionError' });
		gate.inbound = false;
		await ready(client.redis);
		expect(await client.redis.ping()).toBe('PONG');
		expect(await admin.redis.get(key)).toBe('1');
	} finally { await admin.redis.del(key); await fixture.close(); }
});

it('keeps an unsent startup write when the connection drops during the handshake', async () => {
	const fixture = await proxyClient({ lazyConnect: true, disableClientInfo: true, retryStrategy: () => 50 });
	const { client, admin, gate } = fixture;
	const key = 'inttest-outage:handshake-drop';
	const sink = captureRegistered();
	try {
		await admin.redis.del(key);
		gate.inbound = true;
		client.redis.connect().catch(() => {});
		// The INFO ready check is sent and unanswered; the write waits unsent.
		await vi.waitFor(() => {
			expect(client.redis.status).toBe('connect');
			expect(client.redis.commandQueue.toArray().some(entry => entry.command.name === 'info')).toBe(true);
		}, { timeout: 2000 });
		const write = client.redis.set(key, 'after-handshake');
		expect(client.redis.offlineQueue.toArray().some(entry => entry.command.name === 'set')).toBe(true);
		const closed = new Promise(resolve => client.redis.once('close', resolve));
		client.redis.stream.destroy();
		await closed;
		gate.inbound = false;
		expect(await write).toBe('OK');
		expect(await admin.redis.get(key)).toBe('after-handshake');
		expect(JSON.stringify(sink.records)).not.toMatch(/uncertain/);
	} finally { sink.stop(); await admin.redis.del(key); await fixture.close(); }
});

it('rejects a command written during the handshake when that connection drops before ready', async () => {
	const fixture = await proxyClient({ lazyConnect: true, disableClientInfo: true, retryStrategy: () => 50 });
	const { client, admin, gate } = fixture;
	const key = 'inttest-outage:handshake-written';
	const subscriber = admin.duplicate();
	const received = [];
	try {
		await admin.redis.del(key);
		await subscriber.subscribe('inttest-outage:handshake-channel');
		subscriber.on('message', (_channel, message) => received.push(message));
		gate.inbound = true;
		client.redis.connect().catch(() => {});
		await vi.waitFor(() => {
			expect(client.redis.status).toBe('connect');
			expect(client.redis.commandQueue.toArray().some(entry => entry.command.name === 'info')).toBe(true);
		}, { timeout: 2000 });
		// PUBLISH is valid while Redis loads, so ioredis writes it before ready;
		// SET waits in the offline queue.
		const publish = client.redis.publish('inttest-outage:handshake-channel', 'during-handshake');
		const write = client.redis.set(key, 'after-handshake');
		publish.catch(() => {});
		await vi.waitFor(() => expect(received).toEqual(['during-handshake']), { timeout: 2000 });
		expect(client.redis.commandQueue.toArray().some(entry => entry.command.name === 'publish')).toBe(true);
		const closed = new Promise(resolve => client.redis.once('close', resolve));
		const droppedAt = Date.now();
		client.redis.stream.destroy();
		await closed;
		gate.inbound = false;
		const error = await publish.then(() => null, error => error);
		expect(error?.name).toBe('ConnectionError');
		expect(error.message).toMatch(/uncertain/);
		expect(Date.now() - droppedAt).toBeLessThan(1000);
		expect(await write).toBe('OK');
		expect(client.redis.status).toBe('ready');
	} finally { subscriber.disconnect(); await admin.redis.del(key); await fixture.close(); }
});
