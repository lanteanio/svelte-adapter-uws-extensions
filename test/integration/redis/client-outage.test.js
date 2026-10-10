import { createServer, connect } from 'node:net';
import { it, expect } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createRedisClient } from '../../../src/redis/index.js';
import { createRateLimit } from '../../../src/redis/ratelimit.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function ready(redis) {
	for (let i = 0; i < 100; i++) { if (redis.status === 'ready') return; await wait(20); }
	throw new Error('Redis did not become ready');
}

it('expires a startup write without sending it after recovery, then fails fast during an outage', async () => {
	const admin = createBackendClient();
	await admin.redis.ping();
	const source = typeof admin.redis.nodes === 'function' ? admin.redis.nodes('master')[0] : admin.redis;
	const sockets = new Set();
	let blocked = true;
	const buffered = [];
	const server = createServer((socket) => {
		const upstream = connect({ host: source.options.host, port: source.options.port });
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
	const key = 'inttest-outage:expired-startup-write';
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
		await expect(client.redis.ping()).rejects.toThrow(/timed out/);
		expect(Date.now() - started).toBeLessThan(500);
		expect(messages).toBeGreaterThan(2);
		expect(client.redis.status).toBe('ready');
		// Sent-command slots stay in protocol order even after caller timeout.
		expect(client.redis.commandQueue.length).toBe(1);
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
