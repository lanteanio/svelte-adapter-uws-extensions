import { createServer } from 'node:net';
import Redis from 'ioredis';
import { it, expect, vi } from 'vitest';
import { createRedisClient } from '../../src/redis/index.js';
import { MAX_REDIS_QUEUED_COMMANDS } from '../../src/shared/caps.js';
import { ConnectionError } from '../../src/shared/errors.js';
import { openSubscriber } from '../../src/shared/redis-subscriber.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

it('bounds startup queue admission and removes rejected unsent commands', async () => {
	const sockets = new Set();
	const server = createServer((socket) => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options: { lazyConnect: true } });
	try {
		expect(client.redis.options).toMatchObject({ maxRetriesPerRequest: 1, commandTimeout: 5000, connectTimeout: 5000, socketTimeout: 5000 });
		const replies = [];
		for (let i = 0; i <= MAX_REDIS_QUEUED_COMMANDS; i++) replies.push(client.redis.set('queued:' + i, 'never').catch((error) => error));
		expect(client.redis.offlineQueue.length).toBe(MAX_REDIS_QUEUED_COMMANDS);
		expect(await replies.at(-1)).toBeInstanceOf(ConnectionError);
		client.redis.disconnect();
		await Promise.all(replies);
		expect(client.redis.offlineQueue.length).toBe(0);
	} finally {
		client.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise((resolve) => server.close(resolve));
	}
});


it.each(['missing', 'changed'])('preserves native deadlines when the private completion seam is %s', async (kind) => {
	const NativeCommand = Redis.Command;
	const sockets = new Set();
	const server = createServer((socket) => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	let client;
	let commandGetter;
	try {
		// The connection still uses ioredis's own Command implementation. Only
		// the exposed private seam changes, as it may in a future release.
		commandGetter = vi.spyOn(Redis, 'Command', 'get').mockReturnValue(kind === 'missing' ? undefined : class extends NativeCommand {
			constructor(...args) {
				super(...args);
				this.resolve = () => this.promise;
			}
		});
		client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options: { lazyConnect: true, commandTimeout: 30 } });
		commandGetter?.mockRestore();
		await expect(client.redis.set('never', 'applied')).rejects.toThrow('Command timed out');
		expect(client.redis.offlineQueue.length).toBe(0);
	} finally {
		commandGetter?.mockRestore();
		client?.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise((resolve) => server.close(resolve));
	}
});


it('rolls back every offline pipeline member when admission reaches capacity', async () => {
	const sockets = new Set();
	const server = createServer(socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options: { lazyConnect: true } });
	const singles = [], callbacks = [vi.fn(), vi.fn(), vi.fn()];
	try {
		for (let i = 0; i < MAX_REDIS_QUEUED_COMMANDS - 1; i++) singles.push(client.redis.ping().catch(error => error));
		const pipeline = client.redis.pipeline();
		callbacks.forEach(callback => pipeline.ping(callback));
		const reply = pipeline.exec();
		expect(client.redis.offlineQueue.length).toBe(MAX_REDIS_QUEUED_COMMANDS - 1);
		expect((await reply).every(([error]) => error instanceof ConnectionError)).toBe(true);
		callbacks.forEach(callback => expect(callback).toHaveBeenCalledTimes(1));
	} finally {
		client.redis.disconnect(); await Promise.all(singles);
		for (const socket of sockets) socket.destroy();
		await new Promise(resolve => server.close(resolve));
	}
});


const CRLF = String.fromCharCode(13, 10);

/** One RESP array of bulk strings at the start of `text`, or null when incomplete. */
function parseCommand(text) {
	let at = text.indexOf(CRLF);
	if (at < 0) return null;
	const count = Number(text.slice(1, at));
	let pos = at + 2;
	const args = [];
	for (let i = 0; i < count; i++) {
		at = text.indexOf(CRLF, pos);
		if (at < 0) return null;
		const len = Number(text.slice(pos + 1, at));
		if (text.length < at + 2 + len + 2) return null;
		args.push(text.slice(at + 2, at + 2 + len));
		pos = at + 2 + len + 2;
	}
	return { args, length: pos };
}

it('leaves a silent subscriber to the liveness watchdog instead of the command deadline', async () => {
	// Answers the handshake and SUBSCRIBE, never a PING: a host that stopped
	// answering without closing the socket.
	const sockets = new Set();
	let pings = 0;
	const server = createServer((socket) => {
		sockets.add(socket);
		socket.on('close', () => sockets.delete(socket));
		socket.on('error', () => {});
		let buffered = '';
		socket.on('data', (chunk) => {
			buffered += chunk.toString('latin1');
			for (let parsed; (parsed = parseCommand(buffered));) {
				buffered = buffered.slice(parsed.length);
				const [name, ...args] = parsed.args;
				if (name.toLowerCase() === 'ping') pings++;
				else if (name.toLowerCase() === 'subscribe') args.forEach((channel, i) => socket.write(['*3', '$9', 'subscribe', '$' + channel.length, channel, ':' + (i + 1), ''].join(CRLF)));
				else socket.write('+OK' + CRLF);
			}
		});
	});
	await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
	const intervals = [];
	const silenceTimers = [];
	setRuntimeEnv({ timers: {
		setInterval: (cb) => { const t = { cb, cleared: false, unref() {} }; intervals.push(t); return t; },
		clearInterval: (t) => { if (t) t.cleared = true; },
		set: (cb, ms, ...args) => {
			if (ms !== 10000) return setTimeout(cb, ms, ...args);
			const t = { cb, cleared: false, unref() {} };
			silenceTimers.push(t);
			return t;
		},
		clear: (t) => { if (t && typeof t === 'object' && 'cleared' in t) t.cleared = true; else clearTimeout(t); }
	} });
	const sink = captureRegistered();
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options: { commandTimeout: 30, socketTimeout: undefined } });
	try {
		const subscriber = openSubscriber(client, 'pubsub');
		await subscriber.subscribe('chan');
		const stream = subscriber.stream;
		for (const t of intervals) if (!t.cleared) t.cb();
		await vi.waitFor(() => expect(pings).toBe(1));
		// Well past the command deadline, the connection is still the one probed.
		await new Promise((resolve) => setTimeout(resolve, 200));
		expect(subscriber.status).toBe('ready');
		expect(subscriber.stream).toBe(stream);
		expect(stream.destroyed).toBe(false);
		expect(sink.ids()).toEqual([]);
		const live = silenceTimers.filter((t) => !t.cleared);
		expect(live).toHaveLength(1);
		live[0].cb();
		await vi.waitFor(() => expect(stream.destroyed).toBe(true));
		expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.REDIS_SUBSCRIBER_SILENT]);
		expect(sink.records[0].attributes).toEqual({ owner: 'pubsub', timeoutMs: 10000 });
	} finally {
		sink.stop();
		client.redis.disconnect();
		for (const dup of [...sockets]) dup.destroy();
		await client.quit();
		resetRuntimeEnv();
		await new Promise((resolve) => server.close(resolve));
	}
});
