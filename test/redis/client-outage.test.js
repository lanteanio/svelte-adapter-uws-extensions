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
	const client = createRedisClient({ url: 'redis://127.0.0.1:' + server.address().port, autoShutdown: false, options: { commandTimeout: 250, socketTimeout: undefined } });
	try {
		const subscriber = openSubscriber(client, 'pubsub');
		await subscriber.subscribe('chan');
		const stream = subscriber.stream;
		for (const t of intervals) if (!t.cleared) t.cb();
		await vi.waitFor(() => expect(pings).toBe(1));
		// Well past the command deadline, the connection is still the one probed.
		await new Promise((resolve) => setTimeout(resolve, 750));
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

// Answers every command until it reads the first INFO, then stays silent, so
// the ready check and everything after it remain unanswered.
function quietAfterReadyCheck(sockets) {
	return createServer((socket) => {
		sockets.add(socket);
		socket.on('close', () => sockets.delete(socket));
		socket.on('error', () => {});
		let buffer = Buffer.alloc(0);
		let silent = false;
		socket.on('data', (chunk) => {
			buffer = Buffer.concat([buffer, chunk]);
			for (;;) {
				const parts = [];
				let at = buffer.indexOf('\r\n');
				if (buffer[0] !== 0x2a || at < 0) return;
				const count = Number(buffer.subarray(1, at));
				let offset = at + 2;
				for (let i = 0; i < count; i++) {
					at = buffer.indexOf('\r\n', offset);
					if (at < 0) return;
					const length = Number(buffer.subarray(offset + 1, at));
					if (buffer.length < at + 2 + length + 2) return;
					parts.push(buffer.subarray(at + 2, at + 2 + length).toString());
					offset = at + 2 + length + 2;
				}
				buffer = buffer.subarray(offset);
				if (parts[0].toLowerCase() === 'info') silent = true;
				if (!silent) socket.write('+OK\r\n');
			}
		});
	});
}

it('rejects user commands named like handshake commands when the connection drops before ready', async () => {
	const sockets = new Set();
	const server = quietAfterReadyCheck(sockets);
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({
		url: 'redis://127.0.0.1:' + server.address().port,
		autoShutdown: false,
		options: { lazyConnect: true, password: 'secret', db: 2, connectionName: 'handshake', retryStrategy: () => 60000 }
	});
	const outcome = (promise) => promise.then(() => 'resolved', error => error);
	try {
		client.redis.connect().catch(() => {});
		await vi.waitFor(() => {
			expect(client.redis.status).toBe('connect');
			expect(client.redis.commandQueue.toArray().some(entry => entry.command.name === 'info')).toBe(true);
		}, { timeout: 2000 });
		const user = [
			client.redis.info(),
			client.redis.select(2),
			client.redis.client('setname', 'handshake'),
			client.redis.auth('secret')
		].map(outcome);
		outcome(client.redis.set('handshake:key', 'unsent'));
		expect(client.redis.commandQueue.toArray().filter(entry => entry.command.name === 'info')).toHaveLength(2);
		const closed = new Promise(resolve => client.redis.once('close', resolve));
		client.redis.stream.destroy();
		await closed;
		const settled = await Promise.race([Promise.all(user), new Promise(resolve => setTimeout(resolve, 1000, 'pending'))]);
		expect(settled).not.toBe('pending');
		for (const error of settled) {
			expect(error).toBeInstanceOf(ConnectionError);
			expect(error.message).toMatch(/uncertain/);
		}
		// The ready check stays with ioredis, so the unsent write is kept for
		// the next connection instead of being flushed as a handshake failure.
		expect(client.redis.commandQueue.toArray().map(entry => entry.command.name)).toEqual(['info']);
		expect(client.redis.offlineQueue.toArray().map(entry => entry.command.name)).toEqual(['set']);
		expect(client.redis.status).toBe('reconnecting');
	} finally {
		client.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise(resolve => server.close(resolve));
	}
});

it.each(['client', 'duplicate'])('disconnect() between reconnect attempts ends the %s connection and rejects its queue', async (kind) => {
	const server = createServer(socket => { socket.destroy(); });
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({
		url: 'redis://127.0.0.1:' + server.address().port,
		autoShutdown: false,
		options: { lazyConnect: true, retryStrategy: () => 60000, commandTimeout: 60000 }
	});
	const connection = kind === 'client' ? client.redis : client.duplicate({ lazyConnect: true });
	try {
		connection.connect().catch(() => {});
		const queued = connection.get('between-attempts').then(() => 'resolved', error => error);
		await vi.waitFor(() => expect(connection.status).toBe('reconnecting'), { timeout: 2000 });
		const ended = new Promise(resolve => connection.once('end', () => resolve('end')));
		connection.disconnect();
		const pending = new Promise(resolve => setTimeout(resolve, 500, 'pending'));
		expect(await Promise.race([ended, pending])).toBe('end');
		expect(connection.status).toBe('end');
		const error = await Promise.race([queued, pending]);
		expect(error).toBeInstanceOf(Error);
		expect(error.message).toMatch(/closed/i);
		await client.quit();
	} finally {
		connection.disconnect();
		client.redis.disconnect();
		await new Promise(resolve => server.close(resolve));
	}
});


it('rejects a user CLIENT SETINFO sent before the handshake one when the connection drops before ready', async () => {
	// The server never replies, so every command written before ready stays
	// unanswered. ioredis sends its LIB-VER only after reading its package
	// version, later than the 'connect' event, so the application's identical
	// command goes out first.
	const sockets = new Set();
	const server = createServer(socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); socket.on('error', () => {}); });
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({
		url: 'redis://127.0.0.1:' + server.address().port,
		autoShutdown: false,
		options: { lazyConnect: true, password: 'secret', db: 2, connectionName: 'handshake', retryStrategy: () => 60000, commandTimeout: 60000 }
	});
	const outcome = (promise) => promise.then(() => 'resolved', error => error);
	const sent = () => client.redis.commandQueue.toArray().map(entry => [entry.command.name, ...entry.command.args.map(String)].join(' ').toLowerCase());
	const user = [];
	// A listener prepended after creation runs first on the 'connect' event.
	client.redis.prependOnceListener('connect', () => { user.push(outcome(client.redis.info())); });
	client.redis.once('connect', () => {
		user.push(outcome(client.redis.client('SETINFO', 'LIB-VER', 'app')));
		user.push(outcome(client.redis.auth('secret')));
		user.push(outcome(client.redis.select(2)));
		user.push(outcome(client.redis.client('SETNAME', 'handshake')));
	});
	try {
		client.redis.connect().catch(() => {});
		await vi.waitFor(() => {
			expect(sent().filter(line => line.startsWith('client setinfo lib-ver'))).toHaveLength(2);
		}, { timeout: 2000 });
		expect(sent()[0]).toBe('auth secret');
		expect(sent()).toContain('info');
		expect(sent().indexOf('client setinfo lib-ver app')).toBeLessThan(sent().findIndex(line => line.startsWith('client setinfo lib-ver') && !line.endsWith(' app')));
		const closed = new Promise(resolve => client.redis.once('close', resolve));
		client.redis.stream.destroy();
		await closed;
		const settled = await Promise.race([Promise.all(user), new Promise(resolve => setTimeout(resolve, 1000, 'pending'))]);
		expect(settled).not.toBe('pending');
		for (const error of settled) {
			expect(error).toBeInstanceOf(ConnectionError);
			expect(error.message).toMatch(/uncertain/);
		}
		// ioredis ignores the outcome of its own LIB-VER, so it is rejected with
		// the application commands; the rest of its handshake stays with it.
		expect(sent()).toEqual(['auth secret', 'select 2', 'client setname handshake', 'client setinfo lib-name ioredis']);
	} finally {
		client.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise(resolve => server.close(resolve));
	}
});


it('does not run a ready check for a connection that closed during its handshake', async () => {
	// Connection 1 reaches ready and is dropped. Connection 2 answers the
	// handshake up to CLIENT SETINFO LIB-NAME and ends on LIB-VER, so the
	// rejected LIB-VER completes ioredis's CLIENT commands after the close.
	const info = 'redis_version:7.4.0\r\nloading:0\r\nrole:master\r\n';
	const sockets = new Set();
	let connections = 0;
	const server = createServer(socket => {
		const id = ++connections;
		sockets.add(socket);
		socket.on('close', () => sockets.delete(socket));
		socket.on('error', () => {});
		let buffer = Buffer.alloc(0);
		socket.on('data', chunk => {
			buffer = Buffer.concat([buffer, chunk]);
			let reply = '';
			for (;;) {
				const parts = [];
				let at = buffer.indexOf('\r\n');
				if (buffer[0] !== 0x2a || at < 0) break;
				const count = Number(buffer.subarray(1, at));
				let offset = at + 2;
				let complete = true;
				for (let i = 0; i < count; i++) {
					at = buffer.indexOf('\r\n', offset);
					const length = at < 0 ? 0 : Number(buffer.subarray(offset + 1, at));
					if (at < 0 || buffer.length < at + 2 + length + 2) { complete = false; break; }
					parts.push(buffer.subarray(at + 2, at + 2 + length).toString());
					offset = at + 2 + length + 2;
				}
				if (!complete) break;
				buffer = buffer.subarray(offset);
				const line = parts.join(' ').toLowerCase();
				if (id === 2 && line.startsWith('client setinfo lib-ver')) { socket.end(reply); return; }
				reply += line === 'info' ? '$' + info.length + '\r\n' + info + '\r\n' : '+OK\r\n';
			}
			if (reply) socket.write(reply);
		});
	});
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const client = createRedisClient({
		url: 'redis://127.0.0.1:' + server.address().port,
		autoShutdown: false,
		options: { lazyConnect: true, maxRetriesPerRequest: 5, retryStrategy: attempt => (attempt > 3 ? null : 50) }
	});
	const errors = [];
	client.redis.on('error', error => errors.push(error.message));
	try {
		await client.redis.connect();
		for (const socket of sockets) socket.destroy();
		await vi.waitFor(() => {
			expect(connections).toBeGreaterThanOrEqual(3);
			expect(client.redis.status).toBe('ready');
		}, { timeout: 2000 });
		await new Promise(resolve => setTimeout(resolve, 100));
		expect(errors).toEqual([]);
	} finally {
		client.redis.disconnect();
		for (const socket of sockets) socket.destroy();
		await new Promise(resolve => server.close(resolve));
	}
});
