import { createServer } from 'node:net';
import Redis from 'ioredis';
import { it, expect, vi } from 'vitest';
import { createRedisClient } from '../../src/redis/index.js';
import { MAX_REDIS_QUEUED_COMMANDS } from '../../src/shared/caps.js';
import { ConnectionError } from '../../src/shared/errors.js';

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
