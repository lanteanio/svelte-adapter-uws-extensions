import { createServer, connect } from 'node:net';
import { describe, it, expect } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createRedisClient } from '../../../src/redis/index.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { closeConnection } from '../../../src/shared/redis-subscriber.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const RETRY_MS = 500;

async function within(promise, ms = 5000) {
	let timer;
	try {
		return await Promise.race([
			promise,
			new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('shutdown did not settle')), ms); })
		]);
	} finally { clearTimeout(timer); }
}

const settled = promise => promise.then(() => 'resolved', error => 'rejected: ' + error.message);

/** The server connection that owns `key`: the standalone server, or its master in a cluster. */
function ownerOf(admin, key) {
	if (!isClusterBackend()) return admin.redis;
	const owner = admin.redis.slots[keySlot(key)][0];
	const port = Number(owner.slice(owner.lastIndexOf(':') + 1));
	return admin.redis.nodes('master').find(node => node.options.port === port);
}

/**
 * A TCP relay to `target` that can stop accepting. While it refuses, every
 * reconnect attempt fails at once, so a connection stays offline with its
 * queue for as long as the test needs, however slowly the host runs; once it
 * accepts again, an attempt that should not happen would reach the server.
 */
async function relay(target) {
	const sockets = new Set();
	const server = createServer(socket => {
		const upstream = connect({ host: target.host, port: target.port });
		sockets.add(socket); sockets.add(upstream);
		socket.on('error', () => upstream.destroy()); upstream.on('error', () => socket.destroy());
		socket.on('close', () => { sockets.delete(socket); upstream.destroy(); });
		upstream.on('close', () => { sockets.delete(upstream); socket.destroy(); });
		socket.pipe(upstream); upstream.pipe(socket);
	});
	await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
	const { port } = server.address();
	return {
		port,
		refuse: () => { server.close(); },
		accept: () => new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); }),
		close: () => { for (const socket of sockets) socket.destroy(); server.close(); }
	};
}

describe('Redis client terminal shutdown', () => {
	it('rejects queued commands after real socket loss without replaying writes or opening replacement subscribers', async () => {
		const admin = createBackendClient({ keyPrefix: 'inttest-client-quit:' });
		await admin.redis.ping();
		const name = 'quit-' + Date.now();
		const key = admin.key(name);
		const server = ownerOf(admin, key);
		const proxy = await relay(server.options);
		const client = createRedisClient({
			url: 'redis://127.0.0.1:' + proxy.port,
			autoShutdown: false,
			options: {
				connectionName: name, enableOfflineQueue: true,
				maxRetriesPerRequest: null, retryStrategy: () => RETRY_MS,
				commandTimeout: undefined, socketTimeout: undefined
			}
		});
		const duplicates = [];
		const duplicate = client.duplicate;
		client.duplicate = options => { const connection = duplicate(options); duplicates.push(connection); return connection; };
		const bus = createPubSubBus(client, { channel: 'inttest-client-quit:channel' });
		try {
			await bus.activate(mockPlatform());
			expect(duplicates).toHaveLength(1);
			const ids = String(await server.call('CLIENT', 'LIST')).split('\n')
				.filter(row => row.includes('name=' + name + ' '))
				.map(row => /^id=(\d+)/.exec(row)[1]);
			expect(ids).toHaveLength(2);
			proxy.refuse();
			for (const id of ids) await server.call('CLIENT', 'KILL', 'ID', id);
			await expect.poll(() => [client.redis, ...duplicates].every(connection => connection.status === 'reconnecting'), { timeout: 10_000 }).toBe(true);
			const write = settled(client.redis.set(key, 'must-not-run'));
			const subscribe = settled(duplicates[0].subscribe('inttest-client-quit:queued'));
			expect(client.redis.offlineQueue.length).toBeGreaterThan(0);
			expect(duplicates[0].offlineQueue.length).toBeGreaterThan(0);

			await within(client.quit());

			expect(await within(write)).toBe('rejected: Connection is closed.');
			expect(await within(subscribe)).toBe('rejected: Connection is closed.');
			// A reconnect that quit() left scheduled would now reach the server
			// and send the queued write.
			await proxy.accept();
			await new Promise(resolve => setTimeout(resolve, RETRY_MS * 3));
			expect(await admin.redis.get(key)).toBeNull();
			expect(String(await server.call('CLIENT', 'LIST'))).not.toContain('name=' + name + ' ');
			expect(duplicates).toHaveLength(1);
			for (const connection of [client.redis, ...duplicates]) expect(connection.status).toBe('end');
		} finally {
			for (const connection of [client.redis, ...duplicates]) closeConnection(connection);
			await bus.deactivate();
			proxy.close();
			await admin.redis.del(key);
			await admin.quit();
		}
	});
});
