import { describe, it, expect } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createRedisClient } from '../../../src/redis/index.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { closeConnection } from '../../../src/shared/redis-subscriber.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

async function within(promise, ms = 1000) {
	let timer;
	try {
		return await Promise.race([
			promise,
			new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('shutdown did not settle')), ms); })
		]);
	} finally { clearTimeout(timer); }
}

const settled = promise => promise.then(() => 'resolved', error => 'rejected: ' + error.message);

describe('Redis client terminal shutdown', () => {
	it('rejects queued commands after real socket loss without replaying writes or opening replacement subscribers', async () => {
		const admin = createBackendClient({ keyPrefix: 'inttest-client-quit:' });
		await admin.redis.ping();
		const server = isClusterBackend() ? admin.redis.nodes('master')[0] : admin.redis;
		const name = 'quit-' + Date.now();
		const key = admin.key(name);
		const client = createRedisClient({
			url: 'redis://' + server.options.host + ':' + server.options.port,
			autoShutdown: false,
			options: {
				connectionName: name, enableOfflineQueue: true,
				maxRetriesPerRequest: null, retryStrategy: () => 500,
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
			for (const id of ids) await server.call('CLIENT', 'KILL', 'ID', id);
			await expect.poll(() => [client.redis, ...duplicates].every(connection => connection.status === 'reconnecting')).toBe(true);
			const write = settled(client.redis.set(key, 'must-not-run'));
			const subscribe = settled(duplicates[0].subscribe('inttest-client-quit:queued'));
			expect(client.redis.offlineQueue.length).toBeGreaterThan(0);
			expect(duplicates[0].offlineQueue.length).toBeGreaterThan(0);

			await within(client.quit());

			expect(await within(write)).toBe('rejected: Connection is closed.');
			expect(await within(subscribe)).toBe('rejected: Connection is closed.');
			await new Promise(resolve => setTimeout(resolve, 650));
			expect(await admin.redis.get(key)).toBeNull();
			expect(String(await server.call('CLIENT', 'LIST'))).not.toContain('name=' + name + ' ');
			expect(duplicates).toHaveLength(1);
			for (const connection of [client.redis, ...duplicates]) expect(connection.status).toBe('end');
		} finally {
			for (const connection of [client.redis, ...duplicates]) closeConnection(connection);
			await bus.deactivate();
			await admin.redis.del(key);
			await admin.quit();
		}
	});
});
