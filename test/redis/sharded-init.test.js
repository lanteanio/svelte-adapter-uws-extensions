import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

let uWS;
try {
	uWS = (await import('uWebSockets.js')).default;
} catch {
	uWS = null;
}
const describeUWS = uWS ? describe : describe.skip;

describeUWS('sharded bus activation through the real adapter init hook', () => {
	it('returns an activation failure to the caller without an unhandled rejection', async () => {
		const client = mockRedisClient('sharded-init:');
		client.redis.info = () => Promise.reject(new Error('INFO refused'));
		const bus = createShardedBus(client);
		const escaped = [];
		const onUnhandled = (reason) => { escaped.push(reason); };
		process.on('unhandledRejection', onUnhandled);
		try {
			const { createTestServer } = await import('svelte-adapter-uws/testing');
			await expect(createTestServer({
				handler: {
					init: ({ platform }) => bus.activate(platform)
				}
			})).rejects.toThrow('INFO refused');
			await new Promise((resolve) => setTimeout(resolve, 20));
			expect(escaped).toEqual([]);
		} finally {
			process.off('unhandledRejection', onUnhandled);
			await bus.deactivate();
		}
	});
});
