import { describe, it, expect } from 'vitest';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';

const activation = { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01' };
const producer = { traceparent: '00-fedcba9876543210fedcba9876543210-fedcba9876543210-01' };
for (const sharded of [false, true]) {
	describe(sharded ? 'sharded consumer context' : 'pubsub consumer context', () => {
		it.each([undefined, 'invalid', producer.traceparent])('isolates carried context %s from activation', async (traceparent) => {
			const active = new AsyncLocalStorage();
			const spans = [];
			const platform = mockPlatform();
			// Match the adapter trace.run contract: null parent falls back to ALS.
			platform.trace = {
				enabled: true,
				current: () => active.getStore() ?? null,
				run(name, options, fn) {
					const parent = options.parent ?? active.getStore() ?? null;
					spans.push({ name, parent });
					return active.run(parent, fn);
				}
			};
			const client = mockRedisClient();
			const duplicate = client.redis.duplicate.bind(client.redis);
			client.redis.duplicate = (...args) => {
				const subscriber = duplicate(...args);
				const on = subscriber.on.bind(subscriber);
				subscriber.on = (event, fn) => {
					// Socket callbacks inherit the context in which they were created.
					const resume = AsyncLocalStorage.snapshot();
					return on(event, (...values) => resume(fn, ...values));
				};
				return subscriber;
			};
			const bus = (sharded ? createShardedBus : createPubSubBus)(client);
			await active.run(activation, () => bus.activate(platform));
			if (sharded) await bus.follow('chat');
			try {
				const message = JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1, traceparent });
				if (sharded) await client.redis.spublish('uws:sharded:chat', message);
				else await client.redis.publish('uws:pubsub', message);
				expect(platform.published).toHaveLength(1);
				if (traceparent === producer.traceparent) expect(spans).toEqual([{ name: sharded ? 'redis.sharded-pubsub.receive' : 'redis.pubsub.receive', parent: producer }]);
				else expect(spans).toEqual([]);
			} finally { await bus.deactivate(); }
		});
	});
}
