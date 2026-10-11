import { expect, it } from 'vitest';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';

for (const factory of [createPubSubBus, createShardedBus]) {
	for (const traced of [false, true]) {
		it(`${factory.name} ignores non-array batches with tracing ${traced}`, async () => {
			const bus = factory(mockRedisClient());
			const platform = mockPlatform();
			platform.trace.enabled = traced;
			await bus.activate(platform);
			try {
				const wrapped = bus.wrap(platform);
				for (const value of [undefined, null, false, 1, 'room', {}, { length: 1 }]) {
					expect(wrapped.publishBatched(value)).toBeUndefined();
				}
				expect(platform.published).toEqual([]);
				expect(platform.publishedBatches).toEqual([]);
			} finally { await bus.deactivate(); }
		});
	}
}
