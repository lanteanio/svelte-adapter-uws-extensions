import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

for (const [name, createBus] of [['pubsub', createPubSubBus], ['sharded pubsub', createShardedBus]]) {
	describe(name + ' delivery authorization forwarding', () => {
		it('forwards the callback, options, receiver and return value', () => {
			const platform = mockPlatform();
			const callback = async () => true;
			const options = { maxPendingBytes: 128, timeoutMs: 50 };
			const result = Symbol('configured');
			platform.authorizeDelivery = function (receivedCallback, receivedOptions) {
				expect(this).toBe(platform);
				expect(receivedCallback).toBe(callback);
				expect(receivedOptions).toBe(options);
				return result;
			};
			const wrapped = createBus(mockRedisClient()).wrap(platform);
			expect(wrapped.authorizeDelivery(callback, options)).toBe(result);
		});

		it('does not advertise delivery authorization on an older adapter', () => {
			const platform = mockPlatform();
			delete platform.authorizeDelivery;
			const wrapped = createBus(mockRedisClient()).wrap(platform);
			expect(wrapped.authorizeDelivery).toBeUndefined();
		});
	});
}

for (const [name, project] of [
	['pubsub inside sharded', platform => createShardedBus(mockRedisClient()).wrap(createPubSubBus(mockRedisClient()).wrap(platform))],
	['sharded inside pubsub', platform => createPubSubBus(mockRedisClient()).wrap(createShardedBus(mockRedisClient()).wrap(platform))]
]) {
	for (const traced of [false, true]) {
		it(`${name} preserves optional delivery policy configuration with tracing ${traced}`, () => {
			const platform = mockPlatform();
			platform.trace.enabled = traced;
			const callback = async () => true;
			const options = Object.freeze({ maxPendingBytes: 128, timeoutMs: 50 });
			const result = Symbol('source result');
			platform.authorizeDelivery = function (actual, settings) {
				expect(this).toBe(platform);
				expect(actual).toBe(callback);
				expect(settings).toBe(options);
				return result;
			};
			expect(project(platform).authorizeDelivery(callback, options)).toBe(result);
			delete platform.authorizeDelivery;
			expect(project(platform).authorizeDelivery).toBeUndefined();
		});
	}
}
