import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';

describe('redis pubsub bus', () => {
	let client;
	let platform;
	let bus;

	beforeEach(() => {
		client = mockRedisClient();
		platform = mockPlatform();
		bus = createPubSubBus(client);
	});

	describe('wrap', () => {
		it('returns a platform with the same API shape', () => {
			const wrapped = bus.wrap(platform);
			expect(typeof wrapped.publish).toBe('function');
			expect(typeof wrapped.send).toBe('function');
			expect(typeof wrapped.sendTo).toBe('function');
			expect(typeof wrapped.subscribers).toBe('function');
			expect(typeof wrapped.topic).toBe('function');
		});

		it('publish() calls the original platform.publish() without relay: false', () => {
			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'hello' });

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'chat',
				event: 'msg',
				data: { text: 'hello' },
				options: undefined
			});
		});

		it('publish() returns the original platform.publish() result', () => {
			const wrapped = bus.wrap(platform);
			const result = wrapped.publish('chat', 'msg', { text: 'hello' });
			expect(result).toBe(true);
		});

		it('send() delegates to original platform', () => {
			const wrapped = bus.wrap(platform);
			const ws = {};
			wrapped.send(ws, 'chat', 'msg', { text: 'hi' });
			expect(platform.sent).toHaveLength(1);
		});
	});

	describe('activate / deactivate', () => {
		it('activate is idempotent', async () => {
			await bus.activate(platform);
			await bus.activate(platform);
			// Should not throw or create duplicate subscribers
		});

		it('deactivate is safe to call without activate', async () => {
			await bus.deactivate();
			// Should not throw
		});

		it('forwards messages from Redis to local platform with relay: false', async () => {
			await bus.activate(platform);

			// Simulate a message from another instance by publishing directly
			// to Redis on the channel. The subscriber should pick it up.
			const msg = JSON.stringify({
				instanceId: 'other-instance',
				topic: 'chat',
				event: 'msg',
				data: { text: 'from-remote' }
			});
			await client.redis.publish('uws:pubsub', msg);

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'chat',
				event: 'msg',
				data: { text: 'from-remote' },
				options: { relay: false }
			});
		});

		it('suppresses echo (messages from same instance)', async () => {
			// Activate first, then wrap and publish.
			// The bus's own instanceId should be filtered out.
			await bus.activate(platform);

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'local' });

			// Only the direct platform.publish from wrap should be recorded,
			// not a second one from the subscriber (echo suppressed).
			expect(platform.published).toHaveLength(1);
		});

		it('skips malformed messages', async () => {
			await bus.activate(platform);
			await client.redis.publish('uws:pubsub', 'not-json');
			expect(platform.published).toHaveLength(0);
		});

		it('ignores messages on other channels', async () => {
			await bus.activate(platform);
			await client.redis.publish('other:channel', JSON.stringify({
				instanceId: 'other',
				topic: 'x',
				event: 'y',
				data: null
			}));
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('activate failure recovery', () => {
		it('can retry after subscribe() fails', async () => {
			// Create a client whose duplicate's subscribe() will fail once
			const failClient = mockRedisClient();
			let failCount = 0;
			const origDuplicate = failClient.duplicate.bind(failClient);
			failClient.duplicate = () => {
				const dup = origDuplicate();
				const origSubscribe = dup.subscribe.bind(dup);
				dup.subscribe = async (ch) => {
					if (failCount === 0) {
						failCount++;
						throw new Error('connection lost');
					}
					return origSubscribe(ch);
				};
				return dup;
			};

			const failBus = createPubSubBus(failClient);

			// First activate should fail
			await expect(failBus.activate(platform)).rejects.toThrow('connection lost');

			// Second activate should succeed (not stuck on active=true)
			await failBus.activate(platform);

			// Verify it works
			const msg = JSON.stringify({
				instanceId: 'other',
				topic: 'test',
				event: 'ping',
				data: null
			});
			await failClient.redis.publish('uws:pubsub', msg);
			expect(platform.published).toHaveLength(1);

			await failBus.deactivate();
		});
	});

	describe('wrap - options forwarding', () => {
		it('publish() forwards the options argument to the original platform', () => {
			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'hi' }, { relay: false });

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].options).toEqual({ relay: false });
		});

		it('publish() skips Redis when relay: false', async () => {
			const wrapped = bus.wrap(platform);

			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push(ch);
				return origPublish.call(client.redis, ch, msg);
			};

			wrapped.publish('chat', 'msg', { text: 'hi' }, { relay: false });
			expect(publishCalls).toHaveLength(0);

			// Without relay: false, should publish to Redis
			wrapped.publish('chat', 'msg', { text: 'hi' });
			expect(publishCalls).toHaveLength(1);
		});

		it('publish() sends to Redis when options are absent or relay is not false', async () => {
			const wrapped = bus.wrap(platform);

			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push(ch);
				return origPublish.call(client.redis, ch, msg);
			};

			wrapped.publish('chat', 'msg', { text: 'a' });
			wrapped.publish('chat', 'msg', { text: 'b' }, {});
			wrapped.publish('chat', 'msg', { text: 'c' }, { relay: true });
			expect(publishCalls).toHaveLength(3);
		});
	});

	describe('wrap - topic() passthrough', () => {
		it('topic().created() publishes through the wrapped platform (goes to Redis)', async () => {
			const wrapped = bus.wrap(platform);

			const publishCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				publishCalls.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			wrapped.topic('chat').created({ id: 1 });

			// Should have gone through wrapped.publish, reaching both local and Redis
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('chat');
			expect(platform.published[0].event).toBe('created');
			expect(platform.published[0].data).toEqual({ id: 1 });

			expect(publishCalls).toHaveLength(1);
			expect(publishCalls[0].topic).toBe('chat');
			expect(publishCalls[0].event).toBe('created');
		});

		it('topic().publish() routes through wrapped publish', async () => {
			const wrapped = bus.wrap(platform);

			const redisCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				redisCalls.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			wrapped.topic('room').publish('custom-event', { val: 42 });

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].event).toBe('custom-event');
			expect(redisCalls).toHaveLength(1);
		});

		it('topic() helpers (updated, deleted, set, increment, decrement) all route through Redis', () => {
			const wrapped = bus.wrap(platform);

			const redisCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				redisCalls.push(JSON.parse(msg));
				return origPublish.call(client.redis, ch, msg);
			};

			const t = wrapped.topic('items');
			t.updated({ id: 1 });
			t.deleted({ id: 2 });
			t.set('value');
			t.increment(1);
			t.decrement(1);

			expect(redisCalls).toHaveLength(5);
			expect(redisCalls.map((c) => c.event)).toEqual([
				'updated', 'deleted', 'set', 'increment', 'decrement'
			]);
		});
	});

	describe('activate - platform update', () => {
		it('second activate() with a different platform updates the forwarding target', async () => {
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();

			await bus.activate(platform1);
			// Second activate with a different platform
			await bus.activate(platform2);

			const msg = JSON.stringify({
				instanceId: 'other-instance',
				topic: 'chat',
				event: 'msg',
				data: { text: 'hello' }
			});
			await client.redis.publish('uws:pubsub', msg);

			// Should forward through platform2, not platform1
			expect(platform2.published).toHaveLength(1);
			expect(platform2.published[0].topic).toBe('chat');
			expect(platform1.published).toHaveLength(0);
		});
	});

	describe('custom channel', () => {
		it('uses the configured channel name', async () => {
			const customBus = createPubSubBus(client, { channel: 'custom:chan' });
			await customBus.activate(platform);

			const msg = JSON.stringify({
				instanceId: 'other',
				topic: 'test',
				event: 'ping',
				data: null
			});
			await client.redis.publish('custom:chan', msg);

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('test');
		});
	});
});
