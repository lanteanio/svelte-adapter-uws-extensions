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

		it('publish() calls the original platform.publish()', () => {
			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'hello' });

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'chat',
				event: 'msg',
				data: { text: 'hello' }
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

		it('forwards messages from Redis to local platform', async () => {
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
				data: { text: 'from-remote' }
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
