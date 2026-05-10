import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

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
			expect(typeof wrapped.subscribe).toBe('function');
			expect(typeof wrapped.unsubscribe).toBe('function');
			expect(typeof wrapped.checkSubscribe).toBe('function');
			expect(typeof wrapped.bufferedAmount).toBe('function');
			expect(typeof wrapped.onPublishRate).toBe('function');
			expect(typeof wrapped.maxPayloadLength).toBe('number');
			expect(typeof wrapped.topic).toBe('function');
		});

		it('subscribe / unsubscribe / checkSubscribe delegate to the underlying platform', () => {
			const wrapped = bus.wrap(platform);
			const ws = {};

			wrapped.subscribe(ws, 'chat');
			wrapped.unsubscribe(ws, 'chat');
			wrapped.checkSubscribe(ws, 'chat');

			expect(platform.subscribed).toEqual([{ ws, topic: 'chat' }]);
			expect(platform.unsubscribed).toEqual([{ ws, topic: 'chat' }]);
			expect(platform.checkedSubscribe).toEqual([{ ws, topic: 'chat' }]);
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

	describe('batch', () => {
		it('wrapped platform exposes batch() that publishes multiple messages', () => {
			const wrapped = bus.wrap(platform);
			const results = wrapped.batch([
				{ topic: 'chat', event: 'msg', data: { text: 'a' } },
				{ topic: 'chat', event: 'msg', data: { text: 'b' } }
			]);

			expect(results).toEqual([true, true]);
			expect(platform.published).toHaveLength(2);
			expect(platform.published[0].data.text).toBe('a');
			expect(platform.published[1].data.text).toBe('b');
		});
	});

	describe('publishBatched', () => {
		it('throws on non-array input', () => {
			const wrapped = bus.wrap(platform);
			expect(() => wrapped.publishBatched('nope')).toThrow('publishBatched requires an array');
			expect(() => wrapped.publishBatched(null)).toThrow('publishBatched requires an array');
			expect(() => wrapped.publishBatched({ topic: 'x' })).toThrow('publishBatched requires an array');
		});

		it('no-ops on empty array (no Redis call, no local fan-out)', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(ch); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(0);
			expect(platform.published).toHaveLength(0);
			expect(platform.publishedBatches).toHaveLength(0);
		});

		it('calls platform.publishBatched locally with the original messages', () => {
			const wrapped = bus.wrap(platform);
			wrapped.publishBatched([
				{ topic: 'chat', event: 'msg', data: { text: 'a' } },
				{ topic: 'audit', event: 'created', data: { id: 1 } }
			]);

			expect(platform.publishedBatches).toHaveLength(1);
			expect(platform.publishedBatches[0].messages).toHaveLength(2);
			expect(platform.published.filter((p) => p.batched)).toHaveLength(2);
		});

		it('ships ONE Redis envelope per call carrying the whole batch', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				calls.push({ ch, parsed: JSON.parse(msg) });
				return orig.call(client.redis, ch, msg);
			};

			wrapped.publishBatched([
				{ topic: 'chat', event: 'msg', data: { text: 'a' } },
				{ topic: 'chat', event: 'msg', data: { text: 'b' } },
				{ topic: 'chat', event: 'msg', data: { text: 'c' } }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(1);
			expect(calls[0].ch).toBe('uws:pubsub');
			expect(calls[0].parsed.instanceId).toBeDefined();
			expect(calls[0].parsed.topic).toBeUndefined();
			expect(calls[0].parsed.batch).toHaveLength(3);
			expect(calls[0].parsed.batch[0]).toEqual({ topic: 'chat', event: 'msg', data: { text: 'a' } });
		});

		it('honors per-message relay: false (excludes from envelope, keeps local)', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([
				{ topic: 'a', event: 'x', data: 1 },
				{ topic: 'b', event: 'y', data: 2, options: { relay: false } },
				{ topic: 'c', event: 'z', data: 3 }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(1);
			expect(calls[0].batch).toHaveLength(2);
			expect(calls[0].batch.map((m) => m.topic)).toEqual(['a', 'c']);
			// All 3 still went through the local publishBatched.
			expect(platform.publishedBatches[0].messages).toHaveLength(3);
		});

		it('skips Redis entirely when every message has relay: false', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(ch); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([
				{ topic: 'a', event: 'x', data: 1, options: { relay: false } },
				{ topic: 'b', event: 'y', data: 2, options: { relay: false } }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(0);
			expect(platform.publishedBatches).toHaveLength(1);
		});

		it('mixes with publish() in the same tick (both ride the same pipeline flush)', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publish('a', 'x', 1);
			wrapped.publishBatched([{ topic: 'b', event: 'y', data: 2 }]);
			wrapped.publish('c', 'z', 3);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(3);
			expect(calls.find((p) => p.topic === 'a')).toBeDefined();
			expect(calls.find((p) => p.topic === 'c')).toBeDefined();
			expect(calls.find((p) => p.batch)).toBeDefined();
		});

		it('receiver dispatches batched envelope via platform.publishBatched with relay: false', async () => {
			await bus.activate(platform);
			platform.reset();

			const envelope = JSON.stringify({
				instanceId: 'other-instance',
				batch: [
					{ topic: 'a', event: 'x', data: 1 },
					{ topic: 'b', event: 'y', data: 2 }
				]
			});
			await client.redis.publish('uws:pubsub', envelope);

			expect(platform.publishedBatches).toHaveLength(1);
			const local = platform.publishedBatches[0].messages;
			expect(local).toHaveLength(2);
			expect(local[0]).toEqual({
				topic: 'a',
				event: 'x',
				data: 1,
				options: { relay: false }
			});
			expect(local[1].options).toEqual({ relay: false });
		});

		it('receiver echo-suppresses the whole batched envelope on instanceId match', async () => {
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }]);
			await new Promise((r) => setTimeout(r, 5));
			const ownInstanceId = calls[0].instanceId;

			platform.reset();

			// An echoed envelope (matching instanceId) carrying multiple messages
			// must drop the entire batch on one check, not per-message.
			const echo = JSON.stringify({
				instanceId: ownInstanceId,
				batch: [
					{ topic: 'a', event: 'x', data: 1 },
					{ topic: 'b', event: 'y', data: 2 }
				]
			});
			await client.redis.publish('uws:pubsub', echo);

			expect(platform.publishedBatches).toHaveLength(0);
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('wrap - new platform passthroughs', () => {
		it('exposes publishBatched, sendCoalesced, request, requestId, pressure, onPressure', () => {
			const wrapped = bus.wrap(platform);
			expect(typeof wrapped.publishBatched).toBe('function');
			expect(typeof wrapped.sendCoalesced).toBe('function');
			expect(typeof wrapped.request).toBe('function');
			expect(typeof wrapped.requestId).toBe('string');
			expect(wrapped.pressure).toBeDefined();
			expect(typeof wrapped.onPressure).toBe('function');
		});

		it('sendCoalesced delegates to the original platform', () => {
			const wrapped = bus.wrap(platform);
			const ws = {};
			wrapped.sendCoalesced(ws, { key: 'price', payload: { v: 100 } });
			expect(platform.sentCoalesced).toHaveLength(1);
			expect(platform.sentCoalesced[0]).toMatchObject({ ws, key: 'price', payload: { v: 100 } });
		});

		it('request delegates to the original platform', async () => {
			const wrapped = bus.wrap(platform);
			const ws = {};
			await wrapped.request(ws, 'confirm', { op: 'delete' }, { timeoutMs: 100 });
			expect(platform.requested).toHaveLength(1);
			expect(platform.requested[0]).toMatchObject({
				ws,
				event: 'confirm',
				data: { op: 'delete' },
				options: { timeoutMs: 100 }
			});
		});

		it('requestId getter reads from the underlying platform', () => {
			platform.requestId = 'req-abc-123';
			const wrapped = bus.wrap(platform);
			expect(wrapped.requestId).toBe('req-abc-123');
		});

		it('pressure getter reflects platform pressure transitions', () => {
			const wrapped = bus.wrap(platform);
			platform._setPressure({
				active: true, reason: 'MEMORY',
				subscriberRatio: 0, publishRate: 0, memoryMB: 999
			});
			expect(wrapped.pressure.active).toBe(true);
			expect(wrapped.pressure.reason).toBe('MEMORY');
		});

		it('onPressure subscribes to the underlying platform', () => {
			const wrapped = bus.wrap(platform);
			let received = null;
			const off = wrapped.onPressure((snap) => { received = snap; });

			platform._setPressure({
				active: true, reason: 'PUBLISH_RATE',
				subscriberRatio: 0, publishRate: 9999, memoryMB: 0
			});
			expect(received).toMatchObject({ reason: 'PUBLISH_RATE' });

			off();
			platform._setPressure({
				active: false, reason: 'NONE',
				subscriberRatio: 0, publishRate: 0, memoryMB: 0
			});
			// After unsubscribe, no further updates land in `received`.
			expect(received).toMatchObject({ reason: 'PUBLISH_RATE' });
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
			await Promise.resolve(); // flush microtask batch
			expect(publishCalls).toHaveLength(0);

			wrapped.publish('chat', 'msg', { text: 'hi' });
			await Promise.resolve(); // flush microtask batch
			expect(publishCalls).toHaveLength(1);
		});

		it('publish() sends to Redis when options are absent or relay is not false', async () => {
			// Activate so subscriber can receive the batched messages
			await bus.activate(platform);
			platform.reset();

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'a' });
			wrapped.publish('chat', 'msg', { text: 'b' }, {});
			wrapped.publish('chat', 'msg', { text: 'c' }, { relay: true });

			// All 3 publishes are local (synchronous) + relayed via microtask batch.
			// The local publishes happen immediately:
			expect(platform.published).toHaveLength(3);
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
			await Promise.resolve(); // flush microtask batch

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
			await Promise.resolve(); // flush microtask batch

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].event).toBe('custom-event');
			expect(redisCalls).toHaveLength(1);
		});

		it('topic() helpers (updated, deleted, set, increment, decrement) all route through Redis', async () => {
			const wrapped = bus.wrap(platform);

			const t = wrapped.topic('items');
			t.updated({ id: 1 });
			t.deleted({ id: 2 });
			t.set('value');
			t.increment(1);
			t.decrement(1);

			// All 5 events should have been published locally
			expect(platform.published).toHaveLength(5);
			expect(platform.published.map((c) => c.event)).toEqual([
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

	describe('degraded / recovered hooks', () => {
		it('fires onDegraded when the breaker leaves healthy', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);

			breaker.failure();
			expect(degradedCalls).toBe(1);

			await bus.deactivate();
			breaker.destroy();
		});

		it('fires onRecovered when the breaker returns to healthy', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			let recoveredCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				onRecovered: () => { recoveredCalls++; }
			});
			await bus.activate(platform);

			breaker.failure();
			expect(recoveredCalls).toBe(0);
			breaker.reset();
			expect(recoveredCalls).toBe(1);

			await bus.deactivate();
			breaker.destroy();
		});

		it('does not re-fire onDegraded on broken -> probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 30 });
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);

			breaker.failure();
			expect(degradedCalls).toBe(1);

			await new Promise((r) => setTimeout(r, 60));
			expect(breaker.state).toBe('probing');
			expect(degradedCalls).toBe(1);

			await bus.deactivate();
			breaker.destroy();
		});

		it('does not re-fire onDegraded on probing -> broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 30 });
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);

			breaker.failure();
			await new Promise((r) => setTimeout(r, 60));
			expect(breaker.state).toBe('probing');
			expect(degradedCalls).toBe(1);

			breaker.guard();
			breaker.failure();
			expect(breaker.state).toBe('broken');
			expect(degradedCalls).toBe(1);

			await bus.deactivate();
			breaker.destroy();
		});

		it('emits on the default systemChannel without explicit configuration', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, { breaker });
			await bus.activate(platform);
			platform.reset();

			breaker.failure();

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('__realtime');
			expect(platform.published[0].event).toBe('degraded');
			expect(platform.published[0].data).toMatchObject({ at: expect.any(Number) });

			breaker.reset();
			expect(platform.published).toHaveLength(2);
			expect(platform.published[1].topic).toBe('__realtime');
			expect(platform.published[1].event).toBe('recovered');

			await bus.deactivate();
			breaker.destroy();
		});

		it('emits on a custom systemChannel', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, { breaker, systemChannel: '__system' });
			await bus.activate(platform);
			platform.reset();

			breaker.failure();
			expect(platform.published[0].topic).toBe('__system');

			await bus.deactivate();
			breaker.destroy();
		});

		it('does not emit when systemChannel is null', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				systemChannel: null,
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);
			platform.reset();

			breaker.failure();
			expect(platform.published).toHaveLength(0);
			expect(degradedCalls).toBe(1);

			await bus.deactivate();
			breaker.destroy();
		});

		it('does not emit when systemChannel is false', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, { breaker, systemChannel: false });
			await bus.activate(platform);
			platform.reset();

			breaker.failure();
			expect(platform.published).toHaveLength(0);

			await bus.deactivate();
			breaker.destroy();
		});

		it('emits without a callback when only systemChannel is configured', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, { breaker });
			await bus.activate(platform);
			platform.reset();

			breaker.failure();
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].topic).toBe('__realtime');

			await bus.deactivate();
			breaker.destroy();
		});

		it('does nothing without a breaker', async () => {
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);
			platform.reset();

			expect(platform.published).toHaveLength(0);
			expect(degradedCalls).toBe(0);

			await bus.deactivate();
		});

		it('skips Redis when emitting (auto-emit is local only)', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, { breaker });
			await bus.activate(platform);

			const redisCalls = [];
			const origPublish = client.redis.publish;
			client.redis.publish = async (ch, msg) => {
				redisCalls.push({ ch, msg });
				return origPublish.call(client.redis, ch, msg);
			};

			breaker.failure();
			await new Promise((r) => setTimeout(r, 5));

			// The local platform receives the event.
			const localSystem = platform.published.find((p) => p.topic === '__realtime');
			expect(localSystem).toBeDefined();

			// No relay went through Redis - the bus emits directly via the
			// underlying platform, not the wrapped one.
			const relayed = redisCalls.find((c) => {
				try { return JSON.parse(c.msg).topic === '__realtime'; } catch { return false; }
			});
			expect(relayed).toBeUndefined();

			await bus.deactivate();
			breaker.destroy();
		});

		it('a throwing onDegraded does not break emission', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = createPubSubBus(client, {
				breaker,
				onDegraded: () => { throw new Error('user code threw'); }
			});
			await bus.activate(platform);
			platform.reset();

			breaker.failure();

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].event).toBe('degraded');

			await bus.deactivate();
			breaker.destroy();
		});

		it('deactivate unsubscribes from the breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			let degradedCalls = 0;
			const bus = createPubSubBus(client, {
				breaker,
				onDegraded: () => { degradedCalls++; }
			});
			await bus.activate(platform);
			await bus.deactivate();

			breaker.failure();
			expect(degradedCalls).toBe(0);

			breaker.destroy();
		});

		it('validates onDegraded must be a function', () => {
			expect(() => createPubSubBus(client, { onDegraded: 'nope' })).toThrow('onDegraded');
		});

		it('validates onRecovered must be a function', () => {
			expect(() => createPubSubBus(client, { onRecovered: 42 })).toThrow('onRecovered');
		});

		it('validates systemChannel must be a string, null, or false', () => {
			expect(() => createPubSubBus(client, { systemChannel: 123 })).toThrow('systemChannel');
		});
	});

	describe('breaker accounting in activate', () => {
		it('records success() when subscribe succeeds', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const bus = createPubSubBus(client, { breaker });

			breaker.failure();
			expect(breaker.failures).toBe(1);

			await bus.activate(platform);
			expect(breaker.failures).toBe(0);

			await bus.deactivate();
			breaker.destroy();
		});

		it('records failure() when subscribe fails', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const failClient = mockRedisClient('test:');
			const origDuplicate = failClient.duplicate.bind(failClient);
			failClient.duplicate = (overrides) => {
				const dup = origDuplicate(overrides);
				dup.subscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};

			const bus = createPubSubBus(failClient, { breaker });

			await expect(bus.activate(platform)).rejects.toThrow('subscribe failed');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});
	});
});
