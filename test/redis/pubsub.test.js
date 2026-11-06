import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SUBSCRIPTIONS } from '../../src/shared/ws-subscriptions.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { registeredRecord } from '../helpers/diagnostic-line.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { createDegradationPolicy } from '../../src/shared/degradation.js';
import { SUBSCRIBER_RESTORING, SUBSCRIBER_RESTORED } from '../../src/shared/redis-subscriber.js';
import { HINT_HOLD_MS, HINT_MAX_HOLD_MS } from '../../src/shared/delivery-hints.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const TRACE_A = '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01';
const TRACE_B = '00-fedcba9876543210fedcba9876543210-fedcba9876543210-00';
const REDIS_ATTRIBUTES = { 'messaging.system': 'redis' };

// A mock Platform whose tracing provider is switched on, as the adapter's is
// when one is configured at build time. `current()` answers the live context,
// so the relayed envelope carries it.
function tracedPlatform(context = null) {
	const p = mockPlatform();
	p.trace.enabled = true;
	p.traceContext = context;
	return p;
}

// Every envelope that actually reached the bus channel, as a raw subscriber
// on its own connection sees it (single publishes and pipelined ones alike).
async function captureChannel(client, channel = 'uws:pubsub') {
	const envelopes = [];
	const sub = client.duplicate();
	sub.on('message', (ch, message) => {
		if (ch === channel) envelopes.push(JSON.parse(message));
	});
	await sub.subscribe(channel);
	return envelopes;
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

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

		it('forwards live structural trace surfaces through the wrapped Platform', () => {
			platform.traceContext = { traceparent: TRACE_A };
			const wrapped = bus.wrap(platform);
			expect(wrapped.trace).toBe(platform.trace);
			expect(wrapped.traceContext).toEqual({ traceparent: TRACE_A });
			platform.traceContext = { traceparent: TRACE_B };
			expect(wrapped.traceContext).toEqual({ traceparent: TRACE_B });
		});

		it('send() delegates to original platform', () => {
			const wrapped = bus.wrap(platform);
			const ws = {};
			wrapped.send(ws, 'chat', 'msg', { text: 'hi' });
			expect(platform.sent).toHaveLength(1);
		});

		it('topicEpoch() routes to the replay tracker cache when one is wired', () => {
			// The subscribe-ack epoch must carry the same generation domain the
			// resume hook compares against. With a replay tracker present, that
			// is the tracker's per-topic cached generation, not the source
			// platform's single-worker value.
			platform.replay = { cachedEpoch: (t) => (t === 'chat' ? 7 : 3) };
			const wrapped = bus.wrap(platform);
			expect(wrapped.topicEpoch('chat')).toBe(7);
			expect(wrapped.topicEpoch('other')).toBe(3);
		});

		it('topicEpoch() falls back to the source platform when no tracker is wired', () => {
			const wrapped = bus.wrap(platform);
			// mockPlatform.topicEpoch returns the baseline 0 by default.
			expect(wrapped.topicEpoch('chat')).toBe(0);
			platform.topicEpoch = () => 42;
			expect(wrapped.topicEpoch('chat')).toBe(42);
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
		it('ignores non-array publishBatched input', () => {
			const wrapped = bus.wrap(platform);
			expect(() => wrapped.publishBatched('nope')).not.toThrow();
			expect(() => wrapped.publishBatched(null)).not.toThrow();
			expect(() => wrapped.publishBatched({ topic: 'x' })).not.toThrow();
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

		it('receiver publishes each batched message on its own when the platform has no publishBatched', async () => {
			delete platform.publishBatched;
			await bus.activate(platform);
			platform.reset();

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance',
				batch: [
					{ topic: 'a', event: 'x', data: 1 },
					{ topic: 'b', event: 'y', data: 2 }
				]
			}));

			expect(platform.published).toEqual([
				{ topic: 'a', event: 'x', data: 1, options: { relay: false, compress: false } },
				{ topic: 'b', event: 'y', data: 2, options: { relay: false, compress: false } }
			]);
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

	describe('coalesced relay (cluster coalesceBy)', () => {
		it('relayCoalesced relays a coalesced envelope carrying the coalesceKey', async () => {
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.relayCoalesced('prices', 'tick', { p: 5 }, 'btc');
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(1);
			expect(calls[0]).toMatchObject({ coalesced: true, topic: 'prices', event: 'tick', data: { p: 5 }, coalesceKey: 'btc' });
			expect(calls[0].instanceId).toBeDefined();
		});

		it('receiver re-coalesces an inbound coalesced envelope onto local subscribers (no broadcast)', async () => {
			await bus.activate(platform);
			platform.reset();
			const wsA = { id: 'a' };
			const wsB = { id: 'b' };
			platform.forEachSubscriber = (topic, fn) => { if (topic === 'prices') { fn(wsA); fn(wsB); } };

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance',
				coalesced: true,
				topic: 'prices',
				event: 'tick',
				data: { p: 5 },
				coalesceKey: 'btc'
			}));

			expect(platform.sentCoalesced).toHaveLength(2);
			expect(platform.sentCoalesced[0]).toEqual({ ws: wsA, key: 'prices\0btc', topic: 'prices', event: 'tick', data: { p: 5 } });
			expect(platform.sentCoalesced[1].ws).toBe(wsB);
			// A coalesced frame must never be broadcast via publish.
			expect(platform.published).toHaveLength(0);
		});

		it.each(['sendCoalesced', 'forEachSubscriber'])('receiver publishes an inbound coalesced value when the platform has no %s', async (missing) => {
			delete platform[missing];
			await bus.activate(platform);
			platform.reset();

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance',
				coalesced: true,
				topic: 'prices',
				event: 'tick',
				data: { p: 5 },
				coalesceKey: 'btc'
			}));

			// Unsequenced, like the coalesced send it stands in for.
			expect(platform.published).toEqual([
				{ topic: 'prices', event: 'tick', data: { p: 5 }, options: { relay: false, seq: false } }
			]);
		});

		it('receiver echo-suppresses an own-instance coalesced envelope', async () => {
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);
			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };
			wrapped.relayCoalesced('prices', 'tick', { p: 1 }, 'btc');
			await new Promise((r) => setTimeout(r, 5));
			const ownId = calls[0].instanceId;

			platform.reset();
			platform.forEachSubscriber = (topic, fn) => fn({ id: 'x' });
			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: ownId, coalesced: true, topic: 'prices', event: 'tick', data: { p: 1 }, coalesceKey: 'btc'
			}));

			expect(platform.sentCoalesced).toHaveLength(0);
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
			wrapped.sendCoalesced(ws, { key: 'price', topic: 'prices', event: 'tick', data: { v: 100 } });
			expect(platform.sentCoalesced).toHaveLength(1);
			expect(platform.sentCoalesced[0]).toMatchObject({ ws, key: 'price', topic: 'prices', event: 'tick', data: { v: 100 } });
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

		it('carries a traced producer context to a traced peer, outside application data', async () => {
			const context = { traceparent: TRACE_A, tracestate: 'vendor=value' };
			const origin = tracedPlatform(context);
			const remote = tracedPlatform();
			const peer = createPubSubBus(client);
			await bus.activate(origin);
			await peer.activate(remote);

			bus.wrap(origin).publish('chat', 'msg', { traceparent: 'application-data' });
			await settle();

			const delivered = remote.published.find((entry) => entry.event === 'msg');
			expect(delivered.data).toEqual({ traceparent: 'application-data' });
			// Transport fields never become a publish option on the peer.
			expect(delivered.options).toEqual({ relay: false });
			expect(origin.traceOperations).toEqual([
				{ name: 'redis.pubsub.publish', options: { kind: 'producer', parent: context, attributes: REDIS_ATTRIBUTES } }
			]);
			expect(remote.traceOperations).toEqual([
				{ name: 'redis.pubsub.receive', options: { kind: 'consumer', parent: context, attributes: REDIS_ATTRIBUTES } }
			]);
			await bus.deactivate();
			await peer.deactivate();
		});

		it('on a Platform without a tracing provider reads no trace context and relays untraced envelopes', async () => {
			let reads = 0;
			Object.defineProperty(platform, 'traceContext', {
				get() { reads++; return { traceparent: TRACE_A }; },
				configurable: true
			});
			platform.publishWire = () => true;
			const envelopes = await captureChannel(client);
			const wrapped = bus.wrap(platform);

			wrapped.publish('chat', 'one', 1);
			wrapped.publish('chat', 'two', 2, { seq: 7 });
			wrapped.batch([{ topic: 'chat', event: 'b', data: 3 }]);
			wrapped.publishBatched([{ topic: 'chat', event: 'pb', data: 4 }]);
			wrapped.relayCoalesced('chat', 'c', 5, 'k');
			wrapped.publishWire('chat', 'w', 6, { shared: true, capability: 'cap' });
			await settle();

			expect(reads).toBe(0);
			expect(platform.traceOperations).toEqual([]);
			expect(envelopes).toHaveLength(6);
			for (const env of envelopes) {
				expect(env).not.toHaveProperty('traceparent');
				expect(env).not.toHaveProperty('tracestate');
			}
		});

		it('on a Platform without a tracing provider ignores a carried context on receive', async () => {
			await bus.activate(platform);
			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'peer', topic: 'chat', event: 'msg', data: 1, traceparent: TRACE_A
			}));
			await settle();

			expect(platform.published.filter((entry) => entry.event === 'msg')).toHaveLength(1);
			expect(platform.traceOperations).toEqual([]);
			expect(platform.traceRestored).toEqual([]);
			await bus.deactivate();
		});

		it('makes batch, publishBatched and relayCoalesced one producer operation per call, local delivery included', async () => {
			const origin = tracedPlatform({ traceparent: TRACE_A });
			const envelopes = await captureChannel(client);
			let depth = 0;
			const run = origin.trace.run;
			origin.trace.run = (name, options, fn) => {
				depth++;
				try { return run(name, options, fn); } finally { depth--; }
			};
			const localDepths = [];
			const batch = origin.batch.bind(origin);
			origin.batch = (messages) => { localDepths.push(depth); return batch(messages); };
			const publishBatched = origin.publishBatched.bind(origin);
			origin.publishBatched = (messages) => { localDepths.push(depth); return publishBatched(messages); };
			const wrapped = bus.wrap(origin);

			wrapped.batch([
				{ topic: 'a', event: 'x', data: 1 },
				{ topic: 'b', event: 'x', data: 2 },
				{ topic: 'c', event: 'x', data: 3 }
			]);
			expect(origin.traceOperations).toHaveLength(1);
			wrapped.publishBatched([
				{ topic: 'a', event: 'y', data: 1 },
				{ topic: 'b', event: 'y', data: 2 }
			]);
			expect(origin.traceOperations).toHaveLength(2);
			wrapped.relayCoalesced('a', 'z', 1, 'k');
			expect(origin.traceOperations).toHaveLength(3);
			await settle();

			for (const op of origin.traceOperations) {
				expect(op).toEqual({
					name: 'redis.pubsub.publish',
					options: { kind: 'producer', parent: { traceparent: TRACE_A }, attributes: REDIS_ATTRIBUTES }
				});
			}
			expect(localDepths).toEqual([1, 1]);
			// Three batch envelopes, one publishBatched envelope, one coalesced.
			expect(envelopes).toHaveLength(5);
			expect(envelopes.map((env) => env.traceparent)).toEqual([TRACE_A, TRACE_A, TRACE_A, TRACE_A, TRACE_A]);
		});

		it('opens no producer operation for a publish that does not relay', () => {
			const origin = tracedPlatform({ traceparent: TRACE_A });
			origin.publishWire = () => true;
			const wrapped = bus.wrap(origin);

			wrapped.publish('chat', 'local', 1, { relay: false });
			wrapped.publishWire('chat', 'wire', 1, { shared: false, capability: 'cap' });
			wrapped.publishWire('chat', 'wire', 1, { shared: true, capability: 'cap' }, { relay: false });

			expect(origin.traceOperations).toEqual([]);
			expect(origin.published.map((entry) => entry.event)).toEqual(['local']);

			// Batches with nothing to relay open none either.
			wrapped.batch([]);
			wrapped.batch([{ topic: 'chat', event: 'b-local', data: 1, options: { relay: false } }]);
			wrapped.publishBatched([{ topic: 'chat', event: 'pb-local', data: 1, options: { relay: false } }]);
			expect(origin.traceOperations).toEqual([]);
		});

		it('delivers a traced batch locally before it reads the entries, so the platform rejects a malformed one', () => {
			const origin = tracedPlatform({ traceparent: TRACE_A });
			const rejection = new TypeError('platform rejects the batch');
			const refuseMalformed = (messages) => {
				if (!Array.isArray(messages) || messages.some((m) => !m)) throw rejection;
				return [];
			};
			origin.batch = refuseMalformed;
			origin.publishBatched = refuseMalformed;
			const wrapped = bus.wrap(origin);

			// A null entry or a non-array counts as relaying, so each takes the
			// traced path and reaches the platform, whose own error comes back.
			expect(() => wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, null])).toThrow(rejection);
			expect(() => wrapped.batch([null])).toThrow(rejection);
			expect(() => wrapped.batch('not an array')).toThrow(rejection);
			expect(origin.traceOperations).toHaveLength(3);
			// publishBatched checks for an array itself, traced or not.
			expect(() => wrapped.publishBatched('not an array')).not.toThrow();
		});

		it('relays a near-cap envelope without its trace context instead of letting every peer drop it', async () => {
			const origin = createPubSubBus(client, { maxEnvelopeBytes: 600 });
			const peer = createPubSubBus(client, { maxEnvelopeBytes: 600 });
			const context = { traceparent: TRACE_A, tracestate: 'vendor=' + 'y'.repeat(200) };
			const source = tracedPlatform(context);
			const remote = tracedPlatform();
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			await origin.activate(source);
			await peer.activate(remote);
			const data = 'x'.repeat(400);
			origin.wrap(source).publish('chat', 'msg', data);
			origin.wrap(source).publish('chat', 'msg', data);
			await settle();

			const delivered = remote.published.filter((entry) => entry.event === 'msg');
			expect(delivered.map((entry) => entry.data)).toEqual([data, data]);
			expect(remote.traceOperations.map((op) => op.options.parent)).toEqual([]);
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.pubsub.trace-context.omitted');
			expect(JSON.parse(warn.mock.calls[0][0].slice(warn.mock.calls[0][0].indexOf('{'))).attributes).toEqual({ maxEnvelopeBytes: 600 });

			// A payload that fits with its context keeps it.
			origin.wrap(source).publish('chat', 'small', 'x');
			await settle();
			expect(remote.traceOperations.map((op) => op.options.parent)).toEqual([context]);
			warn.mockRestore();
			await origin.deactivate();
			await peer.deactivate();
		});

		it('measures the envelope cap in bytes, so multi-byte data drops the trace fields in time', async () => {
			const origin = createPubSubBus(client, { maxEnvelopeBytes: 600 });
			const peer = createPubSubBus(client, { maxEnvelopeBytes: 600 });
			const source = tracedPlatform({ traceparent: TRACE_A });
			const remote = mockPlatform();
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			await origin.activate(source);
			await peer.activate(remote);
			// 170 three-byte characters: the plain envelope fits in 600 bytes,
			// the traced one does not, though both are far under 600 characters.
			const data = String.fromCharCode(0x20ac).repeat(170);
			origin.wrap(source).publish('chat', 'msg', data);
			await settle();

			expect(remote.published.filter((entry) => entry.event === 'msg').map((entry) => entry.data)).toEqual([data]);
			expect(warn).toHaveBeenCalledOnce();
			warn.mockRestore();
			await origin.deactivate();
			await peer.deactivate();
		});

		it('does not report an envelope that is over the cap on its own as a trace omission', async () => {
			const origin = createPubSubBus(client, { maxEnvelopeBytes: 600 });
			const source = tracedPlatform({ traceparent: TRACE_A });
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			const wrapped = origin.wrap(source);

			wrapped.publish('chat', 'big', 'x'.repeat(1000));
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.pubsub.envelope.oversized');
			warn.mockClear();

			// The one report is still available for a genuine omission.
			wrapped.publish('chat', 'near', 'x'.repeat(480));
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.pubsub.trace-context.omitted');
			warn.mockRestore();
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
			await new Promise((r) => setTimeout(r, 5)); // flush tick batch
			expect(publishCalls).toHaveLength(0);

			wrapped.publish('chat', 'msg', { text: 'hi' });
			await new Promise((r) => setTimeout(r, 5)); // flush tick batch
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
			await new Promise((r) => setTimeout(r, 5)); // flush tick batch

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
			await new Promise((r) => setTimeout(r, 5)); // flush tick batch

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
			// Without a systemChannel the bus does not probe the breaker itself,
			// so it stays probing until a caller decides.
			const bus = createPubSubBus(client, {
				breaker,
				systemChannel: null,
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
			// Without a systemChannel the bus does not probe the breaker itself,
			// so it stays probing until a caller decides.
			const bus = createPubSubBus(client, {
				breaker,
				systemChannel: null,
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

	describe('refetch hint after lost delivery', () => {
		const cleanups = [];
		beforeEach(() => {
			vi.useFakeTimers();
			// The waits are measured on the monotonic clock; run it on the fake one.
			setRuntimeEnv({ clock: { monotonic: () => Date.now() } });
		});
		afterEach(async () => {
			while (cleanups.length) await cleanups.pop()();
			resetRuntimeEnv();
			vi.useRealTimers();
		});
		const tick = () => vi.advanceTimersByTimeAsync(5);
		const hold = () => vi.advanceTimersByTimeAsync(HINT_HOLD_MS + 5);

		// One instance: its own platform, breaker and bus on the shared client,
		// and the subscriber connection the bus opened.
		async function instance(options = {}) {
			const p = mockPlatform();
			const breaker = createCircuitBreaker({ failureThreshold: 1, ...options.breaker });
			const subs = [];
			const source = options.client || client;
			const opened = { ...source, duplicate: (o) => { const dup = source.duplicate(o); subs.push(dup); return dup; } };
			const b = createPubSubBus(opened, { breaker, ...options.bus });
			await b.activate(p);
			p.reset();
			cleanups.push(async () => { await b.deactivate(); breaker.destroy(); });
			return { platform: p, breaker, bus: b, wrapped: b.wrap(p), subscriber: () => subs[subs.length - 1] };
		}
		const frames = (p, event = 'recovered') => p.published.filter((f) => f.topic === '__realtime' && f.event === event);
		const refetches = (p) => frames(p).filter((f) => f.data.recovery?.refetch === true);
		const failPublishes = () => {
			const original = client.redis.publish;
			client.redis.publish = async () => { throw new Error('connection lost'); };
			return () => { client.redis.publish = original; };
		};
		const emit = (sub, event) => sub._listeners.get(event)();
		const restore = (inst) => {
			emit(inst.subscriber(), SUBSCRIBER_RESTORING);
			emit(inst.subscriber(), SUBSCRIBER_RESTORED);
		};

		it('asks the other instances to refetch when relays failed before the breaker opened', async () => {
			const a = await instance({ breaker: { failureThreshold: 2 } });
			const b = await instance();
			const healed = failPublishes();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			// One failure is under the threshold: still healthy, but the message is lost.
			expect(a.breaker.state).toBe('healthy');
			a.breaker.failure();
			expect(a.breaker.state).toBe('broken');
			healed();
			const envelopes = await captureChannel(client);
			a.breaker.reset();
			await tick();

			// The request carries nothing of the sender's outage.
			const hints = envelopes.filter((e) => e.topic === '__realtime');
			expect(hints).toHaveLength(1);
			expect(Object.keys(hints[0]).sort()).toEqual(['data', 'event', 'instanceId', 's', 'topic']);
			expect(hints[0]).toMatchObject({ event: 'recovered', s: false });
			expect(Object.keys(hints[0].data).sort()).toEqual(['at', 'recovery']);
			expect(hints[0].data.recovery).toEqual({ refetch: true });

			// The peer holds its own frame for further reports, then publishes it locally.
			expect(frames(b.platform)).toHaveLength(0);
			await hold();
			expect(frames(b.platform)).toHaveLength(1);
			expect(frames(b.platform)[0].data).toEqual({ at: expect.any(Number), recovery: { refetch: true } });
			expect(frames(b.platform)[0].options).toEqual({ relay: false, seq: false });
			// The sender's own clients missed nothing: its subscriber stayed up.
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data.recovery).toBeUndefined();
		});

		it('announces a relay that failed under the threshold after the next successful relay', async () => {
			const a = await instance({ breaker: { failureThreshold: 5 } });
			const b = await instance();
			const healed = failPublishes();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			healed();
			await hold();
			expect(frames(b.platform)).toHaveLength(0);
			a.wrapped.publish('chat', 'msg', { n: 2 });
			await tick();
			await hold();
			expect(a.breaker.state).toBe('healthy');
			expect(refetches(b.platform)).toHaveLength(1);
			expect(frames(a.platform)).toHaveLength(0);
		});

		it('announces relays shed by a full relay queue', async () => {
			const a = await instance({ breaker: { failureThreshold: 5 } });
			const b = await instance();
			// Nine envelopes of about 1 MB exceed the queue's 8 MiB.
			const payload = 'x'.repeat(1_000_000);
			for (let i = 0; i < 9; i++) a.wrapped.publish('chat', 'msg', payload);
			for (let i = 0; i < 12; i++) await tick();
			await hold();
			expect(b.platform.published.filter((f) => f.topic === 'chat')).toHaveLength(8);
			expect(refetches(b.platform)).toHaveLength(1);
		});

		it('counts relays refused while the breaker is open', async () => {
			const a = await instance();
			const b = await instance();
			a.breaker.failure();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			a.breaker.reset();
			await tick();
			await hold();
			expect(refetches(b.platform)).toHaveLength(1);
		});

		it('sends no request and no hint when nothing was lost', async () => {
			const a = await instance();
			const b = await instance();
			const envelopes = await captureChannel(client);
			a.breaker.failure();
			a.breaker.reset();
			await tick();
			await hold();
			expect(envelopes.filter((e) => e.topic === '__realtime')).toHaveLength(0);
			expect(frames(b.platform)).toHaveLength(0);
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data).toEqual({ at: expect.any(Number) });
		});

		it('holds a peer request while this instance is degraded and puts it on its own recovered frame', async () => {
			const a = await instance();
			const b = await instance();
			a.breaker.failure();
			b.breaker.failure();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			a.breaker.reset();
			await tick();
			await hold();
			// A peer's recovery is not this instance's: its clients stay degraded.
			expect(frames(b.platform)).toHaveLength(0);
			b.breaker.reset();
			await hold();
			expect(frames(b.platform)).toHaveLength(1);
			expect(frames(b.platform)[0].data.recovery).toEqual({ refetch: true });
		});

		it('gives each client one refetch frame when every peer lost relays', async () => {
			const all = [];
			for (let i = 0; i < 5; i++) all.push(await instance());
			for (const inst of all) inst.breaker.failure();
			for (const inst of all) inst.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			for (const inst of all) inst.breaker.reset();
			for (let i = 0; i < 4; i++) await tick();
			await hold();
			for (const inst of all) {
				expect(refetches(inst.platform)).toHaveLength(1);
				expect(frames(inst.platform)).toHaveLength(2);
			}
		});

		it('merges the peer requests into its own recovered frame when that one asks for a refetch', async () => {
			const all = [];
			for (let i = 0; i < 5; i++) all.push(await instance());
			for (const inst of all) inst.breaker.failure();
			for (const inst of all) inst.wrapped.publish('chat', 'msg', { n: 1 });
			for (const inst of all) restore(inst);
			await tick();
			for (const inst of all) inst.breaker.reset();
			for (let i = 0; i < 4; i++) await tick();
			await hold();
			for (const inst of all) {
				expect(frames(inst.platform)).toHaveLength(1);
				expect(frames(inst.platform)[0].data.recovery).toEqual({ refetch: true });
			}
		});

		it('asks its own clients to refetch once a reconnected subscriber restored the channel', async () => {
			const a = await instance();
			emit(a.subscriber(), SUBSCRIBER_RESTORING);
			await hold();
			expect(frames(a.platform)).toHaveLength(0);
			emit(a.subscriber(), SUBSCRIBER_RESTORED);
			await hold();
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data).toEqual({ at: expect.any(Number), recovery: { refetch: true } });
			expect(frames(a.platform)[0].options).toEqual({ relay: false, seq: false });
			// Said once.
			a.breaker.failure();
			a.breaker.reset();
			await hold();
			expect(frames(a.platform)[1].data.recovery).toBeUndefined();
		});

		it('puts a reconnect during the outage on the breaker recovered frame', async () => {
			const a = await instance();
			a.breaker.failure();
			restore(a);
			await hold();
			expect(frames(a.platform)).toHaveLength(0);
			a.breaker.reset();
			await hold();
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data.recovery).toEqual({ refetch: true });
		});

		it('waits for a restore still running when the breaker recovers', async () => {
			const a = await instance();
			a.breaker.failure();
			emit(a.subscriber(), SUBSCRIBER_RESTORING);
			a.breaker.reset();
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data.recovery).toBeUndefined();
			emit(a.subscriber(), SUBSCRIBER_RESTORED);
			await hold();
			expect(frames(a.platform)).toHaveLength(2);
			expect(frames(a.platform)[1].data.recovery).toEqual({ refetch: true });
		});

		it('drops a held frame when the breaker opens again during the hold', async () => {
			const a = await instance();
			a.breaker.failure();
			restore(a);
			a.breaker.reset();
			a.breaker.failure();
			await hold();
			expect(frames(a.platform)).toHaveLength(0);
			a.breaker.reset();
			await hold();
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data.recovery).toEqual({ refetch: true });
		});

		it('asks its own clients to refetch after activation failed and then succeeded', async () => {
			const failing = mockRedisClient();
			const original = failing.duplicate.bind(failing);
			let refusals = 1;
			failing.duplicate = (o) => {
				const dup = original(o);
				const subscribe = dup.subscribe.bind(dup);
				dup.subscribe = async (...ch) => {
					if (refusals > 0) { refusals--; throw new Error('connection lost'); }
					return subscribe(...ch);
				};
				return dup;
			};
			const p = mockPlatform();
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const b = createPubSubBus(failing, { breaker });
			cleanups.push(async () => { await b.deactivate(); breaker.destroy(); });
			await expect(b.activate(p)).rejects.toThrow('connection lost');
			await b.activate(p);
			expect(frames(p)).toHaveLength(0);
			await hold();
			expect(frames(p)).toHaveLength(1);
			expect(frames(p)[0].data.recovery).toEqual({ refetch: true });
		});

		it('builds every hint frame through the degradation policy', async () => {
			const recovery = { bannerCopy: 'Live updates are back.', messageKey: 'live.restored', clearCache: true };
			const policy = () => createDegradationPolicy({ recovery, recoveryJitterMs: 2000 });
			const expected = { ...recovery, refetch: true };
			const a = await instance({ bus: { degradationPolicy: policy() } });
			const b = await instance({ bus: { degradationPolicy: policy() } });

			// A restored subscriber.
			restore(b);
			await hold();
			expect(frames(b.platform)).toHaveLength(1);
			expect(frames(b.platform)[0].data.recovery).toEqual(expected);
			expect(frames(b.platform)[0].options).toEqual({ relay: false, seq: false, jitterMs: 2000 });

			// A peer request, at least HINT_MAX_HOLD_MS after the first.
			a.breaker.failure();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			a.breaker.reset();
			await tick();
			await hold();
			expect(frames(b.platform)).toHaveLength(1);
			await vi.advanceTimersByTimeAsync(HINT_MAX_HOLD_MS);
			expect(frames(b.platform)).toHaveLength(2);
			expect(frames(b.platform)[1].data.recovery).toEqual(expected);
			expect(frames(b.platform)[1].options).toEqual({ relay: false, seq: false, jitterMs: 2000 });
			// The policy's own envelope is left as it was.
			expect(recovery).toEqual({ bannerCopy: 'Live updates are back.', messageKey: 'live.restored', clearCache: true });
		});

		it('leaves refetch to a policy that sets it', async () => {
			const decided = { refetch: false, bannerCopy: 'Live updates are back.' };
			const a = await instance({ bus: { degradationPolicy: createDegradationPolicy({ recovery: decided }) } });
			a.breaker.failure();
			restore(a);
			a.breaker.reset();
			await hold();
			expect(frames(a.platform)).toHaveLength(1);
			expect(frames(a.platform)[0].data.recovery).toEqual(decided);
		});

		it('passes a valid recovered event without a refetch hint through untouched', async () => {
			const a = await instance();
			const b = await instance();
			a.wrapped.publish('__realtime', 'recovered', { at: 1, recovery: { custom: 'x' } });
			await tick();
			await hold();
			expect(frames(b.platform)).toHaveLength(1);
			expect(frames(b.platform)[0].data).toEqual({ at: 1, recovery: { custom: 'x' } });
		});

		it('delivers a peer request unchanged on a bus without a breaker', async () => {
			const a = await instance();
			const p = mockPlatform();
			const plain = createPubSubBus(client);
			await plain.activate(p);
			cleanups.push(() => plain.deactivate());
			a.breaker.failure();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			a.breaker.reset();
			await tick();
			expect(frames(p)).toHaveLength(1);
			expect(frames(p)[0].data.recovery).toEqual({ refetch: true });
		});

		it('sends the request again when it was itself lost', async () => {
			const a = await instance();
			const b = await instance();
			a.breaker.failure();
			a.wrapped.publish('chat', 'msg', { n: 1 });
			await tick();
			const healed = failPublishes();
			a.breaker.reset();
			await tick();
			await hold();
			expect(frames(b.platform)).toHaveLength(0);
			healed();
			// The failed request tripped the breaker again; its recovery resends it.
			a.breaker.reset();
			await tick();
			await hold();
			expect(refetches(b.platform)).toHaveLength(1);
		});
	});

	describe('hooks.open', () => {
		it('exposes an async open hook', () => {
			expect(bus.hooks).toBeDefined();
			expect(typeof bus.hooks.open).toBe('function');
		});

		it('subscribes the connection to the default systemChannel', async () => {
			const ws = mockWs();
			await bus.hooks.open(ws, { platform });

			expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__realtime')).toBe(true);
			expect(ws.isSubscribed('__realtime')).toBe(true);

			await bus.deactivate();
		});

		it('subscribes the connection to a custom systemChannel', async () => {
			const customBus = createPubSubBus(client, { systemChannel: '__status' });
			const ws = mockWs();

			await customBus.hooks.open(ws, { platform });

			expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__status')).toBe(true);
			expect(ws.isSubscribed('__status')).toBe(true);

			await customBus.deactivate();
		});

		it('does not subscribe when systemChannel is null', async () => {
			const noChBus = createPubSubBus(client, { systemChannel: null });
			const ws = { id: 'w1' };

			await noChBus.hooks.open(ws, { platform });

			expect(platform.subscribed).toEqual([]);

			await noChBus.deactivate();
		});

		it('does not subscribe when systemChannel is false', async () => {
			const noChBus = createPubSubBus(client, { systemChannel: false });
			const ws = { id: 'w1' };

			await noChBus.hooks.open(ws, { platform });

			expect(platform.subscribed).toEqual([]);

			await noChBus.deactivate();
		});

		// Every console.warn line printed while `fn` runs, with the bus's
		// subscribe refusal given time to settle.
		async function warnings(fn) {
			const lines = [];
			const spy = vi.spyOn(console, 'warn').mockImplementation((line) => { lines.push(line); });
			try { await fn(); await settle(); } finally { spy.mockRestore(); }
			return lines;
		}

		// A bus that publishes degraded / recovered, which needs a breaker.
		function healthBus() {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			return createPubSubBus(client, { breaker });
		}

		it('reports a refused systemChannel subscribe once', async () => {
			const healthy = healthBus();
			const first = mockWs(), second = mockWs();
			first.close();
			second.close();
			const lines = await warnings(async () => {
				await healthy.hooks.open(first, { platform });
				await healthy.hooks.open(second, { platform });
			});

			expect(lines).toHaveLength(1);
			const record = registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBSUB_SYSTEM_CHANNEL_REFUSED);
			expect(record.attributes).toEqual({ reason: 'INTERNAL_ERROR' });

			await healthy.deactivate();
		});

		it('does not consult the application subscribe hook for the bus-owned channel', async () => {
			platform.subscribe = vi.fn(() => 'FORBIDDEN');
			const healthy = healthBus();
			const lines = await warnings(() => healthy.hooks.open(mockWs(), { platform }));

			expect(platform.subscribe).not.toHaveBeenCalled();
			expect(lines).toEqual([]);

			await healthy.deactivate();
		});

		// A rejection that escaped would surface here instead of in the run.
		async function unhandledDuring(fn) {
			const seen = [];
			const onUnhandled = (reason) => { seen.push(reason); };
			process.on('unhandledRejection', onUnhandled);
			try { await fn(); await settle(); } finally { process.off('unhandledRejection', onUnhandled); }
			return seen;
		}

		it('contains a refused systemChannel subscribe and still activates', async () => {
			const healthy = healthBus();
			const activate = vi.spyOn(healthy, 'activate');
			const ws = mockWs();
			ws.close();
			let lines;
			const escaped = await unhandledDuring(async () => {
				lines = await warnings(() => healthy.hooks.open(ws, { platform }));
			});

			expect(escaped).toEqual([]);
			expect(activate).toHaveBeenCalledWith(platform);
			expect(lines).toHaveLength(1);
			expect(registeredRecord(lines[0], EXTENSIONS_ERROR_IDS.PUBSUB_SYSTEM_CHANNEL_REFUSED).attributes).toEqual({ reason: 'INTERNAL_ERROR' });

			await healthy.deactivate();
		});

		it('does not invoke a throwing application subscribe hook', async () => {
			platform.subscribe = () => { throw new Error('platform failure'); };
			const activate = vi.spyOn(bus, 'activate');
			const escaped = await unhandledDuring(() => bus.hooks.open(mockWs(), { platform }));

			expect(escaped).toEqual([]);
			expect(activate).toHaveBeenCalledWith(platform);

			await bus.deactivate();
		});

		it('reports nothing when the systemChannel subscribe succeeds', async () => {
			const healthy = healthBus();
			const lines = await warnings(() => healthy.hooks.open(mockWs(), { platform }));

			expect(lines).toEqual([]);

			await healthy.deactivate();
		});

		it('reports nothing on a bus without a breaker, which publishes no degraded or recovered', async () => {
			const ws = mockWs();
			ws.close();
			const lines = await warnings(() => bus.hooks.open(ws, { platform }));

			expect(lines).toEqual([]);

			await bus.deactivate();
		});

		it('does not wait for an application subscribe hook', async () => {
			platform.subscribe = vi.fn(() => new Promise(() => {}));
			const healthy = healthBus();
			const opened = await Promise.race([
				healthy.hooks.open(mockWs(), { platform }).then(() => 'opened'),
				new Promise((resolve) => setTimeout(() => resolve('waiting'), 500))
			]);

			expect(opened).toBe('opened');
			expect(platform.subscribe).not.toHaveBeenCalled();

			await healthy.deactivate();
		});

		it('activates the Redis subscriber on first call', async () => {
			let duplicates = 0;
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				duplicates++;
				return origDuplicate(overrides);
			};

			expect(duplicates).toBe(0);
			await bus.hooks.open({ id: 'w1' }, { platform });
			expect(duplicates).toBe(1);

			await bus.deactivate();
		});

		it('activate is idempotent across many open calls', async () => {
			let duplicates = 0;
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				duplicates++;
				return origDuplicate(overrides);
			};

			const sockets = [mockWs(), mockWs(), mockWs()];
			for (const ws of sockets) await bus.hooks.open(ws, { platform });

			expect(duplicates).toBe(1);
			for (const ws of sockets) expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__realtime')).toBe(true);

			await bus.deactivate();
		});

		it('survives a missing platform without throwing', async () => {
			await expect(bus.hooks.open({ id: 'w1' }, {})).resolves.toBeUndefined();
			await expect(bus.hooks.open({ id: 'w1' }, undefined)).resolves.toBeUndefined();
			expect(platform.subscribed).toEqual([]);
		});

		it('survives destructuring (open detached from bus.hooks)', async () => {
			let duplicates = 0;
			const origDuplicate = client.duplicate.bind(client);
			client.duplicate = (overrides) => {
				duplicates++;
				return origDuplicate(overrides);
			};

			const { open } = bus.hooks;
			const ws = mockWs();

			await open(ws, { platform });

			expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__realtime')).toBe(true);
			expect(duplicates).toBe(1);

			await bus.deactivate();
		});

		it('subscribe is recorded for the WS and the publish goes to the same topic', async () => {
			// The mock platform does not route publish to its subscribed
			// set, so this is the unit-shape of the bug fix: the WS is
			// recorded in subscribers, AND degraded fires on the same
			// topic. The end-to-end routing assertion lives in the
			// integration test.
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const wiredBus = createPubSubBus(client, { breaker });
			const ws = mockWs();

			await wiredBus.hooks.open(ws, { platform });

			expect(ws.getUserData()[WS_SUBSCRIPTIONS].has('__realtime')).toBe(true);

			platform.reset();
			breaker.failure();

			const degradedFrame = platform.published.find((p) => p.topic === '__realtime' && p.event === 'degraded');
			expect(degradedFrame).toBeDefined();

			await wiredBus.deactivate();
			breaker.destroy();
		});
	});
});

/**
 * Presence-field merge parity across backends.
 *
 * The room presence stream carries field-level deltas as a regular pub/sub
 * frame on the `<roomTopic>:presence` sub-topic with event `update` and data
 * `{ key, ...fields }`. The same browser bundle merges that frame whether the
 * room runs single-process (in-memory) or clustered (redis-backed), so the
 * redis bus MUST relay and redeliver the frame with the field payload intact
 * and without an echo loop. If the redis path mangled the shape, an entry's
 * typing / selection / lock fields would diverge between backends and the
 * shared client merge would break in the cluster only.
 */
describe('presence-field update frame: redis relay parity', () => {
	let client;
	let platform;
	let bus;

	beforeEach(() => {
		client = mockRedisClient();
		platform = mockPlatform();
		bus = createPubSubBus(client);
	});

	it('relays a presence update frame with the field payload intact', async () => {
		const calls = [];
		const orig = client.redis.publish;
		client.redis.publish = async (ch, msg) => {
			calls.push({ ch, parsed: JSON.parse(msg) });
			return orig.call(client.redis, ch, msg);
		};

		const wrapped = bus.wrap(platform);
		wrapped.publish('room:42:presence', 'update', { key: 'u1', typing: true });
		await new Promise((r) => setTimeout(r, 5));

		// Locally delivered identically to the in-memory path (no relay marker
		// on the origin side, full field payload preserved).
		expect(platform.published).toHaveLength(1);
		expect(platform.published[0]).toEqual({
			topic: 'room:42:presence',
			event: 'update',
			data: { key: 'u1', typing: true },
			options: undefined
		});

		// Relayed cross-instance as a single envelope carrying the same frame.
		expect(calls).toHaveLength(1);
		expect(calls[0].parsed.topic).toBe('room:42:presence');
		expect(calls[0].parsed.event).toBe('update');
		expect(calls[0].parsed.data).toEqual({ key: 'u1', typing: true });
	});

	it('redelivers a remote presence update frame with relay:false and identical data', async () => {
		await bus.activate(platform);
		platform.reset();

		// A peer worker published a multi-field presence update (typing cleared,
		// a selection range set, and a lock acquired in one frame). The receiver
		// must hand it to the local platform byte-for-byte so the client merge
		// shallow-merges the same fields it would in-memory.
		const fields = {
			key: 'u7',
			typing: false,
			selection: { start: 3, end: 9 },
			'lock:title': 'u7'
		};
		const envelope = JSON.stringify({
			instanceId: 'peer-worker',
			topic: 'room:42:presence',
			event: 'update',
			data: fields
		});
		await client.redis.publish('uws:pubsub', envelope);

		expect(platform.published).toHaveLength(1);
		expect(platform.published[0]).toEqual({
			topic: 'room:42:presence',
			event: 'update',
			data: fields,
			options: { relay: false }
		});
	});

	it('does not echo a self-published presence update back into the local platform', async () => {
		await bus.activate(platform);
		const wrapped = bus.wrap(platform);

		wrapped.publish('room:42:presence', 'update', { key: 'me', typing: true });

		// Let the relay tick fire so the self-published envelope round-trips back
		// through the bus subscriber; without this await the echo never arrives and
		// the assertion is a tautology. Exactly one local delivery (the direct wrap
		// publish); the subscriber must drop the echoed envelope so the typer is not
		// double-applied.
		await new Promise((r) => setTimeout(r, 5));
		expect(platform.published).toHaveLength(1);
	});
});


it('keeps batch-size buckets in message counts when duration defaults use seconds', async () => {
	const metrics = createMetrics();
	const bus = createPubSubBus(mockRedisClient('batch-buckets:'), { metrics });
	const platform = mockPlatform();
	try {
		await bus.activate(platform);
		const wrapped = bus.wrap(platform);
		for (let i = 0; i < 7; i++) wrapped.publish('room', 'event', i);
		await new Promise((resolve) => setTimeout(resolve, 10));
		const out = metrics.serialize();
		expect(out).toContain('pubsub_relay_batch_size_bucket{le="5"} 0');
		expect(out).toContain('pubsub_relay_batch_size_bucket{le="10"} 1');
		expect(out).toContain('pubsub_relay_batch_size_sum 7');
	} finally {
		await bus.deactivate();
	}
});

describe('redis pubsub concurrent activation', () => {
	const remote = JSON.stringify({ instanceId: 'other-instance', topic: 'chat', event: 'msg', data: { text: 'remote' } });

	it('shares one subscriber between activations that overlap', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		let duplicates = 0;
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			duplicates++;
			return duplicate(overrides);
		};
		const shared = createPubSubBus(client);
		try {
			await Promise.all([shared.activate(platform), shared.activate(platform), shared.activate(platform)]);
			expect(duplicates).toBe(1);
			await client.redis.publish('uws:pubsub', remote);
			expect(platform.published).toHaveLength(1);
		} finally {
			await shared.deactivate();
		}
	});

	it('rejects every overlapping activation with the subscribe error and releases the connection', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const subscribers = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			sub.subscribe = () => Promise.reject(new Error('subscribe refused'));
			sub.quit = () => Promise.reject(new Error('Connection is closed.'));
			sub.disconnect = vi.fn();
			subscribers.push(sub);
			return sub;
		};
		// One failed SUBSCRIBE counts once, however many connections waited on it.
		const breaker = createCircuitBreaker({ failureThreshold: 2 });
		const failing = createPubSubBus(client, { breaker });
		const escaped = [];
		const onUnhandled = (reason) => { escaped.push(reason); };
		process.on('unhandledRejection', onUnhandled);
		try {
			const results = await Promise.allSettled([failing.activate(platform), failing.activate(platform)]);
			expect(results.map((result) => result.reason?.message)).toEqual(['subscribe refused', 'subscribe refused']);
			expect(breaker.state).toBe('healthy');
			await new Promise((resolve) => setImmediate(resolve));
			expect(subscribers).toHaveLength(1);
			expect(subscribers[0].disconnect).toHaveBeenCalledTimes(1);
			expect(escaped).toEqual([]);
		} finally {
			process.off('unhandledRejection', onUnhandled);
			breaker.destroy();
		}
	});

	it('honours a deactivate() that a breaker listener runs while the activation succeeds', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const subscribers = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			sub.disconnect = vi.fn();
			subscribers.push(sub);
			return sub;
		};
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
		const bus = createPubSubBus(client, { breaker });
		let deactivating;
		const unsubscribe = breaker.subscribe((from, to) => {
			if (to === 'healthy') deactivating = bus.deactivate();
		});
		try {
			breaker.failure(new Error('down'));
			await new Promise((resolve) => setTimeout(resolve, 5));
			expect(breaker.state).toBe('probing');
			await bus.activate(platform);
			await deactivating;
			expect(subscribers).toHaveLength(1);
			expect(subscribers[0].disconnect).toHaveBeenCalledTimes(1);
			await client.redis.publish('uws:pubsub', remote);
			expect(platform.published.filter((p) => p.topic === 'chat')).toEqual([]);
		} finally {
			unsubscribe();
			breaker.destroy();
		}
	});

	it('delivers once when activated again while the closed subscriber still receives', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const subscribers = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			// The mock connection keeps receiving after it is closed, as a real
			// one can for messages already read off its socket.
			sub.disconnect = vi.fn();
			subscribers.push(sub);
			return sub;
		};
		const bus = createPubSubBus(client);
		try {
			await bus.activate(platform);
			const closing = bus.deactivate();
			await bus.activate(platform);
			await client.redis.publish('uws:pubsub', remote);
			expect(platform.published).toHaveLength(1);
			await closing;
			expect(subscribers).toHaveLength(2);
			expect(subscribers[0].disconnect).toHaveBeenCalledTimes(1);
			await client.redis.publish('uws:pubsub', remote);
			expect(platform.published).toHaveLength(2);
		} finally {
			await bus.deactivate();
		}
	});
});

describe('redis pubsub lifecycle', () => {
	const remote = JSON.stringify({ instanceId: 'other-instance', topic: 'chat', event: 'msg', data: { text: 'remote' } });
	const within = (promise, ms = 50) => Promise.race([
		promise.then(() => 'resolved', (err) => 'rejected: ' + err.message),
		new Promise((resolve) => setTimeout(() => resolve('pending'), ms))
	]);

	// Subscriber connections whose SUBSCRIBE settles only when the test says
	// so, or never, as one waiting in the offline queue of a connection
	// closed between two reconnect attempts.
	function heldClient() {
		const client = mockRedisClient();
		const subscribers = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			const subscribe = sub.subscribe.bind(sub);
			sub.subscribe = (...channels) => new Promise((resolve, reject) => {
				sub.settle = (err) => (err ? reject(err) : subscribe(...channels).then(resolve));
			});
			sub.disconnect = vi.fn();
			subscribers.push(sub);
			return sub;
		};
		return { client, subscribers };
	}

	it('stays inactive when deactivate() runs while an activation is in flight', async () => {
		const { client, subscribers } = heldClient();
		const platform = mockPlatform();
		const bus = createPubSubBus(client);
		const activating = bus.activate(platform);
		await bus.deactivate();
		subscribers[0].settle();
		await expect(activating).rejects.toThrow('deactivated during activation');
		expect(subscribers[0].disconnect).toHaveBeenCalledTimes(1);
		await client.redis.publish('uws:pubsub', remote);
		expect(platform.published).toEqual([]);
	});

	it('settles an activation whose SUBSCRIBE never settles when deactivate() runs', async () => {
		const { client } = heldClient();
		const bus = createPubSubBus(client);
		const activating = bus.activate(mockPlatform());
		await bus.deactivate();
		expect(await within(activating)).toBe('rejected: pubsub bus: deactivated during activation');
	});

	it('gives a cancelled activation\'s breaker probe back, so the breaker is not left probing with no probe', async () => {
		const { client } = heldClient();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
		try {
			breaker.failure(new Error('down'));
			await new Promise((resolve) => setTimeout(resolve, 5));
			expect(breaker.state).toBe('probing');
			const bus = createPubSubBus(client, { breaker });
			const activating = bus.activate(mockPlatform());
			await bus.deactivate();
			await within(activating);
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
		} finally {
			breaker.destroy();
		}
	});

	it('gives the probe back when opening the subscriber connection throws', async () => {
		const client = mockRedisClient();
		client.duplicate = () => { throw new Error('duplicate failed'); };
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
		try {
			breaker.failure(new Error('down'));
			await new Promise((resolve) => setTimeout(resolve, 5));
			const bus = createPubSubBus(client, { breaker });
			vi.spyOn(console, 'error').mockImplementation(() => {});
			await expect(bus.activate(mockPlatform())).rejects.toThrow('duplicate failed');
			await bus.deactivate();
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
		} finally {
			vi.restoreAllMocks();
			breaker.destroy();
		}
	});

	it('keeps announcing breaker changes after an activation that deactivate() cancelled', async () => {
		const { client, subscribers } = heldClient();
		const platform = mockPlatform();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus(client, { breaker });
		try {
			const first = bus.activate(platform);
			await bus.deactivate();
			const second = bus.activate(platform);
			for (const sub of subscribers) sub.settle();
			await within(first);
			await second;
			breaker.failure(new Error('down'));
			expect(platform.published.filter((p) => p.event === 'degraded')).toHaveLength(1);
		} finally {
			await bus.deactivate();
			breaker.destroy();
		}
	});

	it('resolves deactivate() without waiting for Redis', async () => {
		const client = mockRedisClient();
		const duplicate = client.duplicate.bind(client);
		let sub;
		client.duplicate = (overrides) => {
			sub = duplicate(overrides);
			sub.disconnect = vi.fn();
			return sub;
		};
		const bus = createPubSubBus(client);
		await bus.activate(mockPlatform());
		// Redis unreachable: commands wait in the offline queue.
		sub.unsubscribe = () => new Promise(() => {});
		sub.quit = () => new Promise(() => {});
		expect(await within(bus.deactivate())).toBe('resolved');
		expect(sub.disconnect).toHaveBeenCalledTimes(1);
	});
});

describe('redis pubsub delivery across a reactivation', () => {
	// The first call to `method` on `from` deactivates the bus and activates it
	// on `to` synchronously, as a local handler of the delivered message could.
	function reactivateOnFirstCall(bus, from, to, method, pending) {
		const original = from[method].bind(from);
		let fired = false;
		from[method] = (...args) => {
			const result = original(...args);
			if (!fired) {
				fired = true;
				pending.push(bus.deactivate(), bus.activate(to));
			}
			return result;
		};
	}

	// Delivers one frame on the subscriber connection the bus opened last. Redis
	// sends a message only to the connections subscribed when it was published,
	// never to one a reactivation opens while it is being delivered.
	function trackSubscribers(client) {
		const subs = [];
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			subs.push(sub);
			return sub;
		};
		return (envelope) => subs[subs.length - 1]._listeners.get('message')('uws:pubsub', JSON.stringify(envelope));
	}

	it('publishes the rest of a batch envelope to the platform it arrived on', async () => {
		const client = mockRedisClient();
		const deliver = trackSubscribers(client);
		const bus = createPubSubBus(client);
		const first = mockPlatform();
		const next = mockPlatform();
		delete first.publishBatched;
		await bus.activate(first);
		first.reset();
		const pending = [];
		reactivateOnFirstCall(bus, first, next, 'publish', pending);
		try {
			deliver({
				instanceId: 'other-instance',
				batch: [
					{ topic: 'a', event: 'x', data: 1 },
					{ topic: 'b', event: 'y', data: 2 }
				]
			});
			expect(first.published.map((m) => m.data)).toEqual([1, 2]);
			expect(next.published).toEqual([]);
		} finally {
			await Promise.all(pending);
			await bus.deactivate();
		}
	});

	it('sends the rest of a coalesced envelope to the platform it arrived on', async () => {
		const client = mockRedisClient();
		const deliver = trackSubscribers(client);
		const bus = createPubSubBus(client);
		const first = mockPlatform();
		const next = mockPlatform();
		const wsA = { id: 'a' };
		const wsB = { id: 'b' };
		first.forEachSubscriber = (topic, fn) => { fn(wsA); fn(wsB); };
		next.forEachSubscriber = first.forEachSubscriber;
		await bus.activate(first);
		first.reset();
		const pending = [];
		reactivateOnFirstCall(bus, first, next, 'sendCoalesced', pending);
		try {
			deliver({
				instanceId: 'other-instance',
				coalesced: true,
				topic: 'prices',
				event: 'tick',
				data: { p: 5 },
				coalesceKey: 'btc'
			});
			expect(first.sentCoalesced.map((s) => s.ws)).toEqual([wsA, wsB]);
			expect(next.sentCoalesced).toEqual([]);
		} finally {
			await Promise.all(pending);
			await bus.deactivate();
		}
	});
});
