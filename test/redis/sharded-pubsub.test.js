import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

const TRACE_A = '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01';
const REDIS_ATTRIBUTES = { 'messaging.system': 'redis' };

// A mock Platform whose tracing provider is switched on, as the adapter's is
// when one is configured at build time.
function tracedPlatform(context = null) {
	const p = mockPlatform();
	p.trace.enabled = true;
	p.traceContext = context;
	return p;
}

// Every envelope that reached the given shard channels, as a raw sharded
// subscriber on its own connection sees it.
async function captureShards(client, channels) {
	const envelopes = [];
	const sub = client.duplicate();
	sub.on('smessage', (ch, message) => {
		if (channels.includes(ch)) envelopes.push(JSON.parse(message));
	});
	for (const ch of channels) await sub.ssubscribe(ch);
	return envelopes;
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

describe('redis sharded bus', () => {
	let client;
	let platform;

	beforeEach(() => {
		client = mockRedisClient();
		platform = mockPlatform();
	});

	describe('construction', () => {
		it('rejects a non-function shardKey', () => {
			expect(() => createShardedBus(client, { shardKey: 'topic' })).toThrow('shardKey must be a function');
		});

		it('rejects a non-string channelPrefix', () => {
			expect(() => createShardedBus(client, { channelPrefix: 5 })).toThrow('channelPrefix must be a string');
		});

		it('returns the expected API shape', () => {
			const bus = createShardedBus(client);
			expect(typeof bus.wrap).toBe('function');
			expect(typeof bus.activate).toBe('function');
			expect(typeof bus.deactivate).toBe('function');
			expect(typeof bus.follow).toBe('function');
			expect(typeof bus.unfollow).toBe('function');
			expect(typeof bus.hooks.subscribe).toBe('function');
		});
	});

	describe('activate version check', () => {
		it('throws on Redis < 7', async () => {
			client.redis._info = '# Server\nredis_version:6.2.7\n';
			const bus = createShardedBus(client);
			await expect(bus.activate(platform)).rejects.toThrow('requires Redis 7 or newer');
		});

		it('accepts Redis 7+', async () => {
			client.redis._info = '# Server\nredis_version:7.2.0\n';
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.deactivate();
		});

		it('throws on unparseable INFO output', async () => {
			client.redis._info = 'not a real info dump';
			const bus = createShardedBus(client);
			await expect(bus.activate(platform)).rejects.toThrow('could not parse Redis version');
		});

		it('is idempotent', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.activate(platform);
			await bus.deactivate();
		});
	});

	describe('publish via SPUBLISH', () => {
		it('routes per-topic by default', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			const calls = [];
			const origSpublish = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => {
				calls.push({ ch, msg });
				return origSpublish.call(client.redis, ch, msg);
			};

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat:room1', 'msg', { id: 1 });
			wrapped.publish('chat:room2', 'msg', { id: 2 });
			await new Promise((r) => setTimeout(r, 5));

			expect(calls.find((c) => c.ch === 'uws:sharded:chat:room1')).toBeDefined();
			expect(calls.find((c) => c.ch === 'uws:sharded:chat:room2')).toBeDefined();

			await bus.deactivate();
		});

		it('groups topics via shardKey', async () => {
			const bus = createShardedBus(client, {
				shardKey: (topic) => topic.split(':')[0]
			});
			await bus.activate(platform);

			const calls = [];
			const origSpublish = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => {
				calls.push({ ch, msg });
				return origSpublish.call(client.redis, ch, msg);
			};

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat:room1', 'msg', { id: 1 });
			wrapped.publish('chat:room2', 'msg', { id: 2 });
			await new Promise((r) => setTimeout(r, 5));

			// Both go to the 'chat' shard channel.
			const chatCalls = calls.filter((c) => c.ch === 'uws:sharded:chat');
			expect(chatCalls).toHaveLength(2);

			await bus.deactivate();
		});

		it('skips relay when relay: false is passed', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			const calls = [];
			client.redis.spublish = async (ch, msg) => { calls.push(ch); };

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { id: 1 }, { relay: false });
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(0);
			expect(platform.published).toHaveLength(1);

			await bus.deactivate();
		});
	});

	describe('publishBatched', () => {
		it('throws on non-array input', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			expect(() => wrapped.publishBatched('nope')).toThrow('publishBatched requires an array');
			expect(() => wrapped.publishBatched(null)).toThrow('publishBatched requires an array');

			await bus.deactivate();
		});

		it('no-ops on empty array', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => { calls.push(ch); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(0);
			expect(platform.publishedBatches).toHaveLength(0);

			await bus.deactivate();
		});

		it('calls platform.publishBatched locally', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			wrapped.publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: 1 },
				{ topic: 'chat:room2', event: 'msg', data: 2 }
			]);

			expect(platform.publishedBatches).toHaveLength(1);
			expect(platform.publishedBatches[0].messages).toHaveLength(2);

			await bus.deactivate();
		});

		it('groups messages by shard channel: one SPUBLISH envelope per channel', async () => {
			const bus = createShardedBus(client, {
				shardKey: (t) => t.split(':')[0]
			});
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => {
				calls.push({ ch, parsed: JSON.parse(msg) });
				return orig.call(client.redis, ch, msg);
			};

			// 4 messages across 2 shards (chat / audit).
			wrapped.publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: 1 },
				{ topic: 'chat:room2', event: 'msg', data: 2 },
				{ topic: 'audit:org1', event: 'created', data: 3 },
				{ topic: 'audit:org2', event: 'created', data: 4 }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(2);
			const chat = calls.find((c) => c.ch === 'uws:sharded:chat');
			const audit = calls.find((c) => c.ch === 'uws:sharded:audit');
			expect(chat.parsed.batch).toHaveLength(2);
			expect(audit.parsed.batch).toHaveLength(2);
			expect(chat.parsed.batch.map((m) => m.data)).toEqual([1, 2]);
			expect(audit.parsed.batch.map((m) => m.data)).toEqual([3, 4]);

			await bus.deactivate();
		});

		it('per-topic shardKey (default) ships one envelope per topic', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => { calls.push({ ch }); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: 1 },
				{ topic: 'chat:room2', event: 'msg', data: 2 }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(2);
			expect(calls.map((c) => c.ch).sort()).toEqual([
				'uws:sharded:chat:room1',
				'uws:sharded:chat:room2'
			]);

			await bus.deactivate();
		});

		it('honors per-message relay: false (excludes from envelope, keeps local)', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([
				{ topic: 'a', event: 'x', data: 1 },
				{ topic: 'a', event: 'y', data: 2, options: { relay: false } },
				{ topic: 'a', event: 'z', data: 3 }
			]);
			await new Promise((r) => setTimeout(r, 5));

			// One channel ('uws:sharded:a'), one envelope, two messages.
			expect(calls).toHaveLength(1);
			expect(calls[0].batch).toHaveLength(2);
			expect(calls[0].batch.map((m) => m.event)).toEqual(['x', 'z']);
			// Local fan-out gets all 3.
			expect(platform.publishedBatches[0].messages).toHaveLength(3);

			await bus.deactivate();
		});

		it('skips Redis entirely when every message has relay: false', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const wrapped = bus.wrap(platform);

			const calls = [];
			const orig = client.redis.spublish;
			client.redis.spublish = async (ch, msg) => { calls.push(ch); return orig.call(client.redis, ch, msg); };

			wrapped.publishBatched([
				{ topic: 'a', event: 'x', data: 1, options: { relay: false } },
				{ topic: 'b', event: 'y', data: 2, options: { relay: false } }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(calls).toHaveLength(0);

			await bus.deactivate();
		});

		it('cross-instance: receiver dispatches batched envelope via platform.publishBatched', async () => {
			const busA = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const busB = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const platformA = mockPlatform();
			const platformB = mockPlatform();

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');
			await busB.follow('chat:room2');

			const wrapped = busA.wrap(platformA);
			wrapped.publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: { id: 1 } },
				{ topic: 'chat:room2', event: 'msg', data: { id: 2 } }
			]);
			await new Promise((r) => setTimeout(r, 5));

			// B received the batched envelope and re-dispatched via publishBatched.
			expect(platformB.publishedBatches).toHaveLength(1);
			const local = platformB.publishedBatches[0].messages;
			expect(local).toHaveLength(2);
			expect(local[0].options).toEqual({ relay: false });
			expect(local[1].options).toEqual({ relay: false });

			await busA.deactivate();
			await busB.deactivate();
		});

		it('cross-instance: receiver re-coalesces a coalesced envelope onto local subscribers', async () => {
			const busA = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const busB = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const wsX = { id: 'x' };
			platformB.forEachSubscriber = (topic, fn) => { if (topic === 'chat:room1') fn(wsX); };

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');

			const wrapped = busA.wrap(platformA);
			wrapped.relayCoalesced('chat:room1', 'tick', { v: 9 }, 'k1');
			await new Promise((r) => setTimeout(r, 5));

			// B re-coalesced onto its local subscriber; never broadcast via publish.
			expect(platformB.sentCoalesced).toHaveLength(1);
			expect(platformB.sentCoalesced[0]).toEqual({ ws: wsX, key: 'chat:room1\0k1', topic: 'chat:room1', event: 'tick', data: { v: 9 } });
			expect(platformB.published).toHaveLength(0);

			await busA.deactivate();
			await busB.deactivate();
		});

		it('cross-instance: a receiver without publishBatched publishes each batched message on its own', async () => {
			const busA = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const busB = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			delete platformB.publishBatched;

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');
			await busB.follow('chat:room2');

			busA.wrap(platformA).publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: { id: 1 } },
				{ topic: 'chat:room2', event: 'msg', data: { id: 2 } }
			]);
			await new Promise((r) => setTimeout(r, 5));

			expect(platformB.published).toEqual([
				{ topic: 'chat:room1', event: 'msg', data: { id: 1 }, options: { relay: false } },
				{ topic: 'chat:room2', event: 'msg', data: { id: 2 }, options: { relay: false } }
			]);

			await busA.deactivate();
			await busB.deactivate();
		});

		it.each(['sendCoalesced', 'forEachSubscriber'])('cross-instance: a receiver without %s publishes the coalesced value', async (missing) => {
			const busA = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const busB = createShardedBus(client, { shardKey: (t) => t.split(':')[0] });
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			delete platformB[missing];

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');

			busA.wrap(platformA).relayCoalesced('chat:room1', 'tick', { v: 9 }, 'k1');
			await new Promise((r) => setTimeout(r, 5));

			// Unsequenced, like the coalesced send it stands in for.
			expect(platformB.published).toEqual([
				{ topic: 'chat:room1', event: 'tick', data: { v: 9 }, options: { relay: false, seq: false } }
			]);

			await busA.deactivate();
			await busB.deactivate();
		});

		it('echo-suppresses the whole batched envelope on instanceId match', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat');

			const wrapped = bus.wrap(platform);
			wrapped.publishBatched([
				{ topic: 'chat', event: 'msg', data: 1 },
				{ topic: 'chat', event: 'msg', data: 2 }
			]);
			await new Promise((r) => setTimeout(r, 5));

			// Local fan-out happened (1 publishBatched call recorded), but the
			// SPUBLISHed echo did NOT round-trip back into a second call.
			expect(platform.publishedBatches).toHaveLength(1);

			await bus.deactivate();
		});
	});

	describe('wrap - new platform passthroughs', () => {
		it('exposes publishBatched, sendCoalesced, request, requestId, pressure, onPressure', () => {
			const bus = createShardedBus(client);
			const wrapped = bus.wrap(platform);
			expect(typeof wrapped.publishBatched).toBe('function');
			expect(typeof wrapped.sendCoalesced).toBe('function');
			expect(typeof wrapped.request).toBe('function');
			expect(typeof wrapped.requestId).toBe('string');
			expect(wrapped.pressure).toBeDefined();
			expect(typeof wrapped.onPressure).toBe('function');
		});

		it('sendCoalesced and request delegate to the underlying platform', async () => {
			const bus = createShardedBus(client);
			const wrapped = bus.wrap(platform);
			const ws = {};
			wrapped.sendCoalesced(ws, { key: 'cursor:doc-1', payload: { x: 10, y: 20 } });
			await wrapped.request(ws, 'confirm', { op: 'delete' });

			expect(platform.sentCoalesced).toHaveLength(1);
			expect(platform.requested).toHaveLength(1);
		});

		it('requestId / pressure getters track the underlying platform', () => {
			const bus = createShardedBus(client);
			platform.requestId = 'req-9';
			const wrapped = bus.wrap(platform);
			expect(wrapped.requestId).toBe('req-9');

			platform._setPressure({
				active: true, reason: 'SUBSCRIBERS',
				subscriberRatio: 0.95, publishRate: 0, memoryMB: 0
			});
			expect(wrapped.pressure.reason).toBe('SUBSCRIBERS');
		});

		it('topicEpoch() routes to the replay tracker cache when one is wired', () => {
			// The subscribe-ack epoch must carry the same generation domain the
			// resume hook compares against. With a replay tracker present, that
			// is the tracker's per-topic cached generation, not the source
			// platform's single-worker value.
			const bus = createShardedBus(client);
			platform.replay = { cachedEpoch: (t) => (t === 'chat' ? 7 : 3) };
			const wrapped = bus.wrap(platform);
			expect(wrapped.topicEpoch('chat')).toBe(7);
			expect(wrapped.topicEpoch('other')).toBe(3);
		});

		it('topicEpoch() falls back to the source platform when no tracker is wired', () => {
			const bus = createShardedBus(client);
			const wrapped = bus.wrap(platform);
			// mockPlatform.topicEpoch returns the baseline 0 by default.
			expect(wrapped.topicEpoch('chat')).toBe(0);
			platform.topicEpoch = () => 42;
			expect(wrapped.topicEpoch('chat')).toBe(42);
		});
	});

	describe('follow / unfollow', () => {
		it('SSUBSCRIBE on first follow, SUNSUBSCRIBE on last unfollow', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			let ssubs = 0;
			let sunsubs = 0;
			const origSsub = client.redis.duplicate;
			// Walk the existing duplicate's spy hook indirectly via mock. We
			// instrument the wrapped client's redis instead, but the bus
			// holds its OWN duplicate. Simplest path: override duplicate
			// before activate so we get a hookable instance.
			// Already activated above, so reach into pubsubHandlers.
			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			const dupListeners = dup.listeners;
			expect(dupListeners.has('smessage')).toBe(true);

			await bus.follow('chat:room1');
			expect(dup.shardedChannels.has('uws:sharded:chat:room1')).toBe(true);

			await bus.follow('chat:room1'); // second follow, no SSUBSCRIBE
			await bus.unfollow('chat:room1'); // back to 1
			expect(dup.shardedChannels.has('uws:sharded:chat:room1')).toBe(true);

			await bus.unfollow('chat:room1'); // last
			expect(dup.shardedChannels.has('uws:sharded:chat:room1')).toBe(false);

			await bus.deactivate();
		});

		it('shares a channel across topics with the same shardKey', async () => {
			const bus = createShardedBus(client, {
				shardKey: (t) => t.split(':')[0]
			});
			await bus.activate(platform);

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];

			await bus.follow('chat:room1');
			await bus.follow('chat:room2');
			expect(dup.shardedChannels.size).toBe(1);
			expect(dup.shardedChannels.has('uws:sharded:chat')).toBe(true);

			await bus.unfollow('chat:room1');
			// chat:room2 still followed, channel stays.
			expect(dup.shardedChannels.has('uws:sharded:chat')).toBe(true);

			await bus.unfollow('chat:room2');
			expect(dup.shardedChannels.has('uws:sharded:chat')).toBe(false);

			await bus.deactivate();
		});

		it('throws if follow is called before activate', async () => {
			const bus = createShardedBus(client);
			await expect(bus.follow('chat')).rejects.toThrow('activate() must be called before follow()');
		});
	});

	describe('cross-instance delivery', () => {
		it('forwards smessage to the local platform with relay: false', async () => {
			// Two buses sharing the same client to simulate two replicas.
			const busA = createShardedBus(client);
			const busB = createShardedBus(client);
			const platformA = mockPlatform();
			const platformB = mockPlatform();

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busA.follow('chat');
			await busB.follow('chat');

			const wrapped = busA.wrap(platformA);
			wrapped.publish('chat', 'msg', { from: 'A' });
			await new Promise((r) => setTimeout(r, 5));

			// B received via smessage (its instanceId differs from A's).
			expect(platformB.published.find((p) => p.event === 'msg')).toBeDefined();
			expect(platformB.published.find((p) => p.event === 'msg').options).toEqual({ relay: false });

			await busA.deactivate();
			await busB.deactivate();
		});

		it('carries a traced producer context to a traced peer, outside application data', async () => {
			const busA = createShardedBus(client);
			const busB = createShardedBus(client);
			const platformA = tracedPlatform({ traceparent: TRACE_A });
			const platformB = tracedPlatform();
			await busA.activate(platformA);
			await busB.activate(platformB);
			await busA.follow('chat');
			await busB.follow('chat');

			const wrapped = busA.wrap(platformA);
			expect(wrapped.trace).toBe(platformA.trace);
			wrapped.publish('chat', 'msg', { traceparent: 'application-data' });
			await settle();

			const delivered = platformB.published.find((entry) => entry.event === 'msg');
			expect(delivered.data).toEqual({ traceparent: 'application-data' });
			// Transport fields never become a publish option on the peer.
			expect(delivered.options).toEqual({ relay: false });
			expect(platformA.traceOperations).toEqual([
				{ name: 'redis.sharded-pubsub.publish', options: { kind: 'producer', parent: { traceparent: TRACE_A }, attributes: REDIS_ATTRIBUTES } }
			]);
			expect(platformB.traceOperations).toEqual([
				{ name: 'redis.sharded-pubsub.receive', options: { kind: 'consumer', parent: { traceparent: TRACE_A }, attributes: REDIS_ATTRIBUTES } }
			]);
			await busA.deactivate();
			await busB.deactivate();
		});

		it('on a Platform without a tracing provider reads no trace context and relays untraced envelopes', async () => {
			const bus = createShardedBus(client);
			let reads = 0;
			Object.defineProperty(platform, 'traceContext', {
				get() { reads++; return { traceparent: TRACE_A }; },
				configurable: true
			});
			platform.publishWire = () => true;
			const envelopes = await captureShards(client, ['uws:sharded:chat']);
			const wrapped = bus.wrap(platform);

			wrapped.publish('chat', 'one', 1);
			wrapped.batch([{ topic: 'chat', event: 'b', data: 2 }]);
			wrapped.publishBatched([{ topic: 'chat', event: 'pb', data: 3 }]);
			wrapped.relayCoalesced('chat', 'c', 4, 'k');
			wrapped.publishWire('chat', 'w', 5, { shared: true, capability: 'cap' });
			await settle();

			expect(reads).toBe(0);
			expect(platform.traceOperations).toEqual([]);
			expect(envelopes).toHaveLength(5);
			for (const env of envelopes) {
				expect(env).not.toHaveProperty('traceparent');
				expect(env).not.toHaveProperty('tracestate');
			}
		});

		it('on a Platform without a tracing provider ignores a carried context on receive', async () => {
			const busA = createShardedBus(client);
			const busB = createShardedBus(client);
			const platformA = tracedPlatform({ traceparent: TRACE_A });
			await busA.activate(platformA);
			await busB.activate(platform);
			await busA.follow('chat');
			await busB.follow('chat');

			busA.wrap(platformA).publish('chat', 'msg', 1);
			await settle();

			expect(platform.published.filter((entry) => entry.event === 'msg')).toHaveLength(1);
			expect(platform.traceOperations).toEqual([]);
			expect(platform.traceRestored).toEqual([]);
			await busA.deactivate();
			await busB.deactivate();
		});

		it('makes batch, publishBatched and relayCoalesced one producer operation per call across shard channels', async () => {
			const bus = createShardedBus(client);
			const origin = tracedPlatform({ traceparent: TRACE_A });
			const envelopes = await captureShards(client, ['uws:sharded:a', 'uws:sharded:b', 'uws:sharded:c']);
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
			wrapped.relayCoalesced('c', 'z', 1, 'k');
			expect(origin.traceOperations).toHaveLength(3);
			await settle();

			for (const op of origin.traceOperations) {
				expect(op).toEqual({
					name: 'redis.sharded-pubsub.publish',
					options: { kind: 'producer', parent: { traceparent: TRACE_A }, attributes: REDIS_ATTRIBUTES }
				});
			}
			// Three batch envelopes, one publishBatched envelope per shard
			// channel (two), one coalesced.
			expect(envelopes).toHaveLength(6);
			expect(envelopes.every((env) => env.traceparent === TRACE_A)).toBe(true);
		});

		it('opens no producer operation for a publish that does not relay', () => {
			const bus = createShardedBus(client);
			const origin = tracedPlatform({ traceparent: TRACE_A });
			origin.publishWire = () => true;
			const wrapped = bus.wrap(origin);

			wrapped.publish('chat', 'local', 1, { relay: false });
			wrapped.publishWire('chat', 'wire', 1, { shared: false, capability: 'cap' });

			expect(origin.traceOperations).toEqual([]);

			// Batches with nothing to relay open none either.
			wrapped.batch([]);
			wrapped.batch([{ topic: 'chat', event: 'b-local', data: 1, options: { relay: false } }]);
			wrapped.publishBatched([{ topic: 'chat', event: 'pb-local', data: 1, options: { relay: false } }]);
			expect(origin.traceOperations).toEqual([]);
		});

		it('delivers a traced batch locally before shardKey or the relay reads the entries', () => {
			const order = [];
			const bus = createShardedBus(client, { shardKey: (topic) => { order.push('shardKey'); return topic; } });
			const origin = tracedPlatform({ traceparent: TRACE_A });
			const rejection = new TypeError('platform rejects the batch');
			const refuseMalformed = (messages) => {
				order.push('local');
				if (!Array.isArray(messages) || messages.some((m) => !m)) throw rejection;
				return [];
			};
			origin.batch = refuseMalformed;
			origin.publishBatched = refuseMalformed;
			const wrapped = bus.wrap(origin);

			wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }]);
			expect(order).toEqual(['local', 'shardKey']);

			// A null entry or a non-array counts as relaying, so each takes the
			// traced path and reaches the platform, whose own error comes back
			// before shardKey sees anything.
			order.length = 0;
			expect(() => wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, null])).toThrow(rejection);
			expect(() => wrapped.batch([null])).toThrow(rejection);
			expect(() => wrapped.batch('not an array')).toThrow(rejection);
			expect(order).toEqual(['local', 'local', 'local']);
			// publishBatched checks for an array itself, traced or not.
			expect(() => wrapped.publishBatched('not an array')).toThrow('sharded bus: publishBatched requires an array of messages');
		});

		it('relays near-cap shard envelopes without their trace context and reports that once', async () => {
			const busA = createShardedBus(client, { maxEnvelopeBytes: 600 });
			const busB = createShardedBus(client, { maxEnvelopeBytes: 600 });
			const context = { traceparent: TRACE_A, tracestate: 'vendor=' + 'y'.repeat(200) };
			const platformA = tracedPlatform(context);
			const platformB = tracedPlatform();
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			await busA.activate(platformA);
			await busB.activate(platformB);
			await busA.follow('chat');
			await busB.follow('chat');
			const data = 'x'.repeat(400);
			busA.wrap(platformA).publish('chat', 'msg', data);
			busA.wrap(platformA).publish('chat', 'msg', data);
			await settle();

			const delivered = platformB.published.filter((entry) => entry.event === 'msg');
			expect(delivered.map((entry) => entry.data)).toEqual([data, data]);
			expect(platformB.traceOperations.map((op) => op.options.parent)).toEqual([null, null]);
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.sharded-pubsub.trace-context.omitted');
			warn.mockRestore();
			await busA.deactivate();
			await busB.deactivate();
		});

		it('does not report a shard envelope that is over the cap on its own as a trace omission', () => {
			const bus = createShardedBus(client, { maxEnvelopeBytes: 600 });
			const wrapped = bus.wrap(tracedPlatform({ traceparent: TRACE_A }));
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

			wrapped.publish('chat', 'big', 'x'.repeat(1000));
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.pubsub.envelope.oversized');
			warn.mockClear();

			wrapped.publish('chat', 'near', 'x'.repeat(480));
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.sharded-pubsub.trace-context.omitted');
			warn.mockRestore();
		});

		it('suppresses echo (same instanceId)', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat');

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { id: 1 });
			await new Promise((r) => setTimeout(r, 5));

			// Only the local platform.publish from wrap should be recorded;
			// the smessage from this bus's own SPUBLISH is echo-suppressed.
			expect(platform.published).toHaveLength(1);

			await bus.deactivate();
		});
	});

	describe('hooks helper', () => {
		const ws = {};

		it('subscribe calls follow, close unfollows everything', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			await bus.hooks.subscribe(ws, 'chat:room1', { platform });
			await bus.hooks.subscribe(ws, 'chat:room2', { platform });

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.has('uws:sharded:chat:room1')).toBe(true);
			expect(dup.shardedChannels.has('uws:sharded:chat:room2')).toBe(true);

			await bus.hooks.close(ws, { platform });
			expect(dup.shardedChannels.has('uws:sharded:chat:room1')).toBe(false);
			expect(dup.shardedChannels.has('uws:sharded:chat:room2')).toBe(false);

			await bus.deactivate();
		});

		it('skips topics starting with __', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			await bus.hooks.subscribe(ws, '__internal', { platform });
			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.size).toBe(0);

			await bus.deactivate();
		});

		it('subscribe is idempotent for the same ws+topic', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const w = {};
			await bus.hooks.subscribe(w, 'chat', { platform });
			await bus.hooks.subscribe(w, 'chat', { platform });

			// Second subscribe should be a no-op for this ws (already followed).
			// follow() still got incremented twice, but we don't expose that --
			// behavior we DO care about: deactivate cleans up cleanly.
			await bus.hooks.close(w, { platform });

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.size).toBe(0);

			await bus.deactivate();
		});
	});

	describe('breaker accounting', () => {
		it('records breaker failure when activate INFO query throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			client.redis.info = async () => { throw new Error('connection lost'); };
			const bus = createShardedBus(client, { breaker });

			await expect(bus.activate(platform)).rejects.toThrow('connection lost');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});
	});

	describe('followBatch', () => {
		it('SSUBSCRIBE happens once per channel for an N-topic batch', async () => {
			const bus = createShardedBus(client, {
				shardKey: (topic) => topic.split(':')[0]
			});
			await bus.activate(platform);

			const original = client.redis.duplicate;
			let ssubscribeCalls = 0;
			let lastChannelArgs = null;
			client.redis.duplicate = (opts) => {
				const dup = original.call(client.redis, opts);
				const orig = dup.ssubscribe.bind(dup);
				dup.ssubscribe = async (...channels) => {
					ssubscribeCalls++;
					lastChannelArgs = channels;
					return orig(...channels);
				};
				return dup;
			};
			// Re-activate so the duplicate is created fresh under the spy.
			await bus.deactivate();
			await bus.activate(platform);
			ssubscribeCalls = 0;

			await bus.followBatch([
				'chat:room1', 'chat:room2', 'audit:created', 'audit:updated'
			]);
			expect(ssubscribeCalls).toBe(1); // one batched call
			expect(lastChannelArgs.sort()).toEqual(['uws:sharded:audit', 'uws:sharded:chat']);

			await bus.deactivate();
			client.redis.duplicate = original;
		});

		it('empty array no-ops', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await expect(bus.followBatch([])).resolves.toBeUndefined();
			await bus.deactivate();
		});

		it('non-array input throws', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await expect(bus.followBatch('chat')).rejects.toThrow(/array of topics/);
			await bus.deactivate();
		});

		it('throws if called before activate()', async () => {
			const bus = createShardedBus(client);
			await expect(bus.followBatch(['chat'])).rejects.toThrow(/activate/);
		});

		it('duplicate topics in the input collapse to one refcount bump', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			await bus.followBatch(['chat', 'chat', 'chat']);
			// Single unfollow should be enough to drop the channel.
			await bus.unfollow('chat');

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.has('uws:sharded:chat')).toBe(false);

			await bus.deactivate();
		});

		it('a topic already followed via follow() does not re-SSUBSCRIBE in the batch', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			const original = client.redis.duplicate;
			let ssubscribeCalls = 0;
			client.redis.duplicate = (opts) => {
				const dup = original.call(client.redis, opts);
				const orig = dup.ssubscribe.bind(dup);
				dup.ssubscribe = async (...channels) => {
					ssubscribeCalls++;
					return orig(...channels);
				};
				return dup;
			};
			await bus.deactivate();
			await bus.activate(platform);

			ssubscribeCalls = 0;
			await bus.follow('chat');
			expect(ssubscribeCalls).toBe(1);

			await bus.followBatch(['chat', 'audit']);
			expect(ssubscribeCalls).toBe(2); // one new channel from the batch (audit only)

			await bus.deactivate();
			client.redis.duplicate = original;
		});

		it('rolls back refcounts when SSUBSCRIBE fails', async () => {
			let dupRef;
			const originalDuplicate = client.redis.duplicate;
			client.redis.duplicate = (opts) => {
				const dup = originalDuplicate.call(client.redis, opts);
				dupRef = dup;
				dup.ssubscribe = async () => { throw new Error('subscribe failed'); };
				return dup;
			};
			const bus = createShardedBus(client);
			await bus.activate(platform);

			await expect(bus.followBatch(['chat', 'audit'])).rejects.toThrow('subscribe failed');

			// After rollback, follow() of either topic should still trigger a
			// fresh SSUBSCRIBE (i.e., the refcount got fully unwound to zero).
			let calls = 0;
			dupRef.ssubscribe = async (...channels) => { calls += channels.length; return channels.length; };
			await bus.follow('chat');
			expect(calls).toBe(1);

			await bus.deactivate();
			client.redis.duplicate = originalDuplicate;
		});

		it('inbound messages on the new channels reach the local platform after followBatch', async () => {
			const bus = createShardedBus(client, {
				shardKey: (topic) => topic.split(':')[0]
			});
			await bus.activate(platform);

			await bus.followBatch(['chat:room1', 'audit:created']);

			// Drive a cross-instance smessage on the chat channel.
			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			const listener = dup.listeners.get('smessage');
			listener('uws:sharded:chat', JSON.stringify({
				instanceId: 'OTHER', topic: 'chat:room1', event: 'msg', data: { x: 1 }
			}));
			expect(platform.published.some((p) => p.topic === 'chat:room1' && p.event === 'msg')).toBe(true);

			await bus.deactivate();
		});
	});

	describe('hooks.subscribeBatch', () => {
		it('calls followBatch under the hood and tracks per-ws topics', async () => {
			const bus = createShardedBus(client, {
				shardKey: (topic) => topic.split(':')[0]
			});
			await bus.activate(platform);

			const ws = {};
			await bus.hooks.subscribeBatch(ws, ['chat:room1', 'chat:room2', 'audit:created'], { platform });

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.has('uws:sharded:chat')).toBe(true);
			expect(dup.shardedChannels.has('uws:sharded:audit')).toBe(true);

			// close should unfollow all three topics on this ws.
			await bus.hooks.close(ws, { platform });
			expect(dup.shardedChannels.size).toBe(0);

			await bus.deactivate();
		});

		it('skips __-prefixed topics in the batch', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			const ws = {};
			await bus.hooks.subscribeBatch(ws, ['chat', '__internal', '__realtime'], { platform });

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect([...dup.shardedChannels]).toEqual(['uws:sharded:chat']);

			await bus.deactivate();
		});

		it('skips topics this ws already follows so duplicates do not leak refcount', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);

			const ws = {};
			await bus.hooks.subscribeBatch(ws, ['chat'], { platform });
			await bus.hooks.subscribeBatch(ws, ['chat', 'audit'], { platform });

			// One unsubscribe per topic should remove from the ws's set; the
			// channel only goes away when refcount reaches zero.
			await bus.hooks.unsubscribe(ws, 'chat', { platform });
			await bus.hooks.unsubscribe(ws, 'audit', { platform });

			const handlers = client._pubsubHandlers;
			const dup = handlers[handlers.length - 1];
			expect(dup.shardedChannels.size).toBe(0);

			await bus.deactivate();
		});

		it('empty array no-ops', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const ws = {};
			await expect(bus.hooks.subscribeBatch(ws, [], { platform })).resolves.toBeUndefined();
			await bus.deactivate();
		});

		it('a non-array input is a silent no-op (matches the per-topic subscribe hook tolerance)', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			const ws = {};
			await expect(bus.hooks.subscribeBatch(ws, null, { platform })).resolves.toBeUndefined();
			await expect(bus.hooks.subscribeBatch(ws, undefined, { platform })).resolves.toBeUndefined();
			await bus.deactivate();
		});
	});

	describe('localSubjects', () => {
		it('enumerates followed topics with their live local count', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat:room1');
			await bus.follow('audit:org1');

			const counts = { 'chat:room1': 4, 'audit:org1': 2 };
			platform.subscribers = (t) => counts[t] || 0;

			const out = bus.localSubjects(platform);
			const byTopic = Object.fromEntries(out.map((s) => [s.topic, s.count]));
			expect(byTopic).toEqual({ 'chat:room1': 4, 'audit:org1': 2 });
			await bus.deactivate();
		});

		it('omits topics with 0 local subscribers', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat:room1');
			await bus.follow('empty:topic');

			platform.subscribers = (t) => t === 'chat:room1' ? 3 : 0;

			const out = bus.localSubjects(platform);
			expect(out).toEqual([{ topic: 'chat:room1', count: 3 }]);
			await bus.deactivate();
		});

		it('returns an empty array when nothing is followed', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			expect(bus.localSubjects(platform)).toEqual([]);
			await bus.deactivate();
		});

		it('returns an empty array when given a platform without subscribers()', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat:room1');
			expect(bus.localSubjects({})).toEqual([]);
			await bus.deactivate();
		});
	});

	describe('bus.subscribers (cluster-aware)', () => {
		it('falls back to the local count when no aggregator is wired', async () => {
			const bus = createShardedBus(client);
			await bus.activate(platform);
			await bus.follow('chat:room1');
			platform.subscribers = (t) => t === 'chat:room1' ? 5 : 0;
			expect(bus.subscribers('chat:room1')).toBe(5);
			expect(bus.subscribers('unknown')).toBe(0);
			await bus.deactivate();
		});

		it('returns 0 before activate (no platform reference yet)', () => {
			const bus = createShardedBus(client);
			expect(bus.subscribers('any')).toBe(0);
		});

		it('delegates to the wired aggregator when provided', async () => {
			const fakeAggregator = {
				subscribersOf(topic) {
					return topic === 'shared' ? 17 : 0;
				}
			};
			const bus = createShardedBus(client, { subscribersAggregator: fakeAggregator });
			await bus.activate(platform);
			await bus.follow('shared');
			expect(bus.subscribers('shared')).toBe(17);
			expect(bus.subscribers('other')).toBe(0);
			await bus.deactivate();
		});

		it('rejects an aggregator without subscribersOf', () => {
			expect(() => createShardedBus(client, { subscribersAggregator: {} })).toThrow(/subscribersOf/);
			expect(() => createShardedBus(client, { subscribersAggregator: null })).toThrow(/subscribersOf/);
		});
	});
});


describe.each(['subscribe', 'subscribeBatch'])('failed sharded %s ownership', (hook) => {
	it('allows retry and does not unfollow another socket after failure', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		let subscriber;
		const duplicate = client.redis.duplicate.bind(client.redis);
		client.redis.duplicate = (...args) => {
			subscriber = duplicate(...args);
			return subscriber;
		};
		const bus = createShardedBus(client);
		await bus.activate(platform);
		const subscribe = subscriber.ssubscribe.bind(subscriber);
		const a = {}, b = {};
		const follow = (ws) => bus.hooks[hook](ws, hook === 'subscribe' ? 'chat' : ['chat']);
		try {
			subscriber.ssubscribe = async () => { throw new Error('offline'); };
			await expect(follow(a)).rejects.toThrow('offline');
			subscriber.ssubscribe = subscribe;
			await follow(b);
			await bus.hooks.close(a);
			await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1 }));
			expect(platform.published).toHaveLength(1);
			await follow(a);
			await bus.hooks.close(b);
			await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 2 }));
			expect(platform.published).toHaveLength(2);
		} finally {
			await bus.deactivate();
		}
	});
});

describe.each(['follow', 'followBatch'])('sharded %s breaker admission', (method) => {
	it('rolls back a denied guard without recording another failure', async () => {
		const client = mockRedisClient();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createShardedBus(client, { breaker });
		const platform = mockPlatform();
		await bus.activate(platform);
		const follow = () => bus[method](method === 'follow' ? 'chat' : ['chat']);
		try {
			breaker.failure(new Error('offline'));
			const failure = vi.spyOn(breaker, 'failure');
			await expect(follow()).rejects.toThrow();
			expect(failure).not.toHaveBeenCalled();
			breaker.reset();
			await follow();
			await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1 }));
			expect(platform.published).toHaveLength(1);
			await bus.unfollow('chat');
			await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 2 }));
			expect(platform.published).toHaveLength(1);
		} finally {
			await bus.deactivate();
			breaker.destroy();
		}
	});

	it('returns a cancelled probe when the bus closes during subscribe', async () => {
		const client = mockRedisClient();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
		let resolveSubscribe;
		const duplicate = client.redis.duplicate.bind(client.redis);
		client.redis.duplicate = (...args) => {
			const sub = duplicate(...args);
			sub.ssubscribe = () => new Promise((resolve) => { resolveSubscribe = resolve; });
			return sub;
		};
		const bus = createShardedBus(client, { breaker });
		await bus.activate(mockPlatform());
		try {
			breaker.failure(new Error('offline'));
			await settle();
			const pending = bus[method](method === 'follow' ? 'chat' : ['chat']);
			const rejected = expect(pending).rejects.toThrow('closed during follow');
			await bus.deactivate();
			resolveSubscribe(1);
			await rejected;
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
		} finally {
			await bus.deactivate();
			breaker.destroy();
		}
	});
});


describe.each([['subscribe', 'subscribe'], ['subscribe', 'subscribeBatch'], ['subscribeBatch', 'subscribe'], ['subscribeBatch', 'subscribeBatch']])('concurrent sharded %s / %s', (firstHook, secondHook) => {
	for (const sameTopic of [true, false]) {
		it(`waits for shared admission and rolls both reservations back (same topic: ${sameTopic})`, async () => {
			const client = mockRedisClient();
			const platform = mockPlatform();
			let subscriber;
			const duplicate = client.redis.duplicate.bind(client.redis);
			client.redis.duplicate = (...args) => (subscriber = duplicate(...args));
			const bus = createShardedBus(client, { shardKey: () => 'shared' });
			await bus.activate(platform);
			const realSubscribe = subscriber.ssubscribe.bind(subscriber);
			let reject;
			subscriber.ssubscribe = vi.fn(() => new Promise((_, no) => { reject = no; }));
			const a = {}, b = {}, c = {};
			const secondTopic = sameTopic ? 'chat' : 'audit';
			const follow = (hook, ws, topic) => bus.hooks[hook](ws, hook === 'subscribe' ? topic : [topic]);
			try {
				const first = follow(firstHook, a, 'chat');
				let secondSettled = false;
				const second = follow(secondHook, b, secondTopic).finally(() => { secondSettled = true; });
				const outcomes = Promise.allSettled([first, second]);
				await settle();
				expect(secondSettled).toBe(false);
				expect(subscriber.ssubscribe).toHaveBeenCalledTimes(1);
				reject(new Error('offline'));
				expect((await outcomes).map((result) => result.status)).toEqual(['rejected', 'rejected']);
				subscriber.ssubscribe = realSubscribe;
				await follow('subscribe', c, 'chat');
				await bus.hooks.close(a);
				await bus.hooks.close(b);
				await client.redis.spublish('uws:sharded:shared', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1 }));
				expect(platform.published).toHaveLength(1);
				await follow(firstHook, a, 'chat');
				await bus.hooks.close(c);
				await client.redis.spublish('uws:sharded:shared', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 2 }));
				expect(platform.published).toHaveLength(2);
			} finally { await bus.deactivate(); }
		});
	}
});


it('keeps the second follower after a shared pending admission succeeds', async () => {
	const client = mockRedisClient();
	let subscriber;
	const duplicate = client.redis.duplicate.bind(client.redis);
	client.redis.duplicate = (...args) => (subscriber = duplicate(...args));
	const platform = mockPlatform();
	const bus = createShardedBus(client);
	await bus.activate(platform);
	const subscribe = subscriber.ssubscribe.bind(subscriber);
	let release;
	subscriber.ssubscribe = (...channels) => new Promise((resolve) => { release = () => resolve(subscribe(...channels)); });
	try {
		const first = bus.follow('chat');
		const second = bus.followBatch(['chat']);
		release();
		await Promise.all([first, second]);
		await bus.unfollow('chat');
		await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1 }));
		expect(platform.published).toHaveLength(1);
		await bus.unfollow('chat');
		await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 2 }));
		expect(platform.published).toHaveLength(1);
	} finally { await bus.deactivate(); }
});


it('clears successful admission flags when a released channel cleanup fails', async () => {
	const client = mockRedisClient();
	let subscriber;
	const duplicate = client.redis.duplicate.bind(client.redis);
	client.redis.duplicate = (...args) => (subscriber = duplicate(...args));
	const platform = mockPlatform();
	const bus = createShardedBus(client);
	await bus.activate(platform);
	const subscribe = subscriber.ssubscribe.bind(subscriber);
	const unsubscribe = subscriber.sunsubscribe.bind(subscriber);
	let release;
	subscriber.ssubscribe = (...channels) => new Promise((resolve) => { release = () => resolve(subscribe(...channels)); });
	subscriber.sunsubscribe = vi.fn().mockRejectedValueOnce(new Error('unsubscribe failed')).mockImplementation(unsubscribe);
	try {
		const batch = bus.followBatch(['chat', 'audit']);
		const failed = expect(batch).rejects.toThrow('unsubscribe failed');
		await bus.unfollow('audit');
		release();
		await failed;
		subscriber.ssubscribe = vi.fn(subscribe);
		await bus.follow('chat');
		expect(subscriber.ssubscribe).toHaveBeenCalledTimes(1);
		await client.redis.spublish('uws:sharded:chat', JSON.stringify({ instanceId: 'peer', topic: 'chat', event: 'msg', data: 1 }));
		expect(platform.published).toHaveLength(1);
	} finally { await bus.deactivate(); }
});

describe('sharded channel binding', () => {
	it.each(['single', 'wire', 'coalesced', 'batch'])('rejects forged topics in %s envelopes', async (kind) => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const bus = createShardedBus(client);
		await bus.activate(platform);
		await bus.follow('public:room');
		try {
			platform.forEachSubscriber = (_topic, fn) => fn({});
			platform.sendCoalesced = vi.fn();
			const forged = { topic: 'private:payroll', event: 'msg', data: 'forged' };
			const env = { instanceId: 'peer', ...forged };
			if (kind === 'wire') Object.assign(env, { wire: true, capability: 'missing' });
			if (kind === 'coalesced') env.coalesced = true;
			if (kind === 'batch') env.batch = [forged];
			await client.redis.spublish('uws:sharded:public', JSON.stringify(env));
			expect(platform.published).toEqual([]);
			expect(platform.publishedBatches).toEqual([]);
			expect(platform.sendCoalesced).not.toHaveBeenCalled();
		} finally { await bus.deactivate(); }
	});

	it('keeps valid batch members and supports custom shard keys', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const bus = createShardedBus(client, { shardKey: (topic) => topic.split('/')[0] });
		await bus.activate(platform);
		await bus.follow('tenantA/room');
		try {
			await client.redis.spublish('uws:sharded:tenantA', JSON.stringify({ instanceId: 'peer', batch: [
				{ topic: 'tenantB/payroll', event: 'msg', data: 'forged' },
				{ topic: 'tenantA/room', event: 'msg', data: 'valid' }
			] }));
			expect(platform.publishedBatches).toHaveLength(1);
			expect(platform.publishedBatches[0].messages.map((message) => message.topic)).toEqual(['tenantA/room']);
		} finally { await bus.deactivate(); }
	});
});
