import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

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
});
