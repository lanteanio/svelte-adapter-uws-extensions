/**
 * Integration tests for redis/sharded-pubsub against a real Redis 7 server.
 *
 * NOTE on standalone vs cluster: this stack runs a single standalone
 * Redis 7. Standalone Redis accepts SPUBLISH/SSUBSCRIBE/SUNSUBSCRIBE
 * commands and routes messages between subscribers, but there is no
 * shard topology -- so the "shard isolation" guarantee that matters in
 * a real cluster (a SPUBLISH only fans out to the shard owning the
 * channel) is not exercisable here. What we CAN verify on standalone:
 *
 * - SPUBLISH/SSUBSCRIBE round-trip a payload between two ioredis
 *   duplicate connections (the on-the-wire shape works).
 * - The subscriber emits 'smessage' (not 'message') for sharded events.
 * - Per-topic channel naming (channelPrefix + shardKey(topic)).
 * - follow / unfollow refcount correctness drives SSUBSCRIBE /
 *   SUNSUBSCRIBE on real connection state.
 * - Echo suppression by instanceId on a real round trip.
 *
 * Real shard isolation requires Redis Cluster; that's a deployment
 * concern, not something this test can pin.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createShardedBus } from '../../../redis/sharded-pubsub.js';
import { createPublishRateAggregator } from '../../../redis/publish-rate.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(10);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('redis sharded pubsub bus (integration)', () => {
	let client;
	const buses = [];

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-sharded:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe everything under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');
	});

	afterEach(async () => {
		while (buses.length > 0) {
			const b = buses.pop();
			await b.deactivate().catch(() => {});
		}
	});

	afterAll(async () => {
		await client.quit();
	});

	function track(bus) {
		buses.push(bus);
		return bus;
	}

	function uniqueChannelPrefix(label) {
		return `inttest-sharded:${label}:${Date.now()}:${Math.random().toString(16).slice(2, 8)}:`;
	}

	describe('activate version check (real INFO server)', () => {
		it('activates against Redis 7 (the test stack image)', async () => {
			const bus = track(createShardedBus(client, { channelPrefix: uniqueChannelPrefix('ver') }));
			const platform = mockPlatform();
			await bus.activate(platform);
			// If we got here, the major-version parse and >= 7 gate let us through.
		});
	});

	describe('SPUBLISH / SSUBSCRIBE delivery', () => {
		it('round-trips a payload between two buses on per-topic channels', async () => {
			const channelPrefix = uniqueChannelPrefix('two');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busA.follow('chat');
			await busB.follow('chat');

			busA.wrap(platformA).publish('chat', 'msg', { from: 'A' });

			await waitFor(() => platformB.published.find((p) => p.event === 'msg') !== undefined);
			const got = platformB.published.find((p) => p.event === 'msg');
			expect(got).toEqual({
				topic: 'chat',
				event: 'msg',
				data: { from: 'A' },
				options: { relay: false }
			});
		});

		it('echo suppression: a bus does not double-deliver its own SPUBLISH', async () => {
			const channelPrefix = uniqueChannelPrefix('echo');
			const platform = mockPlatform();
			const bus = track(createShardedBus(client, { channelPrefix }));
			await bus.activate(platform);
			await bus.follow('chat');

			bus.wrap(platform).publish('chat', 'msg', { id: 1 });

			// Generous wait so a missed echo suppression would surface.
			await wait(150);
			expect(platform.published.filter((p) => p.event === 'msg')).toHaveLength(1);
		});

		it('per-topic channel naming: a bus only receives topics it follows', async () => {
			const channelPrefix = uniqueChannelPrefix('topic-isolation');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			// B follows only "kept", not "ignored".
			await busA.follow('kept');
			await busA.follow('ignored');
			await busB.follow('kept');

			const wrapped = busA.wrap(platformA);
			wrapped.publish('kept', 'msg', { id: 1 });
			wrapped.publish('ignored', 'msg', { id: 2 });

			await waitFor(() => platformB.published.find((p) => p.data && p.data.id === 1) !== undefined);
			// Wait long enough that an erroneous "ignored" delivery would arrive.
			await wait(150);
			const onB = platformB.published.filter((p) => p.event === 'msg');
			expect(onB).toHaveLength(1);
			expect(onB[0].data).toEqual({ id: 1 });
		});

		it('shardKey routes multiple topics to one underlying channel', async () => {
			const channelPrefix = uniqueChannelPrefix('shardkey');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			// Group all "chat:*" topics onto one shard channel.
			const busA = track(createShardedBus(client, {
				channelPrefix, shardKey: (t) => t.split(':')[0]
			}));
			const busB = track(createShardedBus(client, {
				channelPrefix, shardKey: (t) => t.split(':')[0]
			}));
			await busA.activate(platformA);
			await busB.activate(platformB);

			// B only needs to follow ONE topic in the shard to receive
			// publishes for SIBLING topics in the same shard.
			await busA.follow('chat:room1');
			await busB.follow('chat:room2');

			busA.wrap(platformA).publish('chat:room1', 'msg', { which: 'room1' });

			// chat:room1 hashes to the same channel B is listening on
			// via "chat:room2" + shardKey.
			await waitFor(() => platformB.published.find((p) => p.event === 'msg') !== undefined);
			expect(platformB.published.find((p) => p.event === 'msg').data).toEqual({ which: 'room1' });
		});

		it('relay: false skips SPUBLISH entirely', async () => {
			const channelPrefix = uniqueChannelPrefix('relay-false');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat');

			busA.wrap(platformA).publish('chat', 'msg', { only: 'local' }, { relay: false });

			expect(platformA.published).toHaveLength(1);
			await wait(150);
			expect(platformB.published).toHaveLength(0);
		});
	});

	describe('publishBatched cross-connection', () => {
		it('a batched envelope round-trips on a per-shard channel and re-dispatches via publishBatched', async () => {
			const channelPrefix = uniqueChannelPrefix('batched');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (t) => t.split(':')[0]
			}));
			const busB = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (t) => t.split(':')[0]
			}));

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');
			await busB.follow('chat:room2');

			busA.wrap(platformA).publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: { i: 1 } },
				{ topic: 'chat:room2', event: 'msg', data: { i: 2 } },
				{ topic: 'chat:room1', event: 'msg', data: { i: 3 } }
			]);

			await waitFor(() => platformB.publishedBatches.length > 0);
			const batch = platformB.publishedBatches[0].messages;
			expect(batch).toHaveLength(3);
			expect(batch.map((m) => m.data.i)).toEqual([1, 2, 3]);
			for (const m of batch) {
				expect(m.options).toEqual({ relay: false });
			}
		});

		it('groups by shard channel: two shards = two SPUBLISH envelopes, two batches on receiver', async () => {
			const channelPrefix = uniqueChannelPrefix('batched-shards');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (t) => t.split(':')[0]
			}));
			const busB = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (t) => t.split(':')[0]
			}));

			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat:room1');
			await busB.follow('audit:org1');

			busA.wrap(platformA).publishBatched([
				{ topic: 'chat:room1', event: 'msg', data: { kind: 'chat-1' } },
				{ topic: 'chat:room1', event: 'msg', data: { kind: 'chat-2' } },
				{ topic: 'audit:org1', event: 'created', data: { kind: 'audit-1' } }
			]);

			// Two batched envelopes (one per shard channel) arrive on B.
			await waitFor(() => platformB.publishedBatches.length === 2);
			const allLocal = platformB.publishedBatches.flatMap((b) => b.messages);
			expect(allLocal).toHaveLength(3);
			const kinds = new Set(allLocal.map((m) => m.data.kind));
			expect(kinds).toEqual(new Set(['chat-1', 'chat-2', 'audit-1']));
		});

		it('echo suppression: emitter is not double-delivered for batched envelopes', async () => {
			const channelPrefix = uniqueChannelPrefix('batched-echo');
			const platform = mockPlatform();
			const bus = track(createShardedBus(client, { channelPrefix }));
			await bus.activate(platform);
			await bus.follow('chat');

			bus.wrap(platform).publishBatched([
				{ topic: 'chat', event: 'msg', data: { i: 1 } },
				{ topic: 'chat', event: 'msg', data: { i: 2 } }
			]);

			// Generous wait so a missed echo suppression would surface as a
			// second publishBatched call on the local platform.
			await wait(200);
			expect(platform.publishedBatches).toHaveLength(1);
		});

		it('preserves order across the wire for a 50-message batch', async () => {
			const channelPrefix = uniqueChannelPrefix('batched-order');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);
			await busB.follow('chat');

			const messages = [];
			for (let i = 0; i < 50; i++) {
				messages.push({ topic: 'chat', event: 'msg', data: { i } });
			}
			busA.wrap(platformA).publishBatched(messages);

			await waitFor(() => platformB.publishedBatches.length > 0);
			const got = platformB.publishedBatches[0].messages.map((m) => m.data.i);
			expect(got).toEqual(messages.map((_, i) => i));
		});
	});

	describe('follow / unfollow lifecycle', () => {
		it('unfollow stops delivery on a real connection', async () => {
			const channelPrefix = uniqueChannelPrefix('unfollow');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);
			await busA.follow('chat');
			await busB.follow('chat');

			busA.wrap(platformA).publish('chat', 'msg', { id: 1 });
			await waitFor(() => platformB.published.find((p) => p.data && p.data.id === 1) !== undefined);

			await busB.unfollow('chat');

			busA.wrap(platformA).publish('chat', 'msg', { id: 2 });
			await wait(150);
			expect(platformB.published.find((p) => p.data && p.data.id === 2)).toBeUndefined();
		});

		it('refcounted follow: only the LAST unfollow tears down the SSUBSCRIBE', async () => {
			const channelPrefix = uniqueChannelPrefix('refcount');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			await busA.follow('chat');
			await busB.follow('chat');
			await busB.follow('chat'); // refcount 2

			await busB.unfollow('chat'); // refcount 1; still subscribed
			busA.wrap(platformA).publish('chat', 'msg', { id: 1 });
			await waitFor(() => platformB.published.find((p) => p.data && p.data.id === 1) !== undefined);

			await busB.unfollow('chat'); // refcount 0; SUNSUBSCRIBE
			busA.wrap(platformA).publish('chat', 'msg', { id: 2 });
			await wait(150);
			expect(platformB.published.find((p) => p.data && p.data.id === 2)).toBeUndefined();
		});

		it('throws when follow is called before activate', async () => {
			const bus = createShardedBus(client, { channelPrefix: uniqueChannelPrefix('preactive') });
			await expect(bus.follow('chat')).rejects.toThrow('activate() must be called before follow()');
		});
	});

	describe('followBatch (real SSUBSCRIBE batching)', () => {
		it('SSUBSCRIBEs grouped channels in one round trip', async () => {
			const channelPrefix = uniqueChannelPrefix('batch');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (topic) => topic.split(':')[0]
			}));
			const busB = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (topic) => topic.split(':')[0]
			}));
			await busA.activate(platformA);
			await busB.activate(platformB);

			await busB.followBatch(['chat:room1', 'chat:room2', 'audit:created']);

			// Cross-instance SPUBLISH on each underlying channel must reach B.
			busA.wrap(platformA).publish('chat:room1', 'msg', { id: 1 });
			busA.wrap(platformA).publish('audit:created', 'event', { id: 2 });

			await waitFor(() => platformB.published.find((p) => p.data && p.data.id === 1) !== undefined);
			await waitFor(() => platformB.published.find((p) => p.data && p.data.id === 2) !== undefined);
		});

		it('subscribeBatch hook lands one round trip per channel for an N-topic batch', async () => {
			const channelPrefix = uniqueChannelPrefix('hookbatch');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (topic) => topic.split(':')[0]
			}));
			const busB = track(createShardedBus(client, {
				channelPrefix,
				shardKey: (topic) => topic.split(':')[0]
			}));
			await busA.activate(platformA);
			await busB.activate(platformB);

			const ws = {};
			await busB.hooks.subscribeBatch(ws, ['org:42:items', 'org:42:audit', 'org:42:notifications'], {
				platform: platformB
			});

			// All three should now route through the single 'org' shard channel.
			busA.wrap(platformA).publish('org:42:items', 'updated', { tag: 'A' });
			busA.wrap(platformA).publish('org:42:audit', 'created', { tag: 'B' });
			busA.wrap(platformA).publish('org:42:notifications', 'incoming', { tag: 'C' });

			await waitFor(() => platformB.published.filter((p) => p.data && ['A', 'B', 'C'].includes(p.data.tag)).length === 3);

			// hooks.close should release every topic this ws followed.
			await busB.hooks.close(ws, { platform: platformB });

			// Wait for SUNSUBSCRIBE to take effect cluster-wide.
			await wait(50);
			busA.wrap(platformA).publish('org:42:items', 'updated', { tag: 'X' });
			await wait(150);
			expect(platformB.published.find((p) => p.data && p.data.tag === 'X')).toBeUndefined();
		});
	});

	describe('bus.subscribers delegating to publish-rate aggregator over real Redis', () => {
		const aggs = [];
		afterEach(async () => {
			while (aggs.length) {
				const a = aggs.pop();
				await a.deactivate().catch(() => {});
			}
		});

		// mockPlatform's `subscribers(topic)` returns 0; override to a
		// per-test count map so we can drive `bus.localSubjects(platform)`
		// (which calls `platform.subscribers(topic)`) into reporting non-zero
		// values per topic.
		function platformWithCounts(counts) {
			const p = mockPlatform();
			p.subscribers = (topic) => counts.get(topic) || 0;
			return p;
		}

		it('returns the cluster-wide sum (local + remote) once both aggregators have broadcast', async () => {
			const channelPrefix = uniqueChannelPrefix('subs');

			const countsA = new Map([['chat:room1', 7], ['chat:room2', 3]]);
			const countsB = new Map([['chat:room1', 12], ['audit:org', 5]]);
			const platformA = platformWithCounts(countsA);
			const platformB = platformWithCounts(countsB);

			// Forward-declare the bus refs so the aggregator's subjects()
			// callback (which fires on each broadcast tick, well after
			// construction) can close over them.
			let busA, busB;
			const aggA = createPublishRateAggregator(client, {
				channel: 'inttest:bus-subs',
				subjects: () => busA.localSubjects(platformA)
			});
			const aggB = createPublishRateAggregator(client, {
				channel: 'inttest:bus-subs',
				subjects: () => busB.localSubjects(platformB)
			});
			aggs.push(aggA, aggB);

			busA = track(createShardedBus(client, {
				channelPrefix, subscribersAggregator: aggA
			}));
			busB = track(createShardedBus(client, {
				channelPrefix, subscribersAggregator: aggB
			}));

			await busA.activate(platformA);
			await busB.activate(platformB);
			await aggA.activate(platformA);
			await aggB.activate(platformB);

			await busA.follow('chat:room1');
			await busA.follow('chat:room2');
			await busB.follow('chat:room1');
			await busB.follow('audit:org');

			await aggA._broadcastNow();
			await aggB._broadcastNow();
			await wait(60);

			// chat:room1: A=7, B=12; cluster sum 19.
			expect(busA.subscribers('chat:room1')).toBe(19);
			expect(busB.subscribers('chat:room1')).toBe(19);
			// chat:room2: only A reports (count 3); both views see 3.
			expect(busA.subscribers('chat:room2')).toBe(3);
			expect(busB.subscribers('chat:room2')).toBe(3);
			// audit:org: only B reports (count 5); both views see 5.
			expect(busA.subscribers('audit:org')).toBe(5);
			expect(busB.subscribers('audit:org')).toBe(5);
			// Topic neither bus is following returns 0 cleanly.
			expect(busA.subscribers('absent')).toBe(0);
		});

		it('partial unfollow + re-follow churn under default (per-topic) shards', async () => {
			// Default shardKey is identity, so each topic owns its own
			// shard channel. Unfollowing one topic SUNSUBSCRIBEs from that
			// channel without affecting others. Tests the SUNSUBSCRIBE
			// wire path under churn.
			const channelPrefix = uniqueChannelPrefix('churn');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createShardedBus(client, { channelPrefix }));
			const busB = track(createShardedBus(client, { channelPrefix }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			const all = ['t1', 't2', 't3', 't4', 't5'];
			await busB.followBatch(all);
			await busB.unfollow('t2');
			await busB.unfollow('t4');
			await wait(50);

			for (const t of all) {
				busA.wrap(platformA).publish(t, 'msg', { topic: t });
			}

			await waitFor(() => {
				const seen = new Set(platformB.published.filter((p) => p.event === 'msg').map((p) => p.data && p.data.topic));
				return seen.has('t1') && seen.has('t3') && seen.has('t5');
			});
			await wait(150);

			const delivered = new Set(platformB.published
				.filter((p) => p.event === 'msg')
				.map((p) => p.data && p.data.topic));
			expect(delivered.has('t1')).toBe(true);
			expect(delivered.has('t3')).toBe(true);
			expect(delivered.has('t5')).toBe(true);
			expect(delivered.has('t2')).toBe(false);
			expect(delivered.has('t4')).toBe(false);

			// Re-follow t4; subsequent publishes reach B again.
			await busB.follow('t4');
			await wait(50);
			platformB.reset();

			busA.wrap(platformA).publish('t4', 'msg', { topic: 't4' });
			await waitFor(() => platformB.published.find(
				(p) => p.event === 'msg' && p.data && p.data.topic === 't4'
			) !== undefined);

			// t2 stays unfollowed -- still no delivery.
			busA.wrap(platformA).publish('t2', 'msg', { topic: 't2' });
			await wait(150);
			const t2 = platformB.published.find(
				(p) => p.event === 'msg' && p.data && p.data.topic === 't2'
			);
			expect(t2).toBeUndefined();
		});

		it('falls back to the local platform.subscribers() count when no aggregator is wired', async () => {
			const channelPrefix = uniqueChannelPrefix('subs-local');
			const counts = new Map([['only-local', 4]]);
			const platform = platformWithCounts(counts);
			const bus = track(createShardedBus(client, { channelPrefix }));
			await bus.activate(platform);
			await bus.follow('only-local');

			expect(bus.subscribers('only-local')).toBe(4);
			expect(bus.subscribers('absent')).toBe(0);
		});
	});
});
