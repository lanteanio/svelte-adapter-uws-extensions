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
});
