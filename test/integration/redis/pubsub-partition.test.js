/**
 * An asymmetric partition between two bus instances, against a real Redis
 * server and, with INTEGRATION_BACKEND=cluster, a real Redis Cluster, for the
 * pub/sub bus and the sharded bus.
 *
 * Instance A logs in as its own ACL user; taking that user's channel access
 * away cuts A off the bus while B keeps working: Redis closes A's subscriber,
 * refuses its re-subscribe, and refuses A's publish, so A's breaker opens and
 * B's never does. Each side then misses what the other published. Giving the
 * access back must leave the clients of both instances asked to refetch, A's
 * through its own `recovered` frame and B's through the request A relays,
 * although B's breaker never left healthy, and B's clients get one such frame.
 */
import { expect, it, vi } from 'vitest';
import { createBackendClient, isClusterBackend } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../../src/shared/breaker.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const recovered = (platform) => platform.published.filter((f) => f.topic === '__realtime' && f.event === 'recovered');
const refetches = (platform) => recovered(platform).filter((f) => f.data.recovery?.refetch === true);
const data = (platform, topic) => platform.published.filter((f) => f.topic === topic).map((f) => f.data);

for (const [name, factory] of [['pub/sub', createPubSubBus], ['sharded', createShardedBus]]) {
	it(`${name} bus asks the clients on both sides of an asymmetric partition to refetch once it heals`, async () => {
		const admin = createBackendClient();
		await admin.redis.ping();
		const nodes = isClusterBackend() ? admin.redis.nodes('all') : [admin.redis];
		const user = 'inttest-partition-' + name.replace('/', '') + '-' + Date.now();
		const password = 'test-partition-password';
		const setUser = (...args) => Promise.all(nodes.map((node) => node.call('ACL', 'SETUSER', user, ...args)));
		await setUser('reset', 'on', '>' + password, '~*', '&*', '+@all');
		const auth = { username: user, password };
		const client = createBackendClient({ options: isClusterBackend() ? { redisOptions: auth } : auth });
		const channel = 'inttest-partition:' + user;
		const options = { channel, channelPrefix: channel + ':' };
		const breakerA = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 200 });
		const breakerB = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 200 });
		const a = factory(client, { ...options, breaker: breakerA });
		const b = factory(admin, { ...options, breaker: breakerB });
		const platformA = mockPlatform(), platformB = mockPlatform();
		try {
			await a.activate(platformA);
			await b.activate(platformB);
			if (name === 'sharded') {
				await a.follow('room');
				await b.follow('room');
			}
			const wrappedA = a.wrap(platformA), wrappedB = b.wrap(platformB);
			wrappedB.publish('room', 'event', 'before');
			await vi.waitFor(() => expect(data(platformA, 'room')).toEqual(['before']), { timeout: 5000 });

			// Partition A: Redis closes its subscriber, and nothing it publishes
			// reaches the bus.
			await setUser('resetchannels');
			wrappedA.publish('room', 'event', 'from-a');
			await vi.waitFor(() => expect(breakerA.state).not.toBe('healthy'), { timeout: 5000 });
			wrappedB.publish('room', 'event', 'from-b');
			await new Promise((r) => setTimeout(r, 300));
			expect(breakerB.state).toBe('healthy');
			expect(recovered(platformA)).toEqual([]);
			expect(recovered(platformB)).toEqual([]);

			// Heal. A's next relay takes the breaker's probe; the subscriber's
			// refused restore is retried on its own backoff.
			await setUser('allchannels');
			await vi.waitFor(async () => {
				if (breakerA.state === 'probing') wrappedA.publish('room', 'event', 'after');
				await new Promise((r) => setTimeout(r, 50));
				expect(breakerA.state).toBe('healthy');
			}, { timeout: 10000, interval: 100 });
			await vi.waitFor(() => {
				expect(refetches(platformA).length).toBeGreaterThan(0);
				expect(refetches(platformB)).toHaveLength(1);
			}, { timeout: 30000, interval: 100 });

			// Both sides lost the other's message during the partition.
			expect(data(platformA, 'room')).not.toContain('from-b');
			expect(data(platformB, 'room')).not.toContain('from-a');
			// B's frame is its own, local only, with nothing of A's outage on it.
			expect(refetches(platformB)[0].data).toEqual({ at: expect.any(Number), recovery: { refetch: true } });
			expect(refetches(platformB)[0].options).toEqual({ relay: false, seq: false });
			expect(breakerB.state).toBe('healthy');
		} finally {
			await a.deactivate();
			await b.deactivate();
			breakerA.destroy();
			breakerB.destroy();
			await client.quit();
			await Promise.all(nodes.map((node) => node.call('ACL', 'DELUSER', user)));
			await admin.quit();
		}
	}, 60000);
}
