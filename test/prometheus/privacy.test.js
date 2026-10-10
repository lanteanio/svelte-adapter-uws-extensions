import { describe, it, expect, vi } from 'vitest';
import { createMetrics, wirePublishRateMetrics, wireClusterPublishRateMetrics } from '../../src/prometheus/index.js';
import { createReplay } from '../../src/redis/replay.js';
import { createPresence } from '../../src/redis/presence.js';
import { createGroup } from '../../src/redis/groups.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createDistributedLock } from '../../src/redis/lock.js';
import { createLeader } from '../../src/redis/leader.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const sensitive = 'alice@example.com';
describe('identifier-free built-in metric labels', () => {
	it('never records a replay topic identifier, including after payload erasure', async () => {
		const metrics = createMetrics();
		const replay = createReplay(mockRedisClient(), { metrics, forgetUserId: ({ data }) => data.userId });
		await replay.publish(mockPlatform(), 'user:' + sensitive, 'message', { userId: sensitive });
		expect(await replay.purgeUser(null, sensitive)).toBe(1);
		expect(metrics.serialize()).toContain('replay_publishes_total{topic="all"} 1');
		expect(metrics.serialize()).not.toContain(sensitive);
	});

	it('maps group, tenant, lock and leader identifiers before recording them', async () => {
		const client = mockRedisClient();
		const metrics = createMetrics();
		const group = createGroup(client, sensitive, { metrics });
		const leader = createLeader(client, { key: sensitive, metrics, leaseMs: 60000 });
		try {
			await group.join(mockWs({ id: 'member' }), mockPlatform());
			const limit = createRateLimit(client, { points: 10, interval: 1000, tenant: () => sensitive, metrics });
			await limit.consume(mockWs({ ip: '127.0.0.1' }));
			await createDistributedLock(client, { metrics }).withLock(sensitive, async () => {});
			for (let i = 0; i < 100 && !leader.isLeader(); i++) await new Promise((resolve) => setTimeout(resolve, 1));
			expect(leader.isLeader()).toBe(true);
			const output = metrics.serialize();
			for (const family of ['group_joins_total{group="all"}', 'ratelimit_allowed_total{tenant_id="all"}', 'lock_acquired_total{key_class="all"}', 'leader_acquired_total{key_class="all"}']) expect(output).toContain(family + ' 1');
			expect(output).not.toContain(sensitive);
		} finally {
			group.destroy();
			await leader.stop();
		}
	});

	it.each([false, true])('sums sampled publish-rate classes and clears old snapshots (cluster=%s)', (cluster) => {
		const metrics = createMetrics();
		const topics = [{ topic: sensitive, messagesPerSec: 3, bytesPerSec: 30 }, { topic: 'other@example.com', messagesPerSec: 7, bytesPerSec: 70 }];
		const source = cluster ? { topPublishers: topics } : { pressure: { topPublishers: topics } };
		(cluster ? wireClusterPublishRateMetrics : wirePublishRateMetrics)(source, metrics);
		const prefix = cluster ? 'cluster' : 'ws';
		expect(metrics.serialize()).toContain(prefix + '_topic_publish_rate{topic="all"} 10');
		expect(metrics.serialize()).toContain(prefix + '_topic_publish_bytes{topic="all"} 100');
		expect(metrics.serialize()).not.toContain(sensitive);
		topics.splice(0);
		expect(metrics.serialize()).not.toContain('topic="all"');
	});

	it('sums presence memberships by mapped class and resets the departed class', async () => {
		vi.useFakeTimers();
		const metrics = createMetrics();
		const presence = createPresence(mockRedisClient(), { metrics, heartbeat: 50, topicThrottle: 0 });
		const platform = mockPlatform();
		const ws = mockWs({ id: 'member' });
		try {
			await presence.join(ws, sensitive, platform);
			await presence.join(ws, 'other@example.com', platform);
			await vi.advanceTimersByTimeAsync(60);
			expect(metrics.serialize()).toContain('presence_total_online{topic="all"} 2');
			expect(metrics.serialize()).not.toContain(sensitive);
			await presence.leave(ws, platform);
			await vi.advanceTimersByTimeAsync(60);
			expect(metrics.serialize()).not.toContain('presence_total_online{topic="all"}');
		} finally {
			presence.destroy();
			vi.useRealTimers();
		}
	});
	it('collects every tracker sharing a registry and releases a destroyed tracker', async () => {
		const metrics = createMetrics();
		const client = mockRedisClient();
		const first = createPresence(client, { metrics });
		const second = createPresence(client, { metrics });
		try {
			await first.join(mockWs({ id: 'first' }), 'first@example.com', mockPlatform());
			await second.join(mockWs({ id: 'second' }), 'second@example.com', mockPlatform());
			expect(metrics.serialize()).toContain('presence_total_online{topic="all"} 2');
			second.destroy();
			expect(metrics.serialize()).toContain('presence_total_online{topic="all"} 1');
		} finally {
			first.destroy();
			second.destroy();
		}
		expect(metrics.serialize()).not.toContain('presence_total_online{topic="all"}');
	});

});
