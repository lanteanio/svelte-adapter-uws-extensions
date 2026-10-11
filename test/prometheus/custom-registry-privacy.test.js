import { describe, it, expect } from 'vitest';
import { createMetrics } from '../../src/prometheus/index.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createReplay as createRedisReplay } from '../../src/redis/replay.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createReplay as createPgReplay } from '../../src/postgres/replay.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const topic = 'private:user-123';

function customMetrics() {
	const registry = createMetrics();
	return {
		counter: registry.counter,
		gauge: registry.gauge,
		histogram: registry.histogram,
		serialize: registry.serialize
	};
}

function expectPrivate(metrics, name) {
	const output = metrics.serialize();
	expect(output).toContain(`${name}{topic="all"} 1`);
	expect(output).not.toContain(topic);
	expect(output).not.toContain('user-123');
}

describe('custom metric registry topic privacy', () => {
	it('collapses cursor update labels without a topic mapper', () => {
		const metrics = customMetrics();
		const cursor = createCursor(mockRedisClient(), {
			metrics, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0
		});
		try {
			cursor.update(mockWs({ id: 'member' }), topic, { x: 1 }, mockPlatform());
			expectPrivate(metrics, 'cursor_updates_total');
		} finally {
			cursor.destroy();
		}
	});

	it('collapses stream replay labels without a topic mapper', async () => {
		const metrics = customMetrics();
		const replay = createRedisReplay(mockRedisClient(), { metrics, storage: 'stream' });
		await replay.publish(mockPlatform(), topic, 'change', { value: 1 });
		expectPrivate(metrics, 'replay_publishes_total');
	});

	it('collapses sharded relay labels without a topic mapper', async () => {
		const metrics = customMetrics();
		const bus = createShardedBus(mockRedisClient(), { metrics });
		const platform = mockPlatform();
		try {
			await bus.activate(platform);
			bus.wrap(platform).publish(topic, 'change', { value: 1 });
			await new Promise((resolve) => setTimeout(resolve, 10));
			expectPrivate(metrics, 'sharded_pubsub_messages_relayed_total');
		} finally {
			await bus.deactivate();
		}
	});

	it('collapses Postgres replay labels without a topic mapper', async () => {
		const metrics = customMetrics();
		const replay = createPgReplay(mockPgClient(), { metrics, cleanupInterval: 0 });
		try {
			await replay.publish(mockPlatform(), topic, 'change', { value: 1 });
			expectPrivate(metrics, 'replay_publishes_total');
		} finally {
			replay.destroy();
		}
	});
});
