import { it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

it('reconciles silent shard loss and stops old watchdogs across deactivate and restart', async () => {
	const timers = new Set();
	setRuntimeEnv({ timers: {
		set: (cb, ms) => { const h = { cb, ms, unref() {} }; timers.add(h); return h; },
		clear: (h) => timers.delete(h)
	} });
	const client = mockRedisClient();
	const node = client.redis.duplicate();
	const slots = [[0, 16383, ['127.0.0.1', 6379, 'master']]];
	node.cluster = async (cmd) => cmd === 'MYID' ? 'master' : slots;
	client.redis.nodes = () => [node];
	client.redis.cluster = node.cluster;
	client.redis.refreshSlotsCache = (cb) => cb?.();
	let subscriber;
	const duplicate = node.duplicate.bind(node);
	node.duplicate = (...args) => {
		subscriber = duplicate(...args);
		subscriber.status = 'ready';
		subscriber.condition = { subscriber: { channels: () => [...subscriber._shardedChannels] } };
		return subscriber;
	};
	const bus = createShardedBus(client);
	const flush = async () => { for (let n = 0; n < 30; n++) await Promise.resolve(); };
	try {
		await Promise.all([bus.activate(mockPlatform()), bus.activate(mockPlatform())]);
		expect([...timers].filter((h) => h.ms === 1000)).toHaveLength(1);
		await bus.follow('room');
		const oldTick = [...timers].find((h) => h.ms === 1000);
		expect(oldTick).toBeDefined();
		subscriber._shardedChannels.clear();
		timers.delete(oldTick);
		oldTick.cb();
		for (const h of [...timers]) { if (h.ms === 0) { timers.delete(h); h.cb(); } }
		await flush();
		expect(subscriber._shardedChannels.has('uws:sharded:room')).toBe(true);
		await bus.deactivate();
		expect([...timers].filter((h) => h.ms === 1000)).toHaveLength(0);
		await Promise.all([bus.activate(mockPlatform()), bus.activate(mockPlatform())]);
		expect([...timers].filter((h) => h.ms === 1000)).toHaveLength(1);
		await bus.follow('room');
		const count = timers.size;
		oldTick.cb();
		await flush();
		expect(timers.size).toBe(count);
		await bus.deactivate();
		oldTick.cb();
		expect([...timers].filter((h) => h.ms === 1000)).toHaveLength(0);
	} finally {
		await bus.deactivate();
		resetRuntimeEnv();
	}
});
