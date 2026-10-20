// A sharded bus on a cluster re-subscribes the channels of a master
// connection that ended on a fresh connection. When that one ends too before
// the SSUBSCRIBE reply is handled, the channel has no owner connection, so the
// reconciliation is not complete and must run again. Real ioredis rejects the
// commands of a connection that closes, but a reply already read off the
// socket can still resolve after 'end', which is the order modelled here.
import { it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

it('retries a reconcile whose replacement connection ended before its SSUBSCRIBE reply', async () => {
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
	const subscribers = [];
	const duplicate = node.duplicate.bind(node);
	node.duplicate = (...args) => {
		const sub = duplicate(...args);
		sub.status = 'ready';
		sub.condition = { subscriber: { channels: () => [...sub._shardedChannels] } };
		// The second connection ends while its SSUBSCRIBE is in flight, and the
		// reply resolves after the end event.
		if (subscribers.length === 1) {
			const ssubscribe = sub.ssubscribe.bind(sub);
			sub.ssubscribe = async (...channels) => {
				const result = await ssubscribe(...channels);
				sub.status = 'end';
				sub._shardedChannels.clear();
				sub._listeners.get('end')();
				return result;
			};
		}
		subscribers.push(sub);
		return sub;
	};
	const flush = async () => { for (let n = 0; n < 50; n++) await Promise.resolve(); };
	// Run the pending reconcile, retry and watchdog timers, but not the
	// subscriber liveness PINGs.
	const runTimers = async () => {
		for (const h of [...timers]) {
			if (h.ms >= 30000) continue;
			timers.delete(h);
			h.cb();
		}
		await flush();
	};
	const bus = createShardedBus(client);
	const platform = mockPlatform();
	try {
		await bus.activate(platform);
		await bus.follow('room');
		expect(subscribers).toHaveLength(1);

		// The first master connection ends: its channel goes to the reconcile.
		subscribers[0].status = 'end';
		subscribers[0]._shardedChannels.clear();
		subscribers[0]._listeners.get('end')();
		for (let round = 0; round < 6 && subscribers.length < 3; round++) await runTimers();
		await flush();

		expect(subscribers).toHaveLength(3);
		expect(subscribers[2]._shardedChannels.has('uws:sharded:room')).toBe(true);

		await client.redis.spublish('uws:sharded:room', JSON.stringify({ instanceId: 'peer', topic: 'room', event: 'msg', data: 1 }));
		expect(platform.published.filter((p) => p.event === 'msg')).toHaveLength(1);
	} finally {
		await bus.deactivate();
		resetRuntimeEnv();
	}
});
