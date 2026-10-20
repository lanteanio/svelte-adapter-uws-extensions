// The sharded bus's activate() / deactivate() when they interleave: calls
// that overlap, a deactivate() while an activation waits on Redis, and
// connections that outlive a reactivation.
import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

const envelope = (topic) => JSON.stringify({ instanceId: 'peer', topic, event: 'msg', data: 1 });
const within = (promise, ms = 50) => Promise.race([
	promise.then(() => 'resolved', (err) => 'rejected: ' + err.message),
	new Promise((resolve) => setTimeout(() => resolve('pending'), ms))
]);

/** A client whose INFO answers only when the test says so, or never. */
function heldInfo() {
	const client = mockRedisClient();
	const info = client.redis.info.bind(client.redis);
	const held = { answer: null };
	client.redis.info = (...args) => new Promise((resolve) => {
		held.answer = () => info(...args).then(resolve);
	});
	return { client, held };
}

/** A cluster client of one master whose CLUSTER MYID answers when the test says so. */
function heldCluster() {
	const client = mockRedisClient();
	const node = client.redis.duplicate();
	const slots = [[0, 16383, ['127.0.0.1', 6379, 'master']]];
	const held = { answer: null };
	node.cluster = (cmd) => (cmd === 'MYID'
		? new Promise((resolve) => { held.answer = () => resolve('master'); })
		: Promise.resolve(slots));
	client.redis.nodes = () => [node];
	client.redis.cluster = node.cluster;
	client.redis.refreshSlotsCache = (cb) => cb?.();
	const opened = [];
	const duplicate = node.duplicate.bind(node);
	node.duplicate = (...args) => {
		const sub = duplicate(...args);
		sub.status = 'ready';
		sub.disconnect = vi.fn();
		opened.push(sub);
		return sub;
	};
	return { client, held, opened };
}

describe('sharded bus activation lifecycle', () => {
	it('stays inactive when deactivate() runs while INFO is in flight', async () => {
		const { client, held } = heldInfo();
		const bus = createShardedBus(client);
		const activating = bus.activate(mockPlatform());
		await bus.deactivate();
		held.answer();
		await expect(activating).rejects.toThrow('deactivated during activation');
		await expect(bus.follow('room')).rejects.toThrow('activate() must be called');
	});

	it('settles an activation whose INFO never answers when deactivate() runs', async () => {
		const { client } = heldInfo();
		const bus = createShardedBus(client);
		const activating = bus.activate(mockPlatform());
		await bus.deactivate();
		expect(await within(activating)).toBe('rejected: sharded bus: deactivated during activation');
	});

	it('gives a cancelled activation\'s breaker probe back', async () => {
		const { client } = heldInfo();
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 0 });
		try {
			breaker.failure(new Error('down'));
			await new Promise((resolve) => setTimeout(resolve, 5));
			expect(breaker.state).toBe('probing');
			const bus = createShardedBus(client, { breaker });
			const activating = bus.activate(mockPlatform());
			await bus.deactivate();
			await within(activating);
			expect(breaker.state).toBe('probing');
			expect(() => breaker.guard()).not.toThrow();
		} finally {
			breaker.destroy();
		}
	});

	it('stays inactive when deactivate() runs while the cluster topology is read', async () => {
		const { client, held, opened } = heldCluster();
		const bus = createShardedBus(client);
		const activating = bus.activate(mockPlatform());
		for (let n = 0; n < 10 && held.answer === null; n++) await Promise.resolve();
		expect(held.answer).not.toBe(null);
		await bus.deactivate();
		held.answer();
		await expect(activating).rejects.toThrow('deactivated during activation');
		for (let n = 0; n < 10; n++) await Promise.resolve();
		await expect(bus.follow('room')).rejects.toThrow('activate() must be called');
		expect(opened).toEqual([]);
	});

	it('does not let a connection from before a reactivation unfollow a newer connection\'s topic', async () => {
		const client = mockRedisClient();
		const duplicate = client.duplicate.bind(client);
		client.duplicate = (overrides) => {
			const sub = duplicate(overrides);
			// A closed connection receives nothing more.
			sub.disconnect = () => sub._shardedChannels.clear();
			return sub;
		};
		const platform = mockPlatform();
		const bus = createShardedBus(client);
		try {
			const before = { id: 'before' };
			const after = { id: 'after' };
			await bus.activate(platform);
			await bus.hooks.subscribe(before, 'room');
			await bus.deactivate();
			await bus.activate(platform);
			await bus.hooks.subscribe(after, 'room');
			await bus.hooks.unsubscribe(before, 'room');
			await bus.hooks.close(before);
			await client.redis.spublish('uws:sharded:room', envelope('room'));
			expect(platform.published.filter((p) => p.event === 'msg')).toHaveLength(1);
		} finally {
			await bus.deactivate();
		}
	});
});
