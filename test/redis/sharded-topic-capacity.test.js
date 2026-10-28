import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

vi.mock('../../src/shared/caps.js', async (original) => ({
	...(await original()), MAX_SHARDED_BUS_TOPICS: 2
}));

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

async function setup() {
	const client = mockRedisClient();
	const platform = mockPlatform();
	let subscriber;
	const duplicate = client.redis.duplicate.bind(client.redis);
	client.redis.duplicate = (...args) => (subscriber = duplicate(...args));
	const bus = createShardedBus(client);
	await bus.activate(platform);
	const peer = (topic, data) => client.redis.spublish('uws:sharded:' + topic, JSON.stringify({ instanceId: 'peer', topic, event: 'msg', data }));
	const received = (topic) => platform.published.filter((p) => p.topic === topic).map((p) => p.data);
	return { client, platform, bus, subscriber: () => subscriber, peer, received };
}

describe('sharded bus topic capacity', () => {
	it('followBatch refuses a batch that would exceed the cap, without counting any of it', async () => {
		const { bus, subscriber, peer, received } = await setup();
		try {
			await bus.follow('one');
			await expect(bus.followBatch(['one', 'two', 'three'])).rejects.toMatchObject({ code: 'SHARDED_TOPIC_CAPACITY_EXCEEDED' });
			// Nothing in the refused batch was counted: one unfollow of the topic
			// followed before still drops the channel, and 'two' was never taken.
			await bus.unfollow('one');
			expect(subscriber()._shardedChannels.size).toBe(0);
			await bus.followBatch(['two', 'three']);
			await peer('two', 1);
			expect(received('two')).toEqual([1]);
		} finally { await bus.deactivate(); }
	});

	it('followBatch admits a batch of already-followed topics at the cap', async () => {
		const { bus, peer, received } = await setup();
		try {
			await bus.followBatch(['one', 'two']);
			await bus.followBatch(['two', 'one']);
			await bus.unfollow('one');
			await peer('one', 1);
			expect(received('one')).toEqual([1]);
		} finally { await bus.deactivate(); }
	});

	it('a refused subscribeBatch does not release a channel another socket follows when it closes', async () => {
		const { bus, subscriber, peer, received } = await setup();
		const filler = {}, a = {}, b = {};
		try {
			await bus.hooks.subscribe(filler, 'one');
			await bus.hooks.subscribe(filler, 'two');
			await expect(bus.hooks.subscribeBatch(a, ['chat'])).rejects.toMatchObject({ code: 'SHARDED_TOPIC_CAPACITY_EXCEEDED' });
			await bus.hooks.unsubscribe(filler, 'two');
			await bus.hooks.subscribe(b, 'chat');
			await bus.hooks.close(a);
			expect(subscriber()._shardedChannels.has('uws:sharded:chat')).toBe(true);
			await peer('chat', 1);
			expect(received('chat')).toEqual([1]);
		} finally { await bus.deactivate(); }
	});

	it('the single-topic subscribe hook reports the cap with the same error and records nothing', async () => {
		const { bus, subscriber, peer, received } = await setup();
		const filler = {}, a = {}, b = {};
		try {
			await bus.hooks.subscribe(filler, 'one');
			await bus.hooks.subscribe(filler, 'two');
			await expect(bus.hooks.subscribe(a, 'chat')).rejects.toMatchObject({ code: 'SHARDED_TOPIC_CAPACITY_EXCEEDED' });
			await bus.hooks.unsubscribe(filler, 'two');
			await bus.hooks.subscribe(b, 'chat');
			await bus.hooks.close(a);
			expect(subscriber()._shardedChannels.has('uws:sharded:chat')).toBe(true);
			await peer('chat', 1);
			expect(received('chat')).toEqual([1]);
		} finally { await bus.deactivate(); }
	});

	it('a refused subscribeBatch leaves topics the socket already followed in place', async () => {
		const { bus, peer, received } = await setup();
		const a = {};
		try {
			await bus.hooks.subscribeBatch(a, ['one', 'two']);
			await expect(bus.hooks.subscribeBatch(a, ['one', 'chat'])).rejects.toMatchObject({ code: 'SHARDED_TOPIC_CAPACITY_EXCEEDED' });
			await peer('one', 1);
			expect(received('one')).toEqual([1]);
			await bus.hooks.close(a);
			await peer('one', 2);
			expect(received('one')).toEqual([1]);
		} finally { await bus.deactivate(); }
	});

	it('subscribeBatch never records an empty topic it did not count', async () => {
		const { bus, subscriber } = await setup();
		const a = {}, b = {};
		try {
			// follow() counts the empty topic; followBatch skips it.
			await bus.hooks.subscribe(b, '');
			await bus.hooks.subscribeBatch(a, ['']);
			await bus.hooks.close(a);
			expect(subscriber()._shardedChannels.has('uws:sharded:')).toBe(true);
		} finally { await bus.deactivate(); }
	});
});

describe.each(['subscribe', 'subscribeBatch'])('sharded %s closed-socket handling', (hook) => {
	const call = (bus, ws, topic) => bus.hooks[hook](ws, hook === 'subscribe' ? topic : [topic]);

	it('does not follow for a socket that already closed', async () => {
		const { bus, subscriber, peer, received } = await setup();
		const a = {};
		try {
			await bus.hooks.close(a);
			await call(bus, a, 'chat');
			expect(subscriber()._shardedChannels.size).toBe(0);
			await peer('chat', 1);
			expect(received('chat')).toEqual([]);
		} finally { await bus.deactivate(); }
	});
});

describe.each(['close', 'unsubscribe'])('sharded subscribeBatch admission rollback after %s', (action) => {
	it('keeps a topic another socket holds after a mixed admission fails', async () => {
		const { bus, subscriber, peer, received } = await setup();
		const sub = subscriber();
		const realSubscribe = sub.ssubscribe.bind(sub);
		const a = {}, b = {};
		try {
			await bus.hooks.subscribe(b, 'held');
			let reject;
			sub.ssubscribe = () => new Promise((_, no) => { reject = no; });
			const pending = bus.hooks.subscribeBatch(a, ['held', 'new']);
			const failed = expect(pending).rejects.toThrow('offline');
			await settle();
			if (action === 'close') await bus.hooks.close(a);
			else await bus.hooks.unsubscribe(a, 'held');
			reject(new Error('offline'));
			await failed;
			sub.ssubscribe = realSubscribe;
			expect(sub._shardedChannels.has('uws:sharded:held')).toBe(true);
			expect(sub._shardedChannels.has('uws:sharded:new')).toBe(false);
			await peer('held', 1);
			expect(received('held')).toEqual([1]);
		} finally {
			sub.ssubscribe = realSubscribe;
			await bus.deactivate();
		}
	});
});
