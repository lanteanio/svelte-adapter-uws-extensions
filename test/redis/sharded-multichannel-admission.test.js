import { it, expect } from 'vitest';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';

const settle = async () => { for (let index = 0; index < 40; index++) await Promise.resolve(); };
const channel = topic => 'uws:sharded:' + topic;

async function cluster(split = false) {
	const client = mockRedisClient();
	const nodes = Array.from({ length: split ? 2 : 1 }, () => client.redis.duplicate());
	const slots = split ? [[0, 8191, ['127.0.0.1', 6379, 'master0']], [8192, 16383, ['127.0.0.1', 6380, 'master1']]] : [[0, 16383, ['127.0.0.1', 6379, 'master0']]];
	nodes.forEach((node, index) => { node.cluster = async command => command === 'MYID' ? 'master' + index : slots; });
	client.redis.nodes = () => nodes;
	client.redis.cluster = nodes[0].cluster;
	client.redis.refreshSlotsCache = callback => callback?.();
	const subscriptions = new Map(), unsubscriptions = new Map();
	const subscribers = [];
	let holdUnsubscribe = false;
	for (const node of nodes) {
		const duplicate = node.duplicate.bind(node);
		node.duplicate = (...args) => {
			const subscriber = duplicate(...args);
			subscribers.push(subscriber);
			subscriber.status = 'ready';
			subscriber.condition = { subscriber: { channels: () => [...subscriber._shardedChannels] } };
			const subscribe = subscriber.ssubscribe.bind(subscriber);
			const unsubscribe = subscriber.sunsubscribe.bind(subscriber);
			subscriber.ssubscribe = name => new Promise(resolve => subscriptions.set(name, () => {
				subscriptions.delete(name); resolve(subscriber.status === 'end' ? undefined : subscribe(name));
			}));
			subscriber.sunsubscribe = name => holdUnsubscribe ? new Promise(resolve => unsubscriptions.set(name, () => {
				unsubscriptions.delete(name); resolve(unsubscribe(name));
			})) : unsubscribe(name);
			return subscriber;
			};
	}
	const platform = mockPlatform();
	const bus = createShardedBus(client);
	await bus.activate(platform);
	return { client, bus, platform, subscriptions, unsubscriptions, subscribers, holdCleanup() { holdUnsubscribe = true; }, resumeCleanup() { holdUnsubscribe = false; } };
}

it.each([false, true])('restores a confirmed channel whose connection ends before a sibling reply (split=%s)', async split => {
	const { bus, client, platform, subscriptions, subscribers } = await cluster(split);
	let pending;
	try {
		pending = bus.hooks.subscribeBatch({}, ['a', 'b']);
		await settle();
		subscriptions.get(channel('a'))();
		await settle();
		const old = subscribers.find(subscriber => subscriber._shardedChannels.has(channel('a')));
		old.status = 'end';
		old._shardedChannels.clear();
		old._listeners.get('end')();
		subscriptions.get(channel('b'))();
		await settle();
		for (const release of [...subscriptions.values()]) release();
		await pending;
		await client.redis.spublish(channel('a'), JSON.stringify({ instanceId: 'peer', topic: 'a', event: 'msg', data: 'a' }));
		await client.redis.spublish(channel('b'), JSON.stringify({ instanceId: 'peer', topic: 'b', event: 'msg', data: 'b' }));
		expect(platform.published.map(entry => entry.data)).toEqual(['a', 'b']);
	} finally {
		for (const release of subscriptions.values()) release();
		await bus.deactivate();
		await Promise.allSettled([pending]);
	}
});

it.each([false, true])('retains cluster channels until a multi-channel admission settles (restore=%s)', async restore => {
	const state = await cluster();
	const { bus, client, platform, subscriptions, unsubscriptions } = state;
	const a = {}, b = {}, c = {};
	let first, replacement;
	try {
		first = bus.hooks.subscribeBatch(a, ['a', 'b']);
		await settle();
		expect([...subscriptions.keys()].sort()).toEqual([channel('a'), channel('b')]);
		if (restore) {
			state.holdCleanup();
			await bus.hooks.close(a);
			subscriptions.get(channel('a'))(); subscriptions.get(channel('b'))();
			await settle();
			expect(unsubscriptions.has(channel('a'))).toBe(true);
			replacement = bus.hooks.subscribeBatch(b, ['a', 'b']);
			unsubscriptions.get(channel('a'))();
			await settle();
			unsubscriptions.get(channel('b'))();
			await settle();
			expect([...subscriptions.keys()].sort()).toEqual([channel('a'), channel('b')]);
			state.resumeCleanup();
			await bus.hooks.unsubscribe(b, 'a');
		} else await bus.hooks.unsubscribe(a, 'a');
		subscriptions.get(channel('a'))();
		await settle();
		const late = bus.hooks.subscribe(c, 'a');
		subscriptions.get(channel('b'))();
		await Promise.all([first, replacement, late]);
		expect(await client.redis.spublish(channel('a'), JSON.stringify({ instanceId: 'peer', topic: 'a', event: 'msg', data: 1 }))).toBe(1);
		expect(platform.published.map(entry => entry.data)).toEqual([1]);
		await bus.hooks.subscribe({}, 'a');
		await bus.hooks.close(c);
		expect(await client.redis.spublish(channel('a'), JSON.stringify({ instanceId: 'peer', topic: 'a', event: 'msg', data: 2 }))).toBe(1);
		expect(platform.published.map(entry => entry.data)).toEqual([1, 2]);
	} finally {
		for (const release of [...subscriptions.values(), ...unsubscriptions.values()]) release();
		await bus.deactivate();
		await Promise.allSettled([first, replacement]);
	}
});
