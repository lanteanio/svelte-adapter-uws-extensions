import { it, expect } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
	const deadline = Date.now() + 3000;
	while (!check()) {
		if (Date.now() > deadline) throw new Error('subscription barrier timed out');
		await wait(5);
	}
}

it.each([false, true])('delivers after a multi-channel admission changes followers (restore=%s)', async restore => {
	const client = createBackendClient({ keyPrefix: 'inttest-multichannel:' });
	const subscriptions = new Map(), unsubscriptions = new Map();
	const channel = topic => client.key('uws:sharded:' + topic);
	let holdCleanup = false;
	let bus, first, replacement, late;
	try {
		await client.redis.ping();
		for (const node of client.redis.nodes('master')) {
			const duplicate = node.duplicate.bind(node);
			node.duplicate = (...args) => {
				const subscriber = duplicate(...args);
				const subscribe = subscriber.ssubscribe.bind(subscriber);
				const unsubscribe = subscriber.sunsubscribe.bind(subscriber);
				subscriber.ssubscribe = name => new Promise((resolve, reject) => subscriptions.set(name, async () => {
					subscriptions.delete(name);
					try { resolve(await subscribe(name)); } catch (error) { reject(error); }
				}));
				subscriber.sunsubscribe = name => holdCleanup ? new Promise((resolve, reject) => unsubscriptions.set(name, async () => {
					unsubscriptions.delete(name);
					try { resolve(await unsubscribe(name)); } catch (error) { reject(error); }
				})) : unsubscribe(name);
				return subscriber;
			};
		}
		bus = createShardedBus(client, { channelPrefix: client.key('uws:sharded:') });
		const platform = mockPlatform();
		await bus.activate(platform);
		const a = {}, b = {}, c = {};
		first = bus.hooks.subscribeBatch(a, ['a', 'b']);
		await until(() => subscriptions.size === 2);
		if (restore) {
			holdCleanup = true;
			await bus.hooks.close(a);
			await subscriptions.get(channel('a'))();
			await subscriptions.get(channel('b'))();
			await until(() => unsubscriptions.has(channel('a')));
			replacement = bus.hooks.subscribeBatch(b, ['a', 'b']);
			await unsubscriptions.get(channel('a'))();
			await until(() => unsubscriptions.has(channel('b')));
			await unsubscriptions.get(channel('b'))();
			await until(() => subscriptions.size === 2);
			holdCleanup = false;
			await bus.hooks.unsubscribe(b, 'a');
		} else await bus.hooks.unsubscribe(a, 'a');
		await subscriptions.get(channel('a'))();
		await wait(20);
		late = bus.hooks.subscribe(c, 'a');
		await subscriptions.get(channel('b'))();
		await Promise.all([first, replacement, late]);
		const publish = data => client.redis.spublish(channel('a'), JSON.stringify({ instanceId: 'peer', topic: 'a', event: 'msg', data }));
		expect(await publish(1)).toBe(1);
		await until(() => platform.published.length === 1);
		await bus.hooks.subscribe({}, 'a');
		await bus.hooks.close(c);
		expect(await publish(2)).toBe(1);
		await until(() => platform.published.length === 2);
		expect(platform.published.map(entry => entry.data)).toEqual([1, 2]);
	} finally {
		holdCleanup = false;
		for (const release of [...subscriptions.values(), ...unsubscriptions.values()]) await release();
		await bus?.deactivate();
		await Promise.allSettled([first, replacement, late]);
		await client.quit();
	}
});
