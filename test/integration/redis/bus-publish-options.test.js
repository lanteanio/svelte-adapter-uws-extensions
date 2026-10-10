import { it, expect } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check) {
	const deadline = Date.now() + 3000;
	while (!check()) {
		if (Date.now() > deadline) throw new Error('relay delivery timed out');
		await wait(5);
	}
}

const cases = ['pubsub', 'sharded'].flatMap(kind => [false, true].flatMap(traced => [false, true].map(fallback => [kind, traced, fallback])));
it.each(cases)('%s relays compression and coalesced values (traced=%s, fallback=%s)', async (kind, traced, fallback) => {
	const client = createBackendClient({ keyPrefix: 'inttest-bus-options:' });
	const create = kind === 'pubsub'
		? () => createPubSubBus(client, { channel: client.key('bus:') })
		: () => createShardedBus(client, { channelPrefix: client.key('sharded:') });
	const a = create(), b = create();
	const local = mockPlatform(), remote = mockPlatform();
	local.trace.enabled = traced;
	remote.trace.enabled = traced;
	if (fallback) remote.publishBatched = undefined;
	try {
		await a.activate(local);
		await b.activate(remote);
		if (kind === 'sharded') { await b.follow('chat'); await b.follow('other'); }
		const wrapped = a.wrap(local);
		wrapped.publish('chat', 'msg', 101, { compress: false });
		wrapped.batch([
			{ topic: 'chat', event: 'msg', data: 102, options: { compress: true } },
			{ topic: 'chat', event: 'msg', data: 103, options: { compress: false } }
		]);
		wrapped.publishBatched([
			{ topic: 'chat', event: 'msg', data: 0, coalesceKey: 'position' },
			{ topic: 'chat', event: 'msg', data: 1 },
			{ topic: 'chat', event: 'msg', data: 2, coalesceKey: 'position' },
			{ topic: 'chat', event: 'msg', data: 3, coalesceKey: 'local' },
			{ topic: 'chat', event: 'msg', data: 4, coalesceKey: 'local', options: { relay: false } },
			{ topic: 'other', event: 'msg', data: 5, coalesceKey: 'cross-topic' },
			{ topic: 'chat', event: 'msg', data: 6, coalesceKey: 'cross-topic' }
		]);
		await until(() => remote.published.length >= 6);
		await wait(20);
		expect(remote.published.map(entry => entry.data)).toEqual([101, 102, 103, 1, 2, 6]);
		expect(remote.published.slice(0, 3).map(entry => entry.options.compress)).toEqual([false, true, false]);
		if (fallback) expect(remote.published.slice(3).map(entry => entry.options.compress)).toEqual([false, false, false]);
		else expect(remote.publishedBatches[0].options).toBeUndefined();
	} finally {
		await a.deactivate();
		await b.deactivate();
		await client.quit();
	}
});
