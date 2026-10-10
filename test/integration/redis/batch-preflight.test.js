import { describe, expect, it } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
for (const [name, factory] of [['standard', createPubSubBus], ['sharded', createShardedBus]]) {
	describe(`${name} atomic batch preflight (integration)`, () => {
		it('rejects the whole batch before a Redis command and then relays a valid batch', async () => {
			const client = createBackendClient({ keyPrefix: 'inttest-batch-preflight:' });
			const peerClient = createBackendClient({ keyPrefix: 'inttest-batch-preflight:' });
			const channel = 'inttest-preflight:' + name + ':' + Date.now();
			const options = { channel, channelPrefix: channel + ':', maxEnvelopeBytes: 256 };
			const bus = factory(client, options), peer = factory(peerClient, options);
			const source = mockPlatform(), remote = mockPlatform();
			try {
				await bus.activate(source); await peer.activate(remote);
				if (name === 'sharded') { await peer.follow('a'); await peer.follow('b'); }
				const wrapped = bus.wrap(source);
				const command = name === 'sharded' ? 'spublish' : 'publish';
				const calls = async () => {
					const nodes = typeof client.redis.nodes === 'function' ? client.redis.nodes('master') : [client.redis];
					const counts = await Promise.all(nodes.map(async node => Number((await node.info('commandstats')).match(new RegExp('cmdstat_' + command + ':calls=(\\d+)'))?.[1] || 0)));
					return counts.reduce((sum, count) => sum + count, 0);
				};
				const before = await calls();
				expect(() => wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'b', event: 'x', data: 'x'.repeat(512) }])).toThrow(RangeError);
				expect(() => wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'b', event: 'x', data: 1n }])).toThrow(TypeError);
				await wait(30);
				expect(await calls()).toBe(before);
				expect(source.published).toEqual([]);
				expect(remote.published).toEqual([]);
				wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'a', event: 'x', data: 2 }], { compress: true });
				const until = Date.now() + 3000;
				while (remote.published.length < 2 && Date.now() < until) await wait(10);
				expect(source.published.map(message => message.data)).toEqual([1, 2]);
				expect(remote.published.map(message => message.data)).toEqual([1, 2]);
				expect(await calls()).toBe(before + 1);
			} finally {
				await bus.deactivate(); await peer.deactivate();
				await client.quit(); await peerClient.quit();
			}
		});
	});
}
