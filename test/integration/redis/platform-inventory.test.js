import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { WebSocket } from 'ws';
import { createTestServer } from 'svelte-adapter-uws/testing';
import { PLATFORM_MEMBERS } from 'svelte-adapter-uws/platform-contract';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { PROVIDED_ANYWAY } from '../../helpers/wrap-surface.js';

describe('published Platform capabilities through native Redis wrappers', () => {
	it.each(['classic', 'sharded'])('preserves native %s capabilities and remote delivery', async kind => {
		const client = createBackendClient({ keyPrefix: 'inttest-platform-inventory:' });
		const channel = 'platform_' + randomUUID();
		const topic = 'application';
		const create = () => kind === 'classic'
			? createPubSubBus(client, { channel, systemChannel: false })
			: createShardedBus(client, { channelPrefix: channel + ':' });
		const buses = [create(), create()];
		const servers = [];
		let nativeSocket, socket;
		const frames = [];
		let opened;
		const ready = new Promise(resolve => { opened = resolve; });
		try {
			servers.push(await createTestServer({ handler: {} }));
			servers.push(await createTestServer({ handler: {
				open(ws, { platform }) {
					nativeSocket = ws;
					platform.subscribe(ws, topic);
					opened();
				}
			} }));
			for (let index = 0; index < buses.length; index++) await buses[index].activate(servers[index].platform);
			if (kind === 'sharded') await buses[1].follow(topic);
			socket = new WebSocket(servers[1].wsUrl);
			socket.on('message', (raw, binary) => {
				if (!binary) frames.push(JSON.parse(raw.toString()));
			});
			await new Promise((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject); });
			await ready;
			const source = buses[0].wrap(servers[0].platform);
			const target = buses[1].wrap(servers[1].platform);
			for (const member of PLATFORM_MEMBERS) {
				if (servers[1].platform[member] === undefined && Object.hasOwn(PROVIDED_ANYWAY, member)) continue;
				expect(typeof target[member], member).toBe(typeof servers[1].platform[member]);
			}
			expect(target.grantPublish(nativeSocket, topic)).toBe(true);
			expect(target.publishGrant(nativeSocket)).toBe(topic);
			expect(target.revokePublish(nativeSocket)).toBe(true);
			expect(target.publishGrant(nativeSocket)).toBe(null);
			expect(target.metrics).toBe(servers[1].platform.metrics);
			expect(target.assertions).toBe(servers[1].platform.assertions);
			const nonce = randomUUID();
			source.publish(topic, 'remote', { nonce }, { seq: false });
			await expect.poll(() => frames.some(frame => frame.event === 'remote' && frame.data?.nonce === nonce), { timeout: 5000 }).toBe(true);
			const codec = { capability: 'inventory', schemaVersion: 1, encode: () => null };
			target.registerWireCodec(codec);
			target.publishWireBatch(topic, 'publish-batch', [{ data: { n: 1 } }, { data: { n: 2 } }], codec, { seq: false });
			target.sendWireBatch(nativeSocket, topic, 'send-batch', [{ data: { n: 3 } }, { data: { n: 4 } }], codec);
			await expect.poll(() => frames.filter(frame => frame.event === 'publish-batch' || frame.event === 'send-batch').length, { timeout: 5000 }).toBe(4);
			expect(frames.filter(frame => frame.event === 'publish-batch').map(frame => frame.data.n)).toEqual([1, 2]);
			expect(frames.filter(frame => frame.event === 'send-batch').map(frame => frame.data.n)).toEqual([3, 4]);
		} finally {
			for (const bus of buses) await bus.deactivate();
			if (socket && socket.readyState !== WebSocket.CLOSED) await new Promise(resolve => { socket.once('close', resolve); socket.terminate(); });
			for (const server of servers.reverse()) await server.close();
			await client.quit();
		}
	});
});
