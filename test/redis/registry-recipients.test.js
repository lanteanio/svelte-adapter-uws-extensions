import { afterEach, describe, expect, it } from 'vitest';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

describe('registry broadcast recipient counts', () => {
	const registries = [];
	afterEach(async () => {
		await Promise.all(registries.splice(0).map((registry) => registry.destroy()));
	});

	function setup(client = mockRedisClient('app:'), options = {}) {
		const metrics = createMetrics();
		const platform = mockPlatform();
		const registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData().userId,
			attributes: (ws) => ({ cohort: ws.getUserData().cohort }),
			metrics,
			...options
		});
		registries.push(registry);
		return { client, registry, platform, metrics };
	}

	async function connect(owner, id, cohort = 'members') {
		const ws = mockWs({ userId: id, cohort });
		ws.getUserData()[WS_SESSION_ID] = 'socket-' + id;
		await owner.registry.hooks.open(ws, { platform: owner.platform });
		return ws;
	}

	it.each([10, 100, 1000])('returns and records %i local recipients for one call', async (size) => {
		const owner = setup();
		for (let i = 0; i < size; i++) await connect(owner, 'user-' + i);
		await connect(owner, 'outside', 'other');
		owner.platform.sent.length = 0;
		expect(await owner.registry.sendTo({ cohort: 'members' }, 'updates', 'changed', 1)).toBe(size);
		expect(owner.platform.sent).toHaveLength(size);
		const output = await owner.metrics.serialize();
		expect(output).toContain('push_sendto_recipients_total ' + size + '\n');
		expect(output).toContain('push_sendto_total{result="ok"} 1');
	});

	it('returns zero when no recipient matches', async () => {
		const owner = setup();
		await connect(owner, 'outside', 'other');
		expect(await owner.registry.sendTo({ cohort: 'members' }, 'updates', 'changed')).toBe(0);
		expect(owner.platform.sent).toHaveLength(0);
	});

	it('counts remote users instead of their shared owner envelope', async () => {
		const owner = setup();
		const peer = setup(owner.client);
		await connect(owner, 'local');
		await connect(peer, 'remote-a');
		await connect(peer, 'remote-b');
		await new Promise((resolve) => setImmediate(resolve));
		expect(await owner.registry.sendTo({ cohort: 'members' }, 'updates', 'changed')).toBe(3);
		await expect.poll(() => peer.platform.sent.length).toBe(2);
		expect(owner.platform.sent).toHaveLength(1);
		expect(await owner.metrics.serialize()).toContain('push_sendto_recipients_total 3\n');
		expect(await peer.metrics.serialize()).not.toContain('push_sendto_recipients_total 2\n');
	});

	it('excludes throwing local sends and failed remote publishes', async () => {
		const owner = setup();
		const peer = setup(owner.client);
		const refused = await connect(owner, 'closed');
		await connect(owner, 'local');
		await connect(peer, 'remote');
		await new Promise((resolve) => setImmediate(resolve));
		const send = owner.platform.send.bind(owner.platform);
		owner.platform.send = (ws, ...args) => {
			if (ws === refused) throw new Error('closed');
			return send(ws, ...args);
		};
		const publish = owner.client.redis.publish.bind(owner.client.redis);
		owner.client.redis.publish = (channel, ...args) => channel.includes('__push:')
			? Promise.reject(new Error('connection closed')) : publish(channel, ...args);
		expect(await owner.registry.sendTo({ cohort: 'members' }, 'updates', 'changed')).toBe(1);
		expect(owner.platform.sent).toHaveLength(1);
		expect(peer.platform.sent).toHaveLength(0);
		const output = await owner.metrics.serialize();
		expect(output).toContain('push_sendto_recipients_total 1\n');
		expect(output).toContain('push_sendto_total{result="error"} 1');
	});

	it('records local recipients before refusing an oversized remote envelope', async () => {
		const owner = setup(undefined, { maxEnvelopeBytes: 512 });
		const peer = setup(owner.client);
		await connect(owner, 'local');
		await connect(peer, 'remote');
		await new Promise((resolve) => setImmediate(resolve));
		await expect(owner.registry.sendTo({ cohort: 'members' }, 'updates', 'changed', 'x'.repeat(4096)))
			.rejects.toThrow(/maxEnvelopeBytes/);
		expect(owner.platform.sent).toHaveLength(1);
		expect(peer.platform.sent).toHaveLength(0);
		const output = await owner.metrics.serialize();
		expect(output).toContain('push_sendto_recipients_total 1\n');
		expect(output).toContain('push_sendto_total{result="refused"} 1');
	});
});
