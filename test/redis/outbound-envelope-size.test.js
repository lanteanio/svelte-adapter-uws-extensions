import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createTopicBroadcast } from '../../src/redis/topic-broadcast.js';

const settle = () => new Promise((resolve) => setTimeout(resolve, 10));
const factories = [
	['pubsub', (client, options) => createPubSubBus(client, options)],
	['sharded', (client, options) => createShardedBus(client, { ...options, shardKey: () => 'all' })]
];

for (const traced of [false, true]) describe.each(factories)('%s outbound envelope limits, traced=' + traced, (_name, createBus) => {
	it.each(['publish', 'batch', 'publishBatched', 'publishWire', 'relayCoalesced'])('reports and skips oversized %s relays, then recovers', async (method) => {
		const client = mockRedisClient();
		const a = mockPlatform(), b = mockPlatform();
		a.trace.enabled = traced;
		a.traceContext = { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01' };
		a.publishWire = vi.fn(() => true);
		const origin = createBus(client, { maxEnvelopeBytes: 320 });
		const peer = createBus(client, { maxEnvelopeBytes: 320 });
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		await origin.activate(a);
		await peer.activate(b);
		if (peer.follow) await peer.follow('chat');
		const wrapped = origin.wrap(a);
		const publish = vi.spyOn(client.redis, 'publish');
		const spublish = vi.spyOn(client.redis, 'spublish');
		const send = (data) => {
			if (method === 'publishWire') return wrapped.publishWire('chat', 'msg', data, { shared: true, capability: 'test:1' });
			if (method === 'relayCoalesced') return wrapped.relayCoalesced('chat', 'msg', data, 'key');
			if (method === 'publish') return wrapped.publish('chat', 'msg', data);
			return wrapped[method]([{ topic: 'chat', event: 'msg', data }]);
		};
		try {
			send(String.fromCharCode(0x20ac).repeat(120));
			send(String.fromCharCode(0x20ac).repeat(120));
			await settle();
			expect(b.published).toHaveLength(0);
			expect(publish).not.toHaveBeenCalled();
			expect(spublish).not.toHaveBeenCalled();
			expect(warn).toHaveBeenCalledOnce();
			expect(warn.mock.calls[0][0]).toContain('event=redis.pubsub.envelope.oversized');
			expect(warn.mock.calls[0][0]).not.toContain(String.fromCharCode(0x20ac));
			if (method === 'publishWire') expect(a.publishWire).toHaveBeenCalledTimes(2);
			else if (method !== 'relayCoalesced') expect(a.published).toHaveLength(2);
			send('ok');
			await settle();
			if (method !== 'relayCoalesced') expect(b.published).toHaveLength(1);
			send(String.fromCharCode(0x20ac).repeat(120));
			expect(warn).toHaveBeenCalledTimes(2);
		} finally {
			await origin.deactivate();
			await peer.deactivate();
			warn.mockRestore();
		}
	});

	it('checks the complete wire batch envelope', async () => {
		const client = mockRedisClient();
		const a = mockPlatform();
		a.trace.enabled = traced;
		const bus = createBus(client, { maxEnvelopeBytes: 320 });
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			bus.wrap(a).publishBatched(Array.from({ length: 4 }, () => ({ topic: 'chat', event: 'msg', data: 'x'.repeat(50) })));
			expect(warn).toHaveBeenCalledOnce();
			expect(a.published).toHaveLength(4);
		} finally {
			await bus.deactivate();
			warn.mockRestore();
		}
	});
});

it('rejects oversized topic requests before serving local subscribers', async () => {
	const client = mockRedisClient();
	const broadcast = createTopicBroadcast(client, { maxEnvelopeBytes: 320 });
	const handler = vi.fn(async () => []);
	broadcast.onRequest(handler);
	try {
		await expect(broadcast.broadcast('chat', 'msg', String.fromCharCode(0x20ac).repeat(120))).rejects.toThrow('maxEnvelopeBytes');
		expect(handler).not.toHaveBeenCalled();
	} finally {
		await broadcast.destroy();
	}
});

it('reports oversized topic replies without publishing them and re-arms after a valid reply', async () => {
	const client = mockRedisClient();
	const requester = createTopicBroadcast(client, { maxEnvelopeBytes: 320, requestTimeoutMs: 15 });
	const responder = createTopicBroadcast(client, { maxEnvelopeBytes: 320 });
	let data = String.fromCharCode(0x20ac).repeat(120);
	responder.onRequest(async () => [{ ok: true, reply: data }]);
	const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
	try {
		await requester.broadcast('chat', 'msg', null);
		await requester.broadcast('chat', 'msg', null);
		expect(warn).toHaveBeenCalledOnce();
		expect(warn.mock.calls[0][0]).toContain('event=redis.topic-broadcast.envelope.oversized');
		data = 'ok';
		expect(await requester.broadcast('chat', 'msg', null)).toEqual([{ ok: true, reply: 'ok' }]);
		data = String.fromCharCode(0x20ac).repeat(120);
		await requester.broadcast('chat', 'msg', null);
		expect(warn).toHaveBeenCalledTimes(2);
	} finally {
		await requester.destroy();
		await responder.destroy();
		warn.mockRestore();
	}
});
