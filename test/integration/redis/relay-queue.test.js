import { expect, it, vi } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createPubSubBus } from '../../../src/redis/pubsub.js';
import { createShardedBus } from '../../../src/redis/sharded-pubsub.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

for (const [name, factory] of [['standard', createPubSubBus], ['sharded', createShardedBus]]) {
	it.each(['publish', 'publishBatched'])(`${name} delivers one large configured envelope remotely via %s`, async (method) => {
		const sender = createBackendClient(), receiver = createBackendClient();
		const channel = 'inttest-large-relay:' + name + ':' + method;
		const options = { channel, channelPrefix: channel + ':', maxEnvelopeBytes: 16 * 1024 * 1024 };
		const source = mockPlatform(), remote = mockPlatform();
		const bus = factory(sender, options), peer = factory(receiver, options);
		try {
			await bus.activate(source); await peer.activate(remote);
			if (name === 'sharded') await peer.follow('room');
			const data = 'x'.repeat(9 * 1024 * 1024), wrapped = bus.wrap(source);
			if (method === 'publish') wrapped.publish('room', 'event', data);
			else wrapped.publishBatched([{ topic: 'room', event: 'event', data }]);
			expect(source.published).toHaveLength(1);
			await vi.waitFor(() => expect(remote.published).toHaveLength(1), { timeout: 10000 });
			expect(remote.published[0]).toMatchObject({ topic: 'room', event: 'event', data });
		} finally {
			await bus.deactivate(); await peer.deactivate();
			await sender.quit(); await receiver.quit();
		}
	});
}

for (const [name, factory, prefix] of [
	['standard', createPubSubBus, 'pubsub'], ['sharded', createShardedBus, 'sharded_pubsub']
]) it(`${name} bounds a real relay burst and preserves accepted per-channel order across chunks`, async () => {
	const sender = createBackendClient(), receiver = createBackendClient();
	const channel = 'inttest-bounded-relay:' + name;
	const options = { channel, channelPrefix: channel + ':', shardKey: topic => topic };
	const topics = ['room-a', 'room-b', 'room-c', 'room-d'];
	const metrics = createMetrics(), source = mockPlatform(), remote = mockPlatform();
	const bus = factory(sender, { ...options, metrics }), peer = factory(receiver, options);
	const diagnostic = vi.spyOn(console, 'warn').mockImplementation(() => {});
	try {
		await bus.activate(source); await peer.activate(remote);
		if (name === 'sharded') for (const topic of topics) await peer.follow(topic);
		const wrapped = bus.wrap(source);
		for (let i = 0; i < 9000; i++) wrapped.publish(topics[i % topics.length], 'event', i);
		expect(source.published).toHaveLength(9000);
		expect(metrics.serialize()).toContain(prefix + '_relay_queue_envelopes{state="queued"} 8192');
		expect(metrics.serialize()).toContain(prefix + '_messages_local_only_total{reason="queue_full"} 808');
		expect(diagnostic).toHaveBeenCalledTimes(1);
		await vi.waitFor(() => expect(remote.published).toHaveLength(8192), { timeout: 10000 });
		for (let group = 0; group < topics.length; group++) {
			expect(remote.published.filter(message => message.topic === topics[group]).map(message => message.data))
				.toEqual(Array.from({ length: 2048 }, (_, i) => i * topics.length + group));
		}
		await vi.waitFor(() => expect(metrics.serialize()).toContain(prefix + '_relay_queue_envelopes{state="in_flight"} 0'));
		wrapped.publish(topics[0], 'event', 'recovered');
		await vi.waitFor(() => expect(remote.published.at(-1).data).toBe('recovered'));
		await vi.waitFor(() => expect(metrics.serialize()).toContain(prefix + '_relay_attempts_total{outcome="success"} 8193'));
	} finally {
		diagnostic.mockRestore();
		await bus.deactivate(); await peer.deactivate();
		await sender.quit(); await receiver.quit();
	}
});
