import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { invalidHealthEvents } from '../helpers/health-envelope-corpus.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { createDegradationPolicy } from '../../src/shared/degradation.js';
import { HINT_HOLD_MS } from '../../src/shared/delivery-hints.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const shapes = {
	single: entry => ({ instanceId: 'peer', ...entry }),
	batch: entry => ({ instanceId: 'peer', batch: [entry] }),
	coalesced: entry => ({ instanceId: 'peer', coalesced: true, coalesceKey: 'health', ...entry }),
	wire: entry => ({ instanceId: 'peer', wire: true, capability: 'unregistered', ...entry })
};

const policyContainers = [
	['object', () => ({ custom: 'kept', streams: ['application'] })],
	['null-prototype object', () => Object.assign(Object.create(null), { custom: 'kept', messageKey: 'service.status' })],
	['array', () => ['custom', { kept: true }]]
];

for (const kind of ['pubsub', 'sharded']) {
	describe(`${kind} inbound health schema`, () => {
		for (const policyForm of ['static', 'function']) {
			it.each(policyContainers)(`round-trips the ${policyForm} policy producer's %s container`, async (_label, container) => {
				const client = mockRedisClient();
				const local = mockPlatform();
				const remote = mockPlatform();
				const breaker = createCircuitBreaker({ failureThreshold: 1 });
				const mitigation = container();
				const recovery = container();
				const policy = createDegradationPolicy({
					mitigation: policyForm === 'function' ? () => mitigation : mitigation,
					recovery: policyForm === 'function' ? () => recovery : recovery
				});
				const producer = createPubSubBus(client, { channel: 'local-policy', breaker, degradationPolicy: policy });
				const receiver = kind === 'pubsub'
					? createPubSubBus(client, { channel: 'remote-policy' })
					: createShardedBus(client, { allowSystemTopics: true });
				await producer.activate(local);
				await receiver.activate(remote);
				if (kind === 'sharded') await receiver.follow('__realtime');
				const relayed = vi.spyOn(client.redis, 'publish');
				try {
					breaker.failure();
					breaker.reset();
					expect(local.published).toHaveLength(2);
					expect(local.published.map(frame => frame.event)).toEqual(['degraded', 'recovered']);
					expect(local.published[0].data.mitigation).toBe(mitigation);
					expect(local.published[1].data.recovery).toBe(recovery);
					for (const frame of local.published) expect(frame.options).toEqual({ relay: false, seq: false });
					expect(relayed).not.toHaveBeenCalled();
					// Exercise the actual producer's JSON form as a custom peer publish.
					for (const { topic, event, data } of local.published) {
						const raw = JSON.stringify({ instanceId: 'peer', topic, event, data });
						if (kind === 'pubsub') await client.redis.publish('remote-policy', raw);
						else await client.redis.spublish('uws:sharded:__realtime', raw);
					}
					expect(remote.published.map(({ event, data }) => ({ event, data }))).toEqual(
						local.published.map(({ event, data }) => JSON.parse(JSON.stringify({ event, data })))
					);
				} finally {
					relayed.mockRestore();
					await receiver.deactivate();
					await producer.deactivate();
					breaker.destroy();
				}
			});
		}

		it.each([
			['Date', () => new Date('2020-01-01T00:00:00.000Z'), '2020-01-01T00:00:00.000Z'],
			['custom toJSON', () => ({ messageKey: 'service.status', toJSON: () => 'local-only-value' }), 'local-only-value']
		])('keeps %s serialization local without accepting primitive peer details', async (_label, container, serialized) => {
			const client = mockRedisClient();
			const local = mockPlatform();
			const remote = mockPlatform();
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const policy = createDegradationPolicy({ mitigation: container() });
			const producer = createPubSubBus(client, { channel: 'local-policy', breaker, degradationPolicy: policy });
			const receiver = kind === 'pubsub'
				? createPubSubBus(client, { channel: 'remote-policy' })
				: createShardedBus(client, { allowSystemTopics: true });
			await producer.activate(local);
			await receiver.activate(remote);
			if (kind === 'sharded') await receiver.follow('__realtime');
			const relayed = vi.spyOn(client.redis, 'publish');
			const publish = raw => kind === 'pubsub'
				? client.redis.publish('remote-policy', raw)
				: client.redis.spublish('uws:sharded:__realtime', raw);
			try {
				breaker.failure();
				expect(local.published).toHaveLength(1);
				const { topic, event, data, options } = local.published[0];
				expect(options).toEqual({ relay: false, seq: false });
				expect(relayed).not.toHaveBeenCalled();
				const raw = JSON.stringify({ instanceId: 'peer', topic, event, data });
				expect(JSON.parse(raw).data.mitigation).toBe(serialized);
				await publish(raw);
				expect(remote.published).toHaveLength(0);
				await publish(JSON.stringify({ instanceId: 'peer', topic, event, data: { at: 1, mitigation: {} } }));
				expect(remote.published).toHaveLength(1);
			} finally {
				relayed.mockRestore();
				await receiver.deactivate();
				await producer.deactivate();
				breaker.destroy();
			}
		});

		it('validates before a peer refetch can schedule a local recovery', async () => {
			vi.useFakeTimers();
			setRuntimeEnv({ clock: { now: () => Date.now(), wallEpoch: () => Date.now(), monotonic: () => Date.now() } });
			const client = mockRedisClient();
			const platform = mockPlatform();
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const bus = kind === 'pubsub' ? createPubSubBus(client, { breaker }) : createShardedBus(client, { breaker });
			try {
				await bus.activate(platform);
				const publish = data => kind === 'pubsub'
					? client.redis.publish('uws:pubsub', JSON.stringify({ instanceId: 'peer', topic: '__realtime', event: 'recovered', data }))
					: client.redis.spublish('uws:sharded:__realtime', JSON.stringify({ instanceId: 'peer', topic: '__realtime', event: 'recovered', data }));
				await publish({ recovery: { refetch: true } });
				await vi.advanceTimersByTimeAsync(HINT_HOLD_MS + 5);
				expect(platform.published.filter(frame => frame.topic === '__realtime')).toHaveLength(0);
				await publish({ at: 1, recovery: { refetch: true } });
				await vi.advanceTimersByTimeAsync(HINT_HOLD_MS + 5);
				const recovered = platform.published.filter(frame => frame.topic === '__realtime' && frame.event === 'recovered');
				expect(recovered).toHaveLength(1);
				expect(recovered[0].data.recovery.refetch).toBe(true);
			} finally {
				await bus.deactivate();
				breaker.destroy();
				resetRuntimeEnv();
				vi.useRealTimers();
			}
		});

		it.each(Object.keys(shapes))('checks %s controls before delivery', async shape => {
			const client = mockRedisClient();
			const platform = mockPlatform();
			const subscriber = mockWs();
			const other = mockWs();
			platform.subscribe(subscriber, '__realtime');
			platform.subscribe(other, 'application');
			// The default mock walk is empty; exercise actual recipient delivery.
			platform.forEachSubscriber = (topic, visit) => {
				for (const ws of [subscriber, other]) if (ws.isSubscribed(topic)) visit(ws);
			};
			const bus = kind === 'pubsub' ? createPubSubBus(client) : createShardedBus(client, { allowSystemTopics: true });
			await bus.activate(platform);
			if (kind === 'sharded') await bus.follow('__realtime');
			const publish = raw => kind === 'pubsub'
				? client.redis.publish('uws:pubsub', raw)
				: client.redis.spublish('uws:sharded:__realtime', raw);
			try {
				for (const [, event, data] of invalidHealthEvents()) {
					await publish(JSON.stringify(shapes[shape]({ topic: '__realtime', event, data })));
				}
				expect(platform.published).toHaveLength(0);
				expect(platform.sentCoalesced).toHaveLength(0);
				await publish(JSON.stringify(shapes[shape]({ topic: '__realtime', event: 'degraded', data: { at: 1 } })));
				if (shape === 'coalesced') {
					expect(platform.published).toHaveLength(0);
					expect(platform.sentCoalesced).toEqual([{
						ws: subscriber, key: '__realtime\0health', topic: '__realtime', event: 'degraded', data: { at: 1 }
					}]);
				} else {
					expect(platform.published).toHaveLength(1);
					expect(platform.published[0]).toMatchObject({ topic: '__realtime', event: 'degraded', data: { at: 1 } });
					expect(platform.sentCoalesced).toHaveLength(0);
				}
			} finally { await bus.deactivate(); }
		});
	});
}

describe('pubsub owner control topic', () => {
	it('retains known-field policy bounds on array containers before serialization', () => {
		for (const field of ['mitigation', 'recovery']) {
			for (const invalid of [
				{ bannerCopy: 'x'.repeat(2049) }, { streams: [1] }, { retryAfterMs: -1 },
				{ messageKey: 'x'.repeat(129) }, { messageParams: { x: 'x'.repeat(1025) } }, { refetch: 'yes' }
			]) {
				expect(() => createDegradationPolicy({ [field]: Object.assign([], invalid) })).toThrow();
			}
		}
	});

	it('accepts policy-produced detail at the existing notice bounds', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const bus = createPubSubBus(client);
		const policy = createDegradationPolicy({ mitigation: {
			bannerCopy: 'x'.repeat(2048), messageKey: 'x'.repeat(128),
			messageParams: { detail: 'x'.repeat(1024) }, streams: ['application'], retryAfterMs: 1000
		} });
		const data = { at: 0, mitigation: policy.onDegraded({ from: 'healthy', to: 'broken' }).mitigation };
		await bus.activate(platform);
		try {
			await client.redis.publish('uws:pubsub', JSON.stringify({ instanceId: 'peer', topic: '__realtime', event: 'degraded', data }));
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data).toEqual(data);
		} finally { await bus.deactivate(); }
	});

	it('retains application payloads and RPC/replay denials', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const bus = createPubSubBus(client);
		await bus.activate(platform);
		try {
			for (const topic of ['__rpc', '__replay:room', 'application']) {
				await client.redis.publish('uws:pubsub', JSON.stringify({ instanceId: 'peer', topic, event: 'anything', data: null }));
			}
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toMatchObject({ topic: 'application', event: 'anything', data: null });
		} finally { await bus.deactivate(); }
	});

	it('applies the same schema to a configured control topic', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const bus = createPubSubBus(client, { systemChannel: 'custom-health' });
		await bus.activate(platform);
		try {
			await client.redis.publish('uws:pubsub', JSON.stringify({ instanceId: 'peer', topic: 'custom-health', event: 'unknown', data: { at: 1 } }));
			expect(platform.published).toHaveLength(0);
			await client.redis.publish('uws:pubsub', JSON.stringify({ instanceId: 'peer', topic: 'custom-health', event: 'recovered', data: { at: 1, recovery: { custom: 'kept', refetch: false } } }));
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data.recovery.custom).toBe('kept');
		} finally { await bus.deactivate(); }
	});
});
