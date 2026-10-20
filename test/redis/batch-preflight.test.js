import { describe, expect, it, vi } from 'vitest';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const flush = () => new Promise(resolve => setTimeout(resolve, 10));
for (const [name, factory, prefix] of [['standard', createPubSubBus, 'pubsub'], ['sharded', createShardedBus, 'sharded_pubsub']]) {
	for (const traced of [false, true]) describe(`${name} batch preflight, traced=${traced}`, () => {
		async function setup(maxEnvelopeBytes = 256) {
			const client = mockRedisClient();
			const metrics = createMetrics();
			const options = { maxEnvelopeBytes, shardKey: topic => topic, metrics };
			const bus = factory(client, options), peer = factory(client, { ...options, metrics: undefined });
			const source = mockPlatform(), remote = mockPlatform();
			if (traced) { source.trace.enabled = true; source.traceContext = { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01' }; }
			await bus.activate(source); await peer.activate(remote);
			if (name === 'sharded') { await peer.follow('a'); await peer.follow('b'); }
			const sent = [];
			const method = name === 'sharded' ? 'spublish' : 'publish';
			const publish = client.redis[method].bind(client.redis);
			client.redis[method] = (...args) => { sent.push(args); return publish(...args); };
			return { source, remote, sent, metrics, wrapped: bus.wrap(source), async close() { await bus.deactivate(); await peer.deactivate(); } };
		}
		it('rejects an oversized later member before any local or remote batch delivery', async () => {
			const s = await setup();
			try {
				expect(() => s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'b', event: 'x', data: 'x'.repeat(512) }])).toThrow(RangeError);
				await flush();
				expect(s.source.published).toEqual([]);
				expect(s.remote.published).toEqual([]);
				expect(s.sent).toEqual([]);
				expect(s.metrics.serialize()).toContain(prefix + '_batch_rejected_total{reason="oversized"} 1');
			} finally { await s.close(); }
		});
		it('rejects a non-serializable later member before any effect', async () => {
			const s = await setup();
			try {
				expect(() => s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'b', event: 'x', data: 1n }])).toThrow(TypeError);
				await flush();
				expect(s.source.published).toEqual([]);
				expect(s.sent).toEqual([]);
				expect(s.metrics.serialize()).toContain(prefix + '_batch_rejected_total{reason="serialize"} 1');
			} finally { await s.close(); }
		});
		it('applies UTF-8 byte limits and leaves relay:false batches local', async () => {
			const s = await setup();
			try {
				const message = { topic: 'a', event: 'x', data: '\u6f22'.repeat(100) };
				expect(() => s.wrapped.publishBatched([message])).toThrow(RangeError);
				s.wrapped.publishBatched([{ ...message, options: { relay: false } }]);
				await flush();
				expect(s.source.published).toHaveLength(1);
				expect(s.sent).toEqual([]);
			} finally { await s.close(); }
		});
		it('reuses accepted bytes and retains compression, sequence and message order', async () => {
			const s = await setup(2048);
			let serializations = 0;
			try {
				const messages = [1, 2].map(n => ({ topic: 'a', event: 'x', data: { toJSON() { serializations++; return n; } }, options: { seq: n, jitterMs: 10 } }));
				s.wrapped.publishBatched(messages, { compress: true });
				await flush();
				expect(serializations).toBe(2);
				expect(s.source.published).toHaveLength(2);
				expect(s.remote.published.map(m => m.data)).toEqual([1, 2]);
				const envelope = JSON.parse(s.sent[0][1]);
				expect(envelope.compress).toBe(true);
				expect(envelope.batch.map(m => [m.s, m.j])).toEqual([[1, 10], [2, 10]]);
			} finally { await s.close(); }
		});
		if (traced) it('omits an oversized carrier when the plain batch fits', async () => {
			const s = await setup(140);
			const captured = captureRegistered();
			try {
				s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: 'small' }]);
				await flush();
				expect(s.remote.published).toHaveLength(1);
				expect(JSON.parse(s.sent[0][1]).traceparent).toBeUndefined();
				expect(s.metrics.serialize()).not.toContain(prefix + '_batch_rejected_total{reason=');
			} finally { captured.stop(); await s.close(); }
		});
		const big = 'x'.repeat(9 * 1024 * 1024);
		const topicsOf = list => list.map(m => m.topic);
		it.each([['large first', ['a', 'b']], ['large last', ['b', 'a']]])('relays a whole call above the queue byte budget on an idle bus (%s)', async (_, order) => {
			const s = await setup(16 * 1024 * 1024);
			try {
				s.wrapped.publishBatched(order.map(topic => ({ topic, event: 'x', data: topic === 'a' ? big : 'small' })));
				expect(topicsOf(s.source.published)).toEqual(order);
				await vi.waitFor(() => expect(topicsOf(s.remote.published).sort()).toEqual(['a', 'b']));
				expect(s.metrics.serialize()).not.toContain('reason="queue_full"');
			} finally { await s.close(); }
		});
		it.each([['large first', ['a', 'b']], ['large last', ['b', 'a']]])('refuses the whole call when queued work leaves no room for it (%s)', async (_, order) => {
			const s = await setup(16 * 1024 * 1024);
			try {
				s.wrapped.publish('c', 'x', 'queued first');
				const call = () => s.wrapped.publishBatched(order.map(topic => ({ topic, event: 'x', data: topic === 'a' ? big : 'small' })));
				expect(call).toThrow(expect.objectContaining({ name: 'RangeError', code: 'RELAY_QUEUE_FULL' }));
				await flush();
				expect(topicsOf(s.source.published)).toEqual(['c']);
				expect(s.remote.published.filter(m => m.topic !== 'c')).toEqual([]);
				expect(s.metrics.serialize()).toContain(prefix + '_batch_rejected_total{reason="queue_full"} 1');
				expect(s.metrics.serialize()).not.toContain(prefix + '_messages_local_only_total{reason="queue_full"}');
				// The refusal charged nothing: the same call goes through once the queue drains.
				call();
				expect(topicsOf(s.source.published)).toEqual(['c', ...order]);
				await vi.waitFor(() => expect(topicsOf(s.remote.published).filter(t => t !== 'c').sort()).toEqual(['a', 'b']));
			} finally { await s.close(); }
		});
		if (name === 'sharded') it('refuses a call whose shard envelopes together exceed the idle ceiling', async () => {
			const s = await setup(16 * 1024 * 1024);
			try {
				expect(() => s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: big }, { topic: 'b', event: 'x', data: big }])).toThrow(expect.objectContaining({ name: 'RangeError', code: 'RELAY_QUEUE_FULL' }));
				await flush();
				expect(s.source.published).toEqual([]);
				expect(s.sent).toEqual([]);
				expect(s.metrics.serialize()).toContain(prefix + '_batch_rejected_total{reason="queue_full"} 1');
			} finally { await s.close(); }
		});
		it('takes the relay back when local delivery throws', async () => {
			const s = await setup();
			try {
				const failure = new Error('local delivery failed');
				const original = s.source.publishBatched;
				s.source.publishBatched = () => { throw failure; };
				expect(() => s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: 1 }, { topic: 'b', event: 'x', data: 2 }])).toThrow(failure);
				s.source.publishBatched = original;
				await flush();
				expect(s.sent).toEqual([]);
				expect(s.remote.published).toEqual([]);
				s.wrapped.publishBatched([{ topic: 'a', event: 'x', data: 3 }]);
				await flush();
				expect(s.remote.published.map(m => m.data)).toEqual([3]);
			} finally { await s.close(); }
		});
	});
}
