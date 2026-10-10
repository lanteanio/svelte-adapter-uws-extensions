import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createRelayQueue } from '../../src/redis/_relay-queue.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';

beforeEach(() => vi.useFakeTimers());
afterEach(() => { resetRuntimeEnv(); vi.useRealTimers(); vi.restoreAllMocks(); });
function queue(submit = vi.fn(() => Promise.resolve()), options = {}) {
	const dropped = vi.fn(), overflow = vi.fn();
	const internal = createRelayQueue({ submit, dropped, overflow, ...options });
	return { queue: { ...internal, push: entry => internal.push(entry.msg, entry.count) }, submit, dropped, overflow };
}
const entry = (msg = 'x', count = 1) => ({ msg, count });

it('does not keep abandoned relay queues alive through the metrics registry', () => {
	execFileSync(process.execPath, ['--expose-gc', fileURLToPath(new URL('../fixtures/relay-queue-gc.mjs', import.meta.url))], {
		windowsHide: true, timeout: 15000
	});
});

describe('bounded relay admission and scheduling', () => {
	it('admits one large configured envelope and keeps its capacity until settlement', async () => {
		let resolve;
		const { queue: q, submit, dropped, overflow } = queue(vi.fn(() => new Promise(r => { resolve = r; })));
		const msg = 'x'.repeat(9 * 1024 * 1024);
		expect(q.push(entry(msg))).toBe(true);
		expect(q.state.snapshot()).toMatchObject({ retained: 1, retainedBytes: msg.length });
		expect(overflow).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(0);
		expect(submit.mock.calls[0][0].map(value => value.msg)).toEqual([msg]);
		expect(q.push(entry('while pending'))).toBe(false);
		expect(q.push(entry(msg))).toBe(false);
		expect(dropped).toHaveBeenCalledTimes(2);
		expect(overflow).toHaveBeenCalledTimes(1);
		q.clear();
		expect(q.state.snapshot().retainedBytes).toBe(msg.length);
		resolve(); await Promise.resolve();
		expect(q.push(entry('after settlement'))).toBe(true);
		q.clear();
	});
	it('retains submitted capacity until actual settlement and refuses newest entries', async () => {
		let resolve;
		const { queue: q, submit, dropped, overflow } = queue(vi.fn(() => new Promise(r => { resolve = r; })));
		for (let i = 0; i < 8192; i++) expect(q.push(entry(String(i)))).toBe(true);
		await vi.advanceTimersByTimeAsync(0);
		expect(submit).toHaveBeenCalledTimes(1);
		expect(submit.mock.calls[0][0]).toHaveLength(256);
		expect(q.push(entry('refused'))).toBe(false);
		expect(q.push(entry('refused again', 2))).toBe(false);
		expect(dropped.mock.calls).toEqual([['queue_full', 1, 1], ['queue_full', 1, 2]]);
		expect(overflow).toHaveBeenCalledTimes(1);
		await vi.advanceTimersByTimeAsync(1000);
		expect(submit).toHaveBeenCalledTimes(1);
		resolve(); await Promise.resolve();
		expect(q.push(entry('after settlement'))).toBe(true);
		q.clear();
	});
	it('uses encoded UTF-8 byte limits, including an individually large accepted envelope', async () => {
		const { queue: q, submit, dropped } = queue();
		const msg = '😀'.repeat(512 * 1024);
		for (let i = 0; i < 4; i++) expect(q.push(entry(msg))).toBe(true);
		expect(q.push(entry('x'))).toBe(false);
		expect(q.state.snapshot().retainedBytes).toBe(8 * 1024 * 1024);
		await vi.advanceTimersByTimeAsync(0);
		expect(submit.mock.calls[0][0]).toHaveLength(1);
		expect(dropped).toHaveBeenCalledWith('queue_full', 1, 1);
		q.clear();
	});
	it('reconciles conservative bytes before refusal and releases normalized in-flight bytes exactly', async () => {
		let resolve;
		const { queue: q } = queue(() => new Promise(r => { resolve = r; }));
		const msg = 'x'.repeat(512 * 1024);
		q.push(entry(msg)); await vi.advanceTimersByTimeAsync(0);
		for (let i = 1; i < 16; i++) expect(q.push(entry(msg))).toBe(true);
		expect(q.push(entry('x'))).toBe(false);
		expect(q.state.snapshot().retainedBytes).toBe(8 * 1024 * 1024);
		resolve(); await Promise.resolve();
		expect(q.state.snapshot().retainedBytes).toBe(7.5 * 1024 * 1024);
		expect(q.push(entry(msg))).toBe(true);
		q.clear();
		expect(q.state.snapshot().retainedBytes).toBe(0);
	});
	it('yields between bounded chunks and preserves admission order across refills', async () => {
		const events = [], delivered = [];
		const { queue: q } = queue(batch => { events.push('relay'); delivered.push(...batch.map(e => Number(e.msg))); });
		for (let i = 0; i < 600; i++) q.push(entry(String(i)));
		setTimeout(() => events.push('other work'), 0);
		await vi.advanceTimersByTimeAsync(0);
		expect(delivered).toHaveLength(256);
		expect(events).toEqual(['relay', 'other work']);
		for (let i = 600; i < 900; i++) q.push(entry(String(i)));
		await vi.runAllTimersAsync();
		expect(delivered).toEqual(Array.from({ length: 900 }, (_, i) => i));
		expect(q.state.snapshot().retained).toBe(0);
	});
	it('clears unsent payloads without releasing unresolved capacity or reviving old work', async () => {
		let resolve;
		const { queue: q, submit, dropped } = queue(vi.fn(() => new Promise(r => { resolve = r; })));
		for (let i = 0; i < 300; i++) q.push(entry(String(i)));
		await vi.advanceTimersByTimeAsync(0);
		q.clear();
		expect(dropped).toHaveBeenCalledWith('deactivated', 44, 44);
		expect(q.state.snapshot().retained).toBe(256);
		q.push(entry('new generation'));
		resolve(); await vi.runAllTimersAsync();
		expect(submit).toHaveBeenCalledTimes(2);
		expect(submit.mock.calls[1][0].map(e => e.msg)).toEqual(['new generation']);
		q.clear(); resolve();
	});
	it('aggregates shared-registry depth, bytes and oldest age without identifying channels', async () => {
		let clock = 0; setRuntimeEnv({ clock: { monotonic: () => clock } });
		const metrics = createMetrics();
		const first = queue(() => new Promise(() => {}), { metrics, prefix: 'pubsub' }).queue;
		const second = queue(undefined, { metrics, prefix: 'pubsub' }).queue;
		first.push(entry('abc')); await vi.advanceTimersByTimeAsync(0);
		clock = 1000; second.push(entry('xy'));
		clock = 2500;
		const text = metrics.serialize();
		expect(text).toContain('pubsub_relay_queue_envelopes{state="queued"} 1');
		expect(text).toContain('pubsub_relay_queue_envelopes{state="in_flight"} 1');
		expect(text).toContain('pubsub_relay_queue_bytes{state="queued"} 2');
		expect(text).toContain('pubsub_relay_queue_bytes{state="in_flight"} 3');
		expect(text).toContain('pubsub_relay_oldest_age_seconds 2.5');
		first.clear(); second.clear();
	});
});

for (const [name, factory, prefix] of [['standard', createPubSubBus, 'pubsub'], ['sharded', createShardedBus, 'sharded_pubsub']]) {
	it.each(['publish', 'publishBatched'])(`${name} relays a lone configured envelope above the normal queue budget via %s`, async (method) => {
		const diagnostics = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const client = mockRedisClient(), platform = mockPlatform(), metrics = createMetrics();
		const bus = factory(client, { metrics, maxEnvelopeBytes: 16 * 1024 * 1024 });
		try {
			const wrapped = bus.wrap(platform), data = 'x'.repeat(9 * 1024 * 1024);
			if (method === 'publish') wrapped.publish('room', 'event', data);
			else wrapped.publishBatched([{ topic: 'room', event: 'event', data }]);
			expect(platform.published).toHaveLength(1);
			expect(diagnostics).not.toHaveBeenCalled();
			await vi.runAllTimersAsync();
			expect(metrics.serialize()).toContain(prefix + '_relay_attempts_total{outcome="success"} 1');
			expect(metrics.serialize()).not.toContain(prefix + '_messages_local_only_total{reason="queue_full"}');
		} finally { await bus.deactivate(); }
	});
	it(`${name} overload preserves local delivery, reports loss once, and never fails the backend breaker`, async () => {
		const diagnostics = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const client = mockRedisClient(), platform = mockPlatform(), metrics = createMetrics();
		const breaker = { guard: vi.fn(), success: vi.fn(), failure: vi.fn() };
		const bus = factory(client, { metrics, breaker }), wrapped = bus.wrap(platform);
		for (let i = 0; i < 8194; i++) wrapped.publish('private-room', 'event', i);
		expect(platform.published).toHaveLength(8194);
		expect(diagnostics).toHaveBeenCalledTimes(1);
		expect(diagnostics.mock.calls[0][0]).not.toContain('private-room');
		expect(metrics.serialize()).toContain(prefix + '_messages_local_only_total{reason="queue_full"} 2');
		expect(breaker.failure).not.toHaveBeenCalled();
		await bus.deactivate();
		await vi.runAllTimersAsync();
		expect(breaker.guard).not.toHaveBeenCalled();
		expect(metrics.serialize()).toContain(prefix + '_relay_queue_envelopes{state="queued"} 0');
	});
}

it('keeps standard Cluster chunks on one ready master and reselects without owning its lifecycle', async () => {
	const node = () => {
		const redis = mockRedisClient().redis;
		redis.status = 'ready';
		vi.spyOn(redis, 'pipeline');
		vi.spyOn(redis, 'publish');
		vi.spyOn(redis, 'quit');
		return redis;
	};
	const first = node(), second = node();
	let masters = [first, second];
	const client = mockRedisClient();
	client.redis.nodes = () => masters;
	const randomPipeline = vi.spyOn(client.redis, 'pipeline');
	const randomPublish = vi.spyOn(client.redis, 'publish');
	const metrics = createMetrics();
	const bus = createPubSubBus(client, { metrics }), wrapped = bus.wrap(mockPlatform());
	for (let i = 0; i < 600; i++) wrapped.publish('room', 'event', i);
	await vi.runAllTimersAsync();
	expect(first.pipeline).toHaveBeenCalledTimes(3);
	expect(second.pipeline).not.toHaveBeenCalled();
	masters = [second, first]; // Topology enumeration order alone is not failover.
	wrapped.publish('room', 'event', 600); await vi.runAllTimersAsync();
	expect(first.publish).toHaveBeenCalledTimes(601);
	first.status = 'reconnecting';
	wrapped.publish('room', 'event', 601); await vi.runAllTimersAsync();
	expect(second.publish).toHaveBeenCalledTimes(1);
	first.status = 'ready';
	masters = [first]; // Removal of a still-ready handle also triggers reselection.
	wrapped.publish('room', 'event', 602); await vi.runAllTimersAsync();
	expect(first.publish).toHaveBeenCalledTimes(602);
	first.status = 'reconnecting';
	wrapped.publish('room', 'event', 603); await vi.runAllTimersAsync();
	expect(first.publish).toHaveBeenCalledTimes(602);
	expect(metrics.serialize()).toContain('pubsub_messages_local_only_total{reason="backend_error"} 1');
	expect(randomPipeline).not.toHaveBeenCalled();
	expect(randomPublish).not.toHaveBeenCalled();
	await bus.deactivate();
	expect(first.quit).not.toHaveBeenCalled();
	expect(second.quit).not.toHaveBeenCalled();
});
