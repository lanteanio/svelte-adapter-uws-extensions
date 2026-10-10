import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

for (const [name, factory, prefix, method] of [
	['standard', createPubSubBus, 'pubsub', 'publish'],
	['sharded', createShardedBus, 'sharded_pubsub', 'spublish']
]) describe(`${name} relay outcomes`, () => {
	let client, source, metrics, bus, clock, diagnostics;
	beforeEach(() => {
		vi.useFakeTimers(); clock = 0;
		setRuntimeEnv({ clock: { monotonic: () => clock } });
		client = mockRedisClient(); source = mockPlatform(); metrics = createMetrics();
		diagnostics = captureRegistered();
	});
	afterEach(async () => { await bus?.deactivate(); diagnostics.stop(); resetRuntimeEnv(); vi.useRealTimers(); });
	function setup(options = {}) { bus = factory(client, { metrics, shardKey: topic => topic, ...options }); return bus.wrap(source); }
	function count(suffix, labels) {
		const line = metrics.serialize().split('\n').find(line => line.startsWith(prefix + suffix + (labels ? '{' + labels + '} ' : ' ')));
		return line ? Number(line.split(' ').at(-1)) : 0;
	}
	const flush = () => vi.advanceTimersByTimeAsync(0);
	it('counts envelopes separately from their logical message count and reports seconds', async () => {
		const wrapped = setup();
		const original = client.redis[method].bind(client.redis);
		client.redis[method] = (...args) => { clock += 1250; return original(...args); };
		wrapped.publishBatched([{ topic: 'room', event: 'event', data: 1 }, { topic: 'room', event: 'event', data: 2 }]);
		await flush();
		expect(count('_relay_attempts_total', 'outcome="success"')).toBe(1);
		expect(count('_messages_local_only_total', 'reason="backend_error"')).toBe(0);
		expect(count('_relay_duration_seconds_sum')).toBe(1.25);
		expect(count('_relay_duration_seconds_count')).toBe(1);
		expect(source.published).toHaveLength(2);
	});
	it('counts partial pipeline errors only for failed envelopes and all their messages', async () => {
		const wrapped = setup();
		const original = client.redis[method].bind(client.redis); let calls = 0;
		client.redis[method] = (...args) => ++calls === 1 ? Promise.reject(new Error('NOPERM private-room-user@example.test')) : original(...args);
		wrapped.publishBatched([{ topic: 'private-room', event: 'event', data: 1 }, { topic: 'private-room', event: 'event', data: 2 }]);
		wrapped.publish('other', 'event', 3);
		await flush();
		expect(count('_relay_attempts_total', 'outcome="error"')).toBe(1);
		expect(count('_relay_attempts_total', 'outcome="success"')).toBe(1);
		expect(count('_messages_local_only_total', 'reason="backend_error"')).toBe(2);
		expect(source.published).toHaveLength(3);
		expect(metrics.serialize()).not.toContain('private-room');
		expect(metrics.serialize()).not.toContain('user@example.test');
	});
	it('counts a rejected whole pipeline once per envelope, then records recovery', async () => {
		const wrapped = setup();
		const pipeline = client.redis.pipeline.bind(client.redis);
		client.redis.pipeline = () => new Proxy(pipeline(), { get: (target, key) => key === 'exec' ? () => Promise.reject(new Error('connection lost')) : target[key] });
		wrapped.publish('a', 'event', 1); wrapped.publish('b', 'event', 2);
		await flush();
		expect(count('_relay_attempts_total', 'outcome="error"')).toBe(2);
		expect(count('_messages_local_only_total', 'reason="backend_error"')).toBe(2);
		client.redis.pipeline = pipeline;
		wrapped.publish('a', 'event', 3); await flush();
		expect(count('_relay_attempts_total', 'outcome="success"')).toBe(1);
		expect(count('_relay_duration_seconds_count')).toBe(2);
	});
	it('distinguishes open-breaker refusal without inventing a backend duration', async () => {
		const breaker = { guard() { throw new Error('breaker open'); }, success() {}, failure() {} };
		const wrapped = setup({ breaker });
		const publish = vi.spyOn(client.redis, method);
		wrapped.publishBatched([{ topic: 'room', event: 'event', data: 1 }, { topic: 'room', event: 'event', data: 2 }]);
		await flush();
		expect(publish).not.toHaveBeenCalled();
		expect(count('_relay_attempts_total', 'outcome="breaker_open"')).toBe(1);
		expect(count('_messages_local_only_total', 'reason="breaker_open"')).toBe(2);
		expect(count('_relay_duration_seconds_count')).toBe(0);
	});
	it('separates locally retained oversized publishes from atomically refused batches and relay opt-outs', async () => {
		const wrapped = setup({ maxEnvelopeBytes: 128 });
		wrapped.publish('a', 'event', 'x'.repeat(300));
		expect(() => wrapped.publishBatched([{ topic: 'a', event: 'event', data: 'x'.repeat(300) }])).toThrow(RangeError);
		wrapped.publish('a', 'event', 'local', { relay: false });
		await flush();
		expect(source.published).toHaveLength(2);
		expect(count('_messages_local_only_total', 'reason="envelope_oversized"')).toBe(1);
		expect(count('_relay_attempts_total', 'outcome="error"')).toBe(0);
		expect(count('_relay_duration_seconds_count')).toBe(0);
	});
	it('records direct and synchronous publish failures without losing the local result', async () => {
		const wrapped = setup();
		client.redis[method] = () => { throw new Error('synchronous send failure'); };
		wrapped.publish('a', 'event', 1);
		await flush();
		expect(source.published).toHaveLength(1);
		expect(count('_relay_attempts_total', 'outcome="error"')).toBe(1);
		expect(count('_messages_local_only_total', 'reason="backend_error"')).toBe(1);
		expect(count('_relay_duration_seconds_count')).toBe(1);
	});
	it('counts queued messages discarded at deactivation without inventing attempts', async () => {
		const wrapped = setup();
		await bus.activate(source);
		wrapped.publish('a', 'event', 1);
		await bus.deactivate(); await flush();
		expect(count('_messages_local_only_total', 'reason="deactivated"')).toBe(1);
		expect(count('_relay_attempts_total', 'outcome="error"')).toBe(0);
		expect(count('_relay_duration_seconds_count')).toBe(0);
	});
	it('does not read the duration clock when metrics are disabled', async () => {
		const monotonic = vi.fn(() => 0);
		setRuntimeEnv({ clock: { monotonic } });
		const wrapped = setup({ metrics: undefined });
		wrapped.publish('a', 'event', 1); await flush();
		expect(monotonic).not.toHaveBeenCalled();
	});
});
