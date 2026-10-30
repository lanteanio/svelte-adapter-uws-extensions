import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { SUBSCRIBER_RESTORING, SUBSCRIBER_RESTORED } from '../../src/shared/redis-subscriber.js';
import { HINT_HOLD_MS } from '../../src/shared/delivery-hints.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';

const HEALTH_CHANNEL = 'uws:sharded:__realtime';

describe('sharded bus health events and refetch hints', () => {
	let client;
	const cleanups = [];
	beforeEach(() => {
		vi.useFakeTimers();
		client = mockRedisClient();
	});
	afterEach(async () => {
		while (cleanups.length) await cleanups.pop()();
		vi.useRealTimers();
	});
	const tick = () => vi.advanceTimersByTimeAsync(5);
	const hold = () => vi.advanceTimersByTimeAsync(HINT_HOLD_MS + 5);

	async function instance(options = {}) {
		const p = mockPlatform();
		const breaker = options.breaker === false ? undefined : createCircuitBreaker({ failureThreshold: 1, ...options.breaker });
		const subs = [];
		const source = options.client || client;
		const opened = { ...source, duplicate: (o) => { const dup = source.duplicate(o); subs.push(dup); return dup; } };
		const bus = createShardedBus(opened, breaker ? { breaker } : {});
		await bus.activate(p);
		await tick();
		await bus.follow('chat');
		p.reset();
		cleanups.push(async () => { await bus.deactivate(); breaker?.destroy(); });
		return { platform: p, breaker, bus, wrapped: bus.wrap(p), subscriber: () => subs[subs.length - 1] };
	}
	const frames = (p, event = 'recovered') => p.published.filter((f) => f.topic === '__realtime' && f.event === event);
	const refetches = (p) => frames(p).filter((f) => f.data.recovery?.refetch === true);

	it('publishes degraded and recovered on __realtime for its own breaker, local only', async () => {
		const a = await instance();
		a.breaker.failure();
		a.breaker.reset();
		expect(frames(a.platform, 'degraded')).toEqual([{ topic: '__realtime', event: 'degraded', data: { at: expect.any(Number) }, options: { relay: false, seq: false } }]);
		expect(frames(a.platform)).toEqual([{ topic: '__realtime', event: 'recovered', data: { at: expect.any(Number) }, options: { relay: false, seq: false } }]);
	});

	it('publishes nothing on __realtime and holds no health channel without a breaker', async () => {
		const a = await instance({ breaker: false });
		expect(a.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(false);
		await client.redis.spublish(HEALTH_CHANNEL, JSON.stringify({ instanceId: 'peer', topic: '__realtime', event: 'recovered', data: { at: 1, recovery: { refetch: true } } }));
		await hold();
		expect(a.platform.published).toEqual([]);
	});

	it('asks the other instances to refetch after relays were dropped while the breaker was open', async () => {
		const a = await instance();
		const b = await instance();
		expect(b.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(true);
		a.breaker.failure();
		a.wrapped.publish('chat', 'msg', 1);
		await tick();
		a.breaker.reset();
		await tick();
		expect(frames(b.platform)).toHaveLength(0);
		await hold();
		expect(refetches(b.platform)).toHaveLength(1);
		expect(refetches(b.platform)[0]).toEqual({ topic: '__realtime', event: 'recovered', data: { at: expect.any(Number), recovery: { refetch: true } }, options: { relay: false, seq: false } });
		// The sender's subscriber missed nothing.
		expect(refetches(a.platform)).toHaveLength(0);
	});

	it('announces a relay that failed under the threshold after the next successful relay', async () => {
		const a = await instance({ breaker: { failureThreshold: 5 } });
		const b = await instance();
		const spublish = client.redis.spublish;
		client.redis.spublish = async () => { throw new Error('connection lost'); };
		a.wrapped.publish('chat', 'msg', 1);
		await tick();
		client.redis.spublish = spublish;
		await hold();
		expect(frames(b.platform)).toHaveLength(0);
		a.wrapped.publish('chat', 'msg', 2);
		await tick();
		await hold();
		expect(a.breaker.state).toBe('healthy');
		expect(refetches(b.platform)).toHaveLength(1);
	});

	it('announces relays shed by a full relay queue', async () => {
		const a = await instance({ breaker: { failureThreshold: 5 } });
		const b = await instance();
		// Nine envelopes of about 1 MB exceed the queue's 8 MiB.
		const payload = 'x'.repeat(1_000_000);
		for (let i = 0; i < 9; i++) a.wrapped.publish('chat', 'msg', payload);
		for (let i = 0; i < 12; i++) await tick();
		await hold();
		expect(b.platform.published.filter((f) => f.topic === 'chat')).toHaveLength(8);
		expect(refetches(b.platform)).toHaveLength(1);
	});

	it('gives each client one refetch frame when every peer lost relays', async () => {
		const all = [];
		for (let i = 0; i < 5; i++) all.push(await instance());
		for (const inst of all) inst.breaker.failure();
		for (const inst of all) inst.wrapped.publish('chat', 'msg', 1);
		await tick();
		for (const inst of all) inst.breaker.reset();
		for (let i = 0; i < 4; i++) await tick();
		await hold();
		for (const inst of all) expect(refetches(inst.platform)).toHaveLength(1);
	});

	it('asks its own clients to refetch once a reconnected subscriber restored its channels', async () => {
		const a = await instance();
		a.subscriber()._listeners.get(SUBSCRIBER_RESTORING)();
		await hold();
		expect(frames(a.platform)).toHaveLength(0);
		a.subscriber()._listeners.get(SUBSCRIBER_RESTORED)();
		await hold();
		expect(refetches(a.platform)).toHaveLength(1);
	});

	it('leaves an envelope on __realtime without a refetch request alone', async () => {
		const a = await instance();
		await client.redis.spublish(HEALTH_CHANNEL, JSON.stringify({ instanceId: 'peer', topic: '__realtime', event: 'recovered', data: { custom: 'x' } }));
		await hold();
		expect(frames(a.platform)).toHaveLength(0);
	});

	it('reports a refused health channel once and subscribes it on a retry', async () => {
		const sink = captureRegistered();
		cleanups.push(() => sink.stop());
		let refusals = 2;
		const source = mockRedisClient();
		const duplicate = source.duplicate.bind(source);
		source.duplicate = (o) => {
			const dup = duplicate(o);
			const ssubscribe = dup.ssubscribe.bind(dup);
			dup.ssubscribe = async (...channels) => {
				if (channels.includes(HEALTH_CHANNEL) && refusals > 0) { refusals--; throw new Error('NOPERM'); }
				return ssubscribe(...channels);
			};
			return dup;
		};
		const a = await instance({ client: source });
		expect(a.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(false);
		await vi.advanceTimersByTimeAsync(1000);
		expect(a.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(false);
		await vi.advanceTimersByTimeAsync(2000);
		expect(a.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(true);
		expect(sink.ids().filter((id) => id === EXTENSIONS_ERROR_IDS.SHARDED_HEALTH_CHANNEL)).toHaveLength(1);
		expect(a.breaker.state).toBe('healthy');
	});

	it('keeps the health channel when a topic on the same shard channel is unfollowed', async () => {
		const a = await instance();
		await a.bus.follow('__realtime');
		await a.bus.unfollow('__realtime');
		expect(a.subscriber()._shardedChannels.has(HEALTH_CHANNEL)).toBe(true);
	});

	it('stops listening to the breaker on deactivate', async () => {
		const a = await instance();
		await a.bus.deactivate();
		a.breaker.failure();
		expect(a.platform.published).toEqual([]);
	});
});
