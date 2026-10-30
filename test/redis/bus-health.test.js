// The pub/sub and sharded buses share one health announcer per platform and
// topic: one degraded / recovered per transition of the breakers they were
// given together, one degradation policy, and one merged refetch event.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { createDegradationPolicy } from '../../src/shared/degradation.js';
import { SUBSCRIBER_RESTORING, SUBSCRIBER_RESTORED } from '../../src/shared/redis-subscriber.js';
import { HINT_HOLD_MS, HINT_MAX_HOLD_MS } from '../../src/shared/delivery-hints.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const request = (instanceId) => JSON.stringify({ instanceId, topic: '__realtime', event: 'recovered', data: { at: 1, recovery: { refetch: true } }, s: false });

describe('shared health announcer', () => {
	let client;
	const cleanups = [];
	beforeEach(() => {
		vi.useFakeTimers();
		// The waits are measured on the monotonic clock; run it on the fake one.
		setRuntimeEnv({ clock: { monotonic: () => Date.now() } });
		client = mockRedisClient();
	});
	afterEach(async () => {
		while (cleanups.length) await cleanups.pop()();
		resetRuntimeEnv();
		vi.useRealTimers();
	});
	const tick = () => vi.advanceTimersByTimeAsync(5);
	const hold = () => vi.advanceTimersByTimeAsync(HINT_HOLD_MS + 5);
	const frames = (p, event) => p.published.filter((f) => f.topic === '__realtime' && f.event === event);
	const refetches = (p) => frames(p, 'recovered').filter((f) => f.data.recovery?.refetch === true);
	const policy = (copy) => createDegradationPolicy({
		mitigation: { retryAfterMs: 4000, bannerCopy: copy + ' unavailable.' }, jitterMs: 0,
		recovery: { bannerCopy: copy + ' back.' }
	});

	/** Both buses on one platform, each subscriber connection reachable. */
	async function both(breakers, options = {}) {
		const p = mockPlatform();
		const subs = { pubsub: [], sharded: [] };
		const opened = (list) => ({ ...client, duplicate: (o) => { const d = client.duplicate(o); list.push(d); return d; } });
		const ps = createPubSubBus(opened(subs.pubsub), { breaker: breakers[0], degradationPolicy: options.policy });
		const sh = createShardedBus(opened(subs.sharded), { breaker: breakers[1] });
		await ps.activate(p);
		await sh.activate(p);
		await tick();
		p.reset();
		cleanups.push(async () => { await ps.deactivate(); await sh.deactivate(); for (const b of new Set(breakers)) b.destroy(); });
		return { p, ps, sh, subs };
	}

	it('publishes one degraded and one recovered, with the policy, for a breaker both buses share', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const { p } = await both([breaker, breaker], { policy: policy('Live updates') });
		breaker.failure();
		breaker.reset();
		await hold();
		expect(frames(p, 'degraded').map((f) => f.data)).toEqual([{ at: expect.any(Number), mitigation: { retryAfterMs: 4000, bannerCopy: 'Live updates unavailable.' } }]);
		expect(frames(p, 'recovered').map((f) => f.data)).toEqual([{ at: expect.any(Number), recovery: { bannerCopy: 'Live updates back.' } }]);
	});

	it('stays degraded until every breaker of the buses is healthy again', async () => {
		const first = createCircuitBreaker({ failureThreshold: 1 });
		const second = createCircuitBreaker({ failureThreshold: 1 });
		const { p } = await both([first, second], { policy: policy('Live updates') });
		first.failure();
		second.failure();
		expect(frames(p, 'degraded')).toHaveLength(1);
		expect(frames(p, 'degraded')[0].data.mitigation.bannerCopy).toBe('Live updates unavailable.');
		first.reset();
		await hold();
		expect(frames(p, 'recovered')).toHaveLength(0);
		second.reset();
		await hold();
		expect(frames(p, 'recovered')).toHaveLength(1);
		expect(frames(p, 'recovered')[0].data.recovery).toEqual({ bannerCopy: 'Live updates back.' });
		// The other order, from the sharded bus's breaker first.
		second.failure();
		first.failure();
		second.reset();
		await hold();
		expect(frames(p, 'degraded')).toHaveLength(2);
		expect(frames(p, 'recovered')).toHaveLength(1);
		first.reset();
		expect(frames(p, 'recovered')).toHaveLength(2);
	});

	it('merges refetch hints of both buses into one event', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const { p, subs } = await both([breaker, breaker]);
		for (const sub of [subs.pubsub.at(-1), subs.sharded.at(-1)]) {
			sub._listeners.get(SUBSCRIBER_RESTORING)();
			sub._listeners.get(SUBSCRIBER_RESTORED)();
		}
		await client.redis.publish('uws:pubsub', request('peer-1'));
		await client.redis.spublish('uws:sharded:__realtime', request('peer-2'));
		await hold();
		expect(refetches(p)).toHaveLength(1);
	});

	it('uses the policy of the first bus that has one, and the next once that one leaves', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const p = mockPlatform();
		const first = createPubSubBus(client, { breaker, channel: 'one', degradationPolicy: policy('First') });
		const second = createPubSubBus(client, { breaker, channel: 'two', degradationPolicy: policy('Second') });
		await first.activate(p);
		await second.activate(p);
		cleanups.push(async () => { await second.deactivate(); breaker.destroy(); });
		breaker.failure();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation.bannerCopy)).toEqual(['First unavailable.']);
		breaker.reset();
		await first.deactivate();
		breaker.failure();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation.bannerCopy)).toEqual(['First unavailable.', 'Second unavailable.']);
	});

	it('announces the recovery when the bus whose breaker was open leaves', async () => {
		const first = createCircuitBreaker({ failureThreshold: 1 });
		const second = createCircuitBreaker({ failureThreshold: 1 });
		const { p, sh } = await both([first, second]);
		second.failure();
		expect(frames(p, 'degraded')).toHaveLength(1);
		await sh.deactivate();
		expect(frames(p, 'recovered')).toHaveLength(1);
	});

	it('extends the wait while requests keep arriving', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const { p } = await both([breaker, breaker]);
		for (let i = 0; i < 6; i++) {
			await client.redis.publish('uws:pubsub', request('peer'));
			await vi.advanceTimersByTimeAsync(500);
		}
		// The last request arrived 500 ms ago.
		expect(refetches(p)).toHaveLength(0);
		await vi.advanceTimersByTimeAsync(HINT_HOLD_MS - 500 + 5);
		expect(refetches(p)).toHaveLength(1);
	});

	it('caps the wait under requests that never stop, and spaces the events', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const { p } = await both([breaker, breaker]);
		const seen = [];
		for (let t = 0; t < 25000; t += 500) {
			await client.redis.publish('uws:pubsub', request('peer'));
			await vi.advanceTimersByTimeAsync(500);
			seen.push(refetches(p).length);
		}
		// One event when the first wait reaches its cap, the next one cap later.
		expect(seen.indexOf(1)).toBe(HINT_MAX_HOLD_MS / 500 - 1);
		expect(seen.indexOf(2)).toBe(2 * HINT_MAX_HOLD_MS / 500 - 1);
		expect(refetches(p)).toHaveLength(2);
	});

	it('bounds the refetch events a peer causes while it keeps shedding relays', async () => {
		const make = async () => {
			const p = mockPlatform();
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const bus = createPubSubBus(client, { breaker });
			await bus.activate(p);
			p.reset();
			cleanups.push(async () => { await bus.deactivate(); breaker.destroy(); });
			return { p, wrapped: bus.wrap(p) };
		};
		const a = await make();
		const b = await make();
		const payload = 'x'.repeat(1_000_000);
		for (let s = 0; s < 10; s++) {
			for (let i = 0; i < 9; i++) a.wrapped.publish('chat', 'msg', payload);
			for (let i = 0; i < 20; i++) await tick();
			await vi.advanceTimersByTimeAsync(900);
		}
		await vi.advanceTimersByTimeAsync(2000);
		expect(refetches(b.p)).toHaveLength(2);
	});

	it('publishes nothing and leaves no timer behind once the last bus leaves during a wait', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const { p, ps, sh } = await both([breaker, breaker]);
		await client.redis.publish('uws:pubsub', request('peer'));
		await ps.deactivate();
		await sh.deactivate();
		expect(vi.getTimerCount()).toBe(0);
		await hold();
		expect(p.published).toEqual([]);
	});
});
