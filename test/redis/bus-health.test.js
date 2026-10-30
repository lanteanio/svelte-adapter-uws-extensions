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
import { createMetrics } from '../../src/prometheus/index.js';

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

	it('uses the policy of the most recently attached bus that has one, and the earlier once that one leaves', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const p = mockPlatform();
		const first = createPubSubBus(client, { breaker, channel: 'one', degradationPolicy: policy('First') });
		const second = createPubSubBus(client, { breaker, channel: 'two', degradationPolicy: policy('Second') });
		await first.activate(p);
		await second.activate(p);
		cleanups.push(async () => { await first.deactivate(); breaker.destroy(); });
		breaker.failure();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation.bannerCopy)).toEqual(['Second unavailable.']);
		breaker.reset();
		await second.deactivate();
		breaker.failure();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation.bannerCopy)).toEqual(['Second unavailable.', 'First unavailable.']);
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
	it('publishes one refetch event for an outage that also cost the subscriber its channel', async () => {
		const p = mockPlatform();
		const subs = [];
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus({ ...client, duplicate: (o) => { const d = client.duplicate(o); subs.push(d); return d; } }, { breaker });
		await bus.activate(p);
		await tick();
		cleanups.push(async () => { await bus.deactivate(); breaker.destroy(); });
		p.reset();
		breaker.failure();
		subs.at(-1)._listeners.get(SUBSCRIBER_RESTORING)();
		subs.at(-1)._listeners.get(SUBSCRIBER_RESTORED)();
		breaker.reset();
		await vi.advanceTimersByTimeAsync(100);
		await client.redis.publish('uws:pubsub', request('peer-1'));
		await vi.advanceTimersByTimeAsync(200);
		await client.redis.publish('uws:pubsub', request('peer-2'));
		await vi.advanceTimersByTimeAsync(HINT_MAX_HOLD_MS * 2);
		expect(frames(p, 'recovered')).toHaveLength(1);
		expect(refetches(p)).toHaveLength(1);
	});

	it('recovers a breaker no traffic passes through by probing its bus connection', async () => {
		const first = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const idle = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const { p } = await both([first, idle]);
		first.failure();
		idle.failure();
		await vi.advanceTimersByTimeAsync(200);
		first.reset();
		await vi.advanceTimersByTimeAsync(1000);
		expect(idle.state).toBe('healthy');
		expect(frames(p, 'degraded')).toHaveLength(1);
		expect(frames(p, 'recovered')).toHaveLength(1);
	});

	it('keeps a breaker open while the connection answers but may not publish, and stops probing once healthy', async () => {
		// A revoked channel grant: PING still answers, every publish is refused.
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const probes = [];
		let refuse = true;
		const { publish, spublish } = client.redis;
		const guarded = (send) => async function (channel, message) {
			if (JSON.parse(message).batch?.length === 0) probes.push(channel);
			if (refuse) throw new Error('NOPERM this user has no permissions to access the channel');
			return send.call(this, channel, message);
		};
		client.redis.publish = guarded(publish);
		client.redis.spublish = guarded(spublish);
		cleanups.push(() => { client.redis.publish = publish; client.redis.spublish = spublish; });
		const { p } = await both([breaker, breaker]);
		breaker.failure();
		await vi.advanceTimersByTimeAsync(350);
		expect(probes.length).toBe(3);
		expect(breaker.state).not.toBe('healthy');
		expect(frames(p, 'recovered')).toEqual([]);
		refuse = false;
		await vi.advanceTimersByTimeAsync(150);
		expect(breaker.state).toBe('healthy');
		expect(frames(p, 'recovered')).toHaveLength(1);
		const settled = probes.length;
		await vi.advanceTimersByTimeAsync(5000);
		expect(probes.length).toBe(settled);
		// Peers drop the empty batch: nothing reached the platform but the event.
		expect(p.published.filter((f) => f.topic !== '__realtime')).toEqual([]);
	});

	it('takes over the outage of a bus left behind by a reload, with the new policy', async () => {
		const p = mockPlatform();
		const staleBreaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const stale = createPubSubBus(client, { breaker: staleBreaker, channel: 'one', degradationPolicy: policy('Old') });
		await stale.activate(p);
		await tick();
		staleBreaker.failure();
		// The reload creates a new breaker and bus; the old one is never deactivated.
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus(client, { breaker, channel: 'two', degradationPolicy: policy('New') });
		await bus.activate(p);
		await tick();
		cleanups.push(async () => { await bus.deactivate(); await stale.deactivate(); breaker.destroy(); staleBreaker.destroy(); });
		await vi.advanceTimersByTimeAsync(500);
		expect(staleBreaker.state).toBe('healthy');
		expect(frames(p, 'recovered').map((f) => f.data.recovery.bannerCopy)).toEqual(['New back.']);
		p.reset();
		breaker.failure();
		breaker.reset();
		await hold();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation.bannerCopy)).toEqual(['New unavailable.']);
		expect(frames(p, 'recovered').map((f) => f.data.recovery.bannerCopy)).toEqual(['New back.']);
	});

	it('detaches a bus whose connection ended for good', async () => {
		const p = mockPlatform();
		const ended = mockRedisClient();
		const first = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const second = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 100 });
		const live = createPubSubBus(client, { breaker: first, channel: 'one' });
		const gone = createPubSubBus(ended, { breaker: second, channel: 'two' });
		await live.activate(p);
		await gone.activate(p);
		await tick();
		cleanups.push(async () => { await live.deactivate(); await gone.deactivate(); first.destroy(); second.destroy(); });
		p.reset();
		first.failure();
		second.failure();
		ended.redis.status = 'end';
		first.reset();
		await vi.advanceTimersByTimeAsync(500);
		expect(second.state).toBe('probing');
		expect(frames(p, 'recovered')).toHaveLength(1);
	});

	it('counts each published event once in a registry the buses share', async () => {
		const p = mockPlatform();
		const metrics = createMetrics();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const one = createPubSubBus(client, { breaker, metrics, channel: 'one' });
		const two = createPubSubBus(client, { breaker, metrics, channel: 'two' });
		await one.activate(p);
		await two.activate(p);
		cleanups.push(async () => { await one.deactivate(); await two.deactivate(); breaker.destroy(); });
		breaker.failure();
		breaker.reset();
		const text = String(await metrics.serialize());
		expect(text).toMatch(/^pubsub_degraded_total 1$/m);
		expect(text).toMatch(/^pubsub_recovered_total 1$/m);
	});

	it('joins the announcer of the platform a bus wrapped', async () => {
		const p = mockPlatform();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const sh = createShardedBus(client, { breaker });
		const ps = createPubSubBus(client, { breaker, degradationPolicy: policy('Live updates') });
		await sh.activate(p);
		await tick();
		await ps.activate(sh.wrap(p));
		await tick();
		cleanups.push(async () => { await ps.deactivate(); await sh.deactivate(); breaker.destroy(); });
		p.reset();
		breaker.failure();
		breaker.reset();
		await hold();
		expect(frames(p, 'degraded').map((f) => f.data.mitigation?.bannerCopy)).toEqual(['Live updates unavailable.']);
		expect(frames(p, 'recovered')).toHaveLength(1);
		// And the other way round.
		const q = mockPlatform();
		const other = createCircuitBreaker({ failureThreshold: 1 });
		const ps2 = createPubSubBus(client, { breaker: other, channel: 'other' });
		const sh2 = createShardedBus(client, { breaker: other });
		await ps2.activate(q);
		await sh2.activate(ps2.wrap(q));
		await tick();
		cleanups.push(async () => { await ps2.deactivate(); await sh2.deactivate(); other.destroy(); });
		q.reset();
		other.failure();
		other.reset();
		expect(frames(q, 'degraded')).toHaveLength(1);
		expect(frames(q, 'recovered')).toHaveLength(1);
	});

	it('publishes recovered for a breaker that was already open when its bus attached', async () => {
		const p = mockPlatform();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		breaker.failure();
		const bus = createPubSubBus(client, { breaker });
		const pending = bus.activate(p);
		cleanups.push(async () => { await bus.deactivate(); breaker.destroy(); });
		breaker.reset();
		await pending.catch(() => {});
		await tick();
		expect(frames(p, 'degraded')).toHaveLength(0);
		expect(frames(p, 'recovered')).toHaveLength(1);
	});
});
