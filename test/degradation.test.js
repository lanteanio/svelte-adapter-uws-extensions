import { describe, it, expect } from 'vitest';
import { createDegradationPolicy } from '../src/shared/degradation.js';
import { createCircuitBreaker } from '../src/shared/breaker.js';
import { createPubSubBus } from '../src/redis/pubsub.js';
import { mockRedisClient } from './helpers/mock-redis.js';
import { mockPlatform } from './helpers/mock-platform.js';

describe('createDegradationPolicy', () => {
	const degrade = { from: 'healthy', to: 'broken' };

	it('returns a static mitigation with the default jitter derived from retryAfterMs', () => {
		const p = createDegradationPolicy({ mitigation: { retryAfterMs: 8000, bannerCopy: 'Read-only mode' } });
		const out = p.onDegraded(degrade);
		expect(out.mitigation).toEqual({ retryAfterMs: 8000, bannerCopy: 'Read-only mode' });
		expect(out.jitterMs).toBe(2000); // min(8000/4, 5000)
	});

	it('caps the default jitter at 5000ms', () => {
		const p = createDegradationPolicy({ mitigation: { retryAfterMs: 60000 } });
		expect(p.onDegraded(degrade).jitterMs).toBe(5000);
	});

	it('an explicit jitterMs overrides the derived default', () => {
		const p = createDegradationPolicy({ mitigation: { retryAfterMs: 8000 }, jitterMs: 250 });
		expect(p.onDegraded(degrade).jitterMs).toBe(250);
	});

	it('no retryAfterMs and no explicit jitter -> 0 (immediate)', () => {
		const p = createDegradationPolicy({ mitigation: { bannerCopy: 'down' } });
		expect(p.onDegraded(degrade).jitterMs).toBe(0);
	});

	it('the function form receives the transition and can return null', () => {
		const p = createDegradationPolicy({ mitigation: (t) => (t.to === 'broken' ? { bannerCopy: 'down' } : null) });
		expect(p.onDegraded(degrade).mitigation).toEqual({ bannerCopy: 'down' });
		expect(p.onDegraded({ from: 'healthy', to: 'probing' })).toBeNull();
	});

	it('no mitigation configured -> onDegraded returns null', () => {
		expect(createDegradationPolicy({}).onDegraded(degrade)).toBeNull();
	});

	it('resolves a recovery hint + its own jitter window', () => {
		const p = createDegradationPolicy({ recovery: { refetch: true, bannerCopy: 'Back online' }, recoveryJitterMs: 3000 });
		const out = p.onRecovered({ from: 'broken', to: 'healthy' });
		expect(out.recovery).toEqual({ refetch: true, bannerCopy: 'Back online' });
		expect(out.jitterMs).toBe(3000);
	});

	it('validates envelope fields and jitter ranges', () => {
		expect(() => createDegradationPolicy({ mitigation: { retryAfterMs: -1 } })).toThrow(/retryAfterMs/);
		expect(() => createDegradationPolicy({ mitigation: { streams: 'x' } })).toThrow(/streams/);
		expect(() => createDegradationPolicy({ mitigation: { bannerCopy: 5 } })).toThrow(/bannerCopy/);
		expect(() => createDegradationPolicy({ jitterMs: 99999 })).toThrow(/jitterMs/);
		expect(() => createDegradationPolicy(null)).toThrow(/spec/);
	});
});

describe('createPubSubBus degradationPolicy integration', () => {
	function wire(opts) {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const breaker = createCircuitBreaker({ failureThreshold: 1 });
		const bus = createPubSubBus(client, { breaker, ...opts });
		bus.activate(platform);
		return { platform, breaker };
	}
	const find = (platform, event) => platform.published.find((p) => p.topic === '__realtime' && p.event === event);

	it('ships the mitigation + de-herd jitter on the degraded event', () => {
		const policy = createDegradationPolicy({ mitigation: { retryAfterMs: 4000, bannerCopy: 'Read-only', rpcs: ['orders/create'] } });
		const { platform, breaker } = wire({ degradationPolicy: policy });
		breaker.failure(new Error('down')); // healthy -> broken
		const deg = find(platform, 'degraded');
		expect(deg).toBeDefined();
		expect(deg.data.mitigation).toEqual({ retryAfterMs: 4000, bannerCopy: 'Read-only', rpcs: ['orders/create'] });
		expect(deg.options).toMatchObject({ jitterMs: 1000 }); // min(4000/4, 5000)
	});

	it('without a policy the degraded event is { at } only (unchanged)', () => {
		const { platform, breaker } = wire({});
		breaker.failure(new Error('down'));
		const deg = find(platform, 'degraded');
		expect(deg).toBeDefined();
		expect('mitigation' in deg.data).toBe(false);
		expect(deg.options).toBeUndefined();
	});

	it('ships recovery hints + jitter on the recovered event', () => {
		const policy = createDegradationPolicy({ recovery: { refetch: true, bannerCopy: 'Back online' }, recoveryJitterMs: 3000 });
		const { platform, breaker } = wire({ degradationPolicy: policy });
		breaker.failure(new Error('down')); // -> broken
		breaker.reset(); // -> healthy
		const rec = find(platform, 'recovered');
		expect(rec).toBeDefined();
		expect(rec.data.recovery).toEqual({ refetch: true, bannerCopy: 'Back online' });
		expect(rec.options).toMatchObject({ jitterMs: 3000 });
	});

	it('rejects a malformed degradationPolicy', () => {
		const client = mockRedisClient();
		expect(() => createPubSubBus(client, { degradationPolicy: { onDegraded: () => {} } })).toThrow(/degradationPolicy/);
	});
});
