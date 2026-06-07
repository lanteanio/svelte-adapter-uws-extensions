import { describe, it, expect, afterEach } from 'vitest';
import { setRuntimeEnv, resetRuntimeEnv } from '../../shared/runtime.js';
import { createMetrics } from '../../prometheus/index.js';
import { createClockSkewSampler } from '../../redis/clock-skew.js';

// A minimal RedisClient shape: only `.redis.time()` is exercised by the sampler.
function fakeClient(timeFn) {
	return { redis: { time: timeFn }, key: (k) => k };
}

// Build a Redis TIME reply [seconds, microseconds] (strings, as ioredis returns)
// for a given epoch-millisecond value.
function timeReplyFor(epochMs) {
	const sec = Math.floor(epochMs / 1000);
	const usec = (epochMs % 1000) * 1000;
	return [String(sec), String(usec)];
}

// Pin the local wall clock to a fixed epoch through the runtime seam, so skew is
// purely (localMs - redisMs). wallEpoch (not the ~1 Hz cached now) is what the
// sampler reads, and the seam returns it uncached.
function pinLocalClock(localMs) {
	setRuntimeEnv({ clock: { wallEpoch: () => localMs } }, { force: true });
}

const LOCAL_MS = 1_700_000_000_000;

afterEach(() => resetRuntimeEnv());

describe('clock-skew sampler', () => {
	it('reports positive skew when the local clock is ahead of Redis', async () => {
		pinLocalClock(LOCAL_MS);
		const metrics = createMetrics();
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 250)),
			{ metrics, samples: 3, immediate: false }
		);

		const skew = await sampler.sample();
		await sampler.stop();

		expect(skew).toBe(250);
		expect(sampler.current()).toBe(250);
		const out = metrics.serialize();
		expect(out).toContain('# TYPE platform_clock_skew_ms gauge');
		expect(out).toContain('platform_clock_skew_ms 250');
	});

	it('reports negative skew when the local clock is behind Redis', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS + 300)),
			{ samples: 1, immediate: false }
		);

		const skew = await sampler.sample();
		await sampler.stop();

		expect(skew).toBe(-300);
	});

	it('takes the median across reads, rejecting a single jittery sample', async () => {
		pinLocalClock(LOCAL_MS);
		// Three reads in one sample: two land at 250ms skew, one is a wild
		// outlier. The median must pick the stable 250.
		const replies = [
			timeReplyFor(LOCAL_MS - 250),
			timeReplyFor(LOCAL_MS - 100_250), // outlier: skew 100250
			timeReplyFor(LOCAL_MS - 250)
		];
		let i = 0;
		const sampler = createClockSkewSampler(
			fakeClient(async () => replies[i++]),
			{ samples: 3, immediate: false }
		);

		const skew = await sampler.sample();
		await sampler.stop();

		expect(skew).toBe(250);
	});

	it('fires onWarn (not onTrip) when |skew| crosses the warn threshold', async () => {
		pinLocalClock(LOCAL_MS);
		const warned = [];
		const tripped = [];
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false, warnMs: 100, tripMs: 500, onWarn: (s) => warned.push(s), onTrip: (s) => tripped.push(s) }
		);

		await sampler.sample();
		await sampler.stop();

		expect(warned).toEqual([300]);
		expect(tripped).toEqual([]);
	});

	it('fires onTrip when |skew| crosses the trip threshold', async () => {
		pinLocalClock(LOCAL_MS);
		const warned = [];
		const tripped = [];
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS + 600)),
			{ samples: 1, immediate: false, warnMs: 100, tripMs: 500, onWarn: (s) => warned.push(s), onTrip: (s) => tripped.push(s) }
		);

		await sampler.sample();
		await sampler.stop();

		expect(tripped).toEqual([-600]);
		expect(warned).toEqual([]);
	});

	it('reports null and calls onError on a Redis failure, retaining no value', async () => {
		pinLocalClock(LOCAL_MS);
		const errors = [];
		const sampler = createClockSkewSampler(
			fakeClient(async () => { throw new Error('redis down'); }),
			{ samples: 2, immediate: false, onError: (e) => errors.push(e) }
		);

		const skew = await sampler.sample();
		await sampler.stop();

		expect(skew).toBeNull();
		expect(sampler.current()).toBeNull();
		expect(errors).toHaveLength(1);
		expect(errors[0].message).toBe('redis down');
	});

	it('current() is null before the first sample', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS)),
			{ immediate: false }
		);
		expect(sampler.current()).toBeNull();
		await sampler.stop();
	});

	it('stop() is idempotent', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS)),
			{ immediate: false }
		);
		await sampler.stop();
		await expect(sampler.stop()).resolves.toBeUndefined();
	});

	it('samples immediately by default and populates the gauge', async () => {
		pinLocalClock(LOCAL_MS);
		const metrics = createMetrics();
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 40)),
			{ metrics, samples: 1 }
		);

		// The immediate sample is in-flight; await the sampler's own chain by
		// stopping (stop awaits the in-flight sample).
		await sampler.stop();

		expect(sampler.current()).toBe(40);
		expect(metrics.serialize()).toContain('platform_clock_skew_ms 40');
	});

	it('requires a client with a redis connection', () => {
		expect(() => createClockSkewSampler(undefined)).toThrow(/client/);
		expect(() => createClockSkewSampler({})).toThrow(/client/);
	});

	it('validates numeric options', () => {
		const client = fakeClient(async () => timeReplyFor(LOCAL_MS));
		expect(() => createClockSkewSampler(client, { intervalMs: 0 })).toThrow(/intervalMs/);
		expect(() => createClockSkewSampler(client, { samples: 0 })).toThrow(/samples/);
		expect(() => createClockSkewSampler(client, { samples: 1.5 })).toThrow(/samples/);
		expect(() => createClockSkewSampler(client, { warnMs: -1 })).toThrow(/warnMs|tripMs/);
		expect(() => createClockSkewSampler(client, { onSkew: 42 })).toThrow(/onSkew/);
	});
});
