import { describe, it, expect, afterEach } from 'vitest';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { createClockSkewSampler } from '../../src/redis/clock-skew.js';

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

describe('clock-skew fence', () => {
	// A client whose per-call skew is scripted: shift[i] is subtracted from the
	// local clock for read i, so each sample() sees the scripted skew.
	function scriptedClient(shifts) {
		let i = 0;
		return fakeClient(async () => {
			const shift = shifts[Math.min(i++, shifts.length - 1)];
			if (shift instanceof Error) throw shift;
			return timeReplyFor(LOCAL_MS - shift);
		});
	}

	it('fenced() is false and no fence gauge registers when the option is off', async () => {
		pinLocalClock(LOCAL_MS);
		const metrics = createMetrics();
		const sampler = createClockSkewSampler(
			scriptedClient([900]), { metrics, samples: 1, immediate: false }
		);
		await sampler.sample();
		await sampler.stop();
		expect(sampler.fenced()).toBe(false);
		expect(metrics.serialize()).not.toContain('platform_clock_fenced');
	});

	it('enters the fence only after consecutive trip-level samples', async () => {
		pinLocalClock(LOCAL_MS);
		const transitions = [];
		const metrics = createMetrics();
		const sampler = createClockSkewSampler(
			scriptedClient([900, 900]),
			{ metrics, samples: 1, immediate: false, fence: true, onFence: (f) => transitions.push(f) }
		);
		await sampler.sample();
		expect(sampler.fenced()).toBe(false); // one bad sample never sheds authority
		await sampler.sample();
		expect(sampler.fenced()).toBe(true);
		expect(transitions).toEqual([true]);
		expect(metrics.serialize()).toContain('platform_clock_fenced 1');
		await sampler.stop();
	});

	it('a single trip between calm samples never fences', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			scriptedClient([900, 10, 900, 10]),
			{ samples: 1, immediate: false, fence: true }
		);
		for (let i = 0; i < 4; i++) await sampler.sample();
		expect(sampler.fenced()).toBe(false);
		await sampler.stop();
	});

	it('releases only after consecutive below-warn samples; warn-level holds the fence', async () => {
		pinLocalClock(LOCAL_MS);
		const transitions = [];
		const sampler = createClockSkewSampler(
			// 2x trip -> fenced; then warn-level (holds); then 3x calm -> released.
			scriptedClient([900, 900, 200, 10, 10, 10]),
			{ samples: 1, immediate: false, fence: true, onFence: (f) => transitions.push(f) }
		);
		await sampler.sample();
		await sampler.sample();
		expect(sampler.fenced()).toBe(true);
		await sampler.sample(); // warn band: fence holds, calm streak resets
		expect(sampler.fenced()).toBe(true);
		await sampler.sample();
		await sampler.sample();
		expect(sampler.fenced()).toBe(true); // two calm samples are not enough
		await sampler.sample();
		expect(sampler.fenced()).toBe(false);
		expect(transitions).toEqual([true, false]);
		await sampler.stop();
	});

	it('a failed sample never changes fence state', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			scriptedClient([900, new Error('redis down'), 900]),
			{ samples: 1, immediate: false, fence: true }
		);
		await sampler.sample(); // trip 1
		await sampler.sample(); // error: not evidence either way
		expect(sampler.fenced()).toBe(false);
		await sampler.sample(); // trip 2: the error preserved the streak, it did not reset it
		expect(sampler.fenced()).toBe(true);
		await sampler.stop();
	});

	it('honors custom streak thresholds and validates them', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			scriptedClient([900]),
			{ samples: 1, immediate: false, fence: { tripSamples: 1 } }
		);
		await sampler.sample();
		expect(sampler.fenced()).toBe(true);
		await sampler.stop();

		const client = scriptedClient([0]);
		expect(() => createClockSkewSampler(client, { fence: 'yes' })).toThrow(/fence/);
		expect(() => createClockSkewSampler(client, { fence: { tripSamples: 0 } })).toThrow(/tripSamples/);
		expect(() => createClockSkewSampler(client, { onFence: 42 })).toThrow(/onFence/);
	});
});

describe('attachClockFence', () => {
	it('attaches the fence surface as platform.clockFence', async () => {
		pinLocalClock(LOCAL_MS);
		const sampler = createClockSkewSampler(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 900)),
			{ samples: 1, immediate: false, fence: { tripSamples: 1 } }
		);
		const { attachClockFence } = await import('../../src/redis/clock-skew.js');
		const platform = {};
		attachClockFence(platform, sampler);
		expect(platform.clockFence.fenced()).toBe(false);
		await sampler.sample();
		expect(platform.clockFence.fenced()).toBe(true);
		await sampler.stop();

		expect(() => attachClockFence(null, sampler)).toThrow(/platform/);
		expect(() => attachClockFence({}, {})).toThrow(/fenced/);
	});
});
