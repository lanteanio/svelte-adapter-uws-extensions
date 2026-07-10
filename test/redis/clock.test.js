import { describe, it, expect, afterEach } from 'vitest';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { createClusterClock, attachClusterClock } from '../../src/redis/clock.js';

// A minimal RedisClient shape: the clock exercises `.redis.time()` plus, in
// leader mode, `.redis.set()` / `.redis.get()`.
function fakeClient(timeFn, extra = {}) {
	return { redis: { time: timeFn, ...extra }, key: (k) => k };
}

// Build a Redis TIME reply [seconds, microseconds] (strings, as ioredis returns)
// for a given epoch-millisecond value.
function timeReplyFor(epochMs) {
	const sec = Math.floor(epochMs / 1000);
	const usec = (epochMs % 1000) * 1000;
	return [String(sec), String(usec)];
}

// Pin the local wall clock through the runtime seam, so every source skew is
// exactly (localMs - sourceMs). Returns a repin function for clamp tests.
function pinLocalClock(localMs) {
	setRuntimeEnv({ clock: { wallEpoch: () => localMs } }, { force: true });
}

// An NTP source pinned to a fixed epoch (or throwing when handed an Error).
function fakeNtp(valueOrError) {
	return {
		read: async () => {
			if (valueOrError instanceof Error) throw valueOrError;
			return valueOrError;
		}
	};
}

const LOCAL_MS = 1_700_000_000_000;

afterEach(() => resetRuntimeEnv());

describe('cluster clock - validation', () => {
	it('requires a client', () => {
		expect(() => createClusterClock(null)).toThrow('client');
		expect(() => createClusterClock({})).toThrow('client');
	});
	it('validates numeric options', () => {
		const c = fakeClient(async () => timeReplyFor(LOCAL_MS));
		expect(() => createClusterClock(c, { intervalMs: 0 })).toThrow('intervalMs');
		expect(() => createClusterClock(c, { samples: 0 })).toThrow('samples');
		expect(() => createClusterClock(c, { warnMs: -1 })).toThrow('warnMs');
		expect(() => createClusterClock(c, { driftThresholdMs: -1 })).toThrow('driftThresholdMs');
		expect(() => createClusterClock(c, { leaderTtlMs: 0 })).toThrow('leaderTtlMs');
	});
	it('validates the ntp and leader shapes', () => {
		const c = fakeClient(async () => timeReplyFor(LOCAL_MS));
		expect(() => createClusterClock(c, { ntp: {} })).toThrow('ntp');
		expect(() => createClusterClock(c, { leader: {} })).toThrow('isLeader');
		expect(() => createClusterClock(c, { leaderKey: '' })).toThrow('leaderKey');
	});
});

describe('cluster clock - source fusion (now)', () => {
	it('with local+redis the median of two is their midpoint', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false }
		);
		await clock.sample();
		await clock.stop();
		// sources: local = L, redis-corrected = L - 300 -> median = L - 150
		expect(clock.now()).toBe(LOCAL_MS - 150);
		expect(clock.skew()).toBe(300);
	});

	it('with a third source the median outvotes a lone wrong local clock', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false, ntp: fakeNtp(LOCAL_MS - 300) }
		);
		await clock.sample();
		await clock.stop();
		// sources: local = L, redis = L-300, ntp = L-300 -> median = L-300
		expect(clock.now()).toBe(LOCAL_MS - 300);
	});

	it('before the first sample now() falls back to the seam wall clock', () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS)),
			{ immediate: false }
		);
		expect(clock.ready()).toBe(false);
		expect(clock.now()).toBe(LOCAL_MS);
		expect(clock.skew()).toBeNull();
		clock.stop();
	});
});

describe('cluster clock - consistent()', () => {
	it('anchors on the Redis reference (local wall minus skew)', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false }
		);
		await clock.sample();
		await clock.stop();
		expect(clock.consistent()).toBe(LOCAL_MS - 300);
		expect(clock.ready()).toBe(true);
	});

	it('never regresses: a skew step backward holds the last returned value', async () => {
		pinLocalClock(LOCAL_MS);
		let redisAt = LOCAL_MS + 300; // redis ahead: consistent = L + 300
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(redisAt)),
			{ samples: 1, immediate: false }
		);
		await clock.sample();
		expect(clock.consistent()).toBe(LOCAL_MS + 300);

		// The reference jumps back (e.g. a different cluster node answers):
		// the raw reading would regress by 800ms; the clamp holds.
		redisAt = LOCAL_MS - 500;
		await clock.sample();
		await clock.stop();
		expect(clock.consistent()).toBe(LOCAL_MS + 300);
	});
});

describe('cluster clock - thresholds and drift', () => {
	it('fires onWarn in the warn band and onTrip past the trip threshold', async () => {
		pinLocalClock(LOCAL_MS);
		const events = [];
		let redisAt = LOCAL_MS - 250; // |skew| 250: warn band (100..500)
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(redisAt)),
			{
				samples: 1, immediate: false,
				onWarn: (ms) => events.push(['warn', ms]),
				onTrip: (ms) => events.push(['trip', ms])
			}
		);
		await clock.sample();
		expect(clock.tripped()).toBe(false);

		redisAt = LOCAL_MS - 600; // |skew| 600: tripped
		await clock.sample();
		await clock.stop();
		expect(events).toEqual([['warn', 250], ['trip', 600]]);
		expect(clock.tripped()).toBe(true);
	});

	it('reports max pairwise source disagreement as drift and fires onDrift', async () => {
		pinLocalClock(LOCAL_MS);
		const drifts = [];
		const metrics = createMetrics();
		// redis agrees with local (skew 0), ntp is 400ms behind local:
		// pairwise = {|0|, |400|, |0-400|} -> drift 400.
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS)),
			{ samples: 1, immediate: false, ntp: fakeNtp(LOCAL_MS - 400), metrics, onDrift: (d) => drifts.push(d) }
		);
		await clock.sample();
		await clock.stop();
		expect(clock.drift()).toBe(400);
		expect(drifts).toEqual([{ driftMs: 400, sources: { redis: 0, ntp: 400 } }]);
		expect(metrics.serialize()).toContain('platform_clock_drift_ms 400');
	});

	it('a failed NTP read drops the source from the median and reports the error', async () => {
		pinLocalClock(LOCAL_MS);
		const errors = [];
		let ntpValue = LOCAL_MS - 300;
		const ntp = { read: async () => { if (ntpValue instanceof Error) throw ntpValue; return ntpValue; } };
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false, ntp, onError: (e) => errors.push(e.message) }
		);
		await clock.sample();
		expect(clock.now()).toBe(LOCAL_MS - 300); // 3-source median

		ntpValue = new Error('ntp down');
		await clock.sample();
		await clock.stop();
		// Back to the 2-source midpoint - the dead source no longer votes.
		expect(clock.now()).toBe(LOCAL_MS - 150);
		expect(errors).toEqual(['ntp down']);
	});

	it('a failed Redis round keeps the last good state and reports the error', async () => {
		pinLocalClock(LOCAL_MS);
		const errors = [];
		let fail = false;
		const clock = createClusterClock(
			fakeClient(async () => {
				if (fail) throw new Error('redis down');
				return timeReplyFor(LOCAL_MS - 300);
			}),
			{ samples: 1, immediate: false, onError: (e) => errors.push(e.message) }
		);
		await clock.sample();
		fail = true;
		const second = await clock.sample();
		await clock.stop();
		expect(second).toBeNull();
		expect(clock.skew()).toBe(300);
		expect(errors).toEqual(['redis down']);
	});
});

describe('cluster clock - leader stamping', () => {
	it('the leader stamps from its own fused clock and publishes its offset', async () => {
		pinLocalClock(LOCAL_MS);
		const sets = [];
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300), {
				set: async (...args) => { sets.push(args); return 'OK'; }
			}),
			{ samples: 1, immediate: false, leader: { isLeader: () => true }, leaderTtlMs: 90_000 }
		);
		await clock.sample();
		await clock.stop();

		// stamp = own fused clock (median of local, redis-corrected).
		expect(clock.stamp()).toBe(LOCAL_MS - 150);
		// Published offset o = fused - redisEstimate = (L-150) - (L-300) = 150,
		// keyed under the leader key with a PX freshness bound.
		expect(sets).toHaveLength(1);
		const [key, payload, px, ttl] = sets[0];
		expect(key).toBe('clock:leader-offset');
		expect(JSON.parse(payload)).toEqual({ o: 150 });
		expect(px).toBe('PX');
		expect(ttl).toBe(90_000);
	});

	it('a follower reproduces the leader clock through the shared reference', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300), {
				get: async () => JSON.stringify({ o: 500 })
			}),
			{ samples: 1, immediate: false, leader: { isLeader: () => false } }
		);
		await clock.sample();
		await clock.stop();

		// stamp = own redis-corrected reading + leader offset = (L-300) + 500.
		expect(clock.stamp()).toBe(LOCAL_MS + 200);
	});

	it('a follower with a stale leader cache falls back to consistent()', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300), {
				get: async () => JSON.stringify({ o: 500 })
			}),
			{ samples: 1, immediate: false, leader: { isLeader: () => false }, leaderTtlMs: 1_000 }
		);
		await clock.sample();
		await clock.stop();
		expect(clock.stamp()).toBe(LOCAL_MS + 200); // fresh cache

		// The cached offset outlives its freshness bound: fall back.
		pinLocalClock(LOCAL_MS + 5_000);
		expect(clock.stamp()).toBe(LOCAL_MS + 5_000 - 300);
	});

	it('without a leader option stamp() equals consistent()', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false }
		);
		await clock.sample();
		await clock.stop();
		expect(clock.stamp()).toBe(clock.consistent());
	});

	it('a leader read/write failure is reported but never fails the round', async () => {
		pinLocalClock(LOCAL_MS);
		const errors = [];
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300), {
				set: async () => { throw new Error('set failed'); }
			}),
			{ samples: 1, immediate: false, leader: { isLeader: () => true }, onError: (e) => errors.push(e.message) }
		);
		const skew = await clock.sample();
		await clock.stop();
		expect(skew).toBe(300);
		expect(errors).toEqual(['set failed']);
	});
});

describe('cluster clock - platform attachment', () => {
	it('attachClusterClock exposes the clusterClock convention', async () => {
		pinLocalClock(LOCAL_MS);
		const clock = createClusterClock(
			fakeClient(async () => timeReplyFor(LOCAL_MS - 300)),
			{ samples: 1, immediate: false }
		);
		await clock.sample();
		const platform = {};
		attachClusterClock(platform, clock);
		expect(platform.clusterClock.now()).toBe(LOCAL_MS - 150);
		expect(platform.clusterClock.consistent()).toBe(LOCAL_MS - 300);
		expect(platform.clusterClock.stamp()).toBe(LOCAL_MS - 300);
		expect(platform.clusterClock.ready()).toBe(true);
		expect(platform.clusterClock.tripped()).toBe(false);
		await clock.stop();
	});

	it('validates its arguments', () => {
		expect(() => attachClusterClock(null, { now: () => 1, stamp: () => 1 })).toThrow('platform');
		expect(() => attachClusterClock({}, {})).toThrow('clock');
	});
});

describe('cluster clock - lifecycle', () => {
	it('immediate: true takes the startup sample; stop() is idempotent', async () => {
		pinLocalClock(LOCAL_MS);
		let calls = 0;
		const clock = createClusterClock(
			fakeClient(async () => { calls++; return timeReplyFor(LOCAL_MS - 250); }),
			{ samples: 1 }
		);
		await clock.stop();
		await clock.stop();
		expect(calls).toBe(1);
		expect(clock.ready()).toBe(true);
		expect(clock.skew()).toBe(250);
	});
});
