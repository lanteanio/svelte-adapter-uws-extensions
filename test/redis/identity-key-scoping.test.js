import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createIdempotencyStore } from '../../src/redis/idempotency.js';
import { createClusterClock } from '../../src/redis/clock.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

describe('ratelimit clear scoping', () => {
	it('refuses a tenant id that would widen the SCAN glob', async () => {
		const rl = createRateLimit(mockRedisClient('rl:'), { points: 1, interval: 60, keyBy: (ws) => ws.getUserData().k });
		// clear('*') is the whole finding: a nominally tenant-scoped call
		// that wipes every tenant's buckets.
		for (const bad of ['*', 'a?b', 'a[b]', 'a\\b', 'a\0b']) {
			await expect(rl.clear(bad)).rejects.toThrow(/glob|NUL/);
		}
	});

	it('refuses a non-string tenant id with an explanation, not a TypeError', async () => {
		const rl = createRateLimit(mockRedisClient('rl0:'), { points: 1, interval: 60, keyBy: (ws) => ws.getUserData().k });
		// A number does not work anywhere else in the module (bucketKey calls
		// tenantId.indexOf), so accepting it here only bought a confusing
		// failure later. It must not read as a bare TypeError either.
		await expect(rl.clear(42)).rejects.toThrow(/must be a string/);
	});

	it('refuses clear(0) rather than silently clearing every tenant', async () => {
		const client = mockRedisClient('rl0b:');
		const opts = { points: 1, interval: 60, keyBy: (ws) => ws.getUserData().k };
		const rlA = createRateLimit(client, { ...opts, tenant: () => 'tenant-a' });
		const ws = mockWs({ k: 'u1' });
		await rlA.consume(ws);
		expect((await rlA.peek(ws)).allowed).toBe(false);

		// `0` is falsy, so the untenanted branch would build the global `*`
		// glob - the same blast radius as the clear('*') the guard rejects,
		// reached through a call that reads as tenant-scoped.
		await expect(rlA.clear(0)).rejects.toThrow(/must be a string/);
		expect((await rlA.peek(ws)).allowed).toBe(false);
	});

	it('leaves another tenant\'s buckets alone', async () => {
		const client = mockRedisClient('rl2:');
		const opts = { points: 1, interval: 60, keyBy: (ws) => ws.getUserData().k };
		const rlA = createRateLimit(client, { ...opts, tenant: () => 'tenant-a' });
		const rlB = createRateLimit(client, { ...opts, tenant: () => 'tenant-b' });
		const wsA = mockWs({ k: 'u1' });
		const wsB = mockWs({ k: 'u9' });
		await rlA.consume(wsA);
		await rlB.consume(wsB);
		expect((await rlA.peek(wsA)).allowed).toBe(false);
		expect((await rlB.peek(wsB)).allowed).toBe(false);

		await rlA.clear('tenant-a');
		expect((await rlA.peek(wsA)).allowed).toBe(true);
		expect((await rlB.peek(wsB)).allowed).toBe(false);
	});
});

describe('idempotency identity keys', () => {
	it('rejects NUL in either segment of the composite index key', async () => {
		const idem = createIdempotencyStore(mockRedisClient('idem:'), {});
		await expect(idem.acquire('k1', 60, { user: 'c', tenant: 'a\0b' })).rejects.toThrow('NUL');
		await expect(idem.acquire('k1', 60, { user: 'b\0c', tenant: 'a' })).rejects.toThrow('NUL');
		await expect(idem.purgeUser('a', 'b\0c')).rejects.toThrow('NUL');
		await expect(idem.purgeUser('a\0b', 'c')).rejects.toThrow('NUL');
	});

	it('stops the colliding purge from deleting the victim\'s committed result', async () => {
		// The exact collision from the finding: ('a\0b','c') and
		// ('a','b\0c') produce the same NUL-delimited index key, so the
		// attacker's own erasure would delete the victim's committed result
		// and their retried charge would silently re-execute.
		const idem = createIdempotencyStore(mockRedisClient('idem2:'), {});
		const victim = await idem.acquire('charge-1', 60, { user: 'c', tenant: 'a\0b' }).catch((e) => e);
		expect(victim).toBeInstanceOf(Error);
		expect(String(victim.message)).toContain('NUL');

		// With the identity refused up front, the victim's own valid identity
		// is untouched by an attacker's purge attempt.
		const ok = await idem.acquire('charge-1', 60, { user: 'c', tenant: 'ab' });
		expect(ok.acquired).toBe(true);
		await ok.commit({ receipt: 'paid-100' });
		await expect(idem.purgeUser('a', 'b\0c')).rejects.toThrow('NUL');
		const again = await idem.acquire('charge-1', 60, { user: 'c', tenant: 'ab' });
		expect(again.acquired).toBe(false);
		expect(again.result).toEqual({ receipt: 'paid-100' });
	});

	it('indexes and erases a non-string tenant under the same key', async () => {
		// acquire used to typeof-filter meta.tenant while purgeUser coerced
		// its argument, so a numeric tenant indexed under the untenanted
		// scope and the erasure looked somewhere else and silently missed.
		const idem = createIdempotencyStore(mockRedisClient('idem3:'), {});
		const slot = await idem.acquire('op-1', 60, { user: 'u1', tenant: 7 });
		expect(slot.acquired).toBe(true);
		await slot.commit({ ok: true });
		expect(await idem.purgeUser(7, 'u1')).toBeGreaterThan(0);
		const after = await idem.acquire('op-1', 60, { user: 'u1', tenant: 7 });
		expect(after.acquired).toBe(true);
	});
});

describe('cluster clock leader key', () => {
	it('publishes the offset under the client key prefix', async () => {
		const client = mockRedisClient('app:');
		const sets = [];
		const realSet = client.redis.set.bind(client.redis);
		client.redis.set = async (...a) => { sets.push(a); return realSet(...a); };

		const clock = createClusterClock(client, {
			samples: 1, immediate: false, leader: { isLeader: () => true }, leaderTtlMs: 90_000
		});
		await clock.sample();
		await clock.stop();

		// Left unprefixed, two apps sharing one Redis collide on this
		// well-known key and either can shift the other's event ordering.
		expect(sets.some(([k]) => k === 'app:clock:leader-offset')).toBe(true);
		expect(sets.some(([k]) => k === 'clock:leader-offset')).toBe(false);
	});

	// `stamp()` and `consistent()` each take their OWN `wallEpoch()` reading,
	// so on wall time the difference is the offset plus however much real time
	// passed between the two calls - which is 0ms almost always and 1ms when a
	// millisecond boundary happens to fall in between. That is a coin flip, and
	// it was observed failing. Pinning the injectable clock seam (the mock
	// Redis serves TIME from the same seam, so the whole sample is consistent)
	// makes the assertion about the offset and nothing else.
	function withPinnedClock(fn) {
		return async () => {
			setRuntimeEnv({ clock: { wallEpoch: () => 1_700_000_000_000, now: () => 1_700_000_000_000 } });
			try { await fn(); } finally { resetRuntimeEnv(); }
		};
	}

	it('reads a pre-cutover unprefixed key when the rolling-upgrade fallback is enabled', withPinnedClock(async () => {
		const client = mockRedisClient('app:');
		// An instance still running the build that wrote the raw key is the
		// leader during a rollout; a new follower that only looked at the
		// prefixed key would find nothing and silently fall back to local
		// time. Opt in for that window.
		await client.redis.set('clock:leader-offset', JSON.stringify({ o: 1234 }));
		const clock = createClusterClock(client, {
			samples: 1, immediate: false, leader: { isLeader: () => false }, leaderTtlMs: 90_000,
			legacyLeaderKeyFallback: true
		});
		await clock.sample();
		// A follower that picked up the leader offset stamps ahead of its own
		// consistent clock by exactly that offset.
		expect(clock.stamp() - clock.consistent()).toBe(1234);
		await clock.stop();
	}));

	it('ignores the unprefixed key by default, so another app cannot shift this one', withPinnedClock(async () => {
		const client = mockRedisClient('app:');
		// The unprefixed key is the shared name the prefixing exists to
		// escape. Reading it by default hands a follower whatever OTHER app
		// on the same Redis happens to be publishing there - re-opening the
		// collision on the read side.
		await client.redis.set('clock:leader-offset', JSON.stringify({ o: 1234 }));
		const clock = createClusterClock(client, {
			samples: 1, immediate: false, leader: { isLeader: () => false }, leaderTtlMs: 90_000
		});
		await clock.sample();
		expect(clock.stamp() - clock.consistent()).toBe(0);
		await clock.stop();
	}));
});
