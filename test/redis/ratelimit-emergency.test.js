// The fleet-wide emergency scale + the composite multi-dimension limiter.
//
// The emergency factor lives at one shared key, is read through a
// lazily-refreshed cache (never a per-check round trip), scales every
// limiter's effective budget - single-dimension, composite, upgrade
// admission, and the in-process floors - and clamps mid-window so a tighten
// applies immediately. The composite limiter consults every dimension in one
// atomic script and consumes from all of them or from none.

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit, createRateLimitEmergency, createCompositeRateLimit } from '../../src/redis/ratelimit.js';
import { createUpgradeBucket } from '../../src/redis/upgrade-bucket.js';
import { _parseScale } from '../../src/redis/emergency-scale.js';

function mockWs(userData = {}) {
	return { getUserData: () => userData };
}

// Let the reader's background GET land: the first current() fires the
// refresh and returns the previous value; a microtask flush later the cache
// holds the stored factor.
async function warmScale(limiter, ws) {
	await limiter.consume(ws ?? mockWs({ ip: '203.0.113.250' }));
	await Promise.resolve();
	await Promise.resolve();
	await Promise.resolve();
}

describe('ratelimit emergency scale', () => {
	let client;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
	});

	describe('operator surface', () => {
		it('set/get/clear round-trips the shared key', async () => {
			const emergency = createRateLimitEmergency(client);
			expect(await emergency.get()).toBe(1);
			await emergency.set(0.25);
			expect(await emergency.get()).toBe(0.25);
			await emergency.clear();
			expect(await emergency.get()).toBe(1);
		});

		it('the limiter instance and the standalone surface share one key', async () => {
			const limiter = createRateLimit(client, { points: 10, interval: 60000 });
			await limiter.emergency.set(0.5);
			expect(await createRateLimitEmergency(client).get()).toBe(0.5);
		});

		it('rejects out-of-band factors and negative ttl', async () => {
			const emergency = createRateLimitEmergency(client);
			await expect(emergency.set(0)).rejects.toThrow('emergency scale');
			await expect(emergency.set(-1)).rejects.toThrow('emergency scale');
			await expect(emergency.set(Infinity)).rejects.toThrow('emergency scale');
			await expect(emergency.set(0.5, { ttlMs: -1 })).rejects.toThrow('ttlMs');
		});

		it('a corrupt stored value parses as neutral, an out-of-band one clamps', () => {
			expect(_parseScale('garbage')).toBe(1);
			expect(_parseScale(null)).toBe(1);
			expect(_parseScale('0')).toBe(0.001);
			expect(_parseScale('99999999')).toBe(1000);
			expect(_parseScale('0.5')).toBe(0.5);
		});
	});

	describe('consume-path scaling', () => {
		it('tightens the effective budget: scale 0.2 on 10 points admits 2', async () => {
			const limiter = createRateLimit(client, { points: 10, interval: 60000 });
			await limiter.emergency.set(0.2);
			await warmScale(limiter); // warm-up key is its own bucket
			const ws = mockWs({ ip: '1.1.1.1' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(true);
			const third = await limiter.consume(ws);
			expect(third.allowed).toBe(false);
			expect(third.remaining).toBe(0);
		});

		it('clamps an already-filled bucket mid-window (a tighten applies now, not at refill)', async () => {
			// The reader refreshes at most once per refreshMs; the first consume
			// below caches the neutral factor, so the clamp needs the window to
			// elapse before the set() becomes visible.
			const limiter = createRateLimit(client, { points: 10, interval: 60000, emergency: { refreshMs: 1 } });
			const ws = mockWs({ ip: '2.2.2.2' });
			expect((await limiter.consume(ws)).remaining).toBe(9);
			await limiter.emergency.set(0.2); // effective budget 2
			await new Promise((r) => setTimeout(r, 5));
			await warmScale(limiter);
			// The bucket held 9; the clamp drops it to 2 before this consume.
			const clamped = await limiter.consume(ws);
			expect(clamped.allowed).toBe(true);
			expect(clamped.remaining).toBe(1);
		});

		it('loosens with a factor above 1', async () => {
			const limiter = createRateLimit(client, { points: 2, interval: 60000 });
			await limiter.emergency.set(3);
			await warmScale(limiter);
			const ws = mockWs({ ip: '3.3.3.3' });
			for (let i = 0; i < 6; i++) {
				expect((await limiter.consume(ws)).allowed).toBe(true);
			}
			expect((await limiter.consume(ws)).allowed).toBe(false);
		});

		it('a tiny factor never scales a budget below one point', async () => {
			const limiter = createRateLimit(client, { points: 10, interval: 60000 });
			await limiter.emergency.set(0.001);
			await warmScale(limiter);
			const ws = mockWs({ ip: '4.4.4.4' });
			expect((await limiter.consume(ws)).allowed).toBe(true); // max(1, floor(10*0.001)) = 1
			expect((await limiter.consume(ws)).allowed).toBe(false);
		});

		it('stays neutral when the key is absent (zero-config)', async () => {
			const limiter = createRateLimit(client, { points: 3, interval: 60000 });
			await warmScale(limiter);
			const ws = mockWs({ ip: '5.5.5.5' });
			expect((await limiter.consume(ws)).remaining).toBe(2);
		});

		it('fails sticky: a read blip keeps the cached clamp applying', async () => {
			const limiter = createRateLimit(client, { points: 10, interval: 60000, emergency: { refreshMs: 1 } });
			await limiter.emergency.set(0.2);
			await warmScale(limiter);
			// Every further refresh read fails; the cached 0.2 must survive.
			vi.spyOn(client.redis, 'get').mockRejectedValue(new Error('blip'));
			const ws = mockWs({ ip: '6.6.6.6' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(false);
		});

		it('the in-process floor applies the cached clamp while the store is down', async () => {
			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000,
				localFloorOnStorageFailure: true
			});
			await limiter.emergency.set(0.2);
			await warmScale(limiter);
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			const ws = mockWs({ ip: '7.7.7.7' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(false); // floor budget 2, not 10
		});

		it('scales upgrade admission through the same shared key', async () => {
			const limiter = createRateLimit(client, { points: 10, interval: 60000 });
			await limiter.emergency.set(0.2);
			const bucket = createUpgradeBucket(client, { perMinute: 10 });
			// Warm the bucket's own reader.
			await bucket.admit('198.51.100.9');
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
			expect(await bucket.admit('198.51.100.1')).toBe(true);
			expect(await bucket.admit('198.51.100.1')).toBe(true);
			expect(await bucket.admit('198.51.100.1')).toBe(false); // 10 * 0.2 = 2
		});
	});
});

describe('composite ratelimit', () => {
	let client;

	const DIMS = {
		account: { points: 2, interval: 60000, keyBy: (ws) => ws.getUserData().user },
		ip: { points: 3, interval: 60000, keyBy: 'ip' }
	};

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
	});

	describe('validation', () => {
		it('requires at least two and at most eight dimensions', () => {
			expect(() => createCompositeRateLimit(client, { dimensions: { a: { points: 1, interval: 1000, keyBy: 'ip' } } }))
				.toThrow('at least two dimensions');
			const many = {};
			for (let i = 0; i < 9; i++) many['d' + i] = { points: 1, interval: 1000, keyBy: 'ip' };
			expect(() => createCompositeRateLimit(client, { dimensions: many })).toThrow('at most 8');
		});

		it('requires keyBy per dimension (no shared default to collapse the composite)', () => {
			expect(() => createCompositeRateLimit(client, {
				dimensions: { a: { points: 1, interval: 1000, keyBy: 'ip' }, b: { points: 1, interval: 1000 } }
			})).toThrow("dimension 'b' keyBy is required");
		});

		it('rejects a dimension name that would not survive the key layout', () => {
			expect(() => createCompositeRateLimit(client, {
				dimensions: { 'a b': { points: 1, interval: 1000, keyBy: 'ip' }, c: { points: 1, interval: 1000, keyBy: 'ip' } }
			})).toThrow("dimension name 'a b'");
		});

		it('rejects tenant ids carrying key-layout delimiters', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: DIMS,
				tenant: (ws) => ws.getUserData().org
			});
			await expect(composite.consume(mockWs({ user: 'u', ip: '1.1.1.1', org: 'a}b' })))
				.rejects.toThrow('NUL or brace');
		});
	});

	describe('atomic most-strict-wins verdicts', () => {
		it('admits only while every dimension has budget and names the tripped one', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			const ws = mockWs({ user: 'alice', ip: '9.9.9.9' });

			const first = await composite.consume(ws);
			expect(first).toMatchObject({ allowed: true, tripped: null, remaining: { account: 1, ip: 2 } });

			const second = await composite.consume(ws);
			expect(second).toMatchObject({ allowed: true, remaining: { account: 0, ip: 1 } });

			const third = await composite.consume(ws);
			expect(third.allowed).toBe(false);
			expect(third.tripped).toBe('account');
			// NOTHING was consumed on the deny: ip still holds what the second
			// consume left.
			expect(third.remaining.ip).toBe(1);
			const fourth = await composite.consume(ws);
			expect(fourth.remaining.ip).toBe(1);
		});

		it('two users share the ip dimension while keeping their own account budgets', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			const alice = mockWs({ user: 'alice', ip: '8.8.8.8' });
			const bob = mockWs({ user: 'bob', ip: '8.8.8.8' });
			expect((await composite.consume(alice)).allowed).toBe(true);
			expect((await composite.consume(bob)).allowed).toBe(true);
			expect((await composite.consume(alice)).allowed).toBe(true);
			// ip budget (3) exhausted before either account budget (2 each).
			const denied = await composite.consume(bob);
			expect(denied.allowed).toBe(false);
			expect(denied.tripped).toBe('ip');
			expect(denied.remaining.account.constructor).toBe(Number);
		});

		it('declaration order decides when several dimensions would trip', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: {
					first: { points: 1, interval: 60000, keyBy: () => 'k' },
					second: { points: 1, interval: 60000, keyBy: () => 'k' }
				}
			});
			const ws = mockWs({});
			await composite.consume(ws);
			const denied = await composite.consume(ws);
			expect(denied.tripped).toBe('first');
		});

		it('applies the tripped dimension auto-ban and keeps denying on it', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: {
					account: { points: 1, interval: 1000, blockDuration: 60000, keyBy: (ws) => ws.getUserData().user },
					ip: { points: 100, interval: 1000, keyBy: 'ip' }
				}
			});
			const ws = mockWs({ user: 'mallory', ip: '6.6.6.6' });
			await composite.consume(ws); // the one allowed consume: ip 100 -> 99
			const banned = await composite.consume(ws);
			expect(banned).toMatchObject({ allowed: false, tripped: 'account' });
			expect(banned.resetMs).toBe(60000);
			const still = await composite.consume(ws);
			expect(still.tripped).toBe('account');
			expect(still.remaining.ip).toBe(99); // denies consumed nothing further
		});

		it('groups every dimension key for a check under one hash tag', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: DIMS,
				tenant: (ws) => ws.getUserData().org
			});
			await composite.consume(mockWs({ user: 'u1', ip: '1.2.3.4', org: 'acme' }));
			const keys = [...client._hashes.keys()].filter((k) => k.includes('ratelimitc')).sort();
			expect(keys).toEqual([
				'test:v1:ratelimitc:{t:acme}:account\0u1',
				'test:v1:ratelimitc:{t:acme}:ip\x001.2.3.4'
			]);
		});
	});

	describe('admin ops', () => {
		it('reset clears one dimension bucket; ban/unban gate one dimension', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			const ws = mockWs({ user: 'carol', ip: '7.7.7.7' });
			await composite.consume(ws);
			await composite.consume(ws);
			expect((await composite.consume(ws)).tripped).toBe('account');
			await composite.reset('account', 'carol');
			expect((await composite.consume(ws)).allowed).toBe(true);

			await composite.ban('ip', '7.7.7.7', 60000);
			const banned = await composite.consume(ws);
			expect(banned).toMatchObject({ allowed: false, tripped: 'ip' });
			await composite.unban('ip', '7.7.7.7');
			// Both budgets saw real consumption above; reset both so the final
			// consume proves the unban (and only the unban) unblocked the check.
			await composite.reset('account', 'carol');
			await composite.reset('ip', '7.7.7.7');
			expect((await composite.consume(ws)).allowed).toBe(true);
		});

		it('rejects unknown dimensions on admin ops', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			await expect(composite.reset('nope', 'k')).rejects.toThrow("unknown dimension 'nope'");
			await expect(composite.ban('nope', 'k', 1000)).rejects.toThrow("unknown dimension 'nope'");
			await expect(composite.unban('nope', 'k')).rejects.toThrow("unknown dimension 'nope'");
		});

		it('clear(tenant) removes only that tenant hash-tag space', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: DIMS,
				tenant: (ws) => ws.getUserData().org
			});
			await composite.consume(mockWs({ user: 'u', ip: '1.1.1.1', org: 'a' }));
			await composite.consume(mockWs({ user: 'u', ip: '1.1.1.1', org: 'b' }));
			await composite.clear('a');
			const keys = [...client._hashes.keys()].filter((k) => k.includes('ratelimitc'));
			expect(keys.every((k) => k.includes('{t:b}'))).toBe(true);
			expect(keys).toHaveLength(2);
		});
	});

	describe('emergency scale + floor', () => {
		it('the shared factor scales every dimension', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: {
					account: { points: 10, interval: 60000, keyBy: (ws) => ws.getUserData().user },
					ip: { points: 20, interval: 60000, keyBy: 'ip' }
				}
			});
			await composite.emergency.set(0.2);
			await composite.consume(mockWs({ user: 'warm', ip: '203.0.113.7' }));
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
			const ws = mockWs({ user: 'dave', ip: '5.5.5.5' });
			expect((await composite.consume(ws)).allowed).toBe(true);
			expect((await composite.consume(ws)).allowed).toBe(true);
			const denied = await composite.consume(ws);
			expect(denied.allowed).toBe(false);
			expect(denied.tripped).toBe('account'); // 10*0.2=2 trips before 20*0.2=4
		});

		it('the floor keeps all-or-nothing semantics while the store is down', async () => {
			const composite = createCompositeRateLimit(client, {
				dimensions: DIMS,
				localFloorOnStorageFailure: true
			});
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			vi.spyOn(console, 'warn').mockImplementation(() => {});
			const ws = mockWs({ user: 'erin', ip: '4.4.4.4' });
			expect((await composite.consume(ws)).allowed).toBe(true);
			expect((await composite.consume(ws)).allowed).toBe(true);
			const denied = await composite.consume(ws);
			expect(denied.allowed).toBe(false);
			expect(denied.tripped).toBe('account');
			expect(denied.remaining.ip).toBe(1); // deny consumed nothing on the floor either
		});

		it('rejects to the caller without a floor', async () => {
			const composite = createCompositeRateLimit(client, { dimensions: DIMS });
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			await expect(composite.consume(mockWs({ user: 'x', ip: '3.3.3.3' }))).rejects.toThrow('down');
		});
	});
});
