import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createCircuitBreaker } from '../../shared/breaker.js';
import { installFakeRuntimeClock, releaseRuntimeClock } from '../helpers/runtime-clock.js';
import {
	createUpgradeBucket,
	createLocalUpgradeBucket
} from '../../redis/upgrade-bucket.js';

describe('redis upgrade-bucket', () => {
	let client;

	beforeEach(() => {
		vi.restoreAllMocks();
		// The local bucket reads wall time through the runtime clock; bind it to
		// the global Date.now so the per-test vi.spyOn(Date, 'now') drives it.
		installFakeRuntimeClock();
		client = mockRedisClient('test:');
	});

	afterEach(() => {
		releaseRuntimeClock();
	});

	describe('createUpgradeBucket validation', () => {
		it('returns a bucket with the expected API', () => {
			const bucket = createUpgradeBucket(client, { perMinute: 60 });
			expect(typeof bucket.admit).toBe('function');
			expect(typeof bucket.reset).toBe('function');
			expect(typeof bucket.clear).toBe('function');
		});

		it('throws on missing options', () => {
			expect(() => createUpgradeBucket(client)).toThrow('options object is required');
		});

		it('throws on non-positive-integer perMinute', () => {
			expect(() => createUpgradeBucket(client, { perMinute: 0 })).toThrow('perMinute must be a positive integer');
			expect(() => createUpgradeBucket(client, { perMinute: -1 })).toThrow('perMinute must be a positive integer');
			expect(() => createUpgradeBucket(client, { perMinute: 1.5 })).toThrow('perMinute must be a positive integer');
		});

		it('throws on negative blockDuration', () => {
			expect(() => createUpgradeBucket(client, { perMinute: 60, blockDuration: -1 })).toThrow('non-negative');
		});

		it('throws on bad elevated budget shape', () => {
			expect(() => createUpgradeBucket(client, { perMinute: 60, elevated: 5 })).toThrow('elevated must be a budget object');
			expect(() => createUpgradeBucket(client, { perMinute: 60, elevated: { perMinute: 0 } })).toThrow('elevated perMinute must be a positive integer');
		});

		it('throws on bad siege budget shape', () => {
			expect(() => createUpgradeBucket(client, { perMinute: 60, siege: { perMinute: -2 } })).toThrow('siege perMinute must be a positive integer');
		});
	});

	describe('admit - per-IP token bucket', () => {
		it('admits up to the per-minute budget then rejects', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 3 });
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(false);
		});

		it('keys separate IPs into separate buckets', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(false);
			expect(await bucket.admit('5.6.7.8')).toBe(true);
		});

		it('refills after the minute interval', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 2 });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(false);

			Date.now.mockReturnValue(now + 60001);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});

		it('uses a versioned, IP-keyed Redis key', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 5 });
			await bucket.admit('9.9.9.9');
			const keys = [...client._hashes.keys()].filter((k) => k.includes('upgrade-bucket'));
			expect(keys).toHaveLength(1);
			expect(keys[0]).toMatch(/^test:v\d+:upgrade-bucket:9\.9\.9\.9$/);
		});

		it('coerces a null/undefined IP to a stable bucket', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			expect(await bucket.admit(null)).toBe(true);
			expect(await bucket.admit(undefined)).toBe(false);
		});
	});

	describe('admit - posture-keyed budgets', () => {
		it('selects the posture budget by string argument', async () => {
			const bucket = createUpgradeBucket(client, {
				perMinute: 5,
				elevated: { perMinute: 2 },
				siege: { perMinute: 1 }
			});
			// siege budget = 1
			expect(await bucket.admit('a', 'siege')).toBe(true);
			expect(await bucket.admit('a', 'siege')).toBe(false);
			// elevated budget = 2, distinct IP
			expect(await bucket.admit('b', 'elevated')).toBe(true);
			expect(await bucket.admit('b', 'elevated')).toBe(true);
			expect(await bucket.admit('b', 'elevated')).toBe(false);
		});

		it("an omitted siege budget inherits elevated; omitted elevated inherits normal", async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 2, elevated: { perMinute: 1 } });
			// siege inherits elevated (1)
			expect(await bucket.admit('s', 'siege')).toBe(true);
			expect(await bucket.admit('s', 'siege')).toBe(false);
			// unknown / normal posture uses normal (2)
			expect(await bucket.admit('n', 'normal')).toBe(true);
			expect(await bucket.admit('n', 'normal')).toBe(true);
			expect(await bucket.admit('n', 'normal')).toBe(false);
		});

		it('an unknown posture string degrades to the normal budget', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 2, siege: { perMinute: 1 } });
			expect(await bucket.admit('x', 'who-knows')).toBe(true);
			expect(await bucket.admit('x', 'who-knows')).toBe(true);
			expect(await bucket.admit('x', 'who-knows')).toBe(false);
		});
	});

	describe('admit - block duration', () => {
		it('bans an IP once the budget is spent when blockDuration is set', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1, blockDuration: 5000 });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(false); // triggers ban

			// Banned: still rejected while bannedUntil is in the future.
			Date.now.mockReturnValue(now + 1000);
			expect(await bucket.admit('1.2.3.4')).toBe(false);

			// Past the ban window but before the minute interval refills: the bucket
			// is still token-starved, so the IP stays rejected.
			Date.now.mockReturnValue(now + 5001);
			expect(await bucket.admit('1.2.3.4')).toBe(false);

			// After the minute interval, the bucket refills and admits again.
			Date.now.mockReturnValue(now + 60001);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});
	});

	describe('admit - fail open', () => {
		it('admits when the Redis eval throws', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('connection refused'));
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});

		it('admits when the breaker is open', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const bucket = createUpgradeBucket(client, { perMinute: 1, breaker });
			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));

			// First call trips the breaker (and fails open).
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(breaker.state).toBe('broken');
			// With the breaker open, withBreaker throws CircuitBrokenError before
			// touching Redis - still admitted.
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});
	});

	describe('reset / clear', () => {
		it('reset clears one IP bucket', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			expect(await bucket.admit('1.2.3.4')).toBe(true);
			expect(await bucket.admit('1.2.3.4')).toBe(false);

			await bucket.reset('1.2.3.4');
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});

		it('clear removes all bucket state', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			await bucket.admit('1.2.3.4');
			await bucket.admit('5.6.7.8');

			await bucket.clear();

			const keys = [...client._hashes.keys()].filter((k) => k.includes('upgrade-bucket'));
			expect(keys).toHaveLength(0);
			expect(await bucket.admit('1.2.3.4')).toBe(true);
		});

		it('reset is best-effort when Redis throws', async () => {
			const bucket = createUpgradeBucket(client, { perMinute: 1 });
			vi.spyOn(client.redis, 'del').mockRejectedValue(new Error('down'));
			await expect(bucket.reset('1.2.3.4')).resolves.toBeUndefined();
		});
	});

	describe('metrics', () => {
		it('increments admitted / rejected / fail-open counters', async () => {
			const counters = {};
			const metrics = {
				counter(name) {
					counters[name] = counters[name] || { value: 0, inc() { this.value++; } };
					return counters[name];
				}
			};
			const bucket = createUpgradeBucket(client, { perMinute: 1, metrics });

			await bucket.admit('1.2.3.4'); // admitted
			await bucket.admit('1.2.3.4'); // rejected
			expect(counters.upgrade_bucket_admitted_total.value).toBe(1);
			expect(counters.upgrade_bucket_rejected_total.value).toBe(1);

			vi.spyOn(client.redis, 'eval').mockRejectedValue(new Error('down'));
			await bucket.admit('5.6.7.8'); // fail open
			expect(counters.upgrade_bucket_fail_open_total.value).toBe(1);
		});
	});
});

describe('createLocalUpgradeBucket', () => {
	beforeEach(() => {
		vi.restoreAllMocks();
		// The local bucket reads wall time through the runtime clock; bind it to
		// the global Date.now so the per-test vi.spyOn(Date, 'now') drives it.
		installFakeRuntimeClock();
	});

	afterEach(() => {
		releaseRuntimeClock();
	});

	it('returns a bucket with the expected API', () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 60 });
		expect(typeof bucket.admit).toBe('function');
		expect(typeof bucket.reset).toBe('function');
		expect(typeof bucket.clear).toBe('function');
	});

	it('throws on bad options like the Redis variant', () => {
		expect(() => createLocalUpgradeBucket()).toThrow('options object is required');
		expect(() => createLocalUpgradeBucket({ perMinute: 0 })).toThrow('perMinute must be a positive integer');
	});

	it('throws on non-positive maxEntries', () => {
		expect(() => createLocalUpgradeBucket({ perMinute: 1, maxEntries: 0 })).toThrow('maxEntries must be a positive integer');
		expect(() => createLocalUpgradeBucket({ perMinute: 1, maxEntries: 1.5 })).toThrow('maxEntries must be a positive integer');
	});

	it('admits up to the budget then rejects, per IP', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 2 });
		expect(await bucket.admit('1.2.3.4')).toBe(true);
		expect(await bucket.admit('1.2.3.4')).toBe(true);
		expect(await bucket.admit('1.2.3.4')).toBe(false);
		expect(await bucket.admit('5.6.7.8')).toBe(true);
	});

	it('refills after the minute interval', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 1 });
		const now = Date.now();
		vi.spyOn(Date, 'now').mockReturnValue(now);

		expect(await bucket.admit('1.2.3.4')).toBe(true);
		expect(await bucket.admit('1.2.3.4')).toBe(false);

		Date.now.mockReturnValue(now + 60001);
		expect(await bucket.admit('1.2.3.4')).toBe(true);
	});

	it('selects posture-keyed budgets', async () => {
		const bucket = createLocalUpgradeBucket({
			perMinute: 5,
			elevated: { perMinute: 2 },
			siege: { perMinute: 1 }
		});
		expect(await bucket.admit('a', 'siege')).toBe(true);
		expect(await bucket.admit('a', 'siege')).toBe(false);
		expect(await bucket.admit('b', 'elevated')).toBe(true);
		expect(await bucket.admit('b', 'elevated')).toBe(true);
		expect(await bucket.admit('b', 'elevated')).toBe(false);
	});

	it('bans an IP once spent when blockDuration is set', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 1, blockDuration: 5000 });
		const now = Date.now();
		vi.spyOn(Date, 'now').mockReturnValue(now);

		expect(await bucket.admit('1.2.3.4')).toBe(true);
		expect(await bucket.admit('1.2.3.4')).toBe(false); // triggers ban

		Date.now.mockReturnValue(now + 1000);
		expect(await bucket.admit('1.2.3.4')).toBe(false); // still banned

		// Past the ban but before the minute refill: still token-starved.
		Date.now.mockReturnValue(now + 5001);
		expect(await bucket.admit('1.2.3.4')).toBe(false);

		// After the minute interval the bucket refills.
		Date.now.mockReturnValue(now + 60001);
		expect(await bucket.admit('1.2.3.4')).toBe(true);
	});

	it('caps the map size and evicts the least-recently-used IP', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 5, maxEntries: 2 });

		// Fill two slots; touch 'a' first so it is the LRU candidate after 'b'.
		await bucket.admit('a'); // a: tokens 4
		await bucket.admit('b'); // b: tokens 4
		// Re-touch 'a' so 'b' becomes the least-recently-used.
		await bucket.admit('a'); // a: tokens 3, now MRU

		// Inserting a third IP evicts the LRU ('b').
		await bucket.admit('c');

		// 'b' was evicted, so it starts fresh (full budget) instead of resuming.
		// Spend 'b' down to prove it was reset (5 admits succeed from a fresh entry).
		for (let i = 0; i < 5; i++) {
			expect(await bucket.admit('b')).toBe(true);
		}
		expect(await bucket.admit('b')).toBe(false);
	});

	it('re-touching an existing IP does not grow the map', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 5, maxEntries: 100 });
		await bucket.admit('1.2.3.4');
		await bucket.admit('1.2.3.4');
		await bucket.admit('1.2.3.4');
		// No public size accessor; assert behaviorally that the same bucket drained.
		await bucket.admit('1.2.3.4');
		await bucket.admit('1.2.3.4');
		expect(await bucket.admit('1.2.3.4')).toBe(false);
	});

	it('counts evictions via metrics', async () => {
		const counters = {};
		const metrics = {
			counter(name) {
				counters[name] = counters[name] || { value: 0, inc() { this.value++; } };
				return counters[name];
			}
		};
		const bucket = createLocalUpgradeBucket({ perMinute: 5, maxEntries: 1, metrics });
		await bucket.admit('a');
		await bucket.admit('b'); // evicts 'a'
		expect(counters.upgrade_bucket_evicted_total.value).toBe(1);
	});

	it('reset clears one IP and clear drops all', async () => {
		const bucket = createLocalUpgradeBucket({ perMinute: 1 });
		expect(await bucket.admit('1.2.3.4')).toBe(true);
		expect(await bucket.admit('1.2.3.4')).toBe(false);

		await bucket.reset('1.2.3.4');
		expect(await bucket.admit('1.2.3.4')).toBe(true);

		await bucket.admit('5.6.7.8');
		await bucket.clear();
		expect(await bucket.admit('5.6.7.8')).toBe(true);
	});
});
