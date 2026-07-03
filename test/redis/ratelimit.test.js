import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

function mockWs(userData = {}) {
	return { getUserData: () => userData };
}

describe('redis ratelimit', () => {
	let client;
	let limiter;

	beforeEach(() => {
		vi.restoreAllMocks();
		client = mockRedisClient('test:');
		limiter = createRateLimit(client, { points: 5, interval: 1000 });
	});

	describe('createRateLimit', () => {
		it('returns a limiter with the expected API', () => {
			expect(typeof limiter.consume).toBe('function');
			expect(typeof limiter.reset).toBe('function');
			expect(typeof limiter.ban).toBe('function');
			expect(typeof limiter.unban).toBe('function');
			expect(typeof limiter.clear).toBe('function');
		});

		it('throws on missing options', () => {
			expect(() => createRateLimit(client)).toThrow('options object is required');
		});

		it('throws on non-positive points', () => {
			expect(() => createRateLimit(client, { points: 0, interval: 1000 })).toThrow('positive integer');
			expect(() => createRateLimit(client, { points: -1, interval: 1000 })).toThrow('positive integer');
			expect(() => createRateLimit(client, { points: 1.5, interval: 1000 })).toThrow('positive integer');
		});

		it('throws on non-positive interval', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 0 })).toThrow('positive number');
			expect(() => createRateLimit(client, { points: 5, interval: -100 })).toThrow('positive number');
		});

		it('throws on negative blockDuration', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 1000, blockDuration: -1 })).toThrow('non-negative');
		});

		it('throws on invalid keyBy', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 1000, keyBy: 'bad' })).toThrow('keyBy');
		});

		it('throws on a non-function tenant resolver', () => {
			expect(() => createRateLimit(client, { points: 5, interval: 1000, tenant: 'bad' })).toThrow('tenant must be a function');
		});
	});

	describe('tenant scoping', () => {
		it('scopes the bucket key by the tenant resolver (NUL-delimited, IPv6-safe)', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, tenant: (ws) => ws.getUserData().org });
			await lim.consume(mockWs({ ip: '1.2.3.4', org: 'a' }));
			await lim.consume(mockWs({ ip: '1.2.3.4', org: 'b' }));
			const rlKeys = [...client._hashes.keys()].filter((k) => k.includes('ratelimit')).sort();
			expect(rlKeys).toEqual(['test:v1:ratelimit:a\x001.2.3.4', 'test:v1:ratelimit:b\x001.2.3.4']);
		});

		it('gives two tenants on the SAME ip independent buckets', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000, tenant: (ws) => ws.getUserData().org });
			expect((await lim.consume(mockWs({ ip: '9.9.9.9', org: 'a' }))).allowed).toBe(true);
			// B is not exhausted by A - separate bucket.
			expect((await lim.consume(mockWs({ ip: '9.9.9.9', org: 'b' }))).allowed).toBe(true);
			// A's own bucket (points:1) is now exhausted.
			expect((await lim.consume(mockWs({ ip: '9.9.9.9', org: 'a' }))).allowed).toBe(false);
		});

		it('clear(tenant) drops only that tenant; clear() drops all', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, tenant: (ws) => ws.getUserData().org });
			await lim.consume(mockWs({ ip: '1.1.1.1', org: 'a' }));
			await lim.consume(mockWs({ ip: '1.1.1.1', org: 'b' }));
			await lim.clear('a');
			expect([...client._hashes.keys()].filter((k) => k.includes('ratelimit'))).toEqual(['test:v1:ratelimit:b\x001.1.1.1']);
			await lim.clear();
			expect([...client._hashes.keys()].filter((k) => k.includes('ratelimit'))).toEqual([]);
		});

		it('reset(key, tenant) targets the tenant-scoped bucket', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, tenant: (ws) => ws.getUserData().org });
			await lim.consume(mockWs({ ip: '5.5.5.5', org: 'a' }));
			await lim.consume(mockWs({ ip: '5.5.5.5', org: 'b' }));
			await lim.reset('5.5.5.5', 'a');
			const rlKeys = [...client._hashes.keys()].filter((k) => k.includes('ratelimit'));
			expect(rlKeys).toEqual(['test:v1:ratelimit:b\x005.5.5.5']);
		});

		it('no tenant resolver -> byte-identical bare key', async () => {
			await limiter.consume(mockWs({ ip: '2.2.2.2' }));
			expect([...client._hashes.keys()].filter((k) => k.includes('ratelimit'))).toEqual(['test:v1:ratelimit:2.2.2.2']);
		});

		it('rejects a tenant id containing the NUL delimiter (injection-safety)', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 1000, tenant: () => 'a\0b' });
			await expect(lim.consume(mockWs({ ip: '1.2.3.4' }))).rejects.toThrow('NUL byte');
		});

		it('labels the rate-limit counters by tenant_id when a resolver is set', async () => {
			const metrics = createMetrics();
			const lim = createRateLimit(client, { points: 5, interval: 1000, tenant: (ws) => ws.getUserData().org, metrics });
			await lim.consume(mockWs({ ip: '1.2.3.4', org: 'a' }));
			await lim.consume(mockWs({ ip: '1.2.3.4', org: 'b' }));
			const out = metrics.serialize();
			expect(out).toContain('ratelimit_allowed_total{tenant_id="a"} 1');
			expect(out).toContain('ratelimit_allowed_total{tenant_id="b"} 1');
		});

		it('no tenant resolver -> counters carry no tenant_id label (byte-identical metrics)', async () => {
			const metrics = createMetrics();
			const lim = createRateLimit(client, { points: 5, interval: 1000, metrics });
			await lim.consume(mockWs({ ip: '2.2.2.2' }));
			const out = metrics.serialize();
			expect(out).toContain('ratelimit_allowed_total 1');
			expect(out).not.toContain('tenant_id');
		});
	});

	describe('key versioning', () => {
		it('uses versioned key prefix to isolate Lua script versions', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws);

			const allKeys = [...client._hashes.keys()];
			const rlKeys = allKeys.filter((k) => k.includes('ratelimit'));
			expect(rlKeys).toHaveLength(1);
			expect(rlKeys[0]).toMatch(/^test:v\d+:ratelimit:1\.2\.3\.4$/);
		});

		it('clear() only scans versioned keys', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws);

			// Plant an unversioned key that should NOT be cleared
			client._hashes.set('test:ratelimit:old', new Map([['points', '5']]));

			await limiter.clear();

			const allKeys = [...client._hashes.keys()];
			const rlKeys = allKeys.filter((k) => k.includes('ratelimit'));
			// The old unversioned key should survive
			expect(rlKeys).toEqual(['test:ratelimit:old']);
		});
	});

	describe('consume - basic token bucket', () => {
		it('first consume is allowed and decrements remaining', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const result = await limiter.consume(ws);

			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
			expect(result.resetMs).toBeGreaterThan(0);
		});

		it('consuming all points succeeds', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) {
				expect((await limiter.consume(ws)).allowed).toBe(true);
			}
			expect((await limiter.consume(ws)).remaining).toBe(0);
		});

		it('exceeding points is rejected', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) await limiter.consume(ws);

			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(false);
		});

		it('custom cost deducts multiple points', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const result = await limiter.consume(ws, 3);

			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(2);
		});

		it('cost exceeding remaining is rejected', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws, 4); // 1 left

			const result = await limiter.consume(ws, 2);
			expect(result.allowed).toBe(false);
		});
	});

	describe('consume - cost validation', () => {
		it('throws on negative cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, -2)).rejects.toThrow('positive integer');
		});

		it('throws on zero cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 0)).rejects.toThrow('positive integer');
		});

		it('throws on fractional cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 1.5)).rejects.toThrow('positive integer');
		});

		it('throws on non-number cost', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await expect(limiter.consume(ws, 'abc')).rejects.toThrow('positive integer');
		});
	});

	describe('consume - refill', () => {
		it('refills after interval passes', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			for (let i = 0; i < 5; i++) await limiter.consume(ws);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			Date.now.mockReturnValue(now + 1001);
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
		});

		it('partial interval does not refill', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			for (let i = 0; i < 5; i++) await limiter.consume(ws);

			Date.now.mockReturnValue(now + 500);
			expect((await limiter.consume(ws)).allowed).toBe(false);
		});
	});

	describe('consume - auto-ban', () => {
		it('bans when points exhausted and blockDuration set', async () => {
			const rl = createRateLimit(client, { points: 2, interval: 1000, blockDuration: 5000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws);
			const result = await rl.consume(ws);

			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBe(5000);
		});

		it('ban expires after blockDuration', async () => {
			const rl = createRateLimit(client, { points: 2, interval: 1000, blockDuration: 5000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws);
			await rl.consume(ws); // triggers ban

			Date.now.mockReturnValue(now + 5001);
			const result = await rl.consume(ws);
			expect(result.allowed).toBe(true);
		});

		it('during ban, resetMs reflects ban expiry', async () => {
			const rl = createRateLimit(client, { points: 1, interval: 1000, blockDuration: 3000 });
			const ws = mockWs({ ip: '1.2.3.4' });
			const now = Date.now();
			vi.spyOn(Date, 'now').mockReturnValue(now);

			await rl.consume(ws);
			await rl.consume(ws); // triggers ban

			Date.now.mockReturnValue(now + 1000);
			const result = await rl.consume(ws);
			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBe(2000);
		});
	});

	describe('keyBy modes', () => {
		it('ip mode: same IP shares bucket', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '1.2.3.4' });

			await limiter.consume(ws1, 3);
			const result = await limiter.consume(ws2, 1);
			expect(result.remaining).toBe(1);
		});

		it('ip mode: different IPs get separate buckets', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '5.6.7.8' });

			await limiter.consume(ws1, 5);
			expect((await limiter.consume(ws1)).allowed).toBe(false);
			expect((await limiter.consume(ws2)).allowed).toBe(true);
		});

		it('connection mode: each ws gets its own bucket', async () => {
			const rl = createRateLimit(client, { points: 3, interval: 1000, keyBy: 'connection' });
			const ws1 = mockWs({});
			const ws2 = mockWs({});

			await rl.consume(ws1, 3);
			expect((await rl.consume(ws1)).allowed).toBe(false);
			expect((await rl.consume(ws2)).allowed).toBe(true);
		});

		it('custom function: uses return value as key', async () => {
			const rl = createRateLimit(client, {
				points: 3,
				interval: 1000,
				keyBy: (ws) => ws.getUserData().room
			});
			const ws1 = mockWs({ room: 'A' });
			const ws2 = mockWs({ room: 'A' });
			const ws3 = mockWs({ room: 'B' });

			await rl.consume(ws1, 3);
			expect((await rl.consume(ws2)).allowed).toBe(false);
			expect((await rl.consume(ws3)).allowed).toBe(true);
		});
	});

	describe('reset / ban / unban / clear', () => {
		it('reset clears a key bucket', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			for (let i = 0; i < 5; i++) await limiter.consume(ws);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			await limiter.reset('1.2.3.4');
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
			expect(result.remaining).toBe(4);
		});

		it('ban makes consume return false', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.ban('1.2.3.4', 5000);

			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(false);
			expect(result.resetMs).toBeGreaterThan(0);
		});

		it('unban allows consume again', async () => {
			const ws = mockWs({ ip: '1.2.3.4' });
			await limiter.consume(ws); // 4 remaining
			await limiter.ban('1.2.3.4', 60000);
			expect((await limiter.consume(ws)).allowed).toBe(false);

			await limiter.unban('1.2.3.4');
			expect((await limiter.consume(ws)).allowed).toBe(true);
		});

		it('operations on unknown keys are safe', async () => {
			await limiter.reset('nope');
			await limiter.ban('nope');
			await limiter.unban('nope');
			// Should not throw
		});

		it('clear resets all state', async () => {
			const ws1 = mockWs({ ip: '1.2.3.4' });
			const ws2 = mockWs({ ip: '5.6.7.8' });
			await limiter.consume(ws1, 5);
			await limiter.consume(ws2, 5);

			await limiter.clear();

			const r1 = await limiter.consume(ws1);
			expect(r1.allowed).toBe(true);
			expect(r1.remaining).toBe(4);
			expect((await limiter.consume(ws2)).allowed).toBe(true);
		});
	});

	describe('proxy-collapse warning (keyBy: ip)', () => {
		let warnSpy;
		let savedAddressHeaders;

		// Snapshot AND clear every env name isAddressHeaderConfigured() matches (the
		// unprefixed ADDRESS_HEADER and any *_ADDRESS_HEADER envPrefix form), so the
		// positive-warning tests are deterministic regardless of a CI/dev shell that
		// injects a prefixed address-header var. Restored verbatim afterwards.
		function addressHeaderKeys() {
			return Object.keys(process.env).filter((k) => k === 'ADDRESS_HEADER' || k.endsWith('_ADDRESS_HEADER'));
		}

		beforeEach(() => {
			savedAddressHeaders = {};
			for (const k of addressHeaderKeys()) {
				savedAddressHeaders[k] = process.env[k];
				delete process.env[k];
			}
			warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
		});

		afterEach(() => {
			for (const k of addressHeaderKeys()) delete process.env[k];
			for (const [k, v] of Object.entries(savedAddressHeaders)) process.env[k] = v;
		});

		it('warns once on the first denial keyed on a private/loopback address with ADDRESS_HEADER unset', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000 });
			const ws = mockWs({ remoteAddress: '172.17.0.1' });
			await lim.consume(ws);
			const denied = await lim.consume(ws);
			expect(denied.allowed).toBe(false);
			expect(warnSpy).toHaveBeenCalledTimes(1);
			expect(warnSpy.mock.calls[0][0]).toContain('172.17.0.1');
			expect(warnSpy.mock.calls[0][0]).toContain('ADDRESS_HEADER');
		});

		it('fires at most once (latched) across further denials', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000 });
			const ws = mockWs({ remoteAddress: '10.0.0.5' });
			for (let i = 0; i < 4; i++) await lim.consume(ws);
			expect(warnSpy).toHaveBeenCalledTimes(1);
		});

		it('does not warn on an allowed consume', async () => {
			const lim = createRateLimit(client, { points: 5, interval: 60000 });
			const r = await lim.consume(mockWs({ remoteAddress: '127.0.0.1' }));
			expect(r.allowed).toBe(true);
			expect(warnSpy).not.toHaveBeenCalled();
		});

		it('does not warn when the keyed address is public', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000 });
			const ws = mockWs({ remoteAddress: '8.8.8.8' });
			await lim.consume(ws);
			expect((await lim.consume(ws)).allowed).toBe(false);
			expect(warnSpy).not.toHaveBeenCalled();
		});

		it('does not warn when ADDRESS_HEADER is configured', async () => {
			process.env.ADDRESS_HEADER = 'x-forwarded-for';
			const lim = createRateLimit(client, { points: 1, interval: 60000 });
			const ws = mockWs({ remoteAddress: '10.0.0.1' });
			await lim.consume(ws);
			expect((await lim.consume(ws)).allowed).toBe(false);
			expect(warnSpy).not.toHaveBeenCalled();
		});

		it('does not warn in connection mode (no IP keying)', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000, keyBy: 'connection' });
			const ws = mockWs({ remoteAddress: '10.0.0.1' });
			await lim.consume(ws);
			expect((await lim.consume(ws)).allowed).toBe(false);
			expect(warnSpy).not.toHaveBeenCalled();
		});

		it('does not warn with a custom keyBy function', async () => {
			const lim = createRateLimit(client, { points: 1, interval: 60000, keyBy: () => 'room:a' });
			const ws = mockWs({ remoteAddress: '10.0.0.1' });
			await lim.consume(ws);
			expect((await lim.consume(ws)).allowed).toBe(false);
			expect(warnSpy).not.toHaveBeenCalled();
		});
	});

	describe('localFloorOnStorageFailure', () => {
		/** A client whose every Redis call fails, as during an outage. */
		function downClient() {
			return {
				redis: { eval: () => Promise.reject(new Error('redis down')) },
				key: (s) => 'test:' + s
			};
		}

		let warnSpy;
		let wallMs;

		beforeEach(() => {
			warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			wallMs = 1_000_000;
			setRuntimeEnv({ clock: {
				now: () => wallMs,
				monotonic: () => wallMs,
				wallEpoch: () => wallMs
			} });
		});

		afterEach(() => {
			resetRuntimeEnv();
		});

		it('rejects to the caller by default (floor off)', async () => {
			const lim = createRateLimit(downClient(), { points: 5, interval: 1000 });
			await expect(lim.consume(mockWs({ ip: '1.2.3.4' }))).rejects.toThrow('redis down');
		});

		it('validates the option shape', () => {
			expect(() => createRateLimit(downClient(), { points: 5, interval: 1000, localFloorOnStorageFailure: 'yes' }))
				.toThrow('localFloorOnStorageFailure');
			expect(() => createRateLimit(downClient(), { points: 5, interval: 1000, localFloorOnStorageFailure: { points: 0 } }))
				.toThrow('positive integer');
			expect(() => createRateLimit(downClient(), { points: 5, interval: 1000, localFloorOnStorageFailure: { interval: -1 } }))
				.toThrow('positive number');
		});

		it('decides on the in-process bucket while the store is down', async () => {
			const lim = createRateLimit(downClient(), { points: 2, interval: 60000, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(true);
			const denied = await lim.consume(ws);
			expect(denied.allowed).toBe(false);
			expect(denied.resetMs).toBeGreaterThan(0);
		});

		it('keeps distinct keys on distinct floor buckets', async () => {
			const lim = createRateLimit(downClient(), { points: 1, interval: 60000, localFloorOnStorageFailure: true });
			expect((await lim.consume(mockWs({ ip: '1.1.1.1' }))).allowed).toBe(true);
			expect((await lim.consume(mockWs({ ip: '2.2.2.2' }))).allowed).toBe(true);
			expect((await lim.consume(mockWs({ ip: '1.1.1.1' }))).allowed).toBe(false);
		});

		it('refills the floor bucket after the interval', async () => {
			const lim = createRateLimit(downClient(), { points: 1, interval: 60000, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(false);
			wallMs += 60001;
			expect((await lim.consume(ws)).allowed).toBe(true);
		});

		it('applies blockDuration bans on the floor', async () => {
			const lim = createRateLimit(downClient(), { points: 1, interval: 1000, blockDuration: 120000, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			const banned = await lim.consume(ws);
			expect(banned.allowed).toBe(false);
			expect(banned.resetMs).toBe(120000);
			// The interval refill alone does not lift a ban.
			wallMs += 5000;
			expect((await lim.consume(ws)).allowed).toBe(false);
			wallMs += 120001;
			expect((await lim.consume(ws)).allowed).toBe(true);
		});

		it('honors a tighter per-instance floor budget', async () => {
			const lim = createRateLimit(downClient(), {
				points: 10, interval: 60000,
				localFloorOnStorageFailure: { points: 1 }
			});
			const ws = mockWs({ ip: '1.2.3.4' });
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect((await lim.consume(ws)).allowed).toBe(false);
		});

		it('counts floor verdicts in the fallback metric alongside allow/deny', async () => {
			const metrics = createMetrics();
			const lim = createRateLimit(downClient(), { points: 1, interval: 60000, metrics, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			await lim.consume(ws);
			await lim.consume(ws);
			const out = metrics.serialize();
			expect(out).toContain('ratelimit_storage_fallbacks_total 2');
			expect(out).toContain('ratelimit_allowed_total 1');
			expect(out).toContain('ratelimit_denied_total 1');
		});

		it('warns once, not per verdict', async () => {
			const lim = createRateLimit(downClient(), { points: 5, interval: 1000, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			await lim.consume(ws);
			await lim.consume(ws);
			const floorWarns = warnSpy.mock.calls.filter(
				(c) => String(c[0]).includes('in-process floor')
			);
			expect(floorWarns.length).toBe(1);
		});

		it('covers the breaker-open fast path, not only the eval rejection', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const lim = createRateLimit(downClient(), { points: 5, interval: 1000, breaker, localFloorOnStorageFailure: true });
			const ws = mockWs({ ip: '1.2.3.4' });
			// First call fails through eval and trips the breaker; the second
			// is rejected synchronously by the open breaker. Both degrade.
			expect((await lim.consume(ws)).allowed).toBe(true);
			expect(breaker.isHealthy).toBe(false);
			expect((await lim.consume(ws)).allowed).toBe(true);
		});

		it('still surfaces a tenant-id validation error, never floors it', async () => {
			const lim = createRateLimit(downClient(), {
				points: 5, interval: 1000,
				tenant: () => 'a\0b',
				localFloorOnStorageFailure: true
			});
			await expect(lim.consume(mockWs({ ip: '1.2.3.4' }))).rejects.toThrow('NUL');
		});

		it('leaves admin ops rejecting while the store is down', async () => {
			const lim = createRateLimit(downClient(), { points: 5, interval: 1000, localFloorOnStorageFailure: true });
			await expect(lim.ban('1.2.3.4')).rejects.toThrow('redis down');
		});
	});
});
