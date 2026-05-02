import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createDistributedSession } from '../../redis/session.js';
import { createMetrics } from '../../prometheus/index.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

describe('redis distributed session', () => {
	let client;

	beforeEach(() => {
		client = mockRedisClient('app:');
	});

	describe('construction', () => {
		it('requires a redis client', () => {
			expect(() => createDistributedSession(null)).toThrow(/client/);
			expect(() => createDistributedSession({})).toThrow(/client/);
		});

		it('rejects invalid ttlMs', () => {
			expect(() => createDistributedSession(client, { ttlMs: 0 })).toThrow(/ttlMs/);
			expect(() => createDistributedSession(client, { ttlMs: -1 })).toThrow(/ttlMs/);
			expect(() => createDistributedSession(client, { ttlMs: 'soon' })).toThrow(/ttlMs/);
		});

		it('returns the public API shape', () => {
			const s = createDistributedSession(client);
			expect(typeof s.get).toBe('function');
			expect(typeof s.set).toBe('function');
			expect(typeof s.touch).toBe('function');
			expect(typeof s.delete).toBe('function');
			expect(typeof s.clear).toBe('function');
		});
	});

	describe('set / get round trip', () => {
		it('stores and retrieves session data', async () => {
			const s = createDistributedSession(client);
			await s.set('token-abc', { userId: 42, role: 'admin' });
			expect(await s.get('token-abc')).toEqual({ userId: 42, role: 'admin' });
		});

		it('round-trips arrays, nested objects, and primitives', async () => {
			const s = createDistributedSession(client);
			const data = {
				userId: 42,
				roles: ['admin', 'editor'],
				preferences: { theme: 'dark', timezone: 'Europe/Berlin' },
				active: true,
				balance: 100.5
			};
			await s.set('token', data);
			expect(await s.get('token')).toEqual(data);
		});

		it('returns null for a missing token', async () => {
			const s = createDistributedSession(client);
			expect(await s.get('absent')).toBe(null);
		});

		it('returns null for a corrupt (non-JSON) entry', async () => {
			const s = createDistributedSession(client);
			// Plant a malformed value directly.
			await client.redis.set(client.key('sess:corrupt'), 'not-json');
			expect(await s.get('corrupt')).toBe(null);
		});

		it('uses the configured keyPrefix', async () => {
			const s = createDistributedSession(client, { keyPrefix: 'auth:' });
			await s.set('token', { hello: 'world' });
			expect(await client.redis.get(client.key('auth:token'))).toContain('"hello":"world"');
			expect(await client.redis.get(client.key('sess:token'))).toBe(null);
		});

		it('replaces existing data on set (does not merge)', async () => {
			const s = createDistributedSession(client);
			await s.set('token', { userId: 1, name: 'old' });
			await s.set('token', { userId: 2 });
			expect(await s.get('token')).toEqual({ userId: 2 });
		});
	});

	describe('TTL behavior', () => {
		it('writes via SET PX so the TTL is included in the same round-trip', async () => {
			let setCall = null;
			const realSet = client.redis.set.bind(client.redis);
			client.redis.set = async (...args) => {
				setCall = args;
				return realSet(...args);
			};
			const s = createDistributedSession(client, { ttlMs: 60000 });
			await s.set('token', { hi: true });
			expect(setCall[0]).toBe(client.key('sess:token'));
			expect(setCall.slice(2)).toContain('PX');
			expect(setCall[setCall.indexOf('PX') + 1]).toBe(60000);
		});

		it('refreshes the TTL on get when refreshOnGet is true (default)', async () => {
			let pexpireCalls = 0;
			const realPex = client.redis.pexpire.bind(client.redis);
			client.redis.pexpire = async (...args) => { pexpireCalls++; return realPex(...args); };

			const s = createDistributedSession(client, { ttlMs: 60000 });
			await s.set('token', { x: 1 });
			pexpireCalls = 0; // reset after the set's own work
			await s.get('token');
			expect(pexpireCalls).toBe(1);
		});

		it('does not refresh on get when refreshOnGet is false', async () => {
			let pexpireCalls = 0;
			const realPex = client.redis.pexpire.bind(client.redis);
			client.redis.pexpire = async (...args) => { pexpireCalls++; return realPex(...args); };

			const s = createDistributedSession(client, { ttlMs: 60000, refreshOnGet: false });
			await s.set('token', { x: 1 });
			pexpireCalls = 0;
			await s.get('token');
			expect(pexpireCalls).toBe(0);
		});

		it('does not call pexpire on a get miss', async () => {
			let pexpireCalls = 0;
			const realPex = client.redis.pexpire.bind(client.redis);
			client.redis.pexpire = async (...args) => { pexpireCalls++; return realPex(...args); };

			const s = createDistributedSession(client);
			await s.get('absent');
			expect(pexpireCalls).toBe(0);
		});
	});

	describe('touch', () => {
		it('returns true when the session is present', async () => {
			const s = createDistributedSession(client);
			await s.set('token', { x: 1 });
			expect(await s.touch('token')).toBe(true);
		});

		it('returns false when the session is missing', async () => {
			const s = createDistributedSession(client);
			expect(await s.touch('absent')).toBe(false);
		});

		it('does not read or modify the session data', async () => {
			const s = createDistributedSession(client);
			await s.set('token', { x: 1 });
			await s.touch('token');
			expect(await s.get('token')).toEqual({ x: 1 });
		});
	});

	describe('delete', () => {
		it('returns true when the session existed', async () => {
			const s = createDistributedSession(client);
			await s.set('token', { x: 1 });
			expect(await s.delete('token')).toBe(true);
			expect(await s.get('token')).toBe(null);
		});

		it('returns false when the session did not exist', async () => {
			const s = createDistributedSession(client);
			expect(await s.delete('absent')).toBe(false);
		});
	});

	describe('clear', () => {
		it('removes every session under the keyPrefix', async () => {
			const s = createDistributedSession(client);
			await s.set('a', { x: 1 });
			await s.set('b', { x: 2 });
			await s.set('c', { x: 3 });
			await s.clear();
			expect(await s.get('a')).toBe(null);
			expect(await s.get('b')).toBe(null);
			expect(await s.get('c')).toBe(null);
		});

		it('does not touch keys outside the prefix', async () => {
			const s = createDistributedSession(client, { keyPrefix: 'sess:' });
			await s.set('a', { x: 1 });
			await client.redis.set(client.key('cache:other'), 'preserved');

			await s.clear();
			expect(await client.redis.get(client.key('cache:other'))).toBe('preserved');
		});

		it('is a no-op when the store is empty', async () => {
			const s = createDistributedSession(client);
			await expect(s.clear()).resolves.toBeUndefined();
		});
	});

	describe('validation', () => {
		it('rejects empty / non-string tokens', async () => {
			const s = createDistributedSession(client);
			await expect(s.get('')).rejects.toThrow(/token/);
			await expect(s.set('', {})).rejects.toThrow(/token/);
			await expect(s.touch(null)).rejects.toThrow(/token/);
			await expect(s.delete(undefined)).rejects.toThrow(/token/);
		});
	});

	describe('metrics', () => {
		it('counts session_get_total{result="hit|miss"}', async () => {
			const metrics = createMetrics();
			const s = createDistributedSession(client, { metrics });
			await s.set('token', { x: 1 });
			await s.get('token');
			await s.get('token');
			await s.get('absent');

			const out = await metrics.serialize();
			expect(out).toMatch(/session_get_total\{result="hit"\}\s+2/);
			expect(out).toMatch(/session_get_total\{result="miss"\}\s+1/);
		});

		it('counts session_set_total per set call', async () => {
			const metrics = createMetrics();
			const s = createDistributedSession(client, { metrics });
			await s.set('a', { x: 1 });
			await s.set('b', { x: 2 });
			await s.set('a', { x: 3 });

			const out = await metrics.serialize();
			expect(out).toMatch(/session_set_total\s+3/);
		});

		it('counts session_delete_total{result="present|absent"}', async () => {
			const metrics = createMetrics();
			const s = createDistributedSession(client, { metrics });
			await s.set('token', { x: 1 });
			await s.delete('token');
			await s.delete('token');

			const out = await metrics.serialize();
			expect(out).toMatch(/session_delete_total\{result="present"\}\s+1/);
			expect(out).toMatch(/session_delete_total\{result="absent"\}\s+1/);
		});

		it('counts session_touch_total{result="present|absent"}', async () => {
			const metrics = createMetrics();
			const s = createDistributedSession(client, { metrics });
			await s.set('token', { x: 1 });
			await s.touch('token');
			await s.touch('absent');

			const out = await metrics.serialize();
			expect(out).toMatch(/session_touch_total\{result="present"\}\s+1/);
			expect(out).toMatch(/session_touch_total\{result="absent"\}\s+1/);
		});
	});

	describe('breaker integration', () => {
		it('marks failures on Redis errors and trips the breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });
			const s = createDistributedSession(client, { breaker });

			const realSet = client.redis.set.bind(client.redis);
			client.redis.set = async () => { throw new Error('redis down'); };
			await expect(s.set('token', {})).rejects.toThrow(/redis down/);
			await expect(s.set('token', {})).rejects.toThrow(/redis down/);
			expect(breaker.state).toBe('broken');

			client.redis.set = realSet;
		});

		it('marks success on round-trip set then get', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });
			const s = createDistributedSession(client, { breaker });
			await s.set('token', { x: 1 });
			await s.get('token');
			expect(breaker.state).toBe('healthy');
		});
	});
});
