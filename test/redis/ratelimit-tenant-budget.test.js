import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import { WS_ATTRIBUTION } from 'svelte-adapter-uws/testing';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';

const socket = (tenantId, key = 'one') => ({ getUserData: () => ({ tenantId, key, ip: key }) });
const attributed = (tenantId, key = 'one') => ({
	getUserData: () => ({ ip: key, [WS_ATTRIBUTION]: Object.freeze({ tenantId }) })
});
const tenant = (ws) => ws.getUserData().tenantId;
const options = { points: 3, interval: 60000, budget: 'tenant', tenant };

beforeEach(() => {
	setRuntimeEnv({ clock: { now: () => 120000, monotonic: () => 120000, wallEpoch: () => 120000 } });
});
afterEach(() => { vi.restoreAllMocks(); resetRuntimeEnv(); });

for (const refill of ['window', 'sliding', 'gcra']) {
	describe(`${refill} tenant allowance`, () => {
		it('shares weighted costs across principals and limiter instances, keeping other tenants independent', async () => {
			const client = mockRedisClient();
			const a = createRateLimit(client, { ...options, refill });
			const b = createRateLimit(client, { ...options, refill });
			expect((await a.consume(socket('alpha', 'first'), 2)).allowed).toBe(true);
			expect((await b.consume(socket('alpha', 'second'), 2)).allowed).toBe(false);
			expect((await b.consume(socket('alpha', 'third'), 1)).allowed).toBe(true);
			expect((await a.consume(socket('alpha', 'fourth'))).allowed).toBe(false);
			expect((await b.consume(socket('beta', 'first'), 3)).allowed).toBe(true);
		});

		it('peeks at the same shared bucket without spending or calling keyBy', async () => {
			const keyBy = () => { throw new Error('principal key must not run'); };
			const limiter = createRateLimit(mockRedisClient(), { ...options, refill, keyBy });
			expect((await limiter.peek(socket('alpha'), 3)).allowed).toBe(true);
			expect((await limiter.consume(socket('alpha'), 2)).allowed).toBe(true);
			expect((await limiter.peek(socket('alpha', 'other'), 2)).allowed).toBe(false);
			expect((await limiter.peek(socket('alpha', 'other'), 1)).allowed).toBe(true);
			expect((await limiter.consume(socket('alpha', 'other'), 1)).allowed).toBe(true);
		});

		it('admin operations select only the tenant aggregate and never coerce a principal key', async () => {
			const limiter = createRateLimit(mockRedisClient(), { ...options, refill });
			const unread = { toString() { throw new Error('unused key'); } };
			await limiter.ban(unread, 60000, 'alpha');
			expect((await limiter.consume(socket('alpha', 'x'))).allowed).toBe(false);
			expect((await limiter.consume(socket('beta', 'x'))).allowed).toBe(true);
			await limiter.unban(unread, 'alpha');
			expect((await limiter.consume(socket('alpha', 'y'))).allowed).toBe(true);
			await limiter.reset(unread, 'alpha');
			expect((await limiter.consume(socket('alpha', 'z'), 3)).allowed).toBe(true);
			await limiter.clear('alpha');
			expect((await limiter.consume(socket('alpha', 'z'), 3)).allowed).toBe(true);
			expect((await limiter.peek(socket('beta'), 3)).allowed).toBe(false);
		});

		it('keeps aggregate storage and clear separate from legacy principal buckets and emergency settings', async () => {
			const client = mockRedisClient();
			const total = createRateLimit(client, { ...options, refill });
			const principal = createRateLimit(client, { points: 3, interval: 60000, refill });
			await client.redis.set(client.key('v1:ratelimit:emergency'), '1');
			// A legacy bare principal key may equal a tenant identifier.
			await principal.consume(socket(undefined, 'alpha'), 3);
			expect((await total.consume(socket('alpha'), 3)).allowed).toBe(true);
			await total.clear('alpha');
			expect((await principal.peek(socket(undefined, 'alpha'))).allowed).toBe(false);
			await total.consume(socket('alpha'), 3);
			await principal.clear();
			expect((await total.peek(socket('alpha'))).allowed).toBe(false);
			await total.clear();
			expect(await client.redis.get(client.key('v1:ratelimit:emergency'))).toBe('1');
		});

		it('does not reset the shared allowance when erasing one principal', async () => {
			const limiter = createRateLimit(mockRedisClient(), { ...options, refill });
			await limiter.consume(socket('alpha', 'user-a'), 3);
			expect(await limiter.purgeUser('alpha', 'user-a')).toBe(0);
			expect((await limiter.consume(socket('alpha', 'user-b'))).allowed).toBe(false);
		});

		it('shares the configured local floor while storage is unavailable', async () => {
			const client = mockRedisClient();
			client.redis.eval = async () => { throw new Error('offline'); };
			client.redis.evalsha = async () => { throw new Error('offline'); };
			const limiter = createRateLimit(client, { ...options, refill, localFloorOnStorageFailure: true });
			expect((await limiter.consume(socket('alpha', 'first'), 3)).allowed).toBe(true);
			expect((await limiter.peek(socket('alpha', 'second'))).allowed).toBe(false);
			expect((await limiter.consume(socket('alpha', 'second'))).allowed).toBe(false);
			expect((await limiter.consume(socket('beta', 'first'))).allowed).toBe(true);
		});
	});
}

describe('tenant budget identity', () => {
	it('uses the adapter attribution when no resolver is configured', async () => {
		const limiter = createRateLimit(mockRedisClient(), { points: 1, interval: 60000, budget: 'tenant' });
		expect((await limiter.consume(attributed('alpha', 'one'))).allowed).toBe(true);
		expect((await limiter.consume(attributed('alpha', 'two'))).allowed).toBe(false);
		expect((await limiter.consume(attributed('beta', 'one'))).allowed).toBe(true);
	});

	it('explicit tenant resolution overrides attribution including an explicit unscoped answer', async () => {
		const limiter = createRateLimit(mockRedisClient(), { points: 1, interval: 60000, budget: 'tenant', tenant: () => 'override' });
		await limiter.consume(attributed('alpha'));
		expect((await limiter.consume(attributed('beta'))).allowed).toBe(false);
		const unscoped = createRateLimit(mockRedisClient(), { points: 1, interval: 60000, budget: 'tenant', tenant: () => null });
		await expect(unscoped.consume(attributed('alpha'))).rejects.toThrow(/tenant id/);
	});

	it('defaults to principal budgets while deriving the namespace from adapter attribution', async () => {
		const limiter = createRateLimit(mockRedisClient(), { points: 1, interval: 60000 });
		expect((await limiter.consume(attributed('alpha', 'one'))).allowed).toBe(true);
		expect((await limiter.consume(attributed('alpha', 'two'))).allowed).toBe(true);
		expect((await limiter.consume(attributed('beta', 'one'))).allowed).toBe(true);
		expect((await limiter.consume(attributed('alpha', 'one'))).allowed).toBe(false);
	});

	it('validates admin identities before taking a breaker admission', async () => {
		const breaker = { guard: vi.fn(), failure: vi.fn(), success: vi.fn() };
		const limiter = createRateLimit(mockRedisClient(), { ...options, breaker });
		await expect(limiter.reset('unused')).rejects.toThrow(/tenant id/);
		await expect(limiter.unban('unused')).rejects.toThrow(/tenant id/);
		await expect(limiter.clear('')).rejects.toThrow(/tenant id/);
		expect(breaker.guard).not.toHaveBeenCalled();
		expect(breaker.failure).not.toHaveBeenCalled();
	});

	it('clears a literal glob-like tenant id without touching another tenant', async () => {
		const limiter = createRateLimit(mockRedisClient(), options);
		await limiter.consume(socket('*'), 3);
		await limiter.consume(socket('other'), 3);
		await limiter.clear('*');
		expect((await limiter.consume(socket('*'), 3)).allowed).toBe(true);
		expect((await limiter.peek(socket('other'))).allowed).toBe(false);
	});

	for (const id of [null, 'alpha']) {
		it(`reads immutable default attribution once per connection with tenant=${id}`, async () => {
			const ws = attributed(id);
			const get = vi.spyOn(ws, 'getUserData');
			const limiter = createRateLimit(mockRedisClient(), { points: 1, interval: 60000, keyBy: () => 'same' });
			expect((await limiter.consume(ws)).allowed).toBe(true);
			expect((await limiter.consume(ws)).allowed).toBe(false);
			expect((await limiter.peek(ws)).allowed).toBe(false);
			expect(get).toHaveBeenCalledOnce();
		});
	}

	it('keeps explicit tenant resolvers dynamic instead of caching their answers', async () => {
		const resolve = vi.fn().mockReturnValueOnce('alpha').mockReturnValueOnce('beta').mockReturnValueOnce('alpha');
		const limiter = createRateLimit(mockRedisClient(), { points: 1, interval: 60000, budget: 'tenant', tenant: resolve });
		const ws = attributed('ignored');
		expect((await limiter.consume(ws)).allowed).toBe(true);
		expect((await limiter.consume(ws)).allowed).toBe(true);
		expect((await limiter.consume(ws)).allowed).toBe(false);
		expect(resolve).toHaveBeenCalledTimes(3);
	});

	it('refuses invalid budget values and missing or ambiguous tenant identifiers before Redis or fallback', async () => {
		const client = mockRedisClient();
		for (const budget of [null, 'global', '', false]) {
			expect(() => createRateLimit(client, { ...options, budget })).toThrow(/budget/);
		}
		const evalSpy = vi.spyOn(client.redis, 'eval');
		const limiter = createRateLimit(client, { ...options, localFloorOnStorageFailure: true });
		for (const id of [undefined, null, '', 0, 1, {}, 'a\0b']) {
			await expect(limiter.consume(socket(id))).rejects.toThrow(/tenant id/);
			await expect(limiter.peek(socket(id))).rejects.toThrow(/tenant id/);
			await expect(limiter.reset('unused', id)).rejects.toThrow(/tenant id/);
			await expect(limiter.ban('unused', 1000, id)).rejects.toThrow(/tenant id/);
			await expect(limiter.unban('unused', id)).rejects.toThrow(/tenant id/);
		}
		expect(evalSpy).not.toHaveBeenCalled();
	});
});
