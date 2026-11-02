import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { ratelimitIdentityContract } from '../helpers/ratelimit-identity-contract.js';

let client;
beforeEach(() => { client = mockRedisClient('identity:'); });
ratelimitIdentityContract(() => client);

const socket = (tenantId, key) => ({ getUserData: () => ({ tenantId, key }) });
for (const refill of ['window', 'sliding', 'gcra']) {
	describe(`${refill} degraded principal identity`, () => {
		it('keeps the local floor isolated and rejects storage admin calls without mutating it', async () => {
			const down = async () => { throw new Error('storage unavailable'); };
			const redis = { get: async () => null, del: down, hset: down,
				defineCommand(name) { this[name] = down; } };
			const limiter = createRateLimit({ redis, key: value => 'identity:' + value }, {
				points: 1, interval: 60000, refill, localFloorOnStorageFailure: true,
				keyBy: ws => ws.getUserData().key, tenant: ws => ws.getUserData().tenantId
			});
			const warning = vi.spyOn(console, 'warn').mockImplementation(() => {});
			try {
				const unscoped = socket(null, 'tenant-a\0user');
				const victim = socket('tenant-a', 'user');
				expect((await limiter.consume(unscoped)).allowed).toBe(true);
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
				expect((await limiter.peek(victim)).allowed).toBe(true);
				await expect(limiter.reset('tenant-a\0user', null)).rejects.toThrow('storage unavailable');
				await expect(limiter.ban('tenant-a\0user', 60000, null)).rejects.toThrow('storage unavailable');
				await expect(limiter.unban('tenant-a\0user', null)).rejects.toThrow('storage unavailable');
				await expect(limiter.purgeUser(null, 'tenant-a\0user')).rejects.toThrow('storage unavailable');
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
				expect((await limiter.consume(victim)).allowed).toBe(true);
				expect((await limiter.peek(victim)).allowed).toBe(false);
				expect((await limiter.peek(socket('tenant-b', 'user'))).allowed).toBe(true);
			} finally { warning.mockRestore(); }
		});
	});
}
