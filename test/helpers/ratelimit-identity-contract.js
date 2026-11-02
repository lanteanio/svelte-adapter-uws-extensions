import { describe, expect, it } from 'vitest';
import { createRateLimit } from '../../src/redis/ratelimit.js';

const socket = (tenantId, key) => ({ getUserData: () => ({ tenantId, key }) });
const options = {
	points: 2, interval: 60000,
	keyBy: ws => ws.getUserData().key,
	tenant: ws => ws.getUserData().tenantId
};
const joined = 'tenant-a\0user';

export function ratelimitIdentityContract(getClient) {
	for (const refill of ['window', 'sliding', 'gcra']) {
		describe(`${refill} principal tuple identity`, () => {
			it('separates scoped, unscoped and delimiter-bearing principals across instances', async () => {
				const client = getClient();
				const a = createRateLimit(client, { ...options, refill });
				const b = createRateLimit(client, { ...options, refill });
				const victim = socket('tenant-a', 'user');
				const unscoped = socket(null, joined);
				expect((await a.consume(unscoped, 2)).allowed).toBe(true);
				expect((await b.peek(unscoped)).allowed).toBe(false);
				expect((await b.consume(victim, 2)).allowed).toBe(true);
				expect((await a.peek(victim)).allowed).toBe(false);
				expect((await a.consume(socket('tenant-b', 'user'), 2)).allowed).toBe(true);
				expect((await a.consume(socket(null, 'user'), 2)).allowed).toBe(true);
			});

			it('reset and erasure select the exact scope without restoring another allowance', async () => {
				const limiter = createRateLimit(getClient(), { ...options, refill });
				const victim = socket('tenant-a', 'user');
				const unscoped = socket(null, joined);
				await limiter.consume(victim, 2);
				await limiter.consume(unscoped, 2);
				await limiter.reset(joined, null);
				expect((await limiter.consume(unscoped, 2)).allowed).toBe(true);
				expect((await limiter.peek(victim)).allowed).toBe(false);
				expect(await limiter.purgeUser(null, joined)).toBe(1);
				expect((await limiter.peek(unscoped)).allowed).toBe(true);
				expect((await limiter.peek(victim)).allowed).toBe(false);
				await limiter.consume(unscoped, 2);
				expect(await limiter.purgeUser('tenant-a', 'user')).toBe(1);
				expect((await limiter.peek(victim)).allowed).toBe(true);
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
			});

			it('ban and unban cannot cross the null/tenant boundary', async () => {
				const limiter = createRateLimit(getClient(), { ...options, refill });
				const victim = socket('tenant-a', 'user');
				const unscoped = socket(null, joined);
				await limiter.ban(joined, 60000, null);
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
				expect((await limiter.peek(victim)).allowed).toBe(true);
				await limiter.ban('user', 60000, 'tenant-a');
				await limiter.unban(joined, null);
				expect((await limiter.peek(unscoped)).allowed).toBe(true);
				expect((await limiter.peek(victim)).allowed).toBe(false);
				await limiter.unban('user', 'tenant-a');
				expect((await limiter.peek(victim)).allowed).toBe(true);
			});

			it('tenant clear preserves unscoped principals and escapes JSON quoting', async () => {
				const limiter = createRateLimit(getClient(), { ...options, refill });
				const victim = socket('tenant-a', 'user');
				const unscoped = socket(null, joined);
				const quoted = socket('tenant-"quoted"', 'user');
				for (const ws of [victim, unscoped, quoted]) await limiter.consume(ws, 2);
				await limiter.clear('tenant-a');
				expect((await limiter.peek(victim)).allowed).toBe(true);
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
				expect((await limiter.peek(quoted)).allowed).toBe(false);
				await limiter.clear('tenant-"quoted"');
				expect((await limiter.peek(quoted)).allowed).toBe(true);
				expect((await limiter.peek(unscoped)).allowed).toBe(false);
				await limiter.clear();
				expect((await limiter.peek(unscoped)).allowed).toBe(true);
			});

			it('keeps arbitrary custom strings distinct on the UTF-8 backend', async () => {
				const limiter = createRateLimit(getClient(), { ...options, refill });
				const keys = [joined, '\0leading', '[null,"user"]', String.fromCharCode(0xd800), String.fromCodePoint(0xfffd)];
				for (const key of keys) expect((await limiter.consume(socket(null, key), 2)).allowed).toBe(true);
				for (const key of keys) expect((await limiter.peek(socket(null, key))).allowed).toBe(false);
			});

			it('does not read or repurpose ambiguous retired buckets', async () => {
				const client = getClient();
				const infix = { window: 'ratelimit', sliding: 'ratelimits', gcra: 'ratelimitg' }[refill];
				const legacy = client.key('v1:' + infix + ':' + joined);
				await client.redis.hset(legacy, 'points', 0, 'bannedUntil', 9007199254740991);
				const limiter = createRateLimit(client, { ...options, refill });
				expect((await limiter.peek(socket('tenant-a', 'user'))).allowed).toBe(true);
				expect((await limiter.peek(socket(null, joined))).allowed).toBe(true);
				await limiter.consume(socket('tenant-a', 'user'), 2);
				await limiter.clear();
				expect(await client.redis.hget(legacy, 'bannedUntil')).toBe('9007199254740991');
			});
		});
	}
}
