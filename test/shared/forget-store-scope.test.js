import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { topicInTenant } from '../../src/shared/tenant-topic.js';

describe('forget-store tenant scoping', () => {
	it('refuses a tenant id that would change the erasure scope', async () => {
		const forget = createForgetStore([], { redis: mockRedisClient('').redis });
		// Topic scoping is a raw prefix match, so a hierarchical id would let
		// tenant 'a' reach '@t/a/b/...'; '' and non-strings would silently
		// select the untenanted scope instead of erroring.
		for (const bad of ['a/b', '', 'a\0b', 'a*b', 42, 'x'.repeat(65)]) {
			await expect(forget.purgeUser(bad, 'u1')).rejects.toThrow('tenant id');
		}
	});

	it('accepts the tenant-id shapes real deployments use', async () => {
		const client = mockRedisClient('');
		const redis = client.redis;
		// A domain and a namespaced id are ordinary tenant ids, and a thrown
		// erasure is a poor answer to one. Neither can break the `@t/<id>/`
		// prefix match the scope is decided by.
		for (const ok of ['acme.com', 'acme:eu', 'acme-eu_1', '12345', 'a'.repeat(64)]) {
			await redis.hset(`__live-room-owner:@t/${ok}/room1`, 'o', 'user-1', 'j:user-1', '1', 'n:user-1', '1');
			const forget = createForgetStore([], { redis });
			const res = await forget.purgeUser(ok, 'user-1');
			expect(res.rowsAffected.roomOwners, ok).toBe(1);
		}
	});

	it('keeps the prefix match unambiguous for the widened charset', async () => {
		// The reason `/` stays banned: with it, tenant 'a' and tenant 'a/b'
		// would both match '@t/a/b/x'. The characters that were added cannot
		// produce that, because none of them is the separator.
		expect(topicInTenant('a', '@t/ab/x')).toBe(false);
		expect(topicInTenant('a', '@t/a/b/x')).toBe(true);   // genuinely a's subtopic
		expect(topicInTenant('acme.com', '@t/acme.com/x')).toBe(true);
		expect(topicInTenant('acme.com', '@t/acme.community/x')).toBe(false);
		expect(topicInTenant('acme', '@t/acme.com/x')).toBe(false);
		expect(topicInTenant('acme:eu', '@t/acme:eu/x')).toBe(true);
		expect(topicInTenant('acme:eu', '@t/acme:eu2/x')).toBe(false);
	});

	it('erases within the validated tenant', async () => {
		const client = mockRedisClient('');
		const redis = client.redis;
		await redis.hset('__live-room-owner:@t/a/room1', 'o', 'user-1', 'j:user-1', '1', 'n:user-1', '1');
		const forget = createForgetStore([], { redis });

		const res = await forget.purgeUser('a', 'user-1');
		expect(res.rowsAffected.roomOwners).toBe(1);
		expect((await redis.hgetall('__live-room-owner:@t/a/room1')).o).toBeUndefined();
	});

	it('accepts the explicit untenanted scope', async () => {
		const client = mockRedisClient('');
		const redis = client.redis;
		await redis.hset('__live-room-owner:room1', 'o', 'user-1', 'j:user-1', '1', 'n:user-1', '1');
		const forget = createForgetStore([], { redis });
		const res = await forget.purgeUser(null, 'user-1');
		expect(res.rowsAffected.roomOwners).toBe(1);
	});
});

describe('forget-store purge fan-out', () => {
	it('bounds in-flight Redis commands while still covering every room', async () => {
		const client = mockRedisClient('');
		const redis = client.redis;
		const ROOMS = 200;
		for (let i = 0; i < ROOMS; i++) {
			await redis.hset(`__live-room-owner:@t/a/room${i}`, 'o', 'user-1', 'j:user-1', '1', 'n:user-1', '1');
		}

		// Count commands actually in flight. Asserting only that the purge
		// completes would pass with an unbounded Promise.allSettled and with
		// no worker pool at all, which is to say it would not test the fix.
		let inFlight = 0;
		let peak = 0;
		const realEval = redis.eval.bind(redis);
		redis.eval = async (...args) => {
			inFlight++;
			peak = Math.max(peak, inFlight);
			try {
				await new Promise((r) => setTimeout(r, 1));
				return await realEval(...args);
			} finally {
				inFlight--;
			}
		};

		const forget = createForgetStore([], { redis });
		const res = await forget.purgeUser('a', 'user-1');

		expect(res.rowsAffected.roomOwners).toBe(ROOMS);
		expect(res.ownerSuccessions).toHaveLength(ROOMS);
		expect(peak).toBeGreaterThan(1);      // still concurrent
		expect(peak).toBeLessThanOrEqual(16); // but bulkheaded
		for (let i = 0; i < ROOMS; i++) {
			expect((await redis.hgetall(`__live-room-owner:@t/a/room${i}`)).o).toBeUndefined();
		}
		redis.eval = realEval;
	});

	it('keeps per-room failure isolation', async () => {
		const client = mockRedisClient('');
		const redis = client.redis;
		for (let i = 0; i < 5; i++) {
			await redis.hset(`__live-room-owner:@t/a/room${i}`, 'o', 'user-1', 'j:user-1', '1', 'n:user-1', '1');
		}
		const realEval = redis.eval.bind(redis);
		redis.eval = async (...args) => {
			if (String(args[2]).includes('room3')) throw new Error('room3 exploded');
			return realEval(...args);
		};

		const forget = createForgetStore([], { redis });
		// An incomplete erasure must be reported, not quietly partial: the
		// caller has to retry to satisfy the request.
		await expect(forget.purgeUser('a', 'user-1')).rejects.toThrow(/incomplete/);
		// But the worker pool still processed the other rooms rather than
		// abandoning the batch at the first rejection.
		redis.eval = realEval;
		for (const i of [0, 1, 2, 4]) {
			expect((await redis.hgetall(`__live-room-owner:@t/a/room${i}`)).o).toBeUndefined();
		}
	});
});
