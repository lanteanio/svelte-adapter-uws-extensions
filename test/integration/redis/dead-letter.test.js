/**
 * Integration tests for redis/dead-letter against a real Redis/Valkey server.
 *
 * The mock-based suite at test/redis/dead-letter.test.js covers the surface;
 * this file pins what only a real server can prove: the MULTI-paired hash+zset
 * mutations stay in agreement after every operation (ZCARD is the authoritative
 * count), retention clocks off the server's TIME command - a skewed producer
 * stamp can neither mass-evict healthy records nor pin its own - and the
 * PEXPIRE backstop actually lands, so an idle queue self-cleans within ttlMs.
 * The Valkey tiers re-run this file on a real Valkey server.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { redisNowMs, waitRedisMs } from '../helpers/backend-clock.js';
import { createDeadLetter } from '../../../src/redis/dead-letter.js';

const rec = (over = {}) => ({
	webhookId: 'w1',
	topic: 'orders',
	event: 'created',
	data: { n: 1 },
	attempts: 3,
	error: 'boom',
	...over
});

describe('redis dead-letter (integration)', () => {
	let client;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-dlq:' });
	});

	beforeEach(async () => {
		await resetBackendKeys(client);
	});

	afterAll(async () => {
		await client.quit();
	});

	it('add/remove keep the hash and the order set in agreement', async () => {
		const dlq = createDeadLetter(client);
		const now = await redisNowMs(client);
		const id1 = await dlq.add(rec({ failedAt: now - 1_000 }));
		const id2 = await dlq.add(rec({ failedAt: now - 500 }));

		expect(await client.redis.hlen(client.key('dlq:recs:{dlq}'))).toBe(2);
		expect(await client.redis.zcard(client.key('dlq:order:{dlq}'))).toBe(2);

		expect(await dlq.remove(id1)).toBe(true);
		expect(await dlq.remove(id1)).toBe(false); // already gone
		expect(await client.redis.hlen(client.key('dlq:recs:{dlq}'))).toBe(1);
		expect(await client.redis.zcard(client.key('dlq:order:{dlq}'))).toBe(1);
		expect((await dlq.list()).map((r) => r.id)).toEqual([id2]);
	});

	it('clamps an implausible failedAt to the server clock', async () => {
		const dlq = createDeadLetter(client);
		const before = await redisNowMs(client);
		const id = await dlq.add(rec({ failedAt: before + 3_600_000 }));
		const after = await redisNowMs(client);

		const got = await dlq.get(id);
		expect(got.failedAt).toBeGreaterThanOrEqual(Math.floor(before));
		expect(got.failedAt).toBeLessThanOrEqual(Math.ceil(after));
	});

	it('evicts on the server clock: stale records swept, a future stamp cannot mass-evict', async () => {
		const dlq = createDeadLetter(client, { ttlMs: 60_000 });
		const now = await redisNowMs(client);
		await dlq.add(rec({ webhookId: 'stale', failedAt: now - 120_000 }));
		await dlq.add(rec({ webhookId: 'healthy', failedAt: now - 1_000 }));
		// A producer stamping an hour ahead: under a stamp-clocked cutoff this
		// would sweep everything older than future-ttl, i.e. the healthy record.
		await dlq.add(rec({ webhookId: 'skewed', failedAt: now + 3_600_000 }));

		const kept = (await dlq.list()).map((r) => r.webhookId).sort();
		expect(kept).toEqual(['healthy', 'skewed']);
		expect(await dlq.count()).toBe(2);
	});

	it('arms a PEXPIRE backstop so an idle queue self-cleans within ttlMs', async () => {
		const dlq = createDeadLetter(client, { ttlMs: 600 });
		await dlq.add(rec({ failedAt: await redisNowMs(client) }));

		for (const k of ['dlq:recs:{dlq}', 'dlq:order:{dlq}', 'dlq:seq:{dlq}']) {
			const pttl = await client.redis.pttl(client.key(k));
			expect(pttl).toBeGreaterThan(0);
			expect(pttl).toBeLessThanOrEqual(600);
		}

		// No further writes: the keys must expire on their own (server clock).
		await waitRedisMs(client, 800);
		expect(await client.redis.exists(
			client.key('dlq:recs:{dlq}'),
			client.key('dlq:order:{dlq}'),
			client.key('dlq:seq:{dlq}')
		)).toBe(0);
		expect(await dlq.count()).toBe(0);
	});

	it('ttlMs: 0 keeps the keys persistent (no backstop TTL)', async () => {
		const dlq = createDeadLetter(client);
		await dlq.add(rec({ failedAt: await redisNowMs(client) }));
		expect(await client.redis.pttl(client.key('dlq:recs:{dlq}'))).toBe(-1);
		expect(await client.redis.pttl(client.key('dlq:order:{dlq}'))).toBe(-1);
	});
});
