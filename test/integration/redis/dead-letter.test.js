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
import { monotonicNow } from '../../../src/shared/runtime.js';
import { createDeadLetter } from '../../../src/redis/dead-letter.js';
import { createMetrics } from '../../../src/prometheus/index.js';

const byAuthor = (record) => (record && record.data && record.data.author) || null;

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
		const dlq = createDeadLetter(client, { ttlMs: 600, max: 2 });
		const keys = ['recs', 'order', 'seq', 'tenant', 'tenant-order', 'tenant-size', 'tenant-rank'].map((k) => client.key(`dlq:${k}:{dlq}`));
		const now = await redisNowMs(client);
		// With one tenant, an eviction and a remove each empty the rank key and
		// create it again; it keeps a backstop too.
		for (let n = 0; n < 2; n++) await dlq.add(rec({ topic: '@t/t0/orders', failedAt: now }));
		const removable = await dlq.add(rec({ topic: '@t/t0/orders', failedAt: now }));
		expect(await dlq.remove(removable)).toBe(true);

		for (const key of keys) {
			const pttl = await client.redis.pttl(key);
			expect(pttl, key).toBeGreaterThan(0);
			expect(pttl, key).toBeLessThanOrEqual(600);
		}

		// No further writes: the keys must expire on their own (server clock).
		await waitRedisMs(client, 800);
		expect(await client.redis.exists(...keys)).toBe(0);
		expect(await dlq.count()).toBe(0);
	});

	it('ttlMs: 0 keeps the keys persistent (no backstop TTL)', async () => {
		const dlq = createDeadLetter(client);
		await dlq.add(rec({ failedAt: await redisNowMs(client) }));
		expect(await client.redis.pttl(client.key('dlq:recs:{dlq}'))).toBe(-1);
		expect(await client.redis.pttl(client.key('dlq:order:{dlq}'))).toBe(-1);
	});

	it('forget tombstone drops a delivery that raced the purge (real PX, within the window)', async () => {
		const dlq = createDeadLetter(client, { forgetUserId: byAuthor, forgetTombstoneMs: 60_000 });
		const startedAt = monotonicNow();               // delivery already in flight
		expect(await dlq.purgeUser(null, 'u1')).toBe(0); // empty store; tombstone armed
		// The in-flight delivery finally exhausts retries and is captured after the
		// purge; its server-clock start precedes the purge, so it is dropped.
		const dropped = await dlq.add(rec({ data: { author: 'u1', body: 'PII' }, failedAt: await redisNowMs(client), startedAt }));
		expect(dropped).toBeNull();
		expect(await dlq.count()).toBe(0);
		// The tombstone key carries a real PX countdown.
		const pttl = await client.redis.pttl(client.key('dlq:purged:~/u1'));
		expect(pttl).toBeGreaterThan(0);
		expect(pttl).toBeLessThanOrEqual(60_000);
	});

	it('the tombstone PX-expires: a delivery past forgetTombstoneMs resurrects (documented residual)', async () => {
		const dlq = createDeadLetter(client, { forgetUserId: byAuthor, forgetTombstoneMs: 200 });
		const startedAt = monotonicNow();
		await dlq.purgeUser(null, 'u1');
		// Wait past the 200ms tombstone on Redis's own clock: the key expires, so
		// the drop-check finds nothing and the raced delivery is re-inserted. This
		// is the parity residual - a delivery outliving the window resurrects.
		await waitRedisMs(client, 400);
		expect(await client.redis.pttl(client.key('dlq:purged:~/u1'))).toBe(-2); // gone
		const resurrected = await dlq.add(rec({ data: { author: 'u1', body: 'PII' }, failedAt: await redisNowMs(client), startedAt }));
		expect(resurrected).not.toBeNull();
		expect(await dlq.count()).toBe(1);
	});

	it('scopes the tombstone by tenant on a real server', async () => {
		const dlq = createDeadLetter(client, { forgetUserId: byAuthor, forgetTombstoneMs: 60_000 });
		const startedAt = monotonicNow();
		await dlq.purgeUser('acme', 'u1');
		expect(await client.redis.pttl(client.key('dlq:purged:acme/u1'))).toBeGreaterThan(0);
		const failedAt = await redisNowMs(client);
		expect(await dlq.add(rec({ topic: '@t/acme/orders', data: { author: 'u1', body: 'PII' }, failedAt, startedAt }))).toBeNull();
		expect(await dlq.add(rec({ topic: '@t/globex/orders', data: { author: 'u1', body: 'kept' }, failedAt, startedAt }))).not.toBeNull();
		expect((await dlq.list()).map(r => r.topic)).toEqual(['@t/globex/orders']);
	});

	it('evicts from the tenant holding the most records, never from one under its share', async () => {
		const metrics = createMetrics();
		const dlq = createDeadLetter(client, { max: 10, metrics });
		const failedAt = await redisNowMs(client);
		for (let n = 0; n < 5; n++) await dlq.add(rec({ topic: '@t/victim/orders', data: { n }, failedAt }));
		for (let n = 0; n < 30; n++) await dlq.add(rec({ topic: '@t/noisy/orders', data: { n }, failedAt }));

		expect((await dlq.list({ topic: '@t/victim/orders' })).map((r) => r.data.n)).toEqual([4, 3, 2, 1, 0]);
		expect((await dlq.list({ topic: '@t/noisy/orders' })).map((r) => r.data.n)).toEqual([29, 28, 27, 26, 25]);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 25');
		// Every structure agrees after the script's evictions.
		expect(await client.redis.hlen(client.key('dlq:recs:{dlq}'))).toBe(10);
		expect(await client.redis.zcard(client.key('dlq:order:{dlq}'))).toBe(10);
		expect(await client.redis.hlen(client.key('dlq:tenant:{dlq}'))).toBe(10);
		expect(await client.redis.zcard(client.key('dlq:tenant-order:{dlq}'))).toBe(10);
		expect(await client.redis.zrange(client.key('dlq:tenant-size:{dlq}'), 0, -1, 'WITHSCORES')).toEqual(['noisy', '5', 'victim', '5']);
	});

	it('keeps the tenant accounting in step with remove, purgeUser and clear', async () => {
		const dlq = createDeadLetter(client, { max: 4, forgetUserId: byAuthor });
		const failedAt = await redisNowMs(client);
		const first = await dlq.add(rec({ topic: '@t/a/orders', data: { author: 'u1' }, failedAt }));
		await dlq.add(rec({ topic: '@t/a/orders', data: { author: 'u2' }, failedAt }));
		await dlq.add(rec({ topic: '@t/b/orders', data: { author: 'u2' }, failedAt }));
		expect(await dlq.remove(first)).toBe(true);
		expect(await dlq.purgeUser('b', 'u2')).toBe(1);
		expect(await client.redis.zrange(client.key('dlq:tenant-size:{dlq}'), 0, -1, 'WITHSCORES')).toEqual(['a', '1']);
		expect(await client.redis.zcard(client.key('dlq:tenant-order:{dlq}'))).toBe(1);
		for (let n = 0; n < 4; n++) await dlq.add(rec({ topic: '@t/b/orders', data: { n }, failedAt }));
		expect(await dlq.count({ topic: '@t/a/orders' })).toBe(1);
		await dlq.clear();
		for (const key of ['seq', 'recs', 'order', 'tenant', 'tenant-order', 'tenant-size', 'tenant-rank']) {
			expect(await client.redis.exists(client.key(`dlq:${key}:{dlq}`))).toBe(0);
		}
	});

	it('breaks a tie between equally large scopes on the oldest record, whatever the tenant names', async () => {
		for (const order of [['a', 'b', 'z'], ['z', 'b', 'a']]) {
			const dlq = createDeadLetter(client, { max: 2 });
			await dlq.clear();
			const failedAt = await redisNowMs(client);
			const ids = [];
			for (const tenant of order) ids.push(await dlq.add(rec({ topic: `@t/${tenant}/x`, failedAt })));
			// The capture just made survives; the oldest record's tenant pays.
			expect(await dlq.get(ids[2])).not.toBeNull();
			expect((await dlq.list()).map((r) => r.topic).sort()).toEqual(order.slice(1).map((t) => `@t/${t}/x`).sort());
			expect(await client.redis.zcard(client.key('dlq:tenant-rank:{dlq}'))).toBe(2);
		}
	});

	it('gives records stored before tenant accounting the scope of their stored topic', async () => {
		const now = await redisNowMs(client);
		// Records an earlier version wrote: hash and order set, no tenant entries.
		const topics = ['@t/victim/orders', '@t/victim/orders', '@t/victim/orders', 'orders', 'orders', '@t/x'];
		for (const [index, topic] of topics.entries()) {
			const id = String(index + 1);
			await client.redis.hset(client.key('dlq:recs:{dlq}'), id, JSON.stringify({ ...rec({ topic, data: { n: id } }), failedAt: now - 1000 + index }));
			await client.redis.zadd(client.key('dlq:order:{dlq}'), now - 1000 + index, id);
		}
		await client.redis.set(client.key('dlq:seq:{dlq}'), '6');
		const dlq = createDeadLetter(client, { max: 6 });
		for (let n = 0; n < 3; n++) await dlq.add(rec({ topic: '@t/fresh/orders', data: { n }, failedAt: now }));
		// victim is largest, then ties with ~ and fresh at two and holds the
		// oldest record; then fresh is largest.
		expect((await dlq.list({ limit: 10 })).map((r) => r.id).sort()).toEqual(['3', '4', '5', '6', '8', '9']);
		expect(await client.redis.zrange(client.key('dlq:tenant-size:{dlq}'), 0, -1, 'WITHSCORES')).toEqual(['', '1', 'victim', '1', 'fresh', '2', '~', '2']);
		expect(await client.redis.hlen(client.key('dlq:tenant:{dlq}'))).toBe(6);
		expect(await client.redis.zcard(client.key('dlq:tenant-rank:{dlq}'))).toBe(4);
	});

	it('hides records past ttlMs from reads before any later add evicts them', async () => {
		const dlq = createDeadLetter(client, { ttlMs: 400 });
		const id = await dlq.add(rec({ topic: 'old', failedAt: await redisNowMs(client) }));
		await waitRedisMs(client, 250);
		// The second add re-arms the key backstop, so only the read cutoff
		// hides the first record once it is past ttlMs.
		const fresh = await dlq.add(rec({ topic: 'fresh', failedAt: await redisNowMs(client) }));
		await waitRedisMs(client, 250);
		expect(await dlq.get(id)).toBeNull();
		expect(await dlq.get(fresh)).not.toBeNull();
		expect(await dlq.count()).toBe(1);
		expect((await dlq.list()).map(r => r.id)).toEqual([fresh]);
		expect((await dlq.summary()).byTopic).toEqual({ fresh: 1 });
	});
});
