import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createDeadLetter } from '../../src/redis/dead-letter.js';

const rec = (over = {}) => ({
	webhookId: 'w1',
	topic: 'orders',
	event: 'created',
	data: { n: 1 },
	attempts: 3,
	error: 'boom',
	failedAt: 100,
	...over
});

describe('redis dead-letter store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = mockRedisClient('test:');
		store = createDeadLetter(client);
	});

	describe('createDeadLetter', () => {
		it('validates max and ttlMs', () => {
			expect(() => createDeadLetter(client, { max: 0 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { max: 1.5 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { ttlMs: -1 })).toThrow('non-negative integer');
		});
		it('works with no options and exposes the async interface', () => {
			const s = createDeadLetter(client);
			for (const fn of ['add', 'get', 'remove', 'count', 'list', 'summary', 'clear']) {
				expect(typeof s[fn]).toBe('function');
			}
		});
	});

	it('adds and retrieves a record (id merged in, data preserved)', async () => {
		const id = await store.add(rec({ data: { n: 7 } }));
		expect(typeof id).toBe('string');
		const got = await store.get(id);
		expect(got).toMatchObject({ id, webhookId: 'w1', topic: 'orders', event: 'created' });
		expect(got.data).toEqual({ n: 7 });
		expect(await store.count()).toBe(1);
	});

	it('lists newest-first and filters by topic', async () => {
		await store.add(rec({ topic: 'a', failedAt: 10 }));
		await store.add(rec({ topic: 'a', failedAt: 20 }));
		await store.add(rec({ topic: 'b', failedAt: 30 }));
		const all = await store.list();
		expect(all.map((r) => r.failedAt)).toEqual([30, 20, 10]); // newest first
		expect(await store.count({ topic: 'a' })).toBe(2);
		expect((await store.list({ topic: 'b' }))).toHaveLength(1);
		expect((await store.list({ limit: 1 }))).toHaveLength(1);
	});

	it('removes a record', async () => {
		const id = await store.add(rec());
		expect(await store.remove(id)).toBe(true);
		expect(await store.get(id)).toBeNull();
		expect(await store.count()).toBe(0);
		expect(await store.remove('nope')).toBe(false);
	});

	it('summarizes total / byTopic / oldest / newest', async () => {
		await store.add(rec({ topic: 'a', failedAt: 10 }));
		await store.add(rec({ topic: 'a', failedAt: 20 }));
		await store.add(rec({ topic: 'b', failedAt: 30 }));
		const sum = await store.summary();
		expect(sum.total).toBe(3);
		expect(sum.byTopic).toEqual({ a: 2, b: 1 });
		expect(sum.oldest).toBe(10);
		expect(sum.newest).toBe(30);
	});

	it('evicts the oldest beyond max', async () => {
		store = createDeadLetter(client, { max: 2 });
		await store.add(rec({ failedAt: 1 }));
		await store.add(rec({ failedAt: 2 }));
		await store.add(rec({ failedAt: 3 }));
		expect(await store.count()).toBe(2);
		const kept = (await store.list()).map((r) => r.failedAt);
		expect(kept).toEqual([3, 2]); // oldest (1) evicted
	});

	it('drops records older than ttlMs on write, clocked by the server', async () => {
		store = createDeadLetter(client, { ttlMs: 60_000 });
		const now = Date.now();
		await store.add(rec({ webhookId: 'stale', failedAt: now - 120_000 }));
		await store.add(rec({ webhookId: 'fresh', failedAt: now - 1_000 }));
		const kept = await store.list();
		expect(kept.map((r) => r.webhookId)).toEqual(['fresh']);
	});

	it('a future producer stamp cannot mass-evict healthy records (server clock rules)', async () => {
		store = createDeadLetter(client, { ttlMs: 60_000 });
		const now = Date.now();
		await store.add(rec({ webhookId: 'healthy', failedAt: now - 1_000 }));
		// A skewed producer stamps an hour ahead. Under a stamp-clocked cutoff
		// this would sweep everything older than future-ttl, i.e. the healthy
		// record; the server clock keeps it.
		await store.add(rec({ webhookId: 'skewed', failedAt: now + 3_600_000 }));
		const kept = await store.list();
		expect(kept.map((r) => r.webhookId).sort()).toEqual(['healthy', 'skewed']);
	});

	it('clamps an implausible failedAt to the server clock', async () => {
		const before = Date.now();
		const idFuture = await store.add(rec({ failedAt: Date.now() + 3_600_000 }));
		const idMissing = await store.add(rec({ failedAt: undefined }));
		const idZero = await store.add(rec({ failedAt: 0 }));
		const after = Date.now();
		for (const id of [idFuture, idMissing, idZero]) {
			const got = await store.get(id);
			expect(got.failedAt).toBeGreaterThanOrEqual(before);
			expect(got.failedAt).toBeLessThanOrEqual(after + 1);
		}
		// A plausible past stamp is stored untouched.
		const idPast = await store.add(rec({ failedAt: before - 5_000 }));
		expect((await store.get(idPast)).failedAt).toBe(before - 5_000);
	});

	it('clears everything', async () => {
		await store.add(rec());
		await store.add(rec());
		await store.clear();
		expect(await store.count()).toBe(0);
		expect(await store.list()).toEqual([]);
		expect(await store.summary()).toMatchObject({ total: 0, byTopic: {} });
	});
});
