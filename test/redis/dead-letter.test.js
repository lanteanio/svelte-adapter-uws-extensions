import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createDeadLetter } from '../../src/redis/dead-letter.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv, monotonicNow } from '../../src/shared/runtime.js';

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
		it('validates max, ttlMs and forgetTombstoneMs', () => {
			expect(() => createDeadLetter(client, { max: 0 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { max: 1.5 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { ttlMs: -1 })).toThrow('non-negative integer');
			expect(() => createDeadLetter(client, { forgetTombstoneMs: 0 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { forgetTombstoneMs: 1.5 })).toThrow('positive integer');
			expect(() => createDeadLetter(client, { forgetTombstoneMs: -100 })).toThrow('positive integer');
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

	it('counts __proto__ as an ordinary topic in its proto-free summary', async () => {
		await store.add(rec({ topic: '__proto__', failedAt: 10 }));
		await store.add(rec({ topic: '__proto__', failedAt: 20 }));
		await store.add(rec({ topic: 'constructor', failedAt: 30 }));
		await store.add(rec({ topic: 'prototype', failedAt: 40 }));
		await store.add(rec({ topic: 'safe', failedAt: 50 }));

		const sum = await store.summary();
		expect(sum.total).toBe(5);
		expect(Object.getPrototypeOf(sum.byTopic)).toBe(null);
		expect(sum.byTopic.__proto__).toBe(2);
		expect(sum.byTopic.constructor).toBe(1);
		expect(sum.byTopic.prototype).toBe(1);
		expect(Object.keys(sum.byTopic).sort()).toEqual(['__proto__', 'constructor', 'prototype', 'safe']);
		expect(Object.values(sum.byTopic).reduce((n, count) => n + count, 0)).toBe(sum.total);
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

	it('floors a fractional failedAt to integer ms, matching the Postgres clamp', async () => {
		const before = Date.now();
		// A fractional in-range stamp floors to integer ms so both backends agree.
		const idFrac = await store.add(rec({ failedAt: 100.7 }));
		expect((await store.get(idFrac)).failedAt).toBe(100);
		// A sub-1ms stamp floors to 0 and clamps to the server clock (as the Postgres
		// LEAST(NULLIF(floor, 0), now) does), instead of storing an ~1970 stamp.
		const idTiny = await store.add(rec({ failedAt: 0.5 }));
		expect((await store.get(idTiny)).failedAt).toBeGreaterThanOrEqual(before);
	});

	it('clears everything', async () => {
		await store.add(rec());
		await store.add(rec());
		await store.clear();
		expect(await store.count()).toBe(0);
		expect(await store.list()).toEqual([]);
		expect(await store.summary()).toMatchObject({ total: 0, byTopic: {} });
		for (const key of ['seq', 'recs', 'order', 'tenant', 'tenant-order', 'tenant-size', 'tenant-rank']) {
			expect(await client.redis.exists(client.key(`dlq:${key}:{dlq}`))).toBe(0);
		}
	});
});

describe('redis dead-letter fair-share eviction', () => {
	let client;
	beforeEach(() => { client = mockRedisClient('test:'); });

	const topicsKept = async (s) => (await s.list({ limit: 1000 })).map((r) => r.topic + ':' + r.data.n).sort();
	const size = (scope) => client.redis.zscore(client.key('dlq:tenant-size:{dlq}'), scope);

	it('keeps every record of a tenant under its share while another tenant overflows', async () => {
		const metrics = createMetrics();
		const s = createDeadLetter(client, { max: 10, metrics });
		for (let n = 0; n < 5; n++) await s.add(rec({ topic: '@t/victim/orders', data: { n } }));
		for (let n = 0; n < 10; n++) await s.add(rec({ topic: '@t/noisy/orders', data: { n } }));
		expect(await s.count()).toBe(10);
		expect(await s.count({ topic: '@t/victim/orders' })).toBe(5);
		// The noisy tenant lost its own oldest records.
		expect((await s.list({ topic: '@t/noisy/orders' })).map((r) => r.data.n)).toEqual([9, 8, 7, 6, 5]);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 5');
		for (let n = 10; n < 30; n++) await s.add(rec({ topic: '@t/noisy/orders', data: { n } }));
		expect(await s.count({ topic: '@t/victim/orders' })).toBe(5);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 25');
		expect(await size('victim')).toBe('5');
		expect(await size('noisy')).toBe('5');
	});

	it('evicts the largest scope down to the others, then shares the cap', async () => {
		const s = createDeadLetter(client, { max: 6 });
		for (let n = 0; n < 2; n++) await s.add(rec({ topic: 'orders', data: { n } }));
		for (let n = 0; n < 2; n++) await s.add(rec({ topic: '@t/a/orders', data: { n } }));
		for (let n = 0; n < 6; n++) await s.add(rec({ topic: '@t/b/orders', data: { n } }));
		// Untenanted topics are one scope; b had the most records and paid for all of it.
		expect(await topicsKept(s)).toEqual(['@t/a/orders:0', '@t/a/orders:1', '@t/b/orders:4', '@t/b/orders:5', 'orders:0', 'orders:1']);
		// Every scope sits at its share, so the next capture evicts the
		// capturing tenant's own oldest record.
		await s.add(rec({ topic: '@t/a/orders', data: { n: 2 } }));
		expect(await s.count({ topic: '@t/a/orders' })).toBe(2);
		expect(await s.count()).toBe(6);
	});

	it('keeps the tenant accounting in step with remove and purgeUser', async () => {
		const s = createDeadLetter(client, { max: 4, forgetUserId: (r) => r.data.user });
		const first = await s.add(rec({ topic: '@t/a/orders', data: { n: 0, user: 'u1' } }));
		await s.add(rec({ topic: '@t/a/orders', data: { n: 1, user: 'u2' } }));
		await s.add(rec({ topic: '@t/b/orders', data: { n: 0, user: 'u2' } }));
		expect(await s.remove(first)).toBe(true);
		expect(await s.remove(first)).toBe(false);
		expect(await size('a')).toBe('1');
		expect(await s.purgeUser('b', 'u2')).toBe(1);
		expect(await size('b')).toBe(null);
		expect(await client.redis.hlen(client.key('dlq:tenant:{dlq}'))).toBe(1);
		for (let n = 0; n < 4; n++) await s.add(rec({ topic: '@t/b/orders', data: { n: n + 1 } }));
		expect(await s.count({ topic: '@t/a/orders' })).toBe(1);
		expect(await s.count()).toBe(4);
	});

	it('breaks a tie between equally large scopes on the oldest record, whatever the tenant names', async () => {
		for (const order of [['a', 'b', 'z'], ['z', 'b', 'a']]) {
			const s = createDeadLetter(client, { max: 2 });
			await s.clear();
			const ids = [];
			for (const tenant of order) ids.push(await s.add(rec({ topic: `@t/${tenant}/x` })));
			// The capture just made survives; the oldest record's tenant pays.
			expect(await s.get(ids[2])).not.toBeNull();
			expect((await s.list()).map((r) => r.topic).sort()).toEqual(order.slice(1).map((t) => `@t/${t}/x`).sort());
		}
	});

	it('gives records stored before tenant accounting the scope of their stored topic', async () => {
		// Records an earlier version wrote: hash and order set, no tenant entries.
		const topics = ['@t/victim/orders', '@t/victim/orders', '@t/victim/orders', 'orders', 'orders', '@t/x'];
		for (const [index, topic] of topics.entries()) {
			const id = String(index + 1);
			await client.redis.hset(client.key('dlq:recs:{dlq}'), id, JSON.stringify({ ...rec({ topic, data: { n: id } }), failedAt: index + 1 }));
			await client.redis.zadd(client.key('dlq:order:{dlq}'), index + 1, id);
		}
		await client.redis.set(client.key('dlq:seq:{dlq}'), '6');
		const s = createDeadLetter(client, { max: 6 });
		for (let n = 0; n < 3; n++) await s.add(rec({ topic: '@t/fresh/orders', data: { n } }));
		// victim is largest, then ties with ~ and fresh at two and holds the
		// oldest record; then fresh is largest.
		expect((await s.list({ limit: 10 })).map((r) => r.id)).toEqual(['9', '8', '6', '5', '4', '3']);
		expect(await size('victim')).toBe('1');
		expect(await size('~')).toBe('2');
		expect(await size('')).toBe('1');
		expect(await size('fresh')).toBe('2');
		expect(await client.redis.hlen(client.key('dlq:tenant:{dlq}'))).toBe(6);
		expect(await client.redis.zcard(client.key('dlq:tenant-rank:{dlq}'))).toBe(4);
	});

	it('counts a malformed tenant topic in the unknown scope', async () => {
		const s = createDeadLetter(client, { max: 3 });
		await s.add(rec({ topic: '@t/victim/orders' }));
		await s.add(rec({ topic: '@t/' }));
		await s.add(rec({ topic: '@t/' }));
		await s.add(rec({ topic: '@t/' }));
		expect(await s.count({ topic: '@t/victim/orders' })).toBe(1);
		expect(await size('')).toBe('2');
	});
});

describe('redis dead-letter forget tombstone (right-to-erasure completeness)', () => {
	let client;
	// A single controllable virtual clock drives both the Redis server clock
	// (redis.time() -> wallEpoch) and monotonicNow(), so the store's
	// serverNow - (monotonicNow - startedAt) reconciliation is exercised
	// deterministically across a simulated multi-instance timeline.
	let clockMs;

	beforeEach(() => {
		client = mockRedisClient('test:');
		clockMs = 1_000_000;
		setRuntimeEnv({
			clock: {
				now: () => clockMs,
				monotonic: () => clockMs,
				wallEpoch: () => clockMs
			}
		});
	});

	afterEach(() => {
		resetRuntimeEnv();
	});

	const byAuthor = (record) => (record && record.data && record.data.author) || null;
	// The default forget-tombstone PX (createDeadLetter's FORGET_TOMBSTONE_MS).
	const DEFAULT_WINDOW = 10 * 60 * 1000;
	const drec = (over = {}) => ({
		webhookId: 'w',
		topic: 'orders',
		event: 'created',
		data: { author: 'u1', body: 'PII' },
		attempts: 3,
		error: 'x',
		...over
	});

	it('erases only the named tenant, not every tenant with that user id', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		// Tenancy rides the wire topic. Matching on user id alone made one
		// tenant's right-to-erasure destroy every other tenant's dead letters
		// for the same user id, while the sibling legs of the same purge were
		// correctly scoped.
		await s.add(drec({ topic: '@t/acme/orders' }));
		await s.add(drec({ topic: '@t/globex/orders' }));
		await s.add(drec({ topic: 'orders' }));
		expect(await s.count()).toBe(3);

		expect(await s.purgeUser('acme', 'u1')).toBe(1);
		expect(await s.count()).toBe(2);

		// The untenanted scope reaches the unprefixed topic only.
		expect(await s.purgeUser(null, 'u1')).toBe(1);
		expect(await s.count()).toBe(1);

		expect(await s.purgeUser('globex', 'u1')).toBe(1);
		expect(await s.count()).toBe(0);
	});

	it('purgeUser refuses a tenant id its tombstone key cannot carry', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		await s.add(drec({ topic: '@t/a/b/orders' }));
		for (const tenantId of ['a/b', '~', 'a~b', 7]) {
			await expect(s.purgeUser(tenantId, 'u1')).rejects.toThrow(TypeError);
		}
		// Nothing was written or removed by the refused calls.
		expect(await s.count()).toBe(1);
		for (const scope of ['a/b', '~', 'a~b', '7']) {
			expect(await client.redis.exists(client.key('dlq:purged:' + scope + '/u1'))).toBe(0);
		}
		expect(await s.purgeUser('a', 'u1')).toBe(1);
		expect(await createDeadLetter(client).purgeUser('a/b', 'u1').catch((e) => e)).toBeInstanceOf(TypeError);
	});

	it('drops a record whose delivery raced a purge, and keeps a genuinely new one', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });

		// A delivery for u1 starts now.
		const startedAt = monotonicNow();
		// live.forget(u1) runs 5s later while that delivery is still in flight.
		clockMs += 5_000;
		expect(await s.purgeUser(null, 'u1')).toBe(0); // empty store; tombstone armed

		// The in-flight delivery finally exhausts retries and is captured 10s
		// after it started (5s after the purge).
		clockMs += 5_000;
		const dropped = await s.add(drec({ failedAt: clockMs, startedAt }));
		expect(dropped).toBeNull();
		expect(await s.count()).toBe(0);

		// A genuinely NEW delivery for u1, started AFTER the purge, is kept.
		const newStart = monotonicNow();
		clockMs += 2_000;
		const kept = await s.add(drec({ data: { author: 'u1', body: 'new' }, failedAt: clockMs, startedAt: newStart }));
		expect(kept).not.toBeNull();
		expect(await s.count()).toBe(1);
	});

	it("a different user's in-flight delivery is unaffected by another user's tombstone", async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();
		clockMs += 5_000;
		await s.purgeUser(null, 'u1'); // tombstone for u1 only
		clockMs += 5_000;
		const other = await s.add(drec({ data: { author: 'u2', body: 'x' }, failedAt: clockMs, startedAt }));
		expect(other).not.toBeNull();
		expect(await s.count()).toBe(1);
	});

	it('is skew-immune: a long delivery whose monotonic start precedes the purge is still dropped via the server clock', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();
		clockMs += 1;                 // purge 1ms after the delivery started
		await s.purgeUser(null, 'u1');
		// Delivery fails 2 minutes later - a long retry budget, but comfortably
		// inside the 10-minute tombstone window, so the drop is the SKEW property
		// (monotonic start precedes the purge yet the server-clock reconciliation
		// still drops it), not an artifact of riding the tombstone's expiry edge.
		clockMs += 120_000;
		const dropped = await s.add(drec({ attempts: 8, failedAt: clockMs, startedAt }));
		expect(dropped).toBeNull();
	});

	it('without a forgetUserId extractor, purgeUser is a no-op and records are never tombstone-dropped', async () => {
		const s = createDeadLetter(client); // no extractor
		const startedAt = monotonicNow();
		clockMs += 5_000;
		expect(await s.purgeUser(null, 'u1')).toBe(0);
		clockMs += 5_000;
		const id = await s.add(drec({ failedAt: clockMs, startedAt }));
		expect(id).not.toBeNull();
		expect(await s.count()).toBe(1);
	});

	it('an absent startedAt never drops (fallback treats the start as now)', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		clockMs += 5_000;
		await s.purgeUser(null, 'u1');
		clockMs += 5_000;
		// No startedAt: elapsed 0 -> serverStart == serverNow, a past purge cannot
		// be >= it, so the record is kept (matches the in-memory untraced fallback).
		const id = await s.add(drec({ failedAt: clockMs }));
		expect(id).not.toBeNull();
		expect(await s.count()).toBe(1);
	});

	it('purgeUser deletes the user\'s existing records and arms the tombstone in one call', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		clockMs += 1_000;
		await s.add(drec({ data: { author: 'u1', body: 'a' }, failedAt: clockMs, startedAt: monotonicNow() }));
		await s.add(drec({ data: { author: 'u2', body: 'b' }, failedAt: clockMs, startedAt: monotonicNow() }));
		clockMs += 1_000;
		expect(await s.purgeUser(null, 'u1')).toBe(1); // u1's record deleted
		expect(await s.count()).toBe(1);               // u2 remains
		// And a racing u1 delivery captured after the purge is dropped.
		const startedBefore = 1_000_000; // before the purge
		clockMs += 1_000;
		const dropped = await s.add(drec({ data: { author: 'u1', body: 'raced' }, failedAt: clockMs, startedAt: startedBefore }));
		expect(dropped).toBeNull();
		expect(await s.count()).toBe(1);
	});

	it('a raced delivery captured just inside the tombstone window is still dropped', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();     // delivery starts before the purge
		clockMs += 1;
		await s.purgeUser(null, 'u1');         // tombstone armed, PX = DEFAULT_WINDOW
		// Capture one ms before the tombstone would PX-expire: still within the
		// window, so the erased payload is dropped.
		clockMs += DEFAULT_WINDOW - 2;
		const dropped = await s.add(drec({ failedAt: clockMs, startedAt }));
		expect(dropped).toBeNull();
		expect(await s.count()).toBe(0);
	});

	it("a purge in one tenant leaves another tenant's in-flight capture for the same user", async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();
		clockMs += 5_000;
		await s.purgeUser('acme', 'u1');
		clockMs += 5_000;
		expect(await s.add(drec({ topic: '@t/acme/orders', failedAt: clockMs, startedAt }))).toBeNull();
		const otherTenant = await s.add(drec({ topic: '@t/globex/orders', failedAt: clockMs, startedAt }));
		const untenanted = await s.add(drec({ topic: 'orders', failedAt: clockMs, startedAt }));
		expect(otherTenant).not.toBeNull();
		expect(untenanted).not.toBeNull();
		expect((await s.list()).map(r => r.topic).sort()).toEqual(['@t/globex/orders', 'orders']);
	});

	it('an untenanted purge drops only untenanted in-flight captures', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();
		clockMs += 5_000;
		await s.purgeUser(null, 'u1');
		clockMs += 5_000;
		expect(await s.add(drec({ topic: 'orders', failedAt: clockMs, startedAt }))).toBeNull();
		expect(await s.add(drec({ topic: '@t/acme/orders', failedAt: clockMs, startedAt }))).not.toBeNull();
	});

	it('honors a tombstone written under the unscoped key', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();
		clockMs += 5_000;
		await client.redis.set(client.key('dlq:purged:u1'), String(clockMs), 'PX', DEFAULT_WINDOW);
		clockMs += 5_000;
		expect(await s.add(drec({ topic: '@t/acme/orders', failedAt: clockMs, startedAt }))).toBeNull();
		expect(await s.add(drec({ topic: 'orders', failedAt: clockMs, startedAt }))).toBeNull();
		expect(await s.count()).toBe(0);
	});

	it('hides expired records from every read before the next add evicts them', async () => {
		const s = createDeadLetter(client, { ttlMs: 60_000 });
		const oldId = await s.add(drec({ topic: 'a', failedAt: clockMs }));
		clockMs += 30_000;
		const newId = await s.add(drec({ topic: 'b', failedAt: clockMs }));
		clockMs += 30_000; // the first record is now exactly ttlMs old
		expect(await s.get(oldId)).toBeNull();
		expect((await s.get(newId))?.topic).toBe('b');
		expect(await s.count()).toBe(1);
		expect(await s.count({ topic: 'a' })).toBe(0);
		expect((await s.list()).map(r => r.id)).toEqual([newId]);
		expect(await s.summary()).toEqual({ total: 1, byTopic: { b: 1 }, oldest: clockMs - 30_000, newest: clockMs - 30_000 });
	});

	it('documented residual: a delivery captured just past the tombstone window resurrects (tombstone PX-expired)', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor });
		const startedAt = monotonicNow();     // delivery starts before the purge
		clockMs += 1;
		await s.purgeUser(null, 'u1');
		// One ms PAST the tombstone deadline: the mock now models PX, so the
		// tombstone is gone and the drop-check finds nothing - the erased payload
		// is re-inserted. This is the parity residual (same 10-min window as the
		// in-memory store), documented here rather than hidden by a mock that
		// keeps the key forever. `forgetTombstoneMs` widens the window.
		clockMs += DEFAULT_WINDOW + 1;
		const resurrected = await s.add(drec({ failedAt: clockMs, startedAt }));
		expect(resurrected).not.toBeNull();
		expect(await s.count()).toBe(1);
	});

	it('a wider forgetTombstoneMs keeps dropping where the default window would have resurrected', async () => {
		const s = createDeadLetter(client, { forgetUserId: byAuthor, forgetTombstoneMs: 30 * 60 * 1000 });
		const startedAt = monotonicNow();
		clockMs += 1;
		await s.purgeUser(null, 'u1');
		// Past the DEFAULT window (where the residual test resurrected) but inside
		// the widened 30-minute window: the erased payload is still dropped.
		clockMs += DEFAULT_WINDOW + 60_000;
		const dropped = await s.add(drec({ failedAt: clockMs, startedAt }));
		expect(dropped).toBeNull();
		expect(await s.count()).toBe(0);
	});
});
