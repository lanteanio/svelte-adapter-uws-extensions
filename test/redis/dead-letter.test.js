import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createDeadLetter } from '../../src/redis/dead-letter.js';
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

		const sum = await store.summary();
		expect(sum.total).toBe(3);
		expect(Object.getPrototypeOf(sum.byTopic)).toBe(null);
		expect(sum.byTopic.__proto__).toBe(2);
		expect(sum.byTopic.constructor).toBe(1);
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
