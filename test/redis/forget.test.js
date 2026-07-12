// live.forget durable layer: the createForgetStore composer plus the per-store
// purgeUser implementations (connection registry + redis idempotency) exercised
// against the in-memory redis mock.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';
import { createForgetStore } from '../../src/shared/forget-store.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createIdempotencyStore } from '../../src/redis/idempotency.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createDistributedSession } from '../../src/redis/session.js';
import { createDeadLetter } from '../../src/redis/dead-letter.js';
import { createReplay } from '../../src/redis/replay.js';

function wsWithSession(userData, sessionId) {
	const ws = mockWs(userData);
	ws.getUserData()[WS_SESSION_ID] = sessionId;
	return ws;
}

describe('createForgetStore (composer)', () => {
	it('fans out to every wired store and returns a per-store breakdown', async () => {
		const seen = [];
		const a = { async purgeUser(t, u, c) { seen.push(['a', t, u, c]); return 2; } };
		const b = { async purgeUser(t, u, c) { seen.push(['b', t, u, c]); return 3; } };
		const store = createForgetStore({ registry: a, idempotency: b });

		const counts = await store.purgeUser('t1', 'u-1', true);
		expect(counts).toEqual({ registry: 2, idempotency: 3 });
		expect(seen).toEqual([['a', 't1', 'u-1', true], ['b', 't1', 'u-1', true]]);
	});

	it('flattens a nested per-store breakdown into a total', async () => {
		const a = { async purgeUser() { return { conns: 1, maps: 4 }; } };
		const store = createForgetStore({ registry: a });
		expect(await store.purgeUser('t', 'u')).toEqual({ registry: 5 });
	});

	it('attempts every store even when one fails, then rejects (incomplete erasure)', async () => {
		const good = { purged: false, async purgeUser() { this.purged = true; return 1; } };
		const bad = { async purgeUser() { throw new Error('redis down'); } };
		const store = createForgetStore({ good, bad });

		await expect(store.purgeUser('t', 'u')).rejects.toThrow(/incomplete/);
		expect(good.purged).toBe(true); // not short-circuited by bad's failure
	});

	it('skips stores without a purgeUser, and validates the argument', async () => {
		const a = { async purgeUser() { return 1; } };
		const store = createForgetStore({ a, legacy: {}, missing: null });
		expect(await store.purgeUser('t', 'u')).toEqual({ a: 1 });
		expect(await store.purgeUser('t', '')).toEqual({}); // empty userId is a no-op
		expect(() => createForgetStore(null)).toThrow();
	});

	it('accepts an array of stores (index-labelled)', async () => {
		const a = { async purgeUser() { return 1; } };
		const b = { async purgeUser() { return 2; } };
		const store = createForgetStore([a, b]);
		expect(await store.purgeUser('t', 'u')).toEqual({ store0: 1, store1: 2 });
	});
});

describe('connection registry purgeUser', () => {
	let client; let platform; let registry;

	beforeEach(() => {
		client = mockRedisClient('app:');
		platform = mockPlatform();
		registry = createConnectionRegistry(client, {
			identify: (ws) => ws.getUserData()?.userId,
			heartbeat: 60000,
			ttl: 90
		});
	});

	afterEach(async () => { await registry.destroy(); });

	it('deletes the durable row + clears the in-memory maps, unconditionally', async () => {
		const ws = wsWithSession({ userId: 'u-1' }, 'sess-1');
		await registry.hooks.open(ws, { platform });
		expect(registry.size()).toBe(1);
		expect(await registry.lookup('u-1')).not.toBeNull();

		const n = await registry.purgeUser(null, 'u-1');
		expect(n).toBe(1); // conns:{u-1} removed
		expect(registry.size()).toBe(0);
		expect(await registry.lookup('u-1')).toBeNull();
	});

	it('is a no-op count for a user that was never registered', async () => {
		expect(await registry.purgeUser(null, 'ghost')).toBe(0);
		expect(await registry.purgeUser(null, '')).toBe(0);
	});

	it('purges a user this instance does not own (unconditional, not ownership-gated)', async () => {
		// Simulate a row owned by another instance: write conns:{u-2} directly.
		const otherKey = client.key('conns:u-2');
		await client.redis.hset(otherKey, 'instanceId', 'other-instance', 'sessionId', 's', 'ts', 1);
		const n = await registry.purgeUser(null, 'u-2');
		expect(n).toBe(1); // deleted despite not being ours
	});
});

describe('redis idempotency purgeUser', () => {
	let client; let store;

	beforeEach(() => {
		client = mockRedisClient('test:');
		store = createIdempotencyStore(client);
	});

	it('erases a user cached results via the byuser index so a re-acquire is fresh', async () => {
		const slot = await store.acquire('key-1', undefined, { user: 'u-1', tenant: 't1' });
		await slot.commit({ ok: 1 });
		const hit = await store.acquire('key-1');
		expect(hit.result).toEqual({ ok: 1 }); // cached

		const n = await store.purgeUser('t1', 'u-1');
		expect(n).toBe(1);

		const fresh = await store.acquire('key-1');
		expect(fresh.acquired).toBe(true); // erased -> re-runs
	});

	it('does not erase another user/tenant entries', async () => {
		const a = await store.acquire('ka', undefined, { user: 'u-1', tenant: 't1' });
		await a.commit({ v: 'a' });
		const b = await store.acquire('kb', undefined, { user: 'u-2', tenant: 't1' });
		await b.commit({ v: 'b' });

		await store.purgeUser('t1', 'u-1');
		const stillCached = await store.acquire('kb');
		expect(stillCached.result).toEqual({ v: 'b' });
	});

	it('is a no-op for an unindexed / anonymous user', async () => {
		const a = await store.acquire('k-anon'); // no meta -> not indexed
		await a.commit({ v: 1 });
		expect(await store.purgeUser('t1', 'nobody')).toBe(0);
		const stillCached = await store.acquire('k-anon');
		expect(stillCached.result).toEqual({ v: 1 });
	});

	it('purging one tenant leaves the same user in another tenant intact', async () => {
		const a = await store.acquire('ka', undefined, { user: 'u-1', tenant: 't1' });
		await a.commit({ v: 'a' });
		const b = await store.acquire('kb', undefined, { user: 'u-1', tenant: 't2' });
		await b.commit({ v: 'b' });

		expect(await store.purgeUser('t1', 'u-1')).toBe(1);
		expect((await store.acquire('kb')).result).toEqual({ v: 'b' }); // t2 untouched
		expect((await store.acquire('ka')).acquired).toBe(true); // t1 erased -> re-runs
	});
});

describe('redis idempotency byuser index field TTL', () => {
	it('bounds each index field to its own cache entry lifetime (per-field TTL, Redis 7.4+)', async () => {
		const client = mockRedisClient('test:');
		const store = createIdempotencyStore(client, { ttl: 100 });
		const slot = await store.acquire('k1', undefined, { user: 'u-1', tenant: 't1' });
		await slot.commit({ ok: 1 });

		const idxKey = client.key('idem:byuser:t1\0u-1');
		const [ttlMs] = await client.redis.hpttl(idxKey, 'FIELDS', 1, client.key('idem:k1'));
		expect(ttlMs).toBeGreaterThan(0);
		expect(ttlMs).toBeLessThanOrEqual(100 * 1000);
	});

	it('falls back to a whole-key TTL on a pre-7.4 server, and purge still works', async () => {
		const client = mockRedisClient('test:');
		client.redis._info = '# Server\nredis_version:6.2.0\n';
		const store = createIdempotencyStore(client, { ttl: 100 });
		const slot = await store.acquire('k1', undefined, { user: 'u-1', tenant: 't1' });
		await slot.commit({ ok: 1 });

		const idxKey = client.key('idem:byuser:t1\0u-1');
		const [ttlMs] = await client.redis.hpttl(idxKey, 'FIELDS', 1, client.key('idem:k1'));
		expect(ttlMs).toBe(-1); // field present, no per-field TTL (whole-key EXPIRE path)
		expect(await store.purgeUser('t1', 'u-1')).toBe(1);
	});

	it('uses per-field TTL on Valkey 9.0+ and the whole-key fallback below Valkey 9', async () => {
		const modern = mockRedisClient('test:');
		modern.redis._info = '# Server\nredis_version:7.2.4\nserver_name:valkey\nvalkey_version:9.0.0\n';
		const storeModern = createIdempotencyStore(modern, { ttl: 100 });
		const s1 = await storeModern.acquire('k', undefined, { user: 'u', tenant: 't' });
		await s1.commit(1);
		const [modernTtl] = await modern.redis.hpttl(modern.key('idem:byuser:t\0u'), 'FIELDS', 1, modern.key('idem:k'));
		expect(modernTtl).toBeGreaterThan(0);

		const old = mockRedisClient('test:');
		old.redis._info = '# Server\nredis_version:7.2.4\nserver_name:valkey\nvalkey_version:8.1.0\n';
		const storeOld = createIdempotencyStore(old, { ttl: 100 });
		const s2 = await storeOld.acquire('k', undefined, { user: 'u', tenant: 't' });
		await s2.commit(1);
		const [oldTtl] = await old.redis.hpttl(old.key('idem:byuser:t\0u'), 'FIELDS', 1, old.key('idem:k'));
		expect(oldTtl).toBe(-1);
		expect(await storeOld.purgeUser('t', 'u')).toBe(1); // fallback path still purgeable
	});
});

describe('redis presence purgeUser', () => {
	it('removes a user from every topic across the cluster, leaving others intact', async () => {
		const client = mockRedisClient('test:');
		const platform = mockPlatform();
		const presence = createPresence(client, { key: 'id', select: (u) => ({ id: u.id, name: u.name }), heartbeat: 60000, ttl: 180 });
		await presence.join(mockWs({ id: 'u1', name: 'A' }), 'room', platform);
		await presence.join(mockWs({ id: 'u1', name: 'A' }), 'room2', platform);
		await presence.join(mockWs({ id: 'u2', name: 'B' }), 'room', platform);

		const n = await presence.purgeUser(null, 'u1');
		expect(n).toBe(2); // room + room2

		expect((await presence.list('room')).map((d) => d.id)).toEqual(['u2']);
		expect(await presence.list('room2')).toEqual([]);
		presence.destroy();
	});

	it('is a no-op for an absent user', async () => {
		const client = mockRedisClient('test:');
		const presence = createPresence(client, { key: 'id', select: (u) => ({ id: u.id }), heartbeat: 60000, ttl: 180 });
		expect(await presence.purgeUser(null, 'ghost')).toBe(0);
		presence.destroy();
	});

	it('purges a user whose topic has a live sync observer (observer refcount is topic->number, not per-user)', async () => {
		const client = mockRedisClient('test:');
		const platform = mockPlatform();
		const presence = createPresence(client, { key: 'id', select: (u) => ({ id: u.id }), heartbeat: 60000, ttl: 180 });
		await presence.join(mockWs({ id: 'u1' }), 'room', platform);
		await presence.sync(mockWs({ id: 'watcher' }), 'room', platform);

		const n = await presence.purgeUser(null, 'u1');
		expect(n).toBe(1);
		expect(await presence.list('room')).toEqual([]);

		// The observer's cross-instance subscription survives the purge: a
		// fresh join on the same topic must still reach the tap channel.
		await presence.join(mockWs({ id: 'u2' }), 'room', platform);
		expect((await presence.list('room')).map((d) => d.id)).toEqual(['u2']);
		presence.destroy();
	});
});

describe('redis cursor purgeUser', () => {
	it('removes a user cursors across topics, matching value.user.id', async () => {
		const client = mockRedisClient('test:');
		const platform = mockPlatform();
		const c = createCursor(client, { throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0, select: (u) => ({ id: u.id, name: u.name }) });
		c.update(mockWs({ id: 'u1', name: 'A' }), 'canvas', { x: 1 }, platform);
		c.update(mockWs({ id: 'u1', name: 'A' }), 'board', { x: 2 }, platform);
		c.update(mockWs({ id: 'u2', name: 'B' }), 'canvas', { x: 3 }, platform);
		await new Promise((r) => setTimeout(r, 0)); // let the immediate snapshot writes land

		const n = await c.purgeUser(null, 'u1');
		expect(n).toBe(2); // canvas + board

		expect((await c.list('canvas')).map((e) => e.user.id)).toEqual(['u2']);
		expect(await c.list('board')).toEqual([]);
		c.destroy();
	});

	it('is a no-op for an absent user', async () => {
		const client = mockRedisClient('test:');
		const c = createCursor(client, { snapshotIntervalMs: 0, select: (u) => ({ id: u.id }) });
		expect(await c.purgeUser(null, 'ghost')).toBe(0);
		c.destroy();
	});
});

describe('redis session purgeUser', () => {
	it('revokes every session a user holds via the forgetUserId extractor', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: (d) => d && d.userId });
		await session.set('tok-1', { userId: 'u1', n: 1 });
		await session.set('tok-2', { userId: 'u1', n: 2 });
		await session.set('tok-3', { userId: 'u2' });

		const n = await session.purgeUser(null, 'u1');
		expect(n).toBe(2);
		expect(await session.get('tok-1')).toBeNull();
		expect(await session.get('tok-2')).toBeNull();
		expect(await session.get('tok-3')).toEqual({ userId: 'u2' });
	});

	it('is a no-op when no forgetUserId extractor is configured', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client);
		await session.set('tok', { userId: 'u1' });
		expect(await session.purgeUser(null, 'u1')).toBe(0);
		expect(await session.get('tok')).toEqual({ userId: 'u1' });
	});
});

describe('redis session byuser index field TTL', () => {
	const extractor = (d) => d && d.userId;
	const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

	it('bounds each index field to its own session lifetime (per-field TTL, Redis 7.4+)', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000 });
		await session.set('tok-1', { userId: 'u1' });

		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, 'tok-1');
		expect(ttlMs).toBeGreaterThan(0);
		expect(ttlMs).toBeLessThanOrEqual(10000);
	});

	it('falls back to a whole-key TTL on a pre-7.4 server, and purge still works', async () => {
		const client = mockRedisClient('app:');
		client.redis._info = '# Server\nredis_version:6.2.0\n';
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000 });
		await session.set('tok', { userId: 'u1' });

		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(ttlMs).toBe(-1); // field present, no per-field TTL (whole-key PEXPIRE path)
		expect(await session.purgeUser(null, 'u1')).toBe(1);
	});

	it('uses per-field TTL on Valkey 9.0+ and the whole-key fallback below Valkey 9', async () => {
		const modern = mockRedisClient('app:');
		modern.redis._info = '# Server\nredis_version:7.2.4\nserver_name:valkey\nvalkey_version:9.0.0\n';
		const sessModern = createDistributedSession(modern, { forgetUserId: extractor, ttlMs: 10000 });
		await sessModern.set('tok', { userId: 'u1' });
		const [modernTtl] = await modern.redis.hpttl(modern.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(modernTtl).toBeGreaterThan(0);

		const old = mockRedisClient('app:');
		old.redis._info = '# Server\nredis_version:7.2.4\nserver_name:valkey\nvalkey_version:8.1.0\n';
		const sessOld = createDistributedSession(old, { forgetUserId: extractor, ttlMs: 10000 });
		await sessOld.set('tok', { userId: 'u1' });
		const [oldTtl] = await old.redis.hpttl(old.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(oldTtl).toBe(-1);
		expect(await sessOld.purgeUser(null, 'u1')).toBe(1);
	});

	it('touch slides the index field with the record, so a touch-kept session stays purgeable', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000 });
		await session.set('tok', { userId: 'u1' });
		await sleep(150);

		expect(await session.touch('tok')).toBe(true);
		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(ttlMs).toBeGreaterThan(9900); // re-armed by touch, not decayed since set
		expect(await session.purgeUser(null, 'u1')).toBe(1);
	});

	it('touch derives the user through a lifecycle record wrapper', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000 });
		const token = await session.create({ userId: 'u1' });
		await sleep(150);

		expect(await session.touch(token)).toBe(true);
		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, token);
		expect(ttlMs).toBeGreaterThan(9900);
	});

	it('a sliding get refreshes the index field along with the record', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000 });
		await session.set('tok', { userId: 'u1' });
		await sleep(150);

		expect(await session.get('tok')).toEqual({ userId: 'u1' });
		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(ttlMs).toBeGreaterThan(9900);
	});

	it('refreshOnGet: false leaves the index field decaying on reads', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, { forgetUserId: extractor, ttlMs: 10000, refreshOnGet: false });
		await session.set('tok', { userId: 'u1' });
		await sleep(150);

		expect(await session.get('tok')).toEqual({ userId: 'u1' });
		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, 'tok');
		expect(ttlMs).toBeLessThanOrEqual(9900); // read did not re-arm the field
	});

	it('a lifecycle load (withHooks upgrade) refreshes the index field', async () => {
		const client = mockRedisClient('app:');
		const session = createDistributedSession(client, {
			forgetUserId: extractor,
			identify: (ctx) => ctx.token,
			ttlMs: 10000
		});
		const token = await session.create({ userId: 'u1' });
		await sleep(150);

		const hooks = session.withHooks();
		const userData = await hooks.upgrade({ token });
		expect(userData).toBeTruthy();
		const [ttlMs] = await client.redis.hpttl(client.key('sess:byuser:u1'), 'FIELDS', 1, token);
		expect(ttlMs).toBeGreaterThan(9900);
	});
});

describe('redis dead-letter purgeUser', () => {
	it('drops a user records via the forgetUserId extractor (scan at purge)', async () => {
		const client = mockRedisClient('test:');
		const dlq = createDeadLetter(client, { forgetUserId: (rec) => rec.data && rec.data.userId });
		await dlq.add({ webhookId: 'w1', topic: 't', event: 'e', data: { userId: 'u1' }, attempts: 1, error: 'x', failedAt: 100 });
		await dlq.add({ webhookId: 'w2', topic: 't', event: 'e', data: { userId: 'u2' }, attempts: 1, error: 'x', failedAt: 101 });
		expect(await dlq.count()).toBe(2);

		expect(await dlq.purgeUser(null, 'u1')).toBe(1);
		expect(await dlq.count()).toBe(1);
		expect((await dlq.list())[0].data.userId).toBe('u2');
	});

	it('is a no-op without a forgetUserId extractor', async () => {
		const client = mockRedisClient('test:');
		const dlq = createDeadLetter(client);
		await dlq.add({ webhookId: 'w', topic: 't', event: 'e', data: { userId: 'u1' }, attempts: 1, error: 'x', failedAt: 1 });
		expect(await dlq.purgeUser(null, 'u1')).toBe(0);
		expect(await dlq.count()).toBe(1);
	});
});

describe('redis replay purgeUser (sorted-set)', () => {
	it('drops a user buffered events across topics', async () => {
		const client = mockRedisClient('test:');
		const platform = mockPlatform();
		const replay = createReplay(client, { forgetUserId: ({ data }) => data && data.userId });
		await replay.publish(platform, 'room', 'msg', { userId: 'u1', x: 1 });
		await replay.publish(platform, 'room', 'msg', { userId: 'u2', x: 2 });
		await replay.publish(platform, 'board', 'msg', { userId: 'u1', x: 3 });

		expect(await replay.purgeUser(null, 'u1')).toBe(2);
		expect((await replay.since('room', 0)).map((e) => e.data.userId)).toEqual(['u2']);
		expect(await replay.since('board', 0)).toEqual([]);
	});
});

describe('redis ratelimit purgeUser', () => {
	it('clears a user-keyed bucket; a no-op when keyBy is not the userId', async () => {
		const client = mockRedisClient('test:');
		const byUser = createRateLimit(client, { points: 5, interval: 1000, keyBy: (ws) => ws.getUserData().userId });
		const ws = { getUserData: () => ({ userId: 'u-1' }) };
		await byUser.consume(ws); // seeds the 'u-1' bucket
		expect(await byUser.purgeUser(null, 'u-1')).toBe(1);
		expect(await byUser.purgeUser(null, 'u-1')).toBe(0); // already gone

		// ip-keyed limiter: a per-user purge cannot address the bucket -> no-op.
		const byIp = createRateLimit(client, { points: 5, interval: 1000 });
		expect(await byIp.purgeUser(null, 'u-1')).toBe(0);
	});
});
