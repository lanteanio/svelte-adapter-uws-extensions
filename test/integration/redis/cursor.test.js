/**
 * Integration tests for redis/cursor against a real Redis 7 server.
 *
 * Most of cursor.js is HSET/HGET/HDEL/HGETALL plus pub/sub relay --
 * the in-memory mock matches Redis closely there. The mock-based suite
 * at test/redis/cursor.test.js carries the bulk of the behavior
 * coverage. This file pins only the things that need a real wire:
 *
 * - EXPIRE on the hash key with real time semantics (TTL is set, TTL
 *   stays positive).
 * - SCAN over many keys in clear() (the mock fakes SCAN cursors but
 *   real SCAN with COUNT hint behaves differently).
 * - Cross-connection visibility: an entry written by one ioredis
 *   connection is readable by another's HGETALL via list().
 * - Cross-instance pub/sub relay over the wire (one cursor instance's
 *   broadcast reaches another's subscriber).
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createCursor } from '../../../redis/cursor.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(10);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('redis cursor (integration)', () => {
	let client;
	let platform;
	const trackers = [];

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-cursor:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe everything under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');

		platform = mockPlatform();
	});

	afterEach(() => {
		while (trackers.length > 0) {
			const t = trackers.pop();
			try { t.destroy(); } catch { /* ignore */ }
		}
	});

	afterAll(async () => {
		await client.quit();
	});

	function track(t) {
		trackers.push(t);
		return t;
	}

	describe('Redis hash storage', () => {
		it('writes a cursor entry to the topic hash readable from a separate connection', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));
			const ws = mockWs({ id: '1', name: 'Alice' });
			c.update(ws, 'canvas', { x: 42, y: 99 }, platform);

			// Wait for the async pipeline to land in Redis.
			await waitFor(async () => {
				const all = await client.redis.hgetall(client.key('cursor:canvas'));
				return Object.keys(all).length > 0;
			});

			// Read back from a fresh, independent connection.
			const reader = createRedisClient({
				url: process.env.INTEGRATION_REDIS_URL,
				autoShutdown: false
			});
			try {
				const all = await reader.redis.hgetall(client.key('cursor:canvas'));
				const keys = Object.keys(all);
				expect(keys).toHaveLength(1);
				const stored = JSON.parse(all[keys[0]]);
				expect(stored.data).toEqual({ x: 42, y: 99 });
				expect(stored.user).toMatchObject({ id: '1', name: 'Alice' });
				expect(typeof stored.ts).toBe('number');
			} finally {
				await reader.quit();
			}
		});

		it('list() reflects writes after the pipeline lands', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			c.update(ws1, 'canvas', { x: 10 }, platform);
			c.update(ws2, 'canvas', { x: 20 }, platform);

			await waitFor(async () => (await c.list('canvas')).length === 2);

			const list = await c.list('canvas');
			const xs = list.map((e) => e.data.x).sort((a, b) => a - b);
			expect(xs).toEqual([10, 20]);
		});

		it('hdel via remove() drops the entry from the real hash', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));
			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 1 }, platform);
			await waitFor(async () => (await c.list('canvas')).length === 1);

			await c.remove(ws, platform);

			const all = await client.redis.hgetall(client.key('cursor:canvas'));
			expect(Object.keys(all)).toHaveLength(0);
		});
	});

	describe('TTL applied via EXPIRE in the broadcast pipeline', () => {
		it('sets a positive TTL on the topic hash within the configured window', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0, ttl: 30 }));
			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 1 }, platform);

			await waitFor(async () => {
				const t = await client.redis.ttl(client.key('cursor:canvas'));
				return t > 0;
			});
			const ttl = await client.redis.ttl(client.key('cursor:canvas'));
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(30);
		});
	});

	describe('clear() with real SCAN over many keys', () => {
		it('SCAN+UNLINK clears every cursor:* key under the prefix', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));

			// Spread cursors across many topics so SCAN actually has to
			// page through a non-trivial keyspace.
			for (let i = 0; i < 30; i++) {
				const ws = mockWs({ id: String(i) });
				c.update(ws, 'topic-' + i, { x: i }, platform);
			}
			// Wait for all pipelines to settle.
			await waitFor(async () => {
				const all = await client.redis.keys(client.key('cursor:*'));
				return all.length >= 30;
			});

			await c.clear();

			const remaining = await client.redis.keys(client.key('cursor:*'));
			expect(remaining).toHaveLength(0);
		});
	});

	describe('cross-instance pub/sub relay over the wire', () => {
		it('a remote update on cursor:events lands in the other instance with relay: false', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));
			const ws = mockWs({ id: '1' });

			// First update wires the subscriber duplicate up.
			c.update(ws, 'canvas', { x: 0 }, platform);
			// Give the SUBSCRIBE round trip time to land before we publish.
			await wait(100);
			platform.reset();

			// Simulate a peer instance publishing on the same channel.
			// Use a separate publisher client so we are exercising real
			// cross-connection delivery.
			const publisher = createRedisClient({
				url: process.env.INTEGRATION_REDIS_URL,
				autoShutdown: false
			});
			try {
				const remoteMsg = JSON.stringify({
					instanceId: 'remote-instance',
					topic: 'canvas',
					event: 'update',
					payload: { key: 'remote:1', user: { id: '2', name: 'Bob' }, data: { x: 77 } }
				});
				await publisher.redis.publish(client.key('cursor:events'), remoteMsg);

				await waitFor(() =>
					platform.published.find((p) => p.data && p.data.key === 'remote:1') !== undefined
				);

				const got = platform.published.find((p) => p.data && p.data.key === 'remote:1');
				expect(got.topic).toBe('__cursor:canvas');
				expect(got.event).toBe('update');
				expect(got.data.data).toEqual({ x: 77 });
				expect(got.options).toEqual({ relay: false });
			} finally {
				await publisher.quit();
			}
		});

		it('echo suppression: a tracker does not double-publish its own updates', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0 }));
			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 1 }, platform);

			// First update broadcasts locally (recorded immediately) and
			// relays via Redis. The relay loops back through the same
			// instance's subscriber but is filtered by instanceId. If
			// echo suppression were broken, we would see a second
			// "update" event with the same payload.
			await wait(200);
			const updates = platform.published.filter((p) => p.event === 'update' && p.data && p.data.data && p.data.data.x === 1);
			expect(updates).toHaveLength(1);
		});

		it('two real trackers: A.update is visible in B.list and arrives on B as an update event', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = track(createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				select: (ud) => ({ id: ud.id })
			}));
			const trackerB = track(createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				select: (ud) => ({ id: ud.id })
			}));

			const wsA = mockWs({ id: 'alice' });
			const wsB = mockWs({ id: 'bob' });

			// Bring B's subscriber up before A publishes.
			trackerB.update(wsB, 'canvas', { x: 0 }, platformB);
			await wait(100);
			platformA.reset();
			platformB.reset();

			trackerA.update(wsA, 'canvas', { x: 42, y: 7 }, platformA);

			// Wait for the cross-instance relay to land on B.
			await waitFor(() => platformB.published.some(
				(p) => p.event === 'update' && p.data && p.data.user && p.data.user.id === 'alice'
			));

			const onB = platformB.published.find(
				(p) => p.event === 'update' && p.data && p.data.user && p.data.user.id === 'alice'
			);
			expect(onB.topic).toBe('__cursor:canvas');
			expect(onB.data.data).toEqual({ x: 42, y: 7 });
			expect(onB.options).toEqual({ relay: false });

			// B's list() also reflects the cross-instance write via the
			// shared Redis hash.
			const list = await trackerB.list('canvas');
			const aliceEntry = list.find((e) => e.user && e.user.id === 'alice');
			expect(aliceEntry).toBeDefined();
			expect(aliceEntry.data).toEqual({ x: 42, y: 7 });
		});

		it('two real trackers: A.remove fires a remove event on B and clears B.list of the entry', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = track(createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				select: (ud) => ({ id: ud.id })
			}));
			const trackerB = track(createCursor(client, {
				throttle: 0,
				topicThrottle: 0,
				select: (ud) => ({ id: ud.id })
			}));

			const wsA = mockWs({ id: 'alice' });
			const wsB = mockWs({ id: 'bob' });

			trackerB.update(wsB, 'canvas', { x: 0 }, platformB);
			trackerA.update(wsA, 'canvas', { x: 5 }, platformA);

			// Allow the cross-instance update to propagate so B has alice in
			// its view.
			await waitFor(async () => {
				const list = await trackerB.list('canvas');
				return list.some((e) => e.user && e.user.id === 'alice');
			});
			platformB.reset();

			await trackerA.remove(wsA, platformA, 'canvas');

			// Remove event lands on B via the events channel. Cursor's
			// remove publish is fire-and-forget through the subscriberReady
			// promise chain, so under suite load the wire-arrival of the
			// envelope on B can lag a beat - widen the poll window.
			await waitFor(() => platformB.published.some(
				(p) => p.event === 'remove'
			), 5000);

			const removeOnB = platformB.published.find((p) => p.event === 'remove');
			expect(removeOnB.topic).toBe('__cursor:canvas');
			expect(removeOnB.options).toEqual({ relay: false });

			// B's list no longer contains alice.
			const list = await trackerB.list('canvas');
			expect(list.find((e) => e.user && e.user.id === 'alice')).toBeUndefined();
		});

		it('subscriber backfill: bulk event surfaces existing remote entries on first update', async () => {
			// Pre-seed a remote entry directly into the hash from a
			// separate connection.
			const remoteKey = 'remote-instance:1';
			await client.redis.hset(client.key('cursor:canvas'), remoteKey, JSON.stringify({
				user: { id: '2' }, data: { x: 77 }, ts: Date.now()
			}));

			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0, select: (ud) => ({ id: ud.id }) }));
			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 10 }, platform);

			await waitFor(() => platform.published.some(
				(p) => p.event === 'bulk' && p.options && p.options.relay === false
			));

			const bulk = platform.published.find(
				(p) => p.event === 'bulk' && p.options && p.options.relay === false
			);
			const remote = bulk.data.find((e) => e.key === remoteKey);
			expect(remote).toBeDefined();
			expect(remote.data).toEqual({ x: 77 });
		});
	});

	describe('list() filters stale entries by ts vs ttl', () => {
		it('skips an entry whose ts is older than the configured ttl window', async () => {
			const c = track(createCursor(client, { throttle: 0, topicThrottle: 0, ttl: 5 }));
			const ws = mockWs({ id: '1' });
			c.update(ws, 'canvas', { x: 1 }, platform);
			await waitFor(async () => (await c.list('canvas')).length === 1);

			// Manually overwrite the ts to be older than the ttl window
			// without touching the hash key TTL itself.
			const all = await client.redis.hgetall(client.key('cursor:canvas'));
			const onlyKey = Object.keys(all)[0];
			const parsed = JSON.parse(all[onlyKey]);
			parsed.ts = Date.now() - (10 * 1000); // 10s old, ttl is 5s
			await client.redis.hset(client.key('cursor:canvas'), onlyKey, JSON.stringify(parsed));

			const list = await c.list('canvas');
			expect(list).toEqual([]);
		});
	});
});
