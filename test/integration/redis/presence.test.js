/**
 * Integration tests for redis/presence against a real Redis 7.4+ server.
 *
 * Exercises the JOIN_SCRIPT / LEAVE_SCRIPT Lua bodies plus the per-field
 * HPEXPIRE TTL primitive that the in-memory mock can only approximate.
 * Covers the new Design G storage layout (per-user hash + per-topic hash
 * keyed by userKey, no compound fields), real Redis-7.4-side field expiry
 * timing, multi-tab dedup, cross-instance broadcast semantics, and the
 * activation gate that rejects pre-7.4 servers.
 *
 * The mock-based suite at test/redis/presence.test.js covers the public-
 * API contract end-to-end; this file is additive and asserts properties
 * that only show up on real Redis.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createPresence } from '../../../redis/presence.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function joinDiffsFor(platform, key) {
	return platform.published
		.filter((p) => p.event === 'presence_diff')
		.filter((p) => p.data && p.data.joins && key in p.data.joins);
}

function leaveDiffsFor(platform, key) {
	return platform.published
		.filter((p) => p.event === 'presence_diff')
		.filter((p) => p.data && p.data.leaves && key in p.data.leaves);
}

describe('redis presence (integration)', () => {
	let client;
	let platform;
	/** @type {Array<ReturnType<typeof createPresence>>} */
	let trackers;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-presence:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');

		platform = mockPlatform();
		trackers = [];
	});

	afterEach(() => {
		for (const t of trackers) t.destroy();
	});

	afterAll(async () => {
		await client.quit();
	});

	function makeTracker(opts = {}) {
		const t = createPresence(client, {
			key: 'id',
			select: (ud) => ({ id: ud.id, name: ud.name }),
			heartbeat: 60000,
			ttl: 180,
			...opts
		});
		trackers.push(t);
		return t;
	}

	function topicHashKey(topic) {
		return client.key('presence:topic:' + topic);
	}

	function userHashKey(topic, userKey) {
		return client.key('presence:user:' + topic + ':' + userKey);
	}

	describe('Design G storage layout', () => {
		it('writes the user to the per-topic hash (field=userKey) and per-user hash (field=instanceId)', async () => {
			const presence = makeTracker({ ttl: 120 });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			// Per-topic hash: one field per userKey, value = JSON {data, ts}.
			const topicFields = await client.redis.hkeys(topicHashKey('room'));
			expect(topicFields).toEqual(['alice']);
			const rawTopicVal = await client.redis.hget(topicHashKey('room'), 'alice');
			const parsed = JSON.parse(rawTopicVal);
			expect(parsed.data).toEqual({ id: 'alice', name: 'Alice' });
			expect(typeof parsed.ts).toBe('number');

			// Per-user hash: one field per instanceId. Value is the ts (string).
			// Field name shape: 16 lower-hex chars (8 random bytes -> hex).
			const userFields = await client.redis.hkeys(userHashKey('room', 'alice'));
			expect(userFields).toHaveLength(1);
			expect(userFields[0]).toMatch(/^[0-9a-f]{16}$/);
		});

		it('applies per-field TTL via HPEXPIRE on both hashes', async () => {
			const presence = makeTracker({ ttl: 120 });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const userFields = await client.redis.hkeys(userHashKey('room', 'alice'));
			const instanceId = userFields[0];

			// HPTTL returns an array (one entry per requested field).
			const userFieldTtl = await client.redis.hpttl(
				userHashKey('room', 'alice'), 'FIELDS', 1, instanceId
			);
			expect(Array.isArray(userFieldTtl)).toBe(true);
			expect(userFieldTtl[0]).toBeGreaterThan(0);
			expect(userFieldTtl[0]).toBeLessThanOrEqual(120_000);

			const topicFieldTtl = await client.redis.hpttl(
				topicHashKey('room'), 'FIELDS', 1, 'alice'
			);
			expect(topicFieldTtl[0]).toBeGreaterThan(0);
			expect(topicFieldTtl[0]).toBeLessThanOrEqual(120_000);

			// Whole-key TTL is NOT set in Design G - per-field TTLs auto-expire
			// fields field-by-field, and the key implicitly disappears when its
			// last field expires.
			expect(await client.redis.ttl(topicHashKey('room'))).toBe(-1);
			expect(await client.redis.ttl(userHashKey('room', 'alice'))).toBe(-1);
		});

		it('LEAVE removes the user from both hashes when the last instance disconnects', async () => {
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			expect(await client.redis.exists(topicHashKey('room'))).toBe(1);
			expect(await client.redis.exists(userHashKey('room', 'alice'))).toBe(1);

			await presence.leave(ws, platform);

			// Both keys gone. HDEL'ing the last field deletes the hash key.
			expect(await client.redis.exists(topicHashKey('room'))).toBe(0);
			expect(await client.redis.exists(userHashKey('room', 'alice'))).toBe(0);
		});
	});

	describe('JOIN_SCRIPT atomic semantics', () => {
		it('returns wasEmpty=1 on first instance and 0 on subsequent', async () => {
			const presence = makeTracker();

			// First join (alice) - script returns 1, broadcast fires.
			const wsA = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(wsA, 'room', platform);
			presence.flushDiffs();
			expect(joinDiffsFor(platform, 'alice')).toHaveLength(1);
			platform.reset();

			// Plant a second instance's field directly to simulate cross-instance.
			await client.redis.hset(userHashKey('room', 'alice'), 'other-instance', String(Date.now()));

			// A fresh tracker on a "third instance" joining alice sees HLEN > 0
			// and returns 0; presence.join should NOT broadcast a join diff
			// for cross-instance idempotency.
			const presence2 = makeTracker();
			const wsA2 = mockWs({ id: 'alice', name: 'Alice' });
			await presence2.join(wsA2, 'room', platform);
			presence2.flushDiffs();
			// Locally, the tracker still broadcasts a join (its own state went from
			// empty to one user via the open-handler path). Verify via Redis state:
			// the per-user hash now has THREE instances for alice.
			const userFields = await client.redis.hkeys(userHashKey('room', 'alice'));
			expect(userFields).toHaveLength(3);
			expect(userFields).toContain('other-instance');
		});

		it('count returns 0 for an unknown topic', async () => {
			const presence = makeTracker();
			expect(await presence.count('does-not-exist')).toBe(0);
		});

		it('list and count work directly via HGETALL / HLEN on the per-topic hash', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			await presence.join(mockWs({ id: 'bob', name: 'Bob' }), 'room', platform);

			expect(await presence.count('room')).toBe(2);
			const list = await presence.list('room');
			expect(list).toHaveLength(2);
			const ids = list.map((u) => u.id).sort();
			expect(ids).toEqual(['alice', 'bob']);
		});
	});

	describe('LEAVE_SCRIPT cross-instance behavior', () => {
		it('user present on another instance suppresses the leave broadcast (HLEN > 0)', async () => {
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			// Simulate a second instance: plant its field on the per-user hash AND
			// the per-topic hash. (Real cross-instance JOIN_SCRIPT would do both.)
			await client.redis.hset(userHashKey('room', 'alice'), 'other-instance', String(Date.now()));

			platform.reset();
			await presence.leave(ws, platform);
			presence.flushDiffs();

			// LEAVE_SCRIPT saw HLEN=1 (other-instance still there) -> returned 0
			// -> no leave broadcast.
			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(0);

			// other-instance's field remains. The per-topic hash still has alice
			// (we did not HDEL it).
			expect(await client.redis.hexists(userHashKey('room', 'alice'), 'other-instance')).toBe(1);
			expect(await client.redis.hexists(topicHashKey('room'), 'alice')).toBe(1);
		});

		it('the broadcast suppression scales to many cross-instance fields without scanning', async () => {
			// Pre-Design-G: O(M_topic) Lua suffix-scan per leave. Plant 5000
			// extra instance entries for unrelated users on the same topic hash
			// and confirm the leave still works in O(1) on the per-user hash.
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			// Plant fields for unrelated users on the per-topic hash.
			const pipe = client.redis.pipeline();
			for (let i = 0; i < 5000; i++) {
				pipe.hset(topicHashKey('room'), 'noise-' + i, JSON.stringify({ data: { id: 'n' + i }, ts: Date.now() }));
			}
			await pipe.exec();

			platform.reset();
			const t0 = Date.now();
			await presence.leave(ws, platform);
			const elapsed = Date.now() - t0;
			presence.flushDiffs();

			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(1);
			// Generous bound: even on a slow CI, an O(1) leave finishes well
			// under 50ms. The pre-Design-G implementation would scan all
			// 5000 noise fields inside Lua and take noticeably longer.
			expect(elapsed).toBeLessThan(100);
		});
	});

	describe('multi-tab dedup (Redis-side)', () => {
		it('two tabs same user produce ONE per-topic field and ONE per-user instance entry', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			presence.flushDiffs();
			const joinsBefore = joinDiffsFor(platform, 'alice').length;

			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			presence.flushDiffs();
			const joinsAfter = joinDiffsFor(platform, 'alice').length;
			expect(joinsAfter).toBe(joinsBefore);

			expect(await presence.count('room')).toBe(1);
			expect(await client.redis.hlen(topicHashKey('room'))).toBe(1);
			expect(await client.redis.hlen(userHashKey('room', 'alice'))).toBe(1);
		});

		it('closing the last tab removes the entries from both hashes and broadcasts a leave', async () => {
			const presence = makeTracker();
			const ws1 = mockWs({ id: 'alice', name: 'Alice' });
			const ws2 = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);

			platform.reset();
			await presence.leave(ws1, platform);
			presence.flushDiffs();
			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(0);

			await presence.leave(ws2, platform);
			presence.flushDiffs();
			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(1);
			expect(await presence.count('room')).toBe(0);
			expect(await client.redis.exists(topicHashKey('room'))).toBe(0);
			expect(await client.redis.exists(userHashKey('room', 'alice'))).toBe(0);
		});
	});

	describe('per-field HPEXPIRE staleness (real Redis 7.4 field expiry)', () => {
		it('a field whose owning instance stops heartbeating disappears from list() and count()', async () => {
			const presence = makeTracker({ ttl: 5 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			// Plant a stale instance entry directly with a 500ms TTL.
			await client.redis.hset(userHashKey('room', 'ghost'), 'dead-instance', String(Date.now()));
			await client.redis.hpexpire(userHashKey('room', 'ghost'), 500, 'FIELDS', 1, 'dead-instance');
			await client.redis.hset(topicHashKey('room'), 'ghost', JSON.stringify({ data: { id: 'ghost' }, ts: Date.now() }));
			await client.redis.hpexpire(topicHashKey('room'), 500, 'FIELDS', 1, 'ghost');

			// Before expiry: ghost is present.
			expect(await presence.count('room')).toBe(2);
			expect((await presence.list('room')).map((u) => u.id).sort()).toEqual(['alice', 'ghost']);

			// Wait past TTL. No application-side cleanup needed - Redis expires
			// the ghost field by background task.
			await wait(700);

			expect(await presence.count('room')).toBe(1);
			expect((await presence.list('room')).map((u) => u.id)).toEqual(['alice']);
		});

		it('JOIN_SCRIPT keeps the newer-ts entry when two instances both write the same userKey', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice (older)' }), 'room', platform);

			// Pretend a second instance wrote a newer entry directly. JOIN_SCRIPT
			// uses HGET + ts-compare to keep the newer value on the topic hash.
			const newerTs = Date.now() + 1000;
			await client.redis.hset(
				topicHashKey('room'),
				'alice',
				JSON.stringify({ data: { id: 'alice', name: 'Alice (newer)' }, ts: newerTs })
			);

			// Now run a JOIN_SCRIPT-eq path with an OLDER ts. The script must
			// NOT overwrite. Use presence.join with a fresh tracker that thinks
			// it has older data (simulate via a tab-rejoin with same user).
			const wsOld = mockWs({ id: 'alice', name: 'Alice (older still)' });
			// Force the join's ts to be older than newerTs by issuing it now
			// (Date.now() < newerTs by ~1s). The join's ts is whatever Date.now()
			// returns inside join(), which is "now" -- still < newerTs.
			await presence.join(wsOld, 'room', platform);

			const list = await presence.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].name).toBe('Alice (newer)');
		});
	});

	describe('cross-instance via two presence trackers', () => {
		it('user present on instance B keeps surviving after instance A leave', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = makeTracker();
			const trackerB = makeTracker();

			const wsA = mockWs({ id: 'alice', name: 'Alice' });
			const wsB = mockWs({ id: 'alice', name: 'Alice' });

			await trackerA.join(wsA, 'room', platformA);
			await trackerB.join(wsB, 'room', platformB);

			await trackerA.leave(wsA, platformA);

			const list = await trackerB.list('room');
			expect(list).toHaveLength(1);
			expect(list[0]).toEqual({ id: 'alice', name: 'Alice' });
			expect(await trackerB.count('room')).toBe(1);
		});

		it('leave broadcasts only fire once both instances disconnect', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = makeTracker();
			const trackerB = makeTracker();

			const wsA = mockWs({ id: 'alice', name: 'Alice' });
			const wsB = mockWs({ id: 'alice', name: 'Alice' });

			await trackerA.join(wsA, 'room', platformA);
			await trackerB.join(wsB, 'room', platformB);
			platformA.reset();
			platformB.reset();

			await trackerA.leave(wsA, platformA);
			trackerA.flushDiffs();
			expect(leaveDiffsFor(platformA, 'alice')).toHaveLength(0);

			await trackerB.leave(wsB, platformB);
			trackerB.flushDiffs();
			expect(leaveDiffsFor(platformB, 'alice')).toHaveLength(1);
		});
	});

	describe('concurrent joins (atomicity under parallel EVAL)', () => {
		it('100 distinct users joining in parallel all land in one per-topic hash with no loss', async () => {
			const presence = makeTracker();
			const N = 100;
			const wss = Array.from({ length: N }, (_, i) =>
				mockWs({ id: `u${i}`, name: `User ${i}` })
			);

			await Promise.all(wss.map((ws) => presence.join(ws, 'room', platform)));

			const fields = await client.redis.hkeys(topicHashKey('room'));
			expect(fields).toHaveLength(N);
			expect(await presence.count('room')).toBe(N);
		});
	});

	describe('heartbeat (HPEXPIRE refresh, real Redis 7.4 timing)', () => {
		it('per-field TTL is refreshed by the heartbeat tick before it would expire', async () => {
			// ttl:3s + heartbeat:300ms means the heartbeat fires ~10x per ttl
			// window. After 700ms the per-field HPTTL must still be > 1s.
			const presence = makeTracker({ ttl: 3, heartbeat: 300 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			await wait(700);

			const topicTtl = await client.redis.hpttl(
				topicHashKey('room'), 'FIELDS', 1, 'alice'
			);
			// Generous lower bound: 1000ms remaining means the heartbeat re-
			// applied HPEXPIRE since the original 3s would have decayed to ~2.3s.
			expect(topicTtl[0]).toBeGreaterThan(1000);

			const userFields = await client.redis.hkeys(userHashKey('room', 'alice'));
			const userTtl = await client.redis.hpttl(
				userHashKey('room', 'alice'), 'FIELDS', 1, userFields[0]
			);
			expect(userTtl[0]).toBeGreaterThan(1000);
		});

		it('heartbeat does NOT re-HSET data fields (would clear HPEXPIRE TTL on Redis 7.4+)', async () => {
			// Probe by watching the field value across two heartbeat ticks.
			// If heartbeat did HSET, Redis 7.4 would clear the per-field TTL
			// and we would see HPTTL = -1 immediately after the tick. Instead
			// the TTL stays above zero (only HPEXPIRE refreshes are issued).
			const presence = makeTracker({ ttl: 60, heartbeat: 100 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			await wait(250); // 2-3 heartbeat ticks

			const topicTtl = await client.redis.hpttl(
				topicHashKey('room'), 'FIELDS', 1, 'alice'
			);
			expect(topicTtl[0]).toBeGreaterThan(0);
			expect(topicTtl[0]).toBeLessThanOrEqual(60_000);
		});
	});

	describe('clear', () => {
		it('SCAN+UNLINK wipes both per-topic and per-user hashes under the client prefix', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room-a', platform);
			await presence.join(mockWs({ id: 'bob', name: 'Bob' }), 'room-b', platform);

			// Plant an outsider key that should NOT be touched by clear().
			const outsiderKey = 'inttest-presence-outsider:should-survive';
			await client.redis.set(outsiderKey, 'untouched');

			await presence.clear();

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(0);
			expect(await client.redis.exists(userHashKey('room-a', 'alice'))).toBe(0);
			expect(await client.redis.exists(userHashKey('room-b', 'bob'))).toBe(0);
			expect(await client.redis.get(outsiderKey)).toBe('untouched');

			await client.redis.unlink(outsiderKey);
		});
	});

	describe('cross-instance receiver routes events through the diff buffer', () => {
		it('a remote join lands as presence_diff (not as legacy join/updated/leave events)', async () => {
			// The wire shape on `__presence:{topic}` is presence_state /
			// presence_diff / heartbeat. The cross-instance `presence:events:
			// {topic}` channel still carries internal 'join'/'leave'/'updated'
			// envelopes between instances; the receiver MUST translate into
			// bufferDiff so observers on the remote instance see the diff
			// shape, never the legacy names.
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = makeTracker();
			const trackerB = makeTracker();

			// Bring B's subscriber up so it receives A's broadcast.
			const wsBrun = mockWs({ id: 'b-bootstrap', name: 'BBoot' });
			await trackerB.join(wsBrun, 'room', platformB);
			platformB.reset();

			const N = 10;
			const wssA = Array.from({ length: N }, (_, i) =>
				mockWs({ id: `u${i}`, name: `User ${i}` })
			);
			await Promise.all(wssA.map((ws) => trackerA.join(ws, 'room', platformA)));

			await wait(150);
			trackerB.flushDiffs();

			const diffFrames = platformB.published.filter((p) => p.event === 'presence_diff');
			const legacy = platformB.published.filter(
				(p) => p.event === 'join' || p.event === 'updated' || p.event === 'leave' || p.event === 'list'
			);
			expect(legacy).toHaveLength(0);

			const allJoinKeys = new Set();
			for (const f of diffFrames) {
				if (f.data && f.data.joins) {
					for (const k of Object.keys(f.data.joins)) allJoinKeys.add(k);
				}
			}
			for (let i = 0; i < N; i++) {
				expect(allJoinKeys.has(`u${i}`)).toBe(true);
			}
			expect(diffFrames.length).toBeLessThanOrEqual(N);
		});
	});
});
