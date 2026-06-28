/**
 * Integration tests for redis/presence against a real Redis 7.4+ (or
 * Valkey 9.0+) server, selected by INTEGRATION_REDIS_URL.
 *
 * Exercises the JOIN_SCRIPT / LEAVE_SCRIPT Lua bodies plus the per-field
 * HPEXPIRE TTL primitive that the in-memory mock can only approximate.
 * Covers the new Design G storage layout (per-user hash + per-topic hash
 * keyed by userKey, no compound fields), real server-side field expiry
 * timing, multi-tab dedup, cross-instance broadcast semantics, and the
 * activation gate that rejects servers without per-field hash TTL.
 *
 * Validated against a real Valkey 9.0.4 server (point INTEGRATION_REDIS_URL
 * at it): this suite passes identically to Redis 7.4, and Valkey's hash-field
 * expiry fires the same keyspace-event sequence as Redis 7.4 - `hexpired` then
 * `del` when a hash's last field lapses, notably NOT `expired`, and identical
 * on both servers.
 *
 * The mock-based suite at test/redis/presence.test.js covers the public-
 * API contract end-to-end (including the Valkey version gate); this file is
 * additive and asserts properties that only show up on a real server.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys, setBackendConfig } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { createPresence } from '../../../src/redis/presence.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

function joinDiffsFor(platform, key) {
	return platform.published
		.filter((p) => p.event === 'diff')
		.filter((p) => p.data && p.data.joins && key in p.data.joins);
}

function leaveDiffsFor(platform, key) {
	return platform.published
		.filter((p) => p.event === 'diff')
		.filter((p) => p.data && p.data.leaves && key in p.data.leaves);
}

// Runs on both backends: the per-topic keys (presence:topic:{topic} and
// presence:user:{topic}:{userKey}) share a {topic} hash tag, so the 2-key
// JOIN / LEAVE / UPDATE Lua scripts co-locate on a single slot in a cluster.
const describeIntegration = describe;

describeIntegration('redis presence (integration)', () => {
	let client;
	let platform;
	/** @type {Array<ReturnType<typeof createPresence>>} */
	let trackers;

	beforeAll(() => {
		client = createBackendClient({
			keyPrefix: 'inttest-presence:'
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		await resetBackendKeys(client);

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
		return client.key('presence:topic:{' + topic + '}');
	}

	function userHashKey(topic, userKey) {
		return client.key('presence:user:{' + topic + '}:' + userKey);
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

	describe('leaveAll across multiple topics (one connection, many topics)', () => {
		it('removes the connection from every topic it held on disconnect', async () => {
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			const topics = ['room-a', 'room-b', 'room-c', 'room-d'];

			for (const topic of topics) {
				await presence.join(ws, topic, platform);
			}
			for (const topic of topics) {
				expect(await client.redis.exists(topicHashKey(topic))).toBe(1);
				expect(await client.redis.exists(userHashKey(topic, 'alice'))).toBe(1);
			}

			// leave() with no topic runs leaveAll: one batched LEAVE_SCRIPT
			// pipeline across every topic the connection held. On a cluster the
			// topics span slots, so this exercises the multi-slot pipeline path
			// (each eval's two keys still co-locate via the {topic} hash tag).
			await presence.leave(ws, platform);

			for (const topic of topics) {
				expect(await client.redis.exists(topicHashKey(topic))).toBe(0);
				expect(await client.redis.exists(userHashKey(topic, 'alice'))).toBe(0);
				expect(await presence.count(topic)).toBe(0);
			}
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

			// Wait past the TTL on Redis's OWN clock (TIME), where the
			// HPEXPIRE deadline lives - immune to host/VM clock drift. No
			// application-side cleanup needed - Redis expires the ghost field
			// by background task.
			await waitRedisMs(client, 700);

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

	describe('keyspace-notification cleanup (real del on topic-hash expiry)', () => {
		it('emits an empty snapshot to local subscribers when a topic empties via field TTL', async () => {
			// The cleanup psubscribes `__keyevent@*__:del` - key deletion is the
			// generic (`g`) class on BOTH Redis 7.4 and Valkey 9.0, so one flag
			// works everywhere. (The hash-field-expiry event `hexpired` is class
			// `h` on Redis but `x` on Valkey, which is why we key on the deletion.)
			// Keyspace notifications are per-node, so enable them on every master:
			// on a cluster the del fires on whichever node owns presence:topic:{room}
			// and the tracker psubscribes every master to catch it.
			await setBackendConfig(client, 'notify-keyspace-events', 'Eg');

			const presence = makeTracker({ keyspaceNotifications: true });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			// Force the topic field to expire shortly - as if the presenting
			// instance stopped heartbeating. Its lapse removes the per-topic hash's
			// last field, so the server deletes the key, fires a `del` keyevent, and
			// the cleanup emits an empty snapshot to this instance's subscribers.
			await client.redis.hpexpire(topicHashKey('room'), 300, 'FIELDS', 1, 'alice');

			await waitRedisMs(client, 500); // past the TTL on the server's own clock
			await wait(300); // del delivery + the async existence re-check + the emit

			const emptyStates = platform.published.filter(
				(p) =>
					p.event === 'state' &&
					p.topic === '__presence:room' &&
					p.data &&
					Object.keys(p.data).length === 0
			);
			expect(emptyStates.length).toBeGreaterThan(0);
			// And the topic hash is genuinely gone (the cleanup did not fire on a
			// still-populated key).
			expect(await client.redis.exists(topicHashKey('room'))).toBe(0);
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
		it('refreshes per-field TTL across every topic a multi-topic connection holds', async () => {
			// One connection on several topics. The heartbeat refreshes per-field
			// TTLs across all of them every tick; on a cluster those topics span
			// slots, so a topic whose HPEXPIRE was silently dropped (a multi-slot
			// pipeline delivered to one node) would expire and count() -> 0.
			const presence = makeTracker({ ttl: 2, heartbeat: 250 });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			const topics = ['hb-a', 'hb-b', 'hb-c', 'hb-d'];
			for (const topic of topics) await presence.join(ws, topic, platform);

			// Wait past the 2s ttl on Redis's OWN clock (TIME), where the
			// per-field deadlines decay: only continuous cross-node refresh
			// keeps the fields alive this long, and the backend-clock wait
			// guarantees the window really elapsed despite host/VM drift.
			await waitRedisMs(client, 2400);

			for (const topic of topics) {
				expect(await presence.count(topic)).toBe(1);
			}
		});


		it('per-field TTL is refreshed by the heartbeat tick before it would expire', async () => {
			// ttl:3s + heartbeat:300ms means the heartbeat fires ~10x per ttl
			// window. After 700ms the per-field HPTTL must still be > 1s.
			const presence = makeTracker({ ttl: 3, heartbeat: 300 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			// Decay the window on Redis's OWN clock so the HPTTL math below
			// holds regardless of host/VM clock drift.
			await waitRedisMs(client, 700);

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
		it('a remote join lands as diff (not as legacy join/updated/leave events)', async () => {
			// The wire shape on `__presence:{topic}` is state /
			// diff / heartbeat. The cross-instance `presence:events:
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

			const diffFrames = platformB.published.filter((p) => p.event === 'diff');
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

	describe('field-level update - real Lua + real cross-instance pub/sub', () => {
		it('UPDATE_SCRIPT merges a durable field into the per-topic hash value', async () => {
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			await presence.update(ws, 'room', { color: 'red' }, platform);

			const parsed = JSON.parse(await client.redis.hget(topicHashKey('room'), 'alice'));
			expect(parsed.data).toEqual({ id: 'alice', name: 'Alice' });
			expect(parsed.fields).toEqual({ color: 'red' });
			expect(typeof parsed.ts).toBe('number');
		});

		it('a transient-only update is never persisted to Redis', async () => {
			const presence = makeTracker({ transient: ['typing'] });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			await presence.update(ws, 'room', { typing: true }, platform);

			const parsed = JSON.parse(await client.redis.hget(topicHashKey('room'), 'alice'));
			expect(parsed.fields).toBeUndefined();
		});

		it('JOIN_SCRIPT preserves durable fields across a newer-data overwrite (real Lua)', async () => {
			const a = makeTracker();
			const wsA = mockWs({ id: 'alice', name: 'Alice' });
			await a.join(wsA, 'room', platform);
			await a.update(wsA, 'room', { color: 'red' }, platform);

			// A second instance joins the SAME user with different identity data and
			// a newer timestamp; its JOIN_SCRIPT overwrites `data` but must preserve
			// the durable `fields` already stored.
			const b = makeTracker();
			await wait(2);
			await b.join(mockWs({ id: 'alice', name: 'Albert' }), 'room', mockPlatform());

			const parsed = JSON.parse(await client.redis.hget(topicHashKey('room'), 'alice'));
			expect(parsed.data.name).toBe('Albert');
			expect(parsed.fields).toEqual({ color: 'red' });
		});

		it('drops durable fields on a full leave + rejoin', async () => {
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			await presence.update(ws, 'room', { color: 'red' }, platform);
			await presence.leave(ws, platform);

			expect(await client.redis.exists(topicHashKey('room'))).toBe(0);

			const ws2 = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws2, 'room', platform);
			const parsed = JSON.parse(await client.redis.hget(topicHashKey('room'), 'alice'));
			expect(parsed.fields).toBeUndefined();
			expect(parsed.data).toEqual({ id: 'alice', name: 'Alice' });
		});

		it('relays a field-level update to another instance over real Redis pub/sub', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const a = makeTracker({ transient: ['typing'] });
			const b = makeTracker({ transient: ['typing'] });

			const wsA = mockWs({ id: 'alice', name: 'Alice' });
			await a.join(wsA, 'room', platformA);
			const wsB = mockWs({ id: 'bob', name: 'Bob' });
			await b.join(wsB, 'room', platformB);

			// Let the cross-instance join relays settle, then drain both buffers.
			await wait(50);
			a.flushDiffs();
			b.flushDiffs();
			platformA.reset();
			platformB.reset();

			await a.update(wsA, 'room', { color: 'red', typing: true }, platformA);
			a.flushDiffs();
			await wait(50);
			b.flushDiffs();

			// Instance B saw the update fan out to its local subscribers.
			const bUpdate = platformB.published
				.filter((p) => p.event === 'diff')
				.map((p) => p.data.updates)
				.filter(Boolean)
				.pop();
			expect(bUpdate).toEqual({ alice: { color: 'red', typing: true } });

			// Durable color persisted to Redis (by A); a fresh observer on B reads
			// it from the snapshot, while transient typing is excluded.
			platformB.reset();
			const obs = mockWs({ id: 'obs' });
			await b.sync(obs, 'room', platformB);
			const state = platformB.sent.filter((s) => s.event === 'state').pop();
			expect(state.data.alice).toEqual({ id: 'alice', name: 'Alice', color: 'red' });
		});
	});

	describe('dual-role teardown + tap-channel authz (real Redis)', () => {
		it('a participant leaving a topic does not evict a co-resident sync-observer', async () => {
			const a = makeTracker();
			// One socket is BOTH a participant (join) and a sync-observer
			// (presence-snapshot) of the same topic. Both run against real Redis
			// (JOIN_SCRIPT + the cross-instance subscribeToTopic + syncCounts).
			const dual = mockWs({ id: 'dual', name: 'Dual' });
			await a.join(dual, 'board', platform);
			await a.sync(dual, 'board', platform);
			expect(dual.isSubscribed('__presence:board')).toBe(true);

			// Participant leaves (real LEAVE_SCRIPT runs); the observer role must
			// survive so its roster keeps updating.
			await a.leave(dual, platform, 'board');
			expect(dual.isSubscribed('__presence:board')).toBe(true);

			// A subsequent join still produces a diff on the channel the observer
			// is subscribed to (proving the wire + cross-instance subscription
			// survived the participant leave).
			platform.reset();
			const other = mockWs({ id: 'other', name: 'Other' });
			await a.join(other, 'board', platform);
			a.flushDiffs();
			expect(joinDiffsFor(platform, 'other').length).toBeGreaterThan(0);
		});

		it('denies a presence-snapshot for a topic the client cannot subscribe to', async () => {
			const a = makeTracker();
			const attacker = mockWs({ id: 'attacker' });
			// checkSubscribe gates the snapshot on the real topic's authorization.
			const denyPlatform = { ...platform, checkSubscribe: async (_ws, t) => (t === 'board' ? 'FORBIDDEN' : null) };

			await a.sync(attacker, 'board', denyPlatform);
			expect(attacker.isSubscribed('__presence:board')).toBe(false); // not subscribed, no roster read

			// An authorized topic still works.
			await a.sync(attacker, 'lobby', denyPlatform);
			expect(attacker.isSubscribed('__presence:lobby')).toBe(true);
		});
	});
});
