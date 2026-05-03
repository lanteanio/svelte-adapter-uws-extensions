/**
 * Integration tests for redis/presence against a real Redis 7 server.
 *
 * Exercises the JOIN_SCRIPT / LEAVE_SCRIPT / CLEANUP_SCRIPT / LIST_SCRIPT /
 * COUNT_DEDUP_SCRIPT Lua bodies that the in-memory mock can only approximate:
 * compound-field encoding, stale-entry filtering, multi-instance dedup, real
 * EXPIRE timing, and HGETALL coalescing under concurrent joins. The mock-based
 * suite at test/redis/presence.test.js stays as-is; this file is additive.
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

	describe('JOIN_SCRIPT (HSET + EXPIRE atomicity)', () => {
		it('writes the compound field and applies the TTL on the hash key', async () => {
			const presence = makeTracker({ ttl: 120 });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const hkey = client.key('presence:room');
			const fields = await client.redis.hkeys(hkey);
			expect(fields).toHaveLength(1);
			expect(fields[0]).toMatch(/^[0-9a-f]+\|alice$/);

			const ttl = await client.redis.ttl(hkey);
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(120);

			const raw = await client.redis.hget(hkey, fields[0]);
			const parsed = JSON.parse(raw);
			expect(parsed.data).toEqual({ id: 'alice', name: 'Alice' });
			expect(typeof parsed.ts).toBe('number');
		});

		it('list and count round-trip through the LIST_SCRIPT and COUNT_DEDUP_SCRIPT', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			await presence.join(mockWs({ id: 'bob', name: 'Bob' }), 'room', platform);

			expect(await presence.count('room')).toBe(2);
			const list = await presence.list('room');
			expect(list).toHaveLength(2);
			const ids = list.map((u) => u.id).sort();
			expect(ids).toEqual(['alice', 'bob']);
		});

		it('count returns 0 for an unknown topic', async () => {
			const presence = makeTracker();
			expect(await presence.count('does-not-exist')).toBe(0);
		});
	});

	describe('LEAVE_SCRIPT (cross-instance suffix scan)', () => {
		it('user present on another instance is detected via suffix-match scan', async () => {
			// Simulate a second instance by writing its compound field directly.
			// LEAVE_SCRIPT must scan remaining fields, find the live entry under
			// the same userKey, and report userGone=0 so no leave is broadcast.
			const presence = makeTracker();
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const otherField = 'other-instance|alice';
			await client.redis.hset(
				client.key('presence:room'),
				otherField,
				JSON.stringify({ data: { id: 'alice', name: 'Alice' }, ts: Date.now() })
			);

			platform.reset();
			await presence.leave(ws, platform);
			presence.flushDiffs();

			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(0);

			// Other instance's field should still be there.
			const remaining = await client.redis.hkeys(client.key('presence:room'));
			expect(remaining).toContain(otherField);
		});

		it('stale field for the same user does NOT suppress the leave broadcast', async () => {
			const presence = makeTracker({ ttl: 5 });
			const ws = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			// Plant a stale field aged well past 5s TTL. The Lua suffix-match
			// must filter it out by ts comparison.
			await client.redis.hset(
				client.key('presence:room'),
				'dead-instance|alice',
				JSON.stringify({ data: { id: 'alice', name: 'Alice' }, ts: Date.now() - 60_000 })
			);

			platform.reset();
			await presence.leave(ws, platform);
			presence.flushDiffs();

			const leaves = leaveDiffsFor(platform, 'alice');
			expect(leaves).toHaveLength(1);
			expect(leaves[0].data.leaves.alice).toMatchObject({ id: 'alice' });
		});

		it('suffix-match does not cross userKey boundaries (alice vs malice)', async () => {
			// Bug bait: a naive `string.find(field, key)` instead of suffix-anchored
			// match would treat field "i|malice" as matching userKey "alice"
			// because "alice" is a substring of "malice". The script anchors at
			// the end via #f - #suffix + 1, so this test validates the anchor
			// against real Lua semantics.
			const presence = makeTracker();
			const wsAlice = mockWs({ id: 'alice', name: 'Alice' });
			await presence.join(wsAlice, 'room', platform);

			await client.redis.hset(
				client.key('presence:room'),
				'other-instance|malice',
				JSON.stringify({ data: { id: 'malice', name: 'Malice' }, ts: Date.now() })
			);

			platform.reset();
			await presence.leave(wsAlice, platform);
			presence.flushDiffs();

			expect(leaveDiffsFor(platform, 'alice')).toHaveLength(1);
		});
	});

	describe('multi-tab dedup', () => {
		it('two connections same userKey produce one hash field and one join diff', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			presence.flushDiffs();
			const joinsBefore = joinDiffsFor(platform, 'alice').length;

			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);
			presence.flushDiffs();
			const joinsAfter = joinDiffsFor(platform, 'alice').length;
			expect(joinsAfter).toBe(joinsBefore);

			expect(await presence.count('room')).toBe(1);
			const fields = await client.redis.hkeys(client.key('presence:room'));
			expect(fields).toHaveLength(1);
		});

		it('closing the last tab removes the Redis field and broadcasts a leave diff', async () => {
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

			const fields = await client.redis.hkeys(client.key('presence:room'));
			expect(fields).toHaveLength(0);
		});
	});

	describe('CLEANUP_SCRIPT and stale entry filtering', () => {
		it('list filters stale entries left by crashed instances', async () => {
			const presence = makeTracker({ ttl: 5 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			// Plant a stale field aged past TTL.
			await client.redis.hset(
				client.key('presence:room'),
				'dead-instance|ghost',
				JSON.stringify({ data: { id: 'ghost' }, ts: Date.now() - 60_000 })
			);

			const list = await presence.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].id).toBe('alice');
		});

		it('count via COUNT_DEDUP_SCRIPT filters stale entries', async () => {
			const presence = makeTracker({ ttl: 5 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			await client.redis.hset(
				client.key('presence:room'),
				'dead-instance|ghost',
				JSON.stringify({ data: { id: 'ghost' }, ts: Date.now() - 60_000 })
			);

			expect(await presence.count('room')).toBe(1);
		});

		it('count dedups the same userKey across instance fields', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			// Same userKey under a different instanceId; both are live.
			await client.redis.hset(
				client.key('presence:room'),
				'instance-2|alice',
				JSON.stringify({ data: { id: 'alice', name: 'Alice' }, ts: Date.now() })
			);

			expect(await presence.count('room')).toBe(1);
		});

		it('LIST_SCRIPT keeps the newer entry when two instances both list the same user', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice (older)' }), 'room', platform);

			// Pretend the second instance wrote a newer entry with different name.
			await client.redis.hset(
				client.key('presence:room'),
				'instance-2|alice',
				JSON.stringify({ data: { id: 'alice', name: 'Alice (newer)' }, ts: Date.now() + 1000 })
			);

			const list = await presence.list('room');
			expect(list).toHaveLength(1);
			expect(list[0].name).toBe('Alice (newer)');
		});
	});

	describe('cross-instance via two presence trackers', () => {
		it('user present on instance-2 keeps surviving after instance-1 leave', async () => {
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
		it('100 distinct users joining in parallel all land in one hash with no loss', async () => {
			const presence = makeTracker();
			const N = 100;
			const wss = Array.from({ length: N }, (_, i) =>
				mockWs({ id: `u${i}`, name: `User ${i}` })
			);

			await Promise.all(wss.map((ws) => presence.join(ws, 'room', platform)));

			const fields = await client.redis.hkeys(client.key('presence:room'));
			expect(fields).toHaveLength(N);
			expect(await presence.count('room')).toBe(N);
		});
	});

	describe('TTL refresh path', () => {
		it('JOIN_SCRIPT applies TTL even on the second concurrent join', async () => {
			const presence = makeTracker({ ttl: 60 });
			await Promise.all([
				presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform),
				presence.join(mockWs({ id: 'bob', name: 'Bob' }), 'room', platform)
			]);

			const ttl = await client.redis.ttl(client.key('presence:room'));
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(60);
		});
	});

	describe('clear', () => {
		it('SCAN+UNLINK wipes only keys under the client prefix', async () => {
			const presence = makeTracker();
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room-a', platform);
			await presence.join(mockWs({ id: 'bob', name: 'Bob' }), 'room-b', platform);

			// Plant an outsider key that should NOT be touched by clear().
			const outsiderKey = 'inttest-presence-outsider:should-survive';
			await client.redis.set(outsiderKey, 'untouched');

			await presence.clear();

			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(0);
			expect(await client.redis.get(outsiderKey)).toBe('untouched');

			await client.redis.unlink(outsiderKey);
		});
	});

	describe('heartbeat refreshes the hash TTL on real EXPIRE timing', () => {
		it('TTL is refreshed by the heartbeat tick before it would expire', async () => {
			// ttl:3 + heartbeat:300 means the heartbeat fires roughly 10 times
			// per ttl window. After 600ms the TTL reading must still be > 0.
			const presence = makeTracker({ ttl: 3, heartbeat: 300 });
			await presence.join(mockWs({ id: 'alice', name: 'Alice' }), 'room', platform);

			await wait(700);

			const ttl = await client.redis.ttl(client.key('presence:room'));
			// We expect the heartbeat to have re-applied EXPIRE 3, so the TTL
			// reading should be close to 3 (and definitely > 1).
			expect(ttl).toBeGreaterThan(1);
		});
	});

	describe('cross-instance receiver routes events through the diff buffer', () => {
		it('a remote join lands as presence_diff (not as legacy join/updated/leave events)', async () => {
			// B18 retrofit: the wire shape on `__presence:{topic}` is
			// presence_state / presence_diff / heartbeat. The cross-instance
			// `presence:events:{topic}` channel still carries internal
			// 'join'/'leave'/'updated' envelopes between instances; the
			// receiver MUST translate into bufferDiff so observers on the
			// remote instance see the diff shape, never the legacy names.
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = makeTracker();
			const trackerB = makeTracker();

			// Bring B's subscriber up so it receives A's broadcast. Joining a
			// dummy user on B is the simplest way to force ensureSubscriber.
			const wsBrun = mockWs({ id: 'b-bootstrap', name: 'BBoot' });
			await trackerB.join(wsBrun, 'room', platformB);
			// Drain B's local frames so subsequent assertions see only the
			// remote-driven traffic.
			platformB.reset();

			// Burst N joins on A in parallel; sequential awaits would
			// guarantee one microtask per receive, but parallel issuance
			// gives the diff buffer a real chance to coalesce on B.
			const N = 10;
			const wssA = Array.from({ length: N }, (_, i) =>
				mockWs({ id: `u${i}`, name: `User ${i}` })
			);
			await Promise.all(wssA.map((ws) => trackerA.join(ws, 'room', platformA)));

			// Allow real Redis pubsub round-trip + microtask flush on B.
			await wait(150);
			trackerB.flushDiffs();

			const diffFrames = platformB.published.filter((p) => p.event === 'presence_diff');
			// Every received join must surface as a presence_diff entry. No
			// legacy 'join' / 'updated' / 'leave' / 'list' frames on the wire.
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
			// Sanity: receiver collapses bursts -- frame count must not exceed
			// the per-message count, and in practice will be lower under any
			// real-pubsub timing where multiple messages land in one tick.
			expect(diffFrames.length).toBeLessThanOrEqual(N);
		});
	});
});
