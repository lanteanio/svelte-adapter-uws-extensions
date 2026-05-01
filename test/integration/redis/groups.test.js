/**
 * Integration tests for redis/groups against a real Redis 7 server.
 *
 * Exercises the JOIN_SCRIPT body end-to-end: capacity check excluding stale
 * entries, single-roundtrip atomic insert with stale cleanup, and the closed
 * flag check inside the same EVAL. The cross-instance close detection and the
 * heartbeat/CLEANUP_SCRIPT pair also run against real Redis here. The mock
 * suite at test/redis/groups.test.js stays as-is; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createGroup } from '../../../redis/groups.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis groups (integration)', () => {
	let client;
	let platform;
	/** @type {Array<ReturnType<typeof createGroup>>} */
	let groups;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-groups:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');

		platform = mockPlatform();
		groups = [];
	});

	afterEach(() => {
		for (const g of groups) g.destroy();
	});

	afterAll(async () => {
		await client.quit();
	});

	function makeGroup(name, opts = {}) {
		const g = createGroup(client, name, { memberTtl: 120, ...opts });
		groups.push(g);
		return g;
	}

	describe('JOIN_SCRIPT atomicity', () => {
		it('write a member field and return the live members list', async () => {
			const group = makeGroup('lobby', { maxMembers: 5 });
			const ws = mockWs();
			const ok = await group.join(ws, platform);
			expect(ok).toBe(true);

			const fields = await client.redis.hkeys(client.key('group:lobby:members'));
			expect(fields).toHaveLength(1);

			expect(await group.count()).toBe(1);
			const memberSent = platform.sent.find((s) => s.event === 'members');
			expect(memberSent.data).toEqual([{ role: 'member' }]);
		});

		it('respects maxMembers under concurrent joins (script-side capacity check)', async () => {
			const group = makeGroup('race', { maxMembers: 3 });
			const N = 10;

			const results = await Promise.all(
				Array.from({ length: N }, () => group.join(mockWs(), platform))
			);
			const successes = results.filter(Boolean).length;
			const rejects = results.filter((v) => v === false).length;

			expect(successes).toBe(3);
			expect(rejects).toBe(N - 3);
			expect(await group.count()).toBe(3);
		});

		it('a closed group rejects join via the in-script GET on the closed flag', async () => {
			const group = makeGroup('closed', { maxMembers: 5 });
			await group.close(platform);

			const ok = await group.join(mockWs(), platform);
			expect(ok).toBe(false);
		});

		it('cross-instance close: a peer flag set in Redis blocks join from a fresh instance', async () => {
			// Simulate another instance that already wrote the closed flag.
			await client.redis.set(client.key('group:peer-closed:closed'), '1');

			const group = makeGroup('peer-closed', { maxMembers: 5 });
			const ok = await group.join(mockWs(), platform);
			expect(ok).toBe(false);
		});
	});

	describe('member TTL and stale cleanup inside JOIN_SCRIPT', () => {
		it('a stale member field does NOT count toward maxMembers and is HDELed by the script', async () => {
			const group = makeGroup('stale-room', { maxMembers: 2, memberTtl: 1 });
			await group.join(mockWs(), platform);

			// Plant a stale member field (older than 1s memberTtl).
			await client.redis.hset(
				client.key('group:stale-room:members'),
				'dead-instance:42',
				JSON.stringify({ role: 'member', instanceId: 'dead', ts: Date.now() - 60_000 })
			);

			// At this point Redis hash has 2 fields (1 live, 1 stale). Joining
			// another live member must succeed because liveCount < maxMembers,
			// and the stale field must be HDELed by the script.
			const second = makeGroup('stale-room', { maxMembers: 2, memberTtl: 1 });
			const ok = await second.join(mockWs(), platform);
			expect(ok).toBe(true);

			const fields = await client.redis.hkeys(client.key('group:stale-room:members'));
			expect(fields).not.toContain('dead-instance:42');
			expect(fields).toHaveLength(2);
		});

		it('stale members are excluded from count() (COUNT_SCRIPT timestamp filter)', async () => {
			const group = makeGroup('count-stale', { memberTtl: 1 });
			await group.join(mockWs(), platform);

			// Plant a stale member field.
			await client.redis.hset(
				client.key('group:count-stale:members'),
				'dead-instance:99',
				JSON.stringify({ role: 'member', instanceId: 'dead', ts: Date.now() - 60_000 })
			);

			expect(await group.count()).toBe(1);
		});
	});

	describe('leave + close persistence', () => {
		it('leave removes the member field from the Redis hash', async () => {
			const group = makeGroup('leave-test', { maxMembers: 5 });
			const ws = mockWs();
			await group.join(ws, platform);
			expect(await group.count()).toBe(1);

			await group.leave(ws, platform);
			expect(await group.count()).toBe(0);
			const fields = await client.redis.hkeys(client.key('group:leave-test:members'));
			expect(fields).toHaveLength(0);
		});

		it('close persists the closed flag and clears the members hash', async () => {
			const group = makeGroup('close-test', { maxMembers: 5 });
			await group.join(mockWs(), platform);
			await group.close(platform);

			const flag = await client.redis.get(client.key('group:close-test:closed'));
			expect(flag).toBe('1');
			const exists = await client.redis.exists(client.key('group:close-test:members'));
			expect(exists).toBe(0);
		});

		it('after close, count() reads 0', async () => {
			const group = makeGroup('count-after-close', { maxMembers: 5 });
			await group.join(mockWs(), platform);
			await group.close(platform);
			expect(await group.count()).toBe(0);
		});
	});

	describe('roles and meta', () => {
		it('admin and viewer roles round-trip through the JSON-encoded member value', async () => {
			const group = makeGroup('roles', { maxMembers: 5 });
			const wsA = mockWs();
			const wsV = mockWs();
			await group.join(wsA, platform, 'admin');
			await group.join(wsV, platform, 'viewer');

			const all = await client.redis.hgetall(client.key('group:roles:members'));
			const roles = Object.values(all).map((v) => JSON.parse(v).role).sort();
			expect(roles).toEqual(['admin', 'viewer']);
		});

		it('initial meta lands on the meta hash and getMeta reads it back', async () => {
			const group = makeGroup('meta-test', {
				maxMembers: 5,
				meta: { topic: 'gameplay', created: '2026-01-01' }
			});
			const m = await group.getMeta();
			expect(m).toEqual({ topic: 'gameplay', created: '2026-01-01' });
		});

		it('setMeta persists and replaces meta keys', async () => {
			const group = makeGroup('meta-replace', { maxMembers: 5 });
			await group.setMeta({ a: '1', b: '2' });
			expect(await group.getMeta()).toEqual({ a: '1', b: '2' });

			await group.setMeta({ c: '3' });
			const after = await group.getMeta();
			// hmset overlays; both old and new survive (no replace semantics).
			expect(after).toMatchObject({ c: '3' });
		});
	});

	describe('two-instance interaction over real Redis', () => {
		it('member added by instance-1 is visible in instance-2 count via the shared hash', async () => {
			const g1 = makeGroup('cross-1', { maxMembers: 5 });
			const g2 = makeGroup('cross-1', { maxMembers: 5 });

			await g1.join(mockWs(), platform);
			await g1.join(mockWs(), platform);

			// g2 reads the shared Redis hash via COUNT_SCRIPT.
			expect(await g2.count()).toBe(2);
		});

		it('close on instance-1 sets the flag that blocks join on instance-2', async () => {
			const g1 = makeGroup('cross-close', { maxMembers: 5 });
			const g2 = makeGroup('cross-close', { maxMembers: 5 });

			await g1.close(platform);
			const ok = await g2.join(mockWs(), mockPlatform());
			expect(ok).toBe(false);
		});
	});

	describe('heartbeat refreshes member timestamps in real Redis', () => {
		it('a live member survives past memberTtl thanks to heartbeat refresh', async () => {
			// memberTtlMs < 15000 triggers `Math.floor(memberTtlMs / 3)` ms tick.
			// memberTtl: 3 -> heartbeat ~1000ms. After 1500ms, the original
			// timestamp would be 1500ms old; the heartbeat should have rewritten
			// it so count() still reads 1.
			const group = makeGroup('hb', { maxMembers: 5, memberTtl: 3 });
			await group.join(mockWs(), platform);

			await wait(1500);
			expect(await group.count()).toBe(1);
		});
	});
});
