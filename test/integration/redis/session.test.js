/**
 * Integration tests for redis/session against a real Redis 7 server.
 *
 * The mock-based suite at test/redis/session.test.js exhaustively covers
 * the surface; this file pins the wire-level invariants only a real
 * server can prove: actual `SET key json PX ttl` round-trip with PTTL
 * inspection, sliding TTL keeping a session alive past `ttlMs`, real
 * PEXPIRE returning 0 once the key has elapsed, `clear()` SCAN reaching
 * every entry without touching keys outside the prefix, cross-instance
 * read-after-write across two physically separate ioredis connections,
 * and last-writer-wins on concurrent `set` calls.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createDistributedSession } from '../../../redis/session.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis distributed session (integration)', () => {
	let client;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-session:',
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
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('set / get round trip', () => {
		it('writes JSON under the prefixed key with a real PX TTL', async () => {
			const sessions = createDistributedSession(client, { ttlMs: 30_000 });
			await sessions.set('tok-a', { userId: 42, role: 'admin' });

			const raw = await client.redis.get(client.key('sess:tok-a'));
			expect(JSON.parse(raw)).toEqual({ userId: 42, role: 'admin' });

			const pttl = await client.redis.pttl(client.key('sess:tok-a'));
			expect(pttl).toBeGreaterThan(0);
			expect(pttl).toBeLessThanOrEqual(30_000);
		});

		it('reads back the data round-tripped through JSON', async () => {
			const sessions = createDistributedSession(client);
			const payload = { user: { id: 7, name: 'Alice' }, perms: ['read', 'write'] };
			await sessions.set('tok-b', payload);
			expect(await sessions.get('tok-b')).toEqual(payload);
		});

		it('returns null for a missing token', async () => {
			const sessions = createDistributedSession(client);
			expect(await sessions.get('not-set')).toBe(null);
		});

		it('treats a corrupt JSON entry as a miss without throwing', async () => {
			// Plant garbage directly under the session key.
			await client.redis.set(client.key('sess:corrupt'), 'not-json{{{', 'PX', 10_000);
			const sessions = createDistributedSession(client);
			expect(await sessions.get('corrupt')).toBe(null);
			// Subsequent set cleanly overwrites.
			await sessions.set('corrupt', { ok: true });
			expect(await sessions.get('corrupt')).toEqual({ ok: true });
		});
	});

	describe('sliding TTL', () => {
		it('get refreshes the PEXPIRE while the entry is alive (default)', async () => {
			const sessions = createDistributedSession(client, { ttlMs: 600 });
			await sessions.set('tok', { v: 1 });

			// Read repeatedly across what would otherwise be the original
			// expiry window. Each get refreshes; final PTTL is still positive.
			for (let i = 0; i < 6; i++) {
				await wait(150);
				expect(await sessions.get('tok')).toEqual({ v: 1 });
			}
			const pttl = await client.redis.pttl(client.key('sess:tok'));
			expect(pttl).toBeGreaterThan(0);
			expect(pttl).toBeLessThanOrEqual(600);
		});

		it('get does not refresh when refreshOnGet: false', async () => {
			const sessions = createDistributedSession(client, {
				ttlMs: 30_000,
				refreshOnGet: false
			});
			await sessions.set('tok', { v: 1 });

			// Burn some of the original TTL.
			await wait(120);
			const before = await client.redis.pttl(client.key('sess:tok'));
			await sessions.get('tok');
			const after = await client.redis.pttl(client.key('sess:tok'));

			// after must not be larger than before; with refreshOnGet=true it
			// would be back near 30_000. Slack for clock granularity.
			expect(after).toBeLessThanOrEqual(before + 5);
			expect(before - after).toBeGreaterThan(0);
		});

		it('touch returns true and refreshes PEXPIRE while the entry exists', async () => {
			const sessions = createDistributedSession(client, { ttlMs: 30_000 });
			await sessions.set('tok', { v: 1 });
			await wait(80);
			const before = await client.redis.pttl(client.key('sess:tok'));

			expect(await sessions.touch('tok')).toBe(true);
			const after = await client.redis.pttl(client.key('sess:tok'));
			expect(after).toBeGreaterThan(before);
		});

		it('touch returns false once the key has truly elapsed', async () => {
			// Real PEXPIRE returns 0 when the key is absent. A short PX lets
			// us observe the natural-elapsed transition without sleeping
			// long enough to slow the suite.
			const sessions = createDistributedSession(client, { ttlMs: 100 });
			await sessions.set('short', { v: 1 });

			// Wait past the PX so the key is genuinely gone.
			await wait(250);
			expect(await client.redis.exists(client.key('sess:short'))).toBe(0);
			expect(await sessions.touch('short')).toBe(false);
		});
	});

	describe('delete', () => {
		it('returns true and removes the key when present', async () => {
			const sessions = createDistributedSession(client);
			await sessions.set('tok', { v: 1 });
			expect(await sessions.delete('tok')).toBe(true);
			expect(await client.redis.exists(client.key('sess:tok'))).toBe(0);
			expect(await sessions.get('tok')).toBe(null);
		});

		it('returns false when the token was never set', async () => {
			const sessions = createDistributedSession(client);
			expect(await sessions.delete('never')).toBe(false);
		});
	});

	describe('clear (SCAN + UNLINK)', () => {
		it('removes every entry under the session keyPrefix', async () => {
			const sessions = createDistributedSession(client);
			for (let i = 0; i < 25; i++) {
				await sessions.set(`tok-${i}`, { i });
			}
			await sessions.clear();
			for (let i = 0; i < 25; i++) {
				expect(await sessions.get(`tok-${i}`)).toBe(null);
			}
		});

		it('does not touch keys outside the session prefix', async () => {
			const sessions = createDistributedSession(client);
			await sessions.set('tok-1', { v: 1 });
			// Plant unrelated keys under the same client prefix but a
			// different sub-prefix. clear() must not touch these.
			await client.redis.set(client.key('other:keep-1'), 'k1');
			await client.redis.set(client.key('lock:keep-2'), 'k2');

			await sessions.clear();

			expect(await sessions.get('tok-1')).toBe(null);
			expect(await client.redis.get(client.key('other:keep-1'))).toBe('k1');
			expect(await client.redis.get(client.key('lock:keep-2'))).toBe('k2');
		});

		it('honors a custom keyPrefix on the wire', async () => {
			const sessions = createDistributedSession(client, { keyPrefix: 'usess:' });
			await sessions.set('tok', { v: 1 });

			expect(await client.redis.exists(client.key('usess:tok'))).toBe(1);
			expect(await client.redis.exists(client.key('sess:tok'))).toBe(0);
		});
	});

	describe('cross-instance', () => {
		it('write on instance A is readable on instance B', async () => {
			const url = process.env.INTEGRATION_REDIS_URL;
			const clientA = createRedisClient({ url, keyPrefix: 'inttest-session:', autoShutdown: false });
			const clientB = createRedisClient({ url, keyPrefix: 'inttest-session:', autoShutdown: false });
			try {
				const sessionsA = createDistributedSession(clientA);
				const sessionsB = createDistributedSession(clientB);

				await sessionsA.set('shared-tok', { from: 'A', payload: [1, 2, 3] });
				expect(await sessionsB.get('shared-tok')).toEqual({ from: 'A', payload: [1, 2, 3] });

				// B's delete is visible to A.
				await sessionsB.delete('shared-tok');
				expect(await sessionsA.get('shared-tok')).toBe(null);
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});

		it('concurrent set on two instances resolves to last-writer-wins', async () => {
			const url = process.env.INTEGRATION_REDIS_URL;
			const clientA = createRedisClient({ url, keyPrefix: 'inttest-session:', autoShutdown: false });
			const clientB = createRedisClient({ url, keyPrefix: 'inttest-session:', autoShutdown: false });
			try {
				const sessionsA = createDistributedSession(clientA);
				const sessionsB = createDistributedSession(clientB);

				// Order the two sets so B's lands strictly after A's.
				await sessionsA.set('race', { writer: 'A' });
				await sessionsB.set('race', { writer: 'B' });

				// Both readers see B's write.
				expect(await sessionsA.get('race')).toEqual({ writer: 'B' });
				expect(await sessionsB.get('race')).toEqual({ writer: 'B' });
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});
	});
});
