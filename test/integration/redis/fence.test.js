/**
 * Integration tests for redis/fence against a real Redis 7 server.
 *
 * Exercises the HEARTBEAT_SCRIPT and RELEASE_SCRIPT value-match guards
 * end-to-end: real GET-then-PEXPIRE / GET-then-DEL atomicity, real EX/PEXPIRE
 * timing on the fence key, and the force-takeover detection that the mock
 * suite (test/redis/fence.test.js) approximates by mutating its in-memory
 * store. This file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createRedisFence } from '../../../redis/fence.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis fence (integration)', () => {
	let client;
	let fence;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-fence:',
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

		fence = createRedisFence(client);
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('acquire (SET ... EX)', () => {
		it('writes the fence value under the prefixed key with a real TTL', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			const v = await client.redis.get(client.key('fence:task-1'));
			expect(v).toBe('fence-a');
			const ttl = await client.redis.ttl(client.key('fence:task-1'));
			expect(ttl).toBeGreaterThan(0);
			expect(ttl).toBeLessThanOrEqual(30);
		});

		it('overwrites a prior fence value (last writer wins on plain SET)', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			await fence.acquire('task-1', 'fence-b', 30);
			const v = await client.redis.get(client.key('fence:task-1'));
			expect(v).toBe('fence-b');
		});

		it('respects a custom keyPrefix', async () => {
			const f = createRedisFence(client, { keyPrefix: 'lock:' });
			await f.acquire('task-2', 'fence-x', 30);
			const v = await client.redis.get(client.key('lock:task-2'));
			expect(v).toBe('fence-x');
		});
	});

	describe('HEARTBEAT_SCRIPT (value-matched PEXPIRE)', () => {
		it('returns true and refreshes the PEXPIRE while we still own the key', async () => {
			await fence.acquire('task-1', 'fence-a', 30);

			const ok = await fence.heartbeat('task-1', 'fence-a', 60);
			expect(ok).toBe(true);

			// PEXPIRE 60_000ms means TTL reads close to 60.
			const ttl = await client.redis.ttl(client.key('fence:task-1'));
			expect(ttl).toBeGreaterThan(30);
			expect(ttl).toBeLessThanOrEqual(60);
		});

		it('returns false when another fence value owns the key (force takeover)', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			// Simulate another worker stealing the lock.
			await client.redis.set(client.key('fence:task-1'), 'fence-b', 'EX', 30);
			const ok = await fence.heartbeat('task-1', 'fence-a', 60);
			expect(ok).toBe(false);
		});

		it('returns false when the key has expired between acquire and heartbeat', async () => {
			// Acquire with 1-second TTL, wait well past it, then heartbeat
			// must report lost. SET EX has 1-second precision so expiry can
			// land anywhere in the next second; the slack covers that plus
			// Docker-on-Windows clock drift under full-suite load.
			await fence.acquire('task-1', 'fence-a', 1);
			await wait(3000);
			const ok = await fence.heartbeat('task-1', 'fence-a', 30);
			expect(ok).toBe(false);
		});

		it('returns false when the key was never acquired', async () => {
			const ok = await fence.heartbeat('never', 'fence-a', 30);
			expect(ok).toBe(false);
		});

		it('does NOT re-create the key when the heartbeat finds it missing', async () => {
			const ok = await fence.heartbeat('phantom', 'fence-a', 30);
			expect(ok).toBe(false);
			const exists = await client.redis.exists(client.key('fence:phantom'));
			expect(exists).toBe(0);
		});
	});

	describe('RELEASE_SCRIPT (value-matched DEL)', () => {
		it('deletes the key when our fence value still owns it', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			await fence.release('task-1', 'fence-a');
			const exists = await client.redis.exists(client.key('fence:task-1'));
			expect(exists).toBe(0);
		});

		it('does not delete when a different fence value owns the key', async () => {
			await fence.acquire('task-1', 'fence-a', 30);
			await client.redis.set(client.key('fence:task-1'), 'fence-b', 'EX', 30);
			await fence.release('task-1', 'fence-a');
			const v = await client.redis.get(client.key('fence:task-1'));
			expect(v).toBe('fence-b');
		});

		it('is a no-op on an already-released key', async () => {
			await fence.release('never-acquired', 'fence-a');
		});
	});

	describe('end-to-end fence cycle', () => {
		it('acquire, heartbeat several times, release leaves no Redis key behind', async () => {
			await fence.acquire('cycle', 'fence-a', 5);
			expect(await fence.heartbeat('cycle', 'fence-a', 5)).toBe(true);
			expect(await fence.heartbeat('cycle', 'fence-a', 5)).toBe(true);
			await fence.release('cycle', 'fence-a');
			const exists = await client.redis.exists(client.key('fence:cycle'));
			expect(exists).toBe(0);
		});
	});
});
