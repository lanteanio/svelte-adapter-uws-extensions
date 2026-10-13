import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { sessionErasureContract } from '../../helpers/session-erasure-contract.js';
import { createDistributedSession } from '../../../src/redis/session.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-session-erasure:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
sessionErasureContract(() => client);

describe('session erasure index on a real server', () => {
	const options = { forgetUserId: (data) => data?.userId, refreshOnGet: false, ttlMs: 30_000 };

	it('purges through an ioredis-level key prefix', async () => {
		const prefixed = createBackendClient({ options: { keyPrefix: 'inttest-session-io:' } });
		const record = 'inttest-session-io:sess:tok';
		const index = 'inttest-session-io:sess:byuser:target';
		try {
			const sessions = createDistributedSession(prefixed, options);
			await sessions.set('tok', { userId: 'target' });
			expect(await client.redis.get(record)).not.toBeNull();
			expect(await sessions.purgeUser(null, 'target')).toBe(1);
			expect(await client.redis.get(record)).toBeNull();
			expect(await client.redis.exists(index)).toBe(0);
		} finally {
			await client.redis.unlink(record);
			await client.redis.unlink(index);
			await prefixed.quit();
		}
	});

	it('stores no record when the server refuses the index write', async () => {
		const sessions = createDistributedSession(client, options);
		// A string where the index hash belongs makes HSET fail inside EXEC.
		await client.redis.set(client.key('sess:byuser:target'), 'not-a-hash');
		await expect(sessions.set('tok', { userId: 'target' })).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
		expect(await client.redis.get(client.key('sess:tok'))).toBeNull();
	});

	it('keeps the record TTL when touch or get cannot refresh the index first', async () => {
		const sessions = createDistributedSession(client, { ...options, refreshOnGet: true });
		await sessions.set('tok', { userId: 'target' });
		await client.redis.unlink(client.key('sess:byuser:target'));
		await client.redis.set(client.key('sess:byuser:target'), 'not-a-hash');
		await waitRedisMs(client, 300);
		const before = await client.redis.pttl(client.key('sess:tok'));
		await expect(sessions.touch('tok')).rejects.toMatchObject({ code: 'ERASURE_INDEX_INCOMPLETE' });
		expect(await sessions.get('tok')).toEqual({ userId: 'target' });
		expect(await client.redis.pttl(client.key('sess:tok'))).toBeLessThanOrEqual(before);
	});

	it('extends a whole-key index TTL so it never drops a newer field', async () => {
		const sessions = createDistributedSession(client, options);
		const index = client.key('sess:byuser:target');
		// A key TTL left by the whole-key fallback (an older server, or a
		// capability probe that timed out).
		await client.redis.hset(index, 'older', '1');
		await client.redis.pexpire(index, 2_000);
		await sessions.set('tok', { userId: 'target' });
		expect(await client.redis.pttl(index)).toBeGreaterThan(await client.redis.pttl(client.key('sess:tok')));
	});

	it('adds no whole-key TTL to a per-field index', async () => {
		const sessions = createDistributedSession(client, options);
		await sessions.set('tok', { userId: 'target' });
		expect(await client.redis.pttl(client.key('sess:byuser:target'))).toBe(-1);
		const [field] = await client.redis.hpttl(client.key('sess:byuser:target'), 'FIELDS', 1, 'tok');
		expect(field).toBeGreaterThan(await client.redis.pttl(client.key('sess:tok')));
	});
});
