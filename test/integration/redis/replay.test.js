/**
 * Integration tests for redis/replay against a real Redis 7 server.
 *
 * Exercises the actual Lua atomicity (INCR + ZADD + ZREMRANGEBYRANK + EXPIRE
 * in one EVAL) that the in-memory mock can only approximate. The mock-based
 * suite stays at test/redis/replay.test.js; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createReplay } from '../../../redis/replay.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

describe('redis replay (integration)', () => {
	let client;
	let platform;
	let replay;

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-replay:',
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
		replay = createReplay(client, { size: 5 });
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('publish + Lua envelope', () => {
		it('increments seq independently per topic and broadcasts', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			expect(await replay.seq('chat')).toBe(2);
			expect(await replay.seq('todos')).toBe(1);
			expect(platform.published).toHaveLength(3);
		});

		it('Lua-built envelope round-trips through since() with correct shape', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 42, body: 'hi' });
			const all = await replay.since('chat', 0);
			expect(all).toEqual([
				{ seq: 1, topic: 'chat', event: 'created', data: { id: 42, body: 'hi' } }
			]);
		});

		it('handles null data via the cjson.encode/string-splice path', async () => {
			await replay.publish(platform, 'chat', 'ping', null);
			const all = await replay.since('chat', 0);
			expect(all[0]).toEqual({ seq: 1, topic: 'chat', event: 'ping', data: null });
		});
	});

	describe('atomic buffer capping (real ZREMRANGEBYRANK in the Lua script)', () => {
		it('caps buffer at maxSize after sequential publishes', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			expect(await replay.seq('chat')).toBe(7);

			const all = await replay.since('chat', 0);
			expect(all).toHaveLength(5);
			expect(all[0].seq).toBe(3);
			expect(all[4].seq).toBe(7);
		});

		it('concurrent publishes with size: 1 keep exactly the last one', async () => {
			const r = createReplay(client, { size: 1 });

			await Promise.all([
				r.publish(platform, 'chat', 'a', { id: 1 }),
				r.publish(platform, 'chat', 'b', { id: 2 })
			]);

			const all = await r.since('chat', 0);
			expect(all).toHaveLength(1);
			expect(all[0].seq).toBe(2);
		});

		it('concurrent publishes with size: 2 keep exactly the last two', async () => {
			const r = createReplay(client, { size: 2 });

			await Promise.all([
				r.publish(platform, 'chat', 'a', { id: 1 }),
				r.publish(platform, 'chat', 'b', { id: 2 }),
				r.publish(platform, 'chat', 'c', { id: 3 })
			]);

			const all = await r.since('chat', 0);
			expect(all).toHaveLength(2);
			expect(all.map((m) => m.seq)).toEqual([2, 3]);
		});
	});

	describe('since', () => {
		it('returns messages strictly after the cursor', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const missed = await replay.since('chat', 1);
			expect(missed.map((m) => m.seq)).toEqual([2, 3]);
		});

		it('returns empty when caught up', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.since('chat', 1)).toEqual([]);
		});

		it('returns empty for unknown topics', async () => {
			expect(await replay.since('nonexistent', 0)).toEqual([]);
		});
	});

	describe('gap', () => {
		it('reports clean when buffer still has the next entry', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 2)).toEqual({ truncated: false, missingFrom: null });
		});

		it('reports truncated when buffer rolled past lastSeenSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('fresh client (lastSeenSeq=0) is never truncated', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns clean for an unknown topic', async () => {
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: false, missingFrom: null });
		});
	});

	describe('replay', () => {
		it('streams missed messages on __replay:{topic} then end marker', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			expect(platform.sent).toHaveLength(3);
			expect(platform.sent[0]).toMatchObject({
				topic: '__replay:chat', event: 'msg', data: { seq: 2 }
			});
			expect(platform.sent[1]).toMatchObject({
				topic: '__replay:chat', event: 'msg', data: { seq: 3 }
			});
			expect(platform.sent[2]).toMatchObject({
				topic: '__replay:chat', event: 'end'
			});
		});

		it('sends truncated event when buffer was trimmed past sinceSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
		});
	});

	describe('TTL (real EXPIRE applied by the Lua script)', () => {
		it('sets a TTL on both keys when ttl is non-zero', async () => {
			const r = createReplay(client, { size: 5, ttl: 600 });
			await r.publish(platform, 'chat', 'created', { id: 1 });

			const seqTtl = await client.redis.ttl(client.key('replay:seq:chat'));
			const bufTtl = await client.redis.ttl(client.key('replay:buf:chat'));
			expect(seqTtl).toBeGreaterThan(0);
			expect(bufTtl).toBeGreaterThan(0);
			expect(seqTtl).toBeLessThanOrEqual(600);
			expect(bufTtl).toBeLessThanOrEqual(600);
		});

		it('leaves keys with no TTL when ttl is unset', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await client.redis.ttl(client.key('replay:seq:chat'))).toBe(-1);
			expect(await client.redis.ttl(client.key('replay:buf:chat'))).toBe(-1);
		});
	});

	describe('clearTopic / clear', () => {
		it('clearTopic removes only that topic', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clearTopic('chat');

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(1);
		});

		it('clear wipes everything under the prefix', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clear();

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(0);
			expect(await replay.since('chat', 0)).toEqual([]);
		});
	});
});
