/**
 * Integration tests for redis/replay-stream against a real Redis 7 server.
 *
 * The high-value scenarios the in-memory mock cannot fully exercise:
 *
 * - XADD with `MAXLEN ~` trimming. The mock approximates exact-count
 *   trimming; real Redis uses approximate listpack-aligned trimming
 *   that can keep slightly more than maxSize entries.
 * - <seq>-0 stream IDs generated atomically inside the Lua script
 *   (INCR + XADD in one EVAL).
 * - XRANGE with the `(` exclusive prefix on `<seq>-0` IDs --
 *   "everything after this seq" semantics on the wire.
 * - Concurrent publishes ordered by Redis-side sequence.
 * - publishIdempotent dedup cache + WAIT skip on hits.
 *
 * The mock-based suite at test/redis/replay-stream.test.js stays as
 * the exhaustive behavior surface; this file pins what only a real
 * server can verify.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createReplay } from '../../../redis/replay.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

describe('redis replay (stream backend, integration)', () => {
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
			keyPrefix: 'inttest-replay-stream:',
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
		replay = createReplay(client, { storage: 'stream', size: 5 });
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

		it('writes <seq>-0 stream IDs that XRANGE can address by sequence', async () => {
			await replay.publish(platform, 'chat', 'a', { id: 1 });
			await replay.publish(platform, 'chat', 'b', { id: 2 });
			await replay.publish(platform, 'chat', 'c', { id: 3 });

			const entries = await client.redis.xrange(
				client.key('replay:streambuf:chat'), '-', '+'
			);
			expect(entries.map((e) => e[0])).toEqual(['1-0', '2-0', '3-0']);
		});

		it('round-trips full envelope through since() with correct shape', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 42, body: 'hi' });
			const all = await replay.since('chat', 0);
			expect(all).toEqual([
				{ seq: 1, topic: 'chat', event: 'created', data: { id: 42, body: 'hi' } }
			]);
		});

		it('handles null data via the JSON.stringify(data ?? null) path', async () => {
			await replay.publish(platform, 'chat', 'ping', null);
			const all = await replay.since('chat', 0);
			expect(all[0]).toEqual({ seq: 1, topic: 'chat', event: 'ping', data: null });
		});
	});

	describe('atomic buffer capping (real XADD MAXLEN ~ in the Lua script)', () => {
		it('keeps roughly the last maxSize entries after sequential publishes', async () => {
			// XADD MAXLEN ~ trims along listpack-node boundaries. The
			// default stream-node-max-entries is 100, so we publish well
			// past two full nodes' worth so trimming MUST kick in.
			const total = 500;
			for (let i = 1; i <= total; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			expect(await replay.seq('chat')).toBe(total);

			const all = await replay.since('chat', 0);
			// Approximate trimming keeps >= maxSize (5) but bounded by
			// listpack overshoot. The whole buffer must be much smaller
			// than the total written, and the latest entry must be the
			// most recent seq.
			expect(all.length).toBeGreaterThanOrEqual(5);
			expect(all.length).toBeLessThan(total);
			expect(all[all.length - 1].seq).toBe(total);
		});

		it('concurrent publishes assign distinct sequential seqs (atomic INCR + XADD)', async () => {
			const r = createReplay(client, { storage: 'stream', size: 100 });

			await Promise.all(
				Array.from({ length: 20 }, (_, i) =>
					r.publish(platform, 'chat', 'msg', { i })
				)
			);

			expect(await r.seq('chat')).toBe(20);
			const all = await r.since('chat', 0);
			expect(all).toHaveLength(20);
			// Every seq from 1..20 must appear exactly once.
			const seen = new Set(all.map((m) => m.seq));
			expect(seen.size).toBe(20);
			for (let i = 1; i <= 20; i++) expect(seen.has(i)).toBe(true);
			// And they come back in monotonic order.
			for (let i = 1; i < all.length; i++) {
				expect(all[i].seq).toBeGreaterThan(all[i - 1].seq);
			}
		});
	});

	describe('since (XRANGE with `(` exclusive prefix on <seq>-0)', () => {
		it('returns entries strictly after the cursor seq', async () => {
			for (let i = 1; i <= 5; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const missed = await replay.since('chat', 2);
			expect(missed.map((m) => m.seq)).toEqual([3, 4, 5]);
		});

		it('returns empty when caught up', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.since('chat', 1)).toEqual([]);
		});

		it('returns empty for an unknown topic (XRANGE on missing stream)', async () => {
			expect(await replay.since('nonexistent', 0)).toEqual([]);
		});

		it('since(0) returns every buffered entry', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const all = await replay.since('chat', 0);
			expect(all.map((m) => m.seq)).toEqual([1, 2, 3]);
		});
	});

	describe('gap', () => {
		it('reports clean when buffer still has the next entry', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 2)).toEqual({ truncated: false, missingFrom: null });
		});

		it('reports truncated when the stream was trimmed past lastSeenSeq + 1', async () => {
			// Publish well past the listpack node threshold so MAXLEN ~
			// trim must drop the oldest entries.
			for (let i = 1; i <= 500; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const oldest = (await replay.since('chat', 0))[0];
			expect(oldest.seq).toBeGreaterThan(1);

			// gap from before the oldest should report truncation.
			const result = await replay.gap('chat', 1);
			expect(result.truncated).toBe(true);
			expect(result.missingFrom).toBe(2);
		});

		it('fresh client (lastSeenSeq=0) is never truncated', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns clean for an unknown topic', async () => {
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: false, missingFrom: null });
		});

		it('reports truncated when buffer was UNLINKed but seq counter still advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			// Drop the stream but keep the seq counter; this is the
			// "buffer evicted, consumer still has cursor" case.
			await client.redis.unlink(client.key('replay:streambuf:chat'));

			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});
	});

	describe('replay()', () => {
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

		it('sends only end marker when caught up', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);
			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].event).toBe('end');
		});
	});

	describe('TTL (real EXPIRE applied by the Lua script)', () => {
		it('sets a TTL on both keys when ttl is non-zero', async () => {
			const r = createReplay(client, { storage: 'stream', size: 5, ttl: 600 });
			await r.publish(platform, 'chat', 'created', { id: 1 });

			const seqTtl = await client.redis.ttl(client.key('replay:seq:chat'));
			const bufTtl = await client.redis.ttl(client.key('replay:streambuf:chat'));
			expect(seqTtl).toBeGreaterThan(0);
			expect(bufTtl).toBeGreaterThan(0);
			expect(seqTtl).toBeLessThanOrEqual(600);
			expect(bufTtl).toBeLessThanOrEqual(600);
		});

		it('leaves keys with no TTL when ttl is unset', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await client.redis.ttl(client.key('replay:seq:chat'))).toBe(-1);
			expect(await client.redis.ttl(client.key('replay:streambuf:chat'))).toBe(-1);
		});
	});

	describe('publishIdempotent (atomic dedup + XADD in one EVAL)', () => {
		it('first call writes a new entry; second with same (producerId, requestId) is a hit', async () => {
			const r = createReplay(client, { storage: 'stream', size: 50 });

			const a = await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(a).toEqual({ seq: 1, isDuplicate: false });
			expect(platform.published).toHaveLength(1);

			const b = await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(b).toEqual({ seq: 1, isDuplicate: true });
			// No second broadcast.
			expect(platform.published).toHaveLength(1);

			// And no second stream entry.
			const all = await r.since('chat', 0);
			expect(all).toHaveLength(1);
		});

		it('duplicate retry does not advance seq and does not cause false truncation', async () => {
			const r = createReplay(client, { storage: 'stream', size: 50 });
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(await r.seq('chat')).toBe(1);

			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(await r.seq('chat')).toBe(1);
			expect(await r.gap('chat', 1)).toEqual({ truncated: false, missingFrom: null });
		});

		it('different requestId on the same producer is treated as fresh', async () => {
			const r = createReplay(client, { storage: 'stream', size: 50 });
			const a = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const b = await r.publishIdempotent(platform, 'chat', 'msg', { id: 2 }, {
				producerId: 'p1', requestId: 'r2'
			});
			expect(a.seq).toBe(1);
			expect(a.isDuplicate).toBe(false);
			expect(b.seq).toBe(2);
			expect(b.isDuplicate).toBe(false);
			expect(await r.seq('chat')).toBe(2);
		});

		it('cache is topic-scoped: same (producerId, requestId) on a different topic is fresh', async () => {
			const r = createReplay(client, { storage: 'stream', size: 50 });
			const a = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const b = await r.publishIdempotent(platform, 'todos', 'msg', { id: 2 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(a.isDuplicate).toBe(false);
			expect(b.isDuplicate).toBe(false);
		});

		it('sets a TTL on the dedup hash when idempotencyTtl is non-zero', async () => {
			const r = createReplay(client, { storage: 'stream', size: 50, idempotencyTtl: 60 });
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const idmpTtl = await client.redis.ttl(client.key('replay:idmp:p1:chat'));
			expect(idmpTtl).toBeGreaterThan(0);
			expect(idmpTtl).toBeLessThanOrEqual(60);
		});
	});

	describe('clearTopic / clear', () => {
		it('clearTopic removes only that topic', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clearTopic('chat');

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(1);
			expect(await replay.since('chat', 0)).toEqual([]);
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
