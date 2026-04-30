import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay, ReplicationTimeoutError } from '../../redis/replay.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

describe('redis replay (stream backend)', () => {
	let client;
	let platform;
	let replay;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		replay = createReplay(client, { storage: 'stream', size: 5 });
	});

	describe('dispatch', () => {
		it('rejects unknown storage values', () => {
			expect(() => createReplay(client, { storage: 'rocksdb' })).toThrow("storage must be 'sortedset' or 'stream'");
		});

		it('returns the same external API shape as the sorted-set backend', () => {
			const r = createReplay(client, { storage: 'stream' });
			expect(typeof r.publish).toBe('function');
			expect(typeof r.seq).toBe('function');
			expect(typeof r.gap).toBe('function');
			expect(typeof r.since).toBe('function');
			expect(typeof r.replay).toBe('function');
			expect(typeof r.clear).toBe('function');
			expect(typeof r.clearTopic).toBe('function');
		});

		it('uses a different buf-key prefix from the sorted-set backend', async () => {
			const ss = createReplay(client, { size: 5 });
			const st = createReplay(client, { storage: 'stream', size: 5 });
			await ss.publish(platform, 'chat', 'created', { id: 1 });
			await st.publish(platform, 'chat', 'created', { id: 2 });

			expect(client._sortedSets.has(client.key('replay:buf:chat'))).toBe(true);
			expect(client._streams.has(client.key('replay:streambuf:chat'))).toBe(true);
		});
	});

	describe('publish', () => {
		it('calls platform.publish with the same arguments', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 } }
			]);
		});

		it('increments the sequence number', async () => {
			expect(await replay.seq('chat')).toBe(0);
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.seq('chat')).toBe(1);
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			expect(await replay.seq('chat')).toBe(2);
		});

		it('tracks sequences independently per topic', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			expect(await replay.seq('chat')).toBe(2);
			expect(await replay.seq('todos')).toBe(1);
		});

		it('uses <seq>-0 stream IDs', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			const stream = client._streams.get(client.key('replay:streambuf:chat'));
			expect(stream.map((e) => e.id)).toEqual(['1-0', '2-0']);
		});
	});

	describe('since', () => {
		it('returns all messages after a sequence number', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'chat', 'created', { id: 3 });

			const missed = await replay.since('chat', 1);
			expect(missed).toHaveLength(2);
			expect(missed[0]).toEqual({ seq: 2, topic: 'chat', event: 'created', data: { id: 2 } });
			expect(missed[1]).toEqual({ seq: 3, topic: 'chat', event: 'created', data: { id: 3 } });
		});

		it('returns empty array when caught up', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.since('chat', 1)).toEqual([]);
		});

		it('returns all messages when since is 0', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			expect(await replay.since('chat', 0)).toHaveLength(2);
		});
	});

	describe('buffer capping', () => {
		it('caps buffer at maxSize', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.seq('chat')).toBe(7);
			const all = await replay.since('chat', 0);
			expect(all).toHaveLength(5);
			expect(all[0].seq).toBe(3);
			expect(all[4].seq).toBe(7);
		});
	});

	describe('replay', () => {
		it('sends missed messages then end marker', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'chat', 'created', { id: 3 });

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			expect(platform.sent).toHaveLength(3);
			expect(platform.sent[0]).toEqual({ ws: fakeWs, topic: '__replay:chat', event: 'msg', data: { seq: 2, event: 'created', data: { id: 2 } } });
			expect(platform.sent[2].event).toBe('end');
		});

		it('sends only end marker when caught up', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);
			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].event).toBe('end');
		});

		it('truncated event when buffer was trimmed past sinceSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
		});

		it('truncated event when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			client._streams.delete(client.key('replay:streambuf:chat'));

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
		});
	});

	describe('gap', () => {
		it('returns not truncated when lastSeenSeq is 0', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns not truncated when next seq is in the buffer', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 1)).toEqual({ truncated: false, missingFrom: null });
			expect(await replay.gap('chat', 2)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns truncated when buffer was trimmed', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns truncated when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			client._streams.delete(client.key('replay:streambuf:chat'));
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns not truncated when consumer is ahead of the buffer', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 10)).toEqual({ truncated: false, missingFrom: null });
		});
	});

	describe('clear / clearTopic', () => {
		it('clear resets everything', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });
			await replay.clear();
			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.since('chat', 0)).toEqual([]);
		});

		it('clearTopic resets only that topic', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });
			await replay.clearTopic('chat');
			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(1);
		});
	});

	describe('replicated durability', () => {
		it('throws ReplicationTimeoutError when ack < minReplicas', async () => {
			const r = createReplay(client, {
				storage: 'stream',
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 100
			});
			client.redis._waitAcks = 1;

			await expect(r.publish(platform, 'chat', 'created', { id: 1 }))
				.rejects.toBeInstanceOf(ReplicationTimeoutError);
			expect(platform.published).toHaveLength(0);
		});

		it('publishes through when enough replicas ack', async () => {
			const r = createReplay(client, {
				storage: 'stream',
				durability: 'replicated',
				minReplicas: 1,
				replicationTimeoutMs: 100
			});
			client.redis._waitAcks = 1;

			await r.publish(platform, 'chat', 'created', { id: 1 });
			expect(platform.published).toHaveLength(1);
		});
	});

	describe('publishIdempotent', () => {
		let r;

		beforeEach(() => {
			r = createReplay(client, { storage: 'stream', size: 100 });
		});

		it('rejects missing producerId', async () => {
			await expect(r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, { requestId: 'r1' }))
				.rejects.toThrow('producerId must be a non-empty string');
		});

		it('rejects missing requestId', async () => {
			await expect(r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, { producerId: 'p1' }))
				.rejects.toThrow('requestId must be a non-empty string');
		});

		it('rejects missing opts entirely', async () => {
			await expect(r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }))
				.rejects.toThrow('producerId, requestId');
		});

		it('first call returns isDuplicate: false and broadcasts', async () => {
			const result = await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(result.isDuplicate).toBe(false);
			expect(result.seq).toBe(1);
			expect(platform.published).toHaveLength(1);
		});

		it('second call with same (producerId, requestId) returns cached seq, no broadcast', async () => {
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			platform.reset();

			const result = await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(result.isDuplicate).toBe(true);
			expect(result.seq).toBe(1);
			expect(platform.published).toHaveLength(0);
		});

		it('does not advance the seq counter on a duplicate', async () => {
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(await r.seq('chat')).toBe(1);
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(await r.seq('chat')).toBe(1);
		});

		it('does NOT cause false-positive truncation on duplicate retry', async () => {
			// Pin the property: a duplicate retry must not advance the
			// stream past the consumer's lastSeenSeq, so gap() reports
			// no truncation.
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(await r.gap('chat', 1)).toEqual({ truncated: false, missingFrom: null });
		});

		it('different requestId on the same producer is treated as fresh', async () => {
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
		});

		it('different producerId same requestId is treated as fresh (separate namespace)', async () => {
			const a = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const b = await r.publishIdempotent(platform, 'chat', 'msg', { id: 2 }, {
				producerId: 'p2', requestId: 'r1'
			});
			expect(a.isDuplicate).toBe(false);
			expect(b.isDuplicate).toBe(false);
			expect(a.seq).toBe(1);
			expect(b.seq).toBe(2);
		});

		it('cache is topic-scoped: same (producerId, requestId) on a different topic is fresh', async () => {
			const a = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const b = await r.publishIdempotent(platform, 'todos', 'msg', { id: 2 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(a.isDuplicate).toBe(false);
			expect(b.isDuplicate).toBe(false);
		});

		it('replay() includes the cached entry for fresh consumers', async () => {
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			// Even if a later retry hits the cache, an earlier-attached
			// consumer that missed the original publish picks it up via
			// replay since the entry is in the buffer.
			await r.publishIdempotent(platform, 'chat', 'created', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});

			const ws = {};
			platform.reset();
			await r.replay(ws, 'chat', 0, platform);
			const msgs = platform.sent.filter((s) => s.event === 'msg');
			expect(msgs).toHaveLength(1);
			expect(msgs[0].data).toEqual({ seq: 1, event: 'created', data: { id: 1 } });
		});

		it('runs WAIT only on fresh writes when durability is on', async () => {
			const replicated = createReplay(client, {
				storage: 'stream',
				durability: 'replicated',
				minReplicas: 1,
				replicationTimeoutMs: 100
			});
			let waitCalls = 0;
			const origWait = client.redis.wait;
			client.redis.wait = async (n) => { waitCalls++; return Number(n); };

			await replicated.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(waitCalls).toBe(1);

			await replicated.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(waitCalls).toBe(1); // no second WAIT on duplicate

			client.redis.wait = origWait;
		});

		it('throws ReplicationTimeoutError when ack < min on a fresh write', async () => {
			const replicated = createReplay(client, {
				storage: 'stream',
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 100
			});
			client.redis._waitAcks = 1;

			await expect(replicated.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			})).rejects.toBeInstanceOf(ReplicationTimeoutError);

			client.redis._waitAcks = undefined;
		});

		it('exposes Prometheus counters for hits and writes', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const tracked = createReplay(client, { storage: 'stream', metrics });

			await tracked.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			await tracked.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});

			const out = metrics.serialize();
			expect(out).toMatch(/replay_idmp_writes_total\{topic="chat"\} 1/);
			expect(out).toMatch(/replay_idmp_hits_total\{topic="chat"\} 1/);
		});

		it('is absent on the sorted-set backend', () => {
			const ss = createReplay(client, {});
			expect(ss.publishIdempotent).toBeUndefined();
		});
	});

	describe('breaker accounting', () => {
		it('records failure on publish error', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const r = createReplay(client, { storage: 'stream', breaker });

			client.redis.eval = async () => { throw new Error('eval failed'); };

			await expect(r.publish(platform, 'chat', 'msg', { id: 1 })).rejects.toThrow('eval failed');
			expect(breaker.failures).toBe(1);

			breaker.destroy();
		});
	});
});
