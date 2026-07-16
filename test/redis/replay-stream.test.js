import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay, ReplicationTimeoutError, ReplayStorageError, ReplaySerializationError } from '../../src/redis/replay.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

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

			expect(client._sortedSets.has(client.key('replay:buf:{chat}'))).toBe(true);
			expect(client._streams.has(client.key('replay:streambuf:{chat}'))).toBe(true);
		});
	});

	describe('publish', () => {
		it('calls platform.publish with the same arguments plus the authoritative seq', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			// topic/event/data forwarded unchanged; the stream's authoritative seq is
			// threaded as the publish option so the live frame matches the buffer.
			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 }, options: { seq: 1 } }
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
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
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
			client._streams.delete(client.key('replay:streambuf:{chat}'));

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
			client._streams.delete(client.key('replay:streambuf:{chat}'));
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns not truncated when consumer is ahead of the buffer', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 10)).toEqual({ truncated: false, missingFrom: null });
		});

		it('reports a gap when the entry at the next seq is corrupt (decodes it, not just id-matches)', async () => {
			// A newer node wrote seq 2 in an unknown envelope version; replay()/since()
			// drop it on read, so gap() must not call the hole it leaves contiguous -
			// this matches the sorted-set gap() and the strict replay read path.
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			const key = client.key('replay:streambuf:{chat}');
			await client.redis.xadd(key, '2-0', 'v', '99', 'event', 'created', 'data', '{"id":2}');
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('counts the corrupt next-seq entry in replay_corruptions_total', async () => {
			const { createMetrics } = await import('../../src/prometheus/index.js');
			const metrics = createMetrics();
			const tracked = createReplay(client, { storage: 'stream', size: 5, metrics });
			await tracked.publish(platform, 'chat', 'created', { id: 1 });
			const key = client.key('replay:streambuf:{chat}');
			await client.redis.xadd(key, '2-0', 'v', '99', 'event', 'created', 'data', '{"id":2}');
			await tracked.gap('chat', 1);
			expect(metrics.serialize()).toMatch(/replay_corruptions_total\{topic="chat"\} 1/);
		});
	});

	describe('resumeHook', () => {
		it('returns an async function', () => {
			expect(typeof replay.resumeHook()).toBe('function');
		});

		it('no-ops on missing ctx fields', async () => {
			const hook = replay.resumeHook();
			await hook({});
			await hook({}, { platform });
			await hook({}, { lastSeenSeqs: { chat: 1 } });
			expect(platform.sent).toHaveLength(0);
		});

		it('replays a single topic from the given seq', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			expect(platform.sent.filter((s) => s.event === 'msg')).toHaveLength(1);
			expect(platform.sent.find((s) => s.event === 'end')).toBeDefined();
		});

		it('replays multiple topics in iteration order', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 'c1' });
			await replay.publish(platform, 'todos', 'msg', { id: 't1' });
			await replay.publish(platform, 'todos', 'msg', { id: 't2' });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 0, todos: 1 }, platform });

			const chat = platform.sent.filter((s) => s.topic === '__replay:chat');
			const todos = platform.sent.filter((s) => s.topic === '__replay:todos');
			expect(chat).toHaveLength(2);
			expect(todos).toHaveLength(2);
		});

		it('coerces non-numeric or negative sinceSeq to 0', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 'oops' }, platform });

			expect(platform.sent.filter((s) => s.event === 'msg')).toHaveLength(1);
		});

		it('emits truncated when the stream no longer holds the next seq', async () => {
			// size: 5 in this suite; publish 7 to trim seqs 1-2.
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'msg', { id: i });
			}
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			expect(platform.sent.find((s) => s.event === 'truncated')).toBeDefined();
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

		it('clear rotates each topic epoch instead of reusing generation 1', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });
			const chatBefore = await replay.currentEpoch('chat');
			const todosBefore = await replay.currentEpoch('todos');

			await replay.clear();

			expect(await replay.seq('chat')).toBe(0);
			// Epoch ADVANCES rather than being deleted and recreated at 1, so a client
			// that straddled the clear observes a new generation on resume.
			expect(await replay.currentEpoch('chat')).toBeGreaterThan(chatBefore);
			expect(await replay.currentEpoch('todos')).toBeGreaterThan(todosBefore);
		});

		it('a client straddling clear() rehydrates instead of silently seeing contiguity', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			const preEpoch = await replay.currentEpoch('chat');

			await replay.clear();
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, {
				lastSeenSeqs: { chat: 1 },
				lastSeenEpochs: { chat: preEpoch },
				platform
			});

			const rehydrate = platform.sent.find((s) => s.topic === '__replay:chat' && s.event === 'rehydrate');
			expect(rehydrate).toBeDefined();
		});
	});

	describe('per-topic epoch', () => {
		it('starts a never-published topic at the baseline epoch', async () => {
			expect(await replay.currentEpoch('chat')).toBe(0);
		});

		it('bumps the epoch on the first publish of a fresh topic', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			expect(await replay.currentEpoch('chat')).toBe(1);
			// Steady-state publishes do not move the epoch.
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			expect(await replay.currentEpoch('chat')).toBe(1);
		});

		it('bumps the epoch when clearTopic resets the seq space', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			const before = await replay.currentEpoch('chat');
			await replay.clearTopic('chat');
			const after = await replay.currentEpoch('chat');
			expect(after).toBeGreaterThan(before);
			// Next publish restarts seq at 1 and bumps again (fresh seq space).
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			expect(await replay.seq('chat')).toBe(1);
			expect(await replay.currentEpoch('chat')).toBeGreaterThan(after);
		});

		it('exposes the epoch synchronously via cachedEpoch after a read', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.currentEpoch('chat');
			expect(replay.cachedEpoch('chat')).toBe(1);
			// A topic this process has not observed reads the baseline.
			expect(replay.cachedEpoch('never-touched')).toBe(0);
		});

		it('gap-fills when the presented epoch matches the stored epoch', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			const epoch = await replay.currentEpoch('chat');
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, lastSeenEpochs: { chat: epoch }, platform });

			const msgs = platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg');
			const rehydrate = platform.sent.find((s) => s.topic === '__replay:chat' && s.event === 'rehydrate');
			expect(msgs).toHaveLength(1);
			expect(msgs[0].data).toMatchObject({ seq: 2 });
			expect(rehydrate).toBeUndefined();
		});

		it('cold-rehydrates when the presented epoch is stale', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			const stale = await replay.currentEpoch('chat');
			// Reset the seq space, moving the epoch past what the client holds.
			await replay.clearTopic('chat');
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 9 }, lastSeenEpochs: { chat: stale }, platform });

			const rehydrate = platform.sent.find((s) => s.topic === '__replay:chat' && s.event === 'rehydrate');
			const msgs = platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg');
			// Stale epoch: rehydrate, never serve the reset seq space as contiguous.
			expect(rehydrate).toBeDefined();
			expect(msgs).toHaveLength(0);
		});

		it('treats an absent presented epoch as a match (old client gap-fills)', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			platform.reset();

			// No lastSeenEpochs at all: the first-publish epoch bump must NOT
			// cause a spurious rehydrate; gap-fill exactly as before.
			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			const msgs = platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg');
			const rehydrate = platform.sent.find((s) => s.event === 'rehydrate');
			expect(msgs).toHaveLength(1);
			expect(rehydrate).toBeUndefined();
		});

		it('decides each topic independently when one epoch is stale and one matches', async () => {
			await replay.publish(platform, 'fresh', 'msg', { id: 1 });
			await replay.publish(platform, 'fresh', 'msg', { id: 2 });
			const freshEpoch = await replay.currentEpoch('fresh');

			await replay.publish(platform, 'stale', 'msg', { id: 1 });
			const staleEpoch = await replay.currentEpoch('stale');
			await replay.clearTopic('stale');
			await replay.publish(platform, 'stale', 'msg', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, {
				lastSeenSeqs: { fresh: 1, stale: 9 },
				lastSeenEpochs: { fresh: freshEpoch, stale: staleEpoch },
				platform
			});

			const freshMsgs = platform.sent.filter((s) => s.topic === '__replay:fresh' && s.event === 'msg');
			const freshRehydrate = platform.sent.find((s) => s.topic === '__replay:fresh' && s.event === 'rehydrate');
			const staleMsgs = platform.sent.filter((s) => s.topic === '__replay:stale' && s.event === 'msg');
			const staleRehydrate = platform.sent.find((s) => s.topic === '__replay:stale' && s.event === 'rehydrate');

			expect(freshMsgs).toHaveLength(1);
			expect(freshRehydrate).toBeUndefined();
			expect(staleMsgs).toHaveLength(0);
			expect(staleRehydrate).toBeDefined();
		});

		it('bumps the epoch on the first idempotent publish of a fresh topic', async () => {
			const r = createReplay(client, { storage: 'stream', size: 100 });
			const result = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(result.seq).toBe(1);
			expect(await r.currentEpoch('chat')).toBe(1);
		});

		it('does NOT bump the epoch on a duplicate idempotent publish', async () => {
			const r = createReplay(client, { storage: 'stream', size: 100 });
			await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			const after = await r.currentEpoch('chat');
			// The duplicate short-circuits before the seq INCR, so the seq == 1
			// epoch edge never fires a second time.
			const dup = await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
				producerId: 'p1', requestId: 'r1'
			});
			expect(dup.isDuplicate).toBe(true);
			expect(await r.currentEpoch('chat')).toBe(after);
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
			const { createMetrics } = await import('../../src/prometheus/index.js');
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

	describe('storage failure', () => {
		it('publish throws ReplayStorageError with cause when storage fails', async () => {
			const r = createReplay(client, { storage: 'stream' });
			const failure = new Error('redis down');
			client.redis.eval = async () => { throw failure; };

			let caught;
			try { await r.publish(platform, 'chat', 'msg', { id: 1 }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publish');
			expect(caught.cause).toBe(failure);
		});

		it('publish falls back to platform.publish when localFanoutOnStorageFailure: true', async () => {
			const r = createReplay(client, { storage: 'stream', localFanoutOnStorageFailure: true });
			client.redis.eval = async () => { throw new Error('redis down'); };

			const result = await r.publish(platform, 'chat', 'msg', { id: 1 });

			expect(result).toBe(true);
			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'msg', data: { id: 1 } }
			]);
		});

		it('warns once carrying requestId on the localFanout fallback, then suppresses', async () => {
			const r = createReplay(client, { storage: 'stream', localFanoutOnStorageFailure: true });
			client.redis.eval = async () => { throw new Error('redis down'); };
			const warns = [];
			const spy = vi.spyOn(console, 'warn').mockImplementation((msg) => warns.push(msg));
			try {
				platform.requestId = 'req-st';
				await r.publish(platform, 'chat', 'msg', { id: 1 });
				await r.publish(platform, 'chat', 'msg', { id: 2 });
			} finally {
				spy.mockRestore();
			}
			expect(warns).toHaveLength(1);
			expect(warns[0]).toContain('[redis stream replay]');
			expect(warns[0]).toContain('requestId=req-st');
			// The raw topic is deliberately not logged (it can embed user ids).
			expect(warns[0]).not.toContain('chat');
		});

		it('publishIdempotent always throws ReplayStorageError, even with localFanoutOnStorageFailure: true', async () => {
			const r = createReplay(client, { storage: 'stream', localFanoutOnStorageFailure: true });
			client.redis.eval = async () => { throw new Error('redis down'); };

			let caught;
			try {
				await r.publishIdempotent(platform, 'chat', 'msg', { id: 1 }, {
					producerId: 'p1', requestId: 'r1'
				});
			} catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publishIdempotent');
			expect(platform.published).toHaveLength(0);
		});
	});

	describe('serialization failure (caller-input bug)', () => {
		it('publish throws ReplaySerializationError when data contains a BigInt', async () => {
			let caught;
			try { await replay.publish(platform, 'chat', 'msg', { id: 1n }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(caught).not.toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publish');
			expect(caught.cause).toBeInstanceOf(TypeError);
		});

		it('publishIdempotent throws ReplaySerializationError when data contains a BigInt', async () => {
			let caught;
			try {
				await replay.publishIdempotent(platform, 'chat', 'msg', { id: 1n }, {
					producerId: 'p1', requestId: 'r1'
				});
			} catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(caught).not.toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publishIdempotent');
		});

		it('publish does NOT fall back to platform.publish even with localFanoutOnStorageFailure: true', async () => {
			const r = createReplay(client, { storage: 'stream', localFanoutOnStorageFailure: true });

			let caught;
			try { await r.publish(platform, 'chat', 'msg', { id: 1n }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(platform.published).toHaveLength(0);
		});

		it('serialization failure does NOT consume an idempotency slot (no redis.eval call)', async () => {
			let evalCalls = 0;
			const origEval = client.redis.eval;
			client.redis.eval = async (...args) => { evalCalls++; return origEval.call(client.redis, ...args); };

			let caught;
			try {
				await replay.publishIdempotent(platform, 'chat', 'msg', { id: 1n }, {
					producerId: 'p1', requestId: 'r1'
				});
			} catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(evalCalls).toBe(0);

			client.redis.eval = origEval;
		});

		it('circular reference also throws ReplaySerializationError', async () => {
			const data = /** @type {any} */ ({ id: 1 });
			data.self = data;

			let caught;
			try { await replay.publish(platform, 'chat', 'msg', data); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
		});
	});

	describe('sinceSeq input validation (defense-in-depth)', () => {
		beforeEach(async () => {
			for (let i = 0; i < 5; i++) {
				await replay.publish(platform, 'chat', 'msg', { i });
			}
		});

		it('since() rejects negative sinceSeq (no XRANGE-from-start dump)', async () => {
			expect(await replay.since('chat', -1)).toEqual([]);
			expect(await replay.since('chat', -100)).toEqual([]);
			expect(await replay.since('chat', Number.NEGATIVE_INFINITY)).toEqual([]);
		});

		it('since() rejects NaN / Infinity / non-integer', async () => {
			expect(await replay.since('chat', NaN)).toEqual([]);
			expect(await replay.since('chat', Infinity)).toEqual([]);
			expect(await replay.since('chat', 1.5)).toEqual([]);
		});

		it('since() rejects non-number sinceSeq', async () => {
			expect(await replay.since('chat', /** @type {any} */ ('0'))).toEqual([]);
			expect(await replay.since('chat', /** @type {any} */ (null))).toEqual([]);
		});

		it('since() still accepts 0 (resume from start)', async () => {
			expect((await replay.since('chat', 0)).length).toBeGreaterThan(0);
		});

		it('replay() with negative sinceSeq sends only end marker (no buffer dump)', async () => {
			platform.checkSubscribe = async () => null;
			const ws = {};
			await replay.replay(ws, 'chat', -1, platform, 'r1');
			const replayFrames = platform.sent.filter((s) => s.topic === '__replay:chat');
			expect(replayFrames.filter((f) => f.event === 'msg')).toHaveLength(0);
			const end = replayFrames.find((f) => f.event === 'end');
			expect(end).toBeDefined();
		});

		it('replay() with NaN sinceSeq sends only end marker', async () => {
			platform.checkSubscribe = async () => null;
			const ws = {};
			await replay.replay(ws, 'chat', NaN, platform, 'r2');
			const msgFrames = platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg');
			expect(msgFrames).toHaveLength(0);
		});
	});

	describe('covered watermark (recovery-barrier contract)', () => {
		// Mirror of the sorted-set backend's watermark contract: replay() and the
		// resume hook report the highest seq the gap-fill covered so the adapter's
		// replay-to-live cutover dedups its held live frames exactly.
		it('replay() resolves the highest seq it delivered', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			await replay.publish(platform, 'chat', 'msg', { id: 3 });
			expect(await replay.replay({}, 'chat', 1, platform)).toBe(3);
		});

		it('replay() resolves sinceSeq itself when the client is current', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			expect(await replay.replay({}, 'chat', 1, platform)).toBe(1);
			expect(await replay.replay({}, 'empty', 0, platform)).toBe(0);
		});

		it('replay() resolves undefined when denied or malformed (nothing gap-filled)', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			expect(await replay.replay({}, 'chat', -1, platform)).toBeUndefined();
			platform.checkSubscribe = async () => 'FORBIDDEN';
			expect(await replay.replay({}, 'chat', 0, platform)).toBeUndefined();
		});

		it('replay() resolves undefined when sinceSeq exceeds the seq counter (unverifiable claim)', async () => {
			// An inflated client offset must never become a trusted watermark, or
			// a frame published inside the resume window is silently skipped at
			// the barrier flush.
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.reset();
			expect(await replay.replay({}, 'chat', 999, platform)).toBeUndefined();
			// No truncated marker (nothing provably trimmed); the end marker
			// still arrives. A topic with no history is equally unverifiable.
			expect(platform.sent.find((s) => s.event === 'truncated')).toBeUndefined();
			expect(platform.sent.find((s) => s.event === 'end')).toBeDefined();
			expect(await replay.replay({}, 'ghost', 5, platform)).toBeUndefined();
		});

		it('resumeHook omits a liar-offset topic but still reports the others', async () => {
			await replay.publish(platform, 'honest', 'msg', { id: 1 });
			await replay.publish(platform, 'liar', 'msg', { id: 1 });
			platform.reset();

			const hook = replay.resumeHook();
			const covered = await hook({}, { lastSeenSeqs: { honest: 0, liar: 999 }, platform });
			expect(covered).toEqual({ honest: 1 });
		});

		it('resumeHook resolves the per-topic map for every gap-filled topic', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			await replay.publish(platform, 'todos', 'msg', { id: 1 });
			platform.reset();

			const hook = replay.resumeHook();
			const covered = await hook({}, { lastSeenSeqs: { chat: 0, todos: 1 }, platform });
			expect(covered).toEqual({ chat: 2, todos: 1 });
		});

		it('resumeHook omits a rehydrated topic from the watermark map', async () => {
			await replay.publish(platform, 'stale', 'msg', { id: 1 });
			const staleEpoch = await replay.currentEpoch('stale');
			await replay.clearTopic('stale');
			await replay.publish(platform, 'stale', 'msg', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			const covered = await hook({}, {
				lastSeenSeqs: { stale: 9 },
				lastSeenEpochs: { stale: staleEpoch },
				platform
			});
			expect(covered).toEqual({});
		});
	});

	describe('per-entry topic field (dropped; versioned read)', () => {
		it('a new-format publish stores a version + event + data, and since() recovers the topic from the key', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			// The stored entry carries a `v` version discriminator and no topic field
			// - the topic is the per-topic key.
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
			const fieldNames = stream[0].fields.map(([k]) => k);
			expect(fieldNames).toEqual(['v', 'event', 'data']);
			expect(fieldNames).not.toContain('topic');

			// since() still returns the topic, recovered from the param.
			const [msg] = await replay.since('chat', 0);
			expect(msg).toEqual({ seq: 1, topic: 'chat', event: 'created', data: { id: 1 } });
		});

		it('reads a LEGACY entry that still carries a topic field (backward-compatible)', async () => {
			// A pre-change entry wrote the topic into the entry; the reader honors
			// the stored value via the fields.topic ?? topic fallback.
			await client.redis.xadd(
				client.key('replay:streambuf:{chat}'),
				'1-0', 'topic', 'chat', 'event', 'created', 'data', '{"id":1}'
			);
			const [msg] = await replay.since('chat', 0);
			expect(msg).toEqual({ seq: 1, topic: 'chat', event: 'created', data: { id: 1 } });
		});

		it('derives the topic from the key even when a legacy entry carries a disagreeing topic field', async () => {
			// The key is the topic's authoritative home; a stored topic that disagrees
			// with it is exactly the redundancy the versioned envelope removes, so the
			// reader ignores it and returns the key-derived topic.
			await client.redis.xadd(
				client.key('replay:streambuf:{room}'),
				'1-0', 'topic', 'legacy-topic', 'event', 'e', 'data', 'null'
			);
			const [msg] = await replay.since('room', 0);
			expect(msg.topic).toBe('room');
			expect(msg.data).toBe(null);
		});

		it('decodes a stream that mixes a legacy entry and a new-format entry', async () => {
			const key = client.key('replay:streambuf:{chat}');
			await client.redis.xadd(key, '1-0', 'topic', 'chat', 'event', 'old', 'data', '{"n":1}');
			await client.redis.xadd(key, '2-0', 'event', 'new', 'data', '{"n":2}');

			const all = await replay.since('chat', 0);
			expect(all).toEqual([
				{ seq: 1, topic: 'chat', event: 'old', data: { n: 1 } },
				{ seq: 2, topic: 'chat', event: 'new', data: { n: 2 } }
			]);
		});
	});

	describe('generation-safe dedup + bounded dedup memory + versioned envelope', () => {
		let r;
		beforeEach(() => { r = createReplay(client, { storage: 'stream', size: 100 }); });

		it('a within-generation retry dedups to the same seq', async () => {
			const a = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			const b = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			expect(a).toEqual({ seq: 1, isDuplicate: false });
			expect(b).toEqual({ seq: 1, isDuplicate: true });
		});

		it('a retry after a seq-space reset re-publishes instead of returning a dead cached seq', async () => {
			const first = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			expect(first).toEqual({ seq: 1, isDuplicate: false });
			// clearTopic bumps the epoch and wipes seq+stream; the dedup cache survives
			// (idmp is a separate key), so a stale bare-seq return would point the
			// client at an unrelated entry in the restarted numbering.
			await r.clearTopic('chat');
			const retry = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			expect(retry.isDuplicate).toBe(false); // re-published into the new generation
			// A second retry WITHIN the new generation dedups normally again.
			const retry2 = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			expect(retry2).toEqual({ seq: retry.seq, isDuplicate: true });
		});

		it('honors a legacy bare-seq dedup value for one window (backward compatible)', async () => {
			const idmpKey = client.key('replay:idmp:p1:{chat}');
			await client.redis.hset(idmpKey, 'r1', '7'); // pre-versioning value, no epoch
			const res = await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1' });
			expect(res).toEqual({ seq: 7, isDuplicate: true });
		});

		it('does NOT honor a legacy bare-seq dedup value once a reset bumped the epoch', async () => {
			// A pre-versioning bare value carries no generation. After a reset bumps
			// the epoch above 0, the bare seq may point at an unrelated entry in the
			// restarted numbering, so it must re-publish, not return the dead seq.
			const idmpKey = client.key('replay:idmp:p1:{chat}');
			await client.redis.hset(idmpKey, 'r1', '3'); // legacy bare value, no epoch
			await client.redis.set(client.key('replay:epoch:{chat}'), '1'); // a reset occurred
			const retry = await r.publishIdempotent(platform, 'chat', 'e', { n: 9 }, { producerId: 'p1', requestId: 'r1' });
			expect(retry.isDuplicate).toBe(false); // re-published into the current generation
			// The re-stamped value now dedups normally within the current generation.
			const again = await r.publishIdempotent(platform, 'chat', 'e', { n: 9 }, { producerId: 'p1', requestId: 'r1' });
			expect(again).toEqual({ seq: retry.seq, isDuplicate: true });
		});

		it('gives the dedup field a per-field TTL on a server that supports HEXPIRE', async () => {
			await r.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1', idempotencyTtl: 100 });
			const idmpKey = client.key('replay:idmp:p1:{chat}');
			const [ttlMs] = await client.redis.hpttl(idmpKey, 'FIELDS', 1, 'r1');
			expect(ttlMs).toBeGreaterThan(0);
			expect(ttlMs).toBeLessThanOrEqual(100 * 1000);
		});

		it('falls back to a whole-hash TTL (no per-field TTL) on a pre-7.4 / pre-Valkey-9 server', async () => {
			client.redis._info = '# Server\nredis_version:6.2.0\n';
			const rOld = createReplay(client, { storage: 'stream', size: 100 });
			await rOld.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1', idempotencyTtl: 100 });
			const idmpKey = client.key('replay:idmp:p1:{chat}');
			const [ttlMs] = await client.redis.hpttl(idmpKey, 'FIELDS', 1, 'r1');
			expect(ttlMs).toBe(-1); // field present, no per-field TTL (whole-hash EXPIRE path)
		});

		it('uses per-field TTL on Valkey 9.0+ (pinned redis_version notwithstanding)', async () => {
			client.redis._info = '# Server\nredis_version:7.2.4\nserver_name:valkey\nvalkey_version:9.0.0\n';
			const rValkey = createReplay(client, { storage: 'stream', size: 100 });
			await rValkey.publishIdempotent(platform, 'chat', 'e', { n: 1 }, { producerId: 'p1', requestId: 'r1', idempotencyTtl: 100 });
			const idmpKey = client.key('replay:idmp:p1:{chat}');
			const [ttlMs] = await client.redis.hpttl(idmpKey, 'FIELDS', 1, 'r1');
			expect(ttlMs).toBeGreaterThan(0);
		});

		it('drops a stored entry with an unknown envelope version as corruption', async () => {
			const key = client.key('replay:streambuf:{chat}');
			await client.redis.xadd(key, '1-0', 'v', '99', 'event', 'e', 'data', '{"n":1}');
			await client.redis.xadd(key, '2-0', 'v', '1', 'event', 'ok', 'data', '{"n":2}');
			expect(await r.since('chat', 0)).toEqual([{ seq: 2, topic: 'chat', event: 'ok', data: { n: 2 } }]);
		});

		it('drops a stored entry missing a required field as corruption', async () => {
			const key = client.key('replay:streambuf:{chat}');
			await client.redis.xadd(key, '1-0', 'v', '1', 'data', '{"n":1}'); // no event
			expect(await r.since('chat', 0)).toEqual([]);
		});
	});
});
