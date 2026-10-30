/**
 * Integration tests for redis/replay against a real Redis 7 server.
 *
 * Exercises the actual Lua atomicity (INCR + ZADD + ZREMRANGEBYRANK + EXPIRE
 * in one EVAL) that the in-memory mock can only approximate. The mock-based
 * suite stays at test/redis/replay.test.js; this file is additive.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createReplay } from '../../../src/redis/replay.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

// Runs against both standalone and cluster backends. The seq and buf keys
// share a {topic} hash tag so the 2-key publish eval and the per-topic
// UNLINK stay on one slot under Redis Cluster.
const describeIntegration = describe;

describeIntegration('redis replay (integration)', () => {
	let client;
	let platform;
	let replay;

	beforeAll(() => {
		client = createBackendClient({
			keyPrefix: 'inttest-replay:'
		});
	});

	beforeEach(async () => {
		// Wipe everything under our prefix so each test starts clean.
		await resetBackendKeys(client);

		platform = mockPlatform();
		replay = createReplay(client, { size: 5 });
	});

	afterAll(async () => {
		await client.quit();
	});

	it('reports a hole inside the replayed range as truncated', async () => {
		for (let n = 1; n <= 4; n++) await replay.publish(platform, 'holes', 'change', { n });
		await client.redis.zremrangebyscore(client.key('replay:buf:{holes}'), 2, 2);
		platform.reset();
		await replay.replay({}, 'holes', 0, platform);
		expect(platform.sent.map((frame) => frame.event === 'msg' ? frame.data.seq : frame.event)).toEqual(['truncated', 1, 3, 4, 'end']);
	});

	it('correlates real stored history and an empty truncated end', async () => {
		for (let n = 1; n <= 3; n++) await replay.publish(platform, 'wire', 'change', { n });
		await replay.replay({}, 'wire', 0, platform, 'history');
		const messages = platform.sent.filter((frame) => frame.event === 'msg');
		expect(messages.map((frame) => frame.data.seq)).toEqual([1, 2, 3]);
		for (const frame of messages) expect(frame.data.reqId).toBe('history');
		expect(platform.sent.at(-1).data).toEqual({ reqId: 'history' });
		await client.redis.del(client.key('replay:buf:{wire}'));
		platform.reset();
		await replay.replay({}, 'wire', 1, platform, 'expired');
		expect(platform.sent.at(-1).data).toEqual({ reqId: 'expired', truncated: true });
	});

	describe('publish + Lua envelope', () => {
		it('increments seq independently per topic and broadcasts', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			expect(await replay.seq('chat')).toBe(2);
			expect(await replay.seq('todos')).toBe(1);
			expect(platform.published).toHaveLength(3);
			// The authoritative Lua-INCR seq is threaded onto the LIVE frame (not just
			// stored), so a resuming client dedups against the same seq space the
			// buffer replays - across a restart or, with the shared Redis buffer,
			// across instances.
			expect(platform.published[0].options?.seq).toBe(1); // chat #1
			expect(platform.published[1].options?.seq).toBe(2); // chat #2
			expect(platform.published[2].options?.seq).toBe(1); // todos #1
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

		it('gap() at lastSeenSeq 0 stays clean, replay() from 0 reports a trimmed head', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
			platform.reset();
			await replay.replay({}, 'chat', 0, platform);
			expect(platform.sent.some((s) => s.event === 'truncated')).toBe(false);

			for (let i = 2; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
			platform.reset();
			await replay.replay({}, 'chat', 0, platform);
			const events = platform.sent.map((s) => s.event);
			expect(events[0]).toBe('truncated');
			expect(platform.sent.at(-1)).toMatchObject({ event: 'end', data: { truncated: true } });
		});

		it('returns clean for an unknown topic at position 0, truncated past it', async () => {
			expect(await replay.gap('nonexistent', 0)).toEqual({ truncated: false, missingFrom: null });
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: true, missingFrom: 6 });
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

		// Pre-fix bug: a client could send a `resume` frame with
		// lastSeenSeqs:{"private:room":0} and read the entire replay
		// buffer for a topic they could not subscribe to live. The fix
		// routes platform.checkSubscribe before the buffer fetch.
		it('honors platform.checkSubscribe denial: emits denied, no msg/end frames', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'private:room', 'created', { id: i, secret: 'leak' });
			}

			const fakeWs = {};
			platform.reset();
			platform.checkSubscribe = (_ws, _topic) => 'FORBIDDEN';

			await replay.replay(fakeWs, 'private:room', 0, platform);

			// Single denied frame; no msg or end.
			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0]).toMatchObject({
				topic: '__replay:private:room',
				event: 'denied',
				data: { code: 'FORBIDDEN' }
			});
			expect(platform.sent.find((s) => s.event === 'msg')).toBeUndefined();
			expect(platform.sent.find((s) => s.event === 'end')).toBeUndefined();
		});

		it('async checkSubscribe returning a Promise<denial> still denies (no leak via async)', async () => {
			await replay.publish(platform, 'private:room', 'created', { id: 1, secret: 'leak' });

			const fakeWs = {};
			platform.reset();
			platform.checkSubscribe = async () => 'UNAUTHENTICATED';

			await replay.replay(fakeWs, 'private:room', 0, platform);
			expect(platform.sent[0].event).toBe('denied');
			expect(platform.sent[0].data.code).toBe('UNAUTHENTICATED');
			expect(platform.sent.find((s) => s.event === 'msg')).toBeUndefined();
		});

		it('denies when the platform cannot authorize at all', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			const fakeWs = {};
			platform.reset();
			platform.checkSubscribe = undefined;
			await replay.replay(fakeWs, 'chat', 0, platform);
			// This gate is the ONLY authorization on the resume read path, so
			// a platform that cannot authorize has to be refused rather than
			// waved through: degrade availability, never authorization.
			expect(platform.sent.some((s) => s.event === 'msg')).toBe(false);
			expect(platform.sent.some((s) => s.event === 'denied')).toBe(true);
		});
	});

	describe('resumeHook against real Redis', () => {
		it('drives multi-topic gap-fill via the existing replay() pipeline', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 'c1' });
			await replay.publish(platform, 'chat', 'msg', { id: 'c2' });
			await replay.publish(platform, 'todos', 'msg', { id: 't1' });
			await replay.publish(platform, 'todos', 'msg', { id: 't2' });
			await replay.publish(platform, 'todos', 'msg', { id: 't3' });

			const fakeWs = {};
			platform.reset();
			const hook = replay.resumeHook();
			await hook(fakeWs, {
				lastSeenSeqs: { chat: 1, todos: 2 },
				platform
			});

			const chat = platform.sent.filter((s) => s.topic === '__replay:chat');
			const todos = platform.sent.filter((s) => s.topic === '__replay:todos');
			// chat: msg seq=2 + end. todos: msg seq=3 + end.
			expect(chat.filter((s) => s.event === 'msg').map((s) => s.data.seq)).toEqual([2]);
			expect(todos.filter((s) => s.event === 'msg').map((s) => s.data.seq)).toEqual([3]);
			expect(chat.find((s) => s.event === 'end')).toBeDefined();
			expect(todos.find((s) => s.event === 'end')).toBeDefined();
		});

		it('emits truncated for trimmed topics through the wire ZRANGEBYSCORE path', async () => {
			// size: 5; publish 8 to evict seqs 1-3.
			for (let i = 1; i <= 8; i++) {
				await replay.publish(platform, 'chat', 'msg', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			const hook = replay.resumeHook();
			await hook(fakeWs, { lastSeenSeqs: { chat: 1 }, platform });

			expect(platform.sent.find((s) => s.event === 'truncated')).toBeDefined();
		});

		it('resolves the covered watermark from the real ZRANGEBYSCORE read', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 'c1' });
			await replay.publish(platform, 'chat', 'msg', { id: 'c2' });
			await replay.publish(platform, 'todos', 'msg', { id: 't1' });

			platform.reset();
			const hook = replay.resumeHook();
			const covered = await hook({}, { lastSeenSeqs: { chat: 0, todos: 1 }, platform });

			// chat gap-filled up to 2; todos was current at its floor 1. The
			// adapter's replay-to-live cutover dedups its held live frames against
			// exactly these values.
			expect(covered).toEqual({ chat: 2, todos: 1 });
			expect(await replay.replay({}, 'chat', 1, platform)).toBe(2);
		});

		it('never trusts an offset above the real seq counter as a watermark', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 'c1' });
			await replay.publish(platform, 'chat', 'msg', { id: 'c2' });

			platform.reset();
			// Counter is at 2; the claim of 999 is unverifiable, so replay()
			// reports nothing and the hook omits the topic - the adapter falls
			// back to its conservative pre-window floor instead of skipping
			// frames below the inflated claim.
			expect(await replay.replay({}, 'chat', 999, platform)).toBeUndefined();
			const hook = replay.resumeHook();
			expect(await hook({}, { lastSeenSeqs: { chat: 999 }, platform })).toEqual({});
		});
	});

	describe('TTL (real EXPIRE applied by the Lua script)', () => {
		it('sets a TTL on both keys when ttl is non-zero', async () => {
			const r = createReplay(client, { size: 5, ttl: 600 });
			await r.publish(platform, 'chat', 'created', { id: 1 });

			// Every key of the topic, the epoch and use stamp included, carries
			// the same ttl, and a later publish or read never stretches it.
			const kinds = ['seq', 'buf', 'epoch', 'used'];
			for (const kind of kinds) {
				const ttl = await client.redis.ttl(client.key('replay:' + kind + ':{chat}'));
				expect(ttl, kind).toBeGreaterThan(0);
				expect(ttl, kind).toBeLessThanOrEqual(600);
			}
			await r.publish(platform, 'chat', 'created', { id: 2 });
			await r.since('chat', 0);
			await r.seq('chat');
			for (const kind of kinds) {
				expect(await client.redis.ttl(client.key('replay:' + kind + ':{chat}')), kind).toBeLessThanOrEqual(600);
			}
		});

		it('clearTopic keeps the counter and epoch under the same ttl', async () => {
			const r = createReplay(client, { size: 5, ttl: 600 });
			await r.publish(platform, 'chat', 'created', { id: 1 });
			await r.clearTopic('chat');
			for (const kind of ['seq', 'epoch', 'used']) {
				const ttl = await client.redis.ttl(client.key('replay:' + kind + ':{chat}'));
				expect(ttl, kind).toBeGreaterThan(0);
				expect(ttl, kind).toBeLessThanOrEqual(600);
			}
		});

		it('leaves keys with no TTL when ttl is unset', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await client.redis.ttl(client.key('replay:seq:{chat}'))).toBe(-1);
			expect(await client.redis.ttl(client.key('replay:buf:{chat}'))).toBe(-1);
		});
	});

	describe('maxTopics and epochs (real Lua)', () => {
		for (const storage of ['sortedset', 'stream']) {
			it(`${storage}: evicts the least recently published topic and never reuses its epoch`, async () => {
				const r = createReplay(client, { storage, size: 5, maxTopics: 2 });
				await r.publish(platform, 'a', 'created', { id: 1 });
				await new Promise((resolve) => setTimeout(resolve, 1100));
				await r.publish(platform, 'b', 'created', { id: 1 });
				await new Promise((resolve) => setTimeout(resolve, 1100));
				// A steady publish to a stamps its use in a's own slot.
				await r.publish(platform, 'a', 'created', { id: 2 });
				const bEpoch = await r.currentEpoch('b');
				await new Promise((resolve) => setTimeout(resolve, 1100));
				await r.publish(platform, 'c', 'created', { id: 1 });

				expect(await r.seq('a')).toBe(2);
				expect(await r.seq('b')).toBe(0);
				expect(await r.currentEpoch('b')).toBe(0);
				expect(await client.redis.zcard(client.key('replay:topics:{replay-topics}'))).toBe(2);

				await r.publish(platform, 'b', 'created', { id: 2 });
				expect(await r.currentEpoch('b')).not.toBe(bEpoch);
				platform.reset();
				await r.resumeHook()({}, { lastSeenSeqs: { b: 1 }, lastSeenEpochs: { b: bEpoch }, platform });
				expect(platform.sent.some((s) => s.topic === '__replay:b' && s.event === 'rehydrate')).toBe(true);
			});

			it(`${storage}: enrolls topics stored before the upgrade and recreates them under a new epoch`, async () => {
				await client.redis.set(client.key('replay:seq:{room}'), '50');
				await client.redis.set(client.key('replay:epoch:{room}'), '2');
				await client.redis.set(client.key('replay:epoch:{idle}'), '40');
				const r = createReplay(client, { storage, size: 5, maxTopics: 1 });

				await r.publish(platform, 'fresh', 'created', { id: 1 });
				expect(await client.redis.hget(client.key('replay:seed:{replay-topics}'), 'seeded')).toBe('1');
				// Never used since the upgrade, so evicted first.
				expect(await r.seq('room')).toBe(0);

				await r.publish(platform, 'room', 'created', { id: 1 });
				const epoch = await r.currentEpoch('room');
				expect(epoch).not.toBe(2);
				expect(Number.isInteger(epoch) && epoch >= 1 && epoch <= 4294967295).toBe(true);
			});
		}

		for (const storage of ['sortedset', 'stream']) {
			it(`${storage}: keeps a topic stored before the upgrade that is read after it`, async () => {
				// 'recent' sorts first, so it is the first candidate: only its
				// read stamp keeps it.
				await client.redis.set(client.key('replay:seq:{stale}'), '3');
				await client.redis.set(client.key('replay:seq:{recent}'), '3');
				const r = createReplay(client, { storage, size: 5, maxTopics: 2 });
				await r.since('recent', 0);
				await r.publish(platform, 'fresh', 'created', { id: 1 });
				expect(await r.seq('stale')).toBe(0);
				expect(await r.seq('recent')).toBe(3);
			});

			it(`${storage}: a recreated topic draws a new epoch`, async () => {
				const r = createReplay(client, { storage, size: 5, maxTopics: 10 });
				await r.publish(platform, 'a', 'created', { id: 1 });
				const before = await r.currentEpoch('a');
				await client.redis.del(client.key('replay:seq:{a}'));
				await r.publish(platform, 'a', 'created', { id: 2 });
				expect(await r.seq('a')).toBe(1);
				expect(await r.currentEpoch('a')).not.toBe(before);
			});

			it(`${storage}: a read stamps the topic's use in the same round trip`, async () => {
				const r = createReplay(client, { storage, size: 5, maxTopics: 10, ttl: 600 });
				await r.publish(platform, 'chat', 'created', { id: 1 });
				await client.redis.set(client.key('replay:used:{chat}'), '1', 'EX', 600);
				const reader = createReplay(client, { storage, size: 5, maxTopics: 10, ttl: 600 });
				expect((await reader.since('chat', 0)).map((m) => m.seq)).toEqual([1]);
				expect(Number(await client.redis.get(client.key('replay:used:{chat}')))).toBeGreaterThan(1);
				const ttl = await client.redis.ttl(client.key('replay:used:{chat}'));
				expect(ttl).toBeGreaterThan(0);
				expect(ttl).toBeLessThanOrEqual(600);
				// An unknown topic gets no keys from a read.
				await reader.seq('ghost');
				expect(await client.redis.exists(client.key('replay:used:{ghost}'))).toBe(0);
			});
		}

		it('stream: publishIdempotent creates a topic and dedups within its seq space', async () => {
			const r = createReplay(client, { storage: 'stream', size: 5, maxTopics: 1 });
			const first = await r.publishIdempotent(platform, 'room', 'created', { id: 1 }, { producerId: 'p', requestId: 'r1' });
			expect(first).toEqual({ seq: 1, isDuplicate: false });
			expect(await r.publishIdempotent(platform, 'room', 'created', { id: 1 }, { producerId: 'p', requestId: 'r1' }))
				.toEqual({ seq: 1, isDuplicate: true });
			await r.publish(platform, 'other', 'created', { id: 1 });
			// room was evicted: the retry is published into the new seq space.
			const retry = await r.publishIdempotent(platform, 'room', 'created', { id: 1 }, { producerId: 'p', requestId: 'r1' });
			expect(retry).toEqual({ seq: 1, isDuplicate: false });
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
