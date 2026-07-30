import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay, ReplayStorageError, ReplaySerializationError } from '../../src/postgres/replay.js';

describe('postgres replay', () => {
	let client;
	let platform;
	let replay;

	beforeEach(() => {
		client = mockPgClient();
		platform = mockPlatform();
		replay = createReplay(client, { size: 5, cleanupInterval: 0 });
	});

	afterEach(() => {
		replay.destroy();
	});

	describe('createReplay', () => {
		it('validates size option', () => {
			expect(() => createReplay(client, { size: 0 })).toThrow('positive integer');
			expect(() => createReplay(client, { size: -1 })).toThrow('positive integer');
			expect(() => createReplay(client, { size: 1.5 })).toThrow('positive integer');
			expect(() => createReplay(client, { size: 'abc' })).toThrow('positive integer');
		});

		it('validates ttl option', () => {
			expect(() => createReplay(client, { ttl: -1 })).toThrow('non-negative integer');
			expect(() => createReplay(client, { ttl: 1.5 })).toThrow('non-negative integer');
		});

		it('validates table name', () => {
			expect(() => createReplay(client, { table: 'drop table;--' })).toThrow('invalid table name');
			expect(() => createReplay(client, { table: '123bad' })).toThrow('invalid table name');
		});

		it('rejects reserved Postgres schema names', () => {
			expect(() => createReplay(client, { table: 'pg_class' })).toThrow('reserved Postgres schema');
			expect(() => createReplay(client, { table: 'information_schema_tables' })).toThrow('reserved Postgres schema');
		});

		it('works with no options', () => {
			const r = createReplay(client, { cleanupInterval: 0 });
			expect(typeof r.publish).toBe('function');
			expect(typeof r.seq).toBe('function');
			expect(typeof r.since).toBe('function');
			expect(typeof r.replay).toBe('function');
			expect(typeof r.clear).toBe('function');
			expect(typeof r.clearTopic).toBe('function');
			expect(typeof r.destroy).toBe('function');
			r.destroy();
		});
	});

	describe('publish', () => {
		it('calls platform.publish with the same arguments plus the authoritative seq', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			// The topic/event/data are forwarded unchanged; the CTE's authoritative
			// seq is threaded as the publish option so the live frame carries the same
			// seq the buffer replays.
			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 }, options: { seq: 1 } }
			]);
		});

		it('returns platform.publish result', async () => {
			const result = await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(result).toBe(true);
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
	});

	describe('seq', () => {
		it('returns 0 for unknown topics', async () => {
			expect(await replay.seq('nonexistent')).toBe(0);
		});

		it('reads the counter as BIGINT, never narrowing to INTEGER', async () => {
			// seq is stored BIGINT; a ::int cast raised 22003 server-side past
			// 2^31 (~24.9 days at 1k events/s). The mock cannot model the cast,
			// so guard the generated SQL directly: the counter read must cast to
			// bigint, so a revert to ::int fails here without needing a live PG.
			const spy = vi.spyOn(client, 'query');
			await replay.seq('chat');
			const seqRead = spy.mock.calls.find(([arg]) => {
				const text = typeof arg === 'string' ? arg : arg?.text;
				return typeof text === 'string' && text.includes('current_seq');
			});
			expect(seqRead, 'seq() should issue a current_seq read').toBeTruthy();
			const text = typeof seqRead[0] === 'string' ? seqRead[0] : seqRead[0].text;
			expect(text).toContain('::bigint');
			expect(text).not.toContain('::int ');
			spy.mockRestore();
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

		it('returns empty array for unknown topics', async () => {
			expect(await replay.since('nonexistent', 0)).toEqual([]);
		});

		it('returns all messages when since is 0', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });

			const missed = await replay.since('chat', 0);
			expect(missed).toHaveLength(2);
		});

		it('returns [] on a malformed since instead of the entire buffer', async () => {
			// Matches the Redis backends: a negative bound in `seq > $2` would
			// dump the whole buffer for buggy host code that forwards client
			// input unchecked.
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.since('chat', -1)).toEqual([]);
			expect(await replay.since('chat', 1.5)).toEqual([]);
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
			expect(all[0].data).toEqual({ id: 3 });
			expect(all[4].seq).toBe(7);
			expect(all[4].data).toEqual({ id: 7 });
		});
	});

	describe('replay', () => {
		it('sends missed messages on __replay:{topic} then end marker', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'chat', 'created', { id: 3 });

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			expect(platform.sent).toHaveLength(3);

			expect(platform.sent[0]).toEqual({
				ws: fakeWs,
				topic: '__replay:chat',
				event: 'msg',
				data: { seq: 2, event: 'created', data: { id: 2 } }
			});

			expect(platform.sent[1]).toEqual({
				ws: fakeWs,
				topic: '__replay:chat',
				event: 'msg',
				data: { seq: 3, event: 'created', data: { id: 3 } }
			});

			expect(platform.sent[2]).toEqual({
				ws: fakeWs,
				topic: '__replay:chat',
				event: 'end',
				data: { reqId: undefined }
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

		it('sends only end marker for unknown topics', async () => {
			const fakeWs = {};
			await replay.replay(fakeWs, 'nonexistent', 0, platform);

			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].event).toBe('end');
		});

		it('does not affect the publish history', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			platform.published = [];

			await replay.replay({}, 'chat', 0, platform);

			expect(platform.published).toEqual([]);
		});
	});

	describe('truncation detection', () => {
		it('sends truncated event when buffer was trimmed past sinceSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
			expect(truncated[0].data).toBeNull();
		});

		it('does not send truncated when sinceSeq is within buffer', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 2, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(0);
		});

		it('detects truncation when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			// Wipe all buffered rows but leave the seq counter
			await client.query('DELETE FROM svti_replay WHERE topic = $1', ['chat']);

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
			expect(truncated[0].data).toBeNull();

			const end = platform.sent.filter((s) => s.event === 'end');
			expect(end).toHaveLength(1);
		});

		it('does not send truncated when sinceSeq is 0 even with empty buffer', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			await client.query('DELETE FROM svti_replay WHERE topic = $1', ['chat']);

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 0, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(0);
		});
	});

	describe('gap', () => {
		it('validates lastSeenSeq is a non-negative integer', async () => {
			await expect(replay.gap('chat', -1)).rejects.toThrow('non-negative integer');
			await expect(replay.gap('chat', 1.5)).rejects.toThrow('non-negative integer');
			await expect(replay.gap('chat', 'abc')).rejects.toThrow('non-negative integer');
		});

		it('returns not truncated when lastSeenSeq is 0 (fresh client)', async () => {
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

		it('returns not truncated when fully caught up', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 3)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns truncated with missingFrom when buffer was trimmed', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			// size: 5 means seqs 3..7 are buffered, seqs 1, 2 are gone
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns truncated when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			await client.query('DELETE FROM svti_replay WHERE topic = $1', ['chat']);

			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns not truncated for an unknown topic', async () => {
			expect(await replay.gap('nonexistent', 0)).toEqual({ truncated: false, missingFrom: null });
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns not truncated when consumer is ahead of the buffer', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 10)).toEqual({ truncated: false, missingFrom: null });
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

		it('no-ops on empty lastSeenSeqs', async () => {
			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: {}, platform });
			expect(platform.sent).toHaveLength(0);
		});

		it('replays a single topic from the given seq', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			await replay.publish(platform, 'chat', 'msg', { id: 3 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			expect(platform.sent.filter((s) => s.event === 'msg')).toHaveLength(2);
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
			expect(chat.filter((s) => s.event === 'msg')).toHaveLength(1);
			expect(todos.filter((s) => s.event === 'msg')).toHaveLength(1);
		});

		it('coerces non-numeric or negative sinceSeq to 0', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, {
				lastSeenSeqs: { chat: 'oops', todos: -3 },
				platform
			});

			expect(platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg')).toHaveLength(1);
		});

		it('emits truncated when the buffer no longer holds the next seq', async () => {
			// size: 5; publish 7 to trim seqs 1-2.
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'msg', { id: i });
			}
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			expect(platform.sent.find((s) => s.event === 'truncated')).toBeDefined();
		});
	});

	describe('per-topic epoch', () => {
		it('chunks resume epoch lookups instead of sending one oversized ANY array', async () => {
			const topics = Array.from({ length: 1001 }, (_, i) => 'room:' + i);
			const lastSeenSeqs = Object.fromEntries(topics.map((topic) => [topic, 0]));
			// Present a deliberately stale generation so the hook stops after
			// currentEpochs and does not issue 1001 per-topic replay reads.
			const lastSeenEpochs = Object.fromEntries(topics.map((topic) => [topic, 1]));
			const spy = vi.spyOn(client, 'query');
			try {
				await replay.resumeHook()({}, { lastSeenSeqs, lastSeenEpochs, platform });
				const epochReads = spy.mock.calls
					.map(([arg]) => arg)
					.filter((arg) => arg?.name === 'replay_epochs_svti_replay');
				expect(epochReads).toHaveLength(2);
				expect(epochReads.map((arg) => arg.values[0].length)).toEqual([1000, 1]);
				expect(epochReads.flatMap((arg) => arg.values[0])).toEqual(topics);
			} finally {
				spy.mockRestore();
			}
		});

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
			// Next publish restarts seq at 1 but does NOT re-bump: clearTopic keeps
			// the seq-table row (seq reset to 0), so the publish takes the UPDATE
			// branch and carries the already-bumped epoch forward. The bump is done
			// once, atomically, by clearTopic - this is the durable single-bump
			// guarantee that keeps repeated clears monotonic.
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			expect(await replay.seq('chat')).toBe(1);
			expect(await replay.currentEpoch('chat')).toBe(after);
		});

		it('keeps the epoch monotonic across repeated clears', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			expect(await replay.currentEpoch('chat')).toBe(1);
			await replay.clearTopic('chat');
			expect(await replay.currentEpoch('chat')).toBe(2);
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			await replay.clearTopic('chat');
			expect(await replay.currentEpoch('chat')).toBe(3);
			await replay.publish(platform, 'chat', 'msg', { id: 3 });
			await replay.clearTopic('chat');
			expect(await replay.currentEpoch('chat')).toBe(4);
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
	});

	describe('covered watermark (recovery-barrier contract)', () => {
		// Mirror of the Redis backends' watermark contract: replay() and the
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

		it('replay() resolves undefined when the subscribe gate denies', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.checkSubscribe = async () => 'FORBIDDEN';
			expect(await replay.replay({}, 'chat', 0, platform)).toBeUndefined();
		});

		it('replay() resolves undefined on a malformed sinceSeq (defense-in-depth path)', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.reset();
			expect(await replay.replay({}, 'chat', -1, platform)).toBeUndefined();
			expect(await replay.replay({}, 'chat', 1.5, platform)).toBeUndefined();
			// The gate refuses to dump the buffer and keeps the wire shape:
			// no frames, just the end marker per attempt.
			expect(platform.sent.filter((s) => s.event === 'msg')).toHaveLength(0);
			expect(platform.sent.filter((s) => s.event === 'end')).toHaveLength(2);
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

	describe('clear / clearTopic', () => {
		it('clear resets everything', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clear();

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(0);
			expect(await replay.since('chat', 0)).toEqual([]);
		});

		it('clearTopic resets only that topic', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clearTopic('chat');

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(1);
		});

		it('clear deletes data and bumps every epoch inside one BEGIN/COMMIT transaction', async () => {
			// Spy on pool.connect() to capture the transaction-control statements.
			const seen = [];
			const origConnect = client.pool.connect.bind(client.pool);
			client.pool.connect = async () => {
				const c = await origConnect();
				return {
					query: async (textOrObj, values) => {
						const text = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
						seen.push(text.trim());
						return c.query(textOrObj, values);
					},
					release: c.release
				};
			};
			try {
				await replay.clear();
			} finally {
				client.pool.connect = origConnect;
			}
			expect(seen[0]).toBe('BEGIN');
			expect(seen[seen.length - 1]).toBe('COMMIT');
			// The data rows are deleted and the seq-table rows are bump-and-kept
			// (UPDATE ... epoch + 1), not deleted, so the epoch survives a global
			// clear and a republished topic keeps climbing.
			expect(seen.filter((s) => s.startsWith('DELETE')).length).toBe(1);
			expect(seen.some((s) => s.startsWith('UPDATE') && s.includes('epoch + 1'))).toBe(true);
			// Lock order: the seq-counter reset (which locks the row a publish
			// contends on) must run BEFORE the data delete, so a concurrent publish
			// cannot interleave between them and duplicate a (topic, seq).
			const clearReset = seen.findIndex((s) => s.startsWith('UPDATE') && s.includes('epoch + 1'));
			const clearDelete = seen.findIndex((s) => s.startsWith('DELETE'));
			expect(clearReset).toBeLessThan(clearDelete);
		});

		it('clearTopic deletes data and bumps the epoch inside one BEGIN/COMMIT transaction', async () => {
			const seen = [];
			const origConnect = client.pool.connect.bind(client.pool);
			client.pool.connect = async () => {
				const c = await origConnect();
				return {
					query: async (textOrObj, values) => {
						const text = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
						seen.push(text.trim());
						return c.query(textOrObj, values);
					},
					release: c.release
				};
			};
			try {
				await replay.clearTopic('chat');
			} finally {
				client.pool.connect = origConnect;
			}
			expect(seen[0]).toBe('BEGIN');
			expect(seen[seen.length - 1]).toBe('COMMIT');
			// The data row is deleted; the seq-table row is kept (seq reset to 0,
			// epoch bumped) via an upsert so the epoch survives the reset.
			expect(seen.filter((s) => s.startsWith('DELETE')).length).toBe(1);
			expect(seen.some((s) => s.startsWith('INSERT') && s.includes('epoch + 1'))).toBe(true);
			// Lock order: the seq-counter upsert (which locks the row a publish to
			// this topic contends on) must run BEFORE the data delete, so a
			// concurrent publish cannot interleave and duplicate a (topic, seq).
			const topicReset = seen.findIndex((s) => s.startsWith('INSERT') && s.includes('epoch + 1'));
			const topicDelete = seen.findIndex((s) => s.startsWith('DELETE'));
			expect(topicReset).toBeLessThan(topicDelete);
		});
	});

	describe('cross-instance size cap', () => {
		it('fresh instance trims rows left by a previous instance', async () => {
			// Instance1 publishes 4 rows into a size:2 buffer
			const replay1 = createReplay(client, { size: 2, cleanupInterval: 0 });
			await replay1.publish(platform, 'chat', 'msg', { id: 1 });
			await replay1.publish(platform, 'chat', 'msg', { id: 2 });
			await replay1.publish(platform, 'chat', 'msg', { id: 3 });
			// After 3 publishes with size:2, replay1 has trimmed to 2
			let all = await replay1.since('chat', 0);
			expect(all).toHaveLength(2);
			replay1.destroy();

			// Simulate the first instance crashing and publishing more rows
			// by directly inserting into the mock DB
			// Actually, let's just publish one more from a fresh instance
			const replay2 = createReplay(client, { size: 2, cleanupInterval: 0 });
			await replay2.publish(platform, 'chat', 'msg', { id: 4 });

			// The fresh instance should have seeded its count from the DB
			// and trimmed to 2 rows
			all = await replay2.since('chat', 0);
			expect(all).toHaveLength(2);
			expect(all[0].data).toEqual({ id: 3 });
			expect(all[1].data).toEqual({ id: 4 });
			replay2.destroy();
		});
	});

	describe('trim failure does not block live publish', () => {
		it('publish succeeds and live broadcast happens when trim query fails', async () => {
			const r = createReplay(client, { size: 2, cleanupInterval: 0 });

			// Publish 3 messages so trim triggers on the 3rd
			await r.publish(platform, 'chat', 'msg', { id: 1 });
			await r.publish(platform, 'chat', 'msg', { id: 2 });

			// Make the trim query fail
			const origQuery = client.query.bind(client);
			let queryCount = 0;
			client.query = async (textOrObj, values) => {
				const sql = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
				if (sql && sql.includes('DELETE FROM') && sql.includes('seq <=')) {
					throw new Error('trim failed');
				}
				return origQuery(textOrObj, values);
			};

			platform.reset();

			// 3rd publish should still succeed (trim failure is non-fatal)
			await r.publish(platform, 'chat', 'msg', { id: 3 });

			// Live broadcast should have happened
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0].data).toEqual({ id: 3 });

			// seq and since should include the new message
			expect(await r.seq('chat')).toBe(3);
			const msgs = await r.since('chat', 2);
			expect(msgs).toHaveLength(1);
			expect(msgs[0].data).toEqual({ id: 3 });

			client.query = origQuery;
			r.destroy();
		});
	});

	describe('oversize buffer after trim failure', () => {
		it('since() returns more than maxSize entries when trim fails and cleanup is disabled', async () => {
			const r = createReplay(client, { size: 2, cleanupInterval: 0 });

			await r.publish(platform, 'chat', 'msg', { id: 1 });
			await r.publish(platform, 'chat', 'msg', { id: 2 });

			// Make trim fail from now on
			const origQuery = client.query.bind(client);
			client.query = async (textOrObj, values) => {
				const sql = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
				if (sql && sql.includes('DELETE FROM') && sql.includes('seq <=')) {
					throw new Error('trim failed');
				}
				return origQuery(textOrObj, values);
			};

			// These publishes succeed (insert works) but trim is skipped
			await r.publish(platform, 'chat', 'msg', { id: 3 });
			await r.publish(platform, 'chat', 'msg', { id: 4 });

			// Buffer is now oversized: 4 entries with maxSize=2
			const all = await r.since('chat', 0);
			expect(all).toHaveLength(4);

			client.query = origQuery;

			// Next successful publish re-trims
			await r.publish(platform, 'chat', 'msg', { id: 5 });
			const afterTrim = await r.since('chat', 0);
			expect(afterTrim).toHaveLength(2);
			expect(afterTrim[0].data).toEqual({ id: 4 });
			expect(afterTrim[1].data).toEqual({ id: 5 });

			r.destroy();
		});

		it('replay detects truncation after oversize buffer is trimmed', async () => {
			const r = createReplay(client, { size: 2, cleanupInterval: 0 });

			// Build an oversized buffer
			const origQuery = client.query.bind(client);
			await r.publish(platform, 'chat', 'msg', { id: 1 });
			await r.publish(platform, 'chat', 'msg', { id: 2 });

			client.query = async (textOrObj, values) => {
				const sql = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
				if (sql && sql.includes('DELETE FROM') && sql.includes('seq <=')) {
					throw new Error('trim failed');
				}
				return origQuery(textOrObj, values);
			};
			await r.publish(platform, 'chat', 'msg', { id: 3 });
			await r.publish(platform, 'chat', 'msg', { id: 4 });

			client.query = origQuery;

			// Now trim succeeds, trimming seq 1,2,3 (keeping 4,5)
			await r.publish(platform, 'chat', 'msg', { id: 5 });

			// A client that last saw seq 2 should get truncation
			const fakeWs = {};
			platform.reset();
			await r.replay(fakeWs, 'chat', 2, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);

			r.destroy();
		});
	});

	describe('multi-instance sequence safety', () => {
		it('two replay instances produce unique sequences for the same topic', async () => {
			// Both instances share the same mock PG client,
			// simulating two app instances using the same database
			const replay1 = createReplay(client, { size: 100, cleanupInterval: 0 });
			const replay2 = createReplay(client, { size: 100, cleanupInterval: 0 });

			await replay1.publish(platform, 'chat', 'created', { from: 'instance1' });
			await replay2.publish(platform, 'chat', 'created', { from: 'instance2' });
			await replay1.publish(platform, 'chat', 'created', { from: 'instance1-again' });

			const all = await replay1.since('chat', 0);
			expect(all).toHaveLength(3);

			// Sequences must be strictly monotonically increasing with no duplicates
			const seqs = all.map((m) => m.seq);
			expect(seqs).toEqual([1, 2, 3]);

			replay1.destroy();
			replay2.destroy();
		});
	});

	describe('storage failure fallback (localFanout) log-correlation', () => {
		it('warns once carrying requestId on the localFanout fallback, then suppresses', async () => {
			const r = createReplay(client, { localFanoutOnStorageFailure: true, cleanupInterval: 0 });
			const origQuery = client.query.bind(client);
			client.query = async (textOrObj, values) => {
				const name = typeof textOrObj === 'object' ? textOrObj.name : '';
				if (name && name.startsWith('replay_publish_')) throw new Error('db down');
				return origQuery(textOrObj, values);
			};
			const warns = [];
			const spy = vi.spyOn(console, 'warn').mockImplementation((msg) => warns.push(msg));
			try {
				platform.requestId = 'req-pg';
				await r.publish(platform, 'chat', 'msg', { id: 1 });
				await r.publish(platform, 'chat', 'msg', { id: 2 });
			} finally {
				spy.mockRestore();
				client.query = origQuery;
			}
			expect(platform.published).toHaveLength(2); // both fell back to local publish
			expect(warns).toHaveLength(1);
			expect(warns[0]).toContain('[postgres replay]');
			expect(warns[0]).toContain('requestId=req-pg');
			// The raw topic is deliberately not logged (it can embed user ids).
			expect(warns[0]).not.toContain('chat');
			r.destroy();
		});
	});

	describe('serialization failure (caller-input bug)', () => {
		it('throws ReplaySerializationError when data contains a BigInt', async () => {
			let caught;
			try { await replay.publish(platform, 'chat', 'created', { id: 1n }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(caught).not.toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publish');
			expect(caught.cause).toBeInstanceOf(TypeError);
		});

		it('throws ReplaySerializationError when data contains a circular reference', async () => {
			const data = /** @type {any} */ ({ id: 1 });
			data.self = data;

			let caught;
			try { await replay.publish(platform, 'chat', 'created', data); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
		});

		it('does NOT fall back to platform.publish even with localFanoutOnStorageFailure: true', async () => {
			const r = createReplay(client, { localFanoutOnStorageFailure: true, cleanupInterval: 0 });
			try {
				let caught;
				try { await r.publish(platform, 'chat', 'created', { id: 1n }); }
				catch (err) { caught = err; }

				expect(caught).toBeInstanceOf(ReplaySerializationError);
				expect(platform.published).toHaveLength(0);
			} finally {
				r.destroy();
			}
		});

		it('does NOT issue any client.query when serialization fails', async () => {
			let queryCalls = 0;
			const origQuery = client.query.bind(client);
			client.query = (...args) => { queryCalls++; return origQuery(...args); };

			let caught;
			try { await replay.publish(platform, 'chat', 'created', { id: 1n }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(queryCalls).toBe(0);

			client.query = origQuery;
		});
	});

	describe('publishBatch', () => {
		it('persists a single-topic burst with contiguous seqs in caller order', async () => {
			await replay.publishBatch(platform, [
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } },
				{ topic: 'chat', event: 'updated', data: { id: 1, done: true } }
			]);

			expect(await replay.seq('chat')).toBe(3);
			const stored = await replay.since('chat', 0);
			expect(stored).toEqual([
				{ seq: 1, topic: 'chat', event: 'created', data: { id: 1 } },
				{ seq: 2, topic: 'chat', event: 'created', data: { id: 2 } },
				{ seq: 3, topic: 'chat', event: 'updated', data: { id: 1, done: true } }
			]);
		});

		it('broadcasts once via platform.publishBatched with per-message authoritative seqs', async () => {
			await replay.publishBatch(platform, [
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'todos', event: 'created', data: { id: 9 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } }
			]);

			expect(platform.publishedBatches).toHaveLength(1);
			expect(platform.publishedBatches[0].messages).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 }, options: { seq: 1 } },
				{ topic: 'todos', event: 'created', data: { id: 9 }, options: { seq: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 }, options: { seq: 2 } }
			]);
		});

		it('keeps per-topic counters independent across a multi-topic batch', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 0 });
			await replay.publishBatch(platform, [
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'todos', event: 'created', data: { id: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } },
				{ topic: 'todos', event: 'created', data: { id: 2 } }
			]);

			expect(await replay.seq('chat')).toBe(3);
			expect(await replay.seq('todos')).toBe(2);
		});

		it('interleaves with single publish preserving one contiguous seq space', async () => {
			await replay.publishBatch(platform, [
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } }
			]);
			await replay.publish(platform, 'chat', 'created', { id: 3 });
			await replay.publishBatch(platform, [{ topic: 'chat', event: 'created', data: { id: 4 } }]);

			const stored = await replay.since('chat', 0);
			expect(stored.map((m) => m.seq)).toEqual([1, 2, 3, 4]);
			expect(stored.map((m) => m.data.id)).toEqual([1, 2, 3, 4]);
		});

		it('seeds epoch 1 on a fresh topic and carries the epoch across clearTopic', async () => {
			await replay.publishBatch(platform, [{ topic: 'chat', event: 'created', data: { id: 1 } }]);
			expect(await replay.currentEpoch('chat')).toBe(1);

			await replay.clearTopic('chat');
			await replay.publishBatch(platform, [{ topic: 'chat', event: 'created', data: { id: 2 } }]);
			// clearTopic bumps the epoch and resets seq; the batch UPDATE branch
			// must carry the bumped epoch forward, not re-seed 1.
			expect(await replay.currentEpoch('chat')).toBe(2);
			expect(await replay.seq('chat')).toBe(1);
		});

		it('trims each topic that exceeds maxSize after the batch', async () => {
			// size is 5 (beforeEach); an 8-message burst leaves only the newest 5.
			const messages = [];
			for (let i = 1; i <= 8; i++) messages.push({ topic: 'chat', event: 'created', data: { id: i } });
			await replay.publishBatch(platform, messages);

			const stored = await replay.since('chat', 0);
			expect(stored.map((m) => m.seq)).toEqual([4, 5, 6, 7, 8]);
		});

		it('rejects the whole batch fail-closed when one payload cannot serialize', async () => {
			let caught;
			try {
				await replay.publishBatch(platform, [
					{ topic: 'chat', event: 'created', data: { id: 1 } },
					{ topic: 'chat', event: 'created', data: { id: 2n } }
				]);
			} catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplaySerializationError);
			expect(caught.op).toBe('publishBatch');
			expect(await replay.seq('chat')).toBe(0);
			expect(platform.published).toHaveLength(0);
			expect(platform.publishedBatches).toHaveLength(0);
		});

		it('validates message shape before any storage work', async () => {
			await expect(replay.publishBatch(platform, [{ topic: '', event: 'x' }])).rejects.toThrow('non-empty string topic');
			await expect(replay.publishBatch(platform, [{ topic: 'chat' }])).rejects.toThrow('string event');
			await expect(replay.publishBatch(platform, 'nope')).rejects.toThrow('expects an array');
		});

		it('no-ops an empty batch', async () => {
			expect(await replay.publishBatch(platform, [])).toBe(true);
			expect(platform.published).toHaveLength(0);
			expect(platform.publishedBatches).toHaveLength(0);
		});

		it('falls back to unstamped local fan-out when storage fails with localFanoutOnStorageFailure', async () => {
			const r = createReplay(client, { localFanoutOnStorageFailure: true, cleanupInterval: 0 });
			const origQuery = client.query.bind(client);
			client.query = async (textOrObj, values) => {
				const name = typeof textOrObj === 'object' ? textOrObj.name : '';
				if (name && name.startsWith('replay_publishbatch_')) throw new Error('db down');
				return origQuery(textOrObj, values);
			};
			const spy = vi.spyOn(console, 'warn').mockImplementation(() => {});
			try {
				const result = await r.publishBatch(platform, [
					{ topic: 'chat', event: 'created', data: { id: 1 } },
					{ topic: 'chat', event: 'created', data: { id: 2 } }
				]);
				expect(result).toBe(true);
			} finally {
				spy.mockRestore();
				client.query = origQuery;
				r.destroy();
			}
			// Degraded fan-out carries no authoritative seq.
			expect(platform.publishedBatches).toHaveLength(1);
			expect(platform.publishedBatches[0].messages).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } }
			]);
			expect(await replay.seq('chat')).toBe(0);
		});

		it('throws ReplayStorageError on storage failure without the fallback', async () => {
			const origQuery = client.query.bind(client);
			client.query = async (textOrObj, values) => {
				const name = typeof textOrObj === 'object' ? textOrObj.name : '';
				if (name && name.startsWith('replay_publishbatch_')) throw new Error('db down');
				return origQuery(textOrObj, values);
			};
			let caught;
			try {
				await replay.publishBatch(platform, [{ topic: 'chat', event: 'created', data: { id: 1 } }]);
			} catch (err) { caught = err; }
			client.query = origQuery;

			expect(caught).toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publishBatch');
			expect(platform.published).toHaveLength(0);
		});

		it('falls back to per-message publish when the platform lacks publishBatched', async () => {
			const bare = mockPlatform();
			delete bare.publishBatched;
			await replay.publishBatch(bare, [
				{ topic: 'chat', event: 'created', data: { id: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 } }
			]);

			expect(bare.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 }, options: { seq: 1 } },
				{ topic: 'chat', event: 'created', data: { id: 2 }, options: { seq: 2 } }
			]);
		});

		it('extracts user_id per message via forgetUserId', async () => {
			const r = createReplay(client, {
				cleanupInterval: 0,
				forgetUserId: ({ data }) => data?.author
			});
			try {
				await r.publishBatch(platform, [
					{ topic: 'chat', event: 'created', data: { id: 1, author: 'u1' } },
					{ topic: 'chat', event: 'created', data: { id: 2 } }
				]);
				const purged = await r.purgeUser(null, 'u1');
				expect(purged).toBe(1);
				const stored = await r.since('chat', 0);
				expect(stored.map((m) => m.data.id)).toEqual([2]);
			} finally {
				r.destroy();
			}
		});

		it('erases only the named tenant, not every tenant with that user id', async () => {
			const r = createReplay(client, {
				cleanupInterval: 0,
				forgetUserId: ({ data }) => data?.author
			});
			try {
				// Tenancy rides the wire topic as `@t/<tenantId>/<topic>`, which is
				// the scope the room-owner and presence-roster legs of the SAME
				// purge already apply. Deleting on user_id alone honoured the
				// tenant argument nowhere, so one tenant's right-to-erasure
				// destroyed every other tenant's rows for the same user id.
				await r.publishBatch(platform, [
					{ topic: '@t/acme/chat', event: 'created', data: { id: 1, author: 'u1' } },
					{ topic: '@t/globex/chat', event: 'created', data: { id: 2, author: 'u1' } },
					{ topic: 'chat', event: 'created', data: { id: 3, author: 'u1' } }
				]);

				expect(await r.purgeUser('acme', 'u1')).toBe(1);
				expect((await r.since('@t/acme/chat', 0)).map((m) => m.data.id)).toEqual([]);
				expect((await r.since('@t/globex/chat', 0)).map((m) => m.data.id)).toEqual([2]);
				expect((await r.since('chat', 0)).map((m) => m.data.id)).toEqual([3]);

				// The untenanted scope reaches the untenanted topic and nothing else.
				expect(await r.purgeUser(null, 'u1')).toBe(1);
				expect((await r.since('chat', 0)).map((m) => m.data.id)).toEqual([]);
				expect((await r.since('@t/globex/chat', 0)).map((m) => m.data.id)).toEqual([2]);
			} finally {
				r.destroy();
			}
		});

		it('a tenant id sharing a prefix with another does not reach it', async () => {
			const r = createReplay(client, {
				cleanupInterval: 0,
				forgetUserId: ({ data }) => data?.author
			});
			try {
				// A validated tenant id may contain `_`, which is a LIKE wildcard
				// matching any single character - so a LIKE-based scope would let
				// `a_c` reach `abc`.
				await r.publishBatch(platform, [
					{ topic: '@t/a_c/chat', event: 'created', data: { id: 1, author: 'u1' } },
					{ topic: '@t/abc/chat', event: 'created', data: { id: 2, author: 'u1' } },
					{ topic: '@t/acme/chat', event: 'created', data: { id: 3, author: 'u1' } },
					{ topic: '@t/acmecorp/chat', event: 'created', data: { id: 4, author: 'u1' } }
				]);
				expect(await r.purgeUser('a_c', 'u1')).toBe(1);
				expect((await r.since('@t/abc/chat', 0)).map((m) => m.data.id)).toEqual([2]);
				// And a tenant is not a prefix of a longer tenant's namespace.
				expect(await r.purgeUser('acme', 'u1')).toBe(1);
				expect((await r.since('@t/acmecorp/chat', 0)).map((m) => m.data.id)).toEqual([4]);
			} finally {
				r.destroy();
			}
		});
	});
});
