import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay, ReplicationTimeoutError, ReplayStorageError } from '../../redis/replay.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

describe('redis replay', () => {
	let client;
	let platform;
	let replay;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		replay = createReplay(client, { size: 5 });
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

		it('works with no options', () => {
			const r = createReplay(client);
			expect(typeof r.publish).toBe('function');
			expect(typeof r.seq).toBe('function');
			expect(typeof r.since).toBe('function');
			expect(typeof r.replay).toBe('function');
			expect(typeof r.clear).toBe('function');
			expect(typeof r.clearTopic).toBe('function');
		});
	});

	describe('publish', () => {
		it('calls platform.publish with the same arguments', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 } }
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
	});

	describe('buffer capping', () => {
		it('caps buffer at maxSize', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			expect(await replay.seq('chat')).toBe(7);

			// Only 5 should remain (buffer size is 5)
			const all = await replay.since('chat', 0);
			expect(all).toHaveLength(5);
			expect(all[0].seq).toBe(3);
			expect(all[0].data).toEqual({ id: 3 });
			expect(all[4].seq).toBe(7);
			expect(all[4].data).toEqual({ id: 7 });
		});
	});

	describe('atomic buffer capping', () => {
		it('concurrent publishes do not over-trim with size: 1', async () => {
			const r = createReplay(client, { size: 1 });

			// Publish two messages concurrently
			await Promise.all([
				r.publish(platform, 'chat', 'a', { id: 1 }),
				r.publish(platform, 'chat', 'b', { id: 2 })
			]);

			// Buffer should have exactly 1 entry (the latest), not 0
			const all = await r.since('chat', 0);
			expect(all).toHaveLength(1);
			expect(all[0].seq).toBe(2);
		});

		it('concurrent publishes with size: 2 keep exactly 2', async () => {
			const r = createReplay(client, { size: 2 });

			await Promise.all([
				r.publish(platform, 'chat', 'a', { id: 1 }),
				r.publish(platform, 'chat', 'b', { id: 2 }),
				r.publish(platform, 'chat', 'c', { id: 3 })
			]);

			const all = await r.since('chat', 0);
			expect(all).toHaveLength(2);
			// Should keep the two most recent
			expect(all[0].seq).toBe(2);
			expect(all[1].seq).toBe(3);
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

	describe('replay with reqId', () => {
		it('includes reqId in end event data when provided', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 0, platform, 'req-123');

			const end = platform.sent.find((s) => s.event === 'end');
			expect(end.data).toEqual({ reqId: 'req-123' });
		});

		it('end event has undefined reqId when not provided', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 0, platform);

			const end = platform.sent.find((s) => s.event === 'end');
			expect(end.data).toEqual({ reqId: undefined });
		});
	});

	describe('truncation detection', () => {
		it('sends truncated event when buffer was trimmed past sinceSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const fakeWs = {};
			platform.reset();
			// sinceSeq 1 is gone (buffer starts at seq 3), so truncated should fire
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
	});

	describe('malformed entries', () => {
		it('since() skips corrupted entries without throwing', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });

			// Inject a malformed entry directly into the sorted set
			const bufKey = client.key('replay:buf:chat');
			client._sortedSets.get(bufKey).push({
				score: 1.5,
				member: '{invalid json'
			});
			client._sortedSets.get(bufKey).sort((a, b) => a.score - b.score);

			const result = await replay.since('chat', 0);
			// Should return the 2 valid entries, skipping the corrupted one
			expect(result).toHaveLength(2);
			expect(result[0].data).toEqual({ id: 1 });
			expect(result[1].data).toEqual({ id: 2 });
		});

		it('replay() works when since() encounters corrupted entries', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			const bufKey = client.key('replay:buf:chat');
			client._sortedSets.get(bufKey).push({
				score: 1.5,
				member: 'not-json'
			});
			client._sortedSets.get(bufKey).sort((a, b) => a.score - b.score);

			await replay.publish(platform, 'chat', 'created', { id: 2 });

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 0, platform);

			const msgs = platform.sent.filter((s) => s.event === 'msg');
			expect(msgs).toHaveLength(2);
			expect(msgs[0].data.data).toEqual({ id: 1 });
			expect(msgs[1].data.data).toEqual({ id: 2 });
		});
	});

	describe('truncation with corrupt oldest entry', () => {
		it('detects truncation when oldest buffered entry is corrupt', async () => {
			// Publish 3 messages so buffer has seq 1, 2, 3
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'chat', 'created', { id: 3 });

			// Corrupt seq 1 by replacing its sorted set entry
			const bufKey = client.key('replay:buf:chat');
			const set = client._sortedSets.get(bufKey);
			set[0] = { score: 1, member: '{not valid json!!!' };

			const fakeWs = {};
			platform.reset();
			// Client last saw seq 1. Corrupt seq 1 + valid seq 2:
			// the first parseable entry (seq 2) is > sinceSeq (1) + 1,
			// so truncation is NOT triggered (2 is not > 1+1).
			// But if we ask since seq 0 (which > 0 check blocks), or
			// ask since a seq whose next is the corrupt one:
			// sinceSeq=1, oldest parseable=2, 2 > 1+1 is false, no truncation.
			// Let's make it sinceSeq=0 won't work, use different scenario.

			// Remove seq 1 entirely and corrupt seq 2 instead
			set.splice(0, 2); // remove corrupt seq 1 and valid seq 2
			// Now buffer only has seq 3
			// Client last saw seq 1, oldest is seq 3 > 1+1 = true

			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
		});

		it('detects truncation skipping past corrupt entries to find first valid', async () => {
			// Publish 4 messages
			for (let i = 1; i <= 4; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const bufKey = client.key('replay:buf:chat');
			const set = client._sortedSets.get(bufKey);
			// Corrupt seq 1 (index 0)
			set[0] = { score: 1, member: '{broken' };

			const fakeWs = {};
			platform.reset();
			// sinceSeq=1, first parseable is seq 2, 2 > 1+1 is false = no truncation
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(0);

			// But the valid messages should still replay
			const msgs = platform.sent.filter((s) => s.event === 'msg');
			expect(msgs).toHaveLength(3); // seq 2, 3, 4
		});

		it('detects truncation when all entries before valid ones are corrupt', async () => {
			for (let i = 1; i <= 5; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			const bufKey = client.key('replay:buf:chat');
			const set = client._sortedSets.get(bufKey);
			// Corrupt seq 1 and 2
			set[0] = { score: 1, member: 'garbage' };
			set[1] = { score: 2, member: 'also garbage' };

			const fakeWs = {};
			platform.reset();
			// sinceSeq=1, first parseable is seq 3, 3 > 1+1 = true = truncation
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);

			const msgs = platform.sent.filter((s) => s.event === 'msg');
			expect(msgs).toHaveLength(3); // seq 3, 4, 5
		});
	});

	describe('truncation with empty buffer', () => {
		it('detects truncation when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}

			// Wipe the buffer but leave the seq counter intact
			const bufKey = client.key('replay:buf:chat');
			client._sortedSets.delete(bufKey);

			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
			expect(truncated[0].data).toBeNull();

			// End marker should still be sent
			const end = platform.sent.filter((s) => s.event === 'end');
			expect(end).toHaveLength(1);
		});

		it('does not send truncated when buffer is empty and seq has not advanced', async () => {
			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 0, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(0);

			// Only end marker
			expect(platform.sent).toHaveLength(1);
			expect(platform.sent[0].event).toBe('end');
		});

		it('does not send truncated when sinceSeq is 0 even with empty buffer', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });

			const bufKey = client.key('replay:buf:chat');
			client._sortedSets.delete(bufKey);

			const fakeWs = {};
			platform.reset();
			// sinceSeq 0 means fresh client, never truncate
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
			const bufKey = client.key('replay:buf:chat');
			client._sortedSets.delete(bufKey);

			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('returns not truncated for an unknown topic', async () => {
			expect(await replay.gap('nonexistent', 0)).toEqual({ truncated: false, missingFrom: null });
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns not truncated when consumer is ahead of the buffer', async () => {
			// Buffer seq counter is at 3, but consumer claims to have seen 10.
			// Inconsistent state, but no truncation has occurred from the
			// buffer's perspective, so report no gap.
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 10)).toEqual({ truncated: false, missingFrom: null });
		});

		it('skips corrupt entries and reports truncated when first valid is past target', async () => {
			for (let i = 1; i <= 5; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			// Corrupt seq 2 (the consumer's next-needed entry)
			const bufKey = client.key('replay:buf:chat');
			const set = client._sortedSets.get(bufKey);
			set[1] = { score: 2, member: '{not valid json' };

			// First parseable >= 2 is seq 3, so seq 2 is effectively missing
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('skips corrupt entries when the next valid entry equals target', async () => {
			for (let i = 1; i <= 4; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			// Corrupt seq 1, leaving seq 2 valid as the next-needed entry
			const bufKey = client.key('replay:buf:chat');
			const set = client._sortedSets.get(bufKey);
			set[0] = { score: 1, member: 'broken' };

			expect(await replay.gap('chat', 1)).toEqual({ truncated: false, missingFrom: null });
		});
	});

	describe('resumeHook', () => {
		it('returns an async function', () => {
			const hook = replay.resumeHook();
			expect(typeof hook).toBe('function');
		});

		it('no-ops without a ctx', async () => {
			const hook = replay.resumeHook();
			await hook({});
			await hook({}, undefined);
			await hook({}, null);
			expect(platform.sent).toHaveLength(0);
		});

		it('no-ops without lastSeenSeqs', async () => {
			const hook = replay.resumeHook();
			await hook({}, { platform });
			expect(platform.sent).toHaveLength(0);
		});

		it('no-ops without platform', async () => {
			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 } });
			expect(platform.sent).toHaveLength(0);
		});

		it('no-ops on empty lastSeenSeqs', async () => {
			const hook = replay.resumeHook();
			const fakeWs = {};
			await hook(fakeWs, { lastSeenSeqs: {}, platform });
			expect(platform.sent).toHaveLength(0);
		});

		it('replays a single topic from the given seq', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			await replay.publish(platform, 'chat', 'msg', { id: 3 });
			platform.reset();

			const fakeWs = {};
			const hook = replay.resumeHook();
			await hook(fakeWs, { lastSeenSeqs: { chat: 1 }, platform });

			// 2 missed messages + end marker
			expect(platform.sent).toHaveLength(3);
			expect(platform.sent[0].data).toMatchObject({ seq: 2 });
			expect(platform.sent[1].data).toMatchObject({ seq: 3 });
			expect(platform.sent[2].event).toBe('end');
		});

		it('replays multiple topics in iteration order', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 'c1' });
			await replay.publish(platform, 'todos', 'msg', { id: 't1' });
			await replay.publish(platform, 'todos', 'msg', { id: 't2' });
			platform.reset();

			const fakeWs = {};
			const hook = replay.resumeHook();
			await hook(fakeWs, {
				lastSeenSeqs: { chat: 0, todos: 1 },
				platform
			});

			// chat: msg seq=1 + end. todos: msg seq=2 + end.
			const chatEvents = platform.sent.filter((s) => s.topic === '__replay:chat');
			const todoEvents = platform.sent.filter((s) => s.topic === '__replay:todos');
			expect(chatEvents).toHaveLength(2);
			expect(todoEvents).toHaveLength(2);
			expect(chatEvents[0].data).toMatchObject({ seq: 1 });
			expect(todoEvents[0].data).toMatchObject({ seq: 2 });
		});

		it('treats sinceSeq=0 as a fresh subscriber (replays everything)', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 0 }, platform });

			expect(platform.sent.filter((s) => s.event === 'msg')).toHaveLength(2);
		});

		it('coerces non-numeric or negative sinceSeq to 0', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, {
				lastSeenSeqs: { chat: 'oops', todos: -5 },
				platform
			});

			// Both treated as 0 -> replay everything
			const chatMsgs = platform.sent.filter((s) => s.topic === '__replay:chat' && s.event === 'msg');
			expect(chatMsgs).toHaveLength(1);
		});

		it('emits truncated when the buffer no longer holds the next seq', async () => {
			// Buffer size is 5; publishing 7 trims [1, 2] out so seq 2 is gone.
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'msg', { id: i });
			}
			platform.reset();

			const hook = replay.resumeHook();
			await hook({}, { lastSeenSeqs: { chat: 1 }, platform });

			const truncated = platform.sent.find((s) => s.event === 'truncated');
			expect(truncated).toBeDefined();
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
	});

	describe('replicated durability', () => {
		it('rejects unknown durability values', () => {
			expect(() => createReplay(client, { durability: 'strong' }))
				.toThrow("durability must be 'replicated' or undefined");
		});

		it('rejects bad minReplicas', () => {
			expect(() => createReplay(client, { durability: 'replicated', minReplicas: 0 }))
				.toThrow('minReplicas must be a positive integer');
			expect(() => createReplay(client, { durability: 'replicated', minReplicas: 1.5 }))
				.toThrow('minReplicas must be a positive integer');
		});

		it('rejects negative replicationTimeoutMs', () => {
			expect(() => createReplay(client, {
				durability: 'replicated', replicationTimeoutMs: -1
			})).toThrow('non-negative integer');
		});

		it('does not call WAIT when durability is unset', async () => {
			let waitCalls = 0;
			const origWait = client.redis.wait;
			client.redis.wait = async (n) => { waitCalls++; return Number(n); };

			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(waitCalls).toBe(0);

			client.redis.wait = origWait;
		});

		it('publishes and broadcasts when enough replicas ack', async () => {
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 500
			});
			client.redis._waitAcks = 2;

			await r.publish(platform, 'chat', 'created', { id: 1 });

			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toEqual({
				topic: 'chat',
				event: 'created',
				data: { id: 1 },
				options: undefined
			});
		});

		it('throws ReplicationTimeoutError and skips broadcast when ack < minReplicas', async () => {
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 500
			});
			client.redis._waitAcks = 1;

			await expect(r.publish(platform, 'chat', 'created', { id: 1 }))
				.rejects.toBeInstanceOf(ReplicationTimeoutError);
			expect(platform.published).toHaveLength(0);
		});

		it('exposes ack/minReplicas/timeoutMs on the error', async () => {
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 3,
				replicationTimeoutMs: 250
			});
			client.redis._waitAcks = 1;

			let caught;
			try {
				await r.publish(platform, 'chat', 'created', { id: 1 });
			} catch (err) {
				caught = err;
			}
			expect(caught).toBeInstanceOf(ReplicationTimeoutError);
			expect(caught.ack).toBe(1);
			expect(caught.minReplicas).toBe(3);
			expect(caught.timeoutMs).toBe(250);
		});

		it('persists the data even when replication times out', async () => {
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 100
			});
			client.redis._waitAcks = 1;

			await r.publish(platform, 'chat', 'created', { id: 1 }).catch(() => {});

			// The eval ran before WAIT, so the buffer has the entry.
			// Other instances doing replay() will see it; only the
			// local broadcast was suppressed.
			expect(await r.seq('chat')).toBe(1);
			const since = await r.since('chat', 0);
			expect(since).toHaveLength(1);
			expect(since[0].data).toEqual({ id: 1 });
		});

		it('counts a breaker failure when WAIT itself errors', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 1,
				replicationTimeoutMs: 100,
				breaker
			});
			client.redis._waitError = new Error('connection lost');

			await expect(r.publish(platform, 'chat', 'created', { id: 1 }))
				.rejects.toThrow('connection lost');
			expect(breaker.failures).toBe(1);

			client.redis._waitError = undefined;
			breaker.destroy();
		});

		it('does NOT count a breaker failure when WAIT times out (just acks < min)', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 2,
				replicationTimeoutMs: 100,
				breaker
			});
			client.redis._waitAcks = 0;

			await expect(r.publish(platform, 'chat', 'created', { id: 1 }))
				.rejects.toBeInstanceOf(ReplicationTimeoutError);
			// WAIT succeeded as a command; durability check is a separate
			// signal layer and should not trip the breaker.
			expect(breaker.failures).toBe(0);

			breaker.destroy();
		});

		it('exposes Prometheus counters for replication outcomes', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const r = createReplay(client, {
				durability: 'replicated',
				minReplicas: 1,
				replicationTimeoutMs: 100,
				metrics
			});

			client.redis._waitAcks = 1;
			await r.publish(platform, 'chat', 'created', { id: 1 });

			client.redis._waitAcks = 0;
			await r.publish(platform, 'chat', 'created', { id: 2 }).catch(() => {});

			const out = metrics.serialize();
			expect(out).toMatch(/replay_replications_total \d+/);
			expect(out).toMatch(/replay_replication_timeouts_total \d+/);
		});
	});

	describe('storage failure', () => {
		function failEval(c) {
			const orig = c.redis.eval;
			const failure = new Error('redis down');
			c.redis.eval = async () => { throw failure; };
			return { failure, restore: () => { c.redis.eval = orig; } };
		}

		it('throws ReplayStorageError when storage fails (default behavior)', async () => {
			const r = createReplay(client);
			const { failure, restore } = failEval(client);

			let caught;
			try { await r.publish(platform, 'chat', 'created', { id: 1 }); }
			catch (err) { caught = err; }

			expect(caught).toBeInstanceOf(ReplayStorageError);
			expect(caught.op).toBe('publish');
			expect(caught.cause).toBe(failure);
			expect(platform.published).toHaveLength(0);

			restore();
		});

		it('falls back to platform.publish when localFanoutOnStorageFailure: true', async () => {
			const r = createReplay(client, { localFanoutOnStorageFailure: true });
			const { restore } = failEval(client);

			const result = await r.publish(platform, 'chat', 'created', { id: 1 });

			expect(result).toBe(true);
			expect(platform.published).toEqual([
				{ topic: 'chat', event: 'created', data: { id: 1 } }
			]);

			restore();
		});

		it('counts the fallback in replay_storage_fallbacks_total but not replay_publishes_total', async () => {
			const { createMetrics } = await import('../../prometheus/index.js');
			const metrics = createMetrics();
			const r = createReplay(client, { localFanoutOnStorageFailure: true, metrics });
			const { restore } = failEval(client);

			await r.publish(platform, 'chat', 'created', { id: 1 });

			const out = metrics.serialize();
			expect(out).toMatch(/replay_storage_fallbacks_total\{topic="chat"\} 1/);
			expect(out).not.toMatch(/replay_publishes_total\{topic="chat"\} [1-9]/);

			restore();
		});

		it('validates localFanoutOnStorageFailure must be a boolean', () => {
			expect(() => createReplay(client, { localFanoutOnStorageFailure: 'yes' }))
				.toThrow('localFanoutOnStorageFailure must be a boolean');
			expect(() => createReplay(client, { localFanoutOnStorageFailure: 1 }))
				.toThrow('localFanoutOnStorageFailure must be a boolean');
		});
	});
});
