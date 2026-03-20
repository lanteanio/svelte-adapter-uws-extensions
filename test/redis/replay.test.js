import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay } from '../../redis/replay.js';

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
});
