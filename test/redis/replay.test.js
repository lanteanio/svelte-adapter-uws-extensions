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
				data: null
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
