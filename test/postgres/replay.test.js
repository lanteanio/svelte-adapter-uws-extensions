import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay } from '../../postgres/replay.js';

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
});
