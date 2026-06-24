import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createReplay, migrateReplayToStream } from '../../src/redis/replay.js';

describe('migrateReplayToStream (sorted-set -> stream)', () => {
	let client;
	let platform;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
	});

	/** Seed the sorted-set backend for `topic` with `count` published messages. */
	async function seedSortedSet(topic, count, size = 100) {
		const ss = createReplay(client, { size });
		for (let i = 1; i <= count; i++) {
			await ss.publish(platform, topic, 'created', { id: i, body: `m${i}` });
		}
		return ss;
	}

	describe('input validation', () => {
		it('rejects a missing client', async () => {
			await expect(migrateReplayToStream(null)).rejects.toThrow('redis client');
		});
		it('rejects non-string topics', async () => {
			await expect(migrateReplayToStream(client, { topics: ['ok', 42] }))
				.rejects.toThrow('topics must be an array of non-empty strings');
		});
		it('rejects a non-positive size', async () => {
			await expect(migrateReplayToStream(client, { size: 0 })).rejects.toThrow('size must be a positive integer');
		});
		it('rejects a non-boolean force / dryRun', async () => {
			await expect(migrateReplayToStream(client, { force: 'yes' })).rejects.toThrow('force must be a boolean');
			await expect(migrateReplayToStream(client, { dryRun: 1 })).rejects.toThrow('dryRun must be a boolean');
		});
	});

	describe('behavioral equivalence', () => {
		it('a migrated stream returns the identical since() result as the sorted set, seqs preserved', async () => {
			const ss = await seedSortedSet('chat', 5);
			const before = await ss.since('chat', 0);

			const result = await migrateReplayToStream(client, { topics: ['chat'], size: 100 });
			expect(result.migrated).toEqual([{ topic: 'chat', entries: 5, highWaterSeq: 5 }]);
			expect(result.skipped).toEqual([]);

			const st = createReplay(client, { storage: 'stream', size: 100 });
			const after = await st.since('chat', 0);
			expect(after).toEqual(before);
			expect(after.map((m) => m.seq)).toEqual([1, 2, 3, 4, 5]);
		});

		it('writes <seq>-0 stream IDs matching the source seqs', async () => {
			await seedSortedSet('chat', 3);
			await migrateReplayToStream(client, { topics: ['chat'] });
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
			expect(stream.map((e) => e.id)).toEqual(['1-0', '2-0', '3-0']);
		});

		it('writes the compact (event + data, no topic) format', async () => {
			await seedSortedSet('chat', 1);
			await migrateReplayToStream(client, { topics: ['chat'] });
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
			expect(stream[0].fields.map(([k]) => k)).toEqual(['event', 'data']);
		});

		it('round-trips null, nested, and array payloads', async () => {
			const ss = createReplay(client, { size: 100 });
			await ss.publish(platform, 't', 'a', null);
			await ss.publish(platform, 't', 'b', { nested: { x: [1, 2, 3] } });
			await ss.publish(platform, 't', 'c', [{ k: 'v' }, 7, false]);
			const before = await ss.since('t', 0);

			await migrateReplayToStream(client, { topics: ['t'] });
			const st = createReplay(client, { storage: 'stream' });
			expect(await st.since('t', 0)).toEqual(before);
		});
	});

	describe('counters are untouched', () => {
		it('leaves replay:seq:{topic} and replay:epoch:{topic} byte-identical', async () => {
			await seedSortedSet('chat', 4);
			const seqKey = client.key('replay:seq:{chat}');
			const epochKey = client.key('replay:epoch:{chat}');
			const seqBefore = client._store.get(seqKey);
			const epochBefore = client._store.get(epochKey);

			await migrateReplayToStream(client, { topics: ['chat'] });

			expect(client._store.get(seqKey)).toBe(seqBefore);
			expect(client._store.get(epochKey)).toBe(epochBefore);
		});

		it('the stream backend reads the SAME seq/epoch after migration (resume survives)', async () => {
			await seedSortedSet('chat', 4);
			const st = createReplay(client, { storage: 'stream' });
			await migrateReplayToStream(client, { topics: ['chat'] });
			// seq high-water and epoch are shared keys; the stream backend reads them.
			expect(await st.seq('chat')).toBe(4);
			expect(await st.currentEpoch('chat')).toBe(1);
		});

		it('never writes the source sorted set (non-destructive)', async () => {
			await seedSortedSet('chat', 3);
			const srcKey = client.key('replay:buf:{chat}');
			const srcLenBefore = client._sortedSets.get(srcKey).length;
			await migrateReplayToStream(client, { topics: ['chat'] });
			expect(client._sortedSets.get(srcKey).length).toBe(srcLenBefore);
		});
	});

	describe('idempotency + force', () => {
		it('skips an already-migrated topic on re-run (target-exists)', async () => {
			await seedSortedSet('chat', 3);
			await migrateReplayToStream(client, { topics: ['chat'] });

			const again = await migrateReplayToStream(client, { topics: ['chat'] });
			expect(again.migrated).toEqual([]);
			expect(again.skipped).toEqual([{ topic: 'chat', reason: 'target-exists' }]);
			// And the stream was not doubled.
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
			expect(stream).toHaveLength(3);
		});

		it('force re-migrates over an existing (e.g. partial) target', async () => {
			await seedSortedSet('chat', 3);
			// Pre-populate the target with a stale entry to simulate a partial run.
			await client.redis.xadd(client.key('replay:streambuf:{chat}'), '1-0', 'event', 'stale', 'data', '{}');

			const result = await migrateReplayToStream(client, { topics: ['chat'], force: true });
			expect(result.migrated).toEqual([{ topic: 'chat', entries: 3, highWaterSeq: 3 }]);

			const st = createReplay(client, { storage: 'stream' });
			const after = await st.since('chat', 0);
			expect(after.map((m) => m.seq)).toEqual([1, 2, 3]);
			expect(after[0].event).toBe('created'); // not the stale entry
		});
	});

	describe('dryRun', () => {
		it('reports the plan but writes nothing', async () => {
			await seedSortedSet('chat', 4);
			const result = await migrateReplayToStream(client, { topics: ['chat'], dryRun: true });
			expect(result.migrated).toEqual([{ topic: 'chat', entries: 4, highWaterSeq: 4 }]);
			expect(client._streams.has(client.key('replay:streambuf:{chat}'))).toBe(false);
		});

		it('does not UNLINK an existing target under dryRun + force', async () => {
			await seedSortedSet('chat', 2);
			await client.redis.xadd(client.key('replay:streambuf:{chat}'), '9-0', 'event', 'keep', 'data', '{}');
			await migrateReplayToStream(client, { topics: ['chat'], force: true, dryRun: true });
			// The pre-existing target survives a dry run.
			const stream = client._streams.get(client.key('replay:streambuf:{chat}'));
			expect(stream.map((e) => e.id)).toEqual(['9-0']);
		});
	});

	describe('skip-corrupt + degenerate members', () => {
		it('skips a corrupt (non-JSON) source member and migrates the rest', async () => {
			await seedSortedSet('chat', 3);
			// Inject a corrupt member mid-buffer.
			client._sortedSets.get(client.key('replay:buf:{chat}')).push({ score: 2.5, member: 'not-json{' });
			client._sortedSets.get(client.key('replay:buf:{chat}')).sort((a, b) => a.score - b.score);

			const result = await migrateReplayToStream(client, { topics: ['chat'] });
			expect(result.migrated).toEqual([{ topic: 'chat', entries: 3, highWaterSeq: 3 }]);

			const st = createReplay(client, { storage: 'stream' });
			expect((await st.since('chat', 0)).map((m) => m.seq)).toEqual([1, 2, 3]);
		});

		it('skips a member with a non-increasing seq rather than throwing on XADD', async () => {
			await seedSortedSet('chat', 2);
			// A duplicate seq (degenerate) - the explicit <seq>-0 XADD would reject
			// a non-increasing ID, so the migration must skip it, not abort.
			const src = client._sortedSets.get(client.key('replay:buf:{chat}'));
			src.push({ score: 2, member: JSON.stringify({ seq: 2, topic: 'chat', event: 'dup', data: { id: 2 } }) });

			const result = await migrateReplayToStream(client, { topics: ['chat'] });
			expect(result.skipped).toEqual([]);
			const st = createReplay(client, { storage: 'stream' });
			expect((await st.since('chat', 0)).map((m) => m.seq)).toEqual([1, 2]);
		});
	});

	describe('multi-topic SCAN discovery', () => {
		it('discovers and migrates every sorted-set topic when topics is omitted', async () => {
			await seedSortedSet('chat', 2);
			await seedSortedSet('todos', 3);
			await seedSortedSet('rooms', 1);

			const result = await migrateReplayToStream(client);
			const byTopic = Object.fromEntries(result.migrated.map((m) => [m.topic, m.entries]));
			expect(byTopic).toEqual({ chat: 2, todos: 3, rooms: 1 });
			expect(result.skipped).toEqual([]);
		});

		it('does not discover the stream backend\'s own keys (replay:streambuf:)', async () => {
			await seedSortedSet('chat', 1);
			await migrateReplayToStream(client, { topics: ['chat'] }); // creates replay:streambuf:{chat}

			// A second discovery run must NOT re-find the stream key as a source.
			const second = await migrateReplayToStream(client);
			// 'chat' is the only sorted-set topic; it now has a target -> skipped.
			expect(second.migrated).toEqual([]);
			expect(second.skipped).toEqual([{ topic: 'chat', reason: 'target-exists' }]);
		});

		it('migrates only the listed topics when topics is given', async () => {
			await seedSortedSet('chat', 2);
			await seedSortedSet('todos', 2);
			const result = await migrateReplayToStream(client, { topics: ['chat'] });
			expect(result.migrated.map((m) => m.topic)).toEqual(['chat']);
			expect(client._streams.has(client.key('replay:streambuf:{todos}'))).toBe(false);
		});
	});

	describe('empty source', () => {
		it('reports an explicitly-listed empty topic with zero entries', async () => {
			const result = await migrateReplayToStream(client, { topics: ['never-published'] });
			expect(result.migrated).toEqual([{ topic: 'never-published', entries: 0, highWaterSeq: 0 }]);
		});
	});
});
