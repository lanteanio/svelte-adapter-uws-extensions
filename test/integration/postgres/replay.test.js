/**
 * Integration tests for postgres/replay against a real Postgres 16 server.
 *
 * Exercises the actual CTE atomicity (INSERT INTO svti_replay_seq ON CONFLICT
 * DO UPDATE chained to INSERT INTO svti_replay) that the in-memory mock can
 * only approximate. Concurrent publishes must produce a contiguous,
 * gap-free sequence per topic. The mock-based suite stays at
 * test/postgres/replay.test.js; this file is additive and uses a unique
 * table prefix per test so files do not collide with each other.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createReplay } from '../../../postgres/replay.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

describe('postgres replay (integration)', () => {
	let client;
	let platform;
	let replay;
	const TABLE = 'svti_replay_inttest';

	beforeAll(() => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Drop and recreate per test so DDL stays the source of truth and
		// no row from a previous test can leak in.
		await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
		await client.query(`DROP TABLE IF EXISTS ${TABLE}_seq`);
		platform = mockPlatform();
		replay = createReplay(client, { table: TABLE, size: 5, cleanupInterval: 0 });
	});

	afterEach(() => {
		if (replay) replay.destroy();
	});

	afterAll(async () => {
		await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
		await client.query(`DROP TABLE IF EXISTS ${TABLE}_seq`);
		await client.end();
	});

	describe('concurrent first-use (CREATE TABLE race-safety)', () => {
		it('two fresh instances racing their first publish both succeed', async () => {
			// CREATE TABLE IF NOT EXISTS races on concurrent first calls: both
			// connections pass the existence check, both run the create, the
			// loser raises 23505 / 42P07 / 42710. ensureTable() catches those
			// codes and treats the table as already-existing.
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);
			await client.query(`DROP TABLE IF EXISTS ${TABLE}_seq`);

			const a = createReplay(client, { table: TABLE, size: 5, cleanupInterval: 0 });
			const b = createReplay(client, { table: TABLE, size: 5, cleanupInterval: 0 });

			await Promise.all([
				a.publish(platform, 'race', 'first', { id: 1 }),
				b.publish(platform, 'race', 'first', { id: 2 })
			]);

			expect(await a.seq('race')).toBe(2);
			expect((await a.since('race', 0)).map((m) => m.data.id).sort()).toEqual([1, 2]);

			a.destroy();
			b.destroy();
		});
	});

	describe('publish + CTE envelope', () => {
		it('increments seq independently per topic and broadcasts', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'chat', 'created', { id: 2 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			expect(await replay.seq('chat')).toBe(2);
			expect(await replay.seq('todos')).toBe(1);
			expect(platform.published).toHaveLength(3);
		});

		it('round-trips jsonb data through since() with the right shape', async () => {
			const payload = { id: 42, body: 'hi', nested: { tags: ['a', 'b'] } };
			await replay.publish(platform, 'chat', 'created', payload);
			const all = await replay.since('chat', 0);
			expect(all).toEqual([
				{ seq: 1, topic: 'chat', event: 'created', data: payload }
			]);
		});

		it('persists null data as JSON null (not SQL NULL)', async () => {
			await replay.publish(platform, 'chat', 'ping', null);
			const all = await replay.since('chat', 0);
			expect(all[0]).toEqual({ seq: 1, topic: 'chat', event: 'ping', data: null });
		});

		it('round-trips arrays, booleans, numbers, and unicode strings', async () => {
			const payload = {
				bools: [true, false],
				nums: [0, -1, 1.5, 1e10],
				str: 'hello \u00e9 \u4e2d\u6587',
				nested: { a: { b: { c: null } } }
			};
			await replay.publish(platform, 't', 'msg', payload);
			const all = await replay.since('t', 0);
			expect(all[0].data).toEqual(payload);
		});
	});

	describe('atomic CTE under concurrent publishes', () => {
		it('20 concurrent publishes to one topic produce contiguous seqs 1..20', async () => {
			const r = createReplay(client, { table: TABLE, size: 100, cleanupInterval: 0 });
			try {
				await Promise.all(
					Array.from({ length: 20 }, (_, i) =>
						r.publish(platform, 'chat', 'msg', { i })
					)
				);

				const all = await r.since('chat', 0);
				const seqs = all.map((m) => m.seq).sort((a, b) => a - b);
				expect(seqs).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
				expect(await r.seq('chat')).toBe(20);
			} finally {
				r.destroy();
			}
		});

		it('two replay instances racing on one topic still produce unique strictly-monotonic seqs', async () => {
			const r1 = createReplay(client, { table: TABLE, size: 100, cleanupInterval: 0 });
			const r2 = createReplay(client, { table: TABLE, size: 100, cleanupInterval: 0 });
			try {
				const ops = [];
				for (let i = 0; i < 10; i++) {
					ops.push(r1.publish(platform, 'chat', 'msg', { from: 'r1', i }));
					ops.push(r2.publish(platform, 'chat', 'msg', { from: 'r2', i }));
				}
				await Promise.all(ops);

				const all = await r1.since('chat', 0);
				const seqs = all.map((m) => m.seq).sort((a, b) => a - b);
				expect(seqs).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
				expect(new Set(seqs).size).toBe(20);
			} finally {
				r1.destroy();
				r2.destroy();
			}
		});

		it('concurrent publishes to disjoint topics keep their seqs isolated', async () => {
			const r = createReplay(client, { table: TABLE, size: 100, cleanupInterval: 0 });
			try {
				const ops = [];
				for (let i = 0; i < 10; i++) {
					ops.push(r.publish(platform, 'a', 'msg', { i }));
					ops.push(r.publish(platform, 'b', 'msg', { i }));
					ops.push(r.publish(platform, 'c', 'msg', { i }));
				}
				await Promise.all(ops);

				expect(await r.seq('a')).toBe(10);
				expect(await r.seq('b')).toBe(10);
				expect(await r.seq('c')).toBe(10);

				for (const topic of ['a', 'b', 'c']) {
					const seqs = (await r.since(topic, 0)).map((m) => m.seq).sort((x, y) => x - y);
					expect(seqs).toEqual(Array.from({ length: 10 }, (_, i) => i + 1));
				}
			} finally {
				r.destroy();
			}
		});
	});

	describe('seq-based trim', () => {
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

		it('trim is per-topic; other topics are untouched', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			expect((await replay.since('chat', 0)).length).toBe(5);
			expect((await replay.since('todos', 0)).length).toBe(1);
		});
	});

	describe('OFFSET-based periodic cleanup', () => {
		it('cleanupInterval trim trims every topic to maxSize', async () => {
			// Stand up an instance that does NOT trim on publish (size huge)
			// but a second that runs the OFFSET cleanup with a small maxSize.
			const r = createReplay(client, { table: TABLE, size: 1000, cleanupInterval: 0 });
			try {
				for (let i = 1; i <= 8; i++) {
					await r.publish(platform, 'chat', 'msg', { i });
					await r.publish(platform, 'todos', 'msg', { i });
				}
				expect((await r.since('chat', 0)).length).toBe(8);
				expect((await r.since('todos', 0)).length).toBe(8);
			} finally {
				r.destroy();
			}

			// Now manually invoke the cleanup query the periodic timer runs.
			// The OFFSET clause keeps newest `size` and deletes the rest.
			await client.query(`
				DELETE FROM ${TABLE} r
				 USING (
				   SELECT sub.topic,
				          (SELECT seq FROM ${TABLE}
				            WHERE topic = sub.topic
				            ORDER BY seq DESC
				            OFFSET $1
				            LIMIT 1) AS cutoff_seq
				     FROM (SELECT DISTINCT topic FROM ${TABLE}) sub
				 ) cutoffs
				 WHERE r.topic = cutoffs.topic
				   AND cutoffs.cutoff_seq IS NOT NULL
				   AND r.seq <= cutoffs.cutoff_seq
			`, [3]);

			const r2 = createReplay(client, { table: TABLE, size: 1000, cleanupInterval: 0 });
			try {
				const chat = await r2.since('chat', 0);
				const todos = await r2.since('todos', 0);
				expect(chat.map((m) => m.seq)).toEqual([6, 7, 8]);
				expect(todos.map((m) => m.seq)).toEqual([6, 7, 8]);
				// Seq counter must NOT have been reset by cleanup
				expect(await r2.seq('chat')).toBe(8);
				expect(await r2.seq('todos')).toBe(8);
			} finally {
				r2.destroy();
			}
		});

		it('TTL cleanup deletes rows older than ttl seconds based on created_at', async () => {
			await replay.publish(platform, 'chat', 'msg', { id: 1 });
			await replay.publish(platform, 'chat', 'msg', { id: 2 });

			// Push the rows back in time so the TTL filter catches them.
			await client.query(`UPDATE ${TABLE} SET created_at = now() - interval '10 seconds'`);

			await client.query(
				`DELETE FROM ${TABLE} WHERE created_at < now() - interval '1 second' * $1`,
				[5]
			);

			const all = await replay.since('chat', 0);
			expect(all).toHaveLength(0);
		});
	});

	describe('gap probe via index seek', () => {
		it('returns clean when buffer still has the next entry', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 1)).toEqual({ truncated: false, missingFrom: null });
			expect(await replay.gap('chat', 2)).toEqual({ truncated: false, missingFrom: null });
		});

		it('reports truncated when buffer rolled past lastSeenSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('detects truncation when buffer is empty but seq has advanced', async () => {
			for (let i = 1; i <= 3; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			await client.query(`DELETE FROM ${TABLE} WHERE topic = $1`, ['chat']);

			expect(await replay.gap('chat', 1)).toEqual({ truncated: true, missingFrom: 2 });
		});

		it('fresh client (lastSeenSeq=0) is never truncated', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			expect(await replay.gap('chat', 0)).toEqual({ truncated: false, missingFrom: null });
		});

		it('returns clean for an unknown topic', async () => {
			expect(await replay.gap('nonexistent', 5)).toEqual({ truncated: false, missingFrom: null });
		});
	});

	describe('replay flow', () => {
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

		it('sends truncated marker when buffer was trimmed past sinceSeq', async () => {
			for (let i = 1; i <= 7; i++) {
				await replay.publish(platform, 'chat', 'created', { id: i });
			}
			const fakeWs = {};
			platform.reset();
			await replay.replay(fakeWs, 'chat', 1, platform);

			const truncated = platform.sent.filter((s) => s.event === 'truncated');
			expect(truncated).toHaveLength(1);
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

		it('clear wipes everything in both tables', async () => {
			await replay.publish(platform, 'chat', 'created', { id: 1 });
			await replay.publish(platform, 'todos', 'created', { id: 1 });

			await replay.clear();

			expect(await replay.seq('chat')).toBe(0);
			expect(await replay.seq('todos')).toBe(0);
			expect(await replay.since('chat', 0)).toEqual([]);
		});
	});
});
