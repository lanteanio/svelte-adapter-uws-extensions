/**
 * Integration tests for the sorted-set -> stream replay migration and the
 * stream backend's per-entry topic-field trim, against a real Redis 7 server.
 *
 * What only a real server can verify here:
 *
 * - The cross-encoding round-trip: a sorted-set member (a JSON object string)
 *   re-emitted as a stream entry (discrete field/value pairs) reads back through
 *   the real XRANGE / JSON path identically.
 * - Real `MAXLEN ~` listpack-approximate trim when the source is larger than the
 *   target cap (the mock trims to an exact count).
 * - Cluster-aware SCAN discovery across master nodes (the mirror tier).
 * - The credo-4 measurement gate for the topic-field trim: real `MEMORY USAGE`
 *   before/after on a topic/event/data stream vs an event/data stream. The
 *   in-memory mock cannot measure listpack bytes.
 *
 * The mock suites (test/redis/replay-stream-migrate.test.js +
 * test/redis/replay-stream.test.js) stay the exhaustive behavior surface; this
 * file pins what only a real server can verify.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createReplay, migrateReplayToStream } from '../../../src/redis/replay.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

// Runs on standalone and Redis Cluster: every replay key carries a {topic}
// hash-tag, and the discovery SCAN enumerates masters, so nothing CROSSSLOTs.
const describeIntegration = describe;

describeIntegration('replay migration (sorted-set -> stream, integration)', () => {
	let client;
	let platform;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-replay-migrate:' });
	});

	beforeEach(async () => {
		await resetBackendKeys(client);
		platform = mockPlatform();
	});

	afterAll(async () => {
		await client.quit();
	});

	async function seedSortedSet(topic, count, size = 1000) {
		const ss = createReplay(client, { size });
		for (let i = 1; i <= count; i++) {
			await ss.publish(platform, topic, 'created', { id: i, body: `m${i}` });
		}
		return ss;
	}

	describe('cross-encoding round-trip', () => {
		it('a migrated stream returns the identical since() result as the sorted set', async () => {
			const ss = await seedSortedSet('chat', 5);
			const before = await ss.since('chat', 0);

			const result = await migrateReplayToStream(client, { topics: ['chat'], size: 1000 });
			expect(result.migrated).toEqual([{ topic: 'chat', entries: 5, highWaterSeq: 5 }]);

			const st = createReplay(client, { storage: 'stream', size: 1000 });
			const after = await st.since('chat', 0);
			expect(after).toEqual(before);
		});

		it('preserves the shared seq + epoch so the stream backend resumes', async () => {
			await seedSortedSet('chat', 4);
			await migrateReplayToStream(client, { topics: ['chat'] });
			const st = createReplay(client, { storage: 'stream' });
			expect(await st.seq('chat')).toBe(4);
			expect(await st.currentEpoch('chat')).toBe(1);
		});

		it('round-trips a null payload', async () => {
			const ss = createReplay(client, { size: 50 });
			await ss.publish(platform, 'chat', 'ping', null);
			await migrateReplayToStream(client, { topics: ['chat'] });
			const st = createReplay(client, { storage: 'stream' });
			expect(await st.since('chat', 0)).toEqual([
				{ seq: 1, topic: 'chat', event: 'ping', data: null }
			]);
		});
	});

	describe('real MAXLEN ~ trim when source exceeds the target cap', () => {
		it('keeps roughly the newest `size` entries (>= size, < total) with the latest seq present', async () => {
			const total = 250;
			await seedSortedSet('chat', total, total); // sorted set keeps all 250

			const result = await migrateReplayToStream(client, { topics: ['chat'], size: 5 });
			expect(result.entries).toBeUndefined(); // result is { migrated, skipped }
			expect(result.migrated[0].entries).toBe(total); // all were written...

			const st = createReplay(client, { storage: 'stream', size: 5 });
			const all = await st.since('chat', 0);
			// ...but MAXLEN ~ trimmed the stream to roughly the newest few.
			expect(all.length).toBeGreaterThanOrEqual(5);
			expect(all.length).toBeLessThan(total);
			expect(all[all.length - 1].seq).toBe(total);
		});
	});

	describe('cluster-aware SCAN discovery', () => {
		it('discovers and migrates every sorted-set topic when topics is omitted', async () => {
			await seedSortedSet('chat', 2);
			await seedSortedSet('todos', 3);

			const result = await migrateReplayToStream(client);
			const byTopic = Object.fromEntries(result.migrated.map((m) => [m.topic, m.entries]));
			expect(byTopic).toEqual({ chat: 2, todos: 3 });

			// Re-running discovers the same sources but skips them (targets exist),
			// and never re-finds the streambuf keys it just wrote.
			const again = await migrateReplayToStream(client);
			expect(again.migrated).toEqual([]);
			expect(again.skipped.map((s) => s.reason).sort()).toEqual(['target-exists', 'target-exists']);
		});
	});

	describe('topic-field trim: MEMORY USAGE before/after (credo-4 gate)', () => {
		async function memUsage(key) {
			const v = await client.redis.call('MEMORY', 'USAGE', key);
			return v == null ? null : Number(v);
		}

		it('the event/data format is never larger than topic/event/data (small events)', async () => {
			const N = 200;
			const oldKey = client.key('memcmp:old:{cursors}');
			const newKey = client.key('memcmp:new:{cursors}');
			for (let i = 1; i <= N; i++) {
				const data = JSON.stringify({ x: i, y: i });
				await client.redis.xadd(oldKey, `${i}-0`, 'topic', 'cursors', 'event', 'move', 'data', data);
				await client.redis.xadd(newKey, `${i}-0`, 'event', 'move', 'data', data);
			}
			const oldMem = await memUsage(oldKey);
			const newMem = await memUsage(newKey);
			if (oldMem == null || newMem == null) return; // MEMORY USAGE unavailable
			expect(newMem).toBeLessThanOrEqual(oldMem);
			const pct = (100 * (oldMem - newMem) / oldMem).toFixed(1);
			// Surface the measured delta so a CI run can fill the CHANGELOG number.
			console.log(`topic-field trim,small-event (N=${N}): old=${oldMem}B new=${newMem}B saved=${pct}%`);
		});

		it('the saving is negligible for large payloads (the topic string is a small fraction)', async () => {
			const N = 100;
			const big = 'x'.repeat(2000);
			const oldKey = client.key('memcmp:old:{big}');
			const newKey = client.key('memcmp:new:{big}');
			for (let i = 1; i <= N; i++) {
				const data = JSON.stringify({ i, blob: big });
				await client.redis.xadd(oldKey, `${i}-0`, 'topic', 'big', 'event', 'm', 'data', data);
				await client.redis.xadd(newKey, `${i}-0`, 'event', 'm', 'data', data);
			}
			const oldMem = await memUsage(oldKey);
			const newMem = await memUsage(newKey);
			if (oldMem == null || newMem == null) return;
			expect(newMem).toBeLessThanOrEqual(oldMem);
			const pct = (100 * (oldMem - newMem) / oldMem).toFixed(1);
			console.log(`topic-field trim,large-payload (N=${N}, 2KB): old=${oldMem}B new=${newMem}B saved=${pct}%`);
		});
	});
});
