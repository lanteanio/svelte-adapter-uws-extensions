/**
 * Integration test for the Redis Streams replay backend's right-to-erasure
 * (`live.forget`) purge against a real Redis 7 server.
 *
 * The stream backend stores `event` + `data` per entry and re-derives the
 * owning user at purge time: `purgeUser` SCANs `replay:streambuf:{*}`, XRANGEs
 * each stream, maps each entry through `forgetUserId({topic,event,data})`, and
 * XDELs the matches, leaving the seq space intact (the holes read as truncation
 * on resume). The in-memory mock cannot exercise real SCAN + XRANGE + XDEL, so
 * the actual deletion - and that only the target user's entries go, across
 * multiple topic streams - is proven only here.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createBackendClient } from '../helpers/backend.js';
import { createReplay } from '../../../src/redis/replay.js';

const platform = { publish() {} };

describe('redis stream replay right-to-erasure (purgeUser, integration)', () => {
	let client;
	let store;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-forget-stream:' });
		store = createReplay(client, { storage: 'stream', forgetUserId: ({ data }) => data && data.user });
	});

	afterAll(async () => { if (client) await client.quit?.(); });

	it('XDELs only the target user entries across topic streams; survivors and seq holes remain', async () => {
		// Two topics, interleaved users. Distinct topics per test keep the
		// throwaway Redis isolated without a flush.
		await store.publish(platform, 'fs:room', 'msg', { user: 'alice', n: 1 });
		await store.publish(platform, 'fs:room', 'msg', { user: 'bob', n: 2 });
		await store.publish(platform, 'fs:room', 'msg', { user: 'alice', n: 3 });
		await store.publish(platform, 'fs:lobby', 'msg', { user: 'alice', n: 4 });
		await store.publish(platform, 'fs:lobby', 'msg', { user: 'bob', n: 5 });

		// 2 alice in fs:room + 1 alice in fs:lobby = 3 removed.
		expect(await store.purgeUser(null, 'alice')).toBe(3);

		// fs:room keeps only bob (seq 2); the seq counter is untouched so the
		// alice holes (1, 3) read as truncation on resume.
		const room = await store.since('fs:room', 0);
		expect(room.map((m) => m.data.user)).toEqual(['bob']);
		expect(room.map((m) => m.seq)).toEqual([2]);

		// fs:lobby keeps only bob (seq 5).
		const lobby = await store.since('fs:lobby', 0);
		expect(lobby.map((m) => m.data.user)).toEqual(['bob']);

		// The seq counter is NOT rewound by the XDEL, so a purged seq reads as a
		// gap on resume: a client that last saw bob (seq 2) and expects seq 3 (the
		// purged alice entry) is told truncated -> full rehydrate, the safe outcome.
		expect(await store.gap('fs:room', 2)).toEqual({ truncated: true, missingFrom: 3 });

		// Idempotent: a second purge of the same user removes nothing.
		expect(await store.purgeUser(null, 'alice')).toBe(0);
	});

	it('is a no-op without a forgetUserId extractor', async () => {
		const plain = createReplay(client, { storage: 'stream' });
		await plain.publish(platform, 'fs:noext', 'msg', { user: 'alice' });
		expect(await plain.purgeUser(null, 'alice')).toBe(0);
		expect((await plain.since('fs:noext', 0)).length).toBe(1);
	});
});
