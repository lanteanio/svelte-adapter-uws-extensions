/**
 * Integration test for the Redis Streams replay backend's right-to-erasure
 * (`live.forget`) purge against a real Redis 7 server.
 *
 * The stream backend stores `event` + `data` per entry and re-derives the
 * owning user at purge time: `purgeUser` SCANs `replay:streambuf:{*}`, XRANGEs
 * each stream, maps each entry through `forgetUserId({topic,event,data})`, and
 * XDELs the matches, leaving the seq space intact (the holes read as truncation
 * on resume). The double models all three commands, so the unit suite covers
 * this path too - but the XDEL it wraps in a best-effort `catch` is exactly the
 * shape a double can silently no-op, so the real server stays the oracle for
 * the deletion actually happening.
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

	it('erases only the purged tenant, and reports a non-zero count doing it', async () => {
		// The tenant rule: a scoped wire topic is `@t/<tenantId>/<topic>`, and
		// this backend applies it by parsing the topic out of the stream key.
		// It was the last of the four stores to get that rule, and the only one
		// whose deletion runs through a swallowed `catch` - so the COUNT is the
		// assertion that matters. A store that silently deleted nothing would
		// satisfy "the other tenant survived" perfectly.
		await store.publish(platform, '@t/acme/fs:board', 'msg', { user: 'carol', n: 1 });
		await store.publish(platform, '@t/acme/fs:board', 'msg', { user: 'dave', n: 2 });
		await store.publish(platform, '@t/other/fs:board', 'msg', { user: 'carol', n: 3 });
		await store.publish(platform, 'fs:untenanted', 'msg', { user: 'carol', n: 4 });

		const removed = await store.purgeUser('acme', 'carol');
		expect(removed).toBe(1); // acme only - not other, not the untenanted topic

		expect((await store.since('@t/acme/fs:board', 0)).map((m) => m.data.user)).toEqual(['dave']);
		// The other tenant's identically-named user is untouched.
		expect((await store.since('@t/other/fs:board', 0)).map((m) => m.data.n)).toEqual([3]);
		// An untenanted topic is not in the `acme` scope either.
		expect((await store.since('fs:untenanted', 0)).map((m) => m.data.n)).toEqual([4]);
	});

	it('erases the untenanted scope without reaching a tenant-scoped topic', async () => {
		// The mirror direction: `null` means the untenanted scope, not "every
		// scope". Getting this backwards is how a single erasure reached every
		// tenant's buffer in the sibling stores.
		await store.publish(platform, '@t/acme/fs:room2', 'msg', { user: 'erin', n: 1 });
		await store.publish(platform, 'fs:plain', 'msg', { user: 'erin', n: 2 });

		expect(await store.purgeUser(null, 'erin')).toBe(1);
		expect((await store.since('fs:plain', 0))).toEqual([]);
		expect((await store.since('@t/acme/fs:room2', 0)).map((m) => m.data.n)).toEqual([1]);
	});

	it('is a no-op without a forgetUserId extractor', async () => {
		const plain = createReplay(client, { storage: 'stream' });
		await plain.publish(platform, 'fs:noext', 'msg', { user: 'alice' });
		expect(await plain.purgeUser(null, 'alice')).toBe(0);
		expect((await plain.since('fs:noext', 0)).length).toBe(1);
	});
});
