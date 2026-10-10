import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createReplay } from '../../../src/redis/replay.js';
import { createCursor } from '../../../src/redis/cursor.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { setOperationalEventSink } from '../../../src/shared/diagnostic.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';

describe('input-drop diagnostics against Redis', () => {
	let client, records, restore;
	const trackers = [];
	beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-input-drops:' }); });
	beforeEach(async () => {
		await resetBackendKeys(client);
		records = [];
		restore = setOperationalEventSink(record => records.push(record));
	});
	afterEach(() => {
		for (const cursor of trackers.splice(0)) cursor.destroy();
		restore();
	});
	afterAll(async () => { await client.quit(); });
	const secret = 'private-topic-user-payload';
	const drops = () => records.filter(record => record.event.endsWith('.input-dropped'));
	for (const storage of ['sortedset', 'stream']) {
		it(`${storage}: stored corrupt bytes are counted while valid entries still replay`, async () => {
			const metrics = createMetrics();
			const replay = createReplay(client, { storage, metrics });
			await replay.publish(mockPlatform(), secret, 'change', { n: 1 });
			if (storage === 'sortedset') await client.redis.zadd(client.key('replay:buf:{' + secret + '}'), 2, secret);
			else await client.redis.xadd(client.key('replay:streambuf:{' + secret + '}'), '2-0', 'event', 'change', 'data', secret);
			for (let i = 0; i < 3; i++) expect(await replay.since(secret, 0)).toEqual([{ seq: 1, topic: secret, event: 'change', data: { n: 1 } }]);
			expect(metrics.serialize()).toContain('replay_input_dropped_total{reason="corrupt_entry",stage="decode"} 3');
			expect(drops()).toHaveLength(1);
			expect(JSON.stringify(drops())).not.toContain(secret);
		});
	}
	it('cursor receives malformed relay messages and detects snapshot command errors from Redis', async () => {
		const metrics = createMetrics();
		const cursor = createCursor(client, { metrics, throttle: 0, topicThrottle: 0, snapshotIntervalMs: 0 });
		trackers.push(cursor);
		const platform = mockPlatform();
		const ws = mockWs({ id: secret });
		await cursor.attach(ws, secret, platform);
		await client.redis.publish(client.key('cursor:events'), secret);
		await client.redis.publish(client.key('cursor:events'), secret);
		await vi.waitFor(() => expect(metrics.serialize()).toContain('cursor_input_dropped_total{reason="invalid_json",stage="relay_receive"} 2'));
		await client.redis.set(client.key('cursor:{' + secret + '}'), secret); // WRONGTYPE from real HSET/EXPIRE batch
		cursor.update(ws, secret, { x: 1 }, platform);
		await vi.waitFor(() => expect(metrics.serialize()).toContain('cursor_input_dropped_total{reason="redis_failure",stage="snapshot_write"} 1'));
		expect(drops()).toHaveLength(2);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});
	it('cursor reports a corrupt persisted roster without counting expired entries', async () => {
		const metrics = createMetrics();
		const cursor = createCursor(client, { metrics });
		trackers.push(cursor);
		await client.redis.hset(client.key('cursor:{' + secret + '}'), secret, secret, 'expired', JSON.stringify({ ts: 1 }));
		for (let i = 0; i < 2; i++) expect(await cursor.list(secret)).toEqual([]);
		expect(metrics.serialize()).toContain('cursor_input_dropped_total{reason="corrupt_entry",stage="snapshot_read"} 2');
		expect(drops()).toHaveLength(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});
	for (const storage of ['cursor', 'sortedset', 'stream']) {
		it(storage + ': erasure reports corrupt entries and real WRONGTYPE reads while removing valid data', async () => {
			const metrics = createMetrics();
			const cursor = storage === 'cursor';
			const tracker = cursor ? createCursor(client, { metrics }) : createReplay(client, { storage, metrics, forgetUserId: entry => entry.data.user });
			if (cursor) trackers.push(tracker);
			const prefix = cursor ? 'cursor' : 'replay';
			const key = topic => client.key((cursor ? 'cursor:{' : storage === 'sortedset' ? 'replay:buf:{' : 'replay:streambuf:{') + topic + '}');
			if (cursor) await client.redis.hset(key(secret), 'good', JSON.stringify({ user: secret, ts: Date.now() }), 'bad', secret);
			else {
				await tracker.publish(mockPlatform(), secret, 'change', { user: secret });
				if (storage === 'sortedset') await client.redis.zadd(key(secret), 2, secret);
				else await client.redis.xadd(key(secret), '2-0', 'event', 'change', 'data', secret);
			}
			await client.redis.set(key('wrongtype'), secret);
			expect(await tracker.purgeUser(null, secret)).toBe(1);
			expect(await tracker.purgeUser(null, secret)).toBe(0);
			for (const reason of ['corrupt_entry', 'read_failed']) expect(metrics.serialize()).toContain(prefix + '_input_dropped_total{reason="' + reason + '",stage="erase"} 2');
			expect(drops()).toHaveLength(2);
			expect(JSON.stringify(drops())).not.toContain(secret);
		});
	}

});
