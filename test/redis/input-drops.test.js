import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createReplay } from '../../src/redis/replay.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setOperationalEventSink } from '../../src/shared/diagnostic.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const flush = () => new Promise(resolve => setImmediate(resolve));
const secret = 'private-user-payload-and-error';
let records, restore;
const trackers = [];
beforeEach(() => { records = []; restore = setOperationalEventSink(record => records.push(record)); });
afterEach(() => {
	for (const cursor of trackers.splice(0)) cursor.destroy();
	restore();
	vi.restoreAllMocks();
});
function setup(options = {}) {
	const client = mockRedisClient();
	const metrics = createMetrics();
	const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, metrics, ...options });
	trackers.push(cursor);
	return { client, metrics, cursor, platform: mockPlatform(), ws: mockWs({ id: secret }) };
}
function drops(stage) {
	return records.filter(record => record.event.endsWith('.input-dropped') && (!stage || record.attributes.stage === stage));
}
function count(metrics, prefix, reason, stage) {
	const line = metrics.serialize().split('\n').find(line => line.startsWith(prefix + '_input_dropped_total{') && line.includes(`reason="${reason}"`) && line.includes(`stage="${stage}"`));
	return line ? Number(line.slice(line.lastIndexOf(' ') + 1)) : 0;
}
function privateError() {
	const error = new Error(secret);
	error.name = secret;
	error.code = secret;
	return error;
}

describe('bounded corruption and drop diagnostics through Redis stores', () => {
	for (const storage of ['sortedset', 'stream']) {
		it(`${storage}: counts repeated corrupt reads and emits one identifier-free diagnostic`, async () => {
			const client = mockRedisClient();
			const metrics = createMetrics();
			const replay = createReplay(client, { storage, metrics });
			await replay.publish(mockPlatform(), secret, 'healthy', { n: 1 });
			if (storage === 'sortedset') await client.redis.zadd(client.key('replay:buf:{' + secret + '}'), 2, secret);
			else await client.redis.xadd(client.key('replay:streambuf:{' + secret + '}'), '2-0', 'event', secret, 'data', secret);
			for (let i = 0; i < 3; i++) expect(await replay.since(secret, 0)).toHaveLength(1);
			expect(count(metrics, 'replay', 'corrupt_entry', 'decode')).toBe(3);
			expect(drops()).toHaveLength(1);
			expect(drops()[0].attributes).toEqual({ reason: 'corrupt_entry', stage: 'decode' });
			expect(drops()[0].dataClass).toBe('operational');
			expect(JSON.stringify(drops())).not.toContain(secret);
			await replay.gap(secret, 1);
			await replay.replay({}, secret, 0, mockPlatform());
			expect(count(metrics, 'replay', 'corrupt_entry', 'decode')).toBeGreaterThan(3);
			expect(drops()).toHaveLength(1);
		});

		it(`${storage}: emits without metrics and survives a rejecting diagnostic sink`, async () => {
			restore();
			restore = setOperationalEventSink(() => Promise.reject(privateError()));
			const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
			vi.spyOn(console, 'error').mockImplementation(() => {});
			const client = mockRedisClient();
			const replay = createReplay(client, { storage });
			if (storage === 'sortedset') await client.redis.zadd(client.key('replay:buf:{room}'), 1, secret);
			else await client.redis.xadd(client.key('replay:streambuf:{room}'), '1-0', 'event', secret, 'data', secret);
			expect(await replay.since('room', 0)).toEqual([]);
			await flush();
			expect(warn.mock.calls.some(args => args.some(arg => String(arg).includes('input-dropped')))).toBe(true);
			expect(JSON.stringify(warn.mock.calls)).not.toContain(secret);
		});
	}
	it('a throwing drop-counter observer cannot erase the diagnostic or change the replay result', async () => {
		const client = mockRedisClient();
		const metrics = { counter(name) { return { inc() { if (name === 'replay_input_dropped_total') throw privateError(); } }; } };
		const replay = createReplay(client, { metrics });
		await client.redis.zadd(client.key('replay:buf:{room}'), 1, secret);
		expect(await replay.since('room', 0)).toEqual([]);
		expect(drops()).toHaveLength(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	it('cursor relay distinguishes parse, raw and envelope drops from healthy delivery', async () => {
		const { client, cursor, platform, ws, metrics } = setup({ maxEnvelopeBytes: 256 });
		await cursor.attach(ws, 'board', platform);
		const publish = value => client.redis.publish(client.key('cursor:events'), value);
		await publish(secret); await publish(secret);
		await publish('null');
		await publish(JSON.stringify({ instanceId: 'peer', topic: '__forbidden', event: 'join', payload: secret }));
		await publish('x'.repeat(257));
		await publish(JSON.stringify({ instanceId: 'peer', topic: 'board', event: 'join', payload: { key: 'peer:1', user: 'good' } }));
		expect(count(metrics, 'cursor', 'invalid_json', 'relay_receive')).toBe(2);
		expect(count(metrics, 'cursor', 'invalid_envelope', 'relay_receive')).toBe(2);
		expect(count(metrics, 'cursor', 'invalid_raw', 'relay_receive')).toBe(1);
		expect(drops('relay_receive')).toHaveLength(3);
		expect(platform.published.some(frame => frame.event === 'join' && frame.data.user === 'good')).toBe(true);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	it('cursor snapshots count corrupt JSON and timestamps, while expiration remains normal absence', async () => {
		const { client, cursor, platform, ws, metrics } = setup();
		const key = client.key('cursor:{board}');
		await client.redis.hset(key, 'bad', secret, 'shape', JSON.stringify({ ts: secret }), 'expired', JSON.stringify({ ts: 1 }), 'good', JSON.stringify({ user: 'good', ts: Date.now() }));
		for (let i = 0; i < 2; i++) expect((await cursor.list('board')).map(entry => entry.user)).toEqual(['good']);
		expect(count(metrics, 'cursor', 'corrupt_entry', 'snapshot_read')).toBe(4);
		await cursor.attach(ws, 'board', platform); // catch-up decodes the persisted roster too
		expect(count(metrics, 'cursor', 'corrupt_entry', 'snapshot_read')).toBeGreaterThan(4);
		expect(drops('snapshot_read')).toHaveLength(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	for (const snapshotIntervalMs of [0, 10]) {
		it(`cursor snapshot writes report per-command refusal with interval ${snapshotIntervalMs}`, async () => {
			const { client, cursor, platform, ws, metrics } = setup({ snapshotIntervalMs });
			await cursor.attach(ws, 'board', platform);
			vi.spyOn(client.redis, 'hset').mockRejectedValue(privateError());
			for (let i = 0; i < 2; i++) {
				cursor.update(ws, 'board', { x: i }, platform);
				await vi.waitFor(() => expect(count(metrics, 'cursor', 'redis_failure', 'snapshot_write')).toBe(i + 1));
			}
			expect(drops('snapshot_write')).toHaveLength(1);
			expect(JSON.stringify(drops())).not.toContain(secret);
		});
	}

	it('cursor relay and remove failures retain finite diagnostics, without error name/code/message', async () => {
		const { client, cursor, platform, ws, metrics } = setup({ snapshotIntervalMs: 0 });
		await cursor.attach(ws, 'board', platform);
		vi.spyOn(client.redis, 'publish').mockRejectedValue(privateError());
		cursor.update(ws, 'board', { x: 1 }, platform);
		cursor.update(ws, 'board', { x: 2 }, platform);
		await flush();
		expect(count(metrics, 'cursor', 'redis_failure', 'relay_send')).toBeGreaterThan(1);
		expect(drops('relay_send')).toHaveLength(1);
		vi.spyOn(client.redis, 'hdel').mockRejectedValue(privateError());
		await cursor.remove(ws, platform);
		expect(count(metrics, 'cursor', 'redis_failure', 'remove')).toBe(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	it('cursor breaker skips are observable and recovery still processes later updates', async () => {
		let open = false;
		const breaker = { guard() { if (open) throw privateError(); }, success() {}, failure() {} };
		const { cursor, platform, ws, metrics } = setup({ breaker, snapshotIntervalMs: 0 });
		await cursor.attach(ws, 'board', platform);
		open = true;
		cursor.update(ws, 'board', { x: 1 }, platform);
		await expect(cursor.list('board')).rejects.toThrow(secret);
		await cursor.remove(ws, platform);
		for (const stage of ['snapshot_write', 'snapshot_read', 'relay_send', 'remove']) expect(count(metrics, 'cursor', 'breaker_open', stage)).toBeGreaterThan(0);
		open = false;
		cursor.update(ws, 'board', { x: 2 }, platform);
		await flush();
		expect((await cursor.list('board'))[0].data).toEqual({ x: 2 });
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	it('cursor catch-up and snapshot-read failures no longer look like an empty roster', async () => {
		const { client, cursor, platform, ws, metrics } = setup();
		vi.spyOn(client.redis, 'hgetall').mockRejectedValue(privateError());
		await cursor.attach(ws, 'board', platform);
		expect(count(metrics, 'cursor', 'redis_failure', 'catch_up')).toBe(1);
		expect(count(metrics, 'cursor', 'redis_failure', 'snapshot_read')).toBe(1);
		expect(drops()).toHaveLength(2);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});

	it('cursor reports queued data that becomes unserializable before its snapshot timer', async () => {
		const { cursor, platform, ws, metrics } = setup({ snapshotIntervalMs: 10 });
		await cursor.attach(ws, 'board', platform);
		const data = { x: 1 };
		cursor.update(ws, 'board', data, platform);
		data.toJSON = () => { throw privateError(); };
		await vi.waitFor(() => expect(count(metrics, 'cursor', 'serialization_failed', 'snapshot_write')).toBe(1));
		expect(drops('snapshot_write')).toHaveLength(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});
});
