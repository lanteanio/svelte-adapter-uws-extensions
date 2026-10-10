import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createReplay, migrateReplayToStream } from '../../src/redis/replay.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setOperationalEventSink } from '../../src/shared/diagnostic.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';

const secret = 'private-user-topic-payload-error';
let records, restore;
const trackers = [];
beforeEach(() => { records = []; restore = setOperationalEventSink(record => records.push(record)); });
afterEach(() => { for (const tracker of trackers.splice(0)) tracker.destroy(); restore(); vi.restoreAllMocks(); });
const drops = () => records.filter(record => record.event.endsWith('.input-dropped') && record.attributes.stage === 'erase');
function setup(kind, options = {}) {
	const client = mockRedisClient();
	const metrics = createMetrics();
	const cursor = kind === 'cursor';
	const tracker = cursor ? createCursor(client, { metrics }) : createReplay(client, { storage: kind, metrics, forgetUserId: entry => entry.data.user, ...options });
	if (cursor) trackers.push(tracker);
	const key = topic => client.key((cursor ? 'cursor:{' : kind === 'sortedset' ? 'replay:buf:{' : 'replay:streambuf:{') + topic + '}');
	const seed = async (topic = secret) => cursor
		? client.redis.hset(key(topic), 'connection', JSON.stringify({ user: secret, ts: Date.now() }))
		: tracker.publish(mockPlatform(), topic, 'change', { user: secret });
	const corrupt = async () => cursor ? client.redis.hset(key(secret), 'bad', secret)
		: kind === 'sortedset' ? client.redis.zadd(key(secret), 2, secret)
			: client.redis.xadd(key(secret), '2-0', 'event', 'change', 'data', secret);
	const count = reason => {
		const prefix = cursor ? 'cursor' : 'replay';
		const match = metrics.serialize().split('\n').find(line => line.startsWith(prefix + '_input_dropped_total{') && line.includes('reason="' + reason + '"') && line.includes('stage="erase"'));
		return match ? Number(match.slice(match.lastIndexOf(' ') + 1)) : 0;
	};
	return { client, metrics, tracker, seed, corrupt, count };
}
const privateError = () => Object.assign(new Error(secret), { name: secret, code: secret });

for (const kind of ['cursor', 'sortedset', 'stream']) describe(kind + ' erasure observations', () => {
	const read = kind === 'cursor' ? 'hgetall' : kind === 'sortedset' ? 'zrange' : 'xrange';
	const remove = kind === 'cursor' ? 'hdel' : kind === 'sortedset' ? 'zrem' : 'xdel';
	for (const [method, reason] of [['scan', 'scan_failed'], [read, 'read_failed'], [remove, 'delete_failed']]) {
		it('reports ' + reason + ' repeatedly without identifying data, then recovers', async () => {
			const { client, tracker, seed, count } = setup(kind);
			await seed();
			const failure = vi.spyOn(client.redis, method).mockRejectedValue(privateError());
			for (let i = 0; i < 2; i++) expect(await tracker.purgeUser(null, secret)).toBe(0);
			expect(count(reason)).toBe(2);
			expect(drops()).toHaveLength(1);
			expect(drops()[0].attributes).toEqual({ reason, stage: 'erase' });
			expect(JSON.stringify(drops())).not.toContain(secret);
			failure.mockRestore();
			expect(await tracker.purgeUser(null, secret)).toBe(1);
			expect(count(reason)).toBe(2);
		});
	}
	it('distinguishes corrupt skipped entries from another user and valid empty results', async () => {
		const { tracker, seed, corrupt, count } = setup(kind);
		await seed();
		expect(await tracker.purgeUser(null, 'someone-else')).toBe(0);
		expect(drops()).toHaveLength(0);
		await corrupt();
		expect(await tracker.purgeUser(null, secret)).toBe(1);
		expect(await tracker.purgeUser(null, secret)).toBe(0);
		expect(count('corrupt_entry')).toBe(2);
		expect(drops()).toHaveLength(1);
		expect(JSON.stringify(drops())).not.toContain(secret);
	});
});
for (const storage of ['sortedset', 'stream']) it(storage + ' reports a throwing erasure extractor', async () => {
	const { tracker, seed, count } = setup(storage, { forgetUserId() { throw privateError(); } });
	await seed();
	for (let i = 0; i < 2; i++) expect(await tracker.purgeUser(null, secret)).toBe(0);
	expect(count('extractor_failed')).toBe(2);
	expect(drops()).toHaveLength(1);
	expect(JSON.stringify(drops())).not.toContain(secret);
});
it('cursor reports malformed members of an otherwise valid bulk relay', async () => {
	const { client, tracker, metrics } = setup('cursor');
	const platform = mockPlatform();
	await tracker.attach(mockWs({ id: secret }), secret, platform);
	await client.redis.publish(client.key('cursor:events'), JSON.stringify({ instanceId: 'peer', topic: secret, event: 'bulk', payload: [null, { key: 42 }, { key: 'good', data: { x: 1 } }] }));
	expect(metrics.serialize()).toContain('cursor_input_dropped_total{reason="invalid_envelope",stage="relay_receive"} 2');
	expect(JSON.stringify(records.filter(record => record.event.endsWith('.input-dropped')))).not.toContain(secret);
});

it('migration reports corrupt and non-monotonic skipped members without private context', async () => {
	const client = mockRedisClient();
	const key = client.key('replay:buf:{' + secret + '}');
	await client.redis.zadd(key, 1, JSON.stringify({ seq: 1, event: 'change', data: secret }));
	await client.redis.zadd(key, 2, secret);
	await client.redis.zadd(key, 3, JSON.stringify({ seq: 1, event: 'duplicate', data: secret }));
	const result = await migrateReplayToStream(client, { topics: [secret] });
	expect(result.migrated).toEqual([{ topic: secret, entries: 1, highWaterSeq: 1 }]);
	const observed = records.filter(record => record.event.endsWith('.input-dropped'));
	expect(observed.map(record => record.attributes)).toEqual([{ reason: 'corrupt_entry', stage: 'migration' }, { reason: 'non_monotonic', stage: 'migration' }]);
	expect(JSON.stringify(observed)).not.toContain(secret);
});
