import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createAlarmStore } from '../../src/redis/alarm-store.js';

describe('redis durable alarm store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = mockRedisClient('app:');
		store = createAlarmStore(client);
	});

	it('persists an alarm and returns it from due() once overdue', async () => {
		await store.set('room:1', 1000, { path: 'rooms/x', tenantId: 'acme' });
		expect(await store.due(999)).toEqual([]); // not yet due
		const due = await store.due(1000);
		expect(due).toEqual([{ topic: 'room:1', at: 1000, meta: { path: 'rooms/x', tenantId: 'acme' } }]);
	});

	it('delete() is the atomic claim: true once, then false', async () => {
		await store.set('room:claim', 1000, { path: 'p' });
		expect(await store.delete('room:claim')).toBe(true);
		expect(await store.delete('room:claim')).toBe(false);
		expect(await store.due(2000)).toEqual([]); // gone from the due index
	});

	it('set() replaces the pending alarm (one per topic)', async () => {
		await store.set('room:r', 1000, { path: 'p' });
		await store.set('room:r', 5000, { path: 'p2' });
		expect(await store.due(2000)).toEqual([]); // moved out to 5000
		const due = await store.due(5000);
		expect(due).toEqual([{ topic: 'room:r', at: 5000, meta: { path: 'p2' } }]);
	});

	it('due() honors the dueBatch cap', async () => {
		const s = createAlarmStore(client, { dueBatch: 2 });
		await s.set('a', 1, null);
		await s.set('b', 2, null);
		await s.set('c', 3, null);
		expect((await s.due(100)).length).toBe(2);
	});

	it('clear() removes everything', async () => {
		await store.set('x', 1, null);
		await store.set('y', 2, null);
		await store.clear();
		expect(await store.due(100)).toEqual([]);
	});

	it('surfaces a bare row when the meta is missing (so the poll can GC it)', async () => {
		// A zset member with no meta hash entry (corruption / partial external write).
		await client.redis.zadd(client.key('alarm:{alarms}:due'), 500, 'room:bare');
		expect(await store.due(1000)).toEqual([{ topic: 'room:bare', at: 1000, meta: null }]);
	});

	it('validates inputs', async () => {
		await expect(store.set('', 1)).rejects.toThrow(/non-empty/);
		await expect(store.set('t', NaN)).rejects.toThrow(/finite/);
		expect(() => createAlarmStore(client, { dueBatch: 0 })).toThrow(/positive integer/);
		expect(() => createAlarmStore(client, { keyPrefix: 5 })).toThrow(/keyPrefix/);
	});

	it('stays single-slot on a cluster: the claim and due() are atomic across nodes', async () => {
		// Without the `{alarms}` hash tag the due-index + meta keys would land on
		// different nodes, and a MULTI would silently no-op the off-node command -
		// so delete() would mis-report the claim. The tag co-locates them.
		const cclient = mockRedisClient('app:', { cluster: true, nodeCount: 3 });
		const cstore = createAlarmStore(cclient);
		await cstore.set('room:c', 1000, { path: 'p' });
		expect(await cstore.due(1000)).toEqual([{ topic: 'room:c', at: 1000, meta: { path: 'p' } }]);
		expect(await cstore.delete('room:c')).toBe(true); // ZREM + HDEL both applied on one node
		expect(await cstore.due(2000)).toEqual([]);
	});
});
