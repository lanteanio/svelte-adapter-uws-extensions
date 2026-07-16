/**
 * Integration test for createForgetStore's cluster-wide room-owner force-evict
 * and presence-roster erasure against a real Redis 7 server.
 *
 * The realtime layer writes `__live-room-owner:<topic>` and
 * `__live-presence:<topic>` hashes RAW through `platform.redis` (no store
 * prefix), so this suite seeds them the same way - standing in for rooms whose
 * members joined through OTHER instances - and proves the real SCAN
 * enumeration, the real Lua eviction (atomic HDEL + in-script ownership check
 * + successor pick), and the real EXPIRE refresh that the in-memory mock can
 * only approximate. Runs against standalone and, via the mirror config, a real
 * Redis Cluster (the eviction script is single-key, so no hash tag is needed).
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createForgetStore } from '../../../src/shared/forget-store.js';

describe('createForgetStore cluster owner eviction (integration)', () => {
	let client;
	let redis;
	let store;

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-forget-owner:' });
		redis = client.redis;
		store = createForgetStore({}, { redis });
	});

	beforeEach(async () => {
		// The realtime hashes live OUTSIDE the suite's key prefix (they are raw,
		// exactly as platform.redis writes them), so the default prefix-scoped
		// reset cannot reach them - wipe both families explicitly.
		await resetBackendKeys(client, '__live-room-owner:*');
		await resetBackendKeys(client, '__live-presence:*');
	});

	afterAll(async () => { if (client) await client.quit?.(); });

	async function seedOwnerRoom(topic, fields) {
		for (const [f, v] of Object.entries(fields)) {
			await redis.hset('__live-room-owner:' + topic, f, String(v));
		}
	}

	it('evicts a remotely-owned room via the real Lua: successor, membership removal, TTL refresh', async () => {
		await seedOwnerRoom('room1', { o: 'u1', q: 3, 'j:u1': 1, 'j:u2': 2, 'j:u3': 3, 'n:u1': 1, 'n:u2': 1, 'n:u3': 1 });
		// A room the user never joined: must come back untouched, TTL included.
		await seedOwnerRoom('other', { o: 'x', q: 1, 'j:x': 1, 'n:x': 1 });

		const res = await store.purgeUser(null, 'u1');
		expect(res.ownerSuccessions).toEqual([{ topic: 'room1', owner: 'u2', reason: 'succeeded' }]);
		expect(res.rowsAffected.roomOwners).toBe(1);

		const h = await redis.hgetall('__live-room-owner:room1');
		expect(h.o).toBe('u2');
		expect(h['j:u1']).toBeUndefined();
		expect(h['n:u1']).toBeUndefined();
		expect(h['j:u3']).toBe('3');

		// The eviction script refreshes the same idle TTL the realtime join/leave
		// transitions apply on the touched hash; an untouched room keeps its
		// (absent) expiry exactly as it was.
		expect(await redis.ttl('__live-room-owner:room1')).toBeGreaterThan(0);
		expect(await redis.ttl('__live-room-owner:other')).toBe(-1);
	});

	it('vacates an emptied room: role and allocator cleared, key gone', async () => {
		await seedOwnerRoom('solo', { o: 'u1', q: 1, 'j:u1': 1, 'n:u1': 1 });

		const res = await store.purgeUser(null, 'u1');
		expect(res.ownerSuccessions).toEqual([{ topic: 'solo', owner: null, reason: 'vacated' }]);
		expect(await redis.exists('__live-room-owner:solo')).toBe(0);
	});

	it('scopes the real SCAN to the purged tenant', async () => {
		await seedOwnerRoom('@t/acme/roomA', { o: 'u1', q: 2, 'j:u1': 1, 'j:u2': 2, 'n:u1': 1, 'n:u2': 1 });
		await seedOwnerRoom('roomB', { o: 'u1', q: 2, 'j:u1': 1, 'j:u2': 2, 'n:u1': 1, 'n:u2': 1 });

		const scoped = await store.purgeUser('acme', 'u1');
		expect(scoped.ownerSuccessions).toEqual([{ topic: '@t/acme/roomA', owner: 'u2', reason: 'succeeded' }]);
		expect((await redis.hgetall('__live-room-owner:roomB')).o).toBe('u1');
	});

	it('erases the presence roster fields, leaving other members intact', async () => {
		await redis.hset('__live-presence:room1', 'c:u1', '2');
		await redis.hset('__live-presence:room1', 'd:u1', '{"name":"x"}');
		await redis.hset('__live-presence:room1', 'c:u2', '1');
		await redis.hset('__live-presence:room1', 'd:u2', '{}');

		const res = await store.purgeUser(null, 'u1');
		expect(res.rowsAffected.presenceRoster).toBe(2);
		const h = await redis.hgetall('__live-presence:room1');
		expect(h['c:u1']).toBeUndefined();
		expect(h['d:u1']).toBeUndefined();
		expect(h['c:u2']).toBe('1');
	});

	it('is idempotent: a second purge finds nothing and reports no successions', async () => {
		await seedOwnerRoom('room1', { o: 'u1', q: 2, 'j:u1': 1, 'j:u2': 2, 'n:u1': 1, 'n:u2': 1 });

		const first = await store.purgeUser(null, 'u1');
		expect(first.ownerSuccessions).toHaveLength(1);

		const second = await store.purgeUser(null, 'u1');
		expect(second.ownerSuccessions).toEqual([]);
		expect(second.rowsAffected.roomOwners).toBe(0);
		expect((await redis.hgetall('__live-room-owner:room1')).o).toBe('u2');
	});
});
