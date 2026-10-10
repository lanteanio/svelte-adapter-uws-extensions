import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createAlarmStore } from '../../../src/redis/alarm-store.js';
import { alarmGenerationContract } from '../../helpers/alarm-generation-contract.js';

// The mirror config runs these same connection races against a real Cluster.
// Lua must bind the observed generation, metadata and due index atomically.
describe('redis ordered alarm generations (integration)', { retry: 0 }, () => {
	let clientA, clientB, storeA, storeB;
	beforeAll(() => {
		clientA = createBackendClient({ keyPrefix: 'inttest-alarm:' });
		clientB = createBackendClient({ keyPrefix: 'inttest-alarm:' });
		storeA = createAlarmStore(clientA);
		storeB = createAlarmStore(clientB);
	});
	beforeEach(async () => { await resetBackendKeys(clientA); });
	afterAll(async () => {
		await clientA?.quit?.();
		await clientB?.quit?.();
	});
	alarmGenerationContract(() => [storeA, storeB], () => clientA);
	it('bulk due reads retain order across chunks and validate every row before adoption', async () => {
		const bulk = createAlarmStore(clientA, { dueBatch: 600 });
		const dueKey = clientA.key('alarm:{alarms}:due');
		const reservedKey = clientA.key('alarm:{alarms}:reserved');
		const committedKey = clientA.key('alarm:{alarms}:committed');
		const currentKey = clientA.key('alarm:{alarms}:current');
		const entries = Array.from({ length: 600 }, (_, index) => [index, 'legacy:' + index]);
		await clientA.redis.zadd(dueKey, ...entries.flat());
		await clientA.redis.hset(reservedKey, 'legacy:599', 'bad authority');
		await expect(bulk.due(1000)).rejects.toThrow(/inconsistent generation/);
		expect(await clientA.redis.hlen(reservedKey)).toBe(1);
		expect(await clientA.redis.hlen(committedKey)).toBe(0);
		expect(await clientA.redis.hlen(currentKey)).toBe(0);
		await clientA.redis.hdel(reservedKey, 'legacy:599');
		const expected = entries.map(([at, topic]) => ({ topic, at, meta: null, generation: '1' }));
		expect(await bulk.due(1000)).toEqual(expected);
		expect(await bulk.due(1000)).toEqual(expected);
		expect(await clientA.redis.hlen(reservedKey)).toBe(600);
		expect(await clientA.redis.hlen(committedKey)).toBe(600);
		expect(await clientA.redis.hlen(currentKey)).toBe(600);
	});

	it('two independent connections conditionally delete exactly once in every race', async () => {
		for (let round = 0; round < 40; round++) {
			const topic = 'room:race-' + round;
			const generation = await storeA.nextGeneration(topic);
			await storeA.set(topic, 1000 + round, { path: 'rooms/x' }, generation);
			const results = await Promise.all([storeA.delete(topic, generation), storeB.delete(topic, generation)]);
			expect(results.filter(Boolean)).toHaveLength(1);
		}
		expect(await storeA.due(Number.MAX_SAFE_INTEGER)).toEqual([]);
		expect(await clientA.redis.hlen(clientA.key('alarm:{alarms}:meta'))).toBe(0);
	});
	it('concurrent reservations are unique and only the last can commit', async () => {
		const stores = Array.from({ length: 8 }, (_, index) => index % 2 ? storeA : storeB);
		const generations = await Promise.all(stores.map(store => store.nextGeneration('room')));
		expect(new Set(generations).size).toBe(8);
		const committed = await Promise.all(stores.map((store, index) => store.set('room', index, null, generations[index])));
		expect(committed.filter(Boolean)).toHaveLength(1);
		expect((await storeA.get('room')).generation).toBe('8');
		const claims = await Promise.all(stores.map(store => store.delete('room', '8')));
		expect(claims.filter(Boolean)).toHaveLength(1);
	});
	it('checks key types before writing rather than partially committing on WRONGTYPE', async () => {
		const generation = await storeA.nextGeneration('room');
		const metaKey = clientA.key('alarm:{alarms}:meta');
		await clientA.redis.set(metaKey, 'wrong type');
		await expect(storeA.set('room', 1, null, generation)).rejects.toThrow(/WRONGTYPE/);
		expect(await clientA.redis.zscore(clientA.key('alarm:{alarms}:due'), 'room')).toBeNull();
		expect(await clientA.redis.hget(clientA.key('alarm:{alarms}:committed'), 'room')).toBe('0');
		expect(await clientA.redis.get(metaKey)).toBe('wrong type');
	});
	it('reads metadata and generation from one snapshot during repeated replacements', async () => {
		const writer = async () => {
			for (let index = 0; index < 100; index++) {
				const generation = await storeA.nextGeneration('moving');
				await storeA.set('moving', Number(generation), { generation }, generation);
			}
		};
		const reader = async () => {
			let observed = 0;
			for (let index = 0; index < 100; index++) {
				const [row, due] = await Promise.all([storeB.get('moving'), storeB.due(1000)]);
				for (const snapshot of [row, ...due]) {
					if (!snapshot) continue;
					observed++;
					expect(snapshot.meta.generation).toBe(snapshot.generation);
					expect(snapshot.at).toBe(Number(snapshot.generation));
				}
			}
			expect(observed).toBeGreaterThan(0);
		};
		await Promise.all([writer(), reader()]);
	});
});
