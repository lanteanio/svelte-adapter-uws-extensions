import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createAlarmStore } from '../../src/redis/alarm-store.js';
import { alarmGenerationContract } from '../helpers/alarm-generation-contract.js';

describe.each([false, true])('redis ordered alarms (cluster=%s)', cluster => {
	let client, store, peer;
	beforeEach(() => {
		client = mockRedisClient('app:', { cluster });
		store = createAlarmStore(client);
		peer = createAlarmStore(client);
	});
	alarmGenerationContract(() => [store, peer], () => client);

	it('returns only due rows within the configured batch', async () => {
		const bounded = createAlarmStore(client, { dueBatch: 2 });
		for (const [topic, at] of [['a', 1], ['b', 2], ['c', 3]]) {
			await bounded.set(topic, at, null, await bounded.nextGeneration(topic));
		}
		expect(await bounded.due(0)).toEqual([]);
		expect((await bounded.due(10)).map(row => row.topic)).toEqual(['a', 'b']);
	});
	it('retains a legacy bare row with its indexed timestamp and generation', async () => {
		await client.redis.zadd(client.key('alarm:{alarms}:due'), 500, 'bare');
		expect(await store.due(1000)).toEqual([{ topic: 'bare', at: 500, meta: null, generation: '1' }]);
	});
	it('validates topics, times, options and canonical bounded generations', async () => {
		await expect(store.nextGeneration('')).rejects.toThrow(/non-empty/);
		await expect(store.get('')).rejects.toThrow(/non-empty/);
		await expect(store.set('room', NaN, null, '1')).rejects.toThrow(/finite/);
		await expect(store.due(Infinity)).rejects.toThrow(/finite/);
		for (const generation of [undefined, null, 1, '', '0', '01', '-1', '+1', '1.0', '9223372036854775808']) {
			await expect(store.set('room', 1, null, generation)).rejects.toThrow(/generation/);
			await expect(store.delete('room', generation)).rejects.toThrow(/generation/);
		}
		expect(() => createAlarmStore(client, { dueBatch: 0 })).toThrow(/positive integer/);
		expect(() => createAlarmStore(client, { keyPrefix: 5 })).toThrow(/keyPrefix/);
	});
	it.each(['due', 'meta', 'reserved', 'committed', 'current'])('checks the slot of the %s key', suffix => {
		client.key = name => name.endsWith(':' + suffix) ? '{other}:' + name : name;
		expect(() => createAlarmStore(client)).toThrow(/different cluster slots/);
	});
	it.each(['nextGeneration', 'get', 'set', 'delete', 'due', 'clear'])('%s propagates a rejected script without changing the row', async method => {
		const generation = await store.nextGeneration('room');
		await store.set('room', 1, { path: 'keep' }, generation);
		const before = await store.get('room');
		const args = { nextGeneration: ['room'], get: ['room'], set: ['room', 2, null, generation], delete: ['room', generation], due: [10], clear: [] };
		vi.spyOn(client.redis, 'eval').mockRejectedValueOnce(new Error('NOPERM'));
		await expect(store[method](...args[method])).rejects.toThrow('NOPERM');
		expect(await store.get('room')).toEqual(before);
	});
	it('retains a committed generation when its reply is lost and refuses replay', async () => {
		const generation = await store.nextGeneration('room');
		const evaluate = client.redis.eval.bind(client.redis);
		vi.spyOn(client.redis, 'eval').mockImplementationOnce(async (...args) => {
			await evaluate(...args);
			throw new Error('reply lost');
		});
		await expect(store.set('room', 1, null, generation)).rejects.toThrow('reply lost');
		expect(await peer.get('room')).toEqual({ topic: 'room', at: 1, meta: null, generation });
		expect(await peer.set('room', 2, null, generation)).toBe(false);
	});
});
