import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createAlarmStore } from '../../src/redis/alarm-store.js';
import { alarmGenerationContract } from '../helpers/alarm-generation-contract.js';
import { alarmExecutionContract } from '../helpers/alarm-execution-contract.js';

describe.each([false, true])('redis ordered alarms (cluster=%s)', cluster => {
	let client, store, peer;
	beforeEach(() => {
		client = mockRedisClient('app:', { cluster });
		store = createAlarmStore(client);
		peer = createAlarmStore(client);
	});
	alarmGenerationContract(() => [store, peer], () => client);
	alarmExecutionContract({
		stores: () => [store, peer],
		restart: () => createAlarmStore(client),
		fill: count => {
			// Direct writes: the double's ZADD re-sorts on every member.
			const records = client._hashes.get(client.key('alarm:{alarms}:executions')) ?? new Map();
			const order = client._sortedSets.get(client.key('alarm:{alarms}:execution-order')) ?? [];
			for (let index = order.length; index < count; index++) {
				const topic = 'fill:' + String(index).padStart(6, '0'), id = topic + '\0\0' + '1';
				records.set(id, JSON.stringify({ topic, generation: '1', score: '1', payload: '', attempt: '1', status: 'claimed', claimedAt: '1' }));
				order.push({ member: id, score: 0 });
			}
			client._hashes.set(client.key('alarm:{alarms}:executions'), records);
			client._sortedSets.set(client.key('alarm:{alarms}:execution-order'), order);
		}
	});
	it('orders execution ids by topic bytes even when topics contain NUL', async () => {
		for (const topic of ['ab', 'a\0b', 'a', 'a\0']) {
			const generation = await store.nextGeneration(topic);
			await store.set(topic, 1, null, generation);
			await store.claim(topic, generation);
		}
		const page = await store.executions({ limit: 2 });
		expect(page.items.map(item => item.topic)).toEqual(['a', 'a\0']);
		expect((await store.executions({ after: page.next })).items.map(item => item.topic)).toEqual(['a\0b', 'ab']);
		expect(await store.getExecution('a\0b', '1')).toMatchObject({ topic: 'a\0b', status: 'claimed' });
	});
	it('claims a legacy bare row as generation 1 and retains its watermark', async () => {
		await client.redis.zadd(client.key('alarm:{alarms}:due'), 500, 'bare');
		expect(await store.claim('bare', '1')).toEqual({ attempt: 1 });
		expect(await store.getExecution('bare', '1')).toMatchObject({ at: 500, meta: null, attempt: 1 });
		expect(await store.nextGeneration('bare')).toBe('2');
		expect(await store.set('bare', 9, null, '1')).toBe(false);
	});
	it('keeps the claimed execution when the claim reply is lost', async () => {
		const generation = await store.nextGeneration('room');
		await store.set('room', 1, null, generation);
		const evaluate = client.redis.eval.bind(client.redis);
		vi.spyOn(client.redis, 'eval').mockImplementationOnce(async (...args) => {
			await evaluate(...args);
			throw new Error('reply lost');
		});
		await expect(store.claim('room', generation)).rejects.toThrow('reply lost');
		expect(await peer.claim('room', generation)).toBeNull();
		expect(await peer.getExecution('room', generation)).toMatchObject({ attempt: 1, status: 'claimed' });
		expect(await peer.requeue('room', generation, 1, { at: 2 })).toBe(false);
		expect(await peer.requeue('room', generation, 1, { at: 2, allowAbandoned: true })).toBe(true);
		expect(await store.claim('room', generation)).toEqual({ attempt: 2 });
	});
	it('a failed outcome write leaves the claim available for inspection', async () => {
		const generation = await store.nextGeneration('room');
		await store.set('room', 1, null, generation);
		await store.claim('room', generation);
		vi.spyOn(client.redis, 'eval').mockRejectedValueOnce(new Error('NOPERM'));
		await expect(store.fail('room', generation, 1, { errorClass: 'handler', failedAt: 2 })).rejects.toThrow('NOPERM');
		expect(await peer.getExecution('room', generation)).toMatchObject({ status: 'claimed', failedAt: null });
	});
	it('fails closed on a corrupt execution record without rewriting it', async () => {
		const generation = await store.nextGeneration('room');
		await store.set('room', 1, null, generation);
		await store.claim('room', generation);
		const key = client.key('alarm:{alarms}:executions'), id = 'room\0\0' + generation;
		await client.redis.hset(key, id, 'not json');
		await expect(store.complete('room', generation, 1)).rejects.toThrow(/inconsistent execution/);
		await expect(store.getExecution('room', generation)).rejects.toThrow(/inconsistent execution/);
		expect(await client.redis.hget(key, id)).toBe('not json');
	});
	it('clear removes schedules and requeued attempts but keeps retained executions', async () => {
		const kept = await store.nextGeneration('kept');
		await store.set('kept', 1, null, kept);
		await store.claim('kept', kept);
		const retried = await store.nextGeneration('retried');
		await store.set('retried', 1, null, retried);
		await store.claim('retried', retried);
		await store.fail('retried', retried, 1, { errorClass: 'handler', failedAt: 1 });
		await store.requeue('retried', retried, 1, { at: 5 });
		await store.clear();
		expect(await store.get('retried')).toBeNull();
		expect(await client.redis.hlen(client.key('alarm:{alarms}:attempt'))).toBe(0);
		expect(await store.getExecution('kept', kept)).toMatchObject({ status: 'claimed' });
	});

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
	it.each(['due', 'meta', 'reserved', 'committed', 'current', 'attempt', 'executions', 'execution-order'])('checks the slot of the %s key', suffix => {
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
