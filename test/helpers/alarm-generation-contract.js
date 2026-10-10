import { it, expect } from 'vitest';
import { createAlarmStore } from '../../src/redis/alarm-store.js';

/** The same public-operation cases run against the double and native backends. */
export function alarmGenerationContract(stores, client) {
	const key = suffix => client().key('alarm:{alarms}:' + suffix);
	it('reserves without changing the current row and rejects delayed older writes', async () => {
		const [a, b] = stores();
		const first = await a.nextGeneration('room');
		expect(first).toBe('1');
		expect(await a.get('room')).toBeNull();
		expect(await a.set('room', 100, { path: 'first' }, first)).toBe(true);
		const second = await b.nextGeneration('room');
		expect(second).toBe('2');
		expect(await a.get('room')).toEqual({ topic: 'room', at: 100, meta: { path: 'first' }, generation: first });
		const third = await a.nextGeneration('room');
		expect(await a.set('room', 300, { path: 'third' }, third)).toBe(true);
		expect(await b.set('room', 200, { path: 'second' }, second)).toBe(false);
		expect(await a.set('room', 999, null, third)).toBe(false);
		expect(await b.get('room')).toEqual({ topic: 'room', at: 300, meta: { path: 'third' }, generation: third });
	});
	it('fences a due observation and stale cancellation from a replacement', async () => {
		const [a, b] = stores();
		const old = await a.nextGeneration('room');
		await a.set('room', 1, null, old);
		const observed = (await b.due(10))[0];
		const fresh = await a.nextGeneration('room');
		await a.set('room', 2, { path: 'new' }, fresh);
		expect(await b.delete('room', observed.generation)).toBe(false);
		expect(await b.get('room')).toEqual({ topic: 'room', at: 2, meta: { path: 'new' }, generation: fresh });
	});
	it('retains authority after deletion and a fresh store instance', async () => {
		const [a, b] = stores();
		const generation = await a.nextGeneration('room');
		await a.set('room', 1, null, generation);
		expect(await b.delete('room', generation)).toBe(true);
		expect(await a.delete('room', generation)).toBe(false);
		expect(await b.set('room', 2, null, generation)).toBe(false);
		expect(await b.get('room')).toBeNull();
		const restarted = createAlarmStore(client());
		expect(await restarted.nextGeneration('room')).toBe('2');
	});
	it('deleting the current row does not cancel a newer reserved write', async () => {
		const [a, b] = stores();
		const first = await a.nextGeneration('room');
		await a.set('room', 1, null, first);
		const next = await b.nextGeneration('room');
		expect(await a.delete('room', first)).toBe(true);
		expect(await b.set('room', 2, null, next)).toBe(true);
	});
	it('clear invalidates outstanding reservations without resetting allocation', async () => {
		const [a, b] = stores();
		const active = await a.nextGeneration('active');
		await a.set('active', 1, null, active);
		const pending = await b.nextGeneration('active');
		const neverCommitted = await b.nextGeneration('pending');
		await a.clear();
		expect(await b.due(100)).toEqual([]);
		expect(await b.set('active', 2, null, pending)).toBe(false);
		expect(await b.set('pending', 2, null, neverCommitted)).toBe(false);
		expect(await b.nextGeneration('active')).toBe('3');
		expect(await b.nextGeneration('pending')).toBe('2');
	});
	it('adopts legacy rows atomically without rewriting metadata', async () => {
		const [a, b] = stores(), redis = client().redis;
		const raw = '{ "at": 12, "meta": {"alarmId":"legacy","tenantId":"acme"} }';
		await redis.hset(key('meta'), 'legacy', raw);
		await redis.zadd(key('due'), 12, 'legacy');
		const [row, reserved] = await Promise.all([a.get('legacy'), b.nextGeneration('legacy')]);
		expect(row).toEqual({ topic: 'legacy', at: 12, meta: { alarmId: 'legacy', tenantId: 'acme' }, generation: '1' });
		expect(reserved).toBe('2');
		expect(await redis.hget(key('meta'), 'legacy')).toBe(raw);
		expect(await a.delete('legacy', '1')).toBe(true);
		expect(await b.set('legacy', 20, null, reserved)).toBe(true);
	});
	it('clear preserves allocation even for a previously unread legacy row', async () => {
		const [a] = stores(), redis = client().redis;
		await redis.zadd(key('due'), 1, 'legacy');
		await a.clear();
		expect(await a.nextGeneration('legacy')).toBe('2');
		expect(await a.set('legacy', 2, null, '1')).toBe(false);
	});
	it('allocates and compares exact decimal strings above the safe-integer boundary', async () => {
		const [a, b] = stores(), redis = client().redis;
		await redis.hset(key('reserved'), 'large', '9007199254740992');
		await redis.hset(key('committed'), 'large', '9007199254740991');
		expect(await a.nextGeneration('large')).toBe('9007199254740993');
		const latest = await b.nextGeneration('large');
		expect(latest).toBe('9007199254740994');
		expect(await a.set('large', 1, null, '9007199254740993')).toBe(false);
		expect(await b.set('large', 2, null, latest)).toBe(true);
		expect((await a.get('large')).generation).toBe(latest);
	});
	it('rejects exhausted allocation without wrapping or changing the active row', async () => {
		const [a, b] = stores(), redis = client().redis;
		await redis.hset(key('reserved'), 'last', '9223372036854775806');
		await redis.hset(key('committed'), 'last', '9223372036854775805');
		const last = await a.nextGeneration('last');
		expect(last).toBe('9223372036854775807');
		await a.set('last', 7, null, last);
		await expect(b.nextGeneration('last')).rejects.toThrow(/exhausted/);
		expect(await b.get('last')).toEqual({ topic: 'last', at: 7, meta: null, generation: last });
		await b.clear();
		await expect(a.nextGeneration('last')).rejects.toThrow(/exhausted/);
		expect(await a.set('last', 8, null, last)).toBe(false);
	});
	it('fails closed on corrupt authority without repairing or rewriting the schedule', async () => {
		const [a] = stores(), redis = client().redis;
		const generation = await a.nextGeneration('room');
		await a.set('room', 1, { path: 'keep' }, generation);
		const raw = await redis.hget(key('meta'), 'room');
		await redis.hset(key('reserved'), 'room', '01');
		await expect(a.nextGeneration('room')).rejects.toThrow(/authority/);
		await expect(a.clear()).rejects.toThrow(/authority/);
		expect(await redis.hget(key('meta'), 'room')).toBe(raw);
		expect(await redis.hget(key('reserved'), 'room')).toBe('01');
	});
}
