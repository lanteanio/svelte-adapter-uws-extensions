import { it, expect } from 'vitest';
import { MAX_ALARM_EXECUTIONS } from '../../src/shared/alarm-generation.js';

/**
 * Retained-execution cases shared by the Redis double and both native backends.
 *
 * `stores()` returns two stores on independent connections, `restart()` a fresh
 * store instance over the same storage, and `fill(count)` writes `count`
 * unrelated retained executions directly into storage.
 */
export function alarmExecutionContract({ stores, restart, fill }) {
	async function schedule(store, topic, at = 100, meta = { alarmId: 'tick', tenantId: 'acme' }) {
		const generation = await store.nextGeneration(topic);
		expect(await store.set(topic, at, meta, generation)).toBe(true);
		return generation;
	}

	it('claim moves the schedule into a retained execution exactly once', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room');
		expect(await a.claim('room', generation)).toEqual({ attempt: 1 });
		// A lost reply looks like this to the retrying caller: the row is gone.
		expect(await b.claim('room', generation)).toBeNull();
		expect(await b.get('room')).toBeNull();
		expect(await b.due(1000)).toEqual([]);
		const execution = await b.getExecution('room', generation);
		expect(execution).toEqual({
			topic: 'room', at: 100, meta: { alarmId: 'tick', tenantId: 'acme' }, generation,
			attempt: 1, status: 'claimed', claimedAt: expect.any(Number), failedAt: null, errorClass: null
		});
		expect(Number.isSafeInteger(execution.claimedAt)).toBe(true);
	});

	it('claim refuses a stale generation and leaves the replacement scheduled', async () => {
		const [a, b] = stores();
		const old = await schedule(a, 'room', 1);
		const fresh = await schedule(b, 'room', 2, { alarmId: 'new' });
		expect(await a.claim('room', old)).toBeNull();
		expect(await a.claim('missing', '1')).toBeNull();
		expect(await a.getExecution('room', old)).toBeNull();
		expect(await b.get('room')).toEqual({ topic: 'room', at: 2, meta: { alarmId: 'new' }, generation: fresh });
	});

	it('cancellation removes schedules only and never a retained execution', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room');
		await a.claim('room', generation);
		expect(await b.delete('room', generation)).toBe(false);
		expect((await b.getExecution('room', generation)).status).toBe('claimed');
	});

	it('a newer schedule coexists with an older retained execution', async () => {
		const [a, b] = stores();
		const first = await schedule(a, 'room', 1, { alarmId: 'first' });
		await a.claim('room', first);
		const second = await schedule(b, 'room', 2, { alarmId: 'second' });
		expect(await b.get('room')).toEqual({ topic: 'room', at: 2, meta: { alarmId: 'second' }, generation: second });
		expect(await b.claim('room', second)).toEqual({ attempt: 1 });
		expect(await a.complete('room', first, 1)).toBe(true);
		expect(await a.getExecution('room', first)).toBeNull();
		expect(await a.getExecution('room', second)).toMatchObject({ meta: { alarmId: 'second' }, status: 'claimed' });
	});

	it('complete and fail are fenced by the exact attempt', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room');
		await a.claim('room', generation);
		expect(await a.complete('room', generation, 2)).toBe(false);
		expect(await a.fail('room', generation, 2, { errorClass: 'handler', failedAt: 5 })).toBe(false);
		expect(await a.complete('room', '99', 1)).toBe(false);
		expect((await b.getExecution('room', generation)).status).toBe('claimed');
		expect(await a.complete('room', generation, 1)).toBe(true);
		// A retried acknowledgement after a lost reply finds nothing left.
		expect(await b.complete('room', generation, 1)).toBe(false);
		expect(await b.fail('room', generation, 1, { errorClass: 'handler', failedAt: 5 })).toBe(false);
	});

	it('fail retains the original row and records only the classification', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room', 100);
		await a.claim('room', generation);
		expect(await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 5000.9 })).toBe(true);
		const failed = await b.getExecution('room', generation);
		expect(failed).toEqual({
			topic: 'room', at: 100, meta: { alarmId: 'tick', tenantId: 'acme' }, generation,
			attempt: 1, status: 'failed', claimedAt: expect.any(Number), failedAt: 5000, errorClass: 'handler'
		});
		// A repeated outcome after a lost reply succeeds and keeps the first time.
		expect(await b.fail('room', generation, 1, { errorClass: 'handler', failedAt: 9000 })).toBe(true);
		expect((await a.getExecution('room', generation)).failedAt).toBe(5000);
		// Operator disposal of failed work after inspection.
		expect(await b.complete('room', generation, 1)).toBe(true);
		expect(await a.executions()).toEqual({ items: [], next: null });
	});

	it('requeue reschedules a failed attempt with its generation and increments the next claim', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room', 100);
		await a.claim('room', generation);
		await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 150 });
		expect(await b.requeue('room', generation, 1, { at: 500 })).toBe(true);
		expect(await a.getExecution('room', generation)).toBeNull();
		expect(await a.get('room')).toEqual({ topic: 'room', at: 500, meta: { alarmId: 'tick', tenantId: 'acme' }, generation });
		expect(await a.due(499)).toEqual([]);
		expect(await a.due(500)).toEqual([{ topic: 'room', at: 500, meta: { alarmId: 'tick', tenantId: 'acme' }, generation }]);
		// The retry is a new attempt; the old one is fenced everywhere.
		expect(await b.requeue('room', generation, 1, { at: 600 })).toBe(false);
		expect(await a.claim('room', generation)).toEqual({ attempt: 2 });
		expect(await b.complete('room', generation, 1)).toBe(false);
		expect(await b.fail('room', generation, 1, { errorClass: 'handler', failedAt: 1 })).toBe(false);
		expect(await a.getExecution('room', generation)).toMatchObject({ at: 500, attempt: 2, status: 'claimed', failedAt: null, errorClass: null });
		await a.fail('room', generation, 2, { errorClass: 'handler', failedAt: 700 });
		expect(await a.requeue('room', generation, 2, { at: 800 })).toBe(true);
		expect(await b.claim('room', generation)).toEqual({ attempt: 3 });
		expect(await b.complete('room', generation, 3)).toBe(true);
	});

	it('a cancelled requeue is gone and a fresh schedule restarts at attempt one', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room');
		await a.claim('room', generation);
		await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 1 });
		await a.requeue('room', generation, 1, { at: 10 });
		expect(await b.delete('room', generation)).toBe(true);
		expect(await b.claim('room', generation)).toBeNull();
		const next = await schedule(b, 'room', 20);
		expect(await a.claim('room', next)).toEqual({ attempt: 1 });
	});

	it('a replacement schedule drops a pending retry attempt', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room');
		await a.claim('room', generation);
		await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 1 });
		await a.requeue('room', generation, 1, { at: 10 });
		const next = await schedule(b, 'room', 20);
		expect(await a.claim('room', next)).toEqual({ attempt: 1 });
	});

	it('requeue refuses a newer reservation or schedule without changing either record', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room', 100);
		await a.claim('room', generation);
		await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 5 });
		const before = await a.getExecution('room', generation);
		const reserved = await b.nextGeneration('room');
		expect(await a.requeue('room', generation, 1, { at: 1 })).toBe(false);
		expect(await a.get('room')).toBeNull();
		expect(await b.set('room', 9, null, reserved)).toBe(true);
		expect(await a.requeue('room', generation, 1, { at: 1 })).toBe(false);
		expect(await a.get('room')).toEqual({ topic: 'room', at: 9, meta: null, generation: reserved });
		expect(await a.getExecution('room', generation)).toEqual(before);
	});

	it('requeue refuses a mismatched attempt and a claimed record unless abandonment is confirmed', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room', 100);
		await a.claim('room', generation);
		const claimed = await a.getExecution('room', generation);
		expect(await b.requeue('room', generation, 2, { at: 1, allowAbandoned: true })).toBe(false);
		expect(await b.requeue('room', generation, 1, { at: 1 })).toBe(false);
		expect(await b.requeue('room', generation, 1, { at: 1, allowAbandoned: false })).toBe(false);
		expect(await a.getExecution('room', generation)).toEqual(claimed);
		expect(await a.get('room')).toBeNull();
		expect(await b.requeue('room', generation, 1, { at: 1, allowAbandoned: true })).toBe(true);
		expect(await a.claim('room', generation)).toEqual({ attempt: 2 });
	});

	it('executions pages by topic then generation with an exclusive cursor', async () => {
		const [a, b] = stores();
		const claimed = [];
		for (const topic of ['b', 'a', 'a', 'c']) {
			const generation = await schedule(a, topic);
			await a.claim(topic, generation);
			claimed.push([topic, generation]);
		}
		// Ten generations of one topic: '10' sorts before '9'.
		for (let index = 0; index < 8; index++) {
			const generation = await schedule(b, 'a');
			await b.claim('a', generation);
		}
		const all = [];
		let page = await b.executions({ limit: 3 });
		expect(page.items).toHaveLength(3);
		while (true) {
			all.push(...page.items.map(item => [item.topic, item.generation]));
			if (!page.next) break;
			expect(page.next).toEqual({ topic: page.items.at(-1).topic, generation: page.items.at(-1).generation });
			page = await b.executions({ limit: 3, after: page.next });
		}
		expect(all).toEqual([
			['a', '1'], ['a', '10'], ['a', '2'], ['a', '3'], ['a', '4'], ['a', '5'], ['a', '6'], ['a', '7'], ['a', '8'], ['a', '9'],
			['b', '1'], ['c', '1']
		]);
		expect((await a.executions()).items).toHaveLength(12);
		expect((await a.executions({ limit: 5000 })).next).toBeNull();
		expect(await a.executions({ after: { topic: 'b', generation: '1' } })).toMatchObject({ items: [{ topic: 'c', generation: '1' }], next: null });
		expect(await a.executions({ after: { topic: 'c', generation: '1' } })).toEqual({ items: [], next: null });
		expect(await a.executions({ limit: 12 })).toMatchObject({ next: null });
		expect((await a.executions({ limit: 11 })).next).toEqual({ topic: 'b', generation: '1' });
	});

	it('retained executions survive a store restart and stay operable', async () => {
		const [a] = stores();
		const generation = await schedule(a, 'room', 42);
		await a.claim('room', generation);
		await a.fail('room', generation, 1, { errorClass: 'handler', failedAt: 43 });
		const restarted = await restart();
		expect(await restarted.getExecution('room', generation)).toMatchObject({ at: 42, attempt: 1, status: 'failed', failedAt: 43 });
		expect(await restarted.requeue('room', generation, 1, { at: 50 })).toBe(true);
		const again = await restart();
		expect(await again.claim('room', generation)).toEqual({ attempt: 2 });
	});

	it('at capacity claim rejects and keeps the schedule; retained work is never evicted', async () => {
		const [a, b] = stores();
		const generation = await schedule(a, 'room', 7);
		await fill(MAX_ALARM_EXECUTIONS);
		await expect(a.claim('room', generation)).rejects.toMatchObject({ code: 'ALARM_CAPACITY' });
		expect(await b.get('room')).toEqual({ topic: 'room', at: 7, meta: { alarmId: 'tick', tenantId: 'acme' }, generation });
		expect(await b.getExecution('room', generation)).toBeNull();
		const first = (await b.executions({ limit: 1 })).items[0];
		expect(await b.complete(first.topic, first.generation, first.attempt)).toBe(true);
		expect(await a.claim('room', generation)).toEqual({ attempt: 1 });
	});

	it('validates execution arguments before reaching storage', async () => {
		const [a] = stores();
		for (const attempt of [0, -1, 1.5, '1', Number.MAX_SAFE_INTEGER + 1]) {
			await expect(a.complete('room', '1', attempt)).rejects.toThrow(/attempt/);
			await expect(a.fail('room', '1', attempt, { errorClass: 'handler', failedAt: 1 })).rejects.toThrow(/attempt/);
			await expect(a.requeue('room', '1', attempt, { at: 1 })).rejects.toThrow(/attempt/);
		}
		await expect(a.claim('', '1')).rejects.toThrow(/topic/);
		await expect(a.claim('room', '01')).rejects.toThrow(/generation/);
		await expect(a.getExecution('room', 1)).rejects.toThrow(/generation/);
		for (const failure of [undefined, {}, { errorClass: 'Error: boom', failedAt: 1 }, { errorClass: 'handler', failedAt: NaN }]) {
			await expect(a.fail('room', '1', 1, failure)).rejects.toThrow(/failure/);
		}
		for (const options of [undefined, {}, { at: Infinity }, { at: 1, allowAbandoned: 'yes' }]) {
			await expect(a.requeue('room', '1', 1, options)).rejects.toThrow(TypeError);
		}
		await expect(a.requeue('room', '1', Number.MAX_SAFE_INTEGER, { at: 1 })).rejects.toThrow(/exhausted/);
		for (const options of [{ limit: 0 }, { limit: 1.5 }, { after: { topic: 'a' } }, { after: 'a' }, 'x']) {
			await expect(a.executions(options)).rejects.toThrow(TypeError);
		}
	});
}
