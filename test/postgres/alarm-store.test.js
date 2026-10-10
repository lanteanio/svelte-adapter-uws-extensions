import { describe, it, expect, beforeEach } from 'vitest';
import { createAlarmStore } from '../../src/postgres/alarm-store.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';

// This double checks wrapper behavior; native integration tests prove locking,
// transactions and generation ordering across independent pools.
function fakePg() {
	const rows = new Map(), generations = new Map(), base = mockPgClient();
	const client = {
		...base,
		rows,
		query: async (q) => {
			const text = (typeof q === 'string' ? q : q.text).replace(/\s+/g, ' ').trim();
			const values = (typeof q === 'string' ? [] : q.values) || [];
			if (q.name?.startsWith('alarm_reserve_')) {
				const state = generations.get(values[0]) ?? { reserved: 0n, committed: 0n };
				state.reserved++; generations.set(values[0], state);
				return { rows: [{ generation: String(state.reserved) }], rowCount: 1 };
			}
			if (q.name?.startsWith('alarm_set_g_')) {
				const [topic, fireAt, metaJson, tenant, generation] = values, state = generations.get(topic), value = BigInt(generation);
				if (!state || state.reserved !== value || state.committed >= value) return { rows: [], rowCount: 0 };
				state.committed = value;
				rows.set(topic, { topic, fire_at: String(fireAt), meta: metaJson == null ? null : JSON.parse(metaJson), tenant: tenant ?? null, generation });
				return { rows: [], rowCount: 1 };
			}
			if (q.name?.startsWith('alarm_del_g_')) {
				const had = rows.get(values[0])?.generation === values[1] && rows.delete(values[0]);
				return { rows: had ? [{ topic: values[0] }] : [], rowCount: had ? 1 : 0 };
			}
			if (q.name?.startsWith('alarm_get_')) return { rows: rows.has(values[0]) ? [rows.get(values[0])] : [], rowCount: rows.has(values[0]) ? 1 : 0 };
			if (text.startsWith('UPDATE svti_alarms_generations SET committed = reserved')) { for (const state of generations.values()) state.committed = state.reserved; return { rows: [], rowCount: 0 }; }
			if (text === 'DELETE FROM svti_alarms') { rows.clear(); return { rows: [], rowCount: 0 }; }
			if (q.name?.startsWith('alarm_due_g_')) {
				const now = Number(values[0]);
				const limit = Number(values[1]);
				const due = [...rows.entries()]
					.filter(([, r]) => r.fire_at <= now)
					.sort((a, b) => a[1].fire_at - b[1].fire_at)
					.slice(0, limit)
					.map(([, r]) => r);
				return { rows: due, rowCount: due.length };
			}
			return base.query(q);
		}
	};
	client.connect = async options => {
		const connection = await base.connect(options), catalogQuery = connection.query;
		connection.query = (q, values) => {
			const text = typeof q === 'string' ? q : q.text;
			if (/^(BEGIN|COMMIT|ROLLBACK|SET LOCAL|LOCK TABLE)/.test(text)) return Promise.resolve({ rows: [], rowCount: 0 });
			if (text.startsWith('SELECT a.topic FROM svti_alarms')) return Promise.resolve({ rows: [], rowCount: 0 });
			if (/^(UPDATE svti_alarms_generations SET committed = reserved|DELETE FROM svti_alarms$)/.test(text)) return client.query(q);
			return catalogQuery(q, values);
		};
		return connection;
	};
	return client;
}

async function schedule(store, topic, at, meta) {
	const generation = await store.nextGeneration(topic);
	expect(await store.set(topic, at, meta, generation)).toBe(true);
	return generation;
}

describe('postgres durable alarm store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = fakePg();
		store = createAlarmStore(client);
	});

	it('persists and returns a due alarm (BIGINT fire_at parsed back to a number)', async () => {
		await schedule(store, 'room:1', 1000, { alarmId: 'room-expiry', path: 'rooms/x', tenantId: 'acme' });
		expect(await store.due(999)).toEqual([]);
		expect(await store.due(1000)).toEqual([{ topic: 'room:1', at: 1000, generation: '1', meta: { alarmId: 'room-expiry', path: 'rooms/x', tenantId: 'acme' } }]);
	});

	it('delete() is the atomic claim: true once, then false', async () => {
		const generation = await schedule(store, 'room:c', 1000, { path: 'p' });
		expect(await store.delete('room:c', generation)).toBe(true);
		expect(await store.delete('room:c', generation)).toBe(false);
	});

	it('set() upserts (one alarm per topic)', async () => {
		await schedule(store, 'room:r', 1000, { path: 'p' });
		await schedule(store, 'room:r', 5000, { path: 'p2' });
		expect(await store.due(2000)).toEqual([]); // moved out to 5000
		expect(await store.due(5000)).toEqual([{ topic: 'room:r', at: 5000, generation: '2', meta: { path: 'p2' } }]);
	});

	it('mirrors tenantId into the tenant column for ops queries', async () => {
		await schedule(store, 'room:t', 1000, { path: 'p', tenantId: 'acme' });
		expect(client.rows.get('room:t').tenant).toBe('acme');
	});

	it('due() honors the dueBatch cap and fire_at ordering', async () => {
		const s = createAlarmStore(client, { dueBatch: 2 });
		await schedule(s, 'c', 3, null);
		await schedule(s, 'a', 1, null);
		await schedule(s, 'b', 2, null);
		const due = await s.due(100);
		expect(due.map((r) => r.topic)).toEqual(['a', 'b']); // ordered by fire_at, capped at 2
	});

	it('clear() removes everything', async () => {
		await schedule(store, 'x', 1, null);
		await schedule(store, 'y', 2, null);
		await store.clear();
		expect(await store.due(100)).toEqual([]);
		expect(await store.nextGeneration('x')).toBe('2');
	});

	it('reserves without changing the current row and rejects stale mutations', async () => {
		const first = await schedule(store, 'topic', 1, { path: 'first' });
		const next = await store.nextGeneration('topic');
		expect(await store.get('topic')).toMatchObject({ generation: first, meta: { path: 'first' } });
		expect(await store.set('topic', 2, null, first)).toBe(false);
		expect(await store.set('topic', 3, null, next)).toBe(true);
		expect(await store.delete('topic', first)).toBe(false);
		expect(await store.delete('topic', next)).toBe(true);
		expect(await store.set('topic', 4, null, next)).toBe(false);
		expect(await store.get('topic')).toBeNull();
	});

	it('validates inputs', async () => {
		await expect(store.set('', 1)).rejects.toThrow(/non-empty/);
		await expect(store.set('t', Infinity)).rejects.toThrow(/finite/);
		expect(() => createAlarmStore(client, { dueBatch: 0 })).toThrow(/positive integer/);
		expect(() => createAlarmStore(client, { table: 'bad-name!' })).toThrow(/invalid table name/);
	});
});
