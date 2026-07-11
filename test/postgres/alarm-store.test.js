import { describe, it, expect, beforeEach } from 'vitest';
import { createAlarmStore } from '../../src/postgres/alarm-store.js';

// A focused in-memory Postgres double implementing exactly the statements the
// alarm store issues (DDL no-op, INSERT ... ON CONFLICT, DELETE ... RETURNING,
// SELECT ... WHERE fire_at <= ORDER BY LIMIT, and the unqualified clear DELETE).
// The store uses no transactions / advisory locks / SKIP LOCKED, so this models
// its full behaviour; the shared mock-pg keyspace is reserved for the lock-heavy
// plugins (tasks/jobs/idempotency). node-pg returns BIGINT as a string, which the
// double mirrors so the store's Number() parse is exercised.
function fakePg() {
	const rows = new Map(); // topic -> { fire_at, meta, tenant }
	return {
		rows,
		query: async (q) => {
			const text = (typeof q === 'string' ? q : q.text).replace(/\s+/g, ' ').trim();
			const values = (typeof q === 'string' ? [] : q.values) || [];
			if (text.startsWith('CREATE TABLE') || text.startsWith('CREATE INDEX')) return { rows: [], rowCount: 0 };
			// pg-migrate's drift guard now verifies columns after every ensureTable;
			// the real table has exactly these, so the double reports them.
			if (text.includes('information_schema.columns')) {
				return { rows: ['topic', 'fire_at', 'meta', 'tenant'].map((column_name) => ({ column_name })), rowCount: 4 };
			}
			if (text.startsWith('INSERT INTO svti_alarms')) {
				const [topic, fireAt, metaJson, tenant] = values;
				rows.set(topic, { fire_at: Number(fireAt), meta: metaJson == null ? null : JSON.parse(metaJson), tenant: tenant ?? null });
				return { rows: [], rowCount: 1 };
			}
			if (text.startsWith('DELETE FROM svti_alarms WHERE topic')) {
				const had = rows.delete(values[0]);
				return { rows: had ? [{ topic: values[0] }] : [], rowCount: had ? 1 : 0 };
			}
			if (text.startsWith('DELETE FROM svti_alarms')) { rows.clear(); return { rows: [], rowCount: 0 }; }
			if (text.startsWith('SELECT topic, fire_at, meta FROM svti_alarms')) {
				const now = Number(values[0]);
				const limit = Number(values[1]);
				const due = [...rows.entries()]
					.filter(([, r]) => r.fire_at <= now)
					.sort((a, b) => a[1].fire_at - b[1].fire_at)
					.slice(0, limit)
					.map(([topic, r]) => ({ topic, fire_at: String(r.fire_at), meta: r.meta })); // BIGINT -> string
				return { rows: due, rowCount: due.length };
			}
			return { rows: [], rowCount: 0 };
		}
	};
}

describe('postgres durable alarm store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = fakePg();
		store = createAlarmStore(client);
	});

	it('persists and returns a due alarm (BIGINT fire_at parsed back to a number)', async () => {
		await store.set('room:1', 1000, { path: 'rooms/x', tenantId: 'acme' });
		expect(await store.due(999)).toEqual([]);
		expect(await store.due(1000)).toEqual([{ topic: 'room:1', at: 1000, meta: { path: 'rooms/x', tenantId: 'acme' } }]);
	});

	it('delete() is the atomic claim: true once, then false', async () => {
		await store.set('room:c', 1000, { path: 'p' });
		expect(await store.delete('room:c')).toBe(true);
		expect(await store.delete('room:c')).toBe(false);
	});

	it('set() upserts (one alarm per topic)', async () => {
		await store.set('room:r', 1000, { path: 'p' });
		await store.set('room:r', 5000, { path: 'p2' });
		expect(await store.due(2000)).toEqual([]); // moved out to 5000
		expect(await store.due(5000)).toEqual([{ topic: 'room:r', at: 5000, meta: { path: 'p2' } }]);
	});

	it('mirrors tenantId into the tenant column for ops queries', async () => {
		await store.set('room:t', 1000, { path: 'p', tenantId: 'acme' });
		expect(client.rows.get('room:t').tenant).toBe('acme');
	});

	it('due() honors the dueBatch cap and fire_at ordering', async () => {
		const s = createAlarmStore(client, { dueBatch: 2 });
		await s.set('c', 3, null);
		await s.set('a', 1, null);
		await s.set('b', 2, null);
		const due = await s.due(100);
		expect(due.map((r) => r.topic)).toEqual(['a', 'b']); // ordered by fire_at, capped at 2
	});

	it('clear() removes everything', async () => {
		await store.set('x', 1, null);
		await store.set('y', 2, null);
		await store.clear();
		expect(await store.due(100)).toEqual([]);
	});

	it('validates inputs', async () => {
		await expect(store.set('', 1)).rejects.toThrow(/non-empty/);
		await expect(store.set('t', Infinity)).rejects.toThrow(/finite/);
		expect(() => createAlarmStore(client, { dueBatch: 0 })).toThrow(/positive integer/);
		expect(() => createAlarmStore(client, { table: 'bad-name!' })).toThrow(/invalid table name/);
	});
});
