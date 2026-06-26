import { describe, it, expect, beforeEach } from 'vitest';
import { createDeadLetter } from '../../src/postgres/dead-letter.js';

// A focused in-memory Postgres double for exactly the queries the dead-letter
// store issues (autoMigrate:false skips DDL). It mirrors PG semantics for those
// queries (INSERT RETURNING id, id-ordered eviction, count/group, jsonb round
// trip) so the store's orchestration + result mapping is verified in the default
// suite; real-PG fidelity is the job of an integration test.
function fakePg() {
	let rows = [];
	let nextId = 1;
	const hydrate = (r) => ({ ...r, data: r.data == null ? null : JSON.parse(r.data) });
	return {
		async query(sql, values = []) {
			const s = sql.replace(/\s+/g, ' ').trim();
			if (s.startsWith('INSERT INTO svti_dead_letter')) {
				const id = nextId++;
				rows.push({
					svti_dead_letter_id: id,
					webhook_id: values[0], topic: values[1], event: values[2],
					data: values[3], attempts: values[4], error: values[5], failed_at: values[6]
				});
				return { rows: [{ svti_dead_letter_id: id }], rowCount: 1 };
			}
			if (s.startsWith('DELETE FROM svti_dead_letter WHERE svti_dead_letter_id <= (SELECT')) {
				const max = values[0];
				const desc = rows.slice().sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id);
				const cutoffRow = desc[max]; // (max+1)-th newest, like OFFSET max LIMIT 1
				if (!cutoffRow) return { rows: [], rowCount: 0 };
				const before = rows.length;
				rows = rows.filter((r) => r.svti_dead_letter_id > cutoffRow.svti_dead_letter_id);
				return { rows: [], rowCount: before - rows.length };
			}
			if (s === 'DELETE FROM svti_dead_letter WHERE failed_at < $1') {
				const before = rows.length;
				rows = rows.filter((r) => r.failed_at >= values[0]);
				return { rows: [], rowCount: before - rows.length };
			}
			if (s === 'DELETE FROM svti_dead_letter WHERE svti_dead_letter_id = $1') {
				const id = Number(values[0]);
				const before = rows.length;
				rows = rows.filter((r) => r.svti_dead_letter_id !== id);
				return { rows: [], rowCount: before - rows.length };
			}
			if (s === 'DELETE FROM svti_dead_letter') {
				const n = rows.length; rows = []; return { rows: [], rowCount: n };
			}
			if (s === 'SELECT * FROM svti_dead_letter WHERE svti_dead_letter_id = $1') {
				const r = rows.find((x) => x.svti_dead_letter_id === Number(values[0]));
				return { rows: r ? [hydrate(r)] : [], rowCount: r ? 1 : 0 };
			}
			if (s.startsWith('SELECT count(*)::int AS n FROM svti_dead_letter WHERE topic = $1')) {
				return { rows: [{ n: rows.filter((r) => r.topic === values[0]).length }], rowCount: 1 };
			}
			if (s === 'SELECT count(*)::int AS n FROM svti_dead_letter') {
				return { rows: [{ n: rows.length }], rowCount: 1 };
			}
			if (s.startsWith('SELECT * FROM svti_dead_letter WHERE topic = $1 ORDER BY')) {
				const out = rows.filter((r) => r.topic === values[0]).sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id).slice(0, values[1]);
				return { rows: out.map(hydrate), rowCount: out.length };
			}
			if (s.startsWith('SELECT * FROM svti_dead_letter ORDER BY')) {
				const out = rows.slice().sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id).slice(0, values[0]);
				return { rows: out.map(hydrate), rowCount: out.length };
			}
			if (s.startsWith('SELECT count(*)::int AS total')) {
				const total = rows.length;
				return {
					rows: [{
						total,
						oldest: total ? Math.min(...rows.map((r) => r.failed_at)) : null,
						newest: total ? Math.max(...rows.map((r) => r.failed_at)) : null
					}],
					rowCount: 1
				};
			}
			if (s.startsWith('SELECT topic, count(*)::int AS n FROM svti_dead_letter GROUP BY topic')) {
				const map = {};
				for (const r of rows) map[r.topic] = (map[r.topic] || 0) + 1;
				return { rows: Object.entries(map).map(([topic, n]) => ({ topic, n })), rowCount: Object.keys(map).length };
			}
			throw new Error('fakePg: unhandled query: ' + s);
		}
	};
}

const rec = (over = {}) => ({
	webhookId: 'w1', topic: 'orders', event: 'created', data: { n: 1 }, attempts: 3, error: 'boom', failedAt: 100, ...over
});

describe('postgres dead-letter store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = fakePg();
		store = createDeadLetter(client, { autoMigrate: false });
	});

	it('validates max and ttlMs', () => {
		expect(() => createDeadLetter(client, { max: 0 })).toThrow('positive integer');
		expect(() => createDeadLetter(client, { ttlMs: -1 })).toThrow('non-negative integer');
	});

	it('exposes the async interface with no options', () => {
		const s = createDeadLetter(client, { autoMigrate: false });
		for (const fn of ['add', 'get', 'remove', 'count', 'list', 'summary', 'clear']) {
			expect(typeof s[fn]).toBe('function');
		}
	});

	it('adds and retrieves (id as string, jsonb data round-trips)', async () => {
		const id = await store.add(rec({ data: { n: 7 } }));
		expect(typeof id).toBe('string');
		const got = await store.get(id);
		expect(got).toMatchObject({ id, webhookId: 'w1', topic: 'orders', event: 'created', attempts: 3 });
		expect(got.data).toEqual({ n: 7 });
		expect(got.failedAt).toBe(100);
		expect(await store.count()).toBe(1);
	});

	it('lists newest-first and filters by topic', async () => {
		await store.add(rec({ topic: 'a', failedAt: 10 }));
		await store.add(rec({ topic: 'a', failedAt: 20 }));
		await store.add(rec({ topic: 'b', failedAt: 30 }));
		expect((await store.list()).map((r) => r.failedAt)).toEqual([30, 20, 10]);
		expect(await store.count({ topic: 'a' })).toBe(2);
		expect(await store.list({ topic: 'b' })).toHaveLength(1);
		expect(await store.list({ limit: 1 })).toHaveLength(1);
	});

	it('removes a record', async () => {
		const id = await store.add(rec());
		expect(await store.remove(id)).toBe(true);
		expect(await store.get(id)).toBeNull();
		expect(await store.remove('999')).toBe(false);
	});

	it('summarizes total / byTopic / oldest / newest', async () => {
		await store.add(rec({ topic: 'a', failedAt: 10 }));
		await store.add(rec({ topic: 'a', failedAt: 20 }));
		await store.add(rec({ topic: 'b', failedAt: 30 }));
		expect(await store.summary()).toEqual({ total: 3, byTopic: { a: 2, b: 1 }, oldest: 10, newest: 30 });
		await store.clear();
		expect(await store.summary()).toEqual({ total: 0, byTopic: {}, oldest: null, newest: null });
	});

	it('evicts the oldest beyond max on write', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 2 });
		await store.add(rec({ failedAt: 1 }));
		await store.add(rec({ failedAt: 2 }));
		await store.add(rec({ failedAt: 3 }));
		expect(await store.count()).toBe(2);
		expect((await store.list()).map((r) => r.failedAt)).toEqual([3, 2]);
	});

	it('drops records older than ttlMs on write', async () => {
		store = createDeadLetter(client, { autoMigrate: false, ttlMs: 1000 });
		await store.add(rec({ failedAt: 100 }));
		await store.add(rec({ failedAt: 5000 })); // ref 5000, cutoff 4000 -> drops failedAt=100
		expect((await store.list()).map((r) => r.failedAt)).toEqual([5000]);
	});
});
