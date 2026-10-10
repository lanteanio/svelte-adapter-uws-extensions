import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createDeadLetter } from '../../src/postgres/dead-letter.js';
import { createMetrics } from '../../src/prometheus/index.js';

// A focused in-memory Postgres double for exactly the queries the dead-letter
// store issues (autoMigrate:false skips DDL). It mirrors PG semantics for those
// queries (INSERT RETURNING id with the failed_at clamp, id-ordered eviction,
// db-clock TTL cutoff, count/group, jsonb round trip) so the store's
// orchestration + result mapping is verified in the default suite; real-PG
// fidelity is the job of an integration test. `nowFn` is the database clock
// (`extract(epoch from now()) * 1000`), controllable per test.
function fakePg(nowFn = () => Date.now()) {
	let rows = [];
	let nextId = 1;
	const hydrate = (r) => ({ ...r, data: r.data == null ? null : JSON.parse(r.data) });
	return {
		_rows: () => rows,
		async connect() {
			return { query: ({ text, values }) => this.query(text, values), release() {} };
		},
		async query(sql, values = []) {
			let s = sql.replace(/\s+/g, ' ').trim();
			if (s === 'BEGIN' || s === 'COMMIT' || s.includes('set_config')) return { rows: [], rowCount: 0 };
			if (s.includes('pg_try_advisory_xact_lock')) return { rows: [{ acquired: true }], rowCount: 1 };
			let visible = rows;
			if (s.startsWith('SELECT')) {
				const ttl = s.match(/failed_at >= \(extract\(epoch from now\(\)\) \* 1000\)::bigint - (\d+)/);
				if (ttl) visible = rows.filter(r => r.failed_at >= nowFn() - Number(ttl[1]));
				s = s.replace(/(?: AND| WHERE) (?:TRUE|failed_at >= \(extract\(epoch from now\(\)\) \* 1000\)::bigint - \d+)/, '');
			}
			if (s.startsWith('INSERT INTO svti_dead_letter')) {
				const id = nextId++;
				// LEAST(NULLIF($7::bigint, 0), now_ms): Postgres LEAST ignores
				// NULLs, so 0 -> now, a future stamp -> now, a past stamp -> kept.
				const v = Number(values[6]);
				const failedAt = v === 0 ? nowFn() : Math.min(v, nowFn());
				rows.push({
					svti_dead_letter_id: id,
					webhook_id: values[0], topic: values[1], event: values[2],
					data: values[3], attempts: values[4], error: values[5], failed_at: failedAt
				});
				return { rows: [{ svti_dead_letter_id: id }], rowCount: 1 };
			}
			if (s.startsWith('WITH excess AS')) {
				const max = values[0];
				const desc = rows.slice().sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id);
				const cutoffRow = desc[max]; // (max+1)-th newest, like OFFSET max LIMIT 1
				if (!cutoffRow) return { rows: [], rowCount: 0 };
				const before = rows.length;
				const victims = new Set(rows.filter(r => r.svti_dead_letter_id <= cutoffRow.svti_dead_letter_id).slice(0, values[1]));
				rows = rows.filter(r => !victims.has(r));
				return { rows: [], rowCount: before - rows.length };
			}
			if (s.startsWith('WITH expired AS')) {
				const cutoff = nowFn() - values[0];
				const before = rows.length;
				const victims = new Set(rows.filter(r => r.failed_at < cutoff).sort((a, b) => a.failed_at - b.failed_at).slice(0, values[1]));
				rows = rows.filter(r => !victims.has(r));
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
				const r = visible.find((x) => x.svti_dead_letter_id === Number(values[0]));
				return { rows: r ? [hydrate(r)] : [], rowCount: r ? 1 : 0 };
			}
			if (s.startsWith('SELECT count(*)::int AS n FROM svti_dead_letter WHERE topic = $1')) {
				return { rows: [{ n: visible.filter((r) => r.topic === values[0]).length }], rowCount: 1 };
			}
			if (s === 'SELECT count(*)::int AS n FROM svti_dead_letter') {
				return { rows: [{ n: visible.length }], rowCount: 1 };
			}
			if (s.startsWith('SELECT * FROM svti_dead_letter WHERE topic = $1 ORDER BY')) {
				const out = visible.filter((r) => r.topic === values[0]).sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id).slice(0, values[1]);
				return { rows: out.map(hydrate), rowCount: out.length };
			}
			if (s.startsWith('SELECT * FROM svti_dead_letter ORDER BY')) {
				const out = visible.slice().sort((a, b) => b.svti_dead_letter_id - a.svti_dead_letter_id).slice(0, values[0]);
				return { rows: out.map(hydrate), rowCount: out.length };
			}
			if (s.startsWith('SELECT count(*)::int AS total')) {
				const total = visible.length;
				return {
					rows: [{
						total,
						oldest: total ? Math.min(...visible.map((r) => r.failed_at)) : null,
						newest: total ? Math.max(...visible.map((r) => r.failed_at)) : null
					}],
					rowCount: 1
				};
			}
			if (s.startsWith('SELECT topic, count(*)::int AS n FROM svti_dead_letter GROUP BY topic')) {
				const map = {};
				for (const r of visible) map[r.topic] = (map[r.topic] || 0) + 1;
				return { rows: Object.entries(map).map(([topic, n]) => ({ topic, n })), rowCount: Object.keys(map).length };
			}
			throw new Error('fakePg: unhandled query: ' + s);
		}
	};
}

const rec = (over = {}) => ({
	webhookId: 'w1', topic: 'orders', event: 'created', data: { n: 1 }, attempts: 3, error: 'boom', failedAt: 100, ...over
});

afterEach(() => {
	vi.useRealTimers();
});

describe('postgres dead-letter store', () => {
	let client;
	let store;

	beforeEach(() => {
		client = fakePg();
		store = createDeadLetter(client, { autoMigrate: false, cleanupInterval: 0 });
	});
	afterEach(() => store.destroy());

	it('validates max and ttlMs', () => {
		expect(() => createDeadLetter(client, { max: 0 })).toThrow('positive integer');
		expect(() => createDeadLetter(client, { ttlMs: -1 })).toThrow('non-negative integer');
	});

	it('exposes the async interface with no options', () => {
		const s = createDeadLetter(client, { autoMigrate: false });
		for (const fn of ['add', 'get', 'remove', 'count', 'list', 'summary', 'clear']) {
			expect(typeof s[fn]).toBe('function');
		}
		s.destroy();
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

	it('drops records older than ttlMs on write, clocked by the database', async () => {
		let now = 100_000;
		client = fakePg(() => now);
		store = createDeadLetter(client, { autoMigrate: false, ttlMs: 1000, cleanupInterval: 0 });
		await store.add(rec({ failedAt: 99_000 }));
		now = 100_500; // first record is now 1500ms old
		await store.add(rec({ failedAt: 100_400 }));
		expect((await store.list()).map((r) => r.failedAt)).toEqual([100_400]);
	});
	it('bounds write cleanup and preserves the newest records across later passes', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 2, cleanupInterval: 0 });
		for (let i = 0; i < 1502; i++) await client.query('INSERT INTO svti_dead_letter', ['hook', 'orders', 'event', '{}', 1, 'failed', 100]);
		const first = await store.add(rec());
		expect(client._rows()).toHaveLength(503);
		const last = await store.add(rec());
		expect((await store.list()).map(r => r.id)).toEqual([last, first]);
	});
	it('hides expired rows from every reader while a retained backlog awaits deletion', async () => {
		let now = 100_000;
		client = fakePg(() => now);
		store = createDeadLetter(client, { autoMigrate: false, ttlMs: 1000, cleanupInterval: 0 });
		const expired = await store.add(rec({ failedAt: now, topic: 'expired' }));
		const live = await store.add(rec({ failedAt: now, topic: 'live' }));
		client._rows().find(r => String(r.svti_dead_letter_id) === expired).failed_at = 90_000;
		expect(client._rows()).toHaveLength(2);
		expect(await store.get(expired)).toBeNull();
		expect((await store.get(live)).id).toBe(live);
		expect(await store.count()).toBe(1);
		expect(await store.count({ topic: 'expired' })).toBe(0);
		expect((await store.list()).map(r => r.id)).toEqual([live]);
		expect(await store.list({ topic: 'expired' })).toEqual([]);
		expect(await store.summary()).toEqual({ total: 1, byTopic: { live: 1 }, oldest: now, newest: now });
		now += 1001;
		expect(await store.summary()).toEqual({ total: 0, byTopic: {}, oldest: null, newest: null });
	});
	it('coalesces captures committed during cleanup into one trailing pass with the timer disabled', async () => {
		vi.useFakeTimers();
		let resume, entered;
		const blocked = new Promise(resolve => { resume = resolve; });
		const reached = new Promise(resolve => { entered = resolve; });
		const query = client.query.bind(client);
		let held = false;
		client.query = async (sql, values) => {
			const result = await query(sql, values);
			if (sql.trim().startsWith('WITH excess AS') && !held) {
				held = true; entered(); await blocked;
			}
			return result;
		};
		store = createDeadLetter(client, { autoMigrate: false, max: 2, cleanupInterval: 0 });
		const leading = store.add(rec());
		await reached;
		const ids = await Promise.all(Array.from({ length: 10 }, () => store.add(rec())));
		expect(client._rows()).toHaveLength(11);
		resume(); await leading;
		await vi.advanceTimersByTimeAsync(1);
		expect((await store.list()).map(r => r.id)).toEqual(ids.slice(-2).reverse());
		expect(vi.getTimerCount()).toBe(0);
	});

	it('add() still resolves when the size-trim eviction fails (row already persisted)', async () => {
		const base = fakePg();
		const flaky = {
			...base,
			async query(sql, values) {
				const s = sql.replace(/\s+/g, ' ').trim();
				if (s.startsWith('WITH excess AS')) {
					throw new Error('deadlock detected');
				}
				return base.query(sql, values);
			}
		};
		store = createDeadLetter(flaky, { autoMigrate: false, max: 1 });
		const id = await store.add(rec({ webhookId: 'kept' })); // must NOT reject
		expect(id).toBeTruthy();
		expect((await store.get(id)).webhookId).toBe('kept'); // INSERT persisted despite the trim failure
	});

	it('add() still resolves when the TTL eviction fails (row already persisted)', async () => {
		const base = fakePg();
		const flaky = {
			...base,
			async query(sql, values) {
				const s = sql.replace(/\s+/g, ' ').trim();
				if (s.startsWith('WITH expired AS')) {
					throw new Error('statement timeout');
				}
				return base.query(sql, values);
			}
		};
		store = createDeadLetter(flaky, { autoMigrate: false, ttlMs: 1000, cleanupInterval: 0 });
		const id = await store.add(rec({ webhookId: 'kept2', failedAt: Date.now() }));
		expect(id).toBeTruthy();
		expect((await store.get(id)).webhookId).toBe('kept2');
	});

	it('a future producer stamp cannot mass-evict healthy records (db clock rules)', async () => {
		let now = 100_000;
		client = fakePg(() => now);
		store = createDeadLetter(client, { autoMigrate: false, ttlMs: 10_000, cleanupInterval: 0 });
		await store.add(rec({ webhookId: 'healthy', failedAt: 99_000 }));
		// A skewed producer stamps far ahead. Under a stamp-clocked cutoff this
		// would sweep everything older than future-ttl, i.e. the healthy record.
		await store.add(rec({ webhookId: 'skewed', failedAt: 10_000_000 }));
		expect((await store.list()).map((r) => r.webhookId).sort()).toEqual(['healthy', 'skewed']);
	});

	it('clamps an implausible failedAt to the database clock', async () => {
		client = fakePg(() => 50_000);
		store = createDeadLetter(client, { autoMigrate: false });
		const idFuture = await store.add(rec({ failedAt: 99_999_999 }));
		const idMissing = await store.add(rec({ failedAt: undefined }));
		const idZero = await store.add(rec({ failedAt: 0 }));
		for (const id of [idFuture, idMissing, idZero]) {
			expect((await store.get(id)).failedAt).toBe(50_000);
		}
		const idPast = await store.add(rec({ failedAt: 42_000 }));
		expect((await store.get(idPast)).failedAt).toBe(42_000);
	});

	it('the periodic sweep clears an idle queue, and destroy() stops it', async () => {
		vi.useFakeTimers();
		let now = 100_000;
		client = fakePg(() => now);
		store = createDeadLetter(client, { autoMigrate: false, ttlMs: 1000, cleanupInterval: 500 });
		await store.add(rec({ failedAt: 100_000 }));
		expect(await store.count()).toBe(1);

		now = 102_000; // the record ages past ttlMs with no further writes
		await vi.advanceTimersByTimeAsync(500);
		expect(await store.count()).toBe(0); // swept by the timer, not by an add

		store.destroy();
		await store.add(rec({ failedAt: 102_000 }));
		now = 200_000;
		await vi.advanceTimersByTimeAsync(5_000);
		expect(client._rows()).toHaveLength(1); // destroyed -> no physical sweep runs
		expect(await store.count()).toBe(0); // logical expiry still applies
	});
});


it('keeps dead-letter topics private when a registry omits the mapper', async () => {
	const metrics = createMetrics();
	const store = createDeadLetter(fakePg(), { autoMigrate: false, cleanupInterval: 0, metrics: { ...metrics, mapTopic: undefined } });
	await store.add({ webhookId: 'webhook', topic: 'alice@example.com', event: 'event', data: {}, attempts: 1, error: 'offline', failedAt: 1 });
	expect(metrics.serialize()).toContain('dead_letter_added_total{topic="all"} 1');
	expect(metrics.serialize()).not.toContain('alice@example.com');
});
