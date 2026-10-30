import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createDeadLetter } from '../../src/postgres/dead-letter.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { monotonicNow, resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';

// A focused in-memory Postgres double for exactly the queries the dead-letter
// store issues (autoMigrate:false skips DDL). It mirrors PG semantics for those
// queries (INSERT RETURNING id with the failed_at clamp, id-ordered eviction,
// db-clock TTL cutoff, count/group, jsonb round trip) so the store's
// orchestration + result mapping is verified in the default suite; real-PG
// fidelity is the job of an integration test. `nowFn` is the database clock
// (`extract(epoch from now()) * 1000`), controllable per test.
function fakePg(nowFn = () => Date.now()) {
	let rows = [];
	let purges = [];
	let nextId = 1;
	let drift = false;
	const hydrate = (r) => ({ ...r, data: r.data == null ? null : JSON.parse(r.data) });
	// node-postgres sends a number as String(n), and int8 input refuses a
	// fraction (SQLSTATE 22P02), so a bigint parameter must be a whole number.
	function int8(value) {
		if (!/^-?\d+$/.test(String(value))) {
			throw Object.assign(new Error(`invalid input syntax for type bigint: "${value}"`), { code: '22P02' });
		}
		return value;
	}
	// LEAST(NULLIF($7::bigint, 0), dbNow): Postgres LEAST ignores NULLs, so
	// 0 -> now, a future stamp -> now, a past stamp -> kept.
	function insert(values, dbNow) {
		const id = nextId++;
		const v = Number(values[6]);
		rows.push({
			svti_dead_letter_id: id,
			webhook_id: values[0], topic: values[1], event: values[2],
			data: values[3], attempts: values[4], error: values[5],
			failed_at: v === 0 ? dbNow : Math.min(v, dbNow),
			user_id: values[7] ?? null,
			// The column default covers a row written without a scope.
			scope: values[8] ?? ''
		});
		return { rows: [{ svti_dead_letter_id: id }], rowCount: 1 };
	}
	// Only `NOT EXISTS (SELECT 1 FROM <fences> p WHERE <c> AND <c> ...)` with
	// conditions `p.<col> = $n` or `p.<col> >= $n[::type][ - k]` is modelled;
	// anything else throws instead of passing silently.
	function capturePredicate(where, values) {
		const m = /^NOT EXISTS \( SELECT 1 FROM svti_dead_letter_purges p WHERE (.+) \)$/.exec(where);
		if (!m) throw new Error('fakePg: unmodelled capture predicate: ' + where);
		const conditions = m[1].split(' AND ').map((condition) => {
			const c = /^p\.(scope|user_id|purged_at) (=|>=) \$(\d+)(?:::(?:bigint|text))?(?: - (\d+))?$/.exec(condition);
			if (!c) throw new Error('fakePg: unmodelled fence condition: ' + condition);
			const [, column, op, index, minus] = c;
			const bound = values[Number(index) - 1];
			if (op === '=') return (p) => bound != null && p[column] === bound;
			const floor = Number(bound) - Number(minus ?? 0);
			return (p) => bound != null && p[column] >= floor;
		});
		return !purges.some((p) => conditions.every((condition) => condition(p)));
	}
	return {
		_rows: () => rows,
		_purges: () => purges,
		_drift: () => { drift = true; },
		async connect() {
			return {
				query: (sql, values) => this.query(sql.text ?? sql, sql.values ?? values),
				release() {}
			};
		},
		async query(sql, values = []) {
			let s = sql.replace(/\s+/g, ' ').trim();
			if (s === 'BEGIN' || s === 'BEGIN ISOLATION LEVEL READ COMMITTED' || s === 'COMMIT' || s === 'ROLLBACK' || s.includes('set_config')) return { rows: [], rowCount: 0 };
			if (s.includes('pg_try_advisory_xact_lock')) return { rows: [{ acquired: true }], rowCount: 1 };
			// The erasure lock: the clock is read by the outer query, after the
			// locking subquery, so it postdates the lock wait.
			if (s.includes('pg_advisory_xact_lock')) {
				if (s !== "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint AS db_now FROM (SELECT pg_advisory_xact_lock(hashtextextended($1::regclass::oid::text || ':' || $2 || ':' || $3, 0))) AS locked") {
					throw new Error('fakePg: unmodelled erasure lock: ' + s);
				}
				// `::bigint` output: whole milliseconds.
				return { rows: [{ db_now: String(Math.floor(nowFn())) }], rowCount: 1 };
			}
			let visible = rows;
			if (s.startsWith('SELECT')) {
				const ttl = s.match(/failed_at >= \(extract\(epoch from now\(\)\) \* 1000\)::bigint - (\d+)/);
				if (ttl) visible = rows.filter(r => r.failed_at >= nowFn() - Number(ttl[1]));
				s = s.replace(/(?: AND| WHERE) (?:TRUE|failed_at >= \(extract\(epoch from now\(\)\) \* 1000\)::bigint - \d+)/, '');
			}
			if (s.startsWith('INSERT INTO svti_dead_letter_purges')) {
				if (!s.endsWith('SET purged_at = GREATEST(svti_dead_letter_purges.purged_at, EXCLUDED.purged_at)')) {
					throw new Error('fakePg: unmodelled fence write: ' + s);
				}
				const at = Number(int8(values[2]));
				const found = purges.find(p => p.scope === values[0] && p.user_id === values[1]);
				if (found) found.purged_at = Math.max(found.purged_at, at);
				else purges.push({ scope: values[0], user_id: values[1], purged_at: at });
				return { rows: [], rowCount: 1 };
			}
			// The fenced capture, INSERT ... SELECT ... WHERE <predicate>. The
			// predicate is evaluated from the SQL text against the fence rows, so
			// a capture statement without a working fence keeps the payload.
			const fenced = /^INSERT INTO svti_dead_letter \(webhook_id, topic, event, data, attempts, error, failed_at, user_id, scope\) SELECT \$1::text, \$2::text, \$3::text, \$4::jsonb, \$5::integer, \$6::text, LEAST\(NULLIF\(\$7::bigint, 0\), \$(\d+)::bigint\), \$8::text, \$9::text WHERE (.+) RETURNING svti_dead_letter_id$/.exec(s);
			if (fenced) {
				int8(values[6]); int8(values[9]); int8(values[10]);
				if (!capturePredicate(fenced[2], values)) return { rows: [], rowCount: 0 };
				return insert(values, Number(values[Number(fenced[1]) - 1]));
			}
			// Seeding shorthand some tests use to fill the table directly.
			if (s === 'INSERT INTO svti_dead_letter') return insert(values, nowFn());
			if (s === 'INSERT INTO svti_dead_letter (webhook_id, topic, event, data, attempts, error, failed_at, user_id, scope) VALUES ($1, $2, $3, $4, $5, $6, LEAST(NULLIF($7::bigint, 0), (extract(epoch from now()) * 1000)::bigint), $8, $9) RETURNING svti_dead_letter_id') {
				return insert(values, nowFn());
			}
			// The per-scope counts the triggers keep equal the rows grouped by
			// scope, with the lowest id as `oldest`; ranked by count, then the
			// oldest record, then the scope name.
			const scopeCounts = () => {
				const counts = new Map();
				for (const r of rows) {
					const entry = counts.get(r.scope) ?? { scope: r.scope, records: 0, oldest: Infinity };
					entry.records++;
					entry.oldest = Math.min(entry.oldest, r.svti_dead_letter_id);
					counts.set(r.scope, entry);
				}
				return [...counts.values()].sort((a, b) => b.records - a.records || a.oldest - b.oldest || (a.scope < b.scope ? -1 : a.scope > b.scope ? 1 : 0));
			};
			// The fair-share size trim reads the largest scope, bigints as strings.
			if (s === 'SELECT scope, records, (sum(records) OVER ())::bigint - $1::bigint AS excess, lead(records) OVER (ORDER BY records DESC, oldest, scope) AS runner_up FROM svti_dead_letter_scopes ORDER BY records DESC, oldest, scope LIMIT 1') {
				int8(values[0]);
				const [heaviest, runnerUp] = scopeCounts();
				if (!heaviest) return { rows: [], rowCount: 0 };
				return { rows: [{ scope: heaviest.scope, records: String(heaviest.records), excess: String(rows.length - Number(values[0])), runner_up: runnerUp ? String(runnerUp.records) : null }], rowCount: 1 };
			}
			// ...then deletes one scope's oldest ids, the count bound as a value...
			if (s === 'WITH victims AS ( SELECT svti_dead_letter_id FROM svti_dead_letter WHERE scope = $1 ORDER BY svti_dead_letter_id LIMIT $2 FOR UPDATE SKIP LOCKED ) DELETE FROM svti_dead_letter d USING victims WHERE d.svti_dead_letter_id = victims.svti_dead_letter_id') {
				int8(values[1]);
				const victims = new Set(rows.filter(r => r.scope === values[0]).sort((a, b) => a.svti_dead_letter_id - b.svti_dead_letter_id).slice(0, Number(values[1])));
				rows = rows.filter(r => !victims.has(r));
				return { rows: [], rowCount: victims.size };
			}
			// ...or the oldest record of each of the equally large scopes holding
			// the oldest records.
			if (s === 'WITH victims AS ( SELECT svti_dead_letter_id FROM svti_dead_letter WHERE svti_dead_letter_id IN ( SELECT (SELECT min(r.svti_dead_letter_id) FROM svti_dead_letter r WHERE r.scope = c.scope) FROM svti_dead_letter_scopes c WHERE c.records = $1 ORDER BY c.oldest, c.scope LIMIT $2 ) FOR UPDATE SKIP LOCKED ) DELETE FROM svti_dead_letter d USING victims WHERE d.svti_dead_letter_id = victims.svti_dead_letter_id') {
				int8(values[0]); int8(values[1]);
				const oldest = new Set(scopeCounts().filter(c => c.records === Number(values[0])).slice(0, Number(values[1])).map(c => c.oldest));
				const before = rows.length;
				rows = rows.filter(r => !oldest.has(r.svti_dead_letter_id));
				return { rows: [], rowCount: before - rows.length };
			}
			// ready() compares the counts with the rows; `_drift` stands in for
			// counts a write around the triggers left behind.
			if (s.startsWith('SELECT EXISTS ( SELECT 1 FROM (SELECT scope, count(*) AS n FROM svti_dead_letter GROUP BY scope) r FULL JOIN svti_dead_letter_scopes c USING (scope)')) {
				return { rows: [{ drifted: drift }], rowCount: 1 };
			}
			if (s === 'LOCK TABLE svti_dead_letter_scopes IN EXCLUSIVE MODE' || s === 'DELETE FROM svti_dead_letter_scopes') return { rows: [], rowCount: 0 };
			if (s === 'INSERT INTO svti_dead_letter_scopes (scope, records, oldest) SELECT scope, count(*), min(svti_dead_letter_id) FROM svti_dead_letter GROUP BY scope') {
				drift = false;
				return { rows: [], rowCount: scopeCounts().length };
			}
			if (s.startsWith('WITH expired AS') && s.includes('FROM svti_dead_letter_purges')) {
				const cutoff = nowFn() - values[0];
				const before = purges.length;
				purges = purges.filter(p => p.purged_at >= cutoff);
				return { rows: [], rowCount: before - purges.length };
			}
			if (s.startsWith('WITH expired AS')) {
				const cutoff = nowFn() - values[0];
				const before = rows.length;
				const victims = new Set(rows.filter(r => r.failed_at < cutoff).sort((a, b) => a.failed_at - b.failed_at).slice(0, values[1]));
				rows = rows.filter(r => !victims.has(r));
				return { rows: [], rowCount: before - rows.length };
			}
			if (s.startsWith('DELETE FROM svti_dead_letter WHERE user_id = $1')) {
				const scopedEq = s.includes('left(topic, char_length($2)) = $2');
				const before = rows.length;
				rows = rows.filter(r => r.user_id !== values[0]
					|| (scopedEq ? !r.topic.startsWith(values[1]) : r.topic.startsWith(values[1])));
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

function aliasedPg(base, table, coordination) {
	const releases = [];
	async function query(sql, values) {
		const text = sql.text ?? sql;
		values = sql.values ?? values;
		if (text === 'COMMIT' || text === 'ROLLBACK') {
			const result = await base.query(text, values);
			for (const release of releases.splice(0)) release();
			return result;
		}
		if (text.includes('pg_advisory_xact_lock(hashtextextended') && !text.includes('pg_try_advisory_xact_lock')) {
			// `$1::regclass` resolves an unquoted name case-insensitively, so the
			// lock identity is the folded relation name.
			const identity = values[0].toLowerCase();
			coordination.identities.push(identity);
			releases.push(await coordination.acquire(`${identity}:${values[1]}:${values[2]}`));
		}
		if (text.startsWith('INSERT INTO') && text.includes('NOT EXISTS') && coordination.pauseCapture) {
			coordination.captureEntered();
			await coordination.captureGate;
		}
		return base.query(
			text.replaceAll(`${table}_purges`, 'svti_dead_letter_purges').replaceAll(table, 'svti_dead_letter'),
			values
		);
	}
	return {
		...base,
		query,
		async connect() { return { query, release() {} }; }
	};
}

const rec = (over = {}) => ({
	webhookId: 'w1', topic: 'orders', event: 'created', data: { n: 1 }, attempts: 3, error: 'boom', failedAt: 100, ...over
});

afterEach(() => {
	vi.useRealTimers();
	resetRuntimeEnv();
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
		// The seeded rows carry no scope, so they and the captures each keep
		// their newest record under the shared cap.
		expect((await store.list()).map(r => r.id)).toEqual([last, String(Number(first) - 1)]);
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
			if (sql.trim().startsWith('SELECT scope, records,') && !held) {
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
				if (s.startsWith('SELECT scope, records,')) {
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

describe('postgres dead-letter fair-share eviction', () => {
	let client, store;
	beforeEach(() => { client = fakePg(); });
	afterEach(() => store?.destroy());

	it('keeps every record of a tenant under its share while another tenant overflows', async () => {
		const metrics = createMetrics();
		store = createDeadLetter(client, { autoMigrate: false, max: 10, cleanupInterval: 0, metrics });
		for (let n = 0; n < 5; n++) await store.add(rec({ topic: '@t/victim/orders', data: { n } }));
		for (let n = 0; n < 10; n++) await store.add(rec({ topic: '@t/noisy/orders', data: { n } }));
		expect(await store.count()).toBe(10);
		expect(await store.count({ topic: '@t/victim/orders' })).toBe(5);
		// The noisy tenant lost its own oldest records.
		expect((await store.list({ topic: '@t/noisy/orders' })).map(r => r.data.n)).toEqual([9, 8, 7, 6, 5]);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 5');
		for (let n = 10; n < 30; n++) await store.add(rec({ topic: '@t/noisy/orders', data: { n } }));
		expect(await store.count({ topic: '@t/victim/orders' })).toBe(5);
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 25');
	});

	it('breaks a tie between equally large scopes on the oldest record, whatever the tenant names', async () => {
		for (const order of [['a', 'b', 'z'], ['z', 'b', 'a']]) {
			client = fakePg();
			store?.destroy();
			store = createDeadLetter(client, { autoMigrate: false, max: 2, cleanupInterval: 0 });
			const ids = [];
			for (const tenant of order) ids.push(await store.add(rec({ topic: `@t/${tenant}/x` })));
			// The capture just made survives; the oldest record's tenant pays.
			expect(await store.get(ids[2])).not.toBeNull();
			expect((await store.list()).map(r => r.topic).sort()).toEqual(order.slice(1).map(t => `@t/${t}/x`).sort());
		}
	});

	it('stops a scope at the runner-up and lets the tie rule pick the next record', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 3, cleanupInterval: 0 });
		for (const tenant of ['b', 'b', 'a', 'a']) await client.query('INSERT INTO svti_dead_letter', ['hook', `@t/${tenant}/x`, 'event', '{}', 1, 'failed', 100, null, tenant]);
		const last = await store.add(rec({ topic: '@t/a/x' }));
		// a (3) gives up one record to reach b (2); then b holds the oldest.
		expect(client._rows().map(r => String(r.svti_dead_letter_id))).toEqual(['2', '4', last]);
	});

	it('drains equally large scopes in one statement per level, oldest records first', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 4, cleanupInterval: 0 });
		for (const tenant of ['c', 'a', 'd', 'b']) for (let n = 0; n < 2; n++) await client.query('INSERT INTO svti_dead_letter', ['hook', `@t/${tenant}/x`, 'event', JSON.stringify({ n }), 1, 'failed', 100, null, tenant]);
		const deletes = [];
		const query = client.query.bind(client);
		client.query = async (sql, values) => {
			const text = String(sql.text ?? sql).trim();
			if (text.startsWith('WITH victims AS')) deletes.push(values);
			return query(sql, values);
		};
		await store.add(rec({ topic: '@t/e/x' }));
		// Nine records over a cap of four: the four scopes at two each give up
		// their oldest record in one statement, then the oldest single record goes.
		expect(deletes).toEqual([[2, 5], [1, 1]]);
		expect(client._rows().map(r => r.topic + ':' + r.data).sort()).toEqual(['@t/a/x:{"n":1}', '@t/b/x:{"n":1}', '@t/d/x:{"n":1}', '@t/e/x:{"n":1}']);
	});

	it('stores the tenant scope of each topic and evicts the largest scope down to the others', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 5, cleanupInterval: 0 });
		await store.add(rec({ topic: 'orders', data: { n: 0 } }));
		await store.add(rec({ topic: '@t/a/orders', data: { n: 0 } }));
		await store.add(rec({ topic: '@t/', data: { n: 0 } }));
		for (let n = 0; n < 6; n++) await store.add(rec({ topic: '@t/b/orders', data: { n } }));
		// Untenanted topics share '~', a malformed tenant topic counts in ''.
		expect([...new Set(client._rows().map(r => r.scope))].sort()).toEqual(['', 'a', 'b', '~']);
		expect(client._rows().map(r => r.topic + ':' + r.data).sort()).toEqual([
			'@t/:{"n":0}', '@t/a/orders:{"n":0}', '@t/b/orders:{"n":4}', '@t/b/orders:{"n":5}', 'orders:{"n":0}'
		]);
	});

	it('reads the counts once and deletes once per capture over the cap, and never deletes under it', async () => {
		store = createDeadLetter(client, { autoMigrate: false, max: 3, cleanupInterval: 0 });
		const query = client.query.bind(client);
		const issued = [];
		client.query = async (sql, values) => {
			const text = String(sql.text ?? sql).trim();
			if (text.startsWith('SELECT scope, records,')) issued.push('counts');
			if (text.startsWith('WITH victims AS')) issued.push(['delete', ...values]);
			return query(sql, values);
		};
		for (let n = 0; n < 3; n++) await store.add(rec({ topic: '@t/noisy/orders', data: { n } }));
		expect(issued).toEqual(['counts', 'counts', 'counts']);
		issued.length = 0;
		await store.add(rec({ topic: '@t/noisy/orders', data: { n: 3 } }));
		// The scope and the row count are bound values, never subqueries.
		expect(issued).toEqual(['counts', ['delete', 'noisy', 1]]);
		expect(await store.count()).toBe(3);
	});

	it('counts evictions only from a committed cleanup pass', async () => {
		const metrics = createMetrics();
		store = createDeadLetter(client, { autoMigrate: false, max: 1, cleanupInterval: 0, metrics });
		await store.add(rec({ topic: '@t/a/orders' }));
		const query = client.query.bind(client);
		// Fail the commit of the first cleanup pass that evicted.
		let evicting = false, failed = false;
		client.query = async (sql, values) => {
			const text = String(sql.text ?? sql).trim();
			if (text.startsWith('WITH victims AS')) evicting = true;
			if (evicting && !failed && text === 'COMMIT') { failed = true; throw new Error('connection lost'); }
			return query(sql, values);
		};
		expect(await store.add(rec({ topic: '@t/a/orders' }))).not.toBeNull();
		expect(metrics.serialize()).not.toMatch(/^dead_letter_evicted_total /m);
		await store.add(rec({ topic: '@t/a/orders' }));
		expect(metrics.serialize()).toContain('dead_letter_evicted_total 1');
	});
});

describe('postgres dead-letter erasure fences', () => {
	let client, store, clockMs;
	const byAuthor = record => record.data?.author;
	const fencedRecord = (over = {}) => rec({ data: { author: 'u1', private: true }, ...over });

	beforeEach(() => {
		// Fractional like the real monotonic clock (epoch + performance.now());
		// the database side reads whole milliseconds.
		clockMs = 1_000_000.375;
		setRuntimeEnv({ clock: { now: () => clockMs, monotonic: () => clockMs, wallEpoch: () => clockMs } });
		client = fakePg(() => clockMs);
		store = createDeadLetter(client, { autoMigrate: false, cleanupInterval: 0, forgetUserId: byAuthor });
	});
	afterEach(() => store.destroy());

	it('drops an old racing capture and preserves new deliveries and other tenants', async () => {
		const startedAt = monotonicNow();
		// Fractional steps make the delivery duration fractional, as it is on
		// the real clock; the start it yields is bound as a bigint.
		clockMs += 5_000.25;
		expect(await store.purgeUser('acme', 'u1')).toBe(0);
		clockMs += 5_000.25;

		expect(await store.add(fencedRecord({ topic: '@t/acme/orders', startedAt }))).toBeNull();
		const otherTenant = await store.add(fencedRecord({ topic: '@t/globex/orders', startedAt }));
		expect(otherTenant).not.toBeNull();
		const newStart = monotonicNow();
		clockMs += 1_000;
		const newDelivery = await store.add(fencedRecord({ topic: '@t/acme/orders', startedAt: newStart }));
		expect(newDelivery).not.toBeNull();
		expect((await store.list()).map(r => r.id).sort()).toEqual([otherTenant, newDelivery].sort());
	});

	it('writes the tombstone and deletes records in one locked transaction', async () => {
		await store.add(fencedRecord({ topic: '@t/acme/orders' }));
		const statements = [];
		const query = client.query.bind(client);
		client.query = async (sql, values) => {
			statements.push(sql.replace(/\s+/g, ' ').trim());
			return query(sql, values);
		};

		expect(await store.purgeUser('acme', 'u1')).toBe(1);
		expect(await store.count()).toBe(0);
		const begin = statements.indexOf('BEGIN ISOLATION LEVEL READ COMMITTED');
		const lock = statements.findIndex(s => s.includes('pg_advisory_xact_lock'));
		const tombstone = statements.findIndex(s => s.startsWith('INSERT INTO svti_dead_letter_purges'));
		const erase = statements.findIndex(s => s.startsWith('DELETE FROM svti_dead_letter WHERE user_id'));
		const commit = statements.indexOf('COMMIT');
		expect(begin).toBe(0);
		expect(lock).toBeGreaterThan(begin);
		expect(tombstone).toBeGreaterThan(lock);
		expect(erase).toBeGreaterThan(tombstone);
		expect(commit).toBeGreaterThan(erase);
		// Stamped with the clock the lock statement read after the lock.
		expect(client._purges()).toEqual([{ scope: 'acme', user_id: 'u1', purged_at: Math.floor(clockMs) }]);
	});

	it('serializes mixed-case capture and purge on one physical lock identity', async () => {
		let resumeCapture, markEntered;
		const entered = new Promise(resolve => { markEntered = resolve; });
		const chains = new Map();
		const coordination = {
			identities: [],
			pauseCapture: true,
			captureGate: new Promise(resolve => { resumeCapture = resolve; }),
			captureEntered: markEntered,
			async acquire(key) {
				const previous = chains.get(key) ?? Promise.resolve();
				let release;
				const current = new Promise(resolve => { release = resolve; });
				chains.set(key, previous.then(() => current));
				await previous;
				return release;
			}
		};
		const upper = createDeadLetter(aliasedPg(client, 'Case_DLQ', coordination), {
			table: 'Case_DLQ', autoMigrate: false, cleanupInterval: 0, forgetUserId: byAuthor
		});
		const lower = createDeadLetter(aliasedPg(client, 'case_dlq', coordination), {
			table: 'case_dlq', autoMigrate: false, cleanupInterval: 0, forgetUserId: byAuthor
		});
		try {
			const capturing = upper.add(fencedRecord({ startedAt: monotonicNow() }));
			await entered;
			let purgeSettled = false;
			const purging = lower.purgeUser(null, 'u1');
			purging.then(() => { purgeSettled = true; }, () => { purgeSettled = true; });
			await expect.poll(() => coordination.identities.length).toBe(2);
			expect(new Set(coordination.identities)).toEqual(new Set(['case_dlq_purges']));
			expect(purgeSettled).toBe(false);
			resumeCapture();
			expect(await capturing).not.toBeNull();
			expect(await purging).toBe(1);
			expect(await upper.count()).toBe(0);
		} finally {
			resumeCapture();
			upper.destroy();
			lower.destroy();
		}
	});

	// The start is computed from the database clock read after the lock and
	// the monotonic time after that reply, so statement latency can only move
	// it earlier. A start computed before the INSERT was sent, against the
	// INSERT's arrival time, lands one round trip late and misses this purge.
	it('fences a purge made at the delivery start despite statement latency', async () => {
		const startedAt = monotonicNow();
		await store.purgeUser(null, 'u1');
		const query = client.query.bind(client);
		client.query = async (sql, values) => {
			clockMs += 50;
			const result = await query(sql, values);
			clockMs += 50;
			return result;
		};
		expect(await store.add(fencedRecord({ startedAt }))).toBeNull();
		expect(client._rows()).toHaveLength(0);
	});

	// The fence and record reads follow the lock wait; a repeatable-read
	// snapshot taken before the wait would miss rows committed during it.
	it('pins read committed for fenced captures and purges', async () => {
		// The BEGIN of each transaction that takes the erasure lock (the
		// retention pass after a capture has its own plain BEGIN).
		const begins = [];
		let lastBegin;
		const query = client.query.bind(client);
		client.query = async (sql, values) => {
			const text = typeof sql === 'string' ? sql : sql.text;
			if (text.startsWith('BEGIN')) lastBegin = text;
			if (text.includes('pg_advisory_xact_lock(hashtextextended')) begins.push(lastBegin);
			return query(sql, values);
		};
		expect(await store.add(fencedRecord({ topic: '@t/acme/orders', startedAt: monotonicNow() }))).not.toBeNull();
		expect(await store.purgeUser('acme', 'u1')).toBe(1);
		expect(begins).toEqual(['BEGIN ISOLATION LEVEL READ COMMITTED', 'BEGIN ISOLATION LEVEL READ COMMITTED']);
	});

	it('pins read committed for every statement that changes the scope counts', async () => {
		// Unfenced captures, remove, clear and the retention pass all write
		// rows whose triggers update shared per-scope count rows.
		store.destroy();
		store = createDeadLetter(client, { autoMigrate: false, cleanupInterval: 0, max: 1 });
		const begins = [];
		const writes = [];
		let open = false;
		const query = client.query.bind(client);
		client.query = async (sql, values) => {
			const text = (typeof sql === 'string' ? sql : sql.text).replace(/\s+/g, ' ').trim();
			if (text.startsWith('BEGIN')) { begins.push(text); open = true; }
			if (text === 'COMMIT' || text === 'ROLLBACK') open = false;
			if (/^(INSERT INTO svti_dead_letter |DELETE FROM svti_dead_letter|WITH (victims|expired) AS)/.test(text)) writes.push(open);
			return query(sql, values);
		};
		const first = await store.add(rec({ topic: '@t/acme/orders' }));
		await store.add(rec({ topic: '@t/acme/orders' }));
		await store.remove(first);
		await store.clear();
		// Two captures, one eviction, remove and clear, none in autocommit.
		expect(writes).toEqual([true, true, true, true, true]);
		expect(new Set(begins)).toEqual(new Set(['BEGIN ISOLATION LEVEL READ COMMITTED']));
	});

	it('binds an integer start for a delivery that claims an absurdly early start', async () => {
		expect(await store.add(fencedRecord({ startedAt: -1e22 }))).not.toBeNull();
	});

	it('keeps an untraced post-purge capture and refuses an unscopable tenant', async () => {
		await store.purgeUser(null, 'u1');
		clockMs += 1;
		expect(await store.add(fencedRecord())).not.toBeNull();
		await expect(store.purgeUser('bad/tenant', 'u1')).rejects.toThrow(TypeError);
	});

	it('expires and prunes tombstones after ten minutes with bounded write-time cleanup', async () => {
		const startedAt = monotonicNow();
		await store.purgeUser(null, 'u1');
		expect(client._purges()).toHaveLength(1);
		clockMs += 10 * 60 * 1000;
		expect(await store.add(fencedRecord({ startedAt }))).toBeNull();
		clockMs += 1;
		expect(await store.add(fencedRecord({ startedAt }))).not.toBeNull();
		expect(client._purges()).toHaveLength(0);
	});
});


it('keeps dead-letter topics private when a registry omits the mapper', async () => {
	const metrics = createMetrics();
	const store = createDeadLetter(fakePg(), { autoMigrate: false, cleanupInterval: 0, metrics: { ...metrics, mapTopic: undefined } });
	await store.add({ webhookId: 'webhook', topic: 'alice@example.com', event: 'event', data: {}, attempts: 1, error: 'offline', failedAt: 1 });
	expect(metrics.serialize()).toContain('dead_letter_added_total{topic="all"} 1');
	expect(metrics.serialize()).not.toContain('alice@example.com');
});
