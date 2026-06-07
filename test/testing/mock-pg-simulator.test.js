import { describe, it, expect, afterEach } from 'vitest';
import { mockPgClient } from '../../testing/mock-pg.js';
import { createIdempotencyStore } from '../../postgres/idempotency.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../shared/runtime.js';

// Install a virtual clock the test fully controls. The double reads the wall
// epoch through the runtime seam, so overriding `wallEpoch` drives every TTL
// window, fence expiry, and timestamp the double produces. Always restore the
// native environment so an override in one case cannot leak into another.
function installClock(start) {
	let t = start;
	setRuntimeEnv({
		clock: {
			now: () => t,
			wallEpoch: () => t
		}
	});
	return {
		advance(ms) { t += ms; },
		set(ms) { t = ms; },
		read() { return t; }
	};
}

afterEach(() => {
	resetRuntimeEnv();
});

describe('mock-pg simulator-grade behaviour', () => {
	describe('virtual-clock-controlled idempotency TTL', () => {
		it('a pending slot blocks re-acquire until the acquireTtl elapses on the injected clock', async () => {
			const clock = installClock(1_000_000);
			const client = mockPgClient();
			const store = createIdempotencyStore(client, { acquireTtl: 30, cleanupInterval: 0 });

			// First acquire owns the slot.
			const first = await store.acquire('k');
			expect(first.acquired).toBe(true);

			// A second acquire while the slot is pending and unexpired is refused.
			expect(await store.acquire('k')).toEqual({ acquired: false, pending: true });

			// Advance the virtual clock to just before the acquireTtl expiry: still
			// pending. No real time passed - the double's clock is the injected one.
			clock.advance(29_000);
			expect(await store.acquire('k')).toEqual({ acquired: false, pending: true });

			// Cross the 30s acquireTtl boundary: the stale pending row is now
			// expired, so a fresh acquire takes the slot again.
			clock.advance(2_000);
			const reacquired = await store.acquire('k');
			expect(reacquired.acquired).toBe(true);

			store.destroy();
		});

		it('a committed result survives until the long ttl expires on the injected clock', async () => {
			const clock = installClock(5_000_000);
			const client = mockPgClient();
			const store = createIdempotencyStore(client, { ttl: 100, acquireTtl: 30, cleanupInterval: 0 });

			const handle = await store.acquire('order-42');
			expect(handle.acquired).toBe(true);
			await handle.commit({ ok: 1 });

			// Within the ttl the cached result is returned.
			clock.advance(50_000);
			expect(await store.acquire('order-42')).toEqual({ acquired: false, result: { ok: 1 } });

			// Past the ttl the row has expired and the slot is free again.
			clock.advance(60_000);
			const afterTtl = await store.acquire('order-42');
			expect(afterTtl.acquired).toBe(true);

			store.destroy();
		});
	});

	describe('pg_try_advisory_lock contention', () => {
		it('one holder wins the lock and a second connection is refused until release', async () => {
			const client = mockPgClient();
			const a = client.createClient();
			const b = client.createClient();
			await a.connect();
			await b.connect();

			// First connection claims the free lock.
			const r1 = await a.query('SELECT pg_try_advisory_lock($1) AS acquired', [777]);
			expect(r1.rows[0].acquired).toBe(true);

			// Second connection is refused while the first holds it.
			const r2 = await b.query('SELECT pg_try_advisory_lock($1) AS acquired', [777]);
			expect(r2.rows[0].acquired).toBe(false);

			// The holder re-asking is idempotently granted (session re-entrancy).
			const r1again = await a.query('SELECT pg_try_advisory_lock($1) AS acquired', [777]);
			expect(r1again.rows[0].acquired).toBe(true);

			// The holder releases; now the second connection can take it.
			const rel = await a.query('SELECT pg_advisory_unlock($1)', [777]);
			expect(rel.rows[0].pg_advisory_unlock).toBe(true);

			const r2after = await b.query('SELECT pg_try_advisory_lock($1) AS acquired', [777]);
			expect(r2after.rows[0].acquired).toBe(true);

			// A distinct lock id is independent of the contended one.
			const other = await b.query('SELECT pg_try_advisory_lock($1) AS acquired', [999]);
			expect(other.rows[0].acquired).toBe(true);

			await a.end();
			await b.end();
		});

		it('a connection end() releases the locks it still held', async () => {
			const client = mockPgClient();
			const a = client.createClient();
			const b = client.createClient();
			await a.connect();
			await b.connect();

			expect((await a.query('SELECT pg_try_advisory_lock($1) AS acquired', [1])).rows[0].acquired).toBe(true);
			expect((await b.query('SELECT pg_try_advisory_lock($1) AS acquired', [1])).rows[0].acquired).toBe(false);

			// Dropping the holder's session releases the lock without an explicit
			// unlock, matching Postgres session-scoped advisory-lock semantics.
			await a.end();
			expect((await b.query('SELECT pg_try_advisory_lock($1) AS acquired', [1])).rows[0].acquired).toBe(true);

			await b.end();
		});
	});

	describe('FOR UPDATE SKIP LOCKED fairness', () => {
		// Issue the job-claim CTE the jobs plugin uses, but on two pinned
		// connections that each hold their claim inside an open transaction. A
		// row the first connection has locked is invisible to the second, so the
		// two claimers get disjoint row sets - the exact double-claim race a
		// naive slice(0, limit) stub could never surface.
		const CLAIM_SQL = `WITH claimed AS (
			SELECT svti_jobs_id FROM svti_jobs
			 WHERE queue = $1
			   AND (claimed_at IS NULL OR claimed_until < now())
			 ORDER BY svti_jobs_id
			   FOR UPDATE SKIP LOCKED
			 LIMIT $2
		)
		UPDATE svti_jobs t
		   SET claimed_at = now(),
		       claimed_until = now() + ($3 || ' milliseconds')::interval,
		       attempts = t.attempts + 1
		  FROM claimed
		 WHERE t.svti_jobs_id = claimed.svti_jobs_id
		RETURNING t.svti_jobs_id AS id, t.queue, t.payload, t.request_id, t.attempts, t.created_at`;

		async function enqueue(client, queue, n) {
			for (let i = 0; i < n; i++) {
				await client.query({
					text: `INSERT INTO svti_jobs (queue, payload, request_id)
					            VALUES ($1, $2, $3) RETURNING svti_jobs_id AS id`,
					values: [queue, JSON.stringify({ i }), null]
				});
			}
		}

		it('two concurrent claimers get disjoint row sets', async () => {
			const client = mockPgClient();
			await enqueue(client, 'q', 6);

			const a = await client.pool.connect();
			const b = await client.pool.connect();

			await a.query('BEGIN');
			await b.query('BEGIN');

			// A claims 3 rows and holds the locks (transaction still open).
			const aRows = (await a.query({ text: CLAIM_SQL, values: ['q', 3, '30000'] })).rows;
			// B claims while A's locks are held: it must skip A's rows.
			const bRows = (await b.query({ text: CLAIM_SQL, values: ['q', 3, '30000'] })).rows;

			expect(aRows).toHaveLength(3);
			expect(bRows).toHaveLength(3);

			const aIds = aRows.map((r) => r.id);
			const bIds = bRows.map((r) => r.id);
			const overlap = aIds.filter((id) => bIds.includes(id));
			expect(overlap).toEqual([]);

			// Together they cover all six rows exactly once, deterministically
			// ordered by row id (FIFO).
			expect([...aIds, ...bIds].sort((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6]);

			await a.query('COMMIT');
			await b.query('COMMIT');
		});

		it('a row a holder still locks is invisible to a concurrent claimer and reappears once released', async () => {
			const client = mockPgClient();
			await enqueue(client, 'q', 2);

			const a = await client.pool.connect();
			const b = await client.pool.connect();

			// A claims both rows and holds the locks (transaction still open).
			await a.query('BEGIN');
			const aRows = (await a.query({ text: CLAIM_SQL, values: ['q', 2, '30000'] })).rows;
			expect(aRows).toHaveLength(2);

			// While A holds the locks, a concurrent claimer skips every row.
			await b.query('BEGIN');
			const bEmpty = (await b.query({ text: CLAIM_SQL, values: ['q', 2, '30000'] })).rows;
			expect(bEmpty).toHaveLength(0);
			await b.query('COMMIT');

			// A releases its connection (without an explicit COMMIT, release()
			// drops the held row locks). The rows are no longer lock-blocked, but
			// they are still inside their visibility window from A's claim, so a
			// fresh claimer correctly waits for the timeout rather than
			// double-claiming an in-flight row.
			a.release();

			const c = await client.pool.connect();
			await c.query('BEGIN');
			const stillHeld = (await c.query({ text: CLAIM_SQL, values: ['q', 2, '30000'] })).rows;
			expect(stillHeld).toHaveLength(0);
			await c.query('COMMIT');
		});

		it('a claimed row becomes re-claimable once its visibility window expires on the injected clock', async () => {
			const clock = installClock(2_000_000);
			const client = mockPgClient();
			await enqueue(client, 'q', 1);

			// Claim with a 5s visibility window, then release the lock.
			const a = await client.pool.connect();
			await a.query('BEGIN');
			const aRows = (await a.query({ text: CLAIM_SQL, values: ['q', 1, '5000'] })).rows;
			expect(aRows).toHaveLength(1);
			await a.query('COMMIT');

			// Before the window elapses on the virtual clock, the row is still
			// invisible (claimed_until is in the future).
			clock.advance(4_000);
			const b = await client.pool.connect();
			await b.query('BEGIN');
			expect((await b.query({ text: CLAIM_SQL, values: ['q', 1, '5000'] })).rows).toHaveLength(0);
			await b.query('COMMIT');

			// Past the window the row reappears for re-claim.
			clock.advance(2_000);
			const c = await client.pool.connect();
			await c.query('BEGIN');
			const cRows = (await c.query({ text: CLAIM_SQL, values: ['q', 1, '5000'] })).rows;
			expect(cRows).toHaveLength(1);
			expect(cRows[0].attempts).toBe(2);
			await c.query('COMMIT');
		});
	});

	describe('NOTIFY / LISTEN ordered delivery', () => {
		it('delivers each NOTIFY to every registered listener in FIFO order per channel', async () => {
			const client = mockPgClient();

			const conn = client.createClient();
			await conn.connect();
			const received = [];
			conn.on('notification', (msg) => received.push(msg));

			await conn.query('LISTEN "changes"');

			// Three notifications enqueue in call order; delivery is async (next
			// microtask) but ordered FIFO per channel.
			client._notify('changes', 'a');
			client._notify('changes', 'b');
			client._notify('changes', 'c');

			// Nothing delivered synchronously.
			expect(received).toHaveLength(0);

			await Promise.resolve();
			await Promise.resolve();

			expect(received.map((m) => m.payload)).toEqual(['a', 'b', 'c']);
			expect(received.every((m) => m.channel === 'changes')).toBe(true);

			await conn.end();
		});

		it('fans a single NOTIFY out to multiple listeners and drops a connection that UNLISTENs', async () => {
			const client = mockPgClient();

			const c1 = client.createClient();
			const c2 = client.createClient();
			await c1.connect();
			await c2.connect();

			const r1 = [];
			const r2 = [];
			c1.on('notification', (m) => r1.push(m.payload));
			c2.on('notification', (m) => r2.push(m.payload));

			await c1.query('LISTEN "room"');
			await c2.query('LISTEN "room"');

			client._notify('room', 'first');
			await Promise.resolve();
			await Promise.resolve();
			expect(r1).toEqual(['first']);
			expect(r2).toEqual(['first']);

			// c2 stops listening; the next NOTIFY reaches only c1.
			await c2.query('UNLISTEN "room"');
			client._notify('room', 'second');
			await Promise.resolve();
			await Promise.resolve();
			expect(r1).toEqual(['first', 'second']);
			expect(r2).toEqual(['first']);

			// A notification on a channel nobody listens on is a no-op.
			client._notify('other', 'ignored');
			await Promise.resolve();
			expect(r1).toEqual(['first', 'second']);

			await c1.end();
			await c2.end();
		});

		it('only listeners registered at NOTIFY time receive a late subscriber misses an earlier message', async () => {
			const client = mockPgClient();

			const early = client.createClient();
			await early.connect();
			const got = [];
			early.on('notification', (m) => got.push(m.payload));
			await early.query('LISTEN "feed"');

			client._notify('feed', 'before');

			const late = client.createClient();
			await late.connect();
			const lateGot = [];
			late.on('notification', (m) => lateGot.push(m.payload));
			await late.query('LISTEN "feed"');

			client._notify('feed', 'after');

			await Promise.resolve();
			await Promise.resolve();

			expect(got).toEqual(['before', 'after']);
			// The late subscriber only sees notifications emitted after it
			// registered.
			expect(lateGot).toEqual(['after']);

			await early.end();
			await late.end();
		});
	});
});
