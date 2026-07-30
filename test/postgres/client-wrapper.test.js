import { describe, it, expect } from 'vitest';
import { createPgClient } from '../../src/postgres/index.js';
import { withTransaction } from '../../src/shared/pg-tx.js';

const DSN = 'postgres://svc:hunter2@db.internal:5432/app';

/** A pool that hands back the SAME client object on every acquire, as pg-pool does. */
function reusingPool() {
	const client = {
		query: (...args) => {
			const last = args[args.length - 1];
			if (typeof last === 'function') { last(null, { rows: [] }); return undefined; }
			return Promise.resolve({ rows: [] });
		},
		release: () => {}
	};
	return { pool: { connect: async () => client, query: async () => ({ rows: [] }), end: async () => {}, on: () => {} }, client };
}

describe('pooled client query wrapper', () => {
	it('wraps a reused client exactly once across many acquires', async () => {
		// pg-pool returns the same client object every time and release() does
		// not restore the method, so re-wrapping per acquire stacks one
		// closure per checkout: every query gets slower and the connection
		// eventually dies with a call-stack overflow (measured at ~8,350
		// transactions before this guard).
		const { pool, client } = reusingPool();
		const pg = createPgClient({ pool });

		const first = await pg.connect();
		const wrappedQuery = first.query;
		first.release();

		for (let i = 0; i < 20_000; i++) {
			const c = await pg.connect();
			c.release();
		}
		expect(client.query).toBe(wrappedQuery);

		const last = await pg.connect();
		await expect(last.query('SELECT 1')).resolves.toEqual({ rows: [] });
		last.release();
	});

	it('survives 20k transactions on one pooled connection', async () => {
		const { pool } = reusingPool();
		const pg = createPgClient({ pool });
		for (let i = 0; i < 20_000; i++) {
			await withTransaction(pg, async (tx) => { await tx.query('SELECT 1'); });
		}
		await withTransaction(pg, async (tx) => {
			await expect(tx.query('SELECT 1')).resolves.toEqual({ rows: [] });
		});
	});

	it('preserves the callback form', async () => {
		const { pool } = reusingPool();
		const pg = createPgClient({ pool });
		const client = await pg.connect();
		const seen = await new Promise((resolve) => {
			const ret = client.query('SELECT 1', [], (err, res) => resolve({ err, res }));
			expect(ret).toBeUndefined(); // pg returns undefined for the callback form
		});
		expect(seen.err).toBeNull();
		expect(seen.res).toEqual({ rows: [] });
	});

	it('passes a submittable (cursor / query-stream) straight through', async () => {
		const submittable = { submit() {}, marker: true };
		const pool = {
			connect: async () => ({ query: (arg) => arg, release: () => {} }),
			query: async () => ({ rows: [] }), end: async () => {}, on: () => {}
		};
		const client = await createPgClient({ pool }).connect();
		expect(client.query(submittable)).toBe(submittable);
	});

	it('redacts a callback-form error too', async () => {
		const pool = {
			connect: async () => ({
				query: (...args) => { args[args.length - 1](new Error(`auth failed for ${DSN}`)); },
				release: () => {}
			}),
			query: async () => ({ rows: [] }), end: async () => {}, on: () => {}
		};
		const client = await createPgClient({ pool }).connect();
		const err = await new Promise((resolve) => client.query('SELECT 1', [], resolve));
		expect(err.message).not.toContain('hunter2');
	});
});

describe('redacted error fidelity', () => {
	it('keeps the prototype and every diagnostic field', async () => {
		class DatabaseError extends Error {}
		const pool = {
			query: async () => {
				const e = new DatabaseError(`duplicate key on ${DSN}`);
				Object.assign(e, { code: '23505', position: '42', hint: 'try again', where: 'PL/pgSQL', detail: 'Key exists', constraint: 'pk' });
				throw e;
			},
			end: async () => {}, on: () => {}
		};
		const err = await createPgClient({ pool }).query('INSERT ...').catch((e) => e);

		expect(err).toBeInstanceOf(DatabaseError);
		expect(err.message).not.toContain('hunter2');
		// position / hint / where are what make a SQL error diagnosable, and
		// a fresh `new Error()` would drop all of them.
		expect(err.code).toBe('23505');
		expect(err.position).toBe('42');
		expect(err.hint).toBe('try again');
		expect(err.where).toBe('PL/pgSQL');
		expect(err.detail).toBe('Key exists');
		expect(err.constraint).toBe('pk');
	});
});
