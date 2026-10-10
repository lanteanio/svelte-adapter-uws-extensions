import { afterEach, describe, expect, it } from 'vitest';
import pg from 'pg';
import { createPgClient } from '../../../src/postgres/index.js';
import { createMetrics } from '../../../src/prometheus/index.js';
import { createCircuitBreaker, withBreaker } from '../../../src/shared/breaker.js';

const clients = [];
function create(options = {}) {
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, ...options });
	clients.push(client);
	return client;
}
afterEach(async () => { await Promise.all(clients.splice(0).map((client) => client.end())); });

describe('PostgreSQL pool bounds against the real driver', () => {
	it('limits queued work and expires checkout waits behind a held connection', async () => {
		const metrics = createMetrics();
		const client = create({ maxInFlight: 2, metrics, options: { max: 1, connectionTimeoutMillis: 100 } });
		const lease = await client.connect();
		try {
			const first = client.query('SELECT 1').catch((error) => error);
			const second = client.connect().catch((error) => error);
			await expect(client.query('SELECT 2')).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
			expect(client.pool.waitingCount).toBe(2);
			expect(metrics.serialize()).toContain('postgres_pool_waiters 2');
			for (const result of await Promise.all([first, second])) expect(result.message).toBe('timeout exceeded when trying to connect');
			expect(client.pool.waitingCount).toBe(0);
			expect(metrics.serialize()).toContain('postgres_operation_timeouts_total{operation="connect"} 1');
		} finally { lease.release(); }
		await expect(client.query({ text: 'SELECT $1::int AS value', values: [42] })).resolves.toMatchObject({ rows: [{ value: 42 }] });
	});

	it('lets server statement deadlines trip a breaker for slow queries and recovers the pool', async () => {
		const metrics = createMetrics();
		const client = create({ maxInFlight: 1, metrics, options: { max: 1, statement_timeout: 60, query_timeout: 2000 } });
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		try {
			await expect(withBreaker(breaker, () => client.query('SELECT pg_sleep(1)'))).rejects.toMatchObject({ code: '57014' });
			expect(breaker.state).toBe('broken');
			expect(metrics.serialize()).toContain('postgres_operation_timeouts_total{operation="query"} 1');
			await expect(client.query('SELECT 1')).resolves.toHaveProperty('rowCount', 1);
		} finally { breaker.destroy(); }
	});

	it('bounds client query reads with the server deadline disabled', async () => {
		const client = create({ maxInFlight: 1, options: { max: 1, query_timeout: 60, statement_timeout: 0 } });
		await expect(client.query('SELECT pg_sleep(1)')).rejects.toThrow('Query read timeout');
		await expect(client.query('SELECT 1')).resolves.toHaveProperty('rowCount', 1);
	});

	it('keeps capacity reserved while an aborted query continues on the server', async () => {
		const client = create({ maxInFlight: 1, options: { max: 1 } });
		const controller = new AbortController();
		const pending = client.query('SELECT pg_sleep(0.12)', undefined, { signal: controller.signal });
		controller.abort();
		await expect(pending).rejects.toHaveProperty('name', 'AbortError');
		await expect(client.query('SELECT 1')).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
		// Wait for the driver release, then its promise settlement reactions.
		await new Promise((resolve) => client.pool.once('release', resolve));
		await new Promise((resolve) => setImmediate(resolve));
		await expect(client.query('SELECT 1')).resolves.toHaveProperty('rowCount', 1);
	});

	it('returns an aborted late checkout to the real pool exactly once', async () => {
		const client = create({ maxInFlight: 1, options: { max: 1 } });
		const lease = await client.connect();
		const controller = new AbortController();
		const pending = client.connect({ signal: controller.signal });
		controller.abort();
		await expect(pending).rejects.toHaveProperty('name', 'AbortError');
		await expect(client.connect()).rejects.toHaveProperty('code', 'PG_CAPACITY_EXCEEDED');
		lease.release();
		await new Promise((resolve) => setImmediate(resolve));
		expect(client.pool.idleCount).toBe(1);
		expect(client.pool.waitingCount).toBe(0);
		const returned = await client.connect();
		returned.release();
		expect(client.pool.idleCount).toBe(1);
	});

	it('honors disabled owned deadlines and leaves supplied pool settings and ownership intact', async () => {
		const client = create({ maxInFlight: 0, options: { connectionTimeoutMillis: 0, query_timeout: 0, statement_timeout: 0 } });
		expect((await client.query('SHOW statement_timeout')).rows[0].statement_timeout).toBe('0');
		await expect(client.query('SELECT pg_sleep(0.07)')).resolves.toHaveProperty('rowCount', 1);
		const pool = new pg.Pool({ connectionString: process.env.INTEGRATION_POSTGRES_URL, max: 1 });
		try {
			const wrapped = createPgClient({ pool });
			expect((await wrapped.query('SHOW statement_timeout')).rows[0].statement_timeout).toBe('0');
			expect(pool.options.query_timeout).toBeUndefined();
			await wrapped.end();
			await expect(pool.query('SELECT 1')).resolves.toHaveProperty('rowCount', 1);
		} finally { await pool.end(); }
	});
});
