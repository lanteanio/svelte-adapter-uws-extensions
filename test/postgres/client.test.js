import { describe, it, expect, vi } from 'vitest';

// Mock pg module
vi.mock('pg', () => {
	const MockPool = vi.fn(function () {
		this.query = vi.fn(() => Promise.resolve({ rows: [], rowCount: 0 }));
		this.end = vi.fn(() => Promise.resolve());
		this.on = vi.fn();
	});
	return { default: { Pool: MockPool } };
});

const { createPgClient } = await import('../../postgres/index.js');

describe('createPgClient', () => {
	it('returns a client with the expected API', () => {
		const client = createPgClient({ connectionString: 'postgres://localhost/test' });
		expect(client.pool).toBeDefined();
		expect(typeof client.query).toBe('function');
		expect(typeof client.end).toBe('function');
	});

	it('throws if neither connectionString nor pool is provided', () => {
		expect(() => createPgClient()).toThrow('connectionString or pool is required');
		expect(() => createPgClient({})).toThrow('connectionString or pool is required');
	});

	it('wraps an externally-provided pool', () => {
		const externalPool = {
			query: vi.fn(() => Promise.resolve({ rows: [], rowCount: 0 })),
			end: vi.fn(() => Promise.resolve()),
			on: vi.fn()
		};
		const client = createPgClient({ pool: externalPool });
		expect(client.pool).toBe(externalPool);
	});

	it('end() is a no-op when the pool was provided externally', async () => {
		const externalPool = {
			query: vi.fn(() => Promise.resolve({ rows: [], rowCount: 0 })),
			end: vi.fn(() => Promise.resolve()),
			on: vi.fn()
		};
		const client = createPgClient({ pool: externalPool });
		await client.end();
		await client.end();
		expect(externalPool.end).not.toHaveBeenCalled();
	});

	it('createClient() throws when only a pool was provided', () => {
		const externalPool = {
			query: vi.fn(() => Promise.resolve({ rows: [], rowCount: 0 })),
			end: vi.fn(() => Promise.resolve()),
			on: vi.fn()
		};
		const client = createPgClient({ pool: externalPool });
		expect(() => client.createClient()).toThrow('createClient() requires connectionString');
	});

	it('rejects a non-Pool value passed as pool', () => {
		expect(() => createPgClient({ pool: 'not-a-pool' })).toThrow('pool must be a pg.Pool instance');
		expect(() => createPgClient({ pool: { query: 'not a function' } })).toThrow('pool must be a pg.Pool instance');
	});

	it('query delegates to pool.query', async () => {
		const client = createPgClient({ connectionString: 'postgres://localhost/test' });
		await client.query('SELECT 1');
		expect(client.pool.query).toHaveBeenCalledWith('SELECT 1', undefined);
	});

	it('end is idempotent', async () => {
		const client = createPgClient({ connectionString: 'postgres://localhost/test' });
		await client.end();
		await client.end();
		expect(client.pool.end).toHaveBeenCalledTimes(1);
	});
});
