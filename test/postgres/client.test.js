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

	it('throws if connectionString is missing', () => {
		expect(() => createPgClient()).toThrow('connectionString is required');
		expect(() => createPgClient({})).toThrow('connectionString is required');
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
