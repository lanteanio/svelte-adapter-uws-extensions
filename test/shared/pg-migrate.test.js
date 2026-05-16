import { describe, it, expect, vi } from 'vitest';
import { safeCreate, assertSafeTableName } from '../../shared/pg-migrate.js';

/**
 * Minimal mock pg client surface. Tests inject `queryHandler` to
 * simulate failure modes (existing relation, missing columns, etc).
 */
function mockClient(queryHandler) {
	return {
		query: vi.fn(queryHandler)
	};
}

describe('shared/pg-migrate: assertSafeTableName', () => {
	it('accepts valid table names', () => {
		expect(() => assertSafeTableName('svti_tasks', 'pg tasks')).not.toThrow();
		expect(() => assertSafeTableName('a', 'mod')).not.toThrow();
		expect(() => assertSafeTableName('_underscored', 'mod')).not.toThrow();
	});

	it('rejects SQL-injectable shapes', () => {
		expect(() => assertSafeTableName('tasks; DROP TABLE x', 'mod')).toThrow('invalid table name');
		expect(() => assertSafeTableName('1numeric_start', 'mod')).toThrow('invalid table name');
		expect(() => assertSafeTableName('has space', 'mod')).toThrow('invalid table name');
	});

	it('rejects reserved Postgres namespaces (pg_* / information_schema*)', () => {
		expect(() => assertSafeTableName('pg_class', 'mod')).toThrow('reserved Postgres schema');
		expect(() => assertSafeTableName('PG_CATALOG_table', 'mod')).toThrow('reserved Postgres schema');
		expect(() => assertSafeTableName('information_schema_columns', 'mod')).toThrow('reserved Postgres schema');
	});
});

describe('shared/pg-migrate: safeCreate', () => {
	it('runs the DDL normally when no error is thrown', async () => {
		const client = mockClient(() => Promise.resolve({ rows: [] }));
		await safeCreate(client, 'CREATE TABLE foo (id INT)');
		expect(client.query).toHaveBeenCalledTimes(1);
		expect(client.query).toHaveBeenCalledWith('CREATE TABLE foo (id INT)');
	});

	it('swallows 23505 (unique_violation) during DDL race', async () => {
		const err = Object.assign(new Error('duplicate'), { code: '23505' });
		const client = mockClient(() => Promise.reject(err));
		await expect(safeCreate(client, 'CREATE INDEX ...')).resolves.toBeUndefined();
	});

	it('swallows 42P07 (relation already exists)', async () => {
		const err = Object.assign(new Error('relation "foo" already exists'), { code: '42P07' });
		const client = mockClient(() => Promise.reject(err));
		await expect(safeCreate(client, 'CREATE TABLE foo (id INT)')).resolves.toBeUndefined();
	});

	it('swallows 42710 (duplicate_object)', async () => {
		const err = Object.assign(new Error('duplicate index'), { code: '42710' });
		const client = mockClient(() => Promise.reject(err));
		await expect(safeCreate(client, 'CREATE INDEX ...')).resolves.toBeUndefined();
	});

	it('re-throws any other error verbatim', async () => {
		const err = Object.assign(new Error('permission denied'), { code: '42501' });
		const client = mockClient(() => Promise.reject(err));
		await expect(safeCreate(client, 'CREATE TABLE foo (id INT)')).rejects.toThrow('permission denied');
	});

	describe('expectedColumns drift detection', () => {
		it('verifies columns when DDL fires 42P07 AND expectedColumns is supplied', async () => {
			let callCount = 0;
			const client = {
				query: vi.fn((text, values) => {
					callCount++;
					if (callCount === 1) {
						// First call: the CREATE TABLE itself, throws "already exists".
						const err = Object.assign(new Error('relation exists'), { code: '42P07' });
						return Promise.reject(err);
					}
					// Second call: verifyTableColumns query.
					expect(text).toContain('information_schema.columns');
					expect(values).toEqual(['mytable']);
					return Promise.resolve({
						rows: [
							{ column_name: 'id' },
							{ column_name: 'name' },
							{ column_name: 'created_at' }
						]
					});
				})
			};
			await expect(safeCreate(client, 'CREATE TABLE mytable (...)', {
				table: 'mytable',
				columns: ['id', 'name', 'created_at']
			})).resolves.toBeUndefined();
			expect(client.query).toHaveBeenCalledTimes(2);
		});

		it('throws with a clear error when the existing table is missing expected columns', async () => {
			const client = {
				query: vi.fn((text) => {
					if (text.includes('information_schema')) {
						return Promise.resolve({ rows: [{ column_name: 'id' }] });
					}
					const err = Object.assign(new Error('relation exists'), { code: '42P07' });
					return Promise.reject(err);
				})
			};
			await expect(safeCreate(client, 'CREATE TABLE mytable (...)', {
				table: 'mytable',
				columns: ['id', 'name', 'created_at']
			})).rejects.toThrow(/missing expected column\(s\): name, created_at/);
		});

		it('does not run verification when no error is thrown (fresh table)', async () => {
			const client = mockClient(() => Promise.resolve({ rows: [] }));
			await safeCreate(client, 'CREATE TABLE foo (id INT)', {
				table: 'foo',
				columns: ['id']
			});
			// Only the CREATE TABLE call; no follow-up information_schema query.
			expect(client.query).toHaveBeenCalledTimes(1);
		});

		it('does not run verification for non-42P07 errors (e.g. 42710 duplicate_object on index)', async () => {
			const err = Object.assign(new Error('duplicate index'), { code: '42710' });
			const client = mockClient(() => Promise.reject(err));
			await safeCreate(client, 'CREATE INDEX ...', {
				table: 'mytable',
				columns: ['id']
			});
			// Only the index attempt; no information_schema follow-up because
			// this code path doesn't indicate a table-level shape mismatch.
			expect(client.query).toHaveBeenCalledTimes(1);
		});

		it('does not run verification when expectedColumns is omitted (backward compat)', async () => {
			const err = Object.assign(new Error('relation exists'), { code: '42P07' });
			const client = mockClient(() => Promise.reject(err));
			await safeCreate(client, 'CREATE TABLE mytable (id INT)');
			expect(client.query).toHaveBeenCalledTimes(1);
		});

		it('accepts tables with EXTRA columns (forward-migrated tables)', async () => {
			const client = {
				query: vi.fn((text) => {
					if (text.includes('information_schema')) {
						return Promise.resolve({
							rows: [
								{ column_name: 'id' },
								{ column_name: 'name' },
								// Extra columns added in a later schema version.
								{ column_name: 'created_at' },
								{ column_name: 'updated_at' },
								{ column_name: 'request_id' }
							]
						});
					}
					const err = Object.assign(new Error('relation exists'), { code: '42P07' });
					return Promise.reject(err);
				})
			};
			await expect(safeCreate(client, 'CREATE TABLE mytable (...)', {
				table: 'mytable',
				columns: ['id', 'name']
			})).resolves.toBeUndefined();
		});
	});
});
