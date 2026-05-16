import { describe, it, expect } from 'vitest';
import { withTransaction } from '../../shared/pg-tx.js';

function makeMockClient() {
	const queries = [];
	let released = false;
	const client = {
		pool: {
			async connect() {
				return {
					query: async (text, values) => {
						queries.push(text);
						if (text === 'THROW') throw new Error('forced');
						return { rows: [], rowCount: 0 };
					},
					release: () => { released = true; }
				};
			}
		}
	};
	return { client, queries, isReleased: () => released };
}

describe('shared/pg-tx', () => {
	it('wraps fn in BEGIN/COMMIT on a single pooled connection', async () => {
		const { client, queries, isReleased } = makeMockClient();
		await withTransaction(client, async (tx) => {
			await tx.query('DELETE FROM a');
			await tx.query('DELETE FROM b');
		});
		expect(queries).toEqual(['BEGIN', 'DELETE FROM a', 'DELETE FROM b', 'COMMIT']);
		expect(isReleased()).toBe(true);
	});

	it('returns the value fn returns', async () => {
		const { client } = makeMockClient();
		const result = await withTransaction(client, async () => 42);
		expect(result).toBe(42);
	});

	it('issues ROLLBACK and re-throws when fn throws', async () => {
		const { client, queries, isReleased } = makeMockClient();
		await expect(
			withTransaction(client, async (tx) => {
				await tx.query('DELETE FROM a');
				throw new Error('boom');
			})
		).rejects.toThrow('boom');
		expect(queries).toEqual(['BEGIN', 'DELETE FROM a', 'ROLLBACK']);
		expect(isReleased()).toBe(true);
	});

	it('issues ROLLBACK when an inner query throws', async () => {
		const { client, queries, isReleased } = makeMockClient();
		await expect(
			withTransaction(client, async (tx) => {
				await tx.query('THROW');
			})
		).rejects.toThrow('forced');
		expect(queries).toEqual(['BEGIN', 'THROW', 'ROLLBACK']);
		expect(isReleased()).toBe(true);
	});

	it('releases the connection even when ROLLBACK itself throws (best-effort)', async () => {
		const client = {
			pool: {
				async connect() {
					return {
						query: async (text) => {
							if (text === 'ROLLBACK') throw new Error('rollback failed');
							if (text === 'WORK') throw new Error('original failure');
							return { rows: [], rowCount: 0 };
						},
						release: () => {}
					};
				}
			}
		};
		// Original error is preserved despite ROLLBACK throwing.
		await expect(
			withTransaction(client, async (tx) => {
				await tx.query('WORK');
			})
		).rejects.toThrow('original failure');
	});
});
