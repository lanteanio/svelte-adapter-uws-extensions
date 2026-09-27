import { describe, it, expect } from 'vitest';
import { EventEmitter } from 'node:events';
import { withTransaction } from '../../src/shared/pg-tx.js';
import { EXTENSIONS_ERROR_IDS } from '../../src/error-registry.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

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

	// pg-pool returns a client to the idle pool when release() gets no error
	// and destroys it when release(err) is truthy. A connection whose
	// transaction state is uncertain must take the destroy path.

	function makeFailingClient(failOn) {
		let releaseArg = 'never-called';
		const client = {
			pool: {
				async connect() {
					return {
						query: async (text) => {
							if (failOn.includes(text)) throw new Error(text + ' failed');
							return { rows: [], rowCount: 0 };
						},
						release: (err) => { releaseArg = err; }
					};
				}
			}
		};
		return { client, getReleaseArg: () => releaseArg };
	}

	it('releases WITH the error (pool destroys) when ROLLBACK itself fails - original error preserved', async () => {
		const { client, getReleaseArg } = makeFailingClient(['ROLLBACK', 'WORK']);
		await expect(
			withTransaction(client, async (tx) => {
				await tx.query('WORK');
			})
		).rejects.toThrow('WORK failed');
		expect(getReleaseArg()).toBeInstanceOf(Error);
		expect(getReleaseArg().message).toBe('ROLLBACK failed');
	});

	it('releases WITH the error when COMMIT fails', async () => {
		const { client, getReleaseArg } = makeFailingClient(['COMMIT']);
		await expect(withTransaction(client, async () => 1)).rejects.toThrow('COMMIT failed');
		expect(getReleaseArg()).toBeInstanceOf(Error);
	});

	it('releases WITH the error when BEGIN fails', async () => {
		const { client, getReleaseArg } = makeFailingClient(['BEGIN']);
		await expect(withTransaction(client, async () => 1)).rejects.toThrow('BEGIN failed');
		expect(getReleaseArg()).toBeInstanceOf(Error);
	});

	// A checked-out pg client is an EventEmitter whose 'error' event throws
	// when nothing listens, which is what pg emits when the backend ends the
	// connection mid-transaction.
	function makeDroppingClient() {
		const conn = new EventEmitter();
		let releaseArg = 'never-called';
		conn.query = async (text) => {
			if (text === 'WORK') {
				const err = new Error('terminating connection due to administrator command');
				conn.emit('error', err);
				throw err;
			}
			return { rows: [], rowCount: 0 };
		};
		conn.release = (err) => { releaseArg = err; };
		return { client: { pool: { connect: async () => conn } }, conn, getReleaseArg: () => releaseArg };
	}

	it('survives the connection dropping mid-transaction: the work rejects, the failure is reported, the connection is discarded', async () => {
		const { client, conn, getReleaseArg } = makeDroppingClient();
		const sink = captureRegistered();
		try {
			await expect(withTransaction(client, async (tx) => {
				await tx.query('WORK');
			})).rejects.toThrow('terminating connection');
			expect(sink.ids()).toEqual([EXTENSIONS_ERROR_IDS.POSTGRES_TRANSACTION_CLIENT]);
			expect(sink.records[0].attributes.error.message).toBe('terminating connection due to administrator command');
		} finally {
			sink.stop();
		}
		expect(getReleaseArg()).toBeInstanceOf(Error);
		// The listener lives only as long as the checkout.
		expect(conn.listenerCount('error')).toBe(0);
	});

	it('removes its listener when the transaction commits', async () => {
		const { client, conn, getReleaseArg } = makeDroppingClient();
		await withTransaction(client, async (tx) => {
			expect(conn.listenerCount('error')).toBe(1);
			await tx.query('SELECT 1');
		});
		expect(conn.listenerCount('error')).toBe(0);
		expect(getReleaseArg()).toBeUndefined();
	});

	it('releases with NO error after a successful ROLLBACK (connection stays pooled)', async () => {
		const { client, getReleaseArg } = makeFailingClient([]);
		await expect(
			withTransaction(client, async () => {
				throw new Error('work error');
			})
		).rejects.toThrow('work error');
		expect(getReleaseArg()).toBeUndefined();
	});
});
