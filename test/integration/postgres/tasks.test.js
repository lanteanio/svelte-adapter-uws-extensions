/**
 * Integration tests for postgres/tasks against a real Postgres 16 server.
 *
 * Exercises the synchronous run() + await() path against actual SQL: row
 * insertion with status='running', fence-guarded conditional commit, the
 * jsonb round-trip for input/result/error columns, and the read-row contract.
 * The mock-based suite at test/postgres/tasks.test.js stays alive and
 * covers pure JS validation; this file covers what the mock cannot fake.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createTaskRunner, UnknownTaskError, TaskInFlightError } from '../../../postgres/tasks.js';
import { createIdempotencyStore } from '../../../postgres/idempotency.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

const TABLE = 'svti_tasks';

describe('postgres tasks core (integration)', () => {
	let client;
	let runner;

	beforeAll(() => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Ensure the table exists then wipe between tests so rows do not
		// leak across cases. Using an inline runner with autoMigrate=true
		// guarantees the schema is present before TRUNCATE.
		const seed = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		seed.register('noop', async () => null);
		await seed.run('noop', { input: null });
		seed.destroy();

		await client.query(`TRUNCATE ${TABLE}`);
	});

	afterEach(() => {
		if (runner) {
			runner.destroy();
			runner = undefined;
		}
	});

	afterAll(async () => {
		await client.end();
	});

	describe('concurrent first-use (CREATE TABLE race-safety)', () => {
		it('two fresh runners racing their first run() both succeed', async () => {
			// CREATE TABLE IF NOT EXISTS races on concurrent first calls.
			// ensureTable() catches 23505 / 42P07 / 42710 and treats the
			// table as already-existing.
			await client.query(`DROP TABLE IF EXISTS ${TABLE}`);

			const a = createTaskRunner(client, {
				recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0
			});
			const b = createTaskRunner(client, {
				recoveryInterval: 0, cleanupInterval: 0, dispatchInterval: 0
			});
			a.register('echo', async ({ input }) => input);
			b.register('echo', async ({ input }) => input);

			const [resA, resB] = await Promise.all([
				a.run('echo', { input: { who: 'a' } }),
				b.run('echo', { input: { who: 'b' } })
			]);

			expect(resA).toEqual({ who: 'a' });
			expect(resB).toEqual({ who: 'b' });

			a.destroy();
			b.destroy();
		});
	});

	describe('run + commit', () => {
		it('runs a registered handler and returns its result', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('add', async ({ input }) => input.a + input.b);

			const result = await runner.run('add', { input: { a: 2, b: 3 } });
			expect(result).toBe(5);
		});

		it('writes a row with status=committed and the result in the jsonb column', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('echo', async ({ input }) => ({ echoed: input }));

			await runner.run('echo', { input: { greeting: 'hello' } });

			const res = await client.query(`SELECT status, input, result FROM ${TABLE}`);
			expect(res.rows).toHaveLength(1);
			expect(res.rows[0].status).toBe('committed');
			expect(res.rows[0].input).toEqual({ greeting: 'hello' });
			expect(res.rows[0].result).toEqual({ echoed: { greeting: 'hello' } });
		});

		it('round-trips complex jsonb input and result through real postgres', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('shape', async ({ input }) => ({
				received: input,
				meta: { keys: Object.keys(input).length, ts: '2026-01-01' }
			}));

			const input = {
				nested: { a: [1, 2, 3], b: null, c: true },
				utf8: 'cafe',
				empty: {}
			};
			const result = await runner.run('shape', { input });

			expect(result).toEqual({
				received: input,
				meta: { keys: 3, ts: '2026-01-01' }
			});

			const res = await client.query(`SELECT input, result FROM ${TABLE}`);
			expect(res.rows[0].input).toEqual(input);
			expect(res.rows[0].result.received).toEqual(input);
		});

		it('passes a fresh fence UUID and an AbortSignal to the handler', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});

			let captured;
			runner.register('peek', async (ctx) => {
				captured = ctx;
				return 'ok';
			});

			await runner.run('peek', { input: null });

			expect(typeof captured.fence).toBe('string');
			// UUID v4 string length is 36 characters.
			expect(captured.fence).toHaveLength(36);
			expect(captured.signal).toBeInstanceOf(AbortSignal);
			expect(captured.attempt).toBe(1);
		});

		it('forwards idempotencyKey to the handler context and column', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});

			let seen;
			runner.register('echo', async ({ idempotencyKey }) => {
				seen = idempotencyKey;
				return 'ok';
			});

			await runner.run('echo', { input: null, idempotencyKey: 'stable-key-1' });
			expect(seen).toBe('stable-key-1');

			const res = await client.query(`SELECT svti_idempotency_key FROM ${TABLE}`);
			expect(res.rows[0].svti_idempotency_key).toBe('stable-key-1');
		});

		it('persists requestId on the row and exposes ctx.requestId to the handler', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});

			let seen;
			runner.register('echo', async ({ requestId }) => {
				seen = requestId;
				return 'ok';
			});

			await runner.run('echo', { input: null, requestId: 'req-end-to-end' });
			expect(seen).toBe('req-end-to-end');

			const res = await client.query(`SELECT request_id FROM ${TABLE}`);
			expect(res.rows[0].request_id).toBe('req-end-to-end');
		});

		it('extracts requestId from a passed platform.requestId', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});

			let seen;
			runner.register('echo', async ({ requestId }) => { seen = requestId; });
			await runner.run('echo', { input: null, platform: { requestId: 'req-platform-real' } });

			expect(seen).toBe('req-platform-real');
			const res = await client.query(`SELECT request_id FROM ${TABLE}`);
			expect(res.rows[0].request_id).toBe('req-platform-real');
		});
	});

	describe('run + fail', () => {
		it('marks the row as failed with serialised error in the jsonb column', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('boom', async () => {
				const err = new Error('handler exploded');
				err.code = 'E_FAIL';
				throw err;
			});

			await expect(runner.run('boom', { input: null })).rejects.toThrow('handler exploded');

			const res = await client.query(`SELECT status, error FROM ${TABLE}`);
			expect(res.rows).toHaveLength(1);
			expect(res.rows[0].status).toBe('failed');
			expect(res.rows[0].error.message).toBe('handler exploded');
			expect(res.rows[0].error.name).toBe('Error');
			expect(res.rows[0].error.code).toBe('E_FAIL');
			expect(typeof res.rows[0].error.stack).toBe('string');
		});

		it('throws UnknownTaskError when no handler is registered', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			await expect(runner.run('missing', { input: null })).rejects.toThrow(UnknownTaskError);
		});
	});

	describe('retry policy under real postgres', () => {
		it('retries up to maxAttempts, rotates the fence each attempt, and commits on success', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});

			const seenFences = [];
			runner.register('flaky', async ({ attempt, fence }) => {
				seenFences.push(fence);
				if (attempt < 3) throw new Error('try again');
				return 'finally';
			}, {
				retry: { maxAttempts: 3, backoff: () => 0 }
			});

			const result = await runner.run('flaky', { input: null });
			expect(result).toBe('finally');
			expect(seenFences).toHaveLength(3);
			// Each attempt rotates the fence.
			expect(new Set(seenFences).size).toBe(3);

			const res = await client.query(`SELECT status, attempts, fence FROM ${TABLE}`);
			expect(res.rows[0].status).toBe('committed');
			expect(res.rows[0].attempts).toBe(3);
			// Final fence in the row matches the last seen fence.
			expect(res.rows[0].fence).toBe(seenFences[2]);
		});

		it('records attempts=N when retries are exhausted', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('always', async () => {
				throw new Error('persistent');
			}, {
				retry: { maxAttempts: 2, backoff: () => 0 }
			});

			await expect(runner.run('always', { input: null })).rejects.toThrow('persistent');

			const res = await client.query(`SELECT status, attempts FROM ${TABLE}`);
			expect(res.rows[0].status).toBe('failed');
			expect(res.rows[0].attempts).toBe(2);
		});
	});

	describe('schema and indexes', () => {
		it('creates the table with the expected columns', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('echo', async () => null);
			// Force migration to run.
			await runner.run('echo', { input: null });

			const res = await client.query(`
				SELECT column_name, data_type
				  FROM information_schema.columns
				 WHERE table_name = $1
				 ORDER BY ordinal_position
			`, [TABLE]);

			const cols = Object.fromEntries(res.rows.map((r) => [r.column_name, r.data_type]));
			expect(cols.svti_tasks_id).toBe('uuid');
			expect(cols.name).toBe('text');
			expect(cols.input).toBe('jsonb');
			expect(cols.status).toBe('text');
			expect(cols.result).toBe('jsonb');
			expect(cols.error).toBe('jsonb');
			expect(cols.fence).toBe('uuid');
			expect(cols.fence_expires_at).toBe('timestamp with time zone');
			expect(cols.attempts).toBe('integer');
		});

		it('creates the partial indexes on running and terminal rows', async () => {
			runner = createTaskRunner(client, {
				recoveryInterval: 0,
				cleanupInterval: 0,
				dispatchInterval: 0
			});
			runner.register('echo', async () => null);
			await runner.run('echo', { input: null });

			const res = await client.query(`
				SELECT indexname FROM pg_indexes
				 WHERE tablename = $1
				 ORDER BY indexname
			`, [TABLE]);
			const names = res.rows.map((r) => r.indexname);
			expect(names).toContain(`idx_${TABLE}_running_fence`);
			expect(names).toContain(`idx_${TABLE}_terminal_updated`);
		});
	});

	describe('idempotency store + task runner across two instances', () => {
		// Both runners share one Postgres-backed idempotency store. The
		// store is the cross-instance coordination point: only one runner
		// runs the handler; the other observes 'pending' or the cached
		// 'committed' result.
		let store;
		let runnerA;
		let runnerB;

		beforeEach(async () => {
			store = createIdempotencyStore(client, {
				cleanupInterval: 0,
				autoMigrate: true
			});
			// Wipe the idempotency table too. ensureTable() runs implicitly
			// on first acquire(); pre-clean so prior tests do not leak.
			try {
				await client.query('TRUNCATE svti_idempotency');
			} catch {
				// Table may not exist yet; ensure it then truncate.
				await store.acquire('seed-key').then((s) => s.acquired && s.abort());
				await client.query('TRUNCATE svti_idempotency');
			}
		});

		afterEach(() => {
			if (runnerA) { runnerA.destroy(); runnerA = undefined; }
			if (runnerB) { runnerB.destroy(); runnerB = undefined; }
			if (store) { store.destroy(); store = undefined; }
		});

		it('only one runner executes; the other returns the cached result', async () => {
			let aRanCount = 0;
			let bRanCount = 0;
			runnerA = createTaskRunner(client, {
				idempotency: store,
				recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
			});
			runnerB = createTaskRunner(client, {
				idempotency: store,
				recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
			});
			runnerA.register('echo', async ({ input }) => {
				aRanCount += 1;
				return { who: 'A', echoed: input };
			});
			runnerB.register('echo', async ({ input }) => {
				bRanCount += 1;
				return { who: 'B', echoed: input };
			});

			// First call lands on A, runs, commits to the idempotency store.
			const first = await runnerA.run('echo', {
				input: { v: 1 }, idempotencyKey: 'idem-shared'
			});
			expect(first).toEqual({ who: 'A', echoed: { v: 1 } });
			expect(aRanCount).toBe(1);
			expect(bRanCount).toBe(0);

			// Second call from a DIFFERENT runner with the same key returns
			// the cached result -- B's handler does NOT run.
			const second = await runnerB.run('echo', {
				input: { v: 2 }, idempotencyKey: 'idem-shared'
			});
			expect(second).toEqual({ who: 'A', echoed: { v: 1 } });
			expect(bRanCount).toBe(0);
		});

		it('a concurrent in-flight call from another instance throws TaskInFlightError', async () => {
			runnerA = createTaskRunner(client, {
				idempotency: store,
				recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
			});
			runnerB = createTaskRunner(client, {
				idempotency: store,
				recoveryInterval: 0, dispatchInterval: 0, cleanupInterval: 0
			});
			runnerA.register('slow', async () => {
				await wait(300);
				return { ok: true };
			});
			runnerB.register('slow', async () => {
				return { unexpected: true };
			});

			const aPromise = runnerA.run('slow', {
				input: null, idempotencyKey: 'idem-inflight'
			});
			// Give A time to acquire the slot before B tries.
			await wait(50);

			await expect(
				runnerB.run('slow', { input: null, idempotencyKey: 'idem-inflight' })
			).rejects.toBeInstanceOf(TaskInFlightError);

			// A's promise still completes successfully.
			const aResult = await aPromise;
			expect(aResult).toEqual({ ok: true });

			// After A commits, a fresh call from B returns the cached result
			// rather than throwing again.
			const replay = await runnerB.run('slow', {
				input: null, idempotencyKey: 'idem-inflight'
			});
			expect(replay).toEqual({ ok: true });
		});
	});
});
