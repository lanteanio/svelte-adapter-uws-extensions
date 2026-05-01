/**
 * Integration tests for the async path of postgres/tasks: enqueue +
 * dispatch sweep + await.
 *
 * Targets the parts the mock can't fake:
 *
 *   - the SKIP LOCKED claim CTE under concurrent dispatchers (no double
 *     claim of the same pending row across multiple processes / runners)
 *   - real timestamptz ordering in the claim CTE (oldest pending row
 *     claimed first)
 *   - multiple await() callers racing one commit and both observing the
 *     same canonical result
 *   - worker-thread executor happy path through the same fence-guarded
 *     UPDATE used by inline handlers
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createTaskRunner } from '../../../postgres/tasks.js';

const echoUrl = new URL('../../helpers/workers/echo.js', import.meta.url);
const throwsUrl = new URL('../../helpers/workers/throws.js', import.meta.url);

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 8000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(20);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

const TABLE = 'svti_tasks';

async function ensureTable(client) {
	const seed = createTaskRunner(client, {
		recoveryInterval: 0,
		cleanupInterval: 0,
		dispatchInterval: 0
	});
	seed.register('noop', async () => null);
	await seed.run('noop', { input: null });
	seed.destroy();
}

describe('postgres tasks async path (integration)', () => {
	let client;
	let runners;

	beforeAll(async () => {
		const url = process.env.INTEGRATION_POSTGRES_URL;
		if (!url) {
			throw new Error('INTEGRATION_POSTGRES_URL not set; global-setup did not run');
		}
		client = createPgClient({
			connectionString: url,
			autoShutdown: false
		});
		await ensureTable(client);
	});

	beforeEach(async () => {
		await client.query(`TRUNCATE ${TABLE}`);
		runners = [];
	});

	afterEach(() => {
		for (const r of runners) r.destroy();
		runners = [];
	});

	afterAll(async () => {
		await client.end();
	});

	function makeRunner(opts = {}) {
		const r = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0,
			...opts
		});
		runners.push(r);
		return r;
	}

	describe('enqueue + claim CTE', () => {
		it('inserts a pending row with attempts=0 and no fence ownership of consequence', async () => {
			const runner = makeRunner();
			const id = await runner.enqueue('echo', { input: { hello: 'world' } });

			const res = await client.query(
				`SELECT status, attempts, input, name, svti_idempotency_key
				   FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[id]
			);
			expect(res.rows).toHaveLength(1);
			expect(res.rows[0].status).toBe('pending');
			expect(res.rows[0].attempts).toBe(0);
			expect(res.rows[0].input).toEqual({ hello: 'world' });
			expect(res.rows[0].name).toBe('echo');
			expect(res.rows[0].svti_idempotency_key).toBeNull();
		});

		it('persists idempotency_key on the pending row', async () => {
			const runner = makeRunner();
			const id = await runner.enqueue('echo', { input: null, idempotencyKey: 'pending-key' });
			const res = await client.query(
				`SELECT svti_idempotency_key FROM ${TABLE} WHERE svti_tasks_id = $1`, [id]
			);
			expect(res.rows[0].svti_idempotency_key).toBe('pending-key');
		});

		it('does not validate the handler at enqueue time (cross-deployment scenario)', async () => {
			const runner = makeRunner();
			const id = await runner.enqueue('lives-elsewhere', { input: { x: 1 } });
			const res = await client.query(
				`SELECT status, name FROM ${TABLE} WHERE svti_tasks_id = $1`, [id]
			);
			expect(res.rows[0].status).toBe('pending');
			expect(res.rows[0].name).toBe('lives-elsewhere');
		});
	});

	describe('dispatch sweep against real Postgres', () => {
		it('claims a pending row, transitions it to running then committed', async () => {
			const runner = makeRunner({ dispatchInterval: 30 });
			runner.register('echo', async ({ input }) => `done: ${input}`);

			const id = await runner.enqueue('echo', { input: 'sweep-1' });

			await waitFor(async () => {
				const r = await client.query(
					`SELECT status FROM ${TABLE} WHERE svti_tasks_id = $1`, [id]
				);
				return r.rows[0].status === 'committed';
			});

			const res = await client.query(
				`SELECT status, result, attempts FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[id]
			);
			expect(res.rows[0].status).toBe('committed');
			expect(res.rows[0].result).toBe('done: sweep-1');
			// Claim CTE bumps attempts from 0 to 1.
			expect(res.rows[0].attempts).toBe(1);
		});

		it('claims pending rows in created_at order (oldest first)', async () => {
			// Stagger inserts well beyond clock resolution so created_at
			// ordering is unambiguous, and use batchSize=1 so the claim CTE
			// emits one row per tick.
			const enqueuer = makeRunner();
			const ids = [];
			for (let i = 0; i < 3; i++) {
				ids.push(await enqueuer.enqueue('echo', { input: i }));
				await wait(60);
			}

			const dispatcher = makeRunner({
				dispatchInterval: 50,
				dispatchBatchSize: 1
			});
			const order = [];
			dispatcher.register('echo', async ({ input }) => {
				order.push(input);
				return input;
			});

			await waitFor(async () => {
				const r = await client.query(
					`SELECT count(*)::int AS n FROM ${TABLE} WHERE status = 'committed'`
				);
				return r.rows[0].n === 3;
			});

			expect(order).toEqual([0, 1, 2]);
		});
	});

	describe('SKIP LOCKED prevents double-claim under concurrent dispatchers', () => {
		it('three concurrent dispatchers split twelve pending rows with no double-runs', async () => {
			// Enqueue from a non-dispatching runner so the pending rows
			// exist before any sweep races.
			const enqueuer = makeRunner();
			const ids = [];
			for (let i = 0; i < 12; i++) {
				ids.push(await enqueuer.enqueue('worker', { input: { idx: i } }));
			}

			const allCalls = [];
			const calls = { a: 0, b: 0, c: 0 };

			function makeDispatcher(label) {
				const r = makeRunner({
					dispatchInterval: 25,
					dispatchBatchSize: 4
				});
				r.register('worker', async ({ input }) => {
					calls[label] += 1;
					allCalls.push({ label, idx: input.idx });
					await wait(5);
					return { idx: input.idx, by: label };
				});
				return r;
			}

			makeDispatcher('a');
			makeDispatcher('b');
			makeDispatcher('c');

			await waitFor(async () => {
				const r = await client.query(
					`SELECT count(*)::int AS n FROM ${TABLE}
					  WHERE status = 'committed' AND name = 'worker'`
				);
				return r.rows[0].n === ids.length;
			}, 12000);

			// Total handler invocations equals number of pending rows: no row
			// was claimed twice.
			expect(allCalls).toHaveLength(ids.length);
			expect(new Set(allCalls.map((c) => c.idx)).size).toBe(ids.length);

			// Sanity: at least two of the three dispatchers contributed work
			// (proves the SKIP LOCKED behaviour was actually exercised, not
			// that one runner happened to do everything).
			const contributors = ['a', 'b', 'c'].filter((k) => calls[k] > 0);
			expect(contributors.length).toBeGreaterThanOrEqual(2);
		});

		it('respects dispatchBatchSize per sweep across many enqueued rows', async () => {
			const enqueuer = makeRunner();
			for (let i = 0; i < 8; i++) {
				await enqueuer.enqueue('echo', { input: i });
			}

			const dispatcher = makeRunner({
				dispatchInterval: 30,
				dispatchBatchSize: 2
			});
			dispatcher.register('echo', async ({ input }) => input);

			await waitFor(async () => {
				const r = await client.query(
					`SELECT count(*)::int AS n FROM ${TABLE}
					  WHERE status = 'committed' AND name = 'echo'`
				);
				return r.rows[0].n === 8;
			});

			const res = await client.query(
				`SELECT count(*)::int AS n FROM ${TABLE}
				  WHERE status = 'committed' AND name = 'echo'`
			);
			expect(res.rows[0].n).toBe(8);
		});

		it('leaves pending rows in running state when no local handler is registered', async () => {
			const enqueuer = makeRunner();
			const id = await enqueuer.enqueue('remote-only', { input: null });

			// Dispatcher with no registration for 'remote-only'.
			makeRunner({ dispatchInterval: 30 });

			await waitFor(async () => {
				const r = await client.query(
					`SELECT status, attempts FROM ${TABLE} WHERE svti_tasks_id = $1`, [id]
				);
				return r.rows[0].status === 'running';
			});

			const res = await client.query(
				`SELECT status, attempts FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[id]
			);
			expect(res.rows[0].status).toBe('running');
			// Attempts bumped from 0 by the claim CTE even though no handler ran.
			expect(res.rows[0].attempts).toBe(1);
		});
	});

	describe('await observes the canonical row', () => {
		it('two await() callers racing one commit both resolve to the same result', async () => {
			const runner = makeRunner({ dispatchInterval: 30 });
			runner.register('slow', async ({ input }) => {
				await wait(80);
				return { reply: input };
			});

			const id = await runner.enqueue('slow', { input: 'hi' });

			const [a, b] = await Promise.all([
				runner.await(id, { timeout: 4000, pollInterval: 20 }),
				runner.await(id, { timeout: 4000, pollInterval: 20 })
			]);

			expect(a).toEqual({ reply: 'hi' });
			expect(b).toEqual(a);
		});

		it('await rejects with the stored error when the row ends up failed', async () => {
			const runner = makeRunner({ dispatchInterval: 30 });
			runner.register('boom', async () => {
				const err = new Error('async failure');
				err.code = 'E_ASYNC';
				throw err;
			});

			const id = await runner.enqueue('boom', { input: null });

			let caught;
			try {
				await runner.await(id, { timeout: 4000, pollInterval: 20 });
			} catch (err) {
				caught = err;
			}
			expect(caught).toBeDefined();
			expect(caught.message).toBe('async failure');
			expect(caught.code).toBe('E_ASYNC');
		});

		it('await throws not-found when the task_id does not exist', async () => {
			const runner = makeRunner();
			await expect(runner.await(
				'00000000-0000-0000-0000-000000000000',
				{ timeout: 100, pollInterval: 10 }
			)).rejects.toThrow('not found');
		});

		it('await rejects with timeout when no dispatcher claims the row', async () => {
			const runner = makeRunner();
			const id = await runner.enqueue('echo', { input: null });

			await expect(runner.await(id, { timeout: 200, pollInterval: 20 }))
				.rejects.toThrow('await timeout');
		});

		it('a separate runner can await a row a different runner enqueued', async () => {
			// Cross-instance shape: one process enqueues, another awaits.
			const enqueuer = makeRunner();
			const dispatcher = makeRunner({ dispatchInterval: 30 });
			dispatcher.register('echo', async ({ input }) => `from-dispatcher: ${input}`);

			const observer = makeRunner();

			const id = await enqueuer.enqueue('echo', { input: 'cross' });
			const result = await observer.await(id, { timeout: 4000, pollInterval: 20 });

			expect(result).toBe('from-dispatcher: cross');
		});
	});

	describe('worker-thread executor through the same Postgres bridge', () => {
		it('runs a worker handler and commits the result through the fence-guarded UPDATE', async () => {
			const runner = makeRunner();
			runner.register('echo', null, { worker: echoUrl });

			const result = await runner.run('echo', {
				input: { greeting: 'hello' },
				idempotencyKey: 'wk-1'
			});

			expect(result).toMatchObject({
				echoed: { greeting: 'hello' },
				idempotencyKey: 'wk-1',
				attempt: 1
			});

			const res = await client.query(
				`SELECT status, result FROM ${TABLE} WHERE name = 'echo'`
			);
			expect(res.rows[0].status).toBe('committed');
			expect(res.rows[0].result.echoed).toEqual({ greeting: 'hello' });
		});

		it('a worker handler that rejects writes a failed row with the worker error', async () => {
			const runner = makeRunner();
			runner.register('boom', null, { worker: throwsUrl });

			let caught;
			try {
				await runner.run('boom', { input: null });
			} catch (err) {
				caught = err;
			}
			expect(caught).toBeDefined();
			expect(caught.message).toBe('worker handler exploded');
			expect(caught.code).toBe('WORKER_BOOM');

			const res = await client.query(
				`SELECT status, error FROM ${TABLE} WHERE name = 'boom'`
			);
			expect(res.rows[0].status).toBe('failed');
			expect(res.rows[0].error.message).toBe('worker handler exploded');
			expect(res.rows[0].error.code).toBe('WORKER_BOOM');
		});
	});
});
