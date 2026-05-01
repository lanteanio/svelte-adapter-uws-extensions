/**
 * Integration tests for the fence-guarded state machine in postgres/tasks.
 *
 * Targets the parts that real Postgres timestamptz, conditional UPDATE
 * row counts, and the SKIP LOCKED reclaim CTE actually enforce:
 *
 *   - heartbeat extends fence_expires_at past now() in real time
 *   - commit / fail UPDATEs only succeed when fence matches the current row
 *   - the reclaim sweep rotates the fence and bumps attempts on stuck rows
 *   - concurrent reclaim sweeps each pick disjoint rows, never the same one
 *
 * The mock-based tests at test/postgres/tasks-fence.test.js cover the
 * Redis fence provider against an in-memory Redis; this file focuses on
 * what only real Postgres semantics can prove.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createPgClient } from '../../../postgres/index.js';
import { createTaskRunner } from '../../../postgres/tasks.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 5000) {
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

describe('postgres tasks fence semantics (integration)', () => {
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

	describe('heartbeat extends fence_expires_at', () => {
		it('moves fence_expires_at into the future on each heartbeat tick', async () => {
			// fenceTtl=2 sets expiry to now()+2s on insert; heartbeat
			// rewrites it to now()+2s every tick. Fire several ticks across
			// 800ms and confirm the column value advanced relative to the
			// initial value (i.e. the heartbeat did rewrite it, not just
			// hold steady).
			const runner = makeRunner({ fenceTtl: 2, heartbeatInterval: 150 });

			let initialExpiry;
			let observedExpiry;
			runner.register('long', async () => {
				const before = await client.query(`SELECT fence_expires_at FROM ${TABLE} LIMIT 1`);
				initialExpiry = before.rows[0].fence_expires_at;

				await wait(800);

				const after = await client.query(`SELECT fence_expires_at FROM ${TABLE} LIMIT 1`);
				observedExpiry = after.rows[0].fence_expires_at;

				return 'done';
			});

			const result = await runner.run('long', { input: null });
			expect(result).toBe('done');

			// Heartbeats should have rewritten fence_expires_at, pushing it
			// beyond the value captured at handler entry.
			expect(observedExpiry.getTime()).toBeGreaterThan(initialExpiry.getTime());
		});

		it('keeps fence_expires_at strictly greater than now() throughout a long run', async () => {
			const runner = makeRunner({ fenceTtl: 2, heartbeatInterval: 200 });

			let stayedFresh = true;
			runner.register('long', async () => {
				for (let i = 0; i < 6; i++) {
					await wait(150);
					const r = await client.query(
						`SELECT fence_expires_at > now() AS fresh FROM ${TABLE} LIMIT 1`
					);
					if (!r.rows[0].fresh) {
						stayedFresh = false;
						break;
					}
				}
				return 'ok';
			});

			await runner.run('long', { input: null });
			expect(stayedFresh).toBe(true);
		});
	});

	describe('conditional commit / fail with fence', () => {
		it('commit UPDATE affects 0 rows when the fence does not match', async () => {
			// Plant a running row with a known fence.
			const taskId = randomUUID();
			const ourFence = randomUUID();
			const otherFence = randomUUID();

			await client.query(
				`INSERT INTO ${TABLE}
				    (svti_tasks_id, name, input, status, fence, fence_expires_at, attempts)
				 VALUES
				    ($1, 'manual', '{}'::jsonb, 'running', $2, now() + interval '60 seconds', 1)`,
				[taskId, ourFence]
			);

			// Try to commit using a different fence: the conditional UPDATE
			// must miss because fence != $current.
			const res = await client.query(
				`UPDATE ${TABLE}
				    SET status = 'committed',
				        result = '"sneaky"'::jsonb,
				        updated_at = now()
				  WHERE svti_tasks_id = $1 AND fence = $2 AND status = 'running'`,
				[taskId, otherFence]
			);
			expect(res.rowCount).toBe(0);

			// Row still in running state with original fence.
			const after = await client.query(
				`SELECT status, fence, result FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[taskId]
			);
			expect(after.rows[0].status).toBe('running');
			expect(after.rows[0].fence).toBe(ourFence);
			expect(after.rows[0].result).toBeNull();
		});

		it('a runner whose fence has been stolen ends up reading the canonical row', async () => {
			// Start a long handler. While it runs, rotate the fence column
			// directly so the runner's commit UPDATE will affect 0 rows. The
			// runner falls back to readRow() and yields whatever the row
			// currently says.
			const runner = makeRunner({ fenceTtl: 60, heartbeatInterval: 0 });

			let started = false;
			runner.register('slow', async () => {
				started = true;
				await wait(400);
				return 'i-thought-i-was-the-owner';
			});

			const promise = runner.run('slow', { input: null });
			await waitFor(() => started);

			// Steal the fence and pre-commit the row to a different result
			// while the handler is still running.
			await client.query(
				`UPDATE ${TABLE}
				    SET fence = $1,
				        status = 'committed',
				        result = '"stolen-result"'::jsonb,
				        updated_at = now()
				  WHERE name = 'slow'`,
				[randomUUID()]
			);

			// run() resolves with whatever the row currently says when its
			// own commit misses. Because the row is already 'committed' with
			// the stolen result, that wins.
			const value = await promise;
			expect(value).toBe('stolen-result');
		});
	});

	describe('reclaim sweep rotates fence on stuck rows', () => {
		it('reclaims a row whose fence_expires_at is in the past and rotates the fence', async () => {
			// Plant a stuck row directly with an expired fence.
			const taskId = randomUUID();
			const oldFence = randomUUID();
			await client.query(
				`INSERT INTO ${TABLE}
				    (svti_tasks_id, name, input, status, fence, fence_expires_at, attempts, created_at, updated_at)
				 VALUES
				    ($1, 'worker', '{}'::jsonb, 'running', $2, now() - interval '10 seconds', 1, now() - interval '60 seconds', now() - interval '60 seconds')`,
				[taskId, oldFence]
			);

			let calls = 0;
			const runner = makeRunner({ recoveryInterval: 50 });
			runner.register('worker', async () => {
				calls += 1;
				return 'reclaimed';
			});

			await waitFor(async () => {
				const r = await client.query(
					`SELECT status FROM ${TABLE} WHERE svti_tasks_id = $1`, [taskId]
				);
				return r.rows[0].status === 'committed';
			});

			const res = await client.query(
				`SELECT status, fence, attempts, result FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[taskId]
			);
			expect(res.rows[0].status).toBe('committed');
			expect(res.rows[0].fence).not.toBe(oldFence);
			expect(res.rows[0].attempts).toBeGreaterThanOrEqual(2);
			expect(res.rows[0].result).toBe('reclaimed');
			expect(calls).toBeGreaterThanOrEqual(1);
		});

		it('does not reclaim rows whose fence_expires_at is still in the future', async () => {
			const taskId = randomUUID();
			const freshFence = randomUUID();
			await client.query(
				`INSERT INTO ${TABLE}
				    (svti_tasks_id, name, input, status, fence, fence_expires_at, attempts)
				 VALUES
				    ($1, 'worker', '{}'::jsonb, 'running', $2, now() + interval '60 seconds', 1)`,
				[taskId, freshFence]
			);

			let calls = 0;
			const runner = makeRunner({ recoveryInterval: 50 });
			runner.register('worker', async () => {
				calls += 1;
				return 'should-not-run';
			});

			await wait(250);

			const res = await client.query(
				`SELECT status, fence, attempts FROM ${TABLE} WHERE svti_tasks_id = $1`,
				[taskId]
			);
			expect(res.rows[0].status).toBe('running');
			expect(res.rows[0].fence).toBe(freshFence);
			expect(res.rows[0].attempts).toBe(1);
			expect(calls).toBe(0);
		});

		it('two concurrent reclaim sweeps cooperatively claim disjoint stuck rows via SKIP LOCKED', async () => {
			// Plant 6 stuck rows. Spawn two runners with reclaim sweeps; the
			// SKIP LOCKED CTE ensures each row is claimed by exactly one
			// sweep, so the total handler invocation count must equal 6 and
			// each task_id ends up committed exactly once.
			const ids = [];
			for (let i = 0; i < 6; i++) {
				const id = randomUUID();
				ids.push(id);
				await client.query(
					`INSERT INTO ${TABLE}
					    (svti_tasks_id, name, input, status, fence, fence_expires_at, attempts, created_at, updated_at)
					 VALUES
					    ($1, 'worker', $2::jsonb, 'running', gen_random_uuid(),
					     now() - interval '5 seconds', 1,
					     now() - interval '60 seconds', now() - interval '60 seconds')`,
					[id, JSON.stringify({ idx: i })]
				);
			}

			let calls = 0;
			const seenIds = new Set();
			const runnerA = makeRunner({ recoveryInterval: 30, recoveryBatchSize: 10 });
			const runnerB = makeRunner({ recoveryInterval: 30, recoveryBatchSize: 10 });

			const handler = async ({ input }) => {
				calls += 1;
				seenIds.add(input.idx);
				// A small wait lets the second sweep race with the first.
				await wait(10);
				return { idx: input.idx };
			};
			runnerA.register('worker', handler);
			runnerB.register('worker', handler);

			await waitFor(async () => {
				const r = await client.query(
					`SELECT count(*)::int AS n FROM ${TABLE}
					  WHERE status = 'committed' AND name = 'worker'`
				);
				return r.rows[0].n === ids.length;
			}, 8000);

			expect(seenIds.size).toBe(ids.length);
			// Each row claimed exactly once means handler ran exactly N times.
			expect(calls).toBe(ids.length);

			const res = await client.query(
				`SELECT status, attempts FROM ${TABLE} WHERE name = 'worker'`
			);
			expect(res.rows.every((r) => r.status === 'committed')).toBe(true);
			expect(res.rows.every((r) => r.attempts >= 2)).toBe(true);
		});
	});

	describe('terminal-row cleanup', () => {
		it('deletes terminal rows whose updated_at is older than rowTtl', async () => {
			const runner = makeRunner({ rowTtl: 1, cleanupInterval: 50 });
			runner.register('echo', async () => 'ok');

			await runner.run('echo', { input: null });

			// Age the row past rowTtl by manipulating updated_at directly.
			await client.query(
				`UPDATE ${TABLE} SET updated_at = now() - interval '5 seconds' WHERE name = 'echo'`
			);

			await waitFor(async () => {
				const r = await client.query(
					`SELECT count(*)::int AS n FROM ${TABLE} WHERE name = 'echo'`
				);
				return r.rows[0].n === 0;
			});

			const res = await client.query(
				`SELECT count(*)::int AS n FROM ${TABLE} WHERE name = 'echo'`
			);
			expect(res.rows[0].n).toBe(0);
		});

		it('keeps terminal rows that are still within rowTtl', async () => {
			const runner = makeRunner({ rowTtl: 7 * 24 * 3600, cleanupInterval: 50 });
			runner.register('echo', async () => 'ok');

			await runner.run('echo', { input: null });
			await wait(150);

			const res = await client.query(
				`SELECT count(*)::int AS n FROM ${TABLE} WHERE name = 'echo'`
			);
			expect(res.rows[0].n).toBe(1);
		});
	});
});
