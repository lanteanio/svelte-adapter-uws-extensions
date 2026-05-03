/**
 * Integration tests for postgres/tasks worker-thread mode against a real
 * Postgres 16 server.
 *
 * Pins the worker-pool path end-to-end: handler runs in a real
 * `worker_threads` Worker spawned by `_tasks-worker-pool.js`, the result
 * round-trips through the Postgres state machine (insert pending ->
 * running -> committed), and the row's `result` column reflects what the
 * worker returned. The mock-based suite at test/postgres/tasks-worker.test.js
 * covers the in-memory pool surface against a fake pg client; this file
 * covers the joint behavior with real DB transitions.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createPgClient } from '../../../postgres/index.js';
import { createTaskRunner } from '../../../postgres/tasks.js';

const echoUrl = new URL('../../helpers/workers/echo.js', import.meta.url);
const slowUrl = new URL('../../helpers/workers/slow.js', import.meta.url);
const throwsUrl = new URL('../../helpers/workers/throws.js', import.meta.url);
const flakyUrl = new URL('../../helpers/workers/flaky.js', import.meta.url);

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

const TABLE = 'svti_tasks';

describe('postgres tasks worker-thread mode (integration)', () => {
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
		// leak. Mirrors the pattern in tasks.test.js.
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

	it('runs a worker-thread handler and round-trips the result through Postgres', async () => {
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		runner.register('echo', null, { worker: echoUrl });

		const result = await runner.run('echo', {
			input: { greeting: 'hello-from-worker' }
		});
		expect(result.echoed).toEqual({ greeting: 'hello-from-worker' });
		expect(typeof result.fence).toBe('string');
		expect(result.attempt).toBe(1);

		const res = await client.query(`SELECT status, input, result FROM ${TABLE}`);
		expect(res.rows).toHaveLength(1);
		expect(res.rows[0].status).toBe('committed');
		expect(res.rows[0].input).toEqual({ greeting: 'hello-from-worker' });
		expect(res.rows[0].result.echoed).toEqual({ greeting: 'hello-from-worker' });
	});

	it('serializes two concurrent runs against a worker pool of size 1', async () => {
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		runner.register('slow', null, {
			worker: { path: slowUrl, pool: { size: 1, idleTimeout: 0 } }
		});

		const start = Date.now();
		const [a, b] = await Promise.all([
			runner.run('slow', { input: { delayMs: 200 } }),
			runner.run('slow', { input: { delayMs: 200 } })
		]);
		const elapsed = Date.now() - start;

		expect(a).toMatchObject({ ok: true });
		expect(b).toMatchObject({ ok: true });
		// Pool size 1 means the second run waits for the first; total
		// elapsed must be at least the sum of the two delays.
		expect(elapsed).toBeGreaterThanOrEqual(380);

		// Both rows in Postgres reached committed status.
		const res = await client.query(
			`SELECT status FROM ${TABLE} ORDER BY created_at ASC`
		);
		expect(res.rows).toHaveLength(2);
		expect(res.rows.every((r) => r.status === 'committed')).toBe(true);
	});

	it('overlaps two concurrent runs when the worker pool size is 2', async () => {
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		runner.register('slow', null, {
			worker: { path: slowUrl, pool: { size: 2, idleTimeout: 0 } }
		});

		const start = Date.now();
		await Promise.all([
			runner.run('slow', { input: { delayMs: 250 } }),
			runner.run('slow', { input: { delayMs: 250 } })
		]);
		const elapsed = Date.now() - start;

		// Both workers run in parallel; total elapsed should be close to
		// one delay, not two. Allow generous slack for worker spawn cost
		// (Windows + first-call import).
		expect(elapsed).toBeLessThan(900);

		const res = await client.query(`SELECT status FROM ${TABLE}`);
		expect(res.rows).toHaveLength(2);
		expect(res.rows.every((r) => r.status === 'committed')).toBe(true);
	});

	it('records a Postgres failed row when a worker handler throws', async () => {
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		runner.register('boom', null, { worker: throwsUrl });

		await expect(
			runner.run('boom', { input: { reason: 'integration-test' } })
		).rejects.toThrow();

		const res = await client.query(
			`SELECT status, error FROM ${TABLE}`
		);
		expect(res.rows).toHaveLength(1);
		expect(res.rows[0].status).toBe('failed');
		// `error` is stored as jsonb with a serialised Error envelope; just
		// pin that something landed there rather than the exact shape.
		expect(res.rows[0].error).not.toBeNull();
	});

	it('retries a worker handler that throws on attempt 1 and commits on attempt 2', async () => {
		runner = createTaskRunner(client, {
			recoveryInterval: 0,
			cleanupInterval: 0,
			dispatchInterval: 0
		});
		runner.register('flaky', null, {
			worker: flakyUrl,
			retry: { maxAttempts: 3 }
		});

		const result = await runner.run('flaky', { input: { mode: 'throw' } });
		expect(result).toEqual({ recovered: true, attempt: 2 });

		const res = await client.query(`SELECT status, attempts, result FROM ${TABLE}`);
		expect(res.rows).toHaveLength(1);
		expect(res.rows[0].status).toBe('committed');
		expect(res.rows[0].attempts).toBe(2);
		expect(res.rows[0].result).toEqual({ recovered: true, attempt: 2 });
	});

	it('recovery sweep reclaims a row whose fence has elapsed and re-dispatches to the worker handler', async () => {
		// Simulates the "instance died" scenario: a row exists in 'running'
		// state with fence_expires_at < now() because the prior owner's
		// heartbeat stopped refreshing it. The surviving instance's
		// recovery sweep observes the elapsed fence, claims the row
		// (bumping attempts and rotating the fence), and dispatches the
		// handler -- which here is a worker-thread handler.
		//
		// Plants the stuck row directly via SQL so the test does not
		// depend on simulating a worker crash (the pool's exit handler
		// does not reject in-flight slots on a clean process.exit, which
		// makes a true mid-flight crash hang rather than orphan a row).
		const { randomUUID } = await import('node:crypto');
		const taskId = randomUUID();
		const fence = randomUUID();
		await client.query(
			`INSERT INTO ${TABLE}
			   (svti_tasks_id, name, input, status, fence, fence_expires_at, attempts)
			 VALUES ($1, $2, $3::jsonb, 'running', $4, now() - interval '5 seconds', 1)`,
			[taskId, 'flaky', JSON.stringify({ mode: 'throw' }), fence]
		);

		runner = createTaskRunner(client, {
			fenceTtl: 5,
			recoveryInterval: 200,
			dispatchInterval: 0,
			cleanupInterval: 0
		});
		runner.register('flaky', null, {
			worker: flakyUrl,
			retry: { maxAttempts: 1 }
		});

		// Wait for one or two recovery ticks + worker spawn + handler run.
		await wait(1500);

		const res = await client.query(
			`SELECT status, attempts, result FROM ${TABLE} WHERE svti_tasks_id = $1`,
			[taskId]
		);
		expect(res.rows).toHaveLength(1);
		expect(res.rows[0].status).toBe('committed');
		expect(res.rows[0].attempts).toBeGreaterThanOrEqual(2);
		expect(res.rows[0].result).toMatchObject({ recovered: true });
	}, 15_000);
});
