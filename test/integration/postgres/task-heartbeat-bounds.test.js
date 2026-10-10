import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createTaskRunner } from '../../../src/postgres/tasks.js';

function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
describe('task heartbeat blocked database renewal', () => {
	let client;
	beforeAll(() => { client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false }); });
	afterAll(async () => { await client.end(); });
	it('aborts within its confidence budget while a row lock prevents renewal and withholds stale success', async () => {
		const table = 'svti_heartbeat_blocked';
		const runner = createTaskRunner(client, { table, fenceTtl: 1, heartbeatInterval: 100, dispatchInterval: 0, recoveryInterval: 0, cleanupInterval: 0 });
		const entered = deferred(), aborted = deferred(), finish = deferred();
		let signal;
		runner.register('blocked', async context => {
			signal = context.signal;
			signal.addEventListener('abort', () => aborted.resolve(), { once: true });
			entered.resolve();
			await finish.promise;
			return 'stale success';
		});
		const outcome = runner.run('blocked', { input: null }).catch(error => error);
		let lock, recovery;
		try {
			await entered.promise;
			lock = await client.connect();
			await lock.query('BEGIN');
			await lock.query(`SELECT svti_tasks_id FROM ${table} FOR UPDATE`);
			const started = Date.now();
			await aborted.promise;
			expect(Date.now() - started).toBeLessThan(1800);
			expect(signal.aborted).toBe(true);
			await lock.query('ROLLBACK');
			lock.release(); lock = null;
			finish.resolve();
			expect((await outcome).message).toBe('fence lost');
			const row = (await client.query(`SELECT svti_tasks_id AS id, status, result, fence, attempts FROM ${table}`)).rows[0];
			expect(row).toMatchObject({ status: 'running', result: null, attempts: 1 });
			recovery = createTaskRunner(client, { table, autoMigrate: false, fenceTtl: 1, heartbeatInterval: 100, dispatchInterval: 0, recoveryInterval: 20, cleanupInterval: 0 });
			recovery.register('blocked', async () => 'recovered');
			await expect(recovery.await(row.id, { timeout: 5000, pollInterval: 20 })).resolves.toBe('recovered');
			const recovered = (await client.query(`SELECT status, fence, attempts FROM ${table}`)).rows[0];
			expect(recovered).toMatchObject({ status: 'committed', attempts: 2 });
			expect(recovered.fence).not.toBe(row.fence);
		} finally {
			if (lock) { await lock.query('ROLLBACK'); lock.release(); }
			finish.resolve();
			runner.destroy();
			recovery?.destroy();
			await outcome;
			await client.query(`DROP TABLE IF EXISTS ${table}`);
		}
	});
});
