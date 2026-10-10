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
		let lock;
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
			const row = (await client.query(`SELECT status, result FROM ${table}`)).rows[0];
			expect(row).toEqual({ status: 'failed', result: null });
		} finally {
			if (lock) { await lock.query('ROLLBACK'); lock.release(); }
			finish.resolve();
			runner.destroy();
			await outcome;
			await client.query(`DROP TABLE IF EXISTS ${table}`);
		}
	});
});
