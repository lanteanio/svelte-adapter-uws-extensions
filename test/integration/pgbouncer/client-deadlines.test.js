import { afterEach, expect, it } from 'vitest';
import { createPgClient } from '../../../src/postgres/index.js';
import { createJobQueue } from '../../../src/postgres/jobs.js';
import { createNotifyBridge } from '../../../src/postgres/notify.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

const clients = [];
function create(connectionString, options = {}) {
	const client = createPgClient({ connectionString, autoShutdown: false, options });
	clients.push(client);
	return client;
}
afterEach(async () => { await Promise.all(clients.splice(0).map(client => client.end())); });
for (const mode of ['session', 'transaction']) {
	const url = () => process.env['INTEGRATION_PGBOUNCER_' + mode.toUpperCase() + '_URL'];
	it(mode + ' proxy rejects the owned startup statement_timeout by default', async () => {
		const client = create(url());
		await expect(client.query('SELECT 1')).rejects.toMatchObject({ code: '08P01', message: expect.stringContaining('statement_timeout') });
		const standalone = client.createClient();
		await expect(standalone.connect()).rejects.toMatchObject({ code: '08P01', message: expect.stringContaining('statement_timeout') });
	});
	it(mode + ' proxy honors the server role deadline with the startup parameter omitted', async () => {
		const deployment = create(process.env.INTEGRATION_POSTGRES_URL);
		const table = 'proxy_jobs_' + mode;
		const owner = createJobQueue(deployment, { table });
		await owner.ready();
		const runtime = create(url(), { statement_timeout: 0, query_timeout: 2000 });
		const jobs = createJobQueue(runtime, { table, autoMigrate: false });
		await jobs.ready();
		expect((await runtime.query('SHOW statement_timeout')).rows[0].statement_timeout).toBe('125ms');
		await expect(jobs.enqueue('work', { mode })).resolves.toBeDefined();
		await expect(runtime.query('SELECT pg_sleep(1)')).rejects.toMatchObject({ code: '57014' });
		await expect(runtime.query('SELECT 1')).resolves.toHaveProperty('rowCount', 1);
		const standalone = runtime.createClient();
		await standalone.connect();
		try {
			expect((await standalone.query('SHOW statement_timeout')).rows[0].statement_timeout).toBe('125ms');
			await expect(standalone.query('SELECT pg_sleep(1)')).rejects.toMatchObject({ code: '57014' });
		} finally { await standalone.end(); }
	});
}

for (const multiListener of ['all', 'advisory']) {
	it('activates the ' + multiListener + ' notification bridge through the documented session proxy route', async () => {
		const publisher = create(process.env.INTEGRATION_POSTGRES_URL);
		const runtime = create(process.env.INTEGRATION_PGBOUNCER_SESSION_URL, { statement_timeout: 0 });
		const channel = 'proxy_notify_' + multiListener;
		const bridge = createNotifyBridge(runtime, { channel, multiListener, lockId: 814309, autoReconnect: false });
		const platform = mockPlatform();
		try {
			await bridge.activate(platform);
			await publisher.query('SELECT pg_notify($1, $2)', [channel, JSON.stringify({ topic: 'proxy', event: 'ready', data: { mode: multiListener } })]);
			await expect.poll(() => platform.published.length, { timeout: 5000 }).toBe(1);
			expect(platform.published[0]).toMatchObject({ topic: 'proxy', event: 'ready', data: { mode: multiListener } });
			await runtime.end();
			await expect(bridge.activate(platform)).rejects.toThrow('closing');
		} finally { await bridge.deactivate(); }
	});
}
