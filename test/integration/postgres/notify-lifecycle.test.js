import { afterEach, describe, expect, it } from 'vitest';
import pg from 'pg';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { createPgClient } from '../../../src/postgres/index.js';
import { createNotifyBridge } from '../../../src/postgres/notify.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { captureRegistered } from '../../helpers/diagnostic-line.js';

const owners = [], bridges = [], pools = [];
let identity = 0;
function owner(options = {}) {
	const client = createPgClient({ connectionString: process.env.INTEGRATION_POSTGRES_URL, autoShutdown: false, ...options }); owners.push(client); return client;
}
function bridge(client, options = {}) {
	const instance = createNotifyBridge(client, { channel: 'lifecycle_probe', ...options }); bridges.push(instance); return instance;
}
function admin() { const pool = new pg.Pool({ connectionString: process.env.INTEGRATION_POSTGRES_URL, max: 1 }); pools.push(pool); return pool; }
async function until(check) {
	for (let i = 0; i < 100; i++) { if (await check()) return; await new Promise(resolve => setTimeout(resolve, 20)); }
	throw new Error('notification lifecycle condition did not settle');
}
afterEach(async () => {
	for (const instance of bridges.splice(0)) await instance.deactivate();
	for (const client of owners.splice(0)) await client.end();
	for (const pool of pools.splice(0)) await pool.end();
});

describe('Postgres dedicated notification sessions with the native driver', () => {
	it('owned shutdown removes LISTEN, standalone and pool sessions from pg_stat_activity', async () => {
		const application_name = `notify_owned_${process.pid}_${++identity}`;
		const client = owner({ options: { application_name, max: 1 } });
		const watcher = bridge(client); await watcher.activate(mockPlatform());
		const standalone = client.createClient(); await standalone.connect(); await client.query('SELECT 1');
		const observer = admin();
		const count = async () => Number((await observer.query('SELECT count(*) FROM pg_stat_activity WHERE application_name = $1', [application_name])).rows[0].count);
		expect(await count()).toBe(3); const closing = client.end(); expect(client.end()).toBe(closing); await closing;
		await until(async () => await count() === 0); expect(await count()).toBe(0);
	});
	it('advisory election uses one dedicated session per contender and releases them on shutdown', async () => {
		const application_name = `notify_election_${process.pid}_${++identity}`;
		const first = owner({ options: { application_name } }), second = owner({ options: { application_name } });
		const options = { multiListener: 'advisory', lockId: 171305, pollInterval: 20 };
		await Promise.all([bridge(first, options).activate(mockPlatform()), bridge(second, options).activate(mockPlatform())]);
		expect(first.pool.totalCount).toBe(0); expect(second.pool.totalCount).toBe(0);
		const observer = admin();
		const { rows } = await observer.query('SELECT pid FROM pg_stat_activity WHERE application_name = $1', [application_name]);
		expect(rows).toHaveLength(2);
		const locks = await observer.query("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = ANY($1::int[])", [rows.map(row => row.pid)]);
		expect(Number(locks.rows[0].count)).toBe(1);
		await Promise.all([first.end(), second.end()]);
		await until(async () => Number((await observer.query('SELECT count(*) FROM pg_stat_activity WHERE application_name = $1', [application_name])).rows[0].count) === 0);
	});
	it('leaves an external pool usable and its bridge active until explicit deactivation', async () => {
		const pool = admin(); const client = owner({ pool }); const target = mockPlatform(); const watcher = bridge(client);
		await watcher.activate(target); await client.end();
		await pool.query('SELECT pg_notify($1, $2)', ['lifecycle_probe', '{"topic":"x","event":"external"}']);
		await until(() => target.published.length === 1); await watcher.deactivate();
		await expect(pool.query('SELECT 42 AS value')).resolves.toMatchObject({ rows: [{ value: 42 }] });
	});
	it('reconnects once after native backend termination and resumes notifications', async () => {
		const diagnostics = captureRegistered(); const client = owner(); const sessions = [];
		const original = client.createClient.bind(client); client.createClient = () => { const session = original(); sessions.push(session); return session; };
		const target = mockPlatform(); const watcher = bridge(client, { reconnectInterval: 20 });
		try {
			await watcher.activate(target); const observer = admin(); const oldPid = sessions[0].processID;
			await observer.query('SELECT pg_terminate_backend($1)', [oldPid]);
			await until(() => sessions.length === 2 && sessions[1]._connected);
			await until(async () => { await observer.query('SELECT pg_notify($1, $2)', ['lifecycle_probe', '{"topic":"x","event":"recovered"}']); return target.published.length > 0; });
			expect(sessions).toHaveLength(2); expect(sessions[0]._ended).toBe(true);
		} finally { diagnostics.stop(); }
	});
	it('closes a native pg session stalled during startup without awaiting its unresolved connect promise', async () => {
		const server = createServer(); server.listen(0, '127.0.0.1'); await once(server, 'listening');
		const accepted = once(server, 'connection'); const sockets = [];
		server.on('connection', socket => { sockets.push(socket); socket.resume(); });
		const client = owner({ connectionString: `postgres://test:test@127.0.0.1:${server.address().port}/test`, options: { connectionTimeoutMillis: 0, query_timeout: 0, statement_timeout: 0 } });
		const watcher = bridge(client); const activating = watcher.activate(mockPlatform());
		try {
			await accepted; await client.end(); await activating;
			await until(() => sockets.every(socket => socket.destroyed));
			expect(() => client.createClient()).toThrow('closing');
		} finally { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); }
	});
});
