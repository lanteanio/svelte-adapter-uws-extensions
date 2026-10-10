import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { setTimeout } from 'node:timers/promises';
import { createPgClient } from '../../src/postgres/index.js';

const composePath = fileURLToPath(new URL('./docker-compose.pgbouncer.yml', import.meta.url));
const postgres = process.env.INTEGRATION_POSTGRES_HOST_PORT || '55432';
const session = process.env.INTEGRATION_PGBOUNCER_SESSION_PORT || '55433';
const transaction = process.env.INTEGRATION_PGBOUNCER_TRANSACTION_PORT || '55434';
const project = process.env.INTEGRATION_COMPOSE_PROJECT || 'extensions-pgbouncer-' + postgres;
function compose(args) {
	const result = spawnSync('docker', ['compose', '-p', project, '-f', composePath, ...args], { stdio: 'inherit', windowsHide: true });
	if (result.error || result.status !== 0) throw result.error || new Error('PgBouncer compose failed: ' + args.join(' '));
}
async function ready(url) {
	let last;
	for (let attempt = 0; attempt < 30; attempt++) {
		const client = createPgClient({ connectionString: url, autoShutdown: false, options: { statement_timeout: 0, query_timeout: 1000, connectionTimeoutMillis: 1000 } });
		try { await client.query('SELECT 1'); return; } catch (error) { last = error; } finally { await client.end(); }
		await setTimeout(100);
	}
	throw last;
}
export default async function setup() {
	const url = port => 'postgres://test:test@localhost:' + port + '/test';
	process.env.INTEGRATION_POSTGRES_URL = url(postgres);
	process.env.INTEGRATION_PGBOUNCER_SESSION_URL = url(session);
	process.env.INTEGRATION_PGBOUNCER_TRANSACTION_URL = url(transaction);
	try {
		compose(['up', '-d', '--wait']);
		await Promise.all([ready(url(session)), ready(url(transaction))]);
	} catch (error) { compose(['down', '-v']); throw error; }
	return () => compose(['down', '-v']);
}
