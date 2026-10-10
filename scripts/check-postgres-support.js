import { pathToFileURL } from 'node:url';
import { createPgClient } from '../src/postgres/index.js';

export const MIN_POSTGRES_VERSION = 160000;

/** Read server identity without creating or changing any database object. */
export async function verifyPostgresVersion(client) {
	const result = await client.query('SHOW server_version_num');
	const raw = result.rows?.[0]?.server_version_num;
	if (typeof raw !== 'string' || !/^\d{5,6}$/.test(raw)) {
		throw new Error('PostgreSQL preflight: server_version_num is missing or malformed');
	}
	const version = Number(raw);
	if (version < MIN_POSTGRES_VERSION) {
		throw new Error('PostgreSQL preflight: PostgreSQL 16 or newer is required; server_version_num=' + raw + '. Upgrade the server before initializing stores.');
	}
	return { serverVersionNum: version, major: Math.floor(version / 10000), verifiedMajor: 16 };
}

async function main() {
	const connectionString = process.env.DATABASE_URL || process.env.INTEGRATION_POSTGRES_URL;
	if (!connectionString) throw new Error('PostgreSQL preflight: set DATABASE_URL to the database to inspect');
	const client = createPgClient({
		connectionString, autoShutdown: false,
		options: { max: 1, connectionTimeoutMillis: 5000, query_timeout: 5000, statement_timeout: 5000 }
	});
	try {
		const result = await verifyPostgresVersion(client);
		console.log('PostgreSQL preflight OK: server_version_num=' + result.serverVersionNum + '; supported floor=16; repository integration baseline=16');
	} finally {
		await client.end();
	}
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
