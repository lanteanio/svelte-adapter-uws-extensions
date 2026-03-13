/**
 * Postgres client factory for svelte-adapter-uws-extensions.
 *
 * Wraps pg Pool with lifecycle management and graceful shutdown
 * via the SvelteKit `sveltekit:shutdown` event.
 *
 * @module svelte-adapter-uws-extensions/postgres
 */

import pg from 'pg';
import { ConnectionError } from '../shared/errors.js';

const { Pool, Client } = pg;

/**
 * @typedef {Object} PgClientOptions
 * @property {string} connectionString - Postgres connection string
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect
 * @property {import('pg').PoolConfig} [options] - Extra pg Pool options
 */

/**
 * @typedef {Object} PgClient
 * @property {import('pg').Pool} pool - The underlying pg Pool
 * @property {(text: string, values?: any[]) => Promise<import('pg').QueryResult>} query - Run a query
 * @property {() => Promise<void>} end - Gracefully close the pool
 */

/**
 * Create a Postgres client.
 *
 * @param {PgClientOptions} opts
 * @returns {PgClient}
 */
export function createPgClient(opts) {
	if (!opts || !opts.connectionString) {
		throw new ConnectionError('postgres', 'connectionString is required');
	}

	const autoShutdown = opts.autoShutdown !== false;

	let pool;
	try {
		pool = new Pool({
			connectionString: opts.connectionString,
			...(opts.options || {})
		});
	} catch (err) {
		throw new ConnectionError('postgres', 'failed to create pool', err);
	}

	pool.on('error', (err) => {
		// Idle client errors should not crash the process.
		// pg Pool handles reconnection automatically.
		console.error('postgres: idle client error', err.message);
	});

	let ended = false;

	async function end() {
		if (ended) return;
		ended = true;
		await pool.end();
	}

	if (autoShutdown && typeof process !== 'undefined') {
		process.once('sveltekit:shutdown', end);
	}

	const connectionConfig = {
		connectionString: opts.connectionString,
		...(opts.options || {})
	};

	return {
		pool,

		query(text, values) {
			return pool.query(text, values);
		},

		createClient() {
			return new Client(connectionConfig);
		},

		end
	};
}
