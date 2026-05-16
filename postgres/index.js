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
import { redactConnectionUrl } from '../shared/sensitive.js';

const { Pool, Client } = pg;

/**
 * @typedef {Object} PgClientOptions
 * @property {string} [connectionString] - Postgres connection string. Required UNLESS `pool` is provided.
 * @property {import('pg').Pool} [pool] - An existing `pg.Pool` to wrap instead of constructing a new one. Use when your app already maintains a pool (raw `pg` use elsewhere, another framework integration) and you want a single connection footprint against the database. When provided, `autoShutdown` defaults to `false` (the caller owns the pool's lifecycle) and `end()` becomes a no-op.
 * @property {boolean} [autoShutdown=true] - Listen for `sveltekit:shutdown` and disconnect. Defaults to `false` when `pool` is provided.
 * @property {import('pg').PoolConfig} [options] - Extra pg Pool options. Ignored when `pool` is provided.
 */

/**
 * @typedef {Object} PgClient
 * @property {import('pg').Pool} pool - The underlying pg Pool
 * @property {(text: string, values?: any[]) => Promise<import('pg').QueryResult>} query - Run a query
 * @property {() => Promise<void>} end - Gracefully close the pool. When the pool was provided externally, this is a no-op (the caller owns the lifecycle).
 */

/**
 * Create a Postgres client.
 *
 * Two construction modes:
 *
 *   1. Pass `connectionString`: a fresh `pg.Pool` is created and owned by
 *      this client. `end()` closes the pool; `autoShutdown` (default `true`)
 *      attaches a `sveltekit:shutdown` listener.
 *
 *   2. Pass `pool` (an existing `pg.Pool`): the client wraps the provided
 *      pool without constructing its own. `autoShutdown` defaults to `false`
 *      and `end()` is a no-op - the caller is responsible for closing the
 *      pool. Use this when your app already maintains a pool (e.g. shared
 *      with raw `pg` use elsewhere) and wants a single connection footprint
 *      against the database.
 *
 * `connectionString` and `pool` are mutually exclusive in spirit, but you may
 * pass `connectionString` alongside `pool` to enable `createClient()` (a
 * dedicated `pg.Client` for LISTEN/NOTIFY) without losing the
 * shared-pool ownership story.
 *
 * @param {PgClientOptions} opts
 * @returns {PgClient}
 */
export function createPgClient(opts) {
	if (!opts) {
		throw new ConnectionError('postgres', 'connectionString or pool is required');
	}
	const externalPool = opts.pool;
	if (externalPool === undefined && !opts.connectionString) {
		throw new ConnectionError('postgres', 'connectionString or pool is required');
	}
	if (externalPool !== undefined && (typeof externalPool !== 'object' || typeof externalPool.query !== 'function')) {
		throw new ConnectionError('postgres', 'pool must be a pg.Pool instance');
	}

	const ownedPool = externalPool === undefined;
	// Default: auto-shutdown the pool we own. Caller-owned pools default to
	// no auto-shutdown (the caller drives the lifecycle), but the option is
	// still honored if explicitly set.
	const autoShutdown = opts.autoShutdown !== undefined ? opts.autoShutdown !== false : ownedPool;

	let pool;
	if (ownedPool) {
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
			// pg embeds the connection DSN in some failure-mode message
			// strings (auth failed, host unreachable, SSL mismatch); pipe
			// through redactConnectionUrl so the password segment never
			// reaches stderr / log aggregators.
			const message = typeof err?.message === 'string' ? redactConnectionUrl(err.message) : String(err);
			console.error('postgres: idle client error', message);
		});
	} else {
		pool = externalPool;
	}

	let ended = false;

	async function end() {
		if (ended) return;
		ended = true;
		// Only close pools we own. The caller-provided pool is never ours
		// to close; this is a deliberate no-op so graceful-shutdown paths
		// can call end() unconditionally without worrying about ownership.
		if (ownedPool) {
			await pool.end();
		}
	}

	if (autoShutdown && typeof process !== 'undefined') {
		process.once('sveltekit:shutdown', end);
	}

	// createClient() needs a connectionString. If the user only passed a
	// pool, we don't have one - defer the error until createClient() is
	// actually invoked, since most consumers (the task runner, idempotency
	// stores, etc.) only need pool.query() and never createClient().
	const connectionConfig = opts.connectionString
		? { connectionString: opts.connectionString, ...(opts.options || {}) }
		: null;

	return {
		pool,

		query(text, values) {
			return pool.query(text, values);
		},

		createClient() {
			if (!connectionConfig) {
				throw new ConnectionError(
					'postgres',
					'createClient() requires connectionString - pass it alongside pool when wrapping an external pool'
				);
			}
			return new Client(connectionConfig);
		},

		end
	};
}
