/**
 * Postgres-backed job queue for svelte-adapter-uws.
 *
 * Minimal `SELECT ... FOR UPDATE SKIP LOCKED` queue that works on
 * vanilla Postgres 9.5+ without any extensions. For deployments where
 * pgmq is available, a separate `createPgmqWorker` primitive (future
 * item) is the more featureful option; this primitive is for the
 * common case of standard managed Postgres without extension support.
 *
 * Pairs with `createTaskRunner` as the "enqueue here, task runner
 * dequeues" pattern. The task runner has full state-machine
 * semantics (idempotency, fence, retry); this queue is a lighter
 * batch-claim primitive. Pick the shape that fits the workload.
 *
 * Table schema (auto-created if autoMigrate is true):
 *   svti_jobs (
 *     svti_jobs_id  BIGSERIAL   PRIMARY KEY,
 *     queue         TEXT        NOT NULL,
 *     payload       JSONB,
 *     claimed_at    TIMESTAMPTZ,
 *     claimed_until TIMESTAMPTZ,
 *     attempts      INTEGER     NOT NULL DEFAULT 0,
 *     created_at    TIMESTAMPTZ DEFAULT now()
 *   )
 *   + partial index on (queue, svti_jobs_id) WHERE claimed_at IS NULL
 *   + index on (claimed_until) for visibility-timeout sweeps
 *
 * @module svelte-adapter-uws-extensions/postgres/jobs
 */

import { safeCreate, assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';

/**
 * @typedef {Object} JobQueueOptions
 * @property {string} [table='svti_jobs']
 * @property {boolean} [autoMigrate=true]
 * @property {number} [visibilityTimeout=30000] - Default ms a claim is held before another worker can re-claim
 * @property {import('../prometheus/index.js').MetricsRegistry} [metrics]
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker]
 */

/**
 * @typedef {Object} Job
 * @property {string|number} id
 * @property {string} queue
 * @property {unknown} payload
 * @property {number} attempts
 * @property {Date} created_at
 */

/**
 * Create a Postgres-backed job queue.
 *
 * @param {import('./index.js').PgClient} client
 * @param {JobQueueOptions} [options]
 */
export function createJobQueue(client, options = {}) {
	const table = options.table || 'svti_jobs';
	assertSafeTableName(table, 'postgres jobs');
	const autoMigrate = options.autoMigrate !== false;
	const defaultVisibilityTimeout = options.visibilityTimeout ?? 30000;
	if (typeof defaultVisibilityTimeout !== 'number' || !Number.isFinite(defaultVisibilityTimeout) || defaultVisibilityTimeout <= 0) {
		throw new Error('postgres jobs: visibilityTimeout must be a positive number (ms)');
	}

	const b = options.breaker;
	const m = options.metrics;
	const mEnqueued = m?.counter('jobs_enqueued_total', 'Jobs enqueued', ['queue']);
	const mClaimed = m?.counter('jobs_claimed_total', 'Jobs claimed (rows returned by claim)', ['queue']);
	const mCompleted = m?.counter('jobs_completed_total', 'Jobs completed (deleted)', ['queue']);
	const mFailed = m?.counter('jobs_failed_total', 'Jobs released via fail() for retry', ['queue']);

	let migrated = false;

	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				svti_jobs_id  BIGSERIAL   PRIMARY KEY,
				queue         TEXT        NOT NULL,
				payload       JSONB,
				request_id    TEXT,
				claimed_at    TIMESTAMPTZ,
				claimed_until TIMESTAMPTZ,
				attempts      INTEGER     NOT NULL DEFAULT 0,
				created_at    TIMESTAMPTZ DEFAULT now()
			)
		`);
		// Forward-migrate existing 0.5.0-next.1 deployments.
		await safeCreate(client, `
			ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS request_id TEXT
		`);
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_queue_pending
			    ON ${table} (queue, svti_jobs_id)
			    WHERE claimed_at IS NULL
		`);
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_visibility
			    ON ${table} (claimed_until)
			    WHERE claimed_at IS NOT NULL
		`);
		migrated = true;
	}

	function asArray(idOrIds) {
		return Array.isArray(idOrIds) ? idOrIds : [idOrIds];
	}

	return {
		async enqueue(queue, payload, opts = {}) {
			if (typeof queue !== 'string' || queue.length === 0) {
				throw new Error('postgres jobs: queue must be a non-empty string');
			}
			const requestId = resolveRequestId(opts);
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'jobs_enqueue_' + table,
					text: `INSERT INTO ${table} (queue, payload, request_id)
					            VALUES ($1, $2, $3)
					        RETURNING svti_jobs_id AS id`,
					values: [queue, JSON.stringify(payload ?? null), requestId]
				});
			});
			mEnqueued?.inc({ queue });
			return res.rows[0].id;
		},

		async claim(queue, opts = {}) {
			if (typeof queue !== 'string' || queue.length === 0) {
				throw new Error('postgres jobs: queue must be a non-empty string');
			}
			const batchSize = opts.batchSize ?? 1;
			if (!Number.isInteger(batchSize) || batchSize < 1) {
				throw new Error('postgres jobs: batchSize must be a positive integer');
			}
			const visibilityTimeoutMs = opts.visibilityTimeoutMs ?? defaultVisibilityTimeout;
			if (typeof visibilityTimeoutMs !== 'number' || !Number.isFinite(visibilityTimeoutMs) || visibilityTimeoutMs <= 0) {
				throw new Error('postgres jobs: visibilityTimeoutMs must be a positive number');
			}

			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'jobs_claim_' + table,
					text: `WITH claimed AS (
						SELECT svti_jobs_id FROM ${table}
						 WHERE queue = $1
						   AND (claimed_at IS NULL OR claimed_until < now())
						 ORDER BY svti_jobs_id
						   FOR UPDATE SKIP LOCKED
						 LIMIT $2
					)
					UPDATE ${table} t
					   SET claimed_at = now(),
					       claimed_until = now() + ($3 || ' milliseconds')::interval,
					       attempts = t.attempts + 1
					  FROM claimed
					 WHERE t.svti_jobs_id = claimed.svti_jobs_id
					RETURNING t.svti_jobs_id AS id, t.queue, t.payload, t.request_id, t.attempts, t.created_at`,
					values: [queue, batchSize, String(visibilityTimeoutMs)]
				});
			});
			if (res.rows.length > 0) mClaimed?.inc({ queue }, res.rows.length);
			return res.rows.map((row) => ({
				id: row.id,
				queue: row.queue,
				payload: row.payload,
				requestId: row.request_id ?? null,
				attempts: row.attempts,
				created_at: row.created_at
			}));
		},

		async complete(idOrIds) {
			const ids = asArray(idOrIds);
			if (ids.length === 0) return;
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'jobs_complete_' + table,
					text: `DELETE FROM ${table}
					            WHERE svti_jobs_id = ANY($1::bigint[])
					      RETURNING queue`,
					values: [ids]
				});
			});
			if (mCompleted) {
				for (const row of res.rows) mCompleted.inc({ queue: row.queue });
			}
		},

		async fail(idOrIds) {
			const ids = asArray(idOrIds);
			if (ids.length === 0) return;
			const res = await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'jobs_fail_' + table,
					text: `UPDATE ${table}
					           SET claimed_at = NULL,
					               claimed_until = NULL
					         WHERE svti_jobs_id = ANY($1::bigint[])
					     RETURNING queue`,
					values: [ids]
				});
			});
			if (mFailed) {
				for (const row of res.rows) mFailed.inc({ queue: row.queue });
			}
		},

		async extend(idOrIds, additionalMs) {
			const ids = asArray(idOrIds);
			if (ids.length === 0) return;
			if (typeof additionalMs !== 'number' || !Number.isFinite(additionalMs) || additionalMs <= 0) {
				throw new Error('postgres jobs: additionalMs must be a positive number');
			}
			await withBreaker(b, async () => {
				await ensureTable();
				return client.query({
					name: 'jobs_extend_' + table,
					text: `UPDATE ${table}
					           SET claimed_until = claimed_until + ($2 || ' milliseconds')::interval
					         WHERE svti_jobs_id = ANY($1::bigint[])
					           AND claimed_at IS NOT NULL`,
					values: [ids, String(additionalMs)]
				});
			});
		},

		async pending(queue) {
			const res = await withBreaker(b, async () => {
				await ensureTable();
				if (queue !== undefined) {
					return client.query({
						name: 'jobs_pending_q_' + table,
						text: `SELECT COUNT(*)::int AS pending_count
						          FROM ${table}
						         WHERE queue = $1
						           AND claimed_at IS NULL`,
						values: [queue]
					});
				}
				return client.query({
					name: 'jobs_pending_all_' + table,
					text: `SELECT COUNT(*)::int AS pending_count
					          FROM ${table}
					         WHERE claimed_at IS NULL`
				});
			});
			return res.rows[0].pending_count;
		},

		async clear(queue) {
			await withBreaker(b, async () => {
				await ensureTable();
				if (queue !== undefined) {
					return client.query(`DELETE FROM ${table} WHERE queue = $1`, [queue]);
				}
				return client.query(`DELETE FROM ${table}`);
			});
		},

		destroy() {
			// No timers; reserved for symmetry with other extensions.
		}
	};
}

// Resolve a request id from enqueue options. Explicit `requestId` wins over
// the platform-extracted one so callers can override outside an HTTP/WS
// request context.
function resolveRequestId(opts) {
	if (typeof opts.requestId === 'string' && opts.requestId.length > 0) {
		return opts.requestId;
	}
	const fromPlatform = opts.platform && opts.platform.requestId;
	if (typeof fromPlatform === 'string' && fromPlatform.length > 0) {
		return fromPlatform;
	}
	return null;
}
