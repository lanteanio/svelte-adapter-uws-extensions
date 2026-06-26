/**
 * Postgres-backed dead-letter store for svelte-realtime's outbound-webhook DLQ.
 *
 * Implements the same interface as svelte-realtime's in-memory
 * `createDeadLetterStore` (`add` / `get` / `remove` / `count` / `list` /
 * `summary` / `clear`), but persists undeliverable webhook events in a Postgres
 * table so they survive restarts and are shared across every instance in a
 * cluster. Wire it with `configureWebhooks({ deadLetter: createDeadLetter(pgClient) })`
 * (needs svelte-realtime >= 0.6.0-next.40, which awaits the store interface).
 *
 * Table schema (auto-created when `autoMigrate` is true):
 *   svti_dead_letter (
 *     svti_dead_letter_id BIGSERIAL PRIMARY KEY,
 *     webhook_id TEXT,
 *     topic      TEXT   NOT NULL,
 *     event      TEXT,
 *     data       JSONB,
 *     attempts   INT    NOT NULL DEFAULT 0,
 *     error      TEXT,
 *     failed_at  BIGINT NOT NULL
 *   )
 *   + index on (topic) and (failed_at)
 *
 * The collection is bounded by `max` (the oldest captured row, by id, is evicted
 * first) and an optional `ttlMs`, both enforced on write (no background timer) -
 * a webhook DLQ is low-volume, so the per-add trim is cheap and a no-op until
 * over the cap.
 *
 * @module svelte-adapter-uws-extensions/postgres/dead-letter
 */

import { safeCreate, assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';

/**
 * @typedef {Object} PgDeadLetterOptions
 * @property {string} [table='svti_dead_letter'] - Table name.
 * @property {number} [max=1000] - Max retained records (oldest evicted first).
 * @property {number} [ttlMs=0] - Drop records older than this many ms on write (0 = no TTL).
 * @property {boolean} [autoMigrate=true] - Auto-create the table on first use.
 * @property {object} [breaker] - Circuit breaker for fault isolation.
 * @property {object} [metrics] - Prometheus registry for the `dead_letter_added_total` counter.
 */

/**
 * Create a Postgres-backed dead-letter store.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgDeadLetterOptions} [options]
 */
export function createDeadLetter(client, options = {}) {
	if (options.max !== undefined && (!Number.isInteger(options.max) || options.max <= 0)) {
		throw new Error(`postgres dead-letter: max must be a positive integer, got ${options.max}`);
	}
	if (options.ttlMs !== undefined && (!Number.isInteger(options.ttlMs) || options.ttlMs < 0)) {
		throw new Error(`postgres dead-letter: ttlMs must be a non-negative integer, got ${options.ttlMs}`);
	}
	const table = options.table || 'svti_dead_letter';
	const pkCol = table + '_id';
	const max = Number.isInteger(options.max) && options.max > 0 ? options.max : 1000;
	const ttlMs = Number.isInteger(options.ttlMs) && options.ttlMs >= 0 ? options.ttlMs : 0;
	const autoMigrate = options.autoMigrate !== false;

	assertSafeTableName(table, 'postgres dead-letter');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mAdded = m?.counter('dead_letter_added_total', 'Undeliverable webhook events captured to the dead-letter store', ['topic']);

	let migrated = false;
	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				${pkCol}   BIGSERIAL PRIMARY KEY,
				webhook_id TEXT,
				topic      TEXT   NOT NULL,
				event      TEXT,
				data       JSONB,
				attempts   INT    NOT NULL DEFAULT 0,
				error      TEXT,
				failed_at  BIGINT NOT NULL
			)
		`, { table, columns: [pkCol, 'webhook_id', 'topic', 'event', 'data', 'attempts', 'error', 'failed_at'] });
		await safeCreate(client, `CREATE INDEX IF NOT EXISTS idx_${table}_topic ON ${table} (topic)`);
		await safeCreate(client, `CREATE INDEX IF NOT EXISTS idx_${table}_failed_at ON ${table} (failed_at)`);
		migrated = true;
	}

	/** Map a DB row (snake_case, bigints-as-strings) to the camelCase record. */
	function rowToRecord(row) {
		if (!row) return null;
		return {
			id: String(row[pkCol]),
			webhookId: row.webhook_id,
			topic: row.topic,
			event: row.event,
			data: row.data,
			attempts: row.attempts | 0,
			error: row.error,
			failedAt: Number(row.failed_at)
		};
	}

	return {
		/** @param {{ webhookId: string, topic: string, event: string, data: unknown, attempts: number, error: string, failedAt?: number }} rec */
		async add(rec) {
			await ensureTable();
			const failedAt = typeof rec.failedAt === 'number' ? rec.failedAt : 0;
			const res = await withBreaker(b, () => client.query(
				`INSERT INTO ${table} (webhook_id, topic, event, data, attempts, error, failed_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING ${pkCol}`,
				[rec.webhookId, rec.topic, rec.event, JSON.stringify(rec.data ?? null), rec.attempts | 0, rec.error, failedAt]
			));
			// Size trim: keep the newest `max` rows (by id). The OFFSET subquery
			// returns the (max+1)-th newest id, or NULL when under the cap (a no-op).
			await withBreaker(b, () => client.query(
				`DELETE FROM ${table} WHERE ${pkCol} <= (SELECT ${pkCol} FROM ${table} ORDER BY ${pkCol} DESC OFFSET $1 LIMIT 1)`,
				[max]
			));
			// TTL sweep (relative to the just-captured record's time, ~wall-now).
			if (ttlMs > 0) {
				await withBreaker(b, () => client.query(`DELETE FROM ${table} WHERE failed_at < $1`, [failedAt - ttlMs]));
			}
			mAdded?.inc({ topic: mt ? mt(rec.topic) : rec.topic });
			return String(res.rows[0][pkCol]);
		},

		/** @param {string} id */
		async get(id) {
			await ensureTable();
			const res = await withBreaker(b, () => client.query(`SELECT * FROM ${table} WHERE ${pkCol} = $1`, [id]));
			return rowToRecord(res.rows[0]);
		},

		/** @param {string} id @returns {Promise<boolean>} */
		async remove(id) {
			await ensureTable();
			const res = await withBreaker(b, () => client.query(`DELETE FROM ${table} WHERE ${pkCol} = $1`, [id]));
			return res.rowCount > 0;
		},

		/** @param {{ topic?: string }} [filter] */
		async count(filter) {
			await ensureTable();
			const res = (filter && filter.topic !== undefined)
				? await withBreaker(b, () => client.query(`SELECT count(*)::int AS n FROM ${table} WHERE topic = $1`, [filter.topic]))
				: await withBreaker(b, () => client.query(`SELECT count(*)::int AS n FROM ${table}`));
			return res.rows[0].n;
		},

		/** @param {{ topic?: string, limit?: number }} [filter] */
		async list(filter = {}) {
			await ensureTable();
			const limit = Number.isInteger(filter.limit) && filter.limit > 0 ? filter.limit : 100;
			const res = (filter.topic !== undefined)
				? await withBreaker(b, () => client.query(`SELECT * FROM ${table} WHERE topic = $1 ORDER BY ${pkCol} DESC LIMIT $2`, [filter.topic, limit]))
				: await withBreaker(b, () => client.query(`SELECT * FROM ${table} ORDER BY ${pkCol} DESC LIMIT $1`, [limit]));
			return res.rows.map(rowToRecord);
		},

		async summary() {
			await ensureTable();
			const totals = await withBreaker(b, () => client.query(
				`SELECT count(*)::int AS total, min(failed_at) AS oldest, max(failed_at) AS newest FROM ${table}`
			));
			const byTopicRows = await withBreaker(b, () => client.query(
				`SELECT topic, count(*)::int AS n FROM ${table} GROUP BY topic`
			));
			/** @type {Record<string, number>} */
			const byTopic = {};
			for (const r of byTopicRows.rows) byTopic[r.topic] = r.n;
			const t = totals.rows[0];
			return {
				total: t.total,
				byTopic,
				oldest: t.oldest == null ? null : Number(t.oldest),
				newest: t.newest == null ? null : Number(t.newest)
			};
		},

		async clear() {
			await ensureTable();
			await withBreaker(b, () => client.query(`DELETE FROM ${table}`));
		}
	};
}
