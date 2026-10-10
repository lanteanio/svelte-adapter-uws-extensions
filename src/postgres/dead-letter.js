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
 * Retention uses `max` (oldest captured id first) and optional `ttlMs`.
 * Writes and periodic maintenance share bounded cleanup transactions; large
 * backlogs drain over multiple passes. Reads exclude expired records even while
 * physical deletion is pending. Replica maintenance uses a per-table lock.
 *
 * Retention clocks off the DATABASE (`now()`), never the caller-supplied
 * `failedAt`: a skewed or hostile producer stamp can then neither mass-evict
 * healthy records nor pin its own forever. The stored stamp is clamped to
 * (0, db-now] at insert; an implausible stamp becomes the database's now.
 *
 * @module svelte-adapter-uws-extensions/postgres/dead-letter
 */

import { assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';
import { createStoreSchema } from './migrations.js';
import { scheduleCleanup } from './_cleanup.js';
import { tenantTopicSql } from '../shared/tenant-topic.js';

/**
 * @typedef {Object} PgDeadLetterOptions
 * @property {string} [table='svti_dead_letter'] - Table name.
 * @property {number} [max=1000] - Retention target (oldest ids evicted in bounded passes).
 * @property {number} [ttlMs=0] - Drop records older than this many ms (0 = no TTL).
 *   Reads exclude expired rows; bounded write-time and idle cleanup delete them.
 * @property {boolean} [autoMigrate=true] - Auto-create the table on first use.
 * @property {number} [cleanupInterval=60000] - Maximum jitter delay between bounded
 *   cleanup passes (ms). 0 disables the timer; write-time cleanup remains.
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
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('postgres dead-letter: forgetUserId must be a function (record) => userId');
	}
	// Right-to-erasure: a DLQ record's payload is app-defined, so the store cannot
	// find a user's records on its own. When set, this extracts the owning userId
	// from the record at write time into a user_id column so `live.forget` can drop
	// the user's undelivered payloads. Unset => not user-purgeable.
	const forgetUserId = options.forgetUserId;
	const table = options.table || 'svti_dead_letter';
	const pkCol = table + '_id';
	const max = Number.isInteger(options.max) && options.max > 0 ? options.max : 1000;
	const ttlMs = Number.isInteger(options.ttlMs) && options.ttlMs >= 0 ? options.ttlMs : 0;
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	assertSafeTableName(table, 'postgres dead-letter');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic || (() => 'all');
	const mAdded = m?.counter('dead_letter_added_total', 'Undeliverable webhook events captured to the dead-letter store', ['topic']);

	const schema = createStoreSchema(client, 'dead-letter', table, autoMigrate);
	const ensureTable = schema.ensure;

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

	async function trimExpired(budget) {
		if (ttlMs === 0) return;
		while (budget.available) {
			const limit = budget.limit;
			const count = await budget.remove(`WITH expired AS (
				SELECT ${pkCol} FROM ${table}
				WHERE failed_at < (extract(epoch from now()) * 1000)::bigint - $1
				ORDER BY failed_at, ${pkCol} LIMIT $2 FOR UPDATE SKIP LOCKED
			) DELETE FROM ${table} d USING expired WHERE d.${pkCol} = expired.${pkCol}`, [ttlMs, limit]);
			if (count < limit) break;
		}
	}
	async function trimSize(budget) {
		while (budget.available) {
			const limit = budget.limit;
			const count = await budget.remove(`WITH excess AS (
				SELECT ${pkCol} FROM ${table}
				WHERE ${pkCol} <= (SELECT ${pkCol} FROM ${table} ORDER BY ${pkCol} DESC OFFSET $1 LIMIT 1)
				ORDER BY ${pkCol} LIMIT $2 FOR UPDATE SKIP LOCKED
			) DELETE FROM ${table} d USING excess WHERE d.${pkCol} = excess.${pkCol}`, [max, limit]);
			if (count < limit) break;
		}
	}
	let ttlFirst = true;
	const cleanup = scheduleCleanup({
		client, table, kind: 'dead-letter', interval: cleanupInterval, metrics: m, coalesce: true,
		ready: ensureTable, healthy: () => !b || b.isHealthy,
		success: () => b?.success(), failure: error => b?.failure(error),
		async work(budget) {
			const first = ttlFirst ? trimExpired : trimSize;
			const second = ttlFirst ? trimSize : trimExpired;
			ttlFirst = !ttlFirst;
			await first(budget);
			await second(budget);
		}
	});
	const liveRows = ttlMs > 0 ? `failed_at >= (extract(epoch from now()) * 1000)::bigint - ${ttlMs}` : 'TRUE';

	return {
		ready: schema.ready,
		status: schema.status,
		/** @param {{ webhookId: string, topic: string, event: string, data: unknown, attempts: number, error: string, failedAt?: number }} rec */
		async add(rec) {
			await ensureTable();
			// The caller's stamp is display data; the database clamps it to
			// (0, db-now] at insert so retention can trust failed_at without
			// trusting the producer's clock.
			const failedAt =
				typeof rec.failedAt === 'number' && Number.isFinite(rec.failedAt) && rec.failedAt > 0
					? Math.floor(rec.failedAt)
					: 0;
			let userId = null;
			if (forgetUserId) {
				try { const u = forgetUserId(rec); if (typeof u === 'string' && u.length > 0) userId = u; } catch { /* extractor best-effort */ }
			}
			const res = await withBreaker(b, () => client.query(
				`INSERT INTO ${table} (webhook_id, topic, event, data, attempts, error, failed_at, user_id)
				 VALUES ($1, $2, $3, $4, $5, $6,
				         LEAST(NULLIF($7::bigint, 0), (extract(epoch from now()) * 1000)::bigint),
				         $8) RETURNING ${pkCol}`,
				[rec.webhookId, rec.topic, rec.event, JSON.stringify(rec.data ?? null), rec.attempts | 0, rec.error, failedAt, userId]
			));
			// The row is committed (autocommit). Retention trims are best-effort
			// from here: a failed size/TTL delete - or an open breaker - must not
			// reject an add whose record already persisted, or the caller retries
			// and double-captures. Mirrors the Redis backend's evict().catch().
			await cleanup.run().catch(() => { /* capture already persisted */ });
			mAdded?.inc({ topic: mt(rec.topic) });
			return String(res.rows[0][pkCol]);
		},

		/** @param {string} id */
		async get(id) {
			await ensureTable();
			const res = await withBreaker(b, () => client.query(`SELECT * FROM ${table} WHERE ${pkCol} = $1 AND ${liveRows}`, [id]));
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
				? await withBreaker(b, () => client.query(`SELECT count(*)::int AS n FROM ${table} WHERE topic = $1 AND ${liveRows}`, [filter.topic]))
				: await withBreaker(b, () => client.query(`SELECT count(*)::int AS n FROM ${table} WHERE ${liveRows}`));
			return res.rows[0].n;
		},

		/** @param {{ topic?: string, limit?: number }} [filter] */
		async list(filter = {}) {
			await ensureTable();
			const limit = Number.isInteger(filter.limit) && filter.limit > 0 ? filter.limit : 100;
			const res = (filter.topic !== undefined)
				? await withBreaker(b, () => client.query(`SELECT * FROM ${table} WHERE topic = $1 AND ${liveRows} ORDER BY ${pkCol} DESC LIMIT $2`, [filter.topic, limit]))
				: await withBreaker(b, () => client.query(`SELECT * FROM ${table} WHERE ${liveRows} ORDER BY ${pkCol} DESC LIMIT $1`, [limit]));
			return res.rows.map(rowToRecord);
		},

		async summary() {
			await ensureTable();
			const totals = await withBreaker(b, () => client.query(
				`SELECT count(*)::int AS total, min(failed_at) AS oldest, max(failed_at) AS newest FROM ${table} WHERE ${liveRows}`
			));
			const byTopicRows = await withBreaker(b, () => client.query(
				`SELECT topic, count(*)::int AS n FROM ${table} WHERE ${liveRows} GROUP BY topic`
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

		/**
		 * Right-to-erasure (`live.forget`): delete every record stamped with this
		 * user's id (requires a `forgetUserId` extractor; a no-op otherwise).
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} records removed
		 */
		async purgeUser(tenantId, userId) {
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			// Tenant-scoped by the topic namespace, matching the sibling legs of
			// the same purge. See postgres/replay.js for why deleting on
			// `user_id` alone made one tenant's erasure reach every other's.
			const scope = tenantTopicSql(tenantId, 2);
			await ensureTable();
			const res = await withBreaker(b, () => client.query(
				`DELETE FROM ${table} WHERE user_id = $1 AND ${scope.sql}`,
				[userId, scope.value]
			));
			return res.rowCount || 0;
		},

		async clear() {
			await ensureTable();
			await withBreaker(b, () => client.query(`DELETE FROM ${table}`));
		},

		/** Stop retention maintenance. */
		destroy() {
			cleanup.stop();
		}
	};
}
