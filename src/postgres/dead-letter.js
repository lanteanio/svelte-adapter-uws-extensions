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
 * Table schema (migrated automatically when `autoMigrate` is true):
 *   svti_dead_letter (
 *     svti_dead_letter_id BIGSERIAL PRIMARY KEY,
 *     webhook_id TEXT,
 *     topic      TEXT   NOT NULL,
 *     event      TEXT,
 *     data       JSONB,
 *     attempts   INT    NOT NULL DEFAULT 0,
 *     error      TEXT,
 *     failed_at  BIGINT NOT NULL,
 *     user_id    TEXT,
 *     scope      TEXT   NOT NULL DEFAULT ''
 *   )
 *   + index on (topic), (failed_at), (user_id) and (scope, svti_dead_letter_id)
 *
 *   svti_dead_letter_scopes (
 *     scope   TEXT   PRIMARY KEY,
 *     records BIGINT NOT NULL,
 *     oldest  BIGINT NOT NULL
 *   )
 *   + statement triggers on svti_dead_letter that keep `records` equal to the
 *     number of rows per scope and `oldest` at its lowest id, for every INSERT,
 *     UPDATE, DELETE and TRUNCATE
 *
 *   svti_dead_letter_purges (
 *     scope     TEXT   NOT NULL,
 *     user_id   TEXT   NOT NULL,
 *     purged_at BIGINT NOT NULL,
 *     PRIMARY KEY (scope, user_id)
 *   )
 *   + index on (purged_at)
 *
 * Captures and purges for one tenant/user serialize under a transaction-scoped
 * advisory lock. Purge writes its fence and deletes records atomically, so a
 * delivery that started before the purge cannot commit after it and resurrect
 * its payload. A fence lives 10 minutes, as in the in-memory store, and expired
 * fences are pruned by the same bounded cleanup passes.
 *
 * Retention uses `max` and optional `ttlMs`. Above `max`, cleanup deletes the
 * oldest ids of the tenant scope holding the most records; between equally
 * large scopes, the one holding the oldest record pays first. A scope at or
 * under `max / scopes present` never loses records to another scope's
 * overflow. `scope` is the tenant scope the erasure fences use (`~` for an
 * untenanted topic, `''` for a malformed tenant topic); the migration derives
 * it from the topic of rows already present. A size trim reads the per-scope
 * counts and deletes through the (scope, id) index, never walking the whole
 * record table. `ready()` also compares the counts with the rows and rebuilds
 * them when they differ, which only writes that bypass the triggers cause (a
 * restore that loads counts and records with triggers enabled, a session in
 * replica mode).
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
import { withBreaker, pgFailure } from './_breaker.js';
import { createStoreSchema } from './migrations.js';
import { scheduleCleanup } from './_cleanup.js';
import { tenantTopicSql, topicTenantScope } from '../shared/tenant-topic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure, firstReportInProcess } from '../shared/diagnostic.js';
import { monotonicNow } from '../shared/runtime.js';
import { withTransaction } from '../shared/pg-tx.js';

// How long a purge fences captures of deliveries that started before it. The
// in-memory store's window; a delivery whose retries outlast it is the
// residual every backend shares.
const FORGET_TOMBSTONE_MS = 10 * 60 * 1000;

// Every statement that adds or deletes records runs at read committed,
// whatever the server or role default. Captures and purges read fences and
// records after waiting on the erasure lock, and under a repeatable-read
// snapshot they would miss a fence or capture committed during the wait.
// The scope-count triggers update one row per tenant scope that concurrent
// writers share; under repeatable read, a row another transaction changed
// after the snapshot fails the statement with a serialization error, while
// read committed waits for that writer and applies the change on top.
const READ_COMMITTED_TX = { isolation: 'read committed' };
import { defineOptions, option } from '../shared/options.js';

/**
 * @typedef {Object} PgDeadLetterOptions
 * @property {string} [table='svti_dead_letter'] - Table name.
 * @property {number} [max=1000] - Retention target. Above it the oldest ids of
 *   the tenant scope holding the most records are evicted in bounded passes.
 * @property {number} [ttlMs=0] - Drop records older than this many ms (0 = no TTL).
 *   Reads exclude expired rows; bounded write-time and idle cleanup delete them.
 * @property {boolean} [autoMigrate=true] - Apply the versioned schema migrations automatically through a session-capable connection; false only validates the schema read-only on ready().
 * @property {number} [cleanupInterval=60000] - Maximum jitter delay between bounded
 *   cleanup passes (ms). 0 disables the timer; write-time cleanup remains.
 * @property {object} [breaker] - Circuit breaker for fault isolation.
 * @property {object} [metrics] - Prometheus registry for the `dead_letter_added_total`
 *   and `dead_letter_evicted_total` counters.
 */

const validateOptions = defineOptions({
	factory: 'createDeadLetter',
	label: 'postgres dead-letter',
	types: 'PgDeadLetterOptions in postgres/dead-letter.d.ts'
}, {
	table: option.known,
	max: option.known,
	ttlMs: option.known,
	autoMigrate: option.boolean,
	cleanupInterval: option.known,
	forgetUserId: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

/**
 * Create a Postgres-backed dead-letter store.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgDeadLetterOptions} [options]
 */
export function createDeadLetter(client, options = {}) {
	options = validateOptions(options);
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
	const purgeTable = table + '_purges';
	const scopeTable = table + '_scopes';
	const pkCol = table + '_id';
	const max = Number.isInteger(options.max) && options.max > 0 ? options.max : 1000;
	const ttlMs = Number.isInteger(options.ttlMs) && options.ttlMs >= 0 ? options.ttlMs : 0;
	if (ttlMs === 0 && !forgetUserId && firstReportInProcess(EXTENSIONS_ERROR_IDS.POSTGRES_DEAD_LETTER_RETENTION_UNBOUNDED)) {
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.POSTGRES_DEAD_LETTER_RETENTION_UNBOUNDED);
	}
	const autoMigrate = options.autoMigrate !== false;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 60000;

	assertSafeTableName(table, 'postgres dead-letter');

	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic || (() => 'all');
	const mAdded = m?.counter('dead_letter_added_total', 'Undeliverable webhook events captured to the dead-letter store', ['topic']);
	const mEvicted = m?.counter('dead_letter_evicted_total', 'Dead-letter records evicted by the max cap from the tenant scope holding the most records');

	const schema = createStoreSchema(client, 'dead-letter', table, autoMigrate);
	const ensureTable = schema.ensure;

	// Take the per-identity erasure lock and return the database clock read
	// after it was granted. The lock key is the fence table's OID (the same
	// search_path resolution as the statements that follow, so differently
	// cased spellings of one table share it), tenant scope and user; unrelated
	// identities never serialize. The clock comes from the outer query, which
	// runs only once the locking subquery has produced its row.
	async function lockErasure(tx, scope, userId) {
		const res = await tx.query(
			`SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint AS db_now
			   FROM (SELECT pg_advisory_xact_lock(hashtextextended($1::regclass::oid::text || ':' || $2 || ':' || $3, 0))) AS locked`,
			[purgeTable, scope, userId]
		);
		return Number(res.rows[0].db_now);
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
	// Fair-share size trim. A read of the per-scope counts (one row per scope
	// holding records) names the largest scope (ties to the one holding the
	// oldest record), the excess over `max` and the runner-up's count. A scope
	// larger than the runner-up gives up its oldest ids down to the runner-up's
	// count. Equally large scopes each give up their oldest record, the scopes
	// holding the oldest records first, so a tie drains a batch per statement.
	// The deletes bind the scope or count and the row count as values, so the
	// planner walks the (scope, id) index for exactly those rows instead of
	// estimating an unknown limit. Under the cap nothing is deleted. Returns
	// the rows evicted.
	async function trimSize(budget) {
		let evicted = 0;
		while (budget.available) {
			const { rows: [heaviest] } = await budget.query(`SELECT scope, records, (sum(records) OVER ())::bigint - $1::bigint AS excess,
				       lead(records) OVER (ORDER BY records DESC, oldest, scope) AS runner_up
				  FROM ${scopeTable} ORDER BY records DESC, oldest, scope LIMIT 1`, [max]);
			const excess = heaviest ? Number(heaviest.excess) : 0;
			if (excess <= 0) break;
			const records = Number(heaviest.records);
			const runnerUp = heaviest.runner_up == null ? 0 : Number(heaviest.runner_up);
			const count = runnerUp < records
				? await budget.remove(`WITH victims AS (
					SELECT ${pkCol} FROM ${table} WHERE scope = $1 ORDER BY ${pkCol} LIMIT $2 FOR UPDATE SKIP LOCKED
				) DELETE FROM ${table} d USING victims WHERE d.${pkCol} = victims.${pkCol}`, [heaviest.scope, Math.min(excess, budget.limit, records - runnerUp)])
				: await budget.remove(`WITH victims AS (
					SELECT ${pkCol} FROM ${table} WHERE ${pkCol} IN (
						SELECT (SELECT min(r.${pkCol}) FROM ${table} r WHERE r.scope = c.scope)
						  FROM ${scopeTable} c WHERE c.records = $1 ORDER BY c.oldest, c.scope LIMIT $2
					) FOR UPDATE SKIP LOCKED
				) DELETE FROM ${table} d USING victims WHERE d.${pkCol} = victims.${pkCol}`, [records, Math.min(excess, budget.limit)]);
			evicted += count;
			if (count === 0 || count >= excess) break;
		}
		return evicted;
	}
	async function trimPurges(budget) {
		// Without an extractor purgeUser writes no fences, so the pass would
		// only cost each capture two more statements.
		if (!forgetUserId) return;
		while (budget.available) {
			const limit = budget.limit;
			const count = await budget.remove(`WITH expired AS (
				SELECT scope, user_id FROM ${purgeTable}
				WHERE purged_at < (extract(epoch from now()) * 1000)::bigint - $1
				ORDER BY purged_at LIMIT $2 FOR UPDATE SKIP LOCKED
			) DELETE FROM ${purgeTable} p USING expired
			 WHERE p.scope = expired.scope AND p.user_id = expired.user_id`, [FORGET_TOMBSTONE_MS, limit]);
			if (count < limit) break;
		}
	}
	// Rebuild the counts from the rows, holding count updates off meanwhile:
	// a capture waits for the rebuild instead of racing it.
	const REBUILD = [
		`LOCK TABLE ${scopeTable} IN EXCLUSIVE MODE`,
		`DELETE FROM ${scopeTable}`,
		`INSERT INTO ${scopeTable} (scope, records, oldest) SELECT scope, count(*), min(${pkCol}) FROM ${table} GROUP BY scope`
	];
	// Set when ready() found drift but could not take the lock in time; the
	// next cleanup pass rebuilds first, retrying with the pass backoff.
	let repairPending = false;
	let ttlFirst = true;
	const cleanup = scheduleCleanup({
		client, table, kind: 'dead-letter', interval: cleanupInterval, metrics: m, coalesce: true,
		ready: ensureTable, healthy: () => !b || b.isHealthy,
		success: () => b?.success(),
		// A pass deletes rows whose triggers update shared per-scope count rows,
		// which captures update too. A pass that hits its lock timeout waiting
		// for one met ordinary contention, not a failing database, so it stays
		// out of the breaker; the next pass retries.
		failure: error => { if (error?.code !== '55P03') pgFailure(b, error); },
		committed: ({ evicted, repaired }) => {
			if (repaired) repairPending = false;
			if (evicted > 0) mEvicted?.inc(evicted);
		},
		isolation: READ_COMMITTED_TX.isolation,
		async work(budget) {
			const repaired = repairPending;
			if (repaired) for (const statement of REBUILD) await budget.query(statement);
			const expiredFirst = ttlFirst;
			ttlFirst = !ttlFirst;
			if (expiredFirst) await trimExpired(budget);
			const evicted = await trimSize(budget);
			if (!expiredFirst) await trimExpired(budget);
			await trimPurges(budget);
			return { evicted, repaired };
		}
	});
	// The triggers keep the counts for every writer, so they only drift
	// through writes that bypass them (a restore that loads counts and
	// records with triggers enabled, a session in replica mode). ready() compares them with the
	// rows in one read and, on a difference, rebuilds them. When captures keep
	// the count rows locked past the lock timeout, ready() still resolves and
	// the cleanup passes take over the rebuild, so boot never fails on load.
	let reconciled = null;
	function reconcileCounts() {
		reconciled ??= (async () => {
			const drift = await withBreaker(b, () => client.query(`SELECT EXISTS (
				SELECT 1 FROM (SELECT scope, count(*) AS n FROM ${table} GROUP BY scope) r
				FULL JOIN ${scopeTable} c USING (scope) WHERE r.n IS DISTINCT FROM c.records
			) AS drifted`));
			if (!drift.rows[0]?.drifted) return;
			try {
				await withTransaction(client, async (tx) => {
					await tx.query("SELECT set_config('lock_timeout', '1000', true)");
					for (const statement of REBUILD) await tx.query(statement);
				}, READ_COMMITTED_TX);
			} catch (error) {
				if (error?.code !== '55P03') throw error;
				repairPending = true;
				cleanup.run().catch(() => { /* the pass reports its own failure */ });
			}
		})().catch((error) => { reconciled = null; throw error; });
		return reconciled;
	}

	const liveRows = ttlMs > 0 ? `failed_at >= (extract(epoch from now()) * 1000)::bigint - ${ttlMs}` : 'TRUE';

	return {
		ready: () => schema.ready().then(reconcileCounts),
		status: schema.status,
		/**
		 * Retain an undeliverable event. Resolves the new record id, or `null`
		 * when purgeUser erased the record's user after its delivery started.
		 * `startedAt` is the delivery start as a monotonic stamp (threaded by
		 * svelte-realtime's `_fireWebhookOut`), used only for that check and
		 * never stored.
		 * @param {{ webhookId: string, topic: string, event: string, data: unknown, attempts: number, error: string, failedAt?: number, startedAt?: number }} rec
		 * @returns {Promise<string | null>}
		 */
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
			// A record with no user, or whose topic is a malformed tenant topic no
			// purge scope contains, cannot be fenced: a transaction with one
			// INSERT, at read committed for the scope-count trigger. The row's
			// scope column is the same tenant scope ('' for a malformed topic)
			// and drives fair-share eviction.
			const tenantScope = topicTenantScope(rec.topic);
			const scope = userId === null ? null : tenantScope;
			const values = [rec.webhookId, rec.topic, rec.event, JSON.stringify(rec.data ?? null), rec.attempts | 0, rec.error, failedAt, userId, tenantScope ?? ''];
			const res = await withBreaker(b, () => scope === null
				? withTransaction(client, (tx) => tx.query(
					`INSERT INTO ${table} (webhook_id, topic, event, data, attempts, error, failed_at, user_id, scope)
					 VALUES ($1, $2, $3, $4, $5, $6,
					         LEAST(NULLIF($7::bigint, 0), (extract(epoch from now()) * 1000)::bigint),
					         $8, $9) RETURNING ${pkCol}`,
					values
				), READ_COMMITTED_TX)
				: withTransaction(client, async (tx) => {
					// Under the lock purgeUser takes, this capture either commits first
					// (and the purge deletes it) or sees the purge's committed fence.
					const dbNow = await lockErasure(tx, scope, userId);
					// The delivery start on the database clock: dbNow minus the
					// delivery's monotonic duration, read after the lock reply. The
					// reply's latency moves the start earlier, so a purge at or after
					// the true start is always fenced. Monotonic time is fractional
					// and the parameter is a bigint, so floor it, again earlier. An
					// untraced delivery starts now, so an earlier purge does not drop it.
					// Capping the duration at dbNow keeps the start a plain integer.
					const startedAt = typeof rec.startedAt === 'number' && Number.isFinite(rec.startedAt)
						? Math.floor(dbNow - Math.min(dbNow, Math.max(0, monotonicNow() - rec.startedAt)))
						: dbNow;
					// $9 is the record's scope, which is also the fence scope here.
					values.push(startedAt, dbNow);
					return tx.query(
						`INSERT INTO ${table} (webhook_id, topic, event, data, attempts, error, failed_at, user_id, scope)
						 SELECT $1::text, $2::text, $3::text, $4::jsonb, $5::integer, $6::text,
						        LEAST(NULLIF($7::bigint, 0), $11::bigint), $8::text, $9::text
						  WHERE NOT EXISTS (
						        SELECT 1 FROM ${purgeTable} p
						         WHERE p.scope = $9 AND p.user_id = $8
						           AND p.purged_at >= $10::bigint
						           AND p.purged_at >= $11::bigint - ${FORGET_TOMBSTONE_MS}
						  ) RETURNING ${pkCol}`,
						values
					);
				}, READ_COMMITTED_TX));
			// No row: a purge ran while this delivery was in flight.
			if (res.rows.length === 0) return null;
			// The row is committed. Retention trims are best-effort from here:
			// cleanup failure must not reject an acknowledged capture and cause a
			// duplicate retry.
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
			const res = await withBreaker(b, () => withTransaction(client, (tx) => tx.query(`DELETE FROM ${table} WHERE ${pkCol} = $1`, [id]), READ_COMMITTED_TX));
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
			const byTopic = Object.create(null);
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
			if (tenantId != null && tenantId !== '' && (typeof tenantId !== 'string' || /[/~]/.test(tenantId))) {
				throw new TypeError('postgres dead-letter: purgeUser tenant id must be null or a string without "/" or "~"');
			}
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			const scope = tenantTopicSql(tenantId, 2);
			await ensureTable();
			return withBreaker(b, () => withTransaction(client, async (tx) => {
				const fenceScope = tenantId || '~';
				// The fence and the delete commit atomically under the lock add()
				// takes, stamped with the same post-lock clock add() reads. Keep the
				// newest stamp if repeated purges straddle a backwards clock step.
				const purgedAt = await lockErasure(tx, fenceScope, userId);
				await tx.query(
					`INSERT INTO ${purgeTable} (scope, user_id, purged_at)
					 VALUES ($1, $2, $3::bigint)
					 ON CONFLICT (scope, user_id) DO UPDATE
					   SET purged_at = GREATEST(${purgeTable}.purged_at, EXCLUDED.purged_at)`,
					[fenceScope, userId, purgedAt]
				);
				const res = await tx.query(
					`DELETE FROM ${table} WHERE user_id = $1 AND ${scope.sql}`,
					[userId, scope.value]
				);
				return res.rowCount || 0;
			}, READ_COMMITTED_TX));
		},

		async clear() {
			await ensureTable();
			await withBreaker(b, () => withTransaction(client, (tx) => tx.query(`DELETE FROM ${table}`), READ_COMMITTED_TX));
		},

		/** Stop retention maintenance. */
		destroy() {
			cleanup.stop();
		}
	};
}
