/**
 * Postgres-backed durable alarm store for `live.alarm`.
 *
 * Same seam + contract as the Redis backend (`redis/alarm-store`), durable on
 * disk. Persists each room's single pending alarm so it survives a restart and
 * supports an atomic storage claim cluster-wide. Plugs into the realtime `configureAlarm({ store })`
 * seam: realtime owns the in-memory timers + the leader-gated recovery poll; this
 * store is the durable record the poll reads. It runs no background loop of its
 * own.
 *
 * Table schema (auto-created when autoMigrate is true):
 *   svti_alarms (
 *     topic   TEXT   PRIMARY KEY,   -- the wire topic (already tenant-scoped)
 *     fire_at BIGINT NOT NULL,      -- epoch-ms deadline
 *     meta    JSONB,                -- resolver metadata ({ alarmId, path, tenantId })
 *     tenant  TEXT,                 -- tenantId mirrored out for ops queries / cleanup
 *     generation BIGINT NOT NULL   -- exact reserved generation, returned as a string
 *   )
 *   + index on (fire_at) so the due() sweep is a range scan
 *
 * A companion `_generations` table retains allocation and commit watermarks.
 * `delete(topic, generation)` claims only the observed generation; delayed old
 * writers cannot resurrect a cancelled alarm. Handler delivery belongs to realtime.
 *
 * @module svelte-adapter-uws-extensions/postgres/alarm-store
 */

import { assertSafeTableName } from '../shared/pg-migrate.js';
import { createStoreSchema } from './migrations.js';
import { withBreaker } from './_breaker.js';
import { assertAlarmGeneration } from '../shared/alarm-generation.js';

/** Default cap on the rows returned by one `due(now)` sweep. */
const DEFAULT_DUE_BATCH = 100;

/**
 * @typedef {Object} PgAlarmStoreOptions
 * @property {string} [table='svti_alarms'] - Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`.
 * @property {number} [dueBatch=100] - Max alarms returned per `due(now)` sweep.
 * @property {boolean} [autoMigrate=true] - Auto-create the table on first use.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker.
 * @property {any} [metrics] - Optional metrics registry (Prometheus).
 */

/**
 * @typedef {Object} AlarmRow
 * @property {string} topic
 * @property {number} at
 * @property {string} generation
 * @property {{ alarmId?: string, path?: string, tenantId?: string | null } | null} meta
 */

/**
 * Create a Postgres-backed durable alarm store. Wire it into the realtime layer with
 * `configureAlarm({ store: createAlarmStore(client), leader: () => leader.isLeader() })`.
 *
 * @param {import('./index.js').PgClient} client
 * @param {PgAlarmStoreOptions} [options]
 */
export function createAlarmStore(client, options = {}) {
	const dueBatch = options.dueBatch ?? DEFAULT_DUE_BATCH;
	if (!Number.isInteger(dueBatch) || dueBatch < 1) {
		throw new Error(`postgres alarm store: dueBatch must be a positive integer, got ${dueBatch}`);
	}
	const table = options.table || 'svti_alarms';
	const authority = table + '_generations';
	const autoMigrate = options.autoMigrate !== false;
	assertSafeTableName(table, 'postgres alarm store');

	const b = options.breaker;
	const m = options.metrics;
	const mSet = m?.counter('alarm_store_set_total', 'Alarms persisted to the durable store');
	const mClaimed = m?.counter('alarm_store_claimed_total', 'Alarms claimed (delete removed a present row)');
	const mDue = m?.counter('alarm_store_due_total', 'Due alarms returned to the recovery poll');

	const schema = createStoreSchema(client, 'alarm-store', table, autoMigrate);
	const ensureTable = schema.ensure;
	if (autoMigrate) schema.ready().catch(() => {});

	function assertTopic(topic) {
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('postgres alarm store: topic must be a non-empty string');
		}
	}
	const row = value => value ? ({ topic: value.topic, at: Number(value.fire_at), meta: value.meta ?? null, generation: value.generation }) : null;

	return {
		ready: schema.ready,
		status: schema.status,

		async nextGeneration(topic) {
			assertTopic(topic);
			return withBreaker(b, async () => {
				await ensureTable();
				try {
					const result = await client.query({
						name: 'alarm_reserve_' + table,
						text: `INSERT INTO ${authority} (topic, reserved, committed)
						       SELECT $1, 1, 0 WHERE NOT EXISTS (SELECT 1 FROM ${table} a LEFT JOIN ${authority} g ON g.topic = a.topic WHERE a.topic = $1 AND (g.topic IS NULL OR a.generation <> g.committed))
						       ON CONFLICT (topic) DO UPDATE SET reserved = ${authority}.reserved + 1 RETURNING reserved::text AS generation`,
						values: [topic]
					});
					if (!result.rows.length) throw new Error('postgres alarm store: missing or incompatible generation authority for existing alarm');
					return result.rows[0].generation;
				} catch (error) {
					if (error.code === '22003') throw new RangeError('postgres alarm store: generation counter exhausted', { cause: error });
					throw error;
				}
			});
		},

		async get(topic) {
			assertTopic(topic);
			return withBreaker(b, async () => {
				await ensureTable();
				const result = await client.query({ name: 'alarm_get_' + table, text: `SELECT topic, fire_at, meta, generation::text AS generation FROM ${table} WHERE topic = $1`, values: [topic] });
				return row(result.rows[0]);
			});
		},

		async set(topic, at, meta, generation) {
			assertTopic(topic);
			if (typeof at !== 'number' || !Number.isFinite(at)) {
				throw new Error('postgres alarm store: at must be a finite epoch-ms number');
			}
			assertAlarmGeneration(generation, 'postgres alarm store');
			return withBreaker(b, async () => {
				await ensureTable();
				const result = await client.query({
					name: 'alarm_set_g_' + table,
					text: `WITH accepted AS (UPDATE ${authority} g SET committed = $5::bigint WHERE g.topic = $1 AND reserved = $5::bigint AND committed < $5::bigint AND NOT EXISTS (SELECT 1 FROM ${table} a WHERE a.topic = g.topic AND a.generation <> g.committed) RETURNING g.topic)
					       INSERT INTO ${table} (topic, fire_at, meta, tenant, generation)
					       SELECT topic, $2, $3::jsonb, $4, $5::bigint FROM accepted
					       ON CONFLICT (topic) DO UPDATE
					         SET fire_at = EXCLUDED.fire_at, meta = EXCLUDED.meta, tenant = EXCLUDED.tenant, generation = EXCLUDED.generation RETURNING topic`,
					values: [topic, Math.trunc(at), JSON.stringify(meta ?? null), (meta && meta.tenantId != null) ? meta.tenantId : null, generation]
				});
				const accepted = result.rowCount === 1;
				if (accepted) mSet?.inc();
				return accepted;
			});
		},

		async delete(topic, generation) {
			assertTopic(topic);
			assertAlarmGeneration(generation, 'postgres alarm store');
			return withBreaker(b, async () => {
				await ensureTable();
				// Lock the same authority row as set(), then claim only the observed row.
				const res = await client.query({
					name: 'alarm_del_g_' + table,
					text: `WITH authority AS (SELECT topic FROM ${authority} WHERE topic = $1 AND committed = $2::bigint FOR UPDATE)
					       DELETE FROM ${table} a USING authority g WHERE a.topic = g.topic AND a.generation = $2::bigint RETURNING a.topic`,
					values: [topic, generation]
				});
				const claimed = res.rowCount === 1;
				if (claimed) mClaimed?.inc();
				return claimed;
			});
		},

		async due(nowMs) {
			return withBreaker(b, async () => {
				await ensureTable();
				const res = await client.query({
					name: 'alarm_due_g_' + table,
					text: `SELECT topic, fire_at, meta, generation::text AS generation FROM ${table} WHERE fire_at <= $1 ORDER BY fire_at LIMIT $2`,
					values: [Math.trunc(nowMs), dueBatch]
				});
				/** @type {AlarmRow[]} */
				const out = res.rows.map(row);
				if (out.length) mDue?.inc(out.length);
				return out;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				const connection = await client.connect({ signal: AbortSignal.timeout(10000) });
				let failed = true, disconnected = null;
				const onError = error => { disconnected = error; };
				connection.on?.('error', onError);
				const query = text => disconnected ? Promise.reject(disconnected) : connection.query({ text, query_timeout: 11000 });
				try {
					await query('BEGIN ISOLATION LEVEL READ COMMITTED');
					await query("SET LOCAL statement_timeout = '10000ms'; SET LOCAL lock_timeout = '5000ms'");
					// Commands after the lock use fresh snapshots, including a set
					// that committed while clear waited for authority.
					await query(`LOCK TABLE ${authority} IN EXCLUSIVE MODE`);
					const invalid = await query(`SELECT a.topic FROM ${table} a LEFT JOIN ${authority} g ON g.topic = a.topic WHERE g.topic IS NULL OR a.generation <> g.committed LIMIT 1`);
					if (invalid.rows.length) throw new Error('postgres alarm store: missing or incompatible generation authority for existing alarm');
					await query(`UPDATE ${authority} SET committed = reserved WHERE committed <> reserved`);
					await query(`DELETE FROM ${table}`);
					await query('COMMIT');
					failed = false;
				} catch (error) {
					try { await connection.query({ text: 'ROLLBACK', query_timeout: 1000 }); } catch { /* discard below */ }
					throw error;
				} finally {
					connection.release(failed);
					connection.removeListener?.('error', onError);
				}
			});
		},

		// No background loop to tear down - the recovery poll lives in the realtime
		// layer. Present for parity with the other plugin factories.
		async destroy() { /* no-op */ }
	};
}
