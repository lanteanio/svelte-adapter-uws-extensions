/**
 * Postgres-backed durable alarm store for `live.alarm`.
 *
 * Same seam + contract as the Redis backend (`redis/alarm-store`), durable on
 * disk. Persists each room's single pending alarm so it survives a restart and
 * fires once cluster-wide. Plugs into the realtime `configureAlarm({ store })`
 * seam: realtime owns the in-memory timers + the leader-gated recovery poll; this
 * store is the durable record the poll reads. It runs no background loop of its
 * own.
 *
 * Table schema (auto-created when autoMigrate is true):
 *   svti_alarms (
 *     topic   TEXT   PRIMARY KEY,   -- the wire topic (already tenant-scoped)
 *     fire_at BIGINT NOT NULL,      -- epoch-ms deadline
 *     meta    JSONB,                -- realtime resolver metadata ({ path, tenantId })
 *     tenant  TEXT                  -- tenantId mirrored out for ops queries / cleanup
 *   )
 *   + index on (fire_at) so the due() sweep is a range scan
 *
 * Single-fire: `delete(topic)` is the atomic claim - `DELETE ... RETURNING` removes
 * the row and reports whether THIS call removed it, so the precise in-memory timer
 * and the recovery poll can never both fire the same alarm.
 *
 * @module svelte-adapter-uws-extensions/postgres/alarm-store
 */

import { safeCreate, assertSafeTableName } from '../shared/pg-migrate.js';
import { withBreaker } from '../shared/breaker.js';

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
 * @property {{ path?: string, tenantId?: string | null } | null} meta
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
	const autoMigrate = options.autoMigrate !== false;
	assertSafeTableName(table, 'postgres alarm store');

	const b = options.breaker;
	const m = options.metrics;
	const mSet = m?.counter('alarm_store_set_total', 'Alarms persisted to the durable store');
	const mClaimed = m?.counter('alarm_store_claimed_total', 'Alarms claimed (delete removed a present row)');
	const mDue = m?.counter('alarm_store_due_total', 'Due alarms returned to the recovery poll');

	let migrated = false;
	async function ensureTable() {
		if (migrated || !autoMigrate) return;
		await safeCreate(client, `
			CREATE TABLE IF NOT EXISTS ${table} (
				topic   TEXT   PRIMARY KEY,
				fire_at BIGINT NOT NULL,
				meta    JSONB,
				tenant  TEXT
			)
		`, { table, columns: ['topic', 'fire_at', 'meta', 'tenant'] });
		await safeCreate(client, `
			CREATE INDEX IF NOT EXISTS idx_${table}_fire_at ON ${table} (fire_at)
		`);
		migrated = true;
	}
	const readyPromise = autoMigrate ? ensureTable() : Promise.resolve();
	readyPromise.catch(() => {});

	function assertTopic(topic) {
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('postgres alarm store: topic must be a non-empty string');
		}
	}

	return {
		ready: () => readyPromise,

		async set(topic, at, meta) {
			assertTopic(topic);
			if (typeof at !== 'number' || !Number.isFinite(at)) {
				throw new Error('postgres alarm store: at must be a finite epoch-ms number');
			}
			await withBreaker(b, async () => {
				await ensureTable();
				await client.query({
					name: 'alarm_set_' + table,
					text: `INSERT INTO ${table} (topic, fire_at, meta, tenant)
					       VALUES ($1, $2, $3::jsonb, $4)
					       ON CONFLICT (topic) DO UPDATE
					         SET fire_at = EXCLUDED.fire_at, meta = EXCLUDED.meta, tenant = EXCLUDED.tenant`,
					values: [topic, Math.trunc(at), JSON.stringify(meta ?? null), (meta && meta.tenantId != null) ? meta.tenantId : null]
				});
			});
			mSet?.inc();
		},

		async delete(topic) {
			assertTopic(topic);
			return withBreaker(b, async () => {
				await ensureTable();
				// DELETE ... RETURNING is the atomic claim: rowCount is 1 only for the
				// caller that actually removed the row.
				const res = await client.query({
					name: 'alarm_delete_' + table,
					text: `DELETE FROM ${table} WHERE topic = $1 RETURNING topic`,
					values: [topic]
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
					name: 'alarm_due_' + table,
					text: `SELECT topic, fire_at, meta FROM ${table} WHERE fire_at <= $1 ORDER BY fire_at LIMIT $2`,
					values: [Math.trunc(nowMs), dueBatch]
				});
				/** @type {AlarmRow[]} */
				const out = res.rows.map((row) => ({
					topic: row.topic,
					// node-pg returns BIGINT as a string; meta JSONB is already parsed.
					at: Number(row.fire_at),
					meta: row.meta != null ? row.meta : null
				}));
				if (out.length) mDue?.inc(out.length);
				return out;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await ensureTable();
				await client.query(`DELETE FROM ${table}`);
			});
		},

		// No background loop to tear down - the recovery poll lives in the realtime
		// layer. Present for parity with the other plugin factories.
		async destroy() { /* no-op */ }
	};
}
