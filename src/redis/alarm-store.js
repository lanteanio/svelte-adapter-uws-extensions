/**
 * Redis-backed durable alarm store for `live.alarm`.
 *
 * Persists each room's single pending alarm so it survives a process restart and
 * supports an atomic storage claim cluster-wide. Plugs into the realtime `configureAlarm({ store })`
 * seam: the realtime layer owns the in-memory timers + the leader-gated recovery
 * poll; this store is the durable record the poll reads. It runs no background
 * loop of its own (the poll cadence lives in realtime), so it is a pure
 * data-access object - mirroring how `redis/registry` backs `live.push` routing
 * and `redis/idempotency` backs `live.idempotent`.
 *
 * Storage layout (five keys, one Redis hash slot via the `{alarms}` tag):
 *   - `{alarms}:due`  ZSET  score = `at` (epoch-ms), member = wire topic. The
 *     recovery poll reads `ZRANGEBYSCORE -inf now` to find overdue alarms.
 *   - `{alarms}:meta` HASH  field = wire topic, value = `JSON({ at, meta })`. Holds
 *     the realtime resolver metadata (`alarmId`, legacy RPC path and tenant) the
 *     poll needs to re-find `onAlarm` after a restart. A present `alarmId` is
 *     authoritative; recovery does not fall back to the path.
 *
 *   - `{alarms}:reserved`, `:committed`, `:current` HASHes hold exact decimal
 *     generation strings. Reservation and committed watermarks survive deletion
 *     and clear. Legacy schedules acquire generation 1 without rewriting meta.
 * `set` accepts only the latest reservation above the committed watermark;
 * `delete(topic, generation)` removes only the observed scheduled generation.
 *
 * @module svelte-adapter-uws-extensions/redis/alarm-store
 */

import { withBreaker } from '../shared/breaker.js';
import { keySlot } from '../shared/cluster.js';
import { assertAlarmGeneration } from '../shared/alarm-generation.js';
import { ALARM_GENERATION_SCRIPT } from './_alarm-scripts.js';

const COMMAND = 'orderedAlarmGenerationV1';

/** Default cap on the rows returned by one `due(now)` sweep. */
const DEFAULT_DUE_BATCH = 100;

/**
 * @typedef {Object} RedisAlarmStoreOptions
 * @property {string} [keyPrefix='alarm:'] - Prefix prepended (after the client keyPrefix) to the alarm keys.
 * @property {number} [dueBatch=100] - Max alarms returned per `due(now)` sweep (the leader poll drains the rest on subsequent ticks).
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
 * @typedef {Object} RedisAlarmStore
 * @property {(topic: string) => Promise<string>} nextGeneration
 * @property {(topic: string) => Promise<AlarmRow | null>} get
 * @property {(topic: string, at: number, meta: any, generation: string) => Promise<boolean>} set
 * @property {(topic: string, generation: string) => Promise<boolean>} delete
 * @property {(nowMs: number) => Promise<AlarmRow[]>} due
 * @property {() => Promise<void>} clear
 * @property {() => Promise<void>} destroy
 */

/**
 * Create a Redis-backed durable alarm store. Wire it into the realtime layer with
 * `configureAlarm({ store: createAlarmStore(client), leader: () => leader.isLeader() })`.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisAlarmStoreOptions} [options]
 * @returns {RedisAlarmStore}
 */
export function createAlarmStore(client, options = {}) {
	if (options.keyPrefix !== undefined && typeof options.keyPrefix !== 'string') {
		throw new Error('redis alarm store: keyPrefix must be a string');
	}
	const dueBatch = options.dueBatch ?? DEFAULT_DUE_BATCH;
	if (!Number.isInteger(dueBatch) || dueBatch < 1) {
		throw new Error(`redis alarm store: dueBatch must be a positive integer, got ${dueBatch}`);
	}

	const keyPrefix = options.keyPrefix !== undefined ? options.keyPrefix : 'alarm:';
	const redis = client.redis;
	const b = options.breaker;
	const m = options.metrics;
	const mSet = m?.counter('alarm_store_set_total', 'Alarms persisted to the durable store');
	const mClaimed = m?.counter('alarm_store_claimed_total', 'Alarms claimed (delete removed a present row)');
	const mDue = m?.counter('alarm_store_due_total', 'Due alarms returned to the recovery poll');

	const keys = ['due', 'meta', 'reserved', 'committed', 'current']
		.map(name => client.key(keyPrefix + '{alarms}:' + name));
	if (keys.some(key => keySlot(key) !== keySlot(keys[0]))) {
		throw new Error('redis alarm store: alarm keys hash to different cluster slots; atomic operations require them co-located');
	}
	if (typeof redis[COMMAND] !== 'function') redis.defineCommand(COMMAND, { lua: ALARM_GENERATION_SCRIPT });
	const execute = (operation, ...args) => withBreaker(b, () => redis[COMMAND](keys.length, ...keys, operation, ...args));

	function assertTopic(topic) {
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('redis alarm store: topic must be a non-empty string');
		}
	}
	function assertTime(at) {
		if (typeof at !== 'number' || !Number.isFinite(at)) {
			throw new Error('redis alarm store: at must be a finite epoch-ms number');
		}
	}
	function decode(row) {
		if (!row.length) return null;
		const [topic, score, raw, generation] = row;
		let at = Number(score), meta = null;
		if (raw) {
			try {
				const parsed = JSON.parse(raw);
				if (parsed && typeof parsed.at === 'number' && Number.isFinite(parsed.at)) at = parsed.at;
				meta = parsed && parsed.meta != null ? parsed.meta : null;
			} catch { /* retain corrupt metadata as an unresolved bare row */ }
		}
		return { topic, at, meta, generation };
	}

	return {
		async nextGeneration(topic) {
			assertTopic(topic);
			return execute('next', topic);
		},
		async get(topic) {
			assertTopic(topic);
			return decode(await execute('get', topic));
		},
		async set(topic, at, meta, generation) {
			assertTopic(topic);
			assertTime(at);
			assertAlarmGeneration(generation, 'redis alarm store');
			const committed = (await execute('set', topic, generation, at, JSON.stringify({ at, meta: meta ?? null }))) === 1;
			if (committed) mSet?.inc();
			return committed;
		},
		async delete(topic, generation) {
			assertTopic(topic);
			assertAlarmGeneration(generation, 'redis alarm store');
			const removed = (await execute('delete', topic, generation)) === 1;
			if (removed) mClaimed?.inc();
			return removed;
		},
		async due(nowMs) {
			assertTime(nowMs);
			const rows = (await execute('due', nowMs, dueBatch)).map(decode);
			if (rows.length) mDue?.inc(rows.length);
			return rows;
		},
		async clear() {
			await execute('clear');
		},

		// No background loop to tear down - the recovery poll lives in the realtime
		// layer. Present for parity with the other plugin factories.
		async destroy() { /* no-op */ }
	};
}
