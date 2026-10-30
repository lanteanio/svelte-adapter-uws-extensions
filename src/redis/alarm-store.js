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
 * Storage layout (eight keys, one Redis hash slot via the `{alarms}` tag):
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
 *   - `{alarms}:executions` HASH and `{alarms}:execution-order` ZSET hold
 *     retained executions. `claim(topic, generation)` moves the scheduled row
 *     there before the handler runs; `complete` removes the exact attempt and
 *     `fail` marks it failed. `requeue` moves it back to the schedule and
 *     records the next attempt in `{alarms}:attempt`.
 *
 * @module svelte-adapter-uws-extensions/redis/alarm-store
 */

import { withBreaker } from '../shared/breaker.js';
import { keySlot } from '../shared/cluster.js';
import { wallEpoch } from '../shared/runtime.js';
import {
	MAX_ALARM_EXECUTIONS, alarmCapacityError, alarmExecutionsPage, assertAlarmAttempt,
	assertAlarmFailure, assertAlarmGeneration, assertAlarmRequeue
} from '../shared/alarm-generation.js';
import { ALARM_GENERATION_SCRIPT } from './_alarm-scripts.js';
import { defineOptions, option } from '../shared/options.js';

const COMMAND = 'orderedAlarmGenerationV2';
const LABEL = 'redis alarm store';

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
 * @typedef {AlarmRow & { attempt: number, status: 'claimed' | 'failed', claimedAt: number, failedAt: number | null, errorClass: 'handler' | null }} AlarmExecution
 */

/**
 * @typedef {Object} RedisAlarmStore
 * @property {(topic: string) => Promise<string>} nextGeneration
 * @property {(topic: string) => Promise<AlarmRow | null>} get
 * @property {(topic: string, at: number, meta: any, generation: string) => Promise<boolean>} set
 * @property {(topic: string, generation: string) => Promise<boolean>} delete
 * @property {(topic: string, generation: string) => Promise<{ attempt: number } | null>} claim
 * @property {(topic: string, generation: string, attempt: number) => Promise<boolean>} complete
 * @property {(topic: string, generation: string, attempt: number, failure: { errorClass: 'handler', failedAt: number }) => Promise<boolean>} fail
 * @property {(topic: string, generation: string) => Promise<AlarmExecution | null>} getExecution
 * @property {(options?: { limit?: number, after?: { topic: string, generation: string } | null }) => Promise<{ items: AlarmExecution[], next: { topic: string, generation: string } | null }>} executions
 * @property {(topic: string, generation: string, attempt: number, options: { at: number, allowAbandoned?: boolean }) => Promise<boolean>} requeue
 * @property {(nowMs: number) => Promise<AlarmRow[]>} due
 * @property {() => Promise<void>} clear
 * @property {() => Promise<void>} destroy
 */

const validateOptions = defineOptions({
	factory: 'createAlarmStore',
	label: 'redis alarm store',
	types: 'RedisAlarmStoreOptions in redis/alarm-store.d.ts'
}, {
	keyPrefix: option.known,
	dueBatch: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

/**
 * Create a Redis-backed durable alarm store. Wire it into the realtime layer with
 * `configureAlarm({ store: createAlarmStore(client), leader: () => leader.isLeader() })`.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisAlarmStoreOptions} [options]
 * @returns {RedisAlarmStore}
 */
export function createAlarmStore(client, options = {}) {
	validateOptions(options);
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
	const mClaimed = m?.counter('alarm_store_claimed_total', 'Alarms claimed into retained execution storage');
	const mDue = m?.counter('alarm_store_due_total', 'Due alarms returned to the recovery poll');

	const keys = ['due', 'meta', 'reserved', 'committed', 'current', 'attempt', 'executions', 'execution-order']
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
	function assertExecution(topic, generation, attempt) {
		assertTopic(topic);
		assertAlarmGeneration(generation, LABEL);
		if (attempt !== undefined) assertAlarmAttempt(attempt, LABEL);
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
	function decodeExecution(raw) {
		let record;
		try { record = JSON.parse(raw); } catch { record = null; }
		if (!record || typeof record !== 'object') throw new Error('redis alarm store: inconsistent execution record');
		const failed = record.status === 'failed';
		return {
			...decode([record.topic, record.score, record.payload, record.generation]),
			attempt: Number(record.attempt),
			status: failed ? 'failed' : 'claimed',
			claimedAt: Number(record.claimedAt),
			failedAt: failed ? Number(record.failedAt) : null,
			errorClass: failed ? 'handler' : null
		};
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
			return (await execute('delete', topic, generation)) === 1;
		},
		async claim(topic, generation) {
			assertExecution(topic, generation);
			const claimed = await execute('claim', topic, generation, Math.trunc(wallEpoch()), MAX_ALARM_EXECUTIONS);
			if (claimed === null || claimed === undefined) return null;
			// Capacity is refused after the script returns, outside the breaker:
			// a full namespace is not a backend failure.
			if (Number(claimed) === -1) throw alarmCapacityError(LABEL);
			mClaimed?.inc();
			return { attempt: Number(claimed) };
		},
		async complete(topic, generation, attempt) {
			assertExecution(topic, generation, attempt);
			return (await execute('complete', topic, generation, String(attempt))) === 1;
		},
		async fail(topic, generation, attempt, failure) {
			assertExecution(topic, generation, attempt);
			const failedAt = assertAlarmFailure(failure, LABEL);
			return (await execute('fail', topic, generation, String(attempt), failedAt)) === 1;
		},
		async getExecution(topic, generation) {
			assertExecution(topic, generation);
			const raw = await execute('execution', topic, generation);
			return raw ? decodeExecution(raw) : null;
		},
		async executions(options) {
			const { limit, after } = alarmExecutionsPage(options, LABEL);
			const cursor = after ? [after.topic, after.generation] : [];
			const records = (await execute('executions', limit, ...cursor)).map(decodeExecution);
			const items = records.slice(0, limit);
			const last = items.at(-1);
			return { items, next: records.length > limit ? { topic: last.topic, generation: last.generation } : null };
		},
		async requeue(topic, generation, attempt, options) {
			assertExecution(topic, generation, attempt);
			const { at, allowAbandoned, next } = assertAlarmRequeue(attempt, options, LABEL);
			return (await execute('requeue', topic, generation, String(attempt), String(at), String(next), allowAbandoned ? '1' : '0')) === 1;
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
