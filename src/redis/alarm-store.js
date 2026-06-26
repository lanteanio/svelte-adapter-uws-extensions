/**
 * Redis-backed durable alarm store for `live.alarm`.
 *
 * Persists each room's single pending alarm so it survives a process restart and
 * fires once cluster-wide. Plugs into the realtime `configureAlarm({ store })`
 * seam: the realtime layer owns the in-memory timers + the leader-gated recovery
 * poll; this store is the durable record the poll reads. It runs no background
 * loop of its own (the poll cadence lives in realtime), so it is a pure
 * data-access object - mirroring how `redis/registry` backs `live.push` routing
 * and `redis/idempotency` backs `live.idempotent`.
 *
 * Storage layout (two keys, one Redis hash slot via the `{alarms}` tag so a
 * cluster keeps them co-located and the claim stays atomic):
 *   - `{alarms}:due`  ZSET  score = `at` (epoch-ms), member = wire topic. The
 *     recovery poll reads `ZRANGEBYSCORE -inf now` to find overdue alarms.
 *   - `{alarms}:meta` HASH  field = wire topic, value = `JSON({ at, meta })`. Holds
 *     the realtime resolver metadata (the stream's RPC path) the poll needs to
 *     re-find `onAlarm` after a restart.
 *
 * Single-fire: `delete(topic)` is the atomic claim - `ZREM` returns 1 only for the
 * caller that actually removed the member, so the precise in-memory timer and the
 * recovery poll can never both fire the same alarm.
 *
 * @module svelte-adapter-uws-extensions/redis/alarm-store
 */

import { withBreaker } from '../shared/breaker.js';
import { keySlot } from '../shared/cluster.js';

/** Default cap on the rows returned by one `due(now)` sweep. */
const DEFAULT_DUE_BATCH = 100;

/**
 * @typedef {Object} RedisAlarmStoreOptions
 * @property {string} [keyPrefix='alarm:'] - Prefix prepended (after the client keyPrefix) to the two alarm keys.
 * @property {number} [dueBatch=100] - Max alarms returned per `due(now)` sweep (the leader poll drains the rest on subsequent ticks).
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
 * @typedef {Object} RedisAlarmStore
 * @property {(topic: string, at: number, meta?: any) => Promise<void>} set
 * @property {(topic: string) => Promise<boolean>} delete
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

	// Co-located on one slot via the `{alarms}` hash tag, so the MULTI claim
	// (ZREM + HDEL) is atomic on a cluster and never spans nodes. The two keys
	// share a byte-identical prefix through the tag, so a standard `client.key`
	// always co-locates them - but a custom client whose `key()` rewrites keys
	// asymmetrically could split them and silently break the claim (a cross-slot
	// MULTI no-ops the off-node command). Fail fast at construction instead.
	const dueKey = client.key(keyPrefix + '{alarms}:due');
	const metaKey = client.key(keyPrefix + '{alarms}:meta');
	if (keySlot(dueKey) !== keySlot(metaKey)) {
		throw new Error(
			'redis alarm store: the due-index and meta keys hash to different cluster slots ' +
			`("${dueKey}" vs "${metaKey}"); the atomic claim requires them co-located. ` +
			'This usually means a custom client.key() broke the {alarms} hash tag.'
		);
	}

	function assertTopic(topic) {
		if (typeof topic !== 'string' || topic.length === 0) {
			throw new Error('redis alarm store: topic must be a non-empty string');
		}
	}

	return {
		async set(topic, at, meta) {
			assertTopic(topic);
			if (typeof at !== 'number' || !Number.isFinite(at)) {
				throw new Error('redis alarm store: at must be a finite epoch-ms number');
			}
			await withBreaker(b, async () => {
				const payload = JSON.stringify({ at, meta: meta ?? null });
				// Write the meta + the due-index as one atomic unit (same slot) so a
				// crash can never leave a due member without its resolver metadata.
				await redis.multi().hset(metaKey, topic, payload).zadd(dueKey, at, topic).exec();
			});
			mSet?.inc();
		},

		async delete(topic) {
			assertTopic(topic);
			return withBreaker(b, async () => {
				const res = await redis.multi().zrem(dueKey, topic).hdel(metaKey, topic).exec();
				// res[0] = [err, zremCount]. The ZREM count IS the atomic claim: exactly
				// one caller sees 1, everyone else sees 0.
				const n = res && res[0] ? Number(res[0][1]) : 0;
				const claimed = n === 1;
				if (claimed) mClaimed?.inc();
				return claimed;
			});
		},

		async due(nowMs) {
			return withBreaker(b, async () => {
				const topics = await redis.zrangebyscore(dueKey, '-inf', nowMs, 'LIMIT', 0, dueBatch);
				if (!topics || topics.length === 0) return [];
				const metas = await redis.hmget(metaKey, ...topics);
				/** @type {AlarmRow[]} */
				const out = [];
				for (let i = 0; i < topics.length; i++) {
					const raw = metas[i];
					let at = nowMs;
					let meta = null;
					if (raw) {
						try {
							const parsed = JSON.parse(raw);
							if (parsed && typeof parsed.at === 'number') at = parsed.at;
							meta = parsed && parsed.meta != null ? parsed.meta : null;
						} catch { /* corrupt payload -> surface as a bare row, the poll GCs it */ }
					}
					out.push({ topic: topics[i], at, meta });
				}
				if (out.length) mDue?.inc(out.length);
				return out;
			});
		},

		async clear() {
			await withBreaker(b, async () => {
				await redis.multi().del(dueKey).del(metaKey).exec();
			});
		},

		// No background loop to tear down - the recovery poll lives in the realtime
		// layer. Present for parity with the other plugin factories.
		async destroy() { /* no-op */ }
	};
}
