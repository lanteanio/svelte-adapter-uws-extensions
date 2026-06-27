/**
 * Redis-backed dead-letter store for svelte-realtime's outbound-webhook DLQ.
 *
 * Implements the same interface as svelte-realtime's in-memory
 * `createDeadLetterStore` (`add` / `get` / `remove` / `count` / `list` /
 * `summary` / `clear`), but persists records in Redis so undeliverable webhook
 * events survive restarts and are shared across every instance in a cluster.
 * Wire it with `configureWebhooks({ deadLetter: createDeadLetter(redisClient) })`
 * (needs svelte-realtime >= 0.6.0-next.40, which awaits the store interface).
 *
 * Storage layout (one bounded logical collection, NOT sharded by topic):
 *   - `{prefix}dlq:seq:{dlq}`   - INCR counter for record ids
 *   - `{prefix}dlq:recs:{dlq}`  - HASH id -> JSON record (without the id field)
 *   - `{prefix}dlq:order:{dlq}` - sorted set, score = failedAt, member = id
 *
 * The literal `{dlq}` braces are a Redis Cluster hash tag, so all three keys
 * share one slot - every multi-key operation (add, remove, evict) stays on a
 * single node with no cross-slot hazard. A webhook DLQ is low-volume and capped,
 * so co-locating the whole collection on one slot is intentional, not a
 * bottleneck. Operations use plain commands (no Lua); the add path is a few
 * commands rather than an atomic script - a benign race between two concurrent
 * captures can only mis-trim by one under the cap, which the next add corrects.
 *
 * @module svelte-adapter-uws-extensions/redis/dead-letter
 */

import { withBreaker } from '../shared/breaker.js';

/**
 * @typedef {Object} RedisDeadLetterOptions
 * @property {number} [max=1000] - Max retained records (oldest evicted first).
 * @property {number} [ttlMs=0] - Drop records older than this many ms on write (0 = no TTL).
 * @property {object} [breaker] - Circuit breaker for fault isolation (shared `createCircuitBreaker`).
 * @property {object} [metrics] - Prometheus registry for the `dead_letter_added_total` counter.
 */

/**
 * Create a Redis-backed dead-letter store.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisDeadLetterOptions} [options]
 */
export function createDeadLetter(client, options = {}) {
	if (options.max !== undefined && (!Number.isInteger(options.max) || options.max <= 0)) {
		throw new Error(`redis dead-letter: max must be a positive integer, got ${options.max}`);
	}
	if (options.ttlMs !== undefined && (!Number.isInteger(options.ttlMs) || options.ttlMs < 0)) {
		throw new Error(`redis dead-letter: ttlMs must be a non-negative integer, got ${options.ttlMs}`);
	}
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('redis dead-letter: forgetUserId must be a function (record) => userId');
	}
	// Right-to-erasure: a DLQ record's payload is app-defined, so the store cannot
	// tell whose record it is. When set, this maps a record to its owning userId
	// so `live.forget` can drop the user's undelivered payloads. The DLQ is one
	// bounded collection on a single slot, so the extractor runs at purge time
	// over a full scan (no write-path index needed). Unset => not user-purgeable.
	const forgetUserId = options.forgetUserId;
	const max = Number.isInteger(options.max) && options.max > 0 ? options.max : 1000;
	const ttlMs = Number.isInteger(options.ttlMs) && options.ttlMs >= 0 ? options.ttlMs : 0;

	const redis = client.redis;
	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic;
	const mAdded = m?.counter('dead_letter_added_total', 'Undeliverable webhook events captured to the dead-letter store', ['topic']);

	const seqKey = client.key('dlq:seq:{dlq}');
	const recsKey = client.key('dlq:recs:{dlq}');
	const orderKey = client.key('dlq:order:{dlq}');

	/** Merge the hash field key (the id) back into the parsed record. */
	function parseRecord(id, json) {
		if (json == null) return null;
		let rec;
		try { rec = JSON.parse(json); } catch { return null; }
		if (rec == null || typeof rec !== 'object') return null;
		return { id: String(id), ...rec };
	}

	/**
	 * Evict expired (TTL) and over-cap records. `ref` is the reference "now" for
	 * the TTL cutoff - the just-captured record's failedAt, which is ~wall-now.
	 * Best-effort: a failure here never fails the add (capture is best-effort).
	 */
	async function evict(ref) {
		if (ttlMs > 0) {
			// Inclusive cutoff (drop records at least ttlMs old). 1ms of TTL
			// precision is immaterial for a dead-letter retention window.
			const cutoff = ref - ttlMs;
			const expired = await withBreaker(b, () => redis.zrangebyscore(orderKey, '-inf', cutoff));
			if (expired && expired.length) {
				await withBreaker(b, () => redis.zrem(orderKey, ...expired));
				await withBreaker(b, () => redis.hdel(recsKey, ...expired));
			}
		}
		const card = await withBreaker(b, () => redis.zcard(orderKey));
		if (card > max) {
			const over = await withBreaker(b, () => redis.zrange(orderKey, 0, card - max - 1));
			if (over && over.length) {
				await withBreaker(b, () => redis.zrem(orderKey, ...over));
				await withBreaker(b, () => redis.hdel(recsKey, ...over));
			}
		}
	}

	return {
		/** @param {{ webhookId: string, topic: string, event: string, data: unknown, attempts: number, error: string, failedAt?: number }} rec */
		async add(rec) {
			const failedAt = typeof rec.failedAt === 'number' ? rec.failedAt : 0;
			const json = JSON.stringify({
				webhookId: rec.webhookId,
				topic: rec.topic,
				event: rec.event,
				data: rec.data,
				attempts: rec.attempts | 0,
				error: rec.error,
				failedAt
			});
			const id = String(await withBreaker(b, () => redis.incr(seqKey)));
			await withBreaker(b, () => redis.hset(recsKey, id, json));
			await withBreaker(b, () => redis.zadd(orderKey, failedAt, id));
			await evict(failedAt);
			mAdded?.inc({ topic: mt ? mt(rec.topic) : rec.topic });
			return id;
		},

		/** @param {string} id */
		async get(id) {
			const json = await withBreaker(b, () => redis.hget(recsKey, String(id)));
			return parseRecord(id, json);
		},

		/** @param {string} id @returns {Promise<boolean>} */
		async remove(id) {
			const removed = await withBreaker(b, () => redis.hdel(recsKey, String(id)));
			await withBreaker(b, () => redis.zrem(orderKey, String(id)));
			return removed > 0;
		},

		/** @param {{ topic?: string }} [filter] */
		async count(filter) {
			if (!filter || filter.topic === undefined) {
				return await withBreaker(b, () => redis.zcard(orderKey));
			}
			const vals = await withBreaker(b, () => redis.hvals(recsKey));
			let n = 0;
			for (const v of vals) {
				const rec = parseRecord('0', v);
				if (rec && rec.topic === filter.topic) n++;
			}
			return n;
		},

		/** @param {{ topic?: string, limit?: number }} [filter] */
		async list(filter = {}) {
			const limit = Number.isInteger(filter.limit) && filter.limit > 0 ? filter.limit : 100;
			// Newest-first by failedAt via the order set, then hydrate from the hash.
			const ids = await withBreaker(b, () => redis.zrevrange(orderKey, 0, -1));
			if (!ids || ids.length === 0) return [];
			const jsons = await withBreaker(b, () => redis.hmget(recsKey, ...ids));
			const out = [];
			for (let i = 0; i < ids.length && out.length < limit; i++) {
				const rec = parseRecord(ids[i], jsons[i]);
				if (!rec) continue;
				if (filter.topic === undefined || rec.topic === filter.topic) out.push(rec);
			}
			return out;
		},

		async summary() {
			const vals = await withBreaker(b, () => redis.hvals(recsKey));
			/** @type {Record<string, number>} */
			const byTopic = {};
			let oldest = null;
			let newest = null;
			let total = 0;
			for (const v of vals) {
				const rec = parseRecord('0', v);
				if (!rec) continue;
				total++;
				byTopic[rec.topic] = (byTopic[rec.topic] || 0) + 1;
				if (oldest === null || rec.failedAt < oldest) oldest = rec.failedAt;
				if (newest === null || rec.failedAt > newest) newest = rec.failedAt;
			}
			return { total, byTopic, oldest, newest };
		},

		/**
		 * Right-to-erasure (`live.forget`): drop every DLQ record belonging to a
		 * user. Scans the bounded single-slot collection, maps each record through
		 * `forgetUserId`, and HDELs + ZREMs the matches in one MULTI (the three
		 * keys share the {dlq} slot). A no-op without a `forgetUserId` extractor.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} records removed
		 */
		async purgeUser(tenantId, userId) {
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			return withBreaker(b, async () => {
				const all = await redis.hgetall(recsKey);
				if (!all) return 0;
				const ids = [];
				for (const id of Object.keys(all)) {
					const rec = parseRecord(id, all[id]);
					if (!rec) continue;
					let uid;
					try { uid = forgetUserId(rec); } catch { continue; }
					if (uid === userId) ids.push(id);
				}
				if (ids.length === 0) return 0;
				const tx = redis.multi();
				tx.hdel(recsKey, ...ids);
				tx.zrem(orderKey, ...ids);
				await tx.exec();
				return ids.length;
			});
		},

		async clear() {
			await withBreaker(b, () => redis.del(seqKey, recsKey, orderKey));
		}
	};
}
