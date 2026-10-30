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
 *   - `{prefix}dlq:seq:{dlq}`          - INCR counter for record ids
 *   - `{prefix}dlq:recs:{dlq}`         - HASH id -> JSON record (without the id field)
 *   - `{prefix}dlq:order:{dlq}`        - sorted set, score = failedAt, member = id
 *   - `{prefix}dlq:tenant:{dlq}`       - HASH id -> tenant scope of the record
 *   - `{prefix}dlq:tenant-order:{dlq}` - sorted set, score 0, member =
 *     `<scope>/<id zero-padded to 20 digits>`, so a lex range yields one
 *     scope's records oldest first
 *   - `{prefix}dlq:tenant-size:{dlq}`  - sorted set, member = scope, score = its record count
 *   - `{prefix}dlq:tenant-rank:{dlq}`  - sorted set, score 0, one member per scope:
 *     `<999999999999999 - count, 15 digits>/<oldest id, 20 digits>/<scope>`, so
 *     the lowest member is the largest scope, ties to the oldest record
 *
 * The literal `{dlq}` braces are a Redis Cluster hash tag, so every key shares
 * one slot and each mutation is one Lua script over all of them: a record can
 * never exist in one structure and not another (ZCARD of the order set stays
 * the authoritative count). A webhook DLQ is low-volume and capped, so
 * co-locating the whole collection on one slot is intentional, not a
 * bottleneck.
 *
 * Fair-share eviction: above `max`, the capture script evicts the oldest record
 * of the tenant scope holding the most records, one at a time; between equally
 * large scopes, the one holding the oldest record pays, so a capture is never
 * evicted while an older record of an equally large scope exists. A scope
 * holding at most `max / scopes present` records is never the largest while
 * the store is over `max`, so another tenant's overflow cannot evict it. The
 * scope is the one the erasure tombstones use (`@t/<tenant>/` topics, `~` for
 * untenanted); a malformed tenant topic counts in the scope `''`. A record
 * stored without accounting (by an earlier release) is assigned the scope of
 * its stored topic when the next capture rebuilds the accounting.
 *
 * Retention clocks off the SERVER (`TIME`), never the caller-supplied
 * `failedAt`: a skewed or hostile producer stamp can then neither mass-evict
 * healthy records nor pin its own forever. With `ttlMs` set, each add also
 * re-arms a PEXPIRE on every key as a quiet-queue backstop, so an idle
 * DLQ self-cleans within `ttlMs` instead of retaining payloads indefinitely.
 *
 * Reads apply the same server-clock cutoff, so with `ttlMs` set an expired
 * record is never returned, counted or summarized, even before the next add
 * evicts it.
 *
 * Right-to-erasure completeness: `purgeUser` writes a forget tombstone for the
 * purged (tenant scope, user) pair (`{prefix}dlq:purged:<tenant or ~>/<user>` =
 * server time at purge, `forgetTombstoneMs` PX, default 10 min) BEFORE it
 * deletes the user's records, and `add` drops a record of that user in that
 * scope whose delivery was in flight when that purge ran - otherwise a
 * long-running delivery that fails AFTER a `live.forget` would resurrect the
 * erased payload. The purge time and
 * the delivery start are both reconciled onto the shared Redis server clock, so
 * the check is correct across a cluster (a per-process monotonic start on its
 * own is not comparable between instances).
 *
 * @module svelte-adapter-uws-extensions/redis/dead-letter
 */

import { withBreaker } from '../shared/breaker.js';
import { evalCached } from '../shared/eval-cached.js';
import { monotonicNow } from '../shared/runtime.js';
import { topicInTenant, topicTenantScope } from '../shared/tenant-topic.js';
import { EXTENSIONS_ERROR_IDS } from '../error-registry.js';
import { emitRegisteredFailure, firstReportInProcess } from '../shared/diagnostic.js';
import { defineOptions, option } from '../shared/options.js';

// A forget tombstone lives long enough to catch a webhook delivery that was
// already in flight when `live.forget` ran and only fails (and would be
// captured) AFTER the purge swept the store. Sized well above a normal retry
// budget; a single delivery whose total duration exceeds this is a narrow,
// documented residual. Mirrors the in-memory store's 10-minute window.
const FORGET_TOMBSTONE_MS = 10 * 60 * 1000;

// KEYS: seq, recs, order, tenant, tenant-order, tenant-size, tenant-rank.
// `drop` removes one record from every structure and returns 1 when the
// record existed; `rerank` keeps a scope's rank member in step with a change
// to its records. `scope_of` reads a stored record's topic with the tenant
// rule of topicTenantScope, for records stored without accounting.
const PRELUDE = `
local function entry(scope, id)
	return scope .. '/' .. string.rep('0', 20 - #id) .. id
end
local function rank_of(scope)
	local count = redis.call('ZSCORE', KEYS[6], scope)
	local first = redis.call('ZRANGEBYLEX', KEYS[5], '[' .. scope .. '/', '(' .. scope .. '0', 'LIMIT', 0, 1)[1]
	if not count or not first then return nil end
	return string.format('%015.0f', 999999999999999 - tonumber(count)) .. '/' .. string.sub(first, #scope + 2) .. '/' .. scope
end
local function rerank(scope, change)
	local before = rank_of(scope)
	local result = change()
	local after = rank_of(scope)
	if before ~= after then
		if before then redis.call('ZREM', KEYS[7], before) end
		if after then redis.call('ZADD', KEYS[7], 0, after) end
	end
	return result
end
local function drop(id)
	local scope = redis.call('HGET', KEYS[4], id)
	if not scope then
		redis.call('ZREM', KEYS[3], id)
		return redis.call('HDEL', KEYS[2], id)
	end
	return rerank(scope, function()
		redis.call('HDEL', KEYS[4], id)
		redis.call('ZREM', KEYS[5], entry(scope, id))
		if tonumber(redis.call('ZINCRBY', KEYS[6], -1, scope)) <= 0 then redis.call('ZREM', KEYS[6], scope) end
		redis.call('ZREM', KEYS[3], id)
		return redis.call('HDEL', KEYS[2], id)
	end)
end
local function scope_of(json)
	local ok, rec = pcall(cjson.decode, json)
	local topic = nil
	if ok and type(rec) == 'table' then topic = rec.topic end
	if type(topic) ~= 'string' then return '' end
	if string.sub(topic, 1, 3) ~= '@t/' then return '~' end
	local stop = string.find(topic, '/', 4, true)
	if stop and stop > 4 then return string.sub(topic, 4, stop - 1) end
	return ''
end
`;

// ARGV: record json, failedAt, tenant scope, max, ttlMs, expiry cutoff.
// Returns { id, records evicted by the max cap }.
const ADD_SCRIPT = `-- DLQ_ADD
${PRELUDE}
if redis.call('HLEN', KEYS[4]) ~= redis.call('ZCARD', KEYS[3]) or redis.call('ZCARD', KEYS[7]) ~= redis.call('ZCARD', KEYS[6]) then
	-- Records without a tenant entry (stored by an earlier release) get the
	-- scope of their stored topic; entries whose record is gone are dropped,
	-- and the index, counts and ranks are rebuilt from the order set.
	redis.call('DEL', KEYS[5], KEYS[6], KEYS[7])
	for _, id in ipairs(redis.call('HKEYS', KEYS[4])) do
		if not redis.call('ZSCORE', KEYS[3], id) then redis.call('HDEL', KEYS[4], id) end
	end
	for _, id in ipairs(redis.call('ZRANGE', KEYS[3], 0, -1)) do
		local scope = redis.call('HGET', KEYS[4], id)
		if not scope then
			scope = scope_of(redis.call('HGET', KEYS[2], id))
			redis.call('HSET', KEYS[4], id, scope)
		end
		redis.call('ZADD', KEYS[5], 0, entry(scope, id))
		redis.call('ZINCRBY', KEYS[6], 1, scope)
	end
	for _, scope in ipairs(redis.call('ZRANGE', KEYS[6], 0, -1)) do
		local rank = rank_of(scope)
		if rank then redis.call('ZADD', KEYS[7], 0, rank) end
	end
end
redis.call('INCR', KEYS[1])
local id = redis.call('GET', KEYS[1])
redis.call('HSET', KEYS[2], id, ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[2], id)
rerank(ARGV[3], function()
	redis.call('HSET', KEYS[4], id, ARGV[3])
	redis.call('ZADD', KEYS[5], 0, entry(ARGV[3], id))
	redis.call('ZINCRBY', KEYS[6], 1, ARGV[3])
end)
local ttl = tonumber(ARGV[5])
if ttl > 0 then
	for _, expired in ipairs(redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[6])) do drop(expired) end
end
local max = tonumber(ARGV[4])
local evicted = 0
while redis.call('ZCARD', KEYS[3]) > max do
	-- The lowest rank member: the largest scope, ties to the oldest record.
	local top = redis.call('ZRANGEBYLEX', KEYS[7], '-', '+', 'LIMIT', 0, 1)[1]
	if not top then break end
	local scope = string.sub(top, 38)
	local oldest = redis.call('ZRANGEBYLEX', KEYS[5], '[' .. scope .. '/', '(' .. scope .. '0', 'LIMIT', 0, 1)[1]
	local victim = oldest and string.match(string.sub(oldest, #scope + 2), '^0*(%d+)$')
	if victim then evicted = evicted + drop(victim) end
	-- An index or rank entry left without its record goes too, so each
	-- round removes something and the loop ends.
	if oldest then redis.call('ZREM', KEYS[5], oldest) end
	redis.call('ZREM', KEYS[7], top)
	local rank = rank_of(scope)
	if rank then redis.call('ZADD', KEYS[7], 0, rank) end
end
-- Last, because a drop can empty a key and recreate it without a TTL.
if ttl > 0 then
	for i = 1, 7 do redis.call('PEXPIRE', KEYS[i], ttl) end
end
return { id, evicted }
`;

// ARGV: ttlMs, then record ids. Returns how many existed. A drop can empty
// the rank key and recreate it, so it gets the add's TTL backstop again.
const DROP_SCRIPT = `-- DLQ_DROP
${PRELUDE}
local removed = 0
for i = 2, #ARGV do removed = removed + drop(ARGV[i]) end
local ttl = tonumber(ARGV[1])
if ttl > 0 and redis.call('PTTL', KEYS[7]) == -1 then redis.call('PEXPIRE', KEYS[7], ttl) end
return removed
`;

/**
 * @typedef {Object} RedisDeadLetterOptions
 * @property {number} [max=1000] - Max retained records. Above it the oldest
 *   record of the tenant scope holding the most records is evicted first.
 * @property {number} [ttlMs=0] - Drop records older than this many ms (0 = no TTL).
 *   Enforced on each write against the server clock, with a key-TTL backstop so
 *   an idle queue still self-cleans within `ttlMs` of its last write.
 * @property {object} [breaker] - Circuit breaker for fault isolation (shared `createCircuitBreaker`).
 * @property {object} [metrics] - Prometheus registry for the `dead_letter_added_total`
 *   and `dead_letter_evicted_total` counters.
 * @property {number} [forgetTombstoneMs=600000] - How long a per-user forget
 *   tombstone lives (its PX). A delivery whose total retry duration outlives this
 *   resurrects the erased payload once the tombstone expires; size it above your
 *   max retry budget. Default 10 minutes (matches the in-memory store).
 */

const validateOptions = defineOptions({
	factory: 'createDeadLetter',
	label: 'redis dead-letter',
	types: 'RedisDeadLetterOptions in redis/dead-letter.d.ts'
}, {
	max: option.known,
	ttlMs: option.known,
	forgetUserId: option.known,
	forgetTombstoneMs: option.known,
	breaker: option.collaborator,
	metrics: option.collaborator
});

/**
 * Create a Redis-backed dead-letter store.
 *
 * @param {import('./index.js').RedisClient} client
 * @param {RedisDeadLetterOptions} [options]
 */
export function createDeadLetter(client, options = {}) {
	options = validateOptions(options);
	if (options.max !== undefined && (!Number.isInteger(options.max) || options.max <= 0)) {
		throw new Error(`redis dead-letter: max must be a positive integer, got ${options.max}`);
	}
	if (options.ttlMs !== undefined && (!Number.isInteger(options.ttlMs) || options.ttlMs < 0)) {
		throw new Error(`redis dead-letter: ttlMs must be a non-negative integer, got ${options.ttlMs}`);
	}
	if (options.forgetUserId !== undefined && typeof options.forgetUserId !== 'function') {
		throw new Error('redis dead-letter: forgetUserId must be a function (record) => userId');
	}
	if (options.forgetTombstoneMs !== undefined && (!Number.isInteger(options.forgetTombstoneMs) || options.forgetTombstoneMs <= 0)) {
		throw new Error(`redis dead-letter: forgetTombstoneMs must be a positive integer, got ${options.forgetTombstoneMs}`);
	}
	// Right-to-erasure: a DLQ record's payload is app-defined, so the store cannot
	// tell whose record it is. When set, this maps a record to its owning userId
	// so `live.forget` can drop the user's undelivered payloads. The DLQ is one
	// bounded collection on a single slot, so the extractor runs at purge time
	// over a full scan (no write-path index needed). Unset => not user-purgeable.
	const forgetUserId = options.forgetUserId;
	const max = Number.isInteger(options.max) && options.max > 0 ? options.max : 1000;
	const ttlMs = Number.isInteger(options.ttlMs) && options.ttlMs >= 0 ? options.ttlMs : 0;
	if (ttlMs === 0 && !forgetUserId && firstReportInProcess(EXTENSIONS_ERROR_IDS.REDIS_DEAD_LETTER_RETENTION_UNBOUNDED)) {
		emitRegisteredFailure(EXTENSIONS_ERROR_IDS.REDIS_DEAD_LETTER_RETENTION_UNBOUNDED);
	}
	// The forget tombstone window: how long `{prefix}dlq:purged:<user>` lives
	// (its PX). A single delivery whose total retry duration outlives this
	// resurrects the erased payload once the tombstone expires - the documented
	// residual. An operator whose max retry budget exceeds the 10-minute default
	// sizes this above it so no in-flight delivery can outlast the tombstone.
	const forgetTombstoneMs = Number.isInteger(options.forgetTombstoneMs) && options.forgetTombstoneMs > 0
		? options.forgetTombstoneMs
		: FORGET_TOMBSTONE_MS;

	const redis = client.redis;
	const b = options.breaker;
	const m = options.metrics;
	const mt = m?.mapTopic || (() => 'all');
	const mAdded = m?.counter('dead_letter_added_total', 'Undeliverable webhook events captured to the dead-letter store', ['topic']);
	const mEvicted = m?.counter('dead_letter_evicted_total', 'Dead-letter records evicted by the max cap from the tenant scope holding the most records');

	const seqKey = client.key('dlq:seq:{dlq}');
	const recsKey = client.key('dlq:recs:{dlq}');
	const orderKey = client.key('dlq:order:{dlq}');
	const keys = [seqKey, recsKey, orderKey, client.key('dlq:tenant:{dlq}'), client.key('dlq:tenant-order:{dlq}'), client.key('dlq:tenant-size:{dlq}'), client.key('dlq:tenant-rank:{dlq}')];

	// Forget tombstone for one (tenant scope, user) pair. The purge deletes only
	// records whose topic is inside its tenant scope, so the tombstone carries
	// the same scope and `add` looks it up with the scope of the record's own
	// topic: purging a user in one tenant never drops another tenant's capture
	// for the same userId. `~` names the untenanted scope; purgeUser refuses a
	// tenant id holding `~` or `/`, so `<scope>/<user>` is unambiguous. Not
	// hash-tagged into the {dlq} slot: it is only ever read or written on its own.
	function purgedKey(scope, userId) {
		return client.key('dlq:purged:' + scope + '/' + userId);
	}
	// Tombstones written before tombstones were scoped are keyed by the bare
	// userId. `add` still honors them so a purge that ran on an earlier version
	// keeps dropping late captures until its tombstone expires.
	function legacyPurgedKey(userId) {
		return client.key('dlq:purged:' + userId);
	}
	async function serverTime() {
		const t = await redis.time();
		return Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
	}

	// Records at or before this server-clock cutoff are expired: the same
	// inclusive cutoff the capture script applies. -Infinity without a ttlMs.
	async function readCutoff() {
		if (ttlMs === 0) return -Infinity;
		return (await withBreaker(b, serverTime)) - ttlMs;
	}
	const live = (rec, cutoff) => rec !== null && !(rec.failedAt <= cutoff);

	/** Merge the hash field key (the id) back into the parsed record. */
	function parseRecord(id, json) {
		if (json == null) return null;
		let rec;
		try { rec = JSON.parse(json); } catch { return null; }
		if (rec == null || typeof rec !== 'object') return null;
		return { id: String(id), ...rec };
	}

	/** Remove records from every structure in one script; resolves how many existed. */
	function drop(ids) {
		return withBreaker(b, () => evalCached(redis, DROP_SCRIPT, keys.length, ...keys, ttlMs, ...ids));
	}

	return {
		/**
		 * Retain an undeliverable event. Resolves the new record id, or `null` when
		 * a forget tombstone drops it (its delivery was in flight when `live.forget`
		 * ran). `startedAt` is the delivery start as a monotonic stamp (threaded by
		 * svelte-realtime's `_fireWebhookOut`); used only for the forget-race check,
		 * never stored.
		 * @param {{ webhookId: string, topic: string, event: string, data: unknown, attempts: number, error: string, failedAt?: number, startedAt?: number }} rec
		 * @returns {Promise<string | null>}
		 */
		async add(rec) {
			// One TIME round-trip per add is fine - DLQ adds are a rare error
			// path. The caller's failedAt is kept for display, clamped to
			// (0, serverNow]: an implausible stamp (missing, non-finite, <= 0,
			// or in the server's future) becomes serverNow.
			const serverNow = await withBreaker(b, serverTime);

			// Forget tombstone (right-to-erasure completeness): if this record's
			// owning user was purged (live.forget) while this delivery was already
			// in flight, the purge scan ran BEFORE this capture, so persisting now
			// would resurrect the erased user's payload past a confirmed erasure.
			// Drop it instead. The purge time and the delivery start are reconciled
			// onto the shared Redis SERVER clock: the tombstone stores server time at
			// purge; the delivery start is serverNow minus the delivery's monotonic
			// duration (both stamps from THIS delivering process), so the comparison
			// is immune to inter-instance wall-clock skew - a cross-instance monotonic
			// startedAt on its own would be meaningless. The round-trip between the
			// TIME read and monotonicNow() only nudges serverStart slightly earlier,
			// biasing toward dropping (the GDPR-safe direction). An absent startedAt
			// -> elapsed 0 -> serverStart = serverNow, so a past purge never drops it
			// (matches the in-memory store's untraced-start fallback).
			if (forgetUserId) {
				let uid;
				try { uid = forgetUserId(rec); } catch { uid = undefined; }
				const scope = topicTenantScope(rec.topic);
				if (typeof uid === 'string' && uid.length > 0 && scope !== null) {
					// Separate GETs: the two keys may live on different Cluster slots.
					const [scoped, legacy] = await withBreaker(b, () => Promise.all([
						redis.get(purgedKey(scope, uid)),
						redis.get(legacyPurgedKey(uid))
					]));
					const tomb = scoped == null ? legacy : legacy == null ? scoped : String(Math.max(Number(scoped), Number(legacy)));
					if (tomb != null) {
						const purgeTime = Number(tomb);
						const elapsed =
							typeof rec.startedAt === 'number' && Number.isFinite(rec.startedAt)
								? Math.max(0, monotonicNow() - rec.startedAt)
								: 0;
						const serverStart = serverNow - elapsed;
						if (Number.isFinite(purgeTime) && purgeTime >= serverStart) {
							return null;
						}
					}
				}
			}
			// Floor to integer ms then clamp, matching the Postgres backend's
			// LEAST(NULLIF(floor($7), 0), now): a fractional sub-1ms stamp floors
			// to 0 and becomes serverNow on both backends rather than diverging.
			const flooredFailedAt =
				typeof rec.failedAt === 'number' && Number.isFinite(rec.failedAt) && rec.failedAt > 0
					? Math.floor(rec.failedAt)
					: 0;
			const failedAt =
				flooredFailedAt > 0 && flooredFailedAt <= serverNow ? flooredFailedAt : serverNow;
			const json = JSON.stringify({
				webhookId: rec.webhookId,
				topic: rec.topic,
				event: rec.event,
				data: rec.data,
				attempts: rec.attempts | 0,
				error: rec.error,
				failedAt
			});
			// One script inserts the record, re-arms the key TTL backstop (with
			// failedAt <= serverNow every live record is due at or before it, so
			// nothing is dropped early and an idle DLQ self-cleans within ttlMs),
			// drops records at or before the inclusive server-clock cutoff, and
			// evicts over-cap records from the largest tenant scope. The record
			// stamps are display data; serverNow is the only expiry reference.
			const [id, evicted] = await withBreaker(b, () => evalCached(
				redis, ADD_SCRIPT, keys.length, ...keys,
				json, failedAt, topicTenantScope(rec.topic) ?? '', max, ttlMs, ttlMs > 0 ? serverNow - ttlMs : 0
			));
			mAdded?.inc({ topic: mt(rec.topic) });
			if (Number(evicted) > 0) mEvicted?.inc(Number(evicted));
			return String(id);
		},

		/** @param {string} id */
		async get(id) {
			const [cutoff, json] = await Promise.all([readCutoff(), withBreaker(b, () => redis.hget(recsKey, String(id)))]);
			const rec = parseRecord(id, json);
			return live(rec, cutoff) ? rec : null;
		},

		/** @param {string} id @returns {Promise<boolean>} */
		async remove(id) {
			return Number(await drop([String(id)])) > 0;
		},

		/** @param {{ topic?: string }} [filter] */
		async count(filter) {
			const cutoff = await readCutoff();
			if (!filter || filter.topic === undefined) {
				if (cutoff === -Infinity) return await withBreaker(b, () => redis.zcard(orderKey));
				const [card, expired] = await withBreaker(b, () => Promise.all([
					redis.zcard(orderKey),
					redis.zcount(orderKey, '-inf', cutoff)
				]));
				return Math.max(0, Number(card) - Number(expired));
			}
			const vals = await withBreaker(b, () => redis.hvals(recsKey));
			let n = 0;
			for (const v of vals) {
				const rec = parseRecord('0', v);
				if (live(rec, cutoff) && rec.topic === filter.topic) n++;
			}
			return n;
		},

		/** @param {{ topic?: string, limit?: number }} [filter] */
		async list(filter = {}) {
			const limit = Number.isInteger(filter.limit) && filter.limit > 0 ? filter.limit : 100;
			// Newest-first by failedAt via the order set, then hydrate from the hash.
			const cutoff = await readCutoff();
			const ids = await withBreaker(b, () => redis.zrevrange(orderKey, 0, -1));
			if (!ids || ids.length === 0) return [];
			const jsons = await withBreaker(b, () => redis.hmget(recsKey, ...ids));
			const out = [];
			for (let i = 0; i < ids.length && out.length < limit; i++) {
				const rec = parseRecord(ids[i], jsons[i]);
				if (!live(rec, cutoff)) continue;
				if (filter.topic === undefined || rec.topic === filter.topic) out.push(rec);
			}
			return out;
		},

		async summary() {
			const cutoff = await readCutoff();
			const vals = await withBreaker(b, () => redis.hvals(recsKey));
			/** @type {Record<string, number>} */
			// Null-prototype so a stored topic named '__proto__' counts as
			// ordinary data instead of hitting the prototype setter.
			const byTopic = Object.create(null);
			let oldest = null;
			let newest = null;
			let total = 0;
			for (const v of vals) {
				const rec = parseRecord('0', v);
				if (!live(rec, cutoff)) continue;
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
		 * `forgetUserId`, and removes the matches from every structure in one
		 * script (the keys share the {dlq} slot). A no-op without a `forgetUserId`
		 * extractor.
		 * @param {string | null} tenantId
		 * @param {string} userId
		 * @returns {Promise<number>} records removed
		 */
		async purgeUser(tenantId, userId) {
			// The tombstone key joins scope and user with `/` and names the
			// untenanted scope `~`, so a tenant id holding either character would
			// write a tombstone that `add` never looks up.
			if (tenantId != null && tenantId !== '' && (typeof tenantId !== 'string' || /[/~]/.test(tenantId))) {
				throw new TypeError('redis dead-letter: purgeUser tenant id must be null or a string without "/" or "~"');
			}
			if (!forgetUserId || typeof userId !== 'string' || userId.length === 0) return 0;
			return withBreaker(b, async () => {
				// Tombstone FIRST (server clock): a delivery already in flight when
				// this purge runs is captured by add() only AFTER this scan returns,
				// so record the purge time so that late add() drops it. Written even
				// with zero matching records - the in-flight delivery may fail moments
				// later. PX bounds it to recent erasures (a delivery outliving the
				// window is the documented residual, sized by `forgetTombstoneMs`).
				// Server clock (TIME), never the caller's, so add()'s server-clock
				// comparison is skew-immune.
				const serverNow = await serverTime();
				await redis.set(purgedKey(tenantId || '~', userId), String(serverNow), 'PX', forgetTombstoneMs);

				const all = await redis.hgetall(recsKey);
				if (!all) return 0;
				const ids = [];
				for (const id of Object.keys(all)) {
					const rec = parseRecord(id, all[id]);
					if (!rec) continue;
					// Same tenant rule as the sibling legs of this purge: a record
					// whose topic is outside the scope belongs to another tenant.
					if (!topicInTenant(tenantId, rec.topic)) continue;
					let uid;
					try { uid = forgetUserId(rec); } catch { continue; }
					if (uid === userId) ids.push(id);
				}
				if (ids.length === 0) return 0;
				await evalCached(redis, DROP_SCRIPT, keys.length, ...keys, ttlMs, ...ids);
				return ids.length;
			});
		},

		async clear() {
			await withBreaker(b, () => redis.del(...keys));
		}
	};
}
