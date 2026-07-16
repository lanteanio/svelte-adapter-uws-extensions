/**
 * Durable right-to-erasure store for `live.forget`.
 *
 * Composes the per-backend stores an app already wired (connection registry,
 * idempotency, presence, cursor, ...) into the single duck-typed store the
 * realtime layer consumes via `configureForget({ store })`. Each composed store
 * exposes `purgeUser(tenantId, userId, cascade) => Promise<number>`; this
 * fan-out calls them all, returns a per-store breakdown, and - critically for a
 * GDPR erasure - attempts EVERY store even if one fails, then rejects if any
 * failed so the realtime layer surfaces the incomplete erasure for retry
 * (`live.forget` maps a durable rejection to `FORGET_STORE_FAILED`).
 *
 * With `options.redis` (the same client the app stashes on `platform.redis`),
 * the purge additionally erases the user from the CLUSTER-WIDE room state the
 * realtime layer keeps in Redis, and the result becomes the owner-succession
 * envelope `{ rowsAffected, ownerSuccessions }`:
 *
 * - Room-owner hashes (`__live-room-owner:<topic>`): the user's membership
 *   fields are removed from every room in the tenant scope, and where the user
 *   held the owner role an atomic script picks the successor (lowest join
 *   sequence, the same rule as the realtime leave transition) or vacates the
 *   emptied room. Removing the membership everywhere matters beyond the owned
 *   rooms: a leftover join-sequence field could otherwise elect the ERASED
 *   user as a later succession's winner. The ownership check runs INSIDE the
 *   script, so a room whose owner already changed (a local leave racing this
 *   purge on another instance) is skipped without a spurious change.
 * - Presence rosters (`__live-presence:<topic>`): the user's count/data fields
 *   are removed from every roster in the tenant scope. The in-memory presence
 *   purge only releases refs LOCAL to the forgetting instance; refs held on
 *   other instances would otherwise leave the erased user visible in every
 *   roster snapshot until the TTL.
 *
 * The realtime layer publishes each returned succession on the room's `:owner`
 * stream (through the replay buffer), so live subscribers on every replica and
 * resumers see the successor rather than the erased owner. Without
 * `options.redis` the return shape is the plain per-store breakdown,
 * byte-identical to before.
 *
 * The realtime layer never imports this; it only sees `{ purgeUser }`.
 *
 * @example
 * ```js
 * import { createForgetStore } from 'svelte-adapter-uws-extensions/forget-store';
 * import { configureForget } from 'svelte-realtime/server';
 *
 * configureForget({
 *   store: createForgetStore({ registry, idempotency }, { redis }),
 *   platform // for the in-memory presence cluster-roster leg
 * });
 * ```
 *
 * @module svelte-adapter-uws-extensions/forget-store
 */

import { scanKeys } from './redis-scan.js';
import { evalCached } from './eval-cached.js';

// The realtime layer's cluster key shapes (room-owner.js / presence.js /
// tenant.js). These are wire-frozen: the hashes are written through the raw
// `platform.redis` client with no store-level prefix, the owner hash carries
// 'o' (owner) / 'q' (join-seq allocator) / 'j:<key>' (member join seq) /
// 'n:<key>' (cluster connection count), the presence roster carries
// 'c:<key>' (count) / 'd:<key>' (data JSON), and a tenant-scoped wire topic
// is '@t/<tenantId>/<topic>'.
const OWNER_KEY_PREFIX = '__live-room-owner:';
const PRESENCE_KEY_PREFIX = '__live-presence:';
const TENANT_TOPIC_NS = '@t/';
// Mirrors the realtime owner hash's idle TTL: a purge that touches a
// still-populated hash refreshes the same expiry the join/leave transitions
// apply, so the purge never shortens or removes a live room's TTL contract.
const OWNER_TTL_SEC = 3600;

// Force-evict one user from one room-owner hash, atomically. Removes the
// user's membership fields unconditionally; when the user holds the owner
// role, picks the successor exactly like the realtime leave transition (the
// remaining member with the lowest join sequence, lexicographic key compare
// as the tie backstop) or clears the role and allocator when the room
// emptied. The o == user check lives INSIDE the script so a concurrent
// owner change on another replica makes this a no-op rather than a second,
// conflicting succession. Returns {ownerOrEmpty, reason, touchedFieldCount}
// with reason '' when ownership did not change.
const OWNER_FORGET_EVICT_SCRIPT =
	'-- OWNER_FORGET_EVICT\n' +
	"local k = ARGV[1]\n" +
	"local removed = redis.call('HDEL', KEYS[1], 'j:' .. k, 'n:' .. k)\n" +
	"local o = redis.call('HGET', KEYS[1], 'o')\n" +
	"if not o or o ~= k then\n" +
	"  if removed > 0 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end\n" +
	"  return {o or '', '', removed}\n" +
	"end\n" +
	"local all = redis.call('HGETALL', KEYS[1])\n" +
	"local best = nil\n" +
	"local bestSeq = nil\n" +
	"for i = 1, #all, 2 do\n" +
	"  local f = all[i]\n" +
	"  if string.sub(f, 1, 2) == 'j:' then\n" +
	"    local cand = string.sub(f, 3)\n" +
	"    local s = tonumber(all[i + 1])\n" +
	"    if bestSeq == nil or s < bestSeq or (s == bestSeq and cand < best) then\n" +
	"      best = cand\n" +
	"      bestSeq = s\n" +
	"    end\n" +
	"  end\n" +
	"end\n" +
	"if best then\n" +
	"  redis.call('HSET', KEYS[1], 'o', best)\n" +
	"  redis.call('EXPIRE', KEYS[1], ARGV[2])\n" +
	"  return {best, 'succeeded', removed + 1}\n" +
	"end\n" +
	"redis.call('HDEL', KEYS[1], 'o', 'q')\n" +
	"return {'', 'vacated', removed + 1}";

/**
 * Whether a WIRE topic falls inside a tenant scope - the same rule as the
 * realtime layer's own purge scoping: with a tenantId, true iff the topic
 * carries that tenant's namespace prefix; with null, true iff the topic
 * carries NO tenant prefix. A null-tenant erasure can never reach another
 * tenant's rooms and vice versa.
 * @param {string | null} tenantId
 * @param {string} topic
 * @returns {boolean}
 */
function topicInTenant(tenantId, topic) {
	if (tenantId) return topic.startsWith(TENANT_TOPIC_NS + tenantId + '/');
	return !topic.startsWith(TENANT_TOPIC_NS);
}

/**
 * An ioredis `keyPrefix` applies transparently to command keys but NOT to a
 * SCAN MATCH pattern, and scanned keys come back raw (prefix included). Scan
 * with the prefix prepended and strip it from each hit, so the stripped key
 * round-trips through prefixed commands correctly.
 * @param {any} redis
 * @returns {string}
 */
function clientKeyPrefix(redis) {
	const p = redis && redis.options && redis.options.keyPrefix;
	return typeof p === 'string' ? p : '';
}

/**
 * Enumerate the realtime cluster keys under `keyPrefix` (cluster-aware: one
 * SCAN per master) and return `{ key, topic }` pairs for the topics inside
 * the tenant scope, with `key` ready for prefixed command use.
 * @param {any} redis
 * @param {string} keyPrefix `OWNER_KEY_PREFIX` or `PRESENCE_KEY_PREFIX`
 * @param {string | null} tenantId
 * @returns {Promise<Array<{ key: string, topic: string }>>}
 */
async function scanTenantTopics(redis, keyPrefix, tenantId) {
	const raw = clientKeyPrefix(redis);
	const found = await scanKeys(redis, raw + keyPrefix + '*');
	const out = [];
	for (const rawKey of found) {
		const key = raw !== '' && rawKey.startsWith(raw) ? rawKey.slice(raw.length) : rawKey;
		const topic = key.slice(keyPrefix.length);
		if (topicInTenant(tenantId, topic)) out.push({ key, topic });
	}
	return out;
}

/**
 * Cluster-wide room-owner force-evict for one user. GDPR forgets are rare,
 * so the O(total rooms) SCAN needs no reverse index; each room's transition
 * is one single-key atomic script (cluster-safe without hash tags).
 *
 * Each room's script runs independently and a failing one is collected in
 * `failures` rather than aborting the sweep: a room whose eviction ALREADY
 * committed on this call must still report its succession even when a sibling
 * room throws, because the caller signals the whole erasure incomplete and
 * retries, and the retry finds the committed room already handed off (the
 * in-script owner check) and reports nothing for it - so the committed
 * succession would otherwise never reach the wire.
 * @param {any} redis
 * @param {string | null} tenantId
 * @param {string} userId
 * @returns {Promise<{ touched: number, successions: Array<{ topic: string, owner: string | null, reason: string }>, failures: any[] }>}
 */
async function evictOwnerUser(redis, tenantId, userId) {
	let rooms;
	try {
		rooms = await scanTenantTopics(redis, OWNER_KEY_PREFIX, tenantId);
	} catch (err) {
		// The enumeration itself failed: nothing was committed, the whole leg
		// failed. Report it so the erasure is retried.
		return { touched: 0, successions: [], failures: [err] };
	}
	const successions = [];
	const failures = [];
	let touched = 0;
	const settled = await Promise.allSettled(rooms.map(async ({ key, topic }) => {
		const res = await evalCached(redis, OWNER_FORGET_EVICT_SCRIPT, 1, key, userId, String(OWNER_TTL_SEC));
		return { res, topic };
	}));
	for (const s of settled) {
		if (s.status === 'rejected') { failures.push(s.reason); continue; }
		const { res, topic } = s.value;
		if (!res || !Array.isArray(res)) continue;
		const reason = res[1] == null ? '' : String(res[1]);
		if ((Number(res[2]) || 0) > 0) touched++;
		if (reason !== '') {
			successions.push({
				topic,
				owner: res[0] === '' || res[0] == null ? null : String(res[0]),
				reason
			});
		}
	}
	return { touched, successions, failures };
}

/**
 * Remove the user's count/data fields from every presence roster in the
 * tenant scope. A roster whose fields a still-connected remote instance
 * later decrements self-heals: the release's HINCRBY drives the recreated
 * count to zero-or-below and deletes both fields again. Like the owner
 * eviction, a single roster's failure is collected rather than aborting the
 * sweep, so the erasure of the other rosters is not lost.
 * @param {any} redis
 * @param {string | null} tenantId
 * @param {string} userId
 * @returns {Promise<{ removed: number, failures: any[] }>} roster fields removed + per-roster failures
 */
async function purgePresenceRoster(redis, tenantId, userId) {
	let rosters;
	try {
		rosters = await scanTenantTopics(redis, PRESENCE_KEY_PREFIX, tenantId);
	} catch (err) {
		return { removed: 0, failures: [err] };
	}
	const failures = [];
	let removed = 0;
	const settled = await Promise.allSettled(
		rosters.map(({ key }) => Promise.resolve(redis.hdel(key, 'c:' + userId, 'd:' + userId)))
	);
	for (const s of settled) {
		if (s.status === 'rejected') { failures.push(s.reason); continue; }
		removed += Number(s.value) || 0;
	}
	return { removed, failures };
}

/**
 * @typedef {Object} ForgetableStore
 * @property {(tenantId: string | null, userId: string, cascade?: any) => Promise<number | Record<string, number>> | number | Record<string, number>} purgeUser
 */

/**
 * @typedef {Object} ForgetOwnerSuccession
 * @property {string} topic the WIRE data topic (tenant prefix included)
 * @property {string | null} owner the successor, or null when the room emptied
 * @property {string} reason 'succeeded' | 'vacated'
 */

/**
 * Compose wired stores into one `{ purgeUser }`. Accepts a named map
 * (`{ registry, idempotency }`) for a labelled breakdown, or an array (labelled
 * `store0`, `store1`, ...). Entries without a `purgeUser` are skipped, so it is
 * safe to pass a store mix that predates forget support.
 *
 * `options.redis` opts the purge into the cluster-wide room-owner eviction and
 * presence-roster erasure (see the module doc) and switches the resolved shape
 * to the owner-succession envelope `{ rowsAffected, ownerSuccessions }`. The
 * eviction counts land in `rowsAffected` under `roomOwners` and
 * `presenceRoster` (summed into a composed store's count if one shares the
 * name). Accepts the raw ioredis instance or a client wrapper exposing one on
 * `.redis`.
 *
 * @param {Record<string, ForgetableStore | null | undefined> | Array<ForgetableStore | null | undefined>} stores
 * @param {{ redis?: any }} [options]
 * @returns {{ purgeUser: (tenantId: string | null, userId: string, cascade?: any) => Promise<Record<string, number> | { rowsAffected: Record<string, number>, ownerSuccessions: ForgetOwnerSuccession[] }> }}
 */
export function createForgetStore(stores, options) {
	if (!stores || typeof stores !== 'object') {
		throw new Error('createForgetStore: pass a map or array of stores');
	}
	const raw = Array.isArray(stores)
		? stores.map((s, i) => [`store${i}`, s])
		: Object.entries(stores);
	const entries = raw.filter(([, s]) => s && typeof (/** @type {any} */ (s).purgeUser) === 'function');
	let redis = (options && options.redis) || null;
	// Accept a client wrapper: unwrap to the raw ioredis it holds.
	if (redis && typeof redis.scan !== 'function' && redis.redis && typeof redis.redis.scan === 'function') {
		redis = redis.redis;
	}
	if (redis && typeof redis.scan !== 'function') {
		throw new Error('createForgetStore: options.redis must be an ioredis instance (or a client wrapping one on .redis)');
	}

	return {
		async purgeUser(tenantId, userId, cascade) {
			if (typeof userId !== 'string' || userId.length === 0) return {};
			const jobs = entries.map(([name, s]) =>
				Promise.resolve(/** @type {any} */ (s).purgeUser(tenantId, userId, cascade)).then((n) => ({ name, n }))
			);
			if (redis) {
				const scope = tenantId == null ? null : tenantId;
				// The two cluster legs never reject the whole job: each surfaces
				// its per-room/-roster failures in `legFailures` so a room that
				// already committed still contributes its succession.
				jobs.push(evictOwnerUser(redis, scope, userId).then((r) => ({ name: 'roomOwners', n: r.touched, successions: r.successions, legFailures: r.failures })));
				jobs.push(purgePresenceRoster(redis, scope, userId).then((r) => ({ name: 'presenceRoster', n: r.removed, legFailures: r.failures })));
			}
			const settled = await Promise.allSettled(jobs);
			/** @type {Record<string, number>} */
			const counts = {};
			/** @type {ForgetOwnerSuccession[]} */
			const ownerSuccessions = [];
			const failures = [];
			// Failed PURGE UNITS (a store, or one cluster leg with any per-room
			// failure), for the "N of M" message; distinct from `failures`, which
			// lists every underlying reason (a leg can carry several).
			let failedUnits = 0;
			for (const r of settled) {
				if (r.status === 'fulfilled') {
					const { name, n } = r.value;
					// A store may report a number or a nested breakdown; flatten to
					// a total so the realtime layer can sum the whole result. Sum
					// into an existing label so a composed store sharing a built-in
					// leg's name still contributes rather than being overwritten.
					const total = typeof n === 'number' && n > 0 ? n
						: (n && typeof n === 'object' ? Object.values(n).reduce((a, v) => a + (typeof v === 'number' && v > 0 ? v : 0), 0) : 0);
					counts[name] = (counts[name] || 0) + total;
					if (/** @type {any} */ (r.value).successions) {
						ownerSuccessions.push(.../** @type {any} */ (r.value).successions);
					}
					const legFailures = /** @type {any} */ (r.value).legFailures;
					if (legFailures && legFailures.length) {
						failures.push(...legFailures);
						failedUnits++;
					}
				} else {
					failures.push(r.reason);
					failedUnits++;
				}
			}
			if (failures.length) {
				const err = new Error(
					'createForgetStore: ' + failedUnits + ' of ' + (entries.length + (redis ? 2 : 0)) +
					' store(s) failed to purge; the erasure is incomplete and must be retried'
				);
				/** @type {any} */ (err).failures = failures;
				/** @type {any} */ (err).partialCounts = counts;
				// Successions the eviction ALREADY committed before a sibling room
				// failed: the durable state is authoritative for them, so the
				// realtime layer announces them on the `:owner` stream even though
				// the overall erasure is incomplete and will be retried. The retry
				// re-drives only the failed rooms, so each commits its wire
				// announcement exactly once.
				/** @type {any} */ (err).ownerSuccessions = ownerSuccessions;
				throw err;
			}
			return redis ? { rowsAffected: counts, ownerSuccessions } : counts;
		}
	};
}
