/**
 * Stateless data transforms for the Redis-backed presence tracker: the
 * public-view projection, the local-cache identity setter, the deep-equality
 * guard, and the HGETALL parser. No Redis, no timers, no shared state - pure
 * functions over their arguments (makePublicData curries the transient-field
 * set so the projection stays a closure over config rather than module state).
 *
 * @module svelte-adapter-uws-extensions/redis/presence/data
 */

/**
 * Build the public-view projection bound to a set of transient field names.
 *
 * @param {Set<string>} transientFields - Field names broadcast live but never
 *   persisted nor included in snapshots/rosters.
 * @returns {(entry: { data: Record<string, any>, fields?: Record<string, any> | null }) => Record<string, any>}
 */
export function makePublicData(transientFields) {
	/**
	 * The public presence value for a user: identity `data` merged with the
	 * user's durable dynamic `fields`, transient fields stripped. Used by every
	 * snapshot-shaped path (`state` reconstruction from Redis, the heartbeat
	 * roster, the join roster at flush) so a (re)joiner never sees a transient
	 * value. The no-`fields` user (the overwhelming common case) returns
	 * `entry.data` with zero copy - keeping a no-`update()` deployment's wire
	 * byte-identical to a deployment that never calls update(). Works on both a local cache entry
	 * (`{ data, fields }`) and a parsed Redis entry (`{ data, fields, ts }`);
	 * Redis only ever stores durable fields, so the transient strip is a no-op
	 * there but harmless.
	 * @param {{ data: Record<string, any>, fields?: Record<string, any> | null }} entry
	 * @returns {Record<string, any>}
	 */
	return function publicData(entry) {
		if (!entry.fields) return entry.data;
		const out = { ...entry.data };
		for (const k of Object.keys(entry.fields)) {
			if (!transientFields.has(k)) out[k] = entry.fields[k];
		}
		return out;
	};
}

/**
 * Set a user's identity `data` on the local per-topic cache, PRESERVING any
 * dynamic `fields` already tracked for the user. The identity-churn paths
 * (join, data-change, leave-restore, rollback) re-set `data` repeatedly;
 * dynamic fields are user-level and orthogonal, so they must ride across
 * those re-sets rather than be clobbered.
 * @param {Map<string, { data: Record<string, any>, fields: Record<string, any> | null }>} topicData
 * @param {string} key
 * @param {Record<string, any>} data
 */
export function setLocalData(topicData, key, data) {
	const existing = topicData.get(key);
	topicData.set(key, existing ? { data, fields: existing.fields } : { data, fields: null });
}

/**
 * Shallow-then-deep equality check for presence data objects.
 * Avoids redundant Redis writes and broadcasts when a user's
 * data has not actually changed.
 */
export function deepEqual(a, b) {
	if (a === b) return true;
	if (a == null || b == null) return a === b;
	if (typeof a !== 'object' || typeof b !== 'object') return false;
	const keysA = Object.keys(a);
	const keysB = Object.keys(b);
	if (keysA.length !== keysB.length) return false;
	for (let i = 0; i < keysA.length; i++) {
		const k = keysA[i];
		if (!deepEqual(a[k], b[k])) return false;
	}
	return true;
}

/**
 * Parse the per-topic hash HGETALL result into a Map<userKey, {data, ts}>.
 * Staleness filtering and cross-instance deduplication are no longer
 * needed: the new storage layout uses one field per userKey (Redis
 * collapses cross-instance writes via HSET on the same field), and per-
 * field HPEXPIRE removes stale entries before HGETALL sees them. So this
 * is now just a JSON-parse + drop-corrupted loop.
 */
export function parseEntries(all) {
	const seen = new Map();
	for (const userKey of Object.keys(all)) {
		try {
			seen.set(userKey, JSON.parse(all[userKey]));
		} catch { /* corrupted entry */ }
	}
	return seen;
}
