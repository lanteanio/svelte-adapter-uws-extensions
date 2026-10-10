/**
 * Stateless data transforms for the Redis-backed presence tracker: the
 * default identity selector, the public-view projection, the local-cache
 * identity setter, the deep-equality guard, and the HGETALL parser. No Redis,
 * no timers, no shared state - pure functions over their arguments
 * (makeDefaultPresenceSelect and makePublicData curry their config so each
 * stays a closure rather than module state).
 *
 * @module svelte-adapter-uws-extensions/redis/presence/data
 */

import { isReservedPresenceField, withoutReservedNames, mergeFields, newFieldMap } from './field-policy.js';
import { isSensitiveFieldName, isStructurallyUnsafeFieldName } from '../../shared/sensitive-names.js';

/**
 * Build the fail-closed default selector for one tracker. Only the configured
 * dedup key is copied, and only when the field name is safe and its value is a
 * stable JSON scalar (string or finite number). Every display/profile field
 * requires an explicit application allowlist.
 *
 * @param {string} keyField
 * @returns {(obj: unknown) => Record<string, string | number>}
 */
export function makeDefaultPresenceSelect(keyField) {
	return function defaultPresenceSelect(obj) {
		const selected = {};
		if (
			!obj ||
			typeof obj !== 'object' ||
			isSensitiveFieldName(keyField) ||
			isStructurallyUnsafeFieldName(keyField)
		) return selected;

		let value;
		try {
			if (!Object.prototype.hasOwnProperty.call(obj, keyField)) return selected;
			value = obj[keyField];
		} catch {
			return selected;
		}
		if (typeof value !== 'string' && !(typeof value === 'number' && Number.isFinite(value))) {
			return selected;
		}
		Object.defineProperty(selected, keyField, {
			value,
			enumerable: true,
			configurable: true,
			writable: true
		});
		return selected;
	};
}

/**
 * Build the public-view projection bound to a set of transient field names.
 *
 * @param {Set<string>} transientFields - Field names broadcast live but never
 *   persisted nor included in snapshots/rosters.
 * @returns {(entry: { data: Record<string, any>, fields?: Record<string, any> | null, legacyData?: Record<string, any> }) => Record<string, any>}
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
	 * @param {{ data: Record<string, any>, fields?: Record<string, any> | null, legacyData?: Record<string, any> }} entry
	 * @returns {Record<string, any>}
	 */
	return function publicData(entry) {
		const identity = entry.legacyData || entry.data;
		if (!entry.fields) return identity;
		const out = { ...identity };
		for (const k of Object.keys(entry.fields)) {
			// The read-back path is the third fields ingress and gets the same
			// rule as the other two. Durable fields come out of Redis, which a
			// pre-fix instance mid-rolling-upgrade (or anything else with write
			// access) may have stored a reserved name into. Assigned here they
			// would ride into every roster and heartbeat frame, and a
			// '__proto__' would land on `out`'s prototype rather than on `out`.
			if (isReservedPresenceField(k) || transientFields.has(k)) continue;
			out[k] = entry.fields[k];
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
 * @param {Map<string, { data: Record<string, any>, fields: Record<string, any> | null, remoteIdentity?: boolean, legacyData?: Record<string, any>, serializedIdentity?: string }> topicData
 * @param {string} key
 * @param {Record<string, any>} data
 */
export function setLocalData(topicData, key, data) {
	const existing = topicData.get(key);
	const next = existing ? { data, fields: existing.fields } : { data, fields: null };
	if (existing?.remoteIdentity && deepEqual(existing.data, data)) {
		next.remoteIdentity = true;
		if (existing.legacyData) next.legacyData = existing.legacyData;
	}
	topicData.set(key, next);
}

/**
 * Depth bound for `deepEqual`. Past it, two values compare as UNEQUAL,
 * which costs a redundant write and broadcast but never a crash. The
 * comparison runs on raw client values straight off the `presence-update`
 * wire frame, where V8's iterative JSON parser will happily hand over a
 * 200k-deep document that unbounded recursion turns into a stack overflow
 * inside the message handler. Matches the cap the adapter's bundled
 * presence plugin puts on its own equivalent.
 */
export const DEEP_EQUAL_MAX_DEPTH = 256;

/**
 * Visit budget for `deepEqual`, on the same model and for the same reason as
 * `stripInternal`'s: depth alone does not bound the WORK.
 *
 * A comparison walks the ancestor path, so a shared subtree is re-walked once
 * per reference and `let n = {x:1}; for (let i=0;i<24;i++) n = {a:n, b:n}`
 * expands to 2^24 visits at depth 24 - a fourteenth of the depth cap, and 794ms
 * measured. Not reachable from a wire frame (a JSON payload cannot express a
 * shared reference, and nothing in `src/` parses one that can), but presence
 * data is also compared against app-constructed objects, which can.
 *
 * Past it two values compare as UNEQUAL, exactly like the depth cap: a
 * redundant write and broadcast, never a crash. A cycle is bounded by the
 * depth cap for the same reason and needs no separate ancestor set.
 */
export const DEEP_EQUAL_MAX_NODES = 100_000;

/**
 * Depth at which the budget starts being carried.
 *
 * Counting from the root cost 5-7% on ordinary presence shapes - one object
 * allocation per comparison, on a 50ns operation - which is not a price worth
 * paying on the update path. It buys nothing either: the blow-up is
 * EXPONENTIAL in depth, so everything above this threshold is worth at most
 * 2^8 visits however it is shaped. Below the threshold the walk is free;
 * at and past it, every visit is charged.
 */
const BUDGET_FROM_DEPTH = 8;

/**
 * Shallow-then-deep equality check for presence data objects.
 * Avoids redundant Redis writes and broadcasts when a user's
 * data has not actually changed.
 * @param {any} a
 * @param {any} b
 * @param {number} [depth]
 * @param {{ n: number }} [budget] - Shared visit counter for one comparison.
 */
export function deepEqual(a, b, depth, budget) {
	if (a === b) return true;
	if (a == null || b == null) return a === b;
	if (typeof a !== 'object' || typeof b !== 'object') return false;
	if (depth === undefined) depth = 0;
	if (depth >= DEEP_EQUAL_MAX_DEPTH) return false;
	const keysA = Object.keys(a);
	const keysB = Object.keys(b);
	if (keysA.length !== keysB.length) return false;
	// Charging each container its width mirrors `stripInternal`: every value
	// the walk can reach occupies a slot in some container, so the sum of
	// widths bounds the visits. A presence object never gets near
	// BUDGET_FROM_DEPTH, so the hot path allocates nothing and adds one
	// integer compare it was already making room for.
	if (depth >= BUDGET_FROM_DEPTH) {
		if (budget === undefined) budget = { n: 0 };
		budget.n += keysA.length;
		if (budget.n > DEEP_EQUAL_MAX_NODES) return false;
	}
	for (let i = 0; i < keysA.length; i++) {
		const k = keysA[i];
		if (!deepEqual(a[k], b[k], depth + 1, budget)) return false;
	}
	return true;
}

/**
 * Stored dynamic fields minus the names the tracker's `update()` rule refuses.
 * A value that is not a plain object is no fields at all: `publicData` reads
 * `Object.keys`, which turns a stored string or array into index-named fields.
 * Returned as-is when every name is accepted, the common case.
 * @param {unknown} fields
 * @param {(k: string) => boolean} acceptsField
 * @returns {Record<string, any> | null | undefined}
 */
function acceptedFields(fields, acceptsField) {
	if (fields === undefined || fields === null) return fields;
	if (typeof fields !== 'object' || Array.isArray(fields)) return null;
	for (const k in fields) {
		if (!acceptsField(k)) return mergeFields(newFieldMap(), fields, acceptsField);
	}
	return /** @type {Record<string, any>} */ (fields);
}

/**
 * Parse the per-topic hash HGETALL result into a Map<userKey, {data, ts}>.
 * Staleness filtering and cross-instance deduplication are no longer
 * needed: the new storage layout uses one field per userKey (Redis
 * collapses cross-instance writes via HSET on the same field), and per-
 * field HPEXPIRE removes stale entries before HGETALL sees them. So this
 * is now just a JSON-parse + drop-corrupted loop, plus the field rule on each
 * entry's `data` that the local join applies, and the tracker's `update()`
 * rule on its `fields`: anything with write access to the hash can store
 * names neither path writes.
 * @param {Record<string, string>} all
 * @param {(k: string) => boolean} acceptsField - the tracker's `fromServer` field rule
 */
export function parseEntries(all, acceptsField) {
	const seen = new Map();
	for (const userKey of Object.keys(all)) {
		let entry;
		try {
			entry = JSON.parse(all[userKey]);
		} catch { continue; /* corrupted entry */ }
		// A stored value that is not an entry with object data (`null`, a
		// number, an object without `data`) is corrupt too: every consumer
		// reads entry.data as the user's identity.
		if (entry === null || entry.data === null || typeof entry.data !== 'object') continue;
		entry.data = withoutReservedNames(entry.data);
		entry.fields = acceptedFields(entry.fields, acceptsField);
		seen.set(userKey, entry);
	}
	return seen;
}
