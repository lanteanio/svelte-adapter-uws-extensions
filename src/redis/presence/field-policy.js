/**
 * One rule for which presence field names are accepted, applied at every
 * ingress into a live entry's `fields` map: the local `presence-update`
 * wire frame, the cross-instance FIELDS bus envelope, and the durable
 * fields read back out of Redis.
 *
 * The rule matters twice over. `__proto__` assigned into a plain object
 * invokes the prototype setter instead of storing data, which silently
 * corrupts per-field change detection: a later legitimate update whose
 * value equals the attacker-planted prototype value compares equal and is
 * never relayed. And a field name accepted here rides `publicData()` into
 * every roster, diff and heartbeat frame the topic emits, so an ingress
 * that accepts what its siblings reject is a phantom-field injector.
 *
 * The predicate mirrors the dangerous-key half of the adapter's bundled
 * presence plugin (`isReservedField`): the `__` namespace is reserved
 * package-wide, and `constructor` / `prototype` go with it. Containers are
 * null-prototype as well, so the guard is defense in depth rather than the
 * only thing standing between a crafted key and the prototype chain.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/field-policy
 */

import { isUnsafeProjectionFieldName } from '../../shared/sensitive-names.js';

/**
 * True when a client- or bus-supplied field name must not enter a live
 * presence entry.
 * @param {string} k
 * @returns {boolean}
 */
export function isReservedPresenceField(k) {
	// charCodeAt rather than startsWith: this runs per field per user per
	// roster frame, and startsWith allocates its way to a measurable share of
	// publicData at the common 1-8 field shapes. Same test the bus validator
	// already uses for the same prefix.
	return (k.charCodeAt(0) === 95 && k.charCodeAt(1) === 95) || k === 'constructor' || k === 'prototype';
}

/**
 * Copy `src`'s accepted own keys onto `target`. `defineProperty` rather
 * than assignment so a key that slips the predicate still cannot reach a
 * setter on the target or its prototype chain.
 * @param {Record<string, any>} target
 * @param {Record<string, any> | null | undefined} src
 * @param {(k: string) => boolean} [accepts] - a name rule on top of the
 *   reserved names, such as a tracker's `fromServer` field rule
 * @returns {Record<string, any>} target
 */
export function mergeFields(target, src, accepts) {
	if (!src || typeof src !== 'object') return target;
	for (const k of Object.keys(src)) {
		if (isReservedPresenceField(k) || (accepts !== undefined && !accepts(k))) continue;
		Object.defineProperty(target, k, { value: src[k], writable: true, enumerable: true, configurable: true });
	}
	return target;
}

/**
 * A fresh field container. Null-prototype so no key name can reach
 * `Object.prototype`, matching what the adapter's presence plugin does
 * with the same client-controlled key space.
 * @returns {Record<string, any>}
 */
export function newFieldMap() {
	return Object.create(null);
}

/**
 * Whether an object has an own name the field rule refuses. `for...in`
 * rather than Object.keys: no array per entry, and an object from JSON.parse
 * has no inherited enumerable names.
 * @param {Record<string, any>} obj
 */
function hasReservedField(obj) {
	for (const k in obj) {
		if (isReservedPresenceField(k)) return true;
	}
	return false;
}

/**
 * A user's identity `data` without the names the field rule refuses. The
 * local join and the read-back from Redis call this, and the relayed join
 * merges its data through the same rule, so a peer sees the same entry
 * whichever instance it is connected to.
 *
 * The object is returned as-is when it has no such name, which is the case
 * for every ordinary roster entry; rebuilding costs an allocation per entry
 * on every join and sync snapshot.
 * @param {Record<string, any>} data
 * @returns {Record<string, any>}
 */
export function withoutReservedNames(data) {
	return hasReservedField(data) ? mergeFields(newFieldMap(), data) : data;
}

/** `user_id`, `userId` and `USER-ID` all fold to `userid`. */
function foldName(k) {
	return k.toLowerCase().replace(/[^a-z0-9]+/g, '');
}

/**
 * The dynamic field names one tracker accepts, by the in-memory plugin's rule.
 *
 * `fromClient` answers for a wire `presence-update` frame: nothing is
 * accepted unless the application listed the name in `clientUpdateFields`.
 *
 * `fromServer` answers for a direct `update()` call. It is also the rule for
 * the two other ways a dynamic field reaches an entry, a peer instance's
 * relayed update and the fields read back from Redis, because a legitimate one
 * of those came from an `update()` on an instance with the same options. With
 * an allowlist it is the allowlist. Without one it refuses the identity the
 * select produced (the key field, `id`, `userId`, `role`, `roles`,
 * `userRole`, compared with case and separators folded away, so `ID` and
 * `user_id` cannot be planted beside the real one) and every credential-shaped,
 * personal-data or transport name.
 *
 * Neither ever accepts a name {@link isReservedPresenceField} refuses: they are
 * taken out of the allowlist, and the guard refuses them itself. The adapter
 * takes only the three prototype names out of its allowlist; here the whole
 * `__` namespace goes, because the relay and the read-back drop it anyway and a
 * listed `__` name would reach this instance's peers and no other's.
 *
 * @param {string} keyField
 * @param {unknown[] | undefined} clientUpdateFields - an array, or undefined
 */
export function createFieldNameRule(keyField, clientUpdateFields) {
	const allowlist = Array.isArray(clientUpdateFields)
		? new Set(clientUpdateFields.filter((f) => typeof f === 'string' && !isReservedPresenceField(f)))
		: null;
	const identity = new Set([keyField, 'id', 'userId', 'role', 'roles', 'userRole'].map(foldName).filter(Boolean));
	/** @param {string} k */
	const isReserved = (k) => k === keyField || identity.has(foldName(k)) || isUnsafeProjectionFieldName(k);

	// Memoised. The read-back asks this for every field of every stored entry
	// on each join and sync snapshot, a roster repeats a handful of names, and
	// folding the name is most of the cost: unmemoised it made parsing a
	// 1000-entry roster with three fields each half again as slow. Bounded the
	// way the shared verdict cache is: evict one entry on a miss, never touch a
	// hit, and never store a name longer than the private-name rule reads, which
	// it refuses outright.
	/** @type {Map<string, boolean>} */
	const verdicts = new Map();
	/** @type {Iterator<string> | null} */
	let evictCursor = null;
	const evictOne = () => {
		for (let wrapped = 0; wrapped < 2; wrapped++) {
			if (evictCursor === null) evictCursor = verdicts.keys();
			const step = evictCursor.next();
			if (step.done) { evictCursor = null; continue; }
			verdicts.delete(step.value);
			return;
		}
	};
	/** @param {string} k */
	const serverAccepts = (k) => {
		const cached = verdicts.get(k);
		if (cached !== undefined) return cached;
		if (k.length > MAX_REMEMBERED_NAME_LENGTH) return false;
		const verdict = !isReserved(k);
		if (verdicts.size >= MAX_REMEMBERED_VERDICTS) evictOne();
		verdicts.set(k, verdict);
		return verdict;
	};

	return {
		listsClientFields: allowlist !== null,
		/** @param {string} k */
		fromClient: (k) => allowlist !== null && allowlist.has(k),
		/** @type {(k: string) => boolean} */
		fromServer: allowlist !== null ? (k) => allowlist.has(k) : serverAccepts
	};
}

/** Distinct field names a tracker remembers a verdict for. */
const MAX_REMEMBERED_VERDICTS = 1024;

/**
 * Longest name remembered. isUnsafeProjectionFieldName refuses any longer
 * name without reading it, so a longer one is refused here the same way.
 */
const MAX_REMEMBERED_NAME_LENGTH = 64;

/**
 * A once-per-name warning, outside production, for a server-side `update()`
 * whose field was refused because it is reserved identity. Only built when
 * `clientUpdateFields` is unset: with a list, a refused name is simply one the
 * application did not list, and the advice to replace the guard would not fit. Only the finite
 * identity names are reported: the credential-shaped rule matches an unbounded
 * space of names, and reporting those would let a caller drive an unbounded
 * log. Dedup is on the folded name, so `id`, `ID` and `i_d` report once.
 *
 * @param {string} keyField
 * @returns {((k: string) => void) | null}
 */
export function createReservedFieldWarner(keyField) {
	if (process.env.NODE_ENV === 'production') return null;
	const warnable = new Set([keyField, 'id', 'userId', 'role', 'roles', 'userRole'].map(foldName).filter(Boolean));
	const warned = new Set();
	return (k) => {
		const folded = foldName(k);
		if ((k !== keyField && !warnable.has(folded)) || warned.has(folded)) return;
		warned.add(folded);
		const name = k.slice(0, 64).replace(/[^\x20-\x7e]/g, '?');
		console.warn(
			`[extensions] redis presence.update(): field '${name}' is reserved and was dropped. ` +
			'Reserved names are the dedup key field, id, userId, role, roles, userRole, __-prefixed, ' +
			'constructor/prototype and credential-shaped names, so a client cannot overwrite the identity ' +
			`its peers see. If this call is server-owned and intentional, pass clientUpdateFields: [${JSON.stringify(name)}, ...] ` +
			'to replace the guard with an explicit allowlist; clients can then write every listed name too, so a ' +
			'server-owned value is better kept under an unreserved name of its own.'
		);
	};
}
