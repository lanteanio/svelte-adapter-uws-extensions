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
 * @returns {Record<string, any>} target
 */
export function mergeFields(target, src) {
	if (!src || typeof src !== 'object') return target;
	for (const k of Object.keys(src)) {
		if (isReservedPresenceField(k)) continue;
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
