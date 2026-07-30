/**
 * Bounded least-recently-used map over the insertion order a `Map`
 * already maintains.
 *
 * The subtlety this exists to get right: `Map.set` on a key that is
 * already present updates the value but does NOT move the key, so
 * `map.keys().next().value` is the first-INSERTED key, never the
 * least-recently-USED one. A cache that evicts on that order under a
 * spray of fresh keys throws out its hottest entries first - the ones
 * inserted at startup - and keeps the spray. Re-inserting on every touch
 * is what makes the order mean what the eviction assumes.
 *
 * @module svelte-adapter-uws-extensions/shared/lru-map
 */

/**
 * @template V
 * @param {number} maxEntries
 */
export function createLruMap(maxEntries) {
	if (!Number.isInteger(maxEntries) || maxEntries < 1) {
		throw new Error('lru-map: maxEntries must be a positive integer');
	}
	/** @type {Map<string, V>} */
	const map = new Map();

	return {
		/** @param {string} key */
		get(key) {
			if (!map.has(key)) return undefined;
			const value = /** @type {V} */ (map.get(key));
			// Touch: delete + set moves the key to the most-recent end.
			map.delete(key);
			map.set(key, value);
			return value;
		},

		/** @param {string} key */
		has(key) {
			return map.has(key);
		},

		/**
		 * @param {string} key
		 * @param {V} value
		 */
		set(key, value) {
			if (map.has(key)) map.delete(key);
			else if (map.size >= maxEntries) map.delete(/** @type {string} */ (map.keys().next().value));
			map.set(key, value);
		},

		/** @param {string} key */
		delete(key) {
			return map.delete(key);
		},

		clear() {
			map.clear();
		},

		get size() {
			return map.size;
		}
	};
}
