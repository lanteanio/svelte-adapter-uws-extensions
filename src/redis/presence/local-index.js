/**
 * Local reverse index for the Redis-backed presence tracker.
 *
 * Maps `topic + '|' + userKey` to the set of ws connections tracking that
 * (topic, key) on this instance, so the leave path can find another live
 * connection for the same user without scanning every ws. Purely local
 * bookkeeping - no Redis, no slot concerns. findOtherWsData reads the shared
 * wsTopics map to recover the surviving connection's data.
 *
 * @module svelte-adapter-uws-extensions/redis/presence/local-index
 */

/**
 * Create the per-instance (topic, key) -> ws reverse index.
 *
 * @param {Map<any, Map<string, { key: string, data: Record<string, any> }>>} wsTopics
 *   The shared per-connection topic map, read by findOtherWsData to recover a
 *   surviving connection's data. Passed by reference; mutated elsewhere.
 * @returns {{
 *   indexAdd: (topic: string, userKey: string, ws: any) => void,
 *   indexRemove: (topic: string, userKey: string, ws: any) => void,
 *   findOtherWsData: (topic: string, userKey: string, exceptWs: any) => (Record<string, any> | null),
 *   topicKeyCount: (topic: string) => number,
 *   clear: () => void
 * }}
 */
export function createLocalIndex(wsTopics) {
	/**
	 * Reverse index from `topic + '|' + userKey` to the set of ws connections
	 * tracking that (topic, key) on this instance. Mirrors `wsTopics` so the
	 * leave path can find another live connection for the same user without
	 * scanning every ws on the instance.
	 * @type {Map<string, Set<any>>}
	 */
	const topicKeyToWs = new Map();

	/**
	 * Per-topic count of distinct user keys currently in the index (the number of
	 * `topic + '|' + userKey` entries for that topic). Maintained alongside
	 * `topicKeyToWs` so the consistency auditor reads it in O(1) per topic without
	 * scanning. It moves in lockstep with the presence tracker's per-topic
	 * member-count map: both are updated in the same synchronous frame on join,
	 * leave, and rollback, so the two cardinalities must always agree.
	 * @type {Map<string, number>}
	 */
	const topicKeyCounts = new Map();

	function indexAdd(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		let set = topicKeyToWs.get(k);
		if (!set) {
			set = new Set();
			topicKeyToWs.set(k, set);
			topicKeyCounts.set(topic, (topicKeyCounts.get(topic) || 0) + 1);
		}
		set.add(ws);
	}

	function indexRemove(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		const set = topicKeyToWs.get(k);
		if (!set) return;
		set.delete(ws);
		if (set.size === 0) {
			topicKeyToWs.delete(k);
			const n = (topicKeyCounts.get(topic) || 0) - 1;
			if (n > 0) topicKeyCounts.set(topic, n);
			else topicKeyCounts.delete(topic);
		}
	}

	function findOtherWsData(topic, userKey, exceptWs) {
		const set = topicKeyToWs.get(topic + '|' + userKey);
		if (!set) return null;
		let newest = null;
		for (const ws of set) {
			if (ws === exceptWs) continue;
			const entry = wsTopics.get(ws)?.get(topic);
			if (entry && entry.key === userKey) newest = entry.data;
		}
		return newest;
	}

	/**
	 * Distinct local user keys currently indexed for a topic (O(1)). The presence
	 * consistency auditor compares this against the per-topic member-count map.
	 * @param {string} topic
	 * @returns {number}
	 */
	function topicKeyCount(topic) {
		return topicKeyCounts.get(topic) || 0;
	}

	/**
	 * Drop the entire index. The presence tracker's `clear()` resets its
	 * member-count and data maps in one shot; the index MUST be reset alongside
	 * them or `topicKeyCount` would keep reporting stale per-topic keys after a
	 * clear (and the consistency auditor would then see a phantom divergence).
	 */
	function clear() {
		topicKeyToWs.clear();
		topicKeyCounts.clear();
	}

	return { indexAdd, indexRemove, findOtherWsData, topicKeyCount, clear };
}
