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
 *   findOtherWsData: (topic: string, userKey: string, exceptWs: any) => (Record<string, any> | null)
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

	function indexAdd(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		let set = topicKeyToWs.get(k);
		if (!set) {
			set = new Set();
			topicKeyToWs.set(k, set);
		}
		set.add(ws);
	}

	function indexRemove(topic, userKey, ws) {
		const k = topic + '|' + userKey;
		const set = topicKeyToWs.get(k);
		if (!set) return;
		set.delete(ws);
		if (set.size === 0) topicKeyToWs.delete(k);
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

	return { indexAdd, indexRemove, findOtherWsData };
}
