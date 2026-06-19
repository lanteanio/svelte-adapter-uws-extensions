// Bounded, structure-only snapshot builder for the presence consistency
// auditor - the extensions mirror of the adapter's runtime/audit-snapshot.js.
// The window is round-robin: the auditor advances `offset` by the page size each
// tick and wraps at the reported `total`, so an instance tracking a million
// topics allocates a fixed-size slice per tick. We iterate the topic map ONCE
// with a skip-counter rather than materializing the whole population (a spread +
// slice would allocate every topic every tick and defeat the bound).
//
// Pure with respect to its arguments (reads no clock / RNG / module singleton),
// so a unit test drives it with plain Maps and the presence factory closes over
// the live structures at the call site. Structure-only: it carries per-topic
// distinct-key cardinalities and a metric-sanitized topic label, never a user
// key, a connection, or any presence data.

/**
 * Build a bounded snapshot of the round-robin window of locally-tracked presence
 * topics in the shape `checkRedisPresenceLocalIndex` reads.
 *
 * Compares the per-topic member-count map against the local reverse index (both
 * synchronously co-mutated), NOT the local data map (whose commit a join defers
 * past the count increment, so count > data is a legitimate transient).
 *
 * @param {object} args
 * @param {Map<string, Map<string, number>>} args.localCounts - per-topic local
 *   member-count map (topic -> userKey -> refcount); iterated for the window and
 *   read for `.size` as the per-topic distinct-key count and the population total.
 * @param {(topic: string) => number} args.topicKeyCount - the local index's O(1)
 *   per-topic distinct-key count (from `createLocalIndex`).
 * @param {(topic: string) => string} [args.mt] - metric-safe topic transform; the
 *   only topic representation that reaches the (logged) violation context.
 * @param {number} args.offset - window start (round-robin position).
 * @param {number} args.limit - window size (max topics this tick).
 * @returns {{ presenceTopics: Array<{ topic: string, countKeys: number, indexKeys: number }>, total: number }}
 */
export function buildPresenceAuditSnapshot(args) {
	const { localCounts, topicKeyCount, mt, offset, limit } = args;
	const total = localCounts.size;
	/** @type {Array<{ topic: string, countKeys: number, indexKeys: number }>} */
	const presenceTopics = [];
	let i = 0;
	for (const [topic, counts] of localCounts) {
		if (i < offset) { i++; continue; }
		if (presenceTopics.length >= limit) break;
		i++;
		presenceTopics.push({
			topic: mt ? mt(topic) : topic,
			countKeys: counts.size,
			indexKeys: topicKeyCount(topic)
		});
	}
	return { presenceTopics, total };
}
