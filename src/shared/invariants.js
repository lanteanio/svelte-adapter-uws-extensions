// Shared invariant predicates - the extensions mirror of the adapter's
// files/invariants.js. The canonical predicate set is triplicated the way
// runtime.js is, so a tightened invariant tightens both the per-worker auditor
// in the adapter AND the cluster-conscious paths in extensions at once, with
// the same `category` strings (the metric label / assert category) on both
// sides. Cluster-specific predicates (per-instance presence cardinality,
// replay cursor-vs-ring ordering) are layered on top in a later change once
// the redis state surfaces expose a snapshot.
//
// A snapshot is a plain, structure-only object. It carries NO payload bytes and
// NO user data; only the bookkeeping shapes an invariant needs. The canonical
// snapshot shape:
//
//   {
//     connections: [{ id, subscribed: string[], bookkeeping: string[] | null }],
//     topicCounts: { [topic]: number },   // subscribers per topic
//     totalSubscriptions: number          // running cap accountant
//   }
//
// Each predicate accepts that snapshot (or the subset it needs) and returns
// `null` when the invariant holds, or `{ category, context }` for the first
// violation. Dependency-free and reads no clock/RNG/timer, so it is safe to
// import anywhere and trivially deterministic.

/**
 * @typedef {{ id: unknown, subscribed: string[], bookkeeping: string[] | null }} ConnectionSnapshot
 * @typedef {{
 *   connections?: ConnectionSnapshot[],
 *   topicCounts?: Record<string, number>,
 *   totalSubscriptions?: number
 * }} StateSnapshot
 * @typedef {{ category: string, context: unknown } | null} Violation
 */

/**
 * Subscription-bookkeeping invariant: a connection's subscription set (the one
 * fan-out reads) must agree with its cap-counted bookkeeping set. A code path
 * that mutates one without the other (a missing subscribe, a dropped Set type)
 * trips it. Returns the first connection whose two sets disagree.
 *
 * @param {StateSnapshot} snap
 * @returns {Violation}
 */
export function checkSubscriptionBookkeeping(snap) {
	const connections = snap && snap.connections;
	if (!connections) return null;
	for (const conn of connections) {
		const bookkeeping = conn.bookkeeping;
		if (!Array.isArray(bookkeeping)) return { category: 'subs.shape', context: { ws: conn.id } };
		const subscribed = conn.subscribed || [];
		if (bookkeeping.length !== subscribed.length) {
			return {
				category: 'subs.bookkeeping',
				context: { ws: conn.id, bookkeeping: bookkeeping.length, subscribed: subscribed.length }
			};
		}
		const subscribedSet = new Set(subscribed);
		for (const t of bookkeeping) {
			if (!subscribedSet.has(t)) return { category: 'subs.bookkeeping.missing', context: { ws: conn.id, topic: t } };
		}
	}
	return null;
}

/**
 * Cap-accountant invariant: the running `totalSubscriptions` counter must never
 * go negative and must equal the sum of every connection's bookkeeping set.
 * A drift means an add/remove pair fell out of balance. Only evaluated when the
 * snapshot carries the counter and the per-connection sets.
 *
 * @param {StateSnapshot} snap
 * @returns {Violation}
 */
export function checkTotalSubscriptions(snap) {
	if (!snap || typeof snap.totalSubscriptions !== 'number') return null;
	if (snap.totalSubscriptions < 0) {
		return { category: 'subs.total-negative', context: { totalSubscriptions: snap.totalSubscriptions } };
	}
	const connections = snap.connections;
	if (!connections) return null;
	let summed = 0;
	for (const conn of connections) {
		if (Array.isArray(conn.bookkeeping)) summed += conn.bookkeeping.length;
	}
	if (summed !== snap.totalSubscriptions) {
		return {
			category: 'subs.total-mismatch',
			context: { totalSubscriptions: snap.totalSubscriptions, summed }
		};
	}
	return null;
}

/**
 * Topic-index invariant: every topic the index counts must have at least one
 * subscriber. A topic that lingers with a zero (or negative) count is a leaked
 * index entry - the unsubscribe/close path that should have evicted it did not.
 *
 * @param {StateSnapshot} snap
 * @returns {Violation}
 */
export function checkTopicsHaveSubscribers(snap) {
	const topicCounts = snap && snap.topicCounts;
	if (!topicCounts) return null;
	for (const topic of Object.keys(topicCounts)) {
		const count = topicCounts[topic];
		if (!(count > 0)) return { category: 'topic.zero-subscribers', context: { topic, count } };
	}
	return null;
}

/**
 * The default predicate set, in the order the auditor runs them. Cheapest and
 * most fundamental first so a structural break surfaces before the derived
 * accounting checks.
 *
 * @type {Array<(snap: StateSnapshot) => Violation>}
 */
export const defaultInvariants = [
	checkSubscriptionBookkeeping,
	checkTotalSubscriptions,
	checkTopicsHaveSubscribers
];

/**
 * Run a predicate list against a snapshot and collect every violation (one per
 * predicate at most). Pure: no dedup, no clock, no side effect.
 *
 * @param {StateSnapshot} snap
 * @param {Array<(snap: StateSnapshot) => Violation>} [predicates]
 * @returns {Array<{ category: string, context: unknown }>}
 */
export function runInvariants(snap, predicates = defaultInvariants) {
	const out = [];
	for (const predicate of predicates) {
		const v = predicate(snap);
		if (v) out.push(v);
	}
	return out;
}

// - Redis-backed self-consistency predicates ---------------------------------

/**
 * Replay seq-ordering invariant: an instance must never have delivered a topic
 * seq past the highest seq the shared ring has actually published. The resume
 * path serves a client every entry above its lastSeenSeq, so a delivered seq
 * that runs ahead of the ring head means the instance projected a position the
 * durable store cannot back - it would skip the ring's real tail on the next
 * resume. The shape is `{ topic, deliveredSeq, ringHeadSeq }` per topic the
 * instance observed; a `deliveredSeq` of 0 (never delivered) is always in range.
 *
 * The snapshot is built per instance from its OWN delivered frames (the max seq
 * its clients received on each replay channel) and the SHARED ring head read
 * from the store, so the comparison crosses the local view against the shared
 * authority - it is not a same-source tautology.
 *
 * @param {{ replaySeqs?: Array<{ topic: string, deliveredSeq: number, ringHeadSeq: number }> }} snap
 * @returns {Violation}
 */
export function checkRedisReplaySeqRegression(snap) {
	const rows = snap && snap.replaySeqs;
	if (!Array.isArray(rows)) return null;
	for (const row of rows) {
		if (!row) continue;
		const delivered = row.deliveredSeq;
		const head = row.ringHeadSeq;
		if (typeof delivered !== 'number' || typeof head !== 'number') continue;
		if (delivered > head) {
			return {
				category: 'redis.replay.seq-regression',
				context: { topic: row.topic, deliveredSeq: delivered, ringHeadSeq: head }
			};
		}
	}
	return null;
}

// - Structural state hash ----------------------------------------------------

// FNV-1a 32-bit string fold. Module-private and deliberately self-contained (the
// few lines are trivial) so this file keeps its dependency-free,
// safe-to-import-anywhere posture. Fully deterministic - charCodeAt + Math.imul
// over a fixed string, no clock/RNG/locale input.
/** @param {number} h @param {string} str @returns {number} */
function fnvStr(h, str) {
	for (let i = 0; i < str.length; i++) {
		h ^= str.charCodeAt(i);
		h = Math.imul(h, 16777619);
	}
	return h >>> 0;
}

const FNV_OFFSET = 2166136261 >>> 0;

/**
 * Fold a structure-only state projection into a single unsigned 32-bit integer
 * that is stable across runs and processes and order-independent over its input.
 * The algorithm is identical to the adapter's `computeStateHash` so a projection
 * built on either side hashes the same way; the canonical definition is
 * triplicated here the way the predicates are, keeping this module dependency-free.
 *
 * INPUT CONTRACT: `{ topicSeqs }` where `topicSeqs` is a `Record<string, number>`
 * mapping a topic string to the highest non-negative seq that topic was observed
 * at. It is a PLAIN, already-extracted object; the caller does the extraction.
 * Any other property on the input object is ignored, so two inputs that agree on
 * `topicSeqs` hash identically regardless of what else they carry.
 *
 * PRIVACY (structure only): the returned value carries no recoverable
 * identifiers. Topic strings are folded into the hash but never appear verbatim
 * in the integer; nothing else is read. No payload bytes, no event names, no
 * data values, no presence data, no connection/user keys contribute.
 *
 * ORDERING: insertion order must not change the result. Each `[topic, seq]` entry
 * is reduced to a per-entry FNV digest folding the topic STRING and the integer
 * seq (rendered with `String(seq)`, with a `:` separator so a topic/seq boundary
 * cannot collide). The per-entry digests are combined with unsigned 32-bit
 * modular addition, which is commutative and associative, so any iteration order
 * yields the same accumulator. The accumulator starts from a count-seeded base so
 * a state with the same digests but a different number of topics cannot collide.
 *
 * This is a structural divergence DETECTOR, not a cryptographic commitment: a
 * 32-bit fold has a birthday bound, but a real divergence almost always moves a
 * seq integer, which moves that entry's digest.
 *
 * @param {{ topicSeqs?: Record<string, number> }} projection
 * @returns {number} unsigned 32-bit hash
 */
export function computeStateHash(projection) {
	const topicSeqs = (projection && projection.topicSeqs) || {};
	const topics = Object.keys(topicSeqs);
	let acc = fnvStr(FNV_OFFSET, 't:' + topics.length);
	for (const topic of topics) {
		let e = fnvStr(FNV_OFFSET, topic);
		e = fnvStr(e, ':' + String(topicSeqs[topic]));
		acc = (acc + e) >>> 0;
	}
	return acc >>> 0;
}

/**
 * Cross-instance shared-store convergence check. Every instance that subscribes
 * to a topic backed by the shared store (the pub/sub relay, the durable replay
 * ring, the LISTEN/NOTIFY bridge) should end with the SAME delivered-seq run for
 * that topic, because the store is the single authority the relay replicates
 * from. So a compact per-topic max-seq projection of two such instances should
 * hash identically. An instance whose projection hash differs received a
 * different seq run - its local index/cache drifted from the shared store, or
 * the relay dropped, duplicated, or misordered one instance's stream below the
 * others.
 *
 * It reads the seq each instance ACTUALLY DELIVERED to its own clients, not the
 * shared store directly: reading the shared object from every instance would be
 * a tautology (one object, trivially equal). Reading the independently-delivered
 * per-instance streams is what makes the comparison meaningful - a relay that
 * shorts one instance moves only that instance's projection.
 *
 * Each instance projects `topicSeqs[topic] = max(seq)` over its clients' delivered
 * frames, grouped on the routing topic. Only instances with a non-empty projection
 * participate, and they are bucketed by their exact topic set, so a publisher-only
 * instance (or one a total drop fault shut out entirely) with no delivered run is
 * never compared against subscribers - a legitimate per-instance difference, not a
 * divergence.
 *
 * Within a bucket of instances sharing the same topic set the per-instance hashes
 * are grouped by value; a bucket with more than one distinct hash is a divergence.
 * The canonical group order puts the largest group first (the convergent majority)
 * and, on a size tie, sorts the group holding the numerically-largest instance id
 * last, so the reported offender (the last group) is deterministic. The violation
 * context lists the bucket topic set verbatim for diagnosability - diagnostic data,
 * not the privacy-bearing hash. Returns the first divergence or null.
 *
 * @param {Array<{ id: number, topicSeqs: Record<string, number> }>} instances
 *   each instance's already-extracted per-topic delivered max-seq projection
 * @returns {{ category: string, context: any } | null}
 */
export function checkSharedStoreConvergence(instances) {
	/** @type {Array<{ id: number, topics: string[], hash: number }>} */
	const projected = [];
	for (const inst of instances || []) {
		const topicSeqs = (inst && inst.topicSeqs) || {};
		const topics = Object.keys(topicSeqs).sort();
		if (topics.length === 0) continue; // no delivered run: this instance does not participate
		const sorted = {};
		for (const t of topics) sorted[t] = topicSeqs[t];
		projected.push({ id: inst.id, topics, hash: computeStateHash({ topicSeqs: sorted }) });
	}

	// Bucket participating instances by their exact topic set, then look for a
	// bucket carrying more than one distinct hash. The bucket key is the JSON of
	// the sorted topic list (unambiguous - no delimiter a topic could contain),
	// and the list is carried alongside so the violation context uses it directly.
	/** @type {Map<string, { topics: string[], members: Array<{ id: number, hash: number }> }>} */
	const buckets = new Map();
	for (const p of projected) {
		const key = JSON.stringify(p.topics);
		let bucket = buckets.get(key);
		if (!bucket) { bucket = { topics: p.topics, members: [] }; buckets.set(key, bucket); }
		bucket.members.push({ id: p.id, hash: p.hash });
	}
	for (const { topics, members } of buckets.values()) {
		/** @type {Map<number, number[]>} hash -> instance ids */
		const byHash = new Map();
		for (const m of members) {
			let ids = byHash.get(m.hash);
			if (!ids) { ids = []; byHash.set(m.hash, ids); }
			ids.push(m.id);
		}
		if (byHash.size <= 1) continue; // converged within this bucket

		const groups = [...byHash].map(([hash, ids]) => {
			const sorted = ids.slice().sort((a, b) => a - b);
			return { hash, ids: sorted, max: sorted[sorted.length - 1] };
		});
		groups.sort((a, b) => (b.ids.length - a.ids.length) || (a.max - b.max));
		const majority = groups[0];
		const minority = groups[groups.length - 1];
		return {
			category: 'cluster.state-divergence',
			context: {
				topics,
				expectedHash: majority.hash,
				divergentHash: minority.hash,
				instances: minority.ids
			}
		};
	}
	return null;
}
