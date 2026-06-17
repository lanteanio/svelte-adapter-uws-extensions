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
