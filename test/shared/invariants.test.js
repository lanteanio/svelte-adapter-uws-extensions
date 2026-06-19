import { describe, it, expect } from 'vitest';
import {
	checkSubscriptionBookkeeping,
	checkTotalSubscriptions,
	checkTopicsHaveSubscribers,
	checkRedisReplaySeqRegression,
	checkRedisPresenceLocalIndex,
	checkSharedStoreConvergence,
	computeStateHash,
	runInvariants,
	defaultInvariants
} from '../../src/shared/invariants.js';

describe('checkSubscriptionBookkeeping', () => {
	it('returns null when every connection agrees', () => {
		const snap = {
			connections: [
				{ id: 1, subscribed: ['a', 'b'], bookkeeping: ['a', 'b'] },
				{ id: 2, subscribed: [], bookkeeping: [] }
			]
		};
		expect(checkSubscriptionBookkeeping(snap)).toBeNull();
	});

	it('flags subs.shape when bookkeeping is not an array', () => {
		expect(checkSubscriptionBookkeeping({ connections: [{ id: 7, subscribed: ['a'], bookkeeping: null }] }))
			.toEqual({ category: 'subs.shape', context: { ws: 7 } });
	});

	it('flags subs.bookkeeping on a size mismatch', () => {
		expect(checkSubscriptionBookkeeping({ connections: [{ id: 3, subscribed: ['a', 'b'], bookkeeping: ['a'] }] }))
			.toEqual({ category: 'subs.bookkeeping', context: { ws: 3, bookkeeping: 1, subscribed: 2 } });
	});

	it('flags subs.bookkeeping.missing when a bookkeeping topic is absent from subscribed', () => {
		expect(checkSubscriptionBookkeeping({ connections: [{ id: 9, subscribed: ['a', 'x'], bookkeeping: ['a', 'b'] }] }))
			.toEqual({ category: 'subs.bookkeeping.missing', context: { ws: 9, topic: 'b' } });
	});
});

describe('checkTotalSubscriptions', () => {
	it('flags a negative total', () => {
		expect(checkTotalSubscriptions({ totalSubscriptions: -1 }))
			.toEqual({ category: 'subs.total-negative', context: { totalSubscriptions: -1 } });
	});

	it('flags a total that disagrees with the summed bookkeeping', () => {
		const snap = {
			totalSubscriptions: 5,
			connections: [
				{ id: 1, subscribed: ['a'], bookkeeping: ['a'] },
				{ id: 2, subscribed: ['b', 'c'], bookkeeping: ['b', 'c'] }
			]
		};
		expect(checkTotalSubscriptions(snap)).toEqual({
			category: 'subs.total-mismatch',
			context: { totalSubscriptions: 5, summed: 3 }
		});
	});

	it('passes when the total equals the summed bookkeeping', () => {
		const snap = {
			totalSubscriptions: 3,
			connections: [
				{ id: 1, subscribed: ['a'], bookkeeping: ['a'] },
				{ id: 2, subscribed: ['b', 'c'], bookkeeping: ['b', 'c'] }
			]
		};
		expect(checkTotalSubscriptions(snap)).toBeNull();
	});
});

describe('checkTopicsHaveSubscribers', () => {
	it('passes when every topic has a positive count', () => {
		expect(checkTopicsHaveSubscribers({ topicCounts: { a: 1, b: 4 } })).toBeNull();
	});

	it('flags a leaked zero-count topic', () => {
		expect(checkTopicsHaveSubscribers({ topicCounts: { a: 1, b: 0 } }))
			.toEqual({ category: 'topic.zero-subscribers', context: { topic: 'b', count: 0 } });
	});
});

describe('runInvariants / defaultInvariants', () => {
	it('collects one violation per failing predicate', () => {
		const snap = {
			totalSubscriptions: -2,
			topicCounts: { a: 0 },
			connections: [{ id: 1, subscribed: ['a'], bookkeeping: null }]
		};
		const categories = runInvariants(snap).map((v) => v.category).sort();
		expect(categories).toEqual(['subs.shape', 'subs.total-negative', 'topic.zero-subscribers']);
	});

	it('is the documented ordered set', () => {
		expect(defaultInvariants).toEqual([
			checkSubscriptionBookkeeping,
			checkTotalSubscriptions,
			checkTopicsHaveSubscribers
		]);
	});
});

describe('checkRedisReplaySeqRegression', () => {
	it('returns null when every delivered seq is at or below the ring head', () => {
		const snap = { replaySeqs: [
			{ topic: 'a', deliveredSeq: 3, ringHeadSeq: 3 },
			{ topic: 'b', deliveredSeq: 1, ringHeadSeq: 5 }
		] };
		expect(checkRedisReplaySeqRegression(snap)).toBeNull();
	});

	it('treats a never-delivered (0) seq as in range', () => {
		expect(checkRedisReplaySeqRegression({ replaySeqs: [{ topic: 'a', deliveredSeq: 0, ringHeadSeq: 0 }] })).toBeNull();
	});

	it('flags the first topic whose delivered seq runs ahead of the ring head', () => {
		const snap = { replaySeqs: [
			{ topic: 'a', deliveredSeq: 2, ringHeadSeq: 2 },
			{ topic: 'b', deliveredSeq: 9, ringHeadSeq: 4 }
		] };
		expect(checkRedisReplaySeqRegression(snap)).toEqual({
			category: 'redis.replay.seq-regression',
			context: { topic: 'b', deliveredSeq: 9, ringHeadSeq: 4 }
		});
	});

	it('is a no-op on a snapshot without the replaySeqs rows', () => {
		expect(checkRedisReplaySeqRegression({})).toBeNull();
		expect(checkRedisReplaySeqRegression({ replaySeqs: null })).toBeNull();
	});
});

describe('checkRedisPresenceLocalIndex', () => {
	it('returns null when every topic agrees on its count and index cardinality', () => {
		const snap = { presenceTopics: [
			{ topic: 'room', countKeys: 3, indexKeys: 3 },
			{ topic: 'lobby', countKeys: 0, indexKeys: 0 }
		] };
		expect(checkRedisPresenceLocalIndex(snap)).toBeNull();
	});

	it('flags the first topic whose count map and local index disagree', () => {
		const snap = { presenceTopics: [
			{ topic: 'room', countKeys: 2, indexKeys: 2 },
			{ topic: 'lobby', countKeys: 1, indexKeys: 0 }
		] };
		expect(checkRedisPresenceLocalIndex(snap)).toEqual({
			category: 'redis.presence.local-index-desync',
			context: { topic: 'lobby', countKeys: 1, indexKeys: 0 }
		});
	});

	it('skips rows whose cardinalities are not both numbers', () => {
		expect(checkRedisPresenceLocalIndex({ presenceTopics: [{ topic: 'x', countKeys: 1 }] })).toBeNull();
		expect(checkRedisPresenceLocalIndex({ presenceTopics: [null, { topic: 'y', countKeys: 2, indexKeys: 2 }] })).toBeNull();
	});

	it('is a no-op on a snapshot without the presenceTopics rows', () => {
		expect(checkRedisPresenceLocalIndex({})).toBeNull();
		expect(checkRedisPresenceLocalIndex({ presenceTopics: null })).toBeNull();
	});
});

describe('computeStateHash', () => {
	it('is a stable golden value for a fixed projection', () => {
		// Locked vectors so a change to the fold (which would silently desync the
		// cross-instance convergence comparison) fails loudly. The algorithm mirrors
		// the adapter's internal computeStateHash bit-for-bit.
		expect(computeStateHash({ topicSeqs: { room: 2 } })).toBe(2866720852);
		expect(computeStateHash({ topicSeqs: { room: 3 } })).toBe(2883498471);
		expect(computeStateHash({ topicSeqs: {} })).toBe(4103227121);
	});

	it('treats a missing projection the same as an empty one', () => {
		expect(computeStateHash(undefined)).toBe(computeStateHash({ topicSeqs: {} }));
		expect(computeStateHash({})).toBe(computeStateHash({ topicSeqs: {} }));
	});

	it('is order-independent over the topic entries', () => {
		expect(computeStateHash({ topicSeqs: { a: 1, room: 2 } }))
			.toBe(computeStateHash({ topicSeqs: { room: 2, a: 1 } }));
	});

	it('moves when any seq moves', () => {
		expect(computeStateHash({ topicSeqs: { room: 2 } }))
			.not.toBe(computeStateHash({ topicSeqs: { room: 3 } }));
	});

	it('distinguishes an extra zero-seq topic from its absence (count-seeded)', () => {
		expect(computeStateHash({ topicSeqs: { room: 2 } }))
			.not.toBe(computeStateHash({ topicSeqs: { room: 2, ghost: 0 } }));
	});

	it('returns an unsigned 32-bit integer', () => {
		const h = computeStateHash({ topicSeqs: { x: 7, y: 9 } });
		expect(Number.isInteger(h)).toBe(true);
		expect(h).toBeGreaterThanOrEqual(0);
		expect(h).toBeLessThanOrEqual(0xffffffff);
	});
});

describe('checkSharedStoreConvergence', () => {
	it('returns null when every participating instance delivered the same per-topic run', () => {
		const instances = [
			{ id: 0, topicSeqs: { room: 2 } },
			{ id: 1, topicSeqs: { room: 2 } },
			{ id: 2, topicSeqs: { room: 2 } }
		];
		expect(checkSharedStoreConvergence(instances)).toBeNull();
	});

	it('ignores an instance with no delivered run (a total-drop or publisher-only instance)', () => {
		// Instance 1 delivered nothing for the topic, so it never participates and is
		// not a divergence against the instances that did.
		const instances = [
			{ id: 0, topicSeqs: { room: 2 } },
			{ id: 1, topicSeqs: {} }
		];
		expect(checkSharedStoreConvergence(instances)).toBeNull();
	});

	it('only compares instances that share the exact topic set', () => {
		// Different topic sets bucket apart, so they are never compared - a publisher
		// of one topic does not diverge against a subscriber of another.
		const instances = [
			{ id: 0, topicSeqs: { a: 1 } },
			{ id: 1, topicSeqs: { b: 1 } }
		];
		expect(checkSharedStoreConvergence(instances)).toBeNull();
	});

	it('reports the minority instance when one trailed the shared run', () => {
		const instances = [
			{ id: 0, topicSeqs: { room: 2 } },
			{ id: 1, topicSeqs: { room: 2 } },
			{ id: 2, topicSeqs: { room: 1 } }
		];
		const v = checkSharedStoreConvergence(instances);
		expect(v.category).toBe('cluster.state-divergence');
		expect(v.context.topics).toEqual(['room']);
		expect(v.context.instances).toEqual([2]);
		expect(v.context.expectedHash).toBe(computeStateHash({ topicSeqs: { room: 2 } }));
		expect(v.context.divergentHash).toBe(computeStateHash({ topicSeqs: { room: 1 } }));
	});

	it('picks a deterministic offender on an even split (largest-id group last)', () => {
		const instances = [
			{ id: 0, topicSeqs: { room: 2 } },
			{ id: 5, topicSeqs: { room: 1 } }
		];
		const v = checkSharedStoreConvergence(instances);
		expect(v.category).toBe('cluster.state-divergence');
		// On a size tie the group holding the largest id sorts last and is reported.
		expect(v.context.instances).toEqual([5]);
	});

	it('is a no-op on an empty instance list', () => {
		expect(checkSharedStoreConvergence([])).toBeNull();
		expect(checkSharedStoreConvergence(undefined)).toBeNull();
	});
});
