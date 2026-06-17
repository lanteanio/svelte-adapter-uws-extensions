import { describe, it, expect } from 'vitest';
import {
	checkSubscriptionBookkeeping,
	checkTotalSubscriptions,
	checkTopicsHaveSubscribers,
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
