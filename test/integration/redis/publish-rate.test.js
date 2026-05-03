/**
 * Integration tests for redis/publish-rate against a real Redis 7 server.
 *
 * The mock-based suite at test/redis/publish-rate.test.js covers the
 * envelope shape, merge semantics, and metrics paths. This file pins
 * the actual cross-instance round trip: each instance broadcasts on a
 * shared channel, every instance's subscriber receives sibling slices
 * over real ioredis, and the merged top-N reflects the cluster view.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createPublishRateAggregator } from '../../../redis/publish-rate.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis publish-rate aggregator (integration)', () => {
	let client;
	const aggs = [];

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-pubrate:',
			autoShutdown: false
		});
	});

	beforeEach(() => {
		aggs.length = 0;
	});

	afterEach(async () => {
		while (aggs.length) {
			const a = aggs.pop();
			await a.deactivate().catch(() => {});
		}
	});

	afterAll(async () => {
		await client.quit();
	});

	function makeAgg(topPublishers, opts = {}) {
		const p = mockPlatform();
		p._setPressure({
			active: false,
			subscriberRatio: 0,
			publishRate: 0,
			memoryMB: 0,
			reason: 'NONE',
			topPublishers
		});
		const a = createPublishRateAggregator(client, {
			channel: 'inttest:pubrate',
			...opts
		});
		aggs.push(a);
		return { agg: a, platform: p };
	}

	it('cross-instance broadcast: instance A sees instance B s slice over real Redis', async () => {
		const { agg: aggA, platform: pA } = makeAgg([
			{ topic: 'chat:room1', messagesPerSec: 100, bytesPerSec: 1000 }
		]);
		const { agg: aggB, platform: pB } = makeAgg([
			{ topic: 'chat:room1', messagesPerSec: 200, bytesPerSec: 2000 },
			{ topic: 'audit:org', messagesPerSec: 50, bytesPerSec: 500 }
		]);

		await aggA.activate(pA);
		await aggB.activate(pB);

		// Force broadcast immediately rather than waiting for the timer.
		await aggA._broadcastNow();
		await aggB._broadcastNow();
		await wait(50);

		const merged = aggA.topPublishers;
		const chat = merged.find((t) => t.topic === 'chat:room1');
		expect(chat).toMatchObject({ messagesPerSec: 300, contributingInstances: 2 });
		const audit = merged.find((t) => t.topic === 'audit:org');
		expect(audit).toMatchObject({ messagesPerSec: 50, contributingInstances: 1 });
	});

	it('staleAfter ages out a sibling that stops broadcasting', async () => {
		const { agg: aggA, platform: pA } = makeAgg([
			{ topic: 'local', messagesPerSec: 10, bytesPerSec: 100 }
		], { staleAfter: 100 });
		const { agg: aggB, platform: pB } = makeAgg([
			{ topic: 'remote', messagesPerSec: 999, bytesPerSec: 9999 }
		]);

		await aggA.activate(pA);
		await aggB.activate(pB);

		await aggB._broadcastNow();
		await wait(30);

		expect(aggA.topPublishers.find((t) => t.topic === 'remote')).toBeDefined();

		// Wait past staleAfter without B broadcasting again.
		await wait(150);
		expect(aggA.topPublishers.find((t) => t.topic === 'remote')).toBeUndefined();
		expect(aggA.topPublishers.find((t) => t.topic === 'local')).toBeDefined();
	});

	it('rateOf returns the cluster-wide sum across instances', async () => {
		const { agg: aggA, platform: pA } = makeAgg([
			{ topic: 'hot', messagesPerSec: 100, bytesPerSec: 1000 }
		]);
		const { agg: aggB, platform: pB } = makeAgg([
			{ topic: 'hot', messagesPerSec: 200, bytesPerSec: 2000 }
		]);
		const { agg: aggC, platform: pC } = makeAgg([
			{ topic: 'hot', messagesPerSec: 300, bytesPerSec: 3000 }
		]);

		await aggA.activate(pA);
		await aggB.activate(pB);
		await aggC.activate(pC);

		await aggA._broadcastNow();
		await aggB._broadcastNow();
		await aggC._broadcastNow();
		await wait(50);

		expect(aggA.rateOf('hot')).toBe(600);
	});

	describe('subscribersOf (cluster-wide subject counts)', () => {
		function makeAggWithSubjects(topPublishers, subjects, opts = {}) {
			const p = mockPlatform();
			p._setPressure({
				active: false,
				subscriberRatio: 0,
				publishRate: 0,
				memoryMB: 0,
				reason: 'NONE',
				topPublishers
			});
			const a = createPublishRateAggregator(client, {
				channel: 'inttest:pubrate-subs',
				subjects,
				...opts
			});
			aggs.push(a);
			return { agg: a, platform: p };
		}

		it('sums local + remote subscriber counts across instances after a broadcast tick', async () => {
			const { agg: aggA, platform: pA } = makeAggWithSubjects(
				[],
				() => [{ topic: 'feed:42', count: 7 }, { topic: 'feed:99', count: 3 }]
			);
			const { agg: aggB, platform: pB } = makeAggWithSubjects(
				[],
				() => [{ topic: 'feed:42', count: 12 }]
			);

			await aggA.activate(pA);
			await aggB.activate(pB);
			await aggA._broadcastNow();
			await aggB._broadcastNow();
			await wait(50);

			// Both observers see the cluster sum (A:7 + B:12 = 19).
			expect(aggA.subscribersOf('feed:42')).toBe(19);
			expect(aggB.subscribersOf('feed:42')).toBe(19);
			// 'feed:99' only on A; both observers see A's contribution of 3.
			expect(aggA.subscribersOf('feed:99')).toBe(3);
			expect(aggB.subscribersOf('feed:99')).toBe(3);
			// Topic nobody reports returns 0 cleanly.
			expect(aggA.subscribersOf('absent')).toBe(0);
		});

		it('staleAfter pruning removes a contributor when its broadcast tick stops', async () => {
			const { agg: aggA, platform: pA } = makeAggWithSubjects(
				[],
				() => [{ topic: 't', count: 1 }],
				{ staleAfter: 100 }
			);
			const { agg: aggB, platform: pB } = makeAggWithSubjects(
				[],
				() => [{ topic: 't', count: 50 }]
			);

			await aggA.activate(pA);
			await aggB.activate(pB);
			await aggB._broadcastNow();
			await wait(30);

			expect(aggA.subscribersOf('t')).toBe(51);

			await wait(150);
			// B's contribution aged out; only A's local count remains.
			expect(aggA.subscribersOf('t')).toBe(1);
		});

		it('subjects() throwing on one instance does not break the broadcast on the contributing instance', async () => {
			// A's subjects throws; A's broadcast still goes out, the envelope
			// just carries `subs: []` and consumers see no contribution from A.
			let throwOnce = true;
			const { agg: aggA, platform: pA } = makeAggWithSubjects(
				[],
				() => {
					if (throwOnce) {
						throwOnce = false;
						throw new Error('boom');
					}
					return [{ topic: 't', count: 9 }];
				}
			);
			const { agg: aggB, platform: pB } = makeAggWithSubjects(
				[],
				() => [{ topic: 't', count: 5 }]
			);

			await aggA.activate(pA);
			await aggB.activate(pB);
			// A's first broadcast: subjects() throws, envelope still publishes
			// with empty subs.
			await aggA._broadcastNow();
			await aggB._broadcastNow();
			await wait(50);

			// B sees only its own local count + A's empty contribution.
			// (A's local snapshot at this exact moment also throws so its own
			// view of 't' is 0 + remote 5 = 5.)
			expect(aggB.subscribersOf('t')).toBe(5);

			// Subsequent A broadcast succeeds (throwOnce flipped).
			await aggA._broadcastNow();
			await wait(50);
			expect(aggB.subscribersOf('t')).toBe(14);
			expect(aggA.subscribersOf('t')).toBe(14);
		});
	});
});
