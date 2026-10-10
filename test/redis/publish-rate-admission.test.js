import { afterEach, describe, expect, it } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPublishRateAggregator } from '../../src/redis/publish-rate.js';
import { createAdmissionControl } from '../../src/shared/admission.js';

const topicRate = (topic, messagesPerSec) => ({ topic, messagesPerSec, bytesPerSec: 0 });
const tick = () => new Promise(resolve => setImmediate(resolve));

describe('publish-rate admission beyond displayed topics', () => {
	const aggregators = [];
	afterEach(async () => {
		await Promise.all(aggregators.splice(0).map(aggregator => aggregator.deactivate()));
	});
	async function setup(slice = [], publishRate = 0, options = {}) {
		const client = mockRedisClient('admission:');
		const platform = mockPlatform();
		platform._setPressure({ ...platform.pressure, topPublishers: slice, publishRate });
		const aggregator = createPublishRateAggregator(client, options);
		aggregators.push(aggregator);
		await aggregator.activate(platform);
		const accepts = (topic, threshold) => createAdmissionControl({
			aggregator, classes: { publish: { clusterTopPublisher: { threshold } } }
		}).shouldAccept('publish', platform, { topic });
		const remote = async (slice, publishRate, extra = {}) => {
			await client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'remote', ts: Date.now(), slice, publishRate, ...extra
			}));
			await tick();
		};
		return { client, platform, aggregator, accepts, remote };
	}

	it('rejects all thirty equally hot topics while the display remains capped at twenty', async () => {
		const slice = Array.from({ length: 30 }, (_, i) => topicRate(`topic:${i}`, 100));
		const { aggregator, accepts } = await setup(slice, 3000);
		expect(aggregator.topPublishers).toHaveLength(20);
		expect(aggregator.rateOf('topic:29')).toBe(0);
		for (const { topic } of slice) expect(accepts(topic, 100), topic).toBe(false);
	});

	it('covers traffic omitted from the adapter sample itself', async () => {
		const { accepts } = await setup(Array.from({ length: 5 }, (_, i) => topicRate(`top:${i}`, 100)), 3000);
		expect(accepts('omitted', 100)).toBe(false);
		expect(accepts('omitted', 3001)).toBe(true);
	});

	it('uses reported rates outside the merged display and bounds missing contributions', async () => {
		const { aggregator, accepts, remote } = await setup([topicRate('local', 200)], 200, { topN: 1 });
		await remote([topicRate('remote-only', 100)], 100);
		expect(aggregator.rateOf('remote-only')).toBe(0);
		expect(accepts('remote-only', 100)).toBe(false);
		expect(accepts('remote-only', 301)).toBe(true);
	});

	it('uses exact sampled contributions when every instance reports the topic', async () => {
		const { accepts, remote } = await setup([topicRate('known', 60)], 10000);
		await remote([topicRate('known', 20)], 20000);
		expect(accepts('known', 81)).toBe(true);
		expect(accepts('known', 80)).toBe(false);
		expect(accepts('unknown', 100)).toBe(false);
	});

	it('adds the whole sampled total for an instance that omits the requested topic', async () => {
		const { accepts, remote } = await setup([topicRate('known', 60)], 60);
		await remote([topicRate('different', 70)], 70);
		expect(accepts('known', 100)).toBe(false);
		expect(accepts('known', 131)).toBe(true);
	});

	it.each([undefined, null, -1, '100', NaN, Infinity])('fails closed for an omitted topic with invalid or legacy total %s', async total => {
		const { accepts, remote } = await setup();
		await remote([topicRate('reported', 5)], total);
		expect(accepts('omitted', 100)).toBe(false);
		expect(accepts('reported', 6)).toBe(true);
	});

	it('does not infer idle traffic from an inconsistent total', async () => {
		const { accepts } = await setup([topicRate('hot', 100)], 0);
		expect(accepts('omitted', 100)).toBe(false);
	});

	it('refreshes cached bounds when only the total changes', async () => {
		const { accepts, platform } = await setup();
		expect(accepts('new', 1)).toBe(true);
		platform._setPressure({ ...platform.pressure, publishRate: 100 });
		expect(accepts('new', 100)).toBe(false);
		platform._setPressure({ ...platform.pressure, publishRate: 0 });
		expect(accepts('new', 1)).toBe(true);
	});

	it('invalidates cached known-topic bounds on remote updates and staleness', async () => {
		const { accepts, remote } = await setup([topicRate('known', 10)], 10, { staleAfter: 50 });
		await remote([topicRate('known', 20)], 20);
		expect(accepts('known', 31)).toBe(true);
		await remote([topicRate('other', 100)], 100);
		expect(accepts('known', 31)).toBe(false);
		await remote([topicRate('other', 100)], 100, { ts: Date.now() - 100 });
		expect(accepts('known', 31)).toBe(true);
	});

	it('fails closed without an active local sample', async () => {
		const { aggregator, accepts } = await setup();
		await aggregator.deactivate();
		expect(accepts('new', 1)).toBe(false);
	});

	it('does not lose small contributions through subtraction of large instance totals', async () => {
		const { accepts, remote } = await setup([topicRate('known', 60)], 1e308);
		await remote([topicRate('different', 70)], 70);
		expect(accepts('known', 100)).toBe(false);
		expect(accepts('known', 131)).toBe(true);
	});

	it('does not treat malformed sampled rates as a known zero', async () => {
		const { accepts, remote } = await setup();
		await remote([topicRate('bad', -10)], 100);
		expect(accepts('bad', 100)).toBe(false);
	});
});
