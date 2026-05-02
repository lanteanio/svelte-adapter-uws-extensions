import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPublishRateAggregator } from '../../redis/publish-rate.js';
import { createMetrics } from '../../prometheus/index.js';

describe('redis publish-rate aggregator', () => {
	let client;
	let platform;

	beforeEach(() => {
		client = mockRedisClient('app:');
		platform = mockPlatform();
		platform._setPressure({
			active: false,
			subscriberRatio: 0,
			publishRate: 0,
			memoryMB: 0,
			reason: 'NONE',
			topPublishers: [
				{ topic: 'chat:room1', messagesPerSec: 100, bytesPerSec: 1000 },
				{ topic: 'audit:org1', messagesPerSec: 50, bytesPerSec: 500 }
			]
		});
	});

	describe('construction', () => {
		it('requires a redis client', () => {
			expect(() => createPublishRateAggregator(null)).toThrow(/client/);
			expect(() => createPublishRateAggregator({})).toThrow(/client/);
		});

		it('rejects invalid options', () => {
			expect(() => createPublishRateAggregator(client, { publishInterval: 0 })).toThrow();
			expect(() => createPublishRateAggregator(client, { staleAfter: -1 })).toThrow();
			expect(() => createPublishRateAggregator(client, { topN: 0 })).toThrow();
			expect(() => createPublishRateAggregator(client, { topN: 1.5 })).toThrow();
		});

		it('exposes a stable instanceId per aggregator', () => {
			const a = createPublishRateAggregator(client);
			const b = createPublishRateAggregator(client);
			expect(typeof a.instanceId).toBe('string');
			expect(a.instanceId).not.toBe(b.instanceId);
			a.deactivate();
			b.deactivate();
		});
	});

	describe('local slice', () => {
		it('topPublishers reads platform.pressure.topPublishers fresh on each call', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			const snap1 = agg.topPublishers;
			expect(snap1).toHaveLength(2);
			expect(snap1[0]).toMatchObject({
				topic: 'chat:room1',
				messagesPerSec: 100,
				bytesPerSec: 1000,
				contributingInstances: 1
			});

			// Update the platform's slice; next read should reflect it.
			platform._setPressure({
				...platform.pressure,
				topPublishers: [{ topic: 'new:topic', messagesPerSec: 999, bytesPerSec: 9999 }]
			});
			expect(agg.topPublishers[0]).toMatchObject({ topic: 'new:topic', messagesPerSec: 999 });

			await agg.deactivate();
		});

		it('returns an empty array when platform has no pressure data', async () => {
			const bare = mockPlatform();
			// Clear topPublishers
			bare._setPressure({ ...bare.pressure, topPublishers: undefined });
			const agg = createPublishRateAggregator(client);
			await agg.activate(bare);
			expect(agg.topPublishers).toEqual([]);
			await agg.deactivate();
		});

		it('caps the local slice at topN', async () => {
			const big = Array.from({ length: 30 }, (_, i) => ({
				topic: 't' + i, messagesPerSec: 30 - i, bytesPerSec: (30 - i) * 10
			}));
			platform._setPressure({ ...platform.pressure, topPublishers: big });
			const agg = createPublishRateAggregator(client, { topN: 5 });
			await agg.activate(platform);
			expect(agg.topPublishers).toHaveLength(5);
			expect(agg.topPublishers[0].topic).toBe('t0');
			await agg.deactivate();
		});
	});

	describe('cross-instance merging', () => {
		it('merges remote slices with the local slice and sums by topic', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			// Inject a remote slice envelope on the channel.
			const remoteEnvelope = JSON.stringify({
				instanceId: 'other-inst',
				ts: Date.now(),
				slice: [
					{ topic: 'chat:room1', messagesPerSec: 200, bytesPerSec: 2000 },
					{ topic: 'org:42:items', messagesPerSec: 75, bytesPerSec: 750 }
				]
			});
			client.redis.publish('uws:pressure:rates', remoteEnvelope);
			await new Promise((r) => setImmediate(r));

			const merged = agg.topPublishers;
			const chat = merged.find((t) => t.topic === 'chat:room1');
			expect(chat).toMatchObject({
				topic: 'chat:room1',
				messagesPerSec: 300, // 100 local + 200 remote
				bytesPerSec: 3000,
				contributingInstances: 2
			});
			const audit = merged.find((t) => t.topic === 'audit:org1');
			expect(audit.contributingInstances).toBe(1);
			const org = merged.find((t) => t.topic === 'org:42:items');
			expect(org).toMatchObject({ messagesPerSec: 75, contributingInstances: 1 });

			await agg.deactivate();
		});

		it('drops echo (envelope from this instance)', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			// Local slice contributes once; an echoed envelope must NOT
			// double-count the same topic.
			const ourEcho = JSON.stringify({
				instanceId: agg.instanceId,
				ts: Date.now(),
				slice: [{ topic: 'chat:room1', messagesPerSec: 9999, bytesPerSec: 99999 }]
			});
			client.redis.publish('uws:pressure:rates', ourEcho);
			await new Promise((r) => setImmediate(r));

			const chat = agg.topPublishers.find((t) => t.topic === 'chat:room1');
			expect(chat.messagesPerSec).toBe(100); // local only, echo suppressed
			expect(chat.contributingInstances).toBe(1);
			await agg.deactivate();
		});

		it('drops malformed envelopes silently', async () => {
			const metrics = createMetrics();
			const agg = createPublishRateAggregator(client, { metrics });
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', 'not json');
			client.redis.publish('uws:pressure:rates', JSON.stringify({ instanceId: null }));
			client.redis.publish('uws:pressure:rates', JSON.stringify({ instanceId: 'x', slice: 'not array' }));
			await new Promise((r) => setImmediate(r));

			const out = await metrics.serialize();
			expect(out).toMatch(/cluster_publish_rate_parse_errors_total\s+\d/);
			await agg.deactivate();
		});
	});

	describe('staleAfter pruning', () => {
		it('drops a remote slice whose ts is older than staleAfter', async () => {
			const agg = createPublishRateAggregator(client, { staleAfter: 50 });
			await agg.activate(platform);

			const stale = JSON.stringify({
				instanceId: 'gone-inst',
				ts: Date.now() - 200,
				slice: [{ topic: 'phantom', messagesPerSec: 50, bytesPerSec: 500 }]
			});
			client.redis.publish('uws:pressure:rates', stale);
			await new Promise((r) => setImmediate(r));

			const merged = agg.topPublishers;
			expect(merged.find((t) => t.topic === 'phantom')).toBeUndefined();
			await agg.deactivate();
		});
	});

	describe('rateOf', () => {
		it('returns the cluster-wide rate for a topic, summed across instances', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			const remote = JSON.stringify({
				instanceId: 'other',
				ts: Date.now(),
				slice: [{ topic: 'chat:room1', messagesPerSec: 200, bytesPerSec: 2000 }]
			});
			client.redis.publish('uws:pressure:rates', remote);
			await new Promise((r) => setImmediate(r));

			expect(agg.rateOf('chat:room1')).toBe(300);
			await agg.deactivate();
		});

		it('returns 0 for a topic not in the merged top-N', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);
			expect(agg.rateOf('unknown')).toBe(0);
			await agg.deactivate();
		});
	});

	describe('broadcast', () => {
		it('publishes the local slice on the configured channel', async () => {
			const agg = createPublishRateAggregator(client, { channel: 'custom:channel' });
			await agg.activate(platform);

			let received;
			const probeAgg = createPublishRateAggregator(client, { channel: 'custom:channel' });
			await probeAgg.activate(mockPlatform());

			// Use a probe subscriber on the same channel by hooking into mock-redis.
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const origMsg = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (origMsg) origMsg(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});

			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received).toBeDefined();
			expect(received.instanceId).toBe(agg.instanceId);
			expect(Array.isArray(received.slice)).toBe(true);
			expect(received.slice).toHaveLength(2);

			await agg.deactivate();
			await probeAgg.deactivate();
		});

		it('counts cluster_publish_rate_broadcasts_total', async () => {
			const metrics = createMetrics();
			const agg = createPublishRateAggregator(client, { metrics });
			await agg.activate(platform);
			await agg._broadcastNow();
			await agg._broadcastNow();
			const out = await metrics.serialize();
			expect(out).toMatch(/cluster_publish_rate_broadcasts_total\s+2/);
			await agg.deactivate();
		});

		it('counts cluster_publish_rate_received_total when a sibling slice arrives', async () => {
			const metrics = createMetrics();
			const agg = createPublishRateAggregator(client, { metrics });
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'sibling',
				ts: Date.now(),
				slice: [{ topic: 't', messagesPerSec: 1, bytesPerSec: 1 }]
			}));
			await new Promise((r) => setImmediate(r));

			const out = await metrics.serialize();
			expect(out).toMatch(/cluster_publish_rate_received_total\s+1/);
			await agg.deactivate();
		});
	});

	describe('lifecycle', () => {
		it('activate is idempotent', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);
			await expect(agg.activate(platform)).resolves.toBeUndefined();
			await agg.deactivate();
		});

		it('deactivate clears remote slices', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'sib',
				ts: Date.now(),
				slice: [{ topic: 't', messagesPerSec: 50, bytesPerSec: 500 }]
			}));
			await new Promise((r) => setImmediate(r));
			expect(agg.topPublishers.find((t) => t.topic === 't')).toBeDefined();

			await agg.deactivate();
			// After deactivate, both remote and local slices are unavailable
			// (subscriber gone, platform null). Re-activating with platform
			// recovers local; remote starts empty.
			await agg.activate(platform);
			expect(agg.topPublishers.find((t) => t.topic === 't')).toBeUndefined();
			await agg.deactivate();
		});

		it('deactivate is idempotent', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);
			await agg.deactivate();
			await expect(agg.deactivate()).resolves.toBeUndefined();
		});
	});

	describe('subjects (cluster subscriber counts)', () => {
		it('rejects a non-function subjects option', () => {
			expect(() => createPublishRateAggregator(client, { subjects: 'nope' })).toThrow(/subjects/);
			expect(() => createPublishRateAggregator(client, { subjects: 123 })).toThrow(/subjects/);
		});

		it('omits the subs field when no subjects callback is wired', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			let received;
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const orig = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (orig) orig(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});
			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received).toBeDefined();
			expect(received.subs).toBeUndefined();
			await agg.deactivate();
		});

		it('broadcasts subs when the subjects callback returns a list', async () => {
			const agg = createPublishRateAggregator(client, {
				subjects: () => [
					{ topic: 'chat:room1', count: 5 },
					{ topic: 'audit:org1', count: 2 }
				]
			});
			await agg.activate(platform);

			let received;
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const orig = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (orig) orig(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});
			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received).toBeDefined();
			expect(Array.isArray(received.subs)).toBe(true);
			expect(received.subs).toHaveLength(2);
			const byTopic = Object.fromEntries(received.subs.map((s) => [s.topic, s.count]));
			expect(byTopic).toEqual({ 'chat:room1': 5, 'audit:org1': 2 });
			await agg.deactivate();
		});

		it('caps the local subs at topN sorted descending by count', async () => {
			const big = Array.from({ length: 30 }, (_, i) => ({ topic: 't' + i, count: 30 - i }));
			const agg = createPublishRateAggregator(client, {
				topN: 5,
				subjects: () => big
			});
			await agg.activate(platform);

			let received;
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const orig = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (orig) orig(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});
			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received.subs).toHaveLength(5);
			expect(received.subs[0]).toEqual({ topic: 't0', count: 30 });
			expect(received.subs[4]).toEqual({ topic: 't4', count: 26 });
			await agg.deactivate();
		});

		it('dedupes duplicate topics in the local subs slice', async () => {
			const agg = createPublishRateAggregator(client, {
				subjects: () => [
					{ topic: 'chat:room1', count: 3 },
					{ topic: 'chat:room1', count: 2 }
				]
			});
			await agg.activate(platform);

			let received;
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const orig = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (orig) orig(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});
			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received.subs).toHaveLength(1);
			expect(received.subs[0]).toEqual({ topic: 'chat:room1', count: 5 });
			await agg.deactivate();
		});

		it('catches a throwing subjects callback and broadcasts an empty subs slice', async () => {
			const agg = createPublishRateAggregator(client, {
				subjects: () => { throw new Error('boom'); }
			});
			await agg.activate(platform);

			let received;
			const handlers = client._pubsubHandlers;
			const probeDup = handlers[handlers.length - 1];
			const orig = probeDup.listeners.get('message');
			probeDup.listeners.set('message', (ch, raw) => {
				if (orig) orig(ch, raw);
				try { received = JSON.parse(raw); } catch { /* noop */ }
			});
			await agg._broadcastNow();
			await new Promise((r) => setImmediate(r));

			expect(received).toBeDefined();
			expect(received.subs).toEqual([]);
			await agg.deactivate();
		});
	});

	describe('subscribersOf', () => {
		it('returns 0 for an unknown topic', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);
			expect(agg.subscribersOf('nope')).toBe(0);
			await agg.deactivate();
		});

		it('sums the local count and remote contributions', async () => {
			const agg = createPublishRateAggregator(client, {
				subjects: () => [{ topic: 'chat:room1', count: 5 }]
			});
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'instB',
				ts: Date.now(),
				slice: [],
				subs: [{ topic: 'chat:room1', count: 7 }]
			}));
			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'instC',
				ts: Date.now(),
				slice: [],
				subs: [{ topic: 'chat:room1', count: 3 }]
			}));
			await new Promise((r) => setImmediate(r));

			expect(agg.subscribersOf('chat:room1')).toBe(15); // 5 + 7 + 3
			await agg.deactivate();
		});

		it('returns the live local count even when no remotes have reported the topic', async () => {
			const agg = createPublishRateAggregator(client, {
				subjects: () => [{ topic: 'solo', count: 4 }]
			});
			await agg.activate(platform);
			expect(agg.subscribersOf('solo')).toBe(4);
			await agg.deactivate();
		});

		it('returns the remote sum even when subjects is not wired', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'instB',
				ts: Date.now(),
				slice: [],
				subs: [{ topic: 'remote-only', count: 9 }]
			}));
			await new Promise((r) => setImmediate(r));

			expect(agg.subscribersOf('remote-only')).toBe(9);
			await agg.deactivate();
		});

		it('drops a remote subs contribution past staleAfter', async () => {
			const agg = createPublishRateAggregator(client, { staleAfter: 50 });
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'gone',
				ts: Date.now() - 200,
				slice: [],
				subs: [{ topic: 'phantom', count: 12 }]
			}));
			await new Promise((r) => setImmediate(r));

			expect(agg.subscribersOf('phantom')).toBe(0);
			await agg.deactivate();
		});

		it('reads the local slice fresh on every call (no stale cache)', async () => {
			let count = 1;
			const agg = createPublishRateAggregator(client, {
				subjects: () => [{ topic: 'live', count }]
			});
			await agg.activate(platform);

			expect(agg.subscribersOf('live')).toBe(1);
			count = 7;
			expect(agg.subscribersOf('live')).toBe(7);
			await agg.deactivate();
		});
	});

	describe('subs envelope compatibility', () => {
		it('parses an envelope without a subs field cleanly (back-compat)', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'old-style',
				ts: Date.now(),
				slice: [{ topic: 't', messagesPerSec: 10, bytesPerSec: 100 }]
				// no subs field
			}));
			await new Promise((r) => setImmediate(r));

			// slice merged; subs absent doesn't poison anything.
			expect(agg.rateOf('t')).toBe(10);
			expect(agg.subscribersOf('t')).toBe(0);
			await agg.deactivate();
		});

		it('ignores subs field when not subscribing to it (forward-compat)', async () => {
			// Aggregator with no subjects -- still receives subs from siblings,
			// caches them, and exposes via subscribersOf.
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'newer',
				ts: Date.now(),
				slice: [],
				subs: [{ topic: 'shared', count: 4 }]
			}));
			await new Promise((r) => setImmediate(r));

			expect(agg.subscribersOf('shared')).toBe(4);
			await agg.deactivate();
		});

		it('deactivate clears remote subs cache', async () => {
			const agg = createPublishRateAggregator(client);
			await agg.activate(platform);

			client.redis.publish('uws:pressure:rates', JSON.stringify({
				instanceId: 'sibling',
				ts: Date.now(),
				slice: [],
				subs: [{ topic: 't', count: 3 }]
			}));
			await new Promise((r) => setImmediate(r));
			expect(agg.subscribersOf('t')).toBe(3);

			await agg.deactivate();
			await agg.activate(platform);
			expect(agg.subscribersOf('t')).toBe(0);
			await agg.deactivate();
		});
	});
});
