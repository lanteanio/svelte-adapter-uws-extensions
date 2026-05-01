/**
 * Integration tests for redis/pubsub against a real Redis 7 server.
 *
 * The high-value scenario the in-memory mock cannot exercise is real
 * cross-connection pub/sub delivery: ioredis duplicate() opens a
 * separate TCP connection in subscriber mode, the publisher uses the
 * primary connection, and Redis routes the message between them. This
 * file pins that path end-to-end (round trip through PUBLISH on the
 * wire, JSON encoding, channel naming, microtask relay batching).
 *
 * The mock-based suite at test/redis/pubsub.test.js stays as the
 * exhaustive behavior surface; this file focuses on what only a real
 * server can prove.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createRedisClient } from '../../../redis/index.js';
import { createPubSubBus } from '../../../redis/pubsub.js';
import { mockPlatform } from '../../helpers/mock-platform.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(10);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('redis pubsub bus (integration)', () => {
	let client;
	let platform;
	const buses = [];

	beforeAll(() => {
		const url = process.env.INTEGRATION_REDIS_URL;
		if (!url) {
			throw new Error('INTEGRATION_REDIS_URL not set; global-setup did not run');
		}
		client = createRedisClient({
			url,
			keyPrefix: 'inttest-pubsub:',
			autoShutdown: false
		});
	});

	beforeEach(async () => {
		// Wipe everything under our prefix so each test starts clean.
		let cursor = '0';
		do {
			const [next, keys] = await client.redis.scan(
				cursor, 'MATCH', client.key('*'), 'COUNT', 200
			);
			cursor = next;
			if (keys.length > 0) await client.redis.unlink(...keys);
		} while (cursor !== '0');

		platform = mockPlatform();
	});

	afterEach(async () => {
		// Tear down any buses created during the test so subscriber
		// duplicates don't leak between tests.
		while (buses.length > 0) {
			const b = buses.pop();
			await b.deactivate().catch(() => {});
		}
	});

	afterAll(async () => {
		await client.quit();
	});

	function track(bus) {
		buses.push(bus);
		return bus;
	}

	// Each test uses a unique channel so concurrent subscribers from a
	// previous test that didn't fully tear down don't interfere.
	function uniqueChannel(label) {
		return `inttest-pubsub:${label}:${Date.now()}:${Math.random().toString(16).slice(2, 8)}`;
	}

	describe('cross-connection delivery', () => {
		it('delivers a message published on the primary connection to a duplicate subscriber', async () => {
			const channel = uniqueChannel('basic');
			const bus = track(createPubSubBus(client, { channel }));
			await bus.activate(platform);

			// Use a separate publisher client so we are simulating a sibling
			// process publishing in -- the bus's own subscriber duplicate
			// should pick this up via the wire.
			const publisher = createRedisClient({
				url: process.env.INTEGRATION_REDIS_URL,
				autoShutdown: false
			});
			try {
				await publisher.redis.publish(channel, JSON.stringify({
					instanceId: 'remote-instance',
					topic: 'chat',
					event: 'msg',
					data: { text: 'hello from another client' }
				}));

				await waitFor(() => platform.published.length > 0);
				expect(platform.published[0]).toEqual({
					topic: 'chat',
					event: 'msg',
					data: { text: 'hello from another client' },
					options: { relay: false }
				});
			} finally {
				await publisher.quit();
			}
		});

		it('delivery flows through the wrap() publish path -- two buses see each other', async () => {
			const channel = uniqueChannel('twobus');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createPubSubBus(client, { channel }));
			const busB = track(createPubSubBus(client, { channel }));

			await busA.activate(platformA);
			await busB.activate(platformB);

			const wrappedA = busA.wrap(platformA);
			wrappedA.publish('chat', 'msg', { from: 'A' });

			// B's subscriber receives via Redis; A is echo-suppressed.
			await waitFor(() => platformB.published.find((p) => p.event === 'msg') !== undefined);
			const onB = platformB.published.find((p) => p.event === 'msg');
			expect(onB).toEqual({
				topic: 'chat',
				event: 'msg',
				data: { from: 'A' },
				options: { relay: false }
			});

			// A's local publish ran synchronously through wrap(); the
			// echo from its own subscriber must NOT have produced a
			// second entry.
			expect(platformA.published.filter((p) => p.event === 'msg')).toHaveLength(1);
		});

		it('echo suppression: a bus does not double-deliver its own publishes', async () => {
			const channel = uniqueChannel('echo');
			const bus = track(createPubSubBus(client, { channel }));
			await bus.activate(platform);

			const wrapped = bus.wrap(platform);
			wrapped.publish('chat', 'msg', { text: 'self' });

			// Give the round trip a generous window so a missed echo
			// suppression would surface as a double publish.
			await wait(150);
			expect(platform.published.filter((p) => p.event === 'msg')).toHaveLength(1);
		});

		it('round-trips JSON payload shape (nested objects, arrays, nulls)', async () => {
			const channel = uniqueChannel('json');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createPubSubBus(client, { channel }));
			const busB = track(createPubSubBus(client, { channel }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			const payload = {
				nested: { a: 1, b: [2, 3, null], c: 'hello' },
				flag: false,
				absent: null
			};
			busA.wrap(platformA).publish('rooms/42', 'event-name', payload);

			await waitFor(() => platformB.published.find((p) => p.event === 'event-name') !== undefined);
			const got = platformB.published.find((p) => p.event === 'event-name');
			expect(got.data).toEqual(payload);
			expect(got.topic).toBe('rooms/42');
		});

		it('forwards messages to the latest platform when activate() is called again', async () => {
			const channel = uniqueChannel('relatch');
			const platform1 = mockPlatform();
			const platform2 = mockPlatform();

			const bus = track(createPubSubBus(client, { channel }));
			await bus.activate(platform1);
			await bus.activate(platform2);

			const publisher = createRedisClient({
				url: process.env.INTEGRATION_REDIS_URL,
				autoShutdown: false
			});
			try {
				await publisher.redis.publish(channel, JSON.stringify({
					instanceId: 'remote',
					topic: 'chat',
					event: 'msg',
					data: { text: 'relatched' }
				}));
				await waitFor(() => platform2.published.length > 0);
				expect(platform2.published[0].data).toEqual({ text: 'relatched' });
				expect(platform1.published).toHaveLength(0);
			} finally {
				await publisher.quit();
			}
		});

		it('a bus only receives messages on the channel it subscribed to', async () => {
			const chA = uniqueChannel('isolation-a');
			const chB = uniqueChannel('isolation-b');
			const platformA = mockPlatform();
			const platformB = mockPlatform();

			const busA = track(createPubSubBus(client, { channel: chA }));
			const busB = track(createPubSubBus(client, { channel: chB }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			await client.redis.publish(chA, JSON.stringify({
				instanceId: 'remote',
				topic: 'x',
				event: 'y',
				data: null
			}));

			await waitFor(() => platformA.published.length > 0);
			expect(platformA.published).toHaveLength(1);
			await wait(150);
			expect(platformB.published).toHaveLength(0);
		});

		it('skips malformed JSON without crashing the subscriber', async () => {
			const channel = uniqueChannel('malformed');
			const bus = track(createPubSubBus(client, { channel }));
			await bus.activate(platform);

			await client.redis.publish(channel, 'not-json{');
			await wait(100);
			expect(platform.published).toHaveLength(0);

			// Subsequent good message still arrives.
			await client.redis.publish(channel, JSON.stringify({
				instanceId: 'remote',
				topic: 'chat',
				event: 'msg',
				data: 'ok'
			}));
			await waitFor(() => platform.published.length > 0);
			expect(platform.published[0].data).toBe('ok');
		});
	});

	describe('relay batching pipeline', () => {
		it('coalesces multiple wrap.publish() calls in one tick into a single round trip', async () => {
			const channel = uniqueChannel('batch');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createPubSubBus(client, { channel }));
			const busB = track(createPubSubBus(client, { channel }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			const wrappedA = busA.wrap(platformA);
			wrappedA.publish('chat', 'msg', { i: 1 });
			wrappedA.publish('chat', 'msg', { i: 2 });
			wrappedA.publish('chat', 'msg', { i: 3 });

			// All three must arrive on B in publish order. Microtask-batched
			// pipelining must preserve order on the wire.
			await waitFor(() => platformB.published.filter((p) => p.event === 'msg').length === 3);
			const ids = platformB.published.filter((p) => p.event === 'msg').map((p) => p.data.i);
			expect(ids).toEqual([1, 2, 3]);
		});

		it('relays a wrap.batch() of messages to a remote subscriber', async () => {
			const channel = uniqueChannel('wrap-batch');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createPubSubBus(client, { channel }));
			const busB = track(createPubSubBus(client, { channel }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			busA.wrap(platformA).batch([
				{ topic: 'chat', event: 'msg', data: { i: 'a' } },
				{ topic: 'chat', event: 'msg', data: { i: 'b' } }
			]);

			await waitFor(() => platformB.published.filter((p) => p.event === 'msg').length === 2);
			const got = platformB.published.filter((p) => p.event === 'msg').map((p) => p.data.i);
			expect(got).toEqual(['a', 'b']);
		});

		it('relay: false in wrap.publish() skips the wire entirely', async () => {
			const channel = uniqueChannel('relay-false');
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const busA = track(createPubSubBus(client, { channel }));
			const busB = track(createPubSubBus(client, { channel }));
			await busA.activate(platformA);
			await busB.activate(platformB);

			busA.wrap(platformA).publish('chat', 'msg', { only: 'local' }, { relay: false });

			// Local publish on A still recorded.
			expect(platformA.published).toHaveLength(1);

			// Generous wait: nothing should ever land on B.
			await wait(150);
			expect(platformB.published).toHaveLength(0);
		});
	});

	describe('subscriber lifecycle', () => {
		it('deactivate() releases the subscriber and stops delivering', async () => {
			const channel = uniqueChannel('deactivate');
			const bus = createPubSubBus(client, { channel });
			await bus.activate(platform);
			await bus.deactivate();

			await client.redis.publish(channel, JSON.stringify({
				instanceId: 'remote',
				topic: 'x',
				event: 'y',
				data: null
			}));
			await wait(150);
			expect(platform.published).toHaveLength(0);
		});

		it('activate() is idempotent on a real connection', async () => {
			const channel = uniqueChannel('idempotent');
			const bus = track(createPubSubBus(client, { channel }));
			await bus.activate(platform);
			await bus.activate(platform); // must not throw or open a second subscription

			await client.redis.publish(channel, JSON.stringify({
				instanceId: 'remote',
				topic: 'x',
				event: 'ping',
				data: null
			}));
			await waitFor(() => platform.published.length > 0);
			expect(platform.published).toHaveLength(1);
		});
	});
});
