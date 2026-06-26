import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';

const tick = (ms = 15) => new Promise((r) => setTimeout(r, ms));

// The de-herd window must survive the cross-node relay: without it, only the
// originating worker's subscribers stagger and every other node stampedes at t+0.
describe('redis pubsub de-herd window relay', () => {
	let client;
	let platform;
	let bus;

	beforeEach(() => {
		client = mockRedisClient();
		platform = mockPlatform();
		bus = createPubSubBus(client);
	});

	function captureRelay() {
		const calls = [];
		const orig = client.redis.publish;
		client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };
		return calls;
	}

	it('carries jitterMs as `j` on the relayed envelope', async () => {
		const wrapped = bus.wrap(platform);
		const calls = captureRelay();
		wrapped.publish('chat', 'reroute', { x: 1 }, { jitterMs: 5000 });
		await tick();
		expect(calls).toHaveLength(1);
		expect(calls[0]).toMatchObject({ topic: 'chat', event: 'reroute', data: { x: 1 }, j: 5000 });
	});

	it('omits `j` for a non-jittered publish (relay wire unchanged)', async () => {
		const wrapped = bus.wrap(platform);
		const calls = captureRelay();
		wrapped.publish('chat', 'msg', { x: 1 });
		await tick();
		expect(calls).toHaveLength(1);
		expect('j' in calls[0]).toBe(false);
	});

	it('re-applies the window on the receiving worker (cross-node de-herd)', async () => {
		// A second bus on the same Redis receives the jittered publish and re-stamps
		// jitterMs so its own subscribers stagger too.
		const platformB = mockPlatform();
		const busB = createPubSubBus(client);
		busB.activate(platformB);
		await tick();

		bus.wrap(platform).publish('chat', 'reroute', { x: 1 }, { jitterMs: 5000 });
		await tick(25);

		const got = platformB.published.find((p) => p.topic === 'chat' && p.event === 'reroute');
		expect(got).toBeDefined();
		expect(got.options).toMatchObject({ relay: false, jitterMs: 5000 });

		if (busB.deactivate) await busB.deactivate();
	});
});
