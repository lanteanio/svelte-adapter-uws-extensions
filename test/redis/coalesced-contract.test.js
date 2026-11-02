import { afterEach, describe, expect, it } from 'vitest';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { captureRegistered } from '../helpers/diagnostic-line.js';

const buses = [];
afterEach(async () => {
	for (const bus of buses.splice(0)) await bus.deactivate();
});

for (const [name, factory] of [['standard', createPubSubBus], ['sharded', createShardedBus]]) {
	for (const tracing of [false, true]) describe(`${name} coalesced relay, tracing ${tracing}`, () => {
		function setup(options = {}) {
			const client = mockRedisClient();
			const source = mockPlatform();
			source.trace.enabled = tracing;
			source.traceContext = tracing ? { traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01' } : null;
			const bus = factory(client, options);
			buses.push(bus);
			return { client, source, bus, wrapped: bus.wrap(source) };
		}

		it('returns true for queued relay and re-coalesces at the receiver without local delivery', async () => {
			const { client, source, wrapped } = setup();
			const receiver = mockPlatform();
			const socket = {};
			receiver.forEachSubscriber = (topic, visit) => { if (topic === 'room') visit(socket); };
			const remote = factory(client);
			buses.push(remote);
			await remote.activate(receiver);
			if (remote.follow) await remote.follow('room');
			expect(wrapped.relayCoalesced('room', 'move', { x: 4 }, 'cursor')).toBe(true);
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(source.published).toEqual([]);
			expect(source.sentCoalesced).toEqual([]);
			expect(receiver.sentCoalesced).toEqual([{ ws: socket, key: 'room\0cursor', topic: 'room', event: 'move', data: { x: 4 } }]);
		});

		it('serializes application data once, including trace metadata fallback', async () => {
			const { client, wrapped } = setup({ maxEnvelopeBytes: 160 });
			const receiver = mockPlatform();
			const socket = {};
			receiver.forEachSubscriber = (_, visit) => visit(socket);
			const remote = factory(client, { maxEnvelopeBytes: 160 });
			buses.push(remote);
			await remote.activate(receiver);
			if (remote.follow) await remote.follow('room');
			let calls = 0;
			const diagnostics = captureRegistered();
			try {
				expect(wrapped.relayCoalesced('room', 'move', { toJSON() { calls++; return { x: 4 }; } }, null)).toBe(true);
				await new Promise(resolve => setTimeout(resolve, 10));
			} finally { diagnostics.stop(); }
			expect(calls).toBe(1);
			expect(receiver.sentCoalesced).toEqual([{ ws: socket, key: 'room\0', topic: 'room', event: 'move', data: { x: 4 } }]);
		});

		it('returns false when the external relay cannot admit the envelope', () => {
			const { wrapped } = setup({ maxEnvelopeBytes: 128 });
			const diagnostics = captureRegistered();
			try { expect(wrapped.relayCoalesced('room', 'move', 'x'.repeat(1000))).toBe(false); }
			finally { diagnostics.stop(); }
		});

		it.each([undefined, null])('normalizes %s data and coalesce key to null and empty key', async (data) => {
			const { client, wrapped } = setup();
			const receiver = mockPlatform();
			const socket = {};
			receiver.forEachSubscriber = (_, visit) => visit(socket);
			const remote = factory(client);
			buses.push(remote);
			await remote.activate(receiver);
			if (remote.follow) await remote.follow('room');
			expect(wrapped.relayCoalesced('room', 'move', data)).toBe(true);
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(receiver.sentCoalesced[0]).toMatchObject({ key: 'room\0', data: null });
		});

		it.each([['room', 4], [4, 'move'], ['room\n', 'move'], ['room', 'mo"ve'], ['ro\\om', 'move']])('validates names before publishing (%j, %j)', (topic, event) => {
			const { wrapped, source } = setup();
			expect(() => wrapped.relayCoalesced(topic, event, {})).toThrow();
			expect(source.published).toEqual([]);
		});

		it.each([() => {}, Symbol('value'), { toJSON() {} }, 1n])('refuses data without a JSON form', (value) => {
			const { wrapped } = setup();
			expect(() => wrapped.relayCoalesced('room', 'move', value)).toThrow(TypeError);
		});
	});
}
