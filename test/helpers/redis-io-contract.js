import { describe, it, expect } from 'vitest';
import { countRedisIO, assertConstantIO } from './redis-io-counter.js';
import { mockPlatform } from './mock-platform.js';
import { mockWs } from './mock-ws.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createPresence } from '../../src/redis/presence.js';
import { createReplay } from '../../src/redis/replay.js';
import { openSubscriber } from '../../src/shared/redis-subscriber.js';

// A real server delivers within milliseconds, but a loaded host can stall the
// subscriber past the 1 s poll default. The count is read once delivery is
// seen, so a longer deadline adds no I/O to it.
const DELIVERY = { timeout: 10_000 };

// Lower budgets whenever possible. Raising one requires a recorded design reason.
// Measurements exclude connection setup, script-cache warmup, and teardown.
export function redisIOContract(makeClient) {
	describe('Redis I/O budgets', () => {
		it('rejects genuinely scaling work', async () => {
			const raw = makeClient();
			const meter = countRedisIO(raw);
			try {
				await meter.client.redis.get('io-self-check');
				const small = meter.read();
				meter.reset();
				for (let i = 0; i < 6; i++) await meter.client.redis.get('io-self-check');
				expect(() => assertConstantIO(small, meter.read(), 1)).toThrow('Redis I/O budget');
			} finally { await raw.quit(); }
		});

		it('counts commands on subscriber connections duplicated from the client', async () => {
			const raw = makeClient();
			const meter = countRedisIO(raw);
			const subscriber = openSubscriber(meter.client, 'io-self-check');
			try {
				meter.reset();
				await subscriber.subscribe('io-self-check:a');
				await subscriber.subscribe('io-self-check:b');
				await subscriber.unsubscribe('io-self-check:a');
				expect(meter.commands()).toEqual(['subscribe', 'subscribe', 'unsubscribe']);
			} finally { await subscriber.quit(); await raw.quit(); }
		});

		it('coalesces publishes independently of message count', async () => {
			const counts = [];
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const platform = mockPlatform();
				const bus = createPubSubBus(meter.client);
				// No ready check: its INFO would follow a SUBSCRIBE sent while connecting
				// and fail in subscriber mode, dropping the subscription on reconnect.
				const observer = raw.duplicate({ enableReadyCheck: false });
				const received = [];
				observer.on('message', (_channel, value) => received.push(JSON.parse(value)));
				try {
					await raw.redis.get('io-self-check');
					await observer.subscribe('uws:pubsub');
					await bus.activate(platform);
					const wrapped = bus.wrap(platform);
					meter.reset();
					for (let i = 0; i < size; i++) wrapped.publish('room', 'value', i);
					await expect.poll(() => received.length, DELIVERY).toBe(size);
					counts.push(meter.read());
					expect(platform.published).toHaveLength(size);
					expect(received.map(frame => frame.data)).toEqual(Array.from({ length: size }, (_, i) => i));
				} finally { await bus.deactivate(); await observer.quit(); await raw.quit(); }
			}
			// All messages use one channel, hence one node pipeline even in Cluster.
			assertConstantIO(...counts, 1);
		});

		it('sends one batched envelope regardless of subscriber count and skips local-only traffic', async () => {
			const counts = [];
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const bus = createPubSubBus(meter.client);
				const observers = Array.from({ length: size }, () => raw.duplicate({ enableReadyCheck: false }));
				const received = observers.map(() => []);
				try {
					await Promise.all(observers.map(async (observer, index) => {
						observer.on('message', (_channel, value) => received[index].push(JSON.parse(value)));
						await observer.subscribe('uws:pubsub');
					}));
					await bus.activate(mockPlatform());
					const wrapped = bus.wrap(mockPlatform());
					const messages = Array.from({ length: size }, (_, i) => ({ topic: 'room', event: 'value', data: i }));
					meter.reset();
					wrapped.publishBatched(messages);
					await expect.poll(() => received.every(frames => frames.length === 1), DELIVERY).toBe(true);
					counts.push(meter.read());
					for (const frames of received) expect(frames[0].batch.map(frame => frame.data)).toEqual(messages.map(frame => frame.data));
					meter.reset();
					wrapped.publish('room', 'local', 1, { relay: false });
					wrapped.publishBatched([]);
					expect(meter.read()).toBe(0);
				} finally { await bus.deactivate(); await Promise.all(observers.map(observer => observer.quit())); await raw.quit(); }
			}
			assertConstantIO(...counts, 1);
		});

		it('fills a replay window with one read for six times as many frames', async () => {
			const counts = [];
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const replay = createReplay(meter.client, { size: 20 });
				try {
					await replay.clearTopic('room');
					for (let i = 0; i < size; i++) await replay.publish(mockPlatform(), 'room', 'value', i);
					meter.reset();
					const frames = await replay.since('room', 0);
					expect(frames.map(frame => frame.data)).toEqual(Array.from({ length: size }, (_, i) => i));
					counts.push(meter.read());
					meter.reset();
					expect(await replay.since('room', -1)).toEqual([]);
					expect(meter.read()).toBe(0);
				} finally { await raw.quit(); }
			}
			assertConstantIO(...counts, 1);
		});

		// Presence slot fan-out: every key of a topic (its roster hash, pending hash
		// and per-user hashes) carries the `{topic}` hash tag, so a single-topic
		// join, update or leave script and its execMultiSlot leave pipeline land on
		// one slot and one owning master even in Cluster. A leave spanning topics
		// on k distinct owners submits k pipelines; these budgets use one topic.
		// The event PUBLISH is keyless and propagates cluster-wide from one node.
		// SUBSCRIBE / UNSUBSCRIBE go out on the presence subscriber connection.
		it('pins presence writes, unchanged updates and roster reads', async () => {
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const platform = mockPlatform();
				const presence = createPresence(meter.client, { key: 'id', heartbeat: 60000, consistencyAuditIntervalMs: 0, selfPreservation: false });
				try {
					const sockets = Array.from({ length: size }, (_, i) => mockWs({ id: String(i) }));
					for (const ws of sockets) await presence.join(ws, 'room', platform);
					meter.reset();
					await presence.join(mockWs({ id: 'extra' }), 'room', platform);
					expect(meter.read()).toBe(4);
					meter.reset();
					expect(await presence.list('room')).toHaveLength(size + 1);
					expect(meter.read()).toBe(1);
					meter.reset();
					platform.sent.length = 0;
					await Promise.all(sockets.map(ws => presence.sync(ws, 'room', platform)));
					expect(meter.read()).toBe(1);
					expect(platform.sent.filter(frame => frame.event === 'state')).toHaveLength(size);
					meter.reset();
					await presence.update(sockets[0], 'room', { state: 'ready' }, platform);
					expect(meter.read()).toBe(2);
					meter.reset();
					await presence.update(sockets[0], 'room', { state: 'ready' }, platform);
					expect(meter.read()).toBe(0);
					meter.reset();
					await presence.leave(sockets[0], platform, 'room');
					// The leave script publishes the leave event itself.
					expect(meter.read()).toBe(1);
				} finally { await presence.clear('room'); presence.destroy(); await raw.quit(); }
			}
		});

		// One SUBSCRIBE when a topic gains its first local member and one
		// UNSUBSCRIBE when it loses its last, however many members come between.
		// The warm-up join on another topic opens the subscriber connection and
		// runs the version probe, which these budgets exclude.
		it('subscribes on the first join and unsubscribes on the last leave only', async () => {
			const subscribes = [];
			const unsubscribes = [];
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const platform = mockPlatform();
				const presence = createPresence(meter.client, { key: 'id', heartbeat: 60000, consistencyAuditIntervalMs: 0, selfPreservation: false });
				const tally = name => meter.commands().filter(command => command === name).length;
				try {
					await presence.join(mockWs({ id: 'warm' }), 'warm', platform);
					const sockets = Array.from({ length: size }, (_, i) => mockWs({ id: String(i) }));
					meter.reset();
					await presence.join(sockets[0], 'room', platform);
					expect(meter.read()).toBe(5);
					expect(tally('subscribe')).toBe(1);
					for (const ws of sockets.slice(1)) await presence.join(ws, 'room', platform);
					expect(meter.read()).toBe(5 + 4 * (size - 1));
					subscribes.push(tally('subscribe'));
					meter.reset();
					for (const ws of sockets.slice(0, -1)) await presence.leave(ws, platform, 'room');
					expect(meter.read()).toBe(2 * (size - 1));
					expect(tally('unsubscribe')).toBe(0);
					await presence.leave(sockets[size - 1], platform, 'room');
					expect(meter.read()).toBe(2 * (size - 1) + 3);
					unsubscribes.push(tally('unsubscribe'));
				} finally { await presence.clear('room'); await presence.clear('warm'); presence.destroy(); await raw.quit(); }
			}
			assertConstantIO(...subscribes, 1);
			assertConstantIO(...unsubscribes, 1);
		});
	});
}
