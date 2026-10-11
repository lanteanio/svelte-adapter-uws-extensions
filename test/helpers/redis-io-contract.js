import { describe, it, expect } from 'vitest';
import { countRedisIO, assertConstantIO } from './redis-io-counter.js';
import { mockPlatform } from './mock-platform.js';
import { mockWs } from './mock-ws.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createPresence } from '../../src/redis/presence.js';
import { createReplay } from '../../src/redis/replay.js';

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

		it('coalesces publishes independently of message count', async () => {
			const counts = [];
			for (const size of [2, 12]) {
				const raw = makeClient();
				const meter = countRedisIO(raw);
				const platform = mockPlatform();
				const bus = createPubSubBus(meter.client);
				const observer = raw.duplicate();
				const received = [];
				observer.on('message', (_channel, value) => received.push(JSON.parse(value)));
				try {
					await raw.redis.get('io-self-check');
					await observer.subscribe('uws:pubsub');
					await bus.activate(platform);
					const wrapped = bus.wrap(platform);
					meter.reset();
					for (let i = 0; i < size; i++) wrapped.publish('room', 'value', i);
					await expect.poll(() => received.length).toBe(size);
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
				const observers = Array.from({ length: size }, () => raw.duplicate());
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
					await expect.poll(() => received.every(frames => frames.length === 1)).toBe(true);
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
					expect(meter.read()).toBe(2);
				} finally { await presence.clear('room'); presence.destroy(); await raw.quit(); }
			}
		});
	});
}
