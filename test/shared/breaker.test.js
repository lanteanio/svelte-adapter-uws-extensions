import { describe, it, expect, beforeEach, vi } from 'vitest';
import { createCircuitBreaker, CircuitBrokenError } from '../../shared/breaker.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';
import { createPresence } from '../../redis/presence.js';
import { createReplay } from '../../redis/replay.js';
import { createRateLimit } from '../../redis/ratelimit.js';
import { createGroup } from '../../redis/groups.js';
import { createCursor } from '../../redis/cursor.js';

function mockWs(userData = {}) {
	const subscriptions = new Set();
	return {
		getUserData: () => ({ ...userData, __subscriptions: subscriptions }),
		subscribe(topic) { subscriptions.add(topic); },
		unsubscribe(topic) { subscriptions.delete(topic); },
		isSubscribed(topic) { return subscriptions.has(topic); },
		getBufferedAmount() { return 0; }
	};
}

describe('circuit breaker', () => {
	describe('state machine', () => {
		it('starts healthy', () => {
			const breaker = createCircuitBreaker();
			expect(breaker.state).toBe('healthy');
			expect(breaker.isHealthy).toBe(true);
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('stays healthy below the failure threshold', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 3 });
			breaker.failure();
			breaker.failure();
			expect(breaker.state).toBe('healthy');
			expect(breaker.failures).toBe(2);
			breaker.destroy();
		});

		it('transitions to broken when threshold is reached', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 3 });
			breaker.failure();
			breaker.failure();
			breaker.failure();
			expect(breaker.state).toBe('broken');
			expect(breaker.isHealthy).toBe(false);
			breaker.destroy();
		});

		it('guard() throws CircuitBrokenError when broken', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();
			expect(() => breaker.guard()).toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('guard() does not throw when healthy', () => {
			const breaker = createCircuitBreaker();
			expect(() => breaker.guard()).not.toThrow();
			breaker.destroy();
		});

		it('transitions to probing after resetTimeout', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			breaker.failure();
			expect(breaker.state).toBe('broken');

			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');
			breaker.destroy();
		});

		it('allows one probe request through in probing state', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			// First guard() should succeed (the probe)
			expect(() => breaker.guard()).not.toThrow();
			// Second guard() should throw (probe in flight)
			expect(() => breaker.guard()).toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('transitions from probing to healthy on success', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			breaker.guard(); // consume the probe
			breaker.success();

			expect(breaker.state).toBe('healthy');
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('transitions from probing back to broken on failure', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			breaker.guard(); // consume the probe
			breaker.failure();

			expect(breaker.state).toBe('broken');
			breaker.destroy();
		});

		it('success() resets failure count in healthy state', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			breaker.failure();
			breaker.failure();
			expect(breaker.failures).toBe(2);
			breaker.success();
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('reset() forces back to healthy', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();
			expect(breaker.state).toBe('broken');
			breaker.reset();
			expect(breaker.state).toBe('healthy');
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('calls onStateChange callback', () => {
			const transitions = [];
			const breaker = createCircuitBreaker({
				failureThreshold: 1,
				onStateChange: (from, to) => transitions.push({ from, to })
			});
			breaker.failure();
			expect(transitions).toEqual([{ from: 'healthy', to: 'broken' }]);
			breaker.reset();
			expect(transitions).toEqual([
				{ from: 'healthy', to: 'broken' },
				{ from: 'broken', to: 'healthy' }
			]);
			breaker.destroy();
		});

		it('validates options', () => {
			expect(() => createCircuitBreaker({ failureThreshold: 0 })).toThrow();
			expect(() => createCircuitBreaker({ failureThreshold: -1 })).toThrow();
			expect(() => createCircuitBreaker({ failureThreshold: 1.5 })).toThrow();
			expect(() => createCircuitBreaker({ resetTimeout: -1 })).toThrow();
		});
	});

	describe('CircuitBrokenError', () => {
		it('is an instance of Error', () => {
			const err = new CircuitBrokenError();
			expect(err).toBeInstanceOf(Error);
			expect(err.name).toBe('CircuitBrokenError');
			expect(err.message).toContain('circuit breaker');
		});
	});

	describe('extension integration', () => {
		let client, platform;

		beforeEach(() => {
			client = mockRedisClient();
			platform = mockPlatform();
		});

		it('ratelimit: guard() throws on consume when broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure(); // break it

			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000,
				breaker
			});
			const ws = mockWs({ remoteAddress: '1.2.3.4' });

			await expect(limiter.consume(ws)).rejects.toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('ratelimit: tracks success on successful consume', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000,
				breaker
			});
			const ws = mockWs({ remoteAddress: '1.2.3.4' });

			await limiter.consume(ws);
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('replay: guard() throws on publish when broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const replay = createReplay(client, { breaker });
			await expect(replay.publish(platform, 'chat', 'msg', {})).rejects.toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('replay: tracks success on successful publish', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const replay = createReplay(client, { breaker });

			await replay.publish(platform, 'chat', 'msg', { text: 'hi' });
			expect(breaker.failures).toBe(0);
			breaker.destroy();
		});

		it('presence: guard() throws on join when broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const presence = createPresence(client, { breaker, key: 'id' });
			const ws = mockWs({ id: 'u1' });

			await expect(presence.join(ws, 'room', platform)).rejects.toThrow(CircuitBrokenError);
			presence.destroy();
			breaker.destroy();
		});

		it('groups: guard() throws on join when broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const group = createGroup(client, 'lobby', { breaker });
			const ws = mockWs({ id: 'u1' });

			await expect(group.join(ws, platform)).rejects.toThrow(CircuitBrokenError);
			group.destroy();
			breaker.destroy();
		});

		it('extensions work normally without a breaker', async () => {
			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000
			});
			const ws = mockWs({ remoteAddress: '1.2.3.4' });
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
		});

		it('shared breaker trips from failures across extensions', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });

			const replay = createReplay(client, { breaker });
			const limiter = createRateLimit(client, {
				points: 10,
				interval: 60000,
				breaker
			});

			// Sabotage the Redis client to make eval fail
			const origEval = client.redis.eval;
			client.redis.eval = async () => { throw new Error('connection lost'); };

			// First failure from replay
			await replay.publish(platform, 'chat', 'msg', {}).catch(() => {});
			expect(breaker.failures).toBe(1);
			expect(breaker.state).toBe('healthy');

			// Second failure from ratelimit trips the breaker
			const ws = mockWs({ remoteAddress: '1.2.3.4' });
			await limiter.consume(ws).catch(() => {});
			expect(breaker.state).toBe('broken');

			// Both extensions now fail fast
			await expect(replay.publish(platform, 'a', 'b', {})).rejects.toThrow(CircuitBrokenError);
			await expect(limiter.consume(ws)).rejects.toThrow(CircuitBrokenError);

			// Restore and reset
			client.redis.eval = origEval;
			breaker.reset();

			// Should work again
			const result = await limiter.consume(ws);
			expect(result.allowed).toBe(true);
			breaker.destroy();
		});

		it('pubsub: skips relay when breaker is not healthy', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const bus = createPubSubBus(client, { breaker });
			const wrapped = bus.wrap(platform);

			const publishCalls = [];
			client.redis.publish = async (ch, msg) => { publishCalls.push(ch); };

			wrapped.publish('chat', 'msg', { text: 'hi' });
			await Promise.resolve(); // flush microtask

			// Should have skipped the Redis publish
			expect(publishCalls).toHaveLength(0);
			// But local publish should still work
			expect(platform.published).toHaveLength(1);

			breaker.destroy();
		});

		it('cursor: skips Redis pipeline when breaker is not healthy', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const cursor = createCursor(client, { throttle: 0, breaker });
			const ws = mockWs({ id: 'u1' });

			// Should not throw -- local broadcast still works, Redis is skipped
			cursor.update(ws, 'doc', { x: 10 }, platform);
			expect(platform.published.length).toBeGreaterThan(0);

			cursor.destroy();
			breaker.destroy();
		});

		it('replay: replay() does not double-guard when probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const replay = createReplay(client, { breaker });

			await replay.publish(platform, 'chat', 'msg', { id: 1 });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			const ws = mockWs();
			await replay.replay(ws, 'chat', 0, platform);
			expect(breaker.state).toBe('healthy');

			breaker.destroy();
		});

		it('replay: seq() recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const replay = createReplay(client, { breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			await replay.seq('chat');
			expect(breaker.state).toBe('healthy');

			breaker.destroy();
		});

		it('replay: clear() recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const replay = createReplay(client, { breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));

			await replay.clear();
			expect(breaker.state).toBe('healthy');

			breaker.destroy();
		});

		it('ratelimit: reset() recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const limiter = createRateLimit(client, { points: 10, interval: 60000, breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));

			await limiter.reset('test-key');
			expect(breaker.state).toBe('healthy');

			breaker.destroy();
		});

		it('ratelimit: clear() recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const limiter = createRateLimit(client, { points: 10, interval: 60000, breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));

			await limiter.clear();
			expect(breaker.state).toBe('healthy');

			breaker.destroy();
		});

		it('groups: count() recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const group = createGroup(client, 'bp', { breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));

			await group.count();
			expect(breaker.state).toBe('healthy');

			group.destroy();
			breaker.destroy();
		});

		it('presence: clear() guards and recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const presence = createPresence(client, { breaker, key: 'id' });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			await presence.clear();
			expect(breaker.state).toBe('healthy');

			presence.destroy();
			breaker.destroy();
		});

		it('cursor: clear() guards and recovers breaker from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const cursor = createCursor(client, { throttle: 0, breaker });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			await cursor.clear();
			expect(breaker.state).toBe('healthy');

			cursor.destroy();
			breaker.destroy();
		});

		it('groups: close() does not leave breaker healthy if del fails', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const group = createGroup(client, 'close-del', { breaker, memberTtl: 120 });

			const ws = mockWs({ id: 'u1' });
			await group.join(ws, platform);

			const origDel = client.redis.del;
			client.redis.del = async () => { throw new Error('del failed'); };

			await expect(group.close(platform)).rejects.toThrow('del failed');
			expect(breaker.failures).toBe(1);

			client.redis.del = origDel;
			group.destroy();
			breaker.destroy();
		});

		it('postgres replay: clear() does not double-guard on fresh instance', async () => {
			const pgClient = {
				pool: {},
				async query() { return { rows: [], rowCount: 0 }; },
				async end() {}
			};
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const { createReplay: createPgReplay } = await import('../../postgres/replay.js');
			const replay = createPgReplay(pgClient, { breaker, cleanupInterval: 0 });

			breaker.failure();
			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			await replay.clear();
			expect(breaker.state).toBe('healthy');

			replay.destroy();
			breaker.destroy();
		});

		it('postgres replay: publish() blocks DDL on broken breaker', async () => {
			const pgClient = {
				pool: {},
				_queries: [],
				async query(textOrObj) {
					const sql = typeof textOrObj === 'object' ? textOrObj.text : textOrObj;
					pgClient._queries.push(sql);
					return { rows: [{ seq: '1' }], rowCount: 1 };
				},
				async end() {}
			};
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure();

			const { createReplay: createPgReplay } = await import('../../postgres/replay.js');
			const replay = createPgReplay(pgClient, { breaker, cleanupInterval: 0 });

			await expect(replay.publish(platform, 'chat', 'msg', {})).rejects.toThrow(CircuitBrokenError);
			expect(pgClient._queries).toHaveLength(0);

			replay.destroy();
			breaker.destroy();
		});

		it('presence: clear() does not unsubscribe ws when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const presence = createPresence(client, { breaker, key: 'id' });

			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			expect(ws.isSubscribed('__presence:room')).toBe(true);

			breaker.failure();

			await expect(presence.clear()).rejects.toThrow(CircuitBrokenError);
			expect(ws.isSubscribed('__presence:room')).toBe(true);

			breaker.reset();
			expect(await presence.count('room')).toBe(1);

			presence.destroy();
			breaker.destroy();
		});

		it('cursor: clear() does not wipe local state when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const cursor = createCursor(client, { throttle: 0, breaker });

			const ws = mockWs({ id: '1' });
			cursor.update(ws, 'room', { x: 10 }, platform);

			breaker.failure();

			await expect(cursor.clear()).rejects.toThrow(CircuitBrokenError);

			breaker.reset();
			const list = await cursor.list('room');
			expect(list).toHaveLength(1);

			cursor.destroy();
			breaker.destroy();
		});

		it('pubsub: relay metrics only count on successful publish', async () => {
			const metrics = (await import('../../prometheus/index.js')).createMetrics();
			const failClient = mockRedisClient();
			failClient.redis.publish = async () => { throw new Error('publish failed'); };

			const bus = createPubSubBus(failClient, { metrics });
			const wrapped = bus.wrap(platform);

			wrapped.publish('chat', 'msg', { text: 'a' });
			await new Promise((r) => setTimeout(r, 20));

			const output = metrics.serialize();
			const sampleLines = output.split('\n').filter((l) => !l.startsWith('#') && l.includes('pubsub_messages_relayed_total'));
			expect(sampleLines).toHaveLength(0);
		});

		it('postgres replay: DDL failure records breaker failure on fresh instance', async () => {
			let queryCount = 0;
			const pgClient = {
				pool: {},
				async query() {
					queryCount++;
					throw new Error('DDL failed');
				},
				async end() {}
			};
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const { createReplay: createPgReplay } = await import('../../postgres/replay.js');
			const replay = createPgReplay(pgClient, { breaker, cleanupInterval: 0 });

			await expect(replay.publish(platform, 'chat', 'msg', {})).rejects.toThrow('DDL failed');
			expect(breaker.failures).toBe(1);

			replay.destroy();
			breaker.destroy();
		});
	});
});
