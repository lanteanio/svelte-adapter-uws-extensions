import { describe, it, expect, beforeEach, vi } from 'vitest';
import { createCircuitBreaker, CircuitBrokenError, withBreaker } from '../../src/shared/breaker.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createPresence } from '../../src/redis/presence.js';
import { createReplay, ReplayStorageError } from '../../src/redis/replay.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { createGroup } from '../../src/redis/groups.js';
import { createCursor } from '../../src/redis/cursor.js';

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
	describe('per-key isolation', () => {
		it('one key can break without tripping another key or the default', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });
			breaker.failure(undefined, 'a');
			breaker.failure(undefined, 'a');
			expect(breaker.stateOf('a')).toBe('broken');
			expect(() => breaker.guard('a')).toThrow(CircuitBrokenError);
			// Key 'b' and the default '' key are untouched.
			expect(breaker.stateOf('b')).toBe('healthy');
			expect(breaker.state).toBe('healthy'); // default key
			expect(() => breaker.guard('b')).not.toThrow();
			expect(() => breaker.guard()).not.toThrow();
			breaker.destroy();
		});

		it('reset(key) clears only that key', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure(undefined, 'a');
			breaker.failure(undefined, 'b');
			expect(breaker.stateOf('a')).toBe('broken');
			expect(breaker.stateOf('b')).toBe('broken');
			breaker.reset('a');
			expect(breaker.stateOf('a')).toBe('healthy');
			expect(breaker.stateOf('b')).toBe('broken'); // untouched
			breaker.destroy();
		});

		it('withBreaker(b, fn, key) partitions failures by key', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const boom = () => Promise.reject(new Error('x'));
			await expect(withBreaker(breaker, boom, 'a')).rejects.toThrow('x');
			expect(breaker.stateOf('a')).toBe('broken');
			expect(breaker.stateOf('b')).toBe('healthy');
			// A guarded op on the healthy key 'b' runs; on the broken key 'a' it fails fast.
			await expect(withBreaker(breaker, async () => 'ok', 'b')).resolves.toBe('ok');
			await expect(withBreaker(breaker, async () => 'ok', 'a')).rejects.toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('no key -> the single global breaker (byte-identical)', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });
			breaker.failure();
			breaker.failure();
			expect(breaker.state).toBe('broken');
			expect(breaker.failures).toBe(2);
			expect(breaker.stateOf('')).toBe('broken'); // same slot as the no-key default
			breaker.destroy();
		});

		it('caps the per-key state map, evicting the oldest non-default key', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.failure(undefined, 'k0'); // k0 is the oldest non-default key, now broken
			expect(breaker.stateOf('k0')).toBe('broken');
			// Touch enough fresh keys to exceed MAX_BREAKER_KEYS (10000); the oldest
			// non-default key (k0) is evicted and recreates healthy on next access, so
			// the map cannot grow without bound from a keyed caller.
			for (let i = 1; i <= 10001; i++) breaker.stateOf('k' + i);
			expect(breaker.stateOf('k0')).toBe('healthy');
			breaker.destroy();
		});
	});

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

		it('probeConcurrency admits that many concurrent probes, then throws', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50, probeConcurrency: 3 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			expect(breaker.state).toBe('probing');

			expect(() => breaker.guard()).not.toThrow();
			expect(() => breaker.guard()).not.toThrow();
			expect(() => breaker.guard()).not.toThrow();
			// The probe budget is spent.
			expect(() => breaker.guard()).toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('probeConcurrency: the first probe success closes the circuit', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50, probeConcurrency: 2 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			breaker.guard();
			breaker.guard();
			breaker.success(); // first probe reply wins

			expect(breaker.state).toBe('healthy');
			expect(() => breaker.guard()).not.toThrow();
			breaker.destroy();
		});

		it('probeConcurrency: a probe failure re-opens and zeroes the remaining budget', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50, probeConcurrency: 2 });
			breaker.failure();

			await new Promise((r) => setTimeout(r, 80));
			breaker.guard(); // one probe out, one budget slot left
			breaker.failure();

			expect(breaker.state).toBe('broken');
			// The leftover budget slot must not leak a request through while broken.
			expect(() => breaker.guard()).toThrow(CircuitBrokenError);
			breaker.destroy();
		});

		it('validates probeConcurrency', () => {
			expect(() => createCircuitBreaker({ probeConcurrency: 0 })).toThrow('positive integer');
			expect(() => createCircuitBreaker({ probeConcurrency: 1.5 })).toThrow('positive integer');
			expect(() => createCircuitBreaker({ probeConcurrency: 'two' })).toThrow('positive integer');
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

		it('failures counter caps at failureThreshold during sustained outages', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 3 });
			for (let i = 0; i < 10000; i++) breaker.failure();
			expect(breaker.state).toBe('broken');
			expect(breaker.failures).toBe(3);
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

	describe('subscribe', () => {
		it('returns an unsubscribe function', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const unsubscribe = breaker.subscribe(() => {});
			expect(typeof unsubscribe).toBe('function');
			unsubscribe();
			breaker.destroy();
		});

		it('delivers transitions to multiple subscribers', () => {
			const a = [];
			const b = [];
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.subscribe((from, to) => a.push({ from, to }));
			breaker.subscribe((from, to) => b.push({ from, to }));
			breaker.failure();
			expect(a).toEqual([{ from: 'healthy', to: 'broken' }]);
			expect(b).toEqual([{ from: 'healthy', to: 'broken' }]);
			breaker.destroy();
		});

		it('unsubscribed listener stops receiving transitions', () => {
			const a = [];
			const b = [];
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			const unsubscribe = breaker.subscribe((from, to) => a.push({ from, to }));
			breaker.subscribe((from, to) => b.push({ from, to }));
			breaker.failure();
			unsubscribe();
			breaker.reset();
			expect(a).toHaveLength(1);
			expect(b).toHaveLength(2);
			breaker.destroy();
		});

		it('coexists with the constructor onStateChange', () => {
			const a = [];
			const b = [];
			const breaker = createCircuitBreaker({
				failureThreshold: 1,
				onStateChange: (from, to) => a.push({ from, to })
			});
			breaker.subscribe((from, to) => b.push({ from, to }));
			breaker.failure();
			expect(a).toEqual([{ from: 'healthy', to: 'broken' }]);
			expect(b).toEqual([{ from: 'healthy', to: 'broken' }]);
			breaker.destroy();
		});

		it('a throwing listener does not affect the others', () => {
			const a = [];
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			breaker.subscribe(() => { throw new Error('boom'); });
			breaker.subscribe((from, to) => a.push({ from, to }));
			breaker.failure();
			expect(a).toEqual([{ from: 'healthy', to: 'broken' }]);
			breaker.destroy();
		});

		it('rejects a non-function handler', () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			expect(() => breaker.subscribe('not a function')).toThrow('must be a function');
			expect(() => breaker.subscribe(null)).toThrow('must be a function');
			breaker.destroy();
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
			const err = await replay.publish(platform, 'chat', 'msg', {}).catch((e) => e);
			expect(err).toBeInstanceOf(ReplayStorageError);
			expect(err.cause).toBeInstanceOf(CircuitBrokenError);
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
			const replayErr = await replay.publish(platform, 'a', 'b', {}).catch((e) => e);
			expect(replayErr).toBeInstanceOf(ReplayStorageError);
			expect(replayErr.cause).toBeInstanceOf(CircuitBrokenError);
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

			// Should not throw - local broadcast still works, Redis is skipped
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
			// The drift guard verifies columns after ensureTable, so the double
			// answers the information_schema lookup per table (main table + the
			// seq counter); everything else stays an empty result (this test is
			// about the breaker).
			const colsByTable = {
				svti_replay: ['svti_replay_id', 'topic', 'seq', 'event', 'data', 'created_at'],
				svti_replay_seq: ['topic', 'seq', 'epoch']
			};
			// verifyTableColumns calls query(textString, [table]); DDL calls pass a
			// bare string or a {text,values} object - normalize both.
			const answer = (textOrObj, valsArg) => {
				const text = typeof textOrObj === 'string' ? textOrObj : textOrObj?.text;
				const values = (typeof textOrObj === 'string' ? valsArg : textOrObj?.values) || [];
				if (typeof text === 'string' && text.includes('information_schema.columns')) {
					const cols = (colsByTable[values[0]] || []).map((column_name) => ({ column_name }));
					return { rows: cols, rowCount: cols.length };
				}
				return { rows: [], rowCount: 0 };
			};
			const pgClient = {
				pool: {
					connect: async () => ({
						query: async (t, v) => answer(t, v),
						release: () => {}
					})
				},
				async query(t, v) { return answer(t, v); },
				async end() {}
			};
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
			const { createReplay: createPgReplay } = await import('../../src/postgres/replay.js');
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

			const { createReplay: createPgReplay, ReplayStorageError: PgReplayStorageError } = await import('../../src/postgres/replay.js');
			const replay = createPgReplay(pgClient, { breaker, cleanupInterval: 0 });

			const err = await replay.publish(platform, 'chat', 'msg', {}).catch((e) => e);
			expect(err).toBeInstanceOf(PgReplayStorageError);
			expect(err.cause).toBeInstanceOf(CircuitBrokenError);
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
			// topicThrottle: 0 forces the immediate-broadcast path so
			// `queueSnapshot` populates the in-memory pending map
			// synchronously. The default (16ms) defers the snapshot to
			// the next tick; this test asserts state preservation under
			// breaker rejection and shouldn't depend on tick timing.
			const cursor = createCursor(client, { throttle: 0, topicThrottle: 0, breaker });

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
			const metrics = (await import('../../src/prometheus/index.js')).createMetrics();
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
			const { createReplay: createPgReplay } = await import('../../src/postgres/replay.js');
			const replay = createPgReplay(pgClient, { breaker, cleanupInterval: 0 });

			await expect(replay.publish(platform, 'chat', 'msg', {})).rejects.toThrow('DDL failed');
			expect(breaker.failures).toBe(1);

			replay.destroy();
			breaker.destroy();
		});
	});
});
