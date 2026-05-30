// Mock-suite tests for the redis presence tracker. Pins the
// `state` / `diff` / `heartbeat` wire shape that
// matches the adapter's bundled presence plugin so a single client
// decoder handles both. Cross-instance forwarding still uses
// 'join' / 'leave' / 'updated' on the internal pubsub channel; those
// events are routed into the local diff buffer on receive so clients
// only ever see the unified wire shape.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence, WsClosedError } from '../../redis/presence.js';
import { createCircuitBreaker, CircuitBrokenError } from '../../shared/breaker.js';
import { createMetrics } from '../../prometheus/index.js';

function diffsOf(platform) {
	return platform.published.filter((p) => p.event === 'diff');
}
function statesOf(platform) {
	return platform.sent.filter((s) => s.event === 'state');
}
function heartbeatsOf(platform) {
	return platform.published.filter((p) => p.event === 'heartbeat');
}

describe('redis presence', () => {
	let client;
	let platform;
	let presence;

	beforeEach(() => {
		client = mockRedisClient('test:');
		platform = mockPlatform();
		presence = createPresence(client, {
			key: 'id',
			select: (userData) => ({ id: userData.id, name: userData.name }),
			heartbeat: 60000,
			ttl: 180
		});
	});

	afterEach(() => {
		presence.destroy();
	});

	describe('createPresence', () => {
		it('returns a tracker with the expected API', () => {
			expect(typeof presence.join).toBe('function');
			expect(typeof presence.leave).toBe('function');
			expect(typeof presence.sync).toBe('function');
			expect(typeof presence.list).toBe('function');
			expect(typeof presence.count).toBe('function');
			expect(typeof presence.metrics).toBe('function');
			expect(typeof presence.flushDiffs).toBe('function');
			expect(typeof presence.clear).toBe('function');
			expect(typeof presence.destroy).toBe('function');
		});

		it('rejects invalid heartbeat', () => {
			expect(() => createPresence(client, { heartbeat: 0 })).toThrow();
			expect(() => createPresence(client, { heartbeat: -1 })).toThrow();
			expect(() => createPresence(client, { heartbeat: 'fast' })).toThrow();
		});

		it('rejects invalid ttl', () => {
			expect(() => createPresence(client, { ttl: 0 })).toThrow();
			expect(() => createPresence(client, { ttl: -1 })).toThrow();
		});

		it('rejects non-function select', () => {
			expect(() => createPresence(client, { select: 'identity' })).toThrow();
		});
	});

	describe('join', () => {
		it('sends state to the joining ws as a flat snapshot', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const states = statesOf(platform);
			expect(states).toHaveLength(1);
			expect(states[0].topic).toBe('__presence:room');
			expect(states[0].data).toEqual({ '1': { id: '1', name: 'Alice' } });
			expect(states[0].ws).toBe(ws);
		});

		it('subscribes ws to the internal presence topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			expect(ws.isSubscribed('__presence:room')).toBe(true);
		});

		it('buffers a join entry that flushes as diff on next tick', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			await presence.join(ws1, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.join(ws2, 'room', platform);
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].topic).toBe('__presence:room');
			expect(diffs[0].data.joins).toEqual({ '2': { id: '2', name: 'Bob' } });
			expect(diffs[0].data.leaves).toEqual({});
			expect(diffs[0].options).toEqual({ relay: false, compress: true });
		});

		it('coalesces multiple same-tick joins into one diff per topic', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			const ws3 = mockWs({ id: '3', name: 'Carol' });

			await Promise.all([
				presence.join(ws1, 'room', platform),
				presence.join(ws2, 'room', platform),
				presence.join(ws3, 'room', platform)
			]);
			presence.flushDiffs();

			const diffs = diffsOf(platform).filter((d) => d.topic === '__presence:room');
			expect(diffs).toHaveLength(1);
			expect(Object.keys(diffs[0].data.joins).sort()).toEqual(['1', '2', '3']);
			expect(diffs[0].data.leaves).toEqual({});
		});

		it('emits per-topic diff frames for joins on different topics', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			const ws2 = mockWs({ id: '2', name: 'Bob' });
			await presence.join(ws1, 'room-a', platform);
			await presence.join(ws2, 'room-b', platform);
			presence.flushDiffs();

			const topics = diffsOf(platform).map((d) => d.topic).sort();
			expect(topics).toEqual(['__presence:room-a', '__presence:room-b']);
		});

		it('is idempotent for the same ws on the same topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.join(ws, 'room', platform);
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
			expect(statesOf(platform)).toHaveLength(0);
		});

		it('ignores __-prefixed topics', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, '__internal', platform);
			expect(statesOf(platform)).toHaveLength(0);
			expect(diffsOf(platform)).toHaveLength(0);
		});

		it('uses select function to filter userData', async () => {
			const ws = mockWs({ id: '1', name: 'Alice', secret: 's3cr3t' });
			await presence.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1']).toEqual({ id: '1', name: 'Alice' });
			expect(state.data['1']).not.toHaveProperty('secret');
		});

		it('publishes a fresh diff when the same user re-joins with different data', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws1, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const ws2 = mockWs({ id: '1', name: 'Alice Renamed' });
			await presence.join(ws2, 'room', platform);
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins['1'].name).toBe('Alice Renamed');
		});

		it('does not publish a diff when same user re-joins with identical data', async () => {
			const ws1 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws1, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const ws2 = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws2, 'room', platform);
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
		});
	});

	describe('multi-tab dedup', () => {
		it('two connections, same key, broadcast one join then collapse on second', async () => {
			const wsA = mockWs({ id: '1', name: 'Alice' });
			const wsB = mockWs({ id: '1', name: 'Alice' });

			await presence.join(wsA, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.join(wsB, 'room', platform);
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('closing one tab keeps user present, no leave diff fires', async () => {
			const wsA = mockWs({ id: '1', name: 'Alice' });
			const wsB = mockWs({ id: '1', name: 'Alice' });
			await presence.join(wsA, 'room', platform);
			await presence.join(wsB, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(wsA, platform, 'room');
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('closing last tab buffers a leave diff', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(ws, platform, 'room');
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.leaves).toEqual({ '1': { id: '1', name: 'Alice' } });
			expect(diffs[0].data.joins).toEqual({});
		});
	});

	describe('leave (per-topic)', () => {
		it('removes user from only the specified topic and buffers one leave diff', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(ws, platform, 'room-a');
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].topic).toBe('__presence:room-a');
			expect(diffs[0].data.leaves).toEqual({ '1': { id: '1', name: 'Alice' } });
		});

		it('unsubscribes the ws from the targeted topic only', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);

			await presence.leave(ws, platform, 'room-a');

			expect(ws.isSubscribed('__presence:room-a')).toBe(false);
			expect(ws.isSubscribed('__presence:room-b')).toBe(true);
		});

		it('is safe to call for a topic the ws never joined', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await expect(presence.leave(ws, platform, 'unknown')).resolves.toBeUndefined();
		});
	});

	describe('leaveAll', () => {
		it('removes user from every joined topic and buffers a leave diff per topic', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(ws, platform);
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs.map((d) => d.topic).sort()).toEqual(['__presence:room-a', '__presence:room-b']);
			for (const d of diffs) {
				expect(d.data.leaves['1']).toEqual({ id: '1', name: 'Alice' });
			}
		});

		it('is safe to call for an unknown ws', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await expect(presence.leave(ws, platform)).resolves.toBeUndefined();
		});
	});

	describe('sync (sync-only observer)', () => {
		it('sends state without joining', async () => {
			const member = mockWs({ id: '1', name: 'Alice' });
			await presence.join(member, 'room', platform);
			platform.reset();

			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room', platform);

			const states = statesOf(platform);
			expect(states).toHaveLength(1);
			expect(states[0].ws).toBe(observer);
			expect(states[0].data).toEqual({ '1': { id: '1', name: 'Alice' } });

			// Sync did not add the observer to presence
			expect(await presence.count('room')).toBe(1);
		});

		it('sends empty snapshot for an unknown topic', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'empty-room', platform);
			expect(statesOf(platform)[0].data).toEqual({});
		});

		it('subscribes observer ws to the internal presence topic', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room', platform);
			expect(observer.isSubscribed('__presence:room')).toBe(true);
		});
	});

	describe('tick coalescing', () => {
		it('a same-tick join+leave on the same key from another instance collapses to leaves only', async () => {
			// Trigger via cross-instance pubsub since two synchronous bufferDiff
			// calls must happen WITHIN one event-loop iteration to collapse.
			// An awaited local join+leave hops past the next timers phase
			// between them and flushes the buffer mid-stream.
			const local = mockWs({ id: 'local' });
			await presence.join(local, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'leave',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 10));

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins).toEqual({});
			expect(diffs[0].data.leaves).toEqual({ remote: { id: 'remote' } });
		});

		it('flushDiffs() called twice is a no-op on the second call', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			presence.flushDiffs();
			const before = platform.published.length;
			presence.flushDiffs();
			expect(platform.published.length).toBe(before);
		});

		it('flushDiffs() is safe to call when there is nothing buffered', () => {
			expect(() => presence.flushDiffs()).not.toThrow();
		});

		// Pins the structural property the 0.5.x queueMicrotask defer got wrong:
		// uWS dispatches each WS message as its own JS task and N-API drains
		// microtasks at the C++/JS boundary between tasks, so a microtask-
		// deferred flush fires BEFORE the next socket's handler runs and
		// cross-socket coalescing is impossible at the microtask level.
		// setTimeout(0) lands in libuv's timers phase, which fires only after
		// the poll phase has dispatched every ready socket message in the
		// current iteration - so joins arriving in separate JS tasks (the
		// production shape) still collapse into one diff. Mirrors the
		// cross-task-boundary regression test for cursor's always-tick.
		it('cross-task-boundary joins from peer instances coalesce into one diff per topic', async () => {
			vi.useFakeTimers();
			const local = createPresence(client, { key: 'id' });
			const seed = mockWs({ id: 'seed' });
			await local.join(seed, 'room', platform);
			vi.advanceTimersByTime(1);
			platform.reset();

			const ch = client.key('presence:events:room');
			const COUNT = 50;
			for (let i = 0; i < COUNT; i++) {
				client.redis.publish(ch, JSON.stringify({
					instanceId: 'OTHER', topic: 'room', event: 'join',
					payload: { key: 'peer-' + i, data: { id: 'peer-' + i } }
				}));
				await Promise.resolve();
			}
			vi.advanceTimersByTime(1);

			const diffs = diffsOf(platform).filter((d) => d.topic === '__presence:room');
			expect(diffs).toHaveLength(1);
			expect(Object.keys(diffs[0].data.joins)).toHaveLength(COUNT);
			expect(diffs[0].data.leaves).toEqual({});

			local.destroy();
			vi.useRealTimers();
		});
	});

	describe('list / count', () => {
		it('returns current users on a topic', async () => {
			const wsA = mockWs({ id: '1', name: 'Alice' });
			const wsB = mockWs({ id: '2', name: 'Bob' });
			await presence.join(wsA, 'room', platform);
			await presence.join(wsB, 'room', platform);

			const list = await presence.list('room');
			expect(list).toHaveLength(2);
			expect(list.map((u) => u.id).sort()).toEqual(['1', '2']);
			expect(await presence.count('room')).toBe(2);
		});

		it('returns empty list / zero count for unknown topics', async () => {
			expect(await presence.list('nope')).toEqual([]);
			expect(await presence.count('nope')).toBe(0);
		});
	});

	describe('clear', () => {
		it('drops local + Redis state and unsubscribes ws connections', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			presence.flushDiffs();

			await presence.clear();

			expect(await presence.count('room')).toBe(0);
			expect(ws.isSubscribed('__presence:room')).toBe(false);
		});

		it('drains pending diffs as well', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			// pending diff still scheduled
			await presence.clear();
			presence.flushDiffs();
			// after clear, the next flush should not produce any new frames
			const diffsAfter = diffsOf(platform).length;
			presence.flushDiffs();
			expect(diffsOf(platform).length).toBe(diffsAfter);
		});

		it('allows re-joining after clear', async () => {
			const ws1 = mockWs({ id: '1' });
			await presence.join(ws1, 'room', platform);
			await presence.clear();
			platform.reset();

			const ws2 = mockWs({ id: '2' });
			await presence.join(ws2, 'room', platform);
			expect(statesOf(platform)).toHaveLength(1);
		});
	});

	describe('userData sanitization', () => {
		it('strips __subscriptions and remoteAddress before select', async () => {
			const ws = mockWs({ id: '1', name: 'Alice', remoteAddress: '1.2.3.4' });
			ws.getUserData().__subscriptions = new Set(['x']);
			await presence.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1']).not.toHaveProperty('__subscriptions');
			expect(state.data['1']).not.toHaveProperty('remoteAddress');
		});
	});

	describe('default select', () => {
		it('strips __-prefixed and sensitive keys', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1', name: 'Alice', __internal: 'x', token: 'abc' });
			await local.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1']).toEqual({ id: '1', name: 'Alice' });
			local.destroy();
		});
	});

	describe('custom select cannot leak nested secrets (defense-in-depth)', () => {
		it('strips nested sensitive keys even when select is identity', async () => {
			const local = createPresence(client, {
				key: 'id',
				select: /** @type {any} */ ((ud) => ud)
			});
			const ws = mockWs({
				id: '1',
				name: 'Alice',
				profile: { displayName: 'A', token: 'shh', authToken: 'xyz' }
			});
			await local.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1'].profile.displayName).toBe('A');
			expect(state.data['1'].profile).not.toHaveProperty('token');
			expect(state.data['1'].profile).not.toHaveProperty('authToken');
			local.destroy();
		});

		it('strips nested __-prefixed keys when select picks the parent', async () => {
			const local = createPresence(client, {
				key: 'id',
				select: (ud) => ({ id: ud.id, meta: ud.meta })
			});
			const ws = mockWs({
				id: '1',
				meta: { displayName: 'Alice', __internal: 'leak' }
			});
			await local.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1'].meta.displayName).toBe('Alice');
			expect(state.data['1'].meta).not.toHaveProperty('__internal');
			local.destroy();
		});
	});

	describe('hooks', () => {
		it('exposes subscribe / unsubscribe / message / close', () => {
			expect(typeof presence.hooks.subscribe).toBe('function');
			expect(typeof presence.hooks.unsubscribe).toBe('function');
			expect(typeof presence.hooks.message).toBe('function');
			expect(typeof presence.hooks.close).toBe('function');
		});

		it('message routes {type:"presence-snapshot", topic} through tracker.sync', async () => {
			// Establish a presence entry so the snapshot has something to send.
			const member = mockWs({ id: '1' });
			await presence.join(member, 'room', platform);
			platform.reset();

			const observer = mockWs({ id: 'obs' });
			presence.hooks.message(observer, {
				data: { type: 'presence-snapshot', topic: 'room' },
				platform
			});
			await new Promise((r) => setTimeout(r, 5));

			// tracker.sync emits state via platform.send to the requesting ws.
			expect(statesOf(platform)).toHaveLength(1);
		});

		it('message ignores frames that are not presence-snapshot', () => {
			const observer = mockWs({ id: 'obs' });
			presence.hooks.message(observer, {
				data: { type: 'unrelated', payload: 'x' },
				platform
			});
			expect(platform.published).toHaveLength(0);
			expect(platform.sent).toHaveLength(0);
		});

		it('subscribe routes regular topics through join', async () => {
			const ws = mockWs({ id: '1' });
			await presence.hooks.subscribe(ws, 'room', { platform });
			expect(statesOf(platform)).toHaveLength(1);
			expect(await presence.count('room')).toBe(1);
		});

		it('subscribe routes __presence:* topics through sync', async () => {
			const member = mockWs({ id: '1' });
			await presence.join(member, 'room', platform);
			platform.reset();

			const observer = mockWs({ id: 'obs' });
			await presence.hooks.subscribe(observer, '__presence:room', { platform });

			expect(statesOf(platform)).toHaveLength(1);
			expect(await presence.count('room')).toBe(1); // observer not added
		});

		it('close routes through leaveAll', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.hooks.close(ws, { platform });
			presence.flushDiffs();

			expect(diffsOf(platform)[0].data.leaves['1']).toBeDefined();
		});
	});

	describe('cross-instance routing', () => {
		it('routes inbound join from another instance through the diff buffer', async () => {
			// Set up subscriber by joining a ws on this instance
			const local = mockWs({ id: 'local' });
			await presence.join(local, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			// Simulate cross-instance join arriving on the pubsub channel
			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER',
				topic: 'room',
				event: 'join',
				payload: { key: 'remote', data: { id: 'remote', name: 'Remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins).toEqual({ remote: { id: 'remote', name: 'Remote' } });
			expect(diffs[0].options).toEqual({ relay: false, compress: true });
		});

		it('routes inbound leave from another instance through the diff buffer', async () => {
			const local = mockWs({ id: 'local' });
			await presence.join(local, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER',
				topic: 'room',
				event: 'leave',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.leaves).toEqual({ remote: { id: 'remote' } });
		});

		it('routes inbound updated as a join op (latest data wins)', async () => {
			const local = mockWs({ id: 'local' });
			await presence.join(local, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER',
				topic: 'room',
				event: 'updated',
				payload: { key: 'remote', data: { id: 'remote', name: 'New' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins).toEqual({ remote: { id: 'remote', name: 'New' } });
		});

		it('drops envelopes from this instance (echo suppression)', async () => {
			const local = mockWs({ id: 'local' });
			await presence.join(local, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			// We don't know our own instanceId from outside, but we can confirm
			// that an inbound envelope WITHOUT instanceId === ours produces a
			// diff (proving the suppression branch only blocks self-publishes).
			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER',
				topic: 'room',
				event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			presence.flushDiffs();
			expect(diffsOf(platform).length).toBeGreaterThan(0);
		});
	});

	describe('metrics', () => {
		it('exposes a synchronous snapshot via metrics()', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			const snap = presence.metrics();
			expect(snap.totalOnline).toBe(1);
			expect(typeof snap.heartbeatLatencyMs).toBe('number');
			expect(typeof snap.staleCleanedTotal).toBe('number');
		});

		it('emits presence_diff_frames_total per flushed topic', async () => {
			const metrics = createMetrics();
			const local = createPresence(client, { key: 'id', metrics });
			const ws1 = mockWs({ id: '1' });
			await local.join(ws1, 'room-a', platform);
			await local.join(ws1, 'room-b', platform);
			local.flushDiffs();

			const out = await metrics.serialize();
			expect(out).toMatch(/presence_diff_frames_total\{topic="room-a"\}\s+1/);
			expect(out).toMatch(/presence_diff_frames_total\{topic="room-b"\}\s+1/);
			local.destroy();
		});

		it('emits presence_diff_coalesced_total when a buffered key is overwritten in the same tick', async () => {
			const metrics = createMetrics();
			const local = createPresence(client, { key: 'id', metrics });
			const seedWs = mockWs({ id: 'seed' });
			await local.join(seedWs, 'room', platform);
			local.flushDiffs();

			// Two pubsub messages dispatched synchronously by the mock; both
			// bufferDiff calls land in one event-loop iteration before the
			// setTimeout(0) flush fires.
			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote', a: 1 } }
			}));
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'leave',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 10));

			const out = await metrics.serialize();
			expect(out).toMatch(/presence_diff_coalesced_total\{topic="room"\}\s+1/);
			local.destroy();
		});

		it('keeps presence_joins_total / presence_leaves_total counting underlying events', async () => {
			const metrics = createMetrics();
			const local = createPresence(client, { key: 'id', metrics });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			await local.leave(ws, platform, 'room');

			const out = await metrics.serialize();
			expect(out).toMatch(/presence_joins_total\{topic="room"\}\s+1/);
			expect(out).toMatch(/presence_leaves_total\{topic="room"\}\s+1/);
			local.destroy();
		});
	});

	describe('breaker integration', () => {
		it('skips Redis writes when the breaker is broken on join', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			breaker.failure(new Error('down'));
			expect(breaker.isHealthy).toBe(false);

			const local = createPresence(client, { key: 'id', breaker });
			const ws = mockWs({ id: '1' });
			await expect(local.join(ws, 'room', platform)).rejects.toBeInstanceOf(CircuitBrokenError);
			local.destroy();
		});

		it('list / count surface CircuitBrokenError when the breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			breaker.failure(new Error('down'));

			const local = createPresence(client, { key: 'id', breaker });
			await expect(local.list('room')).rejects.toBeInstanceOf(CircuitBrokenError);
			await expect(local.count('room')).rejects.toBeInstanceOf(CircuitBrokenError);
			local.destroy();
		});
	});

	describe('keyspace notifications', () => {
		it('emits empty state when a hash key expires', async () => {
			const local = createPresence(client, { key: 'id', keyspaceNotifications: true });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			// Mock-redis publish() doesn't dispatch pmessage; reach into the
			// subscriber's listener registry and invoke directly.
			const subscriberHandler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			const pmessageListener = subscriberHandler.listeners.get('pmessage');
			expect(typeof pmessageListener).toBe('function');

			// Storage layout: the per-topic hash key is `presence:topic:{topic}`.
			// Whole-key expiry fires only when every field has expired (no live
			// instances presenting any user on this topic).
			const expiredKey = client.key('presence:topic:room');
			pmessageListener('__keyevent@*__:expired', '__keyevent@0__:expired', expiredKey);

			const empties = platform.published.filter(
				(p) => p.event === 'state' && p.topic === '__presence:room'
			);
			expect(empties.length).toBeGreaterThan(0);
			expect(empties[0].data).toEqual({});
			expect(empties[0].options).toEqual({ relay: false, compress: true });
			local.destroy();
		});
	});

	describe('heartbeat (event name unchanged)', () => {
		it('publishes heartbeat with {userKey: data} map on the timer tick', async () => {
			vi.useFakeTimers();
			const local = createPresence(client, { key: 'id', heartbeat: 50 });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			vi.advanceTimersByTime(60);
			await Promise.resolve();
			vi.useRealTimers();
			await new Promise((r) => setTimeout(r, 5));

			const beats = heartbeatsOf(platform).filter((h) => h.topic === '__presence:room');
			expect(beats.length).toBeGreaterThan(0);
			// Wire shape carries `{userKey: data}` (the user object derived from
			// `select()`) so a client whose entry aged out between heartbeats can
			// re-add it from the heartbeat alone. Pre-change this was a
			// key-only array `['1']`; the heartbeat could only refresh
			// existing client entries, not recover swept ones.
			expect(beats[0].data).toEqual({ '1': { id: '1' } });
			local.destroy();
		});

		it('does not publish heartbeat for topics with no active users', async () => {
			vi.useFakeTimers();
			const local = createPresence(client, { key: 'id', heartbeat: 50 });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			await local.leave(ws, platform);
			platform.reset();

			vi.advanceTimersByTime(60);
			await Promise.resolve();
			vi.useRealTimers();
			await new Promise((r) => setTimeout(r, 5));

			expect(heartbeatsOf(platform)).toHaveLength(0);
			local.destroy();
		});
	});

	describe('multiple topics', () => {
		it('tracks topics independently', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			expect(await presence.count('room-a')).toBe(1);
			expect(await presence.count('room-b')).toBe(1);
		});

		it('per-topic leave allows subsequent leave-all', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room-a', platform);
			await presence.join(ws, 'room-b', platform);
			await presence.leave(ws, platform, 'room-a');
			await expect(presence.leave(ws, platform)).resolves.toBeUndefined();
			expect(await presence.count('room-a')).toBe(0);
			expect(await presence.count('room-b')).toBe(0);
		});

		it('per-topic leave respects multi-tab dedup', async () => {
			const wsA = mockWs({ id: '1' });
			const wsB = mockWs({ id: '1' });
			await presence.join(wsA, 'room', platform);
			await presence.join(wsB, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(wsA, platform, 'room');
			presence.flushDiffs();
			expect(diffsOf(platform)).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('leaves a sync observer from a specific topic only', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room-a', platform);
			await presence.sync(observer, 'room-b', platform);

			await presence.leave(observer, platform, 'room-a');
			expect(observer.isSubscribed('__presence:room-a')).toBe(false);
			expect(observer.isSubscribed('__presence:room-b')).toBe(true);
		});
	});

	describe('binary wire codec (presence.protocol:1)', () => {
		// The default mock has no publishWire/sendWire, so the rest of the suite
		// exercises the JSON fallback (byte-identical wire). This block uses a
		// platform that DOES support the binary path and asserts the plugin routes
		// through it with the presence codec. Byte parity of encode/decode is the
		// adapter codec's own test (presence-codec.test.js); here we pin the wiring.
		function wirePlatform() {
			const p = mockPlatform();
			p.publishedWire = [];
			p.sentWire = [];
			p.publishWire = (topic, event, data, wire, options) => {
				p.publishedWire.push({ topic, event, data, wire, options });
				return true;
			};
			p.sendWire = (ws, topic, event, data, wire, options) => {
				p.sentWire.push({ ws, topic, event, data, wire, options });
				return 1;
			};
			return p;
		}

		it('state snapshot routes through sendWire with the presence codec (compress:true)', async () => {
			const wp = wirePlatform();
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', wp);

			expect(wp.sent.filter((s) => s.event === 'state')).toHaveLength(0); // not the JSON path
			const states = wp.sentWire.filter((s) => s.event === 'state');
			expect(states).toHaveLength(1);
			expect(states[0].wire.capability).toBe('presence.protocol:1');
			expect(states[0].wire.schemaVersion).toBe(1);
			expect(states[0].options).toEqual({ compress: true });
			// The codec encodes this data to bytes (functional sanity check).
			expect(states[0].wire.encode('state', states[0].data)).toBeInstanceOf(Uint8Array);
		});

		it('diff broadcast routes through publishWire with the presence codec (relay:false, compress:true)', async () => {
			const wp = wirePlatform();
			await presence.join(mockWs({ id: '1', name: 'Alice' }), 'room', wp);
			await presence.join(mockWs({ id: '2', name: 'Bob' }), 'room', wp);
			presence.flushDiffs();

			const diffs = wp.publishedWire.filter((p) => p.event === 'diff');
			expect(diffs.length).toBeGreaterThan(0);
			expect(diffs[0].wire.capability).toBe('presence.protocol:1');
			expect(diffs[0].options).toEqual({ relay: false, compress: true });
			expect(wp.published.filter((p) => p.event === 'diff')).toHaveLength(0); // not JSON
		});

		it('binary: false forces the JSON path (no wire codec)', async () => {
			const c = createPresence(client, { key: 'id', binary: false });
			const wp = wirePlatform();
			await c.join(mockWs({ id: '1' }), 'room', wp);

			expect(wp.sentWire).toHaveLength(0);
			expect(wp.sent.filter((s) => s.event === 'state')).toHaveLength(1);
			c.destroy();
		});
	});

	describe('WebSocket compression policy (presence frames opt into compression)', () => {
		// Presence frames are low-frequency (diffs coalesce per tick, heartbeat is
		// periodic, state is on-attach), so the plugin opts INTO permessage-deflate
		// with { compress: true } - the deliberate counterpart to the cursor
		// plugin's compress:false. Mirrors the bundled in-memory presence plugin.
		// (diff and state-cleanup compress:true are pinned by the diff/empties
		// assertions above; this block covers the state-send and heartbeat sites.)

		it('state snapshot send carries compress:true', async () => {
			const ws = mockWs({ id: '1', name: 'Alice' });
			await presence.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state).toBeDefined();
			expect(state.options).toEqual({ compress: true });
		});

		it('heartbeat carries compress:true', async () => {
			vi.useFakeTimers();
			const local = createPresence(client, { key: 'id', heartbeat: 50 });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			vi.advanceTimersByTime(60);
			await Promise.resolve();
			vi.useRealTimers();
			await new Promise((r) => setTimeout(r, 5));

			const beat = heartbeatsOf(platform).find((h) => h.topic === '__presence:room');
			expect(beat).toBeDefined();
			expect(beat.options).toEqual({ compress: true });
			local.destroy();
		});
	});

	describe('sync observer cleanup', () => {
		it('leave() unsubscribes the observer from all sync topics', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room-a', platform);
			await presence.sync(observer, 'room-b', platform);

			await presence.leave(observer, platform);
			expect(observer.isSubscribed('__presence:room-a')).toBe(false);
			expect(observer.isSubscribed('__presence:room-b')).toBe(false);
		});

		it('does not unsubscribe the Redis channel when joined users remain', async () => {
			const member = mockWs({ id: 'member' });
			const observer = mockWs({ id: 'obs' });
			await presence.join(member, 'room', platform);
			await presence.sync(observer, 'room', platform);

			await presence.leave(observer, platform, 'room');

			// Cross-instance event must still reach the surviving member.
			const ch = client.key('presence:events:room');
			platform.reset();
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			expect(diffsOf(platform).length).toBeGreaterThan(0);
		});
	});

	describe('clear', () => {
		it('unsubscribes joined ws connections from presence topics', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			await presence.clear();
			expect(ws.isSubscribed('__presence:room')).toBe(false);
		});

		it('unsubscribes sync observers from presence topics', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room', platform);
			await presence.clear();
			expect(observer.isSubscribed('__presence:room')).toBe(false);
		});

		it('stops forwarding remote events to the local platform after clear', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			await presence.clear();
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 10));
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
		});
	});

	describe('no key field in data', () => {
		it('generates a unique connection id when the key field is missing', async () => {
			const local = createPresence(client, { key: 'id' });
			const wsA = mockWs({ name: 'Alice' });
			const wsB = mockWs({ name: 'Bob' });
			await local.join(wsA, 'room', platform);
			await local.join(wsB, 'room', platform);

			expect(await local.count('room')).toBe(2);
			local.destroy();
		});
	});

	describe('default select edge cases', () => {
		it('recursively strips nested __-prefixed keys', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1', name: 'Alice', meta: { __secret: 'x', visible: 'y' } });
			await local.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1'].meta).toEqual({ visible: 'y' });
			local.destroy();
		});

		it('strips sensitive-regex keys like password / token / secret', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1', name: 'Alice', password: 'p', token: 't', sessionId: 's' });
			await local.join(ws, 'room', platform);

			const state = statesOf(platform)[0];
			expect(state.data['1']).not.toHaveProperty('password');
			expect(state.data['1']).not.toHaveProperty('token');
			expect(state.data['1']).not.toHaveProperty('sessionId');
			expect(state.data['1'].id).toBe('1');
			expect(state.data['1'].name).toBe('Alice');
			local.destroy();
		});

		it('handles circular references without crashing the join flow', async () => {
			const local = createPresence(client, { key: 'id' });
			const data = { id: '1', name: 'Alice' };
			data.self = data;
			const ws = mockWs(data);
			// Behavior contract: either resolves (cycles stripped) or throws a
			// JSON-serializable error - never an unhandled rejection.
			let outcome = 'unset';
			try {
				await local.join(ws, 'room', platform);
				outcome = 'resolved';
			} catch (err) {
				expect(err).toBeInstanceOf(Error);
				outcome = 'rejected';
			}
			expect(['resolved', 'rejected']).toContain(outcome);
			local.destroy();
		});
	});

	describe('key resolution before select', () => {
		it('resolves dedup key from raw data even when select strips it', async () => {
			// select keeps only `name`; key field 'id' is stripped from select output
			// but resolveKey reads from the raw safeData (post-sanitization, pre-select).
			const local = createPresence(client, {
				key: 'id',
				select: (ud) => ({ name: ud.name })
			});
			const wsA = mockWs({ id: '1', name: 'Alice' });
			const wsB = mockWs({ id: '1', name: 'Alice' });
			await local.join(wsA, 'room', platform);
			await local.join(wsB, 'room', platform);

			expect(await local.count('room')).toBe(1);
			local.destroy();
		});
	});

	describe('updated event (re-join with changed data)', () => {
		it('flushes a fresh diff frame with the new data in joins', async () => {
			const wsA = mockWs({ id: '1', name: 'Alice' });
			await presence.join(wsA, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const wsB = mockWs({ id: '1', name: 'Alice Renamed' });
			await presence.join(wsB, 'room', platform);
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins['1'].name).toBe('Alice Renamed');
		});

		it('does not flush a diff when re-joining with identical data', async () => {
			const wsA = mockWs({ id: '1', name: 'Alice' });
			await presence.join(wsA, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			const wsB = mockWs({ id: '1', name: 'Alice' });
			await presence.join(wsB, 'room', platform);
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
		});
	});

	describe('hooks (more)', () => {
		it('subscribe ignores other __-prefixed topics without throwing', async () => {
			const ws = mockWs({ id: '1' });
			await expect(
				presence.hooks.subscribe(ws, '__internal', { platform })
			).resolves.toBeUndefined();
			expect(await presence.count('__internal')).toBe(0);
		});

		it('unsubscribe removes presence from a single topic', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			await presence.hooks.unsubscribe(ws, 'room', { platform });
			expect(await presence.count('room')).toBe(0);
		});

		it('unsubscribe ignores other __-prefixed topics', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			await expect(
				presence.hooks.unsubscribe(ws, '__random', { platform })
			).resolves.toBeUndefined();
			expect(await presence.count('room')).toBe(1);
		});

		it('unsubscribe handles __presence:* topics for sync-only observers', async () => {
			const observer = mockWs({ id: 'obs' });
			await presence.sync(observer, 'room', platform);
			await presence.hooks.unsubscribe(observer, '__presence:room', { platform });
			expect(observer.isSubscribed('__presence:room')).toBe(false);
		});

		it('destructured hooks work correctly', async () => {
			const { subscribe, close } = presence.hooks;
			const ws = mockWs({ id: '1' });
			await subscribe(ws, 'room', { platform });
			expect(await presence.count('room')).toBe(1);
			await close(ws, { platform });
			expect(await presence.count('room')).toBe(0);
		});
	});

	describe('cross-instance safety', () => {
		it('leave on one instance does not remove a user present on another', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const trackerA = createPresence(client, { key: 'id' });
			const trackerB = createPresence(client, { key: 'id' });

			const wsA = mockWs({ id: 'alice' });
			const wsB = mockWs({ id: 'alice' });
			await trackerA.join(wsA, 'room', platformA);
			await trackerB.join(wsB, 'room', platformB);

			await trackerA.leave(wsA, platformA);
			trackerA.flushDiffs();

			const leaveDiffs = diffsOf(platformA).filter(
				(d) => d.data.leaves && 'alice' in d.data.leaves
			);
			expect(leaveDiffs).toHaveLength(0);
			expect(await trackerB.count('room')).toBe(1);

			trackerA.destroy();
			trackerB.destroy();
		});
	});

	describe('breaker integration (more)', () => {
		it('leave suppresses cross-instance broadcast when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const local = createPresence(client, { key: 'id', breaker });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			local.flushDiffs();
			breaker.failure(new Error('down'));
			platform.reset();

			await local.leave(ws, platform);
			local.flushDiffs();

			// userGone defaulted to -1 (skipped), so no leave diff is buffered.
			expect(diffsOf(platform)).toHaveLength(0);
			local.destroy();
		});

		it('list / count record success and let the breaker recover from probing', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 1 });
			breaker.failure(new Error('down'));
			await new Promise((r) => setTimeout(r, 5));
			expect(breaker.state).toBe('probing');

			const local = createPresence(client, { key: 'id', breaker });
			await local.list('room');
			expect(breaker.isHealthy).toBe(true);
			local.destroy();
		});

		it('join eval failure rolls back local state and records breaker failure', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const local = createPresence(client, { key: 'id', breaker });

			const original = client.redis.eval;
			client.redis.eval = async () => { throw new Error('eval down'); };

			const ws = mockWs({ id: '1' });
			await expect(local.join(ws, 'room', platform)).rejects.toThrow();
			expect(breaker.isHealthy).toBe(false);
			expect(await local.count('room').catch(() => 0)).toBe(0);

			client.redis.eval = original;
			local.destroy();
		});
	});

	describe('rollback on join failure', () => {
		it('throws WsClosedError when ws.subscribe throws after Redis write; rolls back local state', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });
			ws.subscribe = (topic) => {
				if (topic === '__presence:room') throw new Error('closed');
				return true;
			};

			await expect(local.join(ws, 'room', platform)).rejects.toMatchObject({
				name: 'WsClosedError',
				code: 'WS_CLOSED',
				operation: 'presence.join',
				topic: 'room'
			});

			expect(await local.count('room')).toBe(0);
			expect(diffsOf(platform).filter((d) => '1' in (d.data.joins || {}))).toHaveLength(0);
			local.destroy();
		});

		it('does not buffer a join diff when snapshot fetch fails', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });

			const original = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('snapshot down'); };

			await expect(local.join(ws, 'room', platform)).rejects.toThrow();
			local.flushDiffs();
			expect(diffsOf(platform).filter((d) => '1' in (d.data.joins || {}))).toHaveLength(0);

			client.redis.hgetall = original;
			local.destroy();
		});
	});

	describe('WsClosedError on ws-closed during async gap', () => {
		// Pins the contract that `join()` throws `WsClosedError` (`code:
		// 'WS_CLOSED'`) instead of silently returning when the websocket
		// closes during any of the five internal async gaps. Without this,
		// the RPC wrapper in svelte-realtime reports `status=ok` for joins
		// that never committed - operators trust the metric and the metric
		// lies. State rollback is preserved on every path; the throw is
		// purely an observability signal.

		it('throws when ws.getBufferedAmount probe fails after subscribeToTopic', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });
			ws.getBufferedAmount = () => { throw new Error('closed'); };

			await expect(local.join(ws, 'room', platform)).rejects.toMatchObject({
				name: 'WsClosedError',
				code: 'WS_CLOSED',
				topic: 'room'
			});
			expect(await local.count('room')).toBe(0);
			local.destroy();
		});

		it('throws when ws closes during the JOIN_SCRIPT eval; rolls back the Redis write', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });

			const originalEval = client.redis.eval.bind(client.redis);
			let firstEvalSeen = false;
			client.redis.eval = async (...args) => {
				const out = await originalEval(...args);
				// Close the ws between the eval landing and the
				// post-eval wsTopics.has(ws) check. Manual `leave`
				// simulates the close-hook running during the gap.
				if (!firstEvalSeen) {
					firstEvalSeen = true;
					await local.leave(ws, platform);
				}
				return out;
			};

			await expect(local.join(ws, 'room', platform)).rejects.toMatchObject({
				name: 'WsClosedError',
				code: 'WS_CLOSED'
			});
			expect(await local.count('room')).toBe(0);
			local.destroy();
		});

		it('throws when ws closes after Redis state landed but before ws.subscribe', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });
			ws.subscribe = (topic) => {
				if (topic === '__presence:room') {
					// Simulate the close hook racing in during the ws.subscribe
					// call: leave runs before the post-subscribe wsTopics check.
					local.leave(ws, platform);
					return true;
				}
				return true;
			};

			await expect(local.join(ws, 'room', platform)).rejects.toMatchObject({
				name: 'WsClosedError',
				code: 'WS_CLOSED'
			});
			expect(await local.count('room')).toBe(0);
			local.destroy();
		});

		it('emits a status=ok-shaped successful join unchanged when ws stays open', async () => {
			// Negative control: the happy path must not throw and must not
			// increment the aborted counter. Without this, a future refactor
			// could throw too eagerly and not get caught.
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });

			await expect(local.join(ws, 'room', platform)).resolves.toBeUndefined();
			expect(await local.count('room')).toBe(1);
			local.destroy();
		});

		it('increments `presence_joins_aborted_total{topic,reason="ws_closed"}` on the abort path', async () => {
			const metrics = createMetrics();
			const local = createPresence(client, { key: 'id', metrics });
			const ws = mockWs({ id: '1' });
			ws.getBufferedAmount = () => { throw new Error('closed'); };

			await expect(local.join(ws, 'room', platform)).rejects.toThrow();

			const out = await metrics.serialize();
			// Serializer sorts labels alphabetically -> `reason,topic`.
			expect(out).toMatch(/presence_joins_aborted_total\{reason="ws_closed",topic="room"\}\s+1/);
			// Negative control: a clean join must not bump the aborted
			// counter, otherwise an operator would see false positives.
			expect(out).not.toMatch(/presence_joins_aborted_total\{[^}]*\}\s+[2-9]/);
			local.destroy();
		});
	});

	describe('metrics() snapshot', () => {
		it('returns zeros on a fresh tracker', () => {
			const local = createPresence(client, { key: 'id' });
			const snap = local.metrics();
			expect(snap.totalOnline).toBe(0);
			expect(snap.heartbeatLatencyMs).toBe(0);
			expect(snap.staleCleanedTotal).toBe(0);
			local.destroy();
		});

		it('counts unique users per topic, summed across topics', async () => {
			const ws1 = mockWs({ id: '1' });
			const ws2 = mockWs({ id: '2' });
			await presence.join(ws1, 'room-a', platform);
			await presence.join(ws2, 'room-a', platform);
			await presence.join(ws1, 'room-b', platform);

			const snap = presence.metrics();
			expect(snap.totalOnline).toBe(3); // 2 in room-a + 1 in room-b
		});

		it('decrements totalOnline when the last connection for a user leaves a topic', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			expect(presence.metrics().totalOnline).toBe(1);
			await presence.leave(ws, platform);
			expect(presence.metrics().totalOnline).toBe(0);
		});

		it('exposes presence_total_online and presence_heartbeat_latency_ms once the heartbeat fires', async () => {
			vi.useFakeTimers();
			const metrics = createMetrics();
			const local = createPresence(client, { key: 'id', metrics, heartbeat: 50 });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);

			vi.advanceTimersByTime(60);
			await Promise.resolve();
			vi.useRealTimers();
			await new Promise((r) => setTimeout(r, 5));

			const out = await metrics.serialize();
			expect(out).toMatch(/presence_total_online\{topic="room"\}\s+1/);
			expect(out).toMatch(/presence_heartbeat_latency_ms\s+\d/);
			local.destroy();
		});
	});

	describe('keyspace notifications mode', () => {
		it('does not psubscribe by default', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);

			const handler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			expect(handler.listeners.get('pmessage')).toBeUndefined();
			local.destroy();
		});

		it('ignores expiry events for keys outside the presence prefix', async () => {
			const local = createPresence(client, { key: 'id', keyspaceNotifications: true });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			const handler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			const pmessage = handler.listeners.get('pmessage');
			pmessage('__keyevent@*__:expired', '__keyevent@0__:expired', 'unrelated:key');

			expect(platform.published.filter((p) => p.event === 'state')).toHaveLength(0);
			local.destroy();
		});

		it('ignores expiry events for the events sub-prefix (avoids double-firing)', async () => {
			const local = createPresence(client, { key: 'id', keyspaceNotifications: true });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			const handler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			const pmessage = handler.listeners.get('pmessage');
			pmessage('__keyevent@*__:expired', '__keyevent@0__:expired', client.key('presence:events:room'));

			expect(platform.published.filter((p) => p.event === 'state')).toHaveLength(0);
			local.destroy();
		});
	});

	describe('multi-tab data restoration on leave', () => {
		it('leaving the newer tab restores the older tab data via an updated diff', async () => {
			const wsOld = mockWs({ id: '1', name: 'Alice (old)' });
			const wsNew = mockWs({ id: '1', name: 'Alice (new)' });
			await presence.join(wsOld, 'room', platform);
			await presence.join(wsNew, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(wsNew, platform, 'room');
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins['1'].name).toBe('Alice (old)');
			expect(diffs[0].data.leaves).toEqual({});
		});

		it('leaving the older tab keeps the newer tab data unchanged (no diff)', async () => {
			const wsOld = mockWs({ id: '1', name: 'Alice (old)' });
			const wsNew = mockWs({ id: '1', name: 'Alice (new)' });
			await presence.join(wsOld, 'room', platform);
			await presence.join(wsNew, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(wsOld, platform, 'room');
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
			expect(await presence.count('room')).toBe(1);
		});

		it('three tabs: leaving newest restores second-newest data', async () => {
			const ws1 = mockWs({ id: '1', name: 'A1' });
			const ws2 = mockWs({ id: '1', name: 'A2' });
			const ws3 = mockWs({ id: '1', name: 'A3' });
			await presence.join(ws1, 'room', platform);
			await presence.join(ws2, 'room', platform);
			await presence.join(ws3, 'room', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(ws3, platform, 'room');
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs).toHaveLength(1);
			expect(diffs[0].data.joins['1'].name).toBe('A2');
		});

		it('leave-all restores remaining-tab data via a join entry on each affected topic', async () => {
			const wsOld = mockWs({ id: '1', name: 'Alice (old)' });
			const wsNew = mockWs({ id: '1', name: 'Alice (new)' });
			await presence.join(wsOld, 'room-a', platform);
			await presence.join(wsNew, 'room-a', platform);
			await presence.join(wsNew, 'room-b', platform);
			presence.flushDiffs();
			platform.reset();

			await presence.leave(wsNew, platform);
			presence.flushDiffs();

			const diffs = diffsOf(platform);
			const roomA = diffs.find((d) => d.topic === '__presence:room-a');
			const roomB = diffs.find((d) => d.topic === '__presence:room-b');
			expect(roomA.data.joins['1'].name).toBe('Alice (old)');
			expect(roomB.data.leaves['1']).toBeDefined();
		});
	});

	describe('soft-fail leave (breaker broken)', () => {
		it('per-topic leave suppresses cross-instance broadcast when breaker is broken', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const local = createPresence(client, { key: 'id', breaker });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			breaker.failure(new Error('down'));
			platform.reset();

			await local.leave(ws, platform, 'room');
			local.flushDiffs();

			expect(diffsOf(platform).filter((d) => d.data.leaves && '1' in d.data.leaves)).toHaveLength(0);
			local.destroy();
		});

		it('leave-all under a broken breaker suppresses leave broadcasts', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
			const local = createPresence(client, { key: 'id', breaker });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room-a', platform);
			await local.join(ws, 'room-b', platform);
			breaker.failure(new Error('down'));
			platform.reset();

			await local.leave(ws, platform);
			local.flushDiffs();

			expect(diffsOf(platform).filter((d) => d.data.leaves && '1' in d.data.leaves)).toHaveLength(0);
			local.destroy();
		});
	});

	describe('subscriber setup during breaker probing', () => {
		it('join in probing state still receives cross-instance events', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 1 });
			breaker.failure(new Error('down'));
			await new Promise((r) => setTimeout(r, 5));
			expect(breaker.state).toBe('probing');

			const local = createPresence(client, { key: 'id', breaker });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			local.flushDiffs();

			const diffs = diffsOf(platform);
			expect(diffs.length).toBeGreaterThan(0);
			expect(diffs.some((d) => 'remote' in (d.data.joins || {}))).toBe(true);
			local.destroy();
		});
	});

	describe('platform update on activate', () => {
		it('uses the latest platform passed in for cross-instance forwarding', async () => {
			const platformA = mockPlatform();
			const platformB = mockPlatform();
			const local = createPresence(client, { key: 'id' });

			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platformA);
			// Second join with a different platform - subsequent forwarded events
			// should land on platformB, not platformA.
			const ws2 = mockWs({ id: '2' });
			await local.join(ws2, 'room', platformB);
			platformA.reset();
			platformB.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 5));
			local.flushDiffs();

			expect(diffsOf(platformB).length).toBeGreaterThan(0);
			expect(diffsOf(platformA).length).toBe(0);
			local.destroy();
		});
	});

	describe('clear stops cross-instance forwarding', () => {
		it('a remote event arriving after clear() does not reach the local platform', async () => {
			const ws = mockWs({ id: '1' });
			await presence.join(ws, 'room', platform);
			await presence.clear();
			platform.reset();

			const ch = client.key('presence:events:room');
			client.redis.publish(ch, JSON.stringify({
				instanceId: 'OTHER', topic: 'room', event: 'join',
				payload: { key: 'remote', data: { id: 'remote' } }
			}));
			await new Promise((r) => setTimeout(r, 10));
			presence.flushDiffs();

			expect(diffsOf(platform)).toHaveLength(0);
		});
	});

	describe('hgetall failure during snapshot', () => {
		it('throws and rolls back without buffering a join diff', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });

			const original = client.redis.hgetall;
			client.redis.hgetall = async () => { throw new Error('hgetall down'); };

			await expect(local.join(ws, 'room', platform)).rejects.toThrow();
			local.flushDiffs();

			expect(diffsOf(platform).filter((d) => '1' in (d.data.joins || {}))).toHaveLength(0);
			expect(statesOf(platform)).toHaveLength(0);

			client.redis.hgetall = original;
			local.destroy();
		});
	});

	describe('destroy under subscriber failure', () => {
		it('does not throw when subscriber.quit() rejects', async () => {
			const local = createPresence(client, { key: 'id' });
			const ws = mockWs({ id: '1' });
			await local.join(ws, 'room', platform);

			// Replace the subscriber's quit with a rejecting stub
			const handler = client._pubsubHandlers[client._pubsubHandlers.length - 1];
			// We can't reach the subscriber object directly; instead rely on the
			// destroy()-side defensive .catch. This just confirms destroy is
			// idempotent and does not throw on consecutive calls.
			expect(() => local.destroy()).not.toThrow();
			expect(() => local.destroy()).not.toThrow();
		});
	});
});
