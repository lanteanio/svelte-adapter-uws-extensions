// A multi-worker adapter (workerData.totalWorkers > 1) refuses every publish
// that would draw its per-worker topic counter: the call has to say
// `{ seq: false }`, or carry a positive integer seq with `relay: false`. This
// file runs as one worker of such a runtime - node:worker_threads reports two
// workers, so every module reads the topology the adapter reads - and the
// platform below refuses exactly what the adapter refuses, using the adapter's
// own policy functions. Each module's real publish paths run against it, and a
// refusal a module swallows (the presence diff flush, the cursor remove buffer)
// is still recorded, so a missing declaration cannot pass silently.

import { describe, it, expect, vi, afterEach } from 'vitest';
import * as policy from '../../node_modules/svelte-adapter-uws/src/runtime/handler/cluster-sequence-policy.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPgClient } from '../../src/testing/mock-pg.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { MULTI_WORKER_RUNTIME } from '../../src/shared/sequence-authority.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createGroup } from '../../src/redis/groups.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createReplay as createRedisReplay } from '../../src/redis/replay.js';
import { createReplay as createPgReplay } from '../../src/postgres/replay.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

vi.mock('node:worker_threads', async (importOriginal) => {
	const actual = await importOriginal();
	return { ...actual, workerData: { totalWorkers: 2 } };
});

const CLUSTER = { totalWorkers: 2 };
const sharedCodec = {
	capability: 'test.shared:1',
	schemaVersion: 1,
	shared: true,
	encode: (event, data) => new TextEncoder().encode(JSON.stringify({ event, data }))
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function until(check, timeoutMs = 2000) {
	const start = Date.now();
	while (!check()) {
		if (Date.now() - start > timeoutMs) throw new Error('condition never held');
		await sleep(5);
	}
}

/**
 * A mock platform with the multi-worker adapter's sequence rule on every
 * gated lane. Every refusal is thrown, as the adapter throws it, and recorded.
 * With `wire`, it also has the binary publishWire lane, which presence and
 * cursor then prefer; its frames are recorded apart, in `publishedWire`.
 */
function clusteredPlatform({ wire = false } = {}) {
	const p = mockPlatform();
	p.refused = [];
	p.publishedWire = [];
	const check = (fn) => {
		try { fn(); } catch (err) { p.refused.push(err); throw err; }
	};
	const publish = p.publish;
	p.publish = (topic, event, data, options) => {
		check(() => policy.assertClusterSequenceAuthority(options, CLUSTER));
		return publish(topic, event, data, options);
	};
	if (wire) {
		p.publishWire = (topic, event, data, codec, options) => {
			check(() => policy.assertClusterSequenceAuthority(options, CLUSTER));
			p.publishedWire.push({ topic, event, data, wire: codec, options });
			return true;
		};
		p.registerWireCodec = () => {};
	}
	const batch = p.batch;
	p.batch = (messages) => {
		for (const m of messages) check(() => policy.assertClusterSequenceAuthority(m.options, CLUSTER));
		return batch(messages);
	};
	const publishBatched = p.publishBatched;
	p.publishBatched = (messages) => {
		for (const m of messages) check(() => policy.assertClusterSequenceAuthorityValues(m.options?.seq, m.options?.relay, CLUSTER));
		return publishBatched(messages);
	};
	return p;
}

afterEach(() => vi.restoreAllMocks());

describe('the harness', () => {
	it('reads a multi-worker runtime', () => {
		expect(MULTI_WORKER_RUNTIME).toBe(true);
	});

	it('refuses what the adapter refuses and accepts what it accepts', () => {
		const p = clusteredPlatform({ wire: true });
		expect(() => p.publish('t', 'e', 1)).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		expect(() => p.publish('t', 'e', 1, { relay: false })).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		expect(() => p.publish('t', 'e', 1, { seq: 3 })).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		expect(() => p.publishBatched([{ topic: 't', event: 'e', data: 1 }])).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		expect(() => p.batch([{ topic: 't', event: 'e', data: 1, options: { seq: true } }])).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		expect(p.refused).toHaveLength(5);
		p.refused.length = 0;
		p.publish('t', 'e', 1, { seq: false });
		p.publish('t', 'e', 1, { seq: 3, relay: false });
		p.publishWire('t', 'e', 1, {}, { seq: false });
		expect(p.refused).toEqual([]);
	});
});

describe('presence on a multi-worker runtime', () => {
	it('sends the state, diffs, heartbeats and a peer\'s diff without a refusal', async () => {
		const client = mockRedisClient('cw-presence:');
		const a = clusteredPlatform();
		const b = clusteredPlatform();
		const presenceA = createPresence(client, { select: (u) => ({ id: u.id }), heartbeat: 20 });
		const presenceB = createPresence(client, { select: (u) => ({ id: u.id }), heartbeat: 20 });
		try {
			await presenceB.join(mockWs({ id: 'bob' }), 'room', b);
			const alice = mockWs({ id: 'alice' });
			await presenceA.join(alice, 'room', a);
			await until(() => a.published.some((f) => f.event === 'diff'));
			await until(() => b.published.some((f) => f.event === 'diff' && f.data.joins?.alice));
			await until(() => a.published.some((f) => f.event === 'heartbeat'));
			await presenceA.leave(alice, a);
			await until(() => b.published.some((f) => f.event === 'diff' && f.data.leaves?.alice));
		} finally {
			presenceA.destroy();
			presenceB.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		const frames = [...a.published, ...b.published].filter((f) => f.topic === '__presence:room');
		expect(frames.length).toBeGreaterThan(0);
		for (const f of frames) expect(f.options.seq).toBe(false);
		// A diff is this worker's own: every other worker hears it on the Redis relay.
		for (const f of frames.filter((x) => x.event === 'diff')) expect(f.options.relay).toBe(false);
	});

	it('sends binary frames through publishWire without a refusal', async () => {
		const client = mockRedisClient('cw-presence-wire:');
		const a = clusteredPlatform({ wire: true });
		const presence = createPresence(client, { select: (u) => ({ id: u.id }) });
		try {
			await presence.join(mockWs({ id: 'alice' }), 'room', a);
			await until(() => a.publishedWire.some((f) => f.event === 'diff'));
		} finally {
			presence.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(a.publishedWire.find((f) => f.event === 'diff').options).toEqual({ relay: false, compress: true, seq: false });
	});
});

describe('cursor on a multi-worker runtime', () => {
	it('sends join, moves, a peer\'s moves and removes without a refusal', async () => {
		const client = mockRedisClient('cw-cursor:');
		const a = clusteredPlatform();
		const b = clusteredPlatform();
		const cursorA = createCursor(client, { throttle: 0 });
		const cursorB = createCursor(client, { throttle: 0 });
		try {
			const bob = mockWs({ id: 'bob' });
			cursorB.update(bob, 'canvas', { x: 0, y: 0 }, b);
			const alice = mockWs({ id: 'alice' });
			cursorA.update(alice, 'canvas', { x: 1, y: 2 }, a);
			await until(() => b.published.some((f) => f.event === 'join' && f.data.user?.id === 'alice'));
			cursorA.update(alice, 'canvas', { x: 3, y: 4 }, a);
			await until(() => b.published.some((f) => (f.event === 'update' || f.event === 'bulk') && JSON.stringify(f.data).includes('"x":3')));
			await cursorA.remove(alice, a);
			await until(() => a.published.some((f) => f.event === 'remove'));
			await until(() => b.published.some((f) => f.event === 'remove'));
		} finally {
			cursorA.destroy();
			cursorB.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		const frames = [...a.published, ...b.published].filter((f) => f.topic === '__cursor:canvas');
		expect(frames.length).toBeGreaterThan(0);
		for (const f of frames) expect(f.options).toMatchObject({ relay: false, seq: false });
	});

	it('reaches a worker whose clients only watch', async () => {
		const client = mockRedisClient('cw-cursor-watch:');
		const a = clusteredPlatform();
		const b = clusteredPlatform();
		const cursorA = createCursor(client, { throttle: 0 });
		const cursorB = createCursor(client, { throttle: 0 });
		try {
			// B's only connection attaches and never moves.
			await cursorB.attach(mockWs({ id: 'viewer' }), 'canvas', b);
			const alice = mockWs({ id: 'alice' });
			cursorA.update(alice, 'canvas', { x: 5, y: 6 }, a);
			await until(() => b.published.some((f) => f.event === 'join' && f.data.user?.id === 'alice'));
			await until(() => b.published.some((f) => (f.event === 'update' || f.event === 'bulk') && JSON.stringify(f.data).includes('"x":5')));
			await cursorA.remove(alice, a);
			await until(() => b.published.some((f) => f.event === 'remove'));
		} finally {
			cursorA.destroy();
			cursorB.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
	});

	it('sends binary frames through publishWire without a refusal', async () => {
		const client = mockRedisClient('cw-cursor-wire:');
		const a = clusteredPlatform({ wire: true });
		const cursor = createCursor(client, { throttle: 0 });
		try {
			cursor.update(mockWs({ id: 'alice' }), 'canvas', { x: 1, y: 2 }, a);
			await until(() => a.publishedWire.some((f) => f.event === 'update'));
		} finally {
			cursor.destroy();
		}
		expect(a.refused).toEqual([]);
		for (const f of a.publishedWire) expect(f.options).toEqual({ relay: false, seq: false });
	});
});

describe('groups on a multi-worker runtime', () => {
	it('sends join, broadcast, a peer\'s events, leave and close without a refusal', async () => {
		const client = mockRedisClient('cw-groups:');
		const a = clusteredPlatform();
		const b = clusteredPlatform();
		const groupA = createGroup(client, 'lobby');
		const groupB = createGroup(client, 'lobby');
		try {
			await groupB.join(mockWs({ id: 'bob' }), b);
			const alice = mockWs({ id: 'alice' });
			await groupA.join(alice, a);
			await groupA.publish(a, 'chat', { text: 'hi' });
			await until(() => b.published.some((f) => f.event === 'chat'));
			await groupA.leave(alice, a);
			await groupA.close(a);
			await until(() => b.published.some((f) => f.event === 'close'));
		} finally {
			groupA.destroy();
			groupB.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		const frames = [...a.published, ...b.published].filter((f) => f.topic === '__group:lobby');
		for (const event of ['join', 'chat', 'leave', 'close']) {
			expect(frames.some((f) => f.event === event), event).toBe(true);
		}
		for (const f of frames) expect(f.options).toEqual({ relay: false, seq: false });
	});
});

const BUSES = [
	['pubsub bus', (client, opts) => createPubSubBus(client, opts), async () => {},
		(client, envelope) => client.redis.publish('uws:pubsub', JSON.stringify(envelope))],
	['sharded bus', (client, opts) => createShardedBus(client, opts), async (bus, topic) => { await bus.follow(topic); },
		(client, envelope) => client.redis.spublish('uws:sharded:chat', JSON.stringify(envelope))]
];

// Each bus with and without a tracing provider: wrap() picks the traced or the
// untraced publish paths once, from the platform it wraps.
describe.each(BUSES.flatMap(([name, ...rest]) => [[name, ...rest, false], [name + ', traced', ...rest, true]]))('the %s on a multi-worker runtime', (_name, createBus, follow, inject, traced) => {
	async function pair(prefix, { bus: busOptions, wire } = {}) {
		const client = mockRedisClient(prefix);
		const busA = createBus(client, busOptions);
		const busB = createBus(client, busOptions);
		const a = clusteredPlatform({ wire });
		const b = clusteredPlatform({ wire });
		a.trace.enabled = traced;
		b.trace.enabled = traced;
		await busA.activate(a);
		await busB.activate(b);
		await follow(busB, 'chat');
		await follow(busB, 'feed');
		return { busA, busB, a, b, wrapped: busA.wrap(a) };
	}

	it('publishes seq: false locally without the in-process relay, and the peer delivers it unsequenced', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus1:');
		try {
			expect(wrapped.publish('chat', 'msg', { n: 1 }, { seq: false })).toBe(true);
			await until(() => b.published.length === 1);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		// The bus reaches the sibling workers itself, so the adapter's relay would deliver twice.
		expect(a.published[0].options).toEqual({ seq: false, relay: false });
		expect(b.published[0].options).toEqual({ relay: false, seq: false });
	});

	it('carries an authoritative seq to the peer with relay: false on both sides', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus2:');
		try {
			wrapped.publish('chat', 'msg', { n: 1 }, { seq: 41 });
			await until(() => b.published.length === 1);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		expect(a.published[0].options).toEqual({ seq: 41, relay: false });
		expect(b.published[0].options).toEqual({ relay: false, seq: 41 });
	});

	it('refuses an undeclared publish the way the adapter does, and relays nothing', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus3:');
		try {
			expect(() => wrapped.publish('chat', 'msg', { n: 1 })).toThrow(policy.CLUSTER_SEQUENCE_ERROR);
			wrapped.publish('chat', 'after', { n: 2 }, { seq: false });
			await until(() => b.published.length > 0);
			await sleep(10);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(b.published.map((f) => f.event)).toEqual(['after']);
	});

	it('carries each batch entry\'s seq and de-herd window to the peer', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus4:');
		try {
			wrapped.publishBatched([
				{ topic: 'chat', event: 'a', data: 1, options: { seq: false } },
				{ topic: 'chat', event: 'b', data: 2, options: { seq: 7, jitterMs: 50 } }
			]);
			wrapped.batch([
				{ topic: 'feed', event: 'c', data: 3, options: { seq: false } },
				{ topic: 'feed', event: 'd', data: 4, options: { seq: false, relay: false } }
			]);
			await until(() => b.published.length === 3);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		expect(a.published.map((f) => f.options)).toEqual([
			{ seq: false, relay: false },
			{ seq: 7, jitterMs: 50, relay: false },
			{ seq: false, relay: false },
			{ seq: false, relay: false }
		]);
		const byEvent = Object.fromEntries(b.published.map((f) => [f.event, f.options]));
		expect(byEvent).toEqual({
			a: { relay: false, seq: false },
			b: { relay: false, seq: 7, jitterMs: 50 },
			c: { relay: false, seq: false }
		});
	});

	it('forwards options through topic() helpers, as the adapter\'s do', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus5:');
		try {
			expect(wrapped.topic('chat').created({ id: 1 }, { seq: false })).toBe(true);
			expect(wrapped.topic('chat').increment(undefined, { seq: false })).toBe(true);
			await until(() => b.published.length === 2);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		expect(a.published.map((f) => [f.event, f.data])).toEqual([['created', { id: 1 }], ['increment', 1]]);
	});

	it('leaves the adapter relay on for a frame the other workers\' buses drop', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus7:');
		const options = { seq: false };
		try {
			wrapped.publish('__signal:alice', 'ping', 1, options);
			wrapped.publish('chat', 'after', 2, { seq: false });
			await until(() => b.published.some((f) => f.event === 'after'));
			await sleep(10);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		// Their receivers drop a `__` topic this bus does not admit, so only the
		// adapter's own relay can reach them.
		expect(a.published[0].options).toBe(options);
		expect(b.published.map((f) => f.event)).toEqual(['after']);
	});

	it('keeps the adapter relay off while the breaker is open, as for every frame the bus carries', async () => {
		// The other workers are instances like any other: what Redis does not
		// carry during an outage misses them as it misses every other instance,
		// and nothing is ever delivered to them by two routes.
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const { busA, busB, a, wrapped } = await pair('cw-bus8:', { bus: { breaker } });
		try {
			breaker.failure(new Error('down'));
			wrapped.publish('chat', 'volatile', 1, { seq: false });
			wrapped.publishBatched([{ topic: 'chat', event: 'batched', data: 3, options: { seq: false } }]);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		const chat = a.published.filter((x) => x.topic === 'chat');
		expect(chat).toHaveLength(2);
		for (const f of chat) expect(f.options).toEqual({ seq: false, relay: false });
	});

	it(traced ? 'runs the traced publish paths' : 'runs the untraced publish paths', async () => {
		const { busA, busB, a, wrapped } = await pair('cw-bus11:');
		try {
			wrapped.publish('chat', 'one', 1, { seq: false });
			wrapped.publishBatched([{ topic: 'chat', event: 'two', data: 2, options: { seq: false } }]);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.traceOperations.length > 0).toBe(traced);
	});

	it('decides the relay per batch entry', async () => {
		const { busA, busB, a, wrapped } = await pair('cw-bus9:');
		const kept = { seq: false };
		try {
			wrapped.publishBatched([
				{ topic: '__internal', event: 'kept', data: 0, options: kept },
				{ topic: 'chat', event: 'relayed', data: 1, options: { seq: false } }
			]);
			wrapped.batch([{ topic: '__internal', event: 'kept-too', data: 2, options: kept }]);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		const byEvent = Object.fromEntries(a.published.map((f) => [f.event, f.options]));
		expect(byEvent.kept).toBe(kept);
		expect(byEvent['kept-too']).toBe(kept);
		expect(byEvent.relayed).toEqual({ seq: false, relay: false });
	});

	it('relays a shared-codec publishWire without the in-process relay', async () => {
		const { busA, busB, a, b, wrapped } = await pair('cw-bus10:', { wire: true });
		busB.wrap(b).registerWireCodec(sharedCodec);
		try {
			wrapped.publishWire('chat', 'snap', { n: 1 }, sharedCodec, { seq: false });
			await until(() => b.publishedWire.length === 1);
		} finally {
			await busA.deactivate();
			await busB.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(b.refused).toEqual([]);
		expect(a.publishedWire[0].options).toEqual({ seq: false, relay: false });
		expect(b.publishedWire[0].options).toEqual({ relay: false, seq: false });
	});

	it('delivers a peer envelope without a seq field unsequenced', async () => {
		const client = mockRedisClient('cw-bus6:');
		const bus = createBus(client);
		const b = clusteredPlatform();
		b.trace.enabled = traced;
		await bus.activate(b);
		await follow(bus, 'chat');
		try {
			await inject(client, { instanceId: 'older-peer', topic: 'chat', event: 'msg', data: 1 });
			await inject(client, { instanceId: 'older-peer', batch: [{ topic: 'chat', event: 'b', data: 2 }] });
			await until(() => b.published.length === 2);
		} finally {
			await bus.deactivate();
		}
		expect(b.refused).toEqual([]);
		for (const f of b.published) expect(f.options).toEqual({ relay: false, seq: false });
	});
});

describe('the bus system channel on a multi-worker runtime', () => {
	it('announces degraded and recovered without a refusal', async () => {
		const client = mockRedisClient('cw-sys:');
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 1 });
		const bus = createPubSubBus(client, { breaker });
		const a = clusteredPlatform();
		await bus.activate(a);
		try {
			breaker.failure(new Error('down'));
			await until(() => breaker.state === 'probing');
			breaker.success();
		} finally {
			await bus.deactivate();
		}
		expect(a.refused).toEqual([]);
		const announced = a.published.filter((f) => f.topic === '__realtime');
		expect(announced.map((f) => f.event)).toEqual(['degraded', 'recovered']);
		for (const f of announced) expect(f.options).toEqual({ relay: false, seq: false });
	});
});

describe('replay on a multi-worker runtime', () => {
	it('redis replay publishes its authoritative seq through the bus-wrapped platform', async () => {
		const client = mockRedisClient('cw-replay1:');
		const bus = createPubSubBus(client);
		const a = clusteredPlatform();
		await bus.activate(a);
		const replay = createRedisReplay(client);
		try {
			await replay.publish(bus.wrap(a), 'chat', 'created', { id: 1 });
		} finally {
			replay.destroy?.();
			await bus.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(a.published[0].options).toEqual({ seq: 1, relay: false });
	});

	it('redis replay on the bare platform is refused: nothing would fan the frame out to the other workers', async () => {
		const client = mockRedisClient('cw-replay2:');
		const a = clusteredPlatform();
		const replay = createRedisReplay(client);
		try {
			await expect(replay.publish(a, 'chat', 'created', { id: 1 })).rejects.toThrow(policy.CLUSTER_SEQUENCE_ERROR);
		} finally {
			replay.destroy?.();
		}
	});

	it.each(['sortedset', 'stream'])('the %s storage fallback publishes unsequenced', async (storage) => {
		const client = mockRedisClient('cw-replay3-' + storage + ':');
		const a = clusteredPlatform();
		const replay = createRedisReplay(client, { storage, localFanoutOnStorageFailure: true });
		const orig = client.redis.eval;
		client.redis.eval = async () => { throw new Error('redis down'); };
		const evalsha = client.redis.evalsha;
		client.redis.evalsha = async () => { throw new Error('redis down'); };
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			await replay.publish(a, 'chat', 'created', { id: 1 });
		} finally {
			client.redis.eval = orig;
			client.redis.evalsha = evalsha;
			replay.destroy?.();
		}
		expect(a.refused).toEqual([]);
		expect(a.published[0].options).toEqual({ seq: false });
	});

	it('postgres replay publishes single and batched frames through the bus-wrapped platform', async () => {
		const pg = mockPgClient();
		const client = mockRedisClient('cw-replay4:');
		const bus = createPubSubBus(client);
		const a = clusteredPlatform();
		await bus.activate(a);
		const replay = createPgReplay(pg, { cleanupInterval: 0 });
		try {
			await replay.publish(bus.wrap(a), 'chat', 'created', { id: 1 });
			await replay.publishBatch(bus.wrap(a), [
				{ topic: 'chat', event: 'created', data: { id: 2 } },
				{ topic: 'chat', event: 'created', data: { id: 3 } }
			]);
		} finally {
			replay.destroy();
			await bus.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(a.published.map((f) => f.options)).toEqual([
			{ seq: 1, relay: false },
			{ seq: 2, relay: false },
			{ seq: 3, relay: false }
		]);
	});

	it('the postgres storage fallback publishes unsequenced, single and batched', async () => {
		const pg = mockPgClient();
		const a = clusteredPlatform();
		const replay = createPgReplay(pg, { cleanupInterval: 0, localFanoutOnStorageFailure: true });
		const orig = pg.query.bind(pg);
		pg.query = async (textOrObj, values) => {
			const name = typeof textOrObj === 'object' ? textOrObj.name : '';
			if (name && name.startsWith('replay_publish')) throw new Error('db down');
			return orig(textOrObj, values);
		};
		vi.spyOn(console, 'warn').mockImplementation(() => {});
		try {
			await replay.publish(a, 'chat', 'created', { id: 1 });
			await replay.publishBatch(a, [{ topic: 'chat', event: 'created', data: { id: 2 } }]);
		} finally {
			replay.destroy();
		}
		expect(a.refused).toEqual([]);
		expect(a.published.map((f) => f.options)).toEqual([{ seq: false }, { seq: false }]);
	});
});

/** A LISTEN connection that holds every advisory lock and delivers what `notify` hands it. */
function listeningPg() {
	const listeners = [];
	const conn = {
		on(event, fn) { if (event === 'notification') listeners.push(fn); },
		removeListener() {},
		async connect() {},
		async query(text) {
			return String(text).includes('pg_try_advisory_lock') ? { rows: [{ acquired: true }], rowCount: 1 } : { rows: [], rowCount: 0 };
		},
		async end() {}
	};
	return {
		pool: {},
		createClient: () => conn,
		async query() { return { rows: [], rowCount: 0 }; },
		async end() {},
		notify(channel, payload) { for (const fn of listeners) fn({ channel, payload }); }
	};
}

describe('the postgres notify bridge on a multi-worker runtime', () => {
	it.each(['all', 'advisory'])('delivers a notification unsequenced (%s)', async (multiListener) => {
		const pg = listeningPg();
		const a = clusteredPlatform();
		const bridge = createNotifyBridge(pg, {
			channel: 'changes',
			multiListener,
			...(multiListener === 'advisory' ? { lockId: 7, pollInterval: 5 } : {})
		});
		try {
			await bridge.activate(a);
			await sleep(20);
			pg.notify('changes', JSON.stringify({ topic: 'orders', event: 'created', data: { id: 1 } }));
			await until(() => a.published.length === 1);
		} finally {
			await bridge.deactivate();
		}
		expect(a.refused).toEqual([]);
		expect(a.published[0].options).toEqual(multiListener === 'all' ? { relay: false, seq: false } : { seq: false });
	});
});
