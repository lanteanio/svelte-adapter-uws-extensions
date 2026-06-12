import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform, PLATFORM_KEYS } from '../../testing/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';
import { createShardedBus } from '../../redis/sharded-pubsub.js';

/**
 * Parity contract: every member listed in `PLATFORM_KEYS` (the canonical
 * set of public Platform members the adapter exposes) MUST be present on:
 *
 *   1. `mockPlatform()` itself - otherwise the mock has drifted and
 *      tests using it would silently see `undefined`.
 *   2. The object returned by `bus.wrap(platform)` for every bus that
 *      claims to return a Platform-shaped wrapper.
 *
 * When the adapter adds a new Platform member, append it to
 * `PLATFORM_KEYS` in `testing/mock-platform.js` and these tests fail
 * loudly until the mock and both wraps are updated. That's the
 * defense against the gap that broke `live.upload` chunk discovery
 * in the wild (`maxPayloadLength` / `bufferedAmount` were silently
 * missing from the bus wrap for ~5 prerelease versions because the
 * wrap was an explicit-list with no drift detection).
 */
describe('Platform parity: bus wraps expose every adapter Platform member', () => {
	it('mockPlatform() exposes every member in PLATFORM_KEYS', () => {
		const platform = mockPlatform();
		const missing = PLATFORM_KEYS.filter((k) => platform[k] === undefined);
		expect(missing).toEqual([]);
	});

	it('createPubSubBus().wrap(platform) exposes every member', () => {
		const client = mockRedisClient('test:');
		const bus = createPubSubBus(client);
		const wrapped = bus.wrap(mockPlatform());
		const missing = PLATFORM_KEYS.filter((k) => wrapped[k] === undefined);
		expect(missing).toEqual([]);
	});

	it('createShardedBus().wrap(platform) exposes every member', () => {
		const client = mockRedisClient('test:');
		const bus = createShardedBus(client);
		const wrapped = bus.wrap(mockPlatform());
		const missing = PLATFORM_KEYS.filter((k) => wrapped[k] === undefined);
		expect(missing).toEqual([]);
	});

	it('pubsub wrap forwards maxPayloadLength as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.maxPayloadLength).toBe(1024 * 1024);

		platform.maxPayloadLength = 16 * 1024;
		expect(wrapped.maxPayloadLength).toBe(16 * 1024);
	});

	it('pubsub wrap delegates bufferedAmount to the underlying platform', () => {
		const platform = mockPlatform();
		platform.bufferedAmount = (ws) => ws.__buffered ?? 0;
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.bufferedAmount({ __buffered: 1234 })).toBe(1234);
	});

	it('pubsub wrap delegates onPublishRate to the underlying platform', () => {
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		const seen = [];
		const unsubscribe = wrapped.onPublishRate((events) => seen.push(events));
		platform._emitPublishRate([{ topic: 'hot', messagesPerSec: 9999, bytesPerSec: 1024 }]);
		expect(seen).toHaveLength(1);
		expect(seen[0][0].topic).toBe('hot');

		unsubscribe();
		platform._emitPublishRate([{ topic: 'cold', messagesPerSec: 1, bytesPerSec: 1 }]);
		expect(seen).toHaveLength(1); // unsubscribed
	});

	it('pubsub wrap forwards closedWsAborts as a live getter', () => {
		// Adapter 0.5.5 added `platform.closedWsAborts` (counter that
		// tracks how many times ws-targeted platform methods swallowed
		// a "closed websocket" throw). The wrap must surface it live so
		// operators reading the wrapped seam see the same number as
		// readers of the source platform.
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.closedWsAborts).toBe(0);

		platform.closedWsAborts = 42;
		expect(wrapped.closedWsAborts).toBe(42);
	});

	it('sharded wrap forwards closedWsAborts as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.closedWsAborts).toBe(0);

		platform.closedWsAborts = 7;
		expect(wrapped.closedWsAborts).toBe(7);
	});

	it('pubsub wrap forwards protection as a live getter', () => {
		// The adapter exposes a protection posture ('normal' | 'elevated' |
		// 'siege') that cluster-side admission code reads off the platform.
		// In a cluster the caller's platform reference IS the wrap result, so
		// the wrap must surface the live posture or admission reads a stale
		// value. Default 'normal'; a transition after wrap must propagate.
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.protection).toBe('normal');

		platform.protection = 'siege';
		expect(wrapped.protection).toBe('siege');
	});

	it('sharded wrap forwards protection as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.protection).toBe('normal');

		platform.protection = 'elevated';
		expect(wrapped.protection).toBe('elevated');
	});

	it('pubsub wrap forwards now / monotonic / random as live getters', () => {
		// The adapter projects its injectable clock and RNG onto the Platform
		// (`now` / `monotonic` functions, `random` object) from the runtime
		// module. Cluster-side per-message handlers read these off the wrapped
		// seam, so the wrap must surface them live or a seeded harness clock /
		// RNG never reaches the cluster handlers. Same live-getter contract the
		// protection forwarder uses: a post-wrap reassignment must propagate.
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.now).toBe(platform.now);
		expect(wrapped.monotonic).toBe(platform.monotonic);
		expect(wrapped.random).toBe(platform.random);

		const seededNow = () => 1234;
		const seededMonotonic = () => 5678;
		const seededRandom = { float: () => 0.5, u32: () => 7, uuid: () => 'seed', bytes: (n) => Buffer.alloc(n) };
		platform.now = seededNow;
		platform.monotonic = seededMonotonic;
		platform.random = seededRandom;
		expect(wrapped.now).toBe(seededNow);
		expect(wrapped.now()).toBe(1234);
		expect(wrapped.monotonic).toBe(seededMonotonic);
		expect(wrapped.monotonic()).toBe(5678);
		expect(wrapped.random).toBe(seededRandom);
		expect(wrapped.random.u32()).toBe(7);
	});

	it('sharded wrap forwards now / monotonic / random as live getters', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.now).toBe(platform.now);
		expect(wrapped.monotonic).toBe(platform.monotonic);
		expect(wrapped.random).toBe(platform.random);

		const seededNow = () => 4321;
		const seededMonotonic = () => 8765;
		const seededRandom = { float: () => 0.25, u32: () => 9, uuid: () => 'seed', bytes: (n) => Buffer.alloc(n) };
		platform.now = seededNow;
		platform.monotonic = seededMonotonic;
		platform.random = seededRandom;
		expect(wrapped.now).toBe(seededNow);
		expect(wrapped.now()).toBe(4321);
		expect(wrapped.monotonic).toBe(seededMonotonic);
		expect(wrapped.monotonic()).toBe(8765);
		expect(wrapped.random).toBe(seededRandom);
		expect(wrapped.random.u32()).toBe(9);
	});

	it('pubsub wrap forwards hlc as a live getter', () => {
		// The adapter projects a hybrid logical clock onto the Platform. A
		// cluster-side handler that causally stamps an event reads `hlc` off
		// the wrapped platform, so a missing forwarder leaves cluster stamps
		// reading `undefined`. Same live-getter contract as now / monotonic:
		// a post-wrap reassignment must propagate by reference.
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(typeof wrapped.hlc).toBe('function');
		expect(wrapped.hlc).toBe(platform.hlc);

		const seededHlc = () => ({ wall: 1717, logical: 3, nodeId: 'seed' });
		platform.hlc = seededHlc;
		expect(wrapped.hlc).toBe(seededHlc);
		expect(wrapped.hlc()).toEqual({ wall: 1717, logical: 3, nodeId: 'seed' });
	});

	it('sharded wrap forwards hlc as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(typeof wrapped.hlc).toBe('function');
		expect(wrapped.hlc).toBe(platform.hlc);

		const seededHlc = () => ({ wall: 4321, logical: 9, nodeId: 'seed' });
		platform.hlc = seededHlc;
		expect(wrapped.hlc).toBe(seededHlc);
		expect(wrapped.hlc()).toEqual({ wall: 4321, logical: 9, nodeId: 'seed' });
	});

	it('pubsub wrap falls back to native clock / RNG / HLC when the platform omits them', () => {
		// The peer-dependency floor admits adapters that predate the injectable
		// runtime projection, where now / monotonic / random / hlc are undefined.
		// A cluster-side handler reading them off the wrapped seam must still get a
		// working clock / RNG / HLC - sourced from this package's own runtime - so
		// the wrap degrades gracefully instead of throwing 'undefined is not a
		// function'. Simulate such an adapter by deleting the projected members.
		const platform = mockPlatform();
		delete platform.now;
		delete platform.monotonic;
		delete platform.random;
		delete platform.hlc;
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		expect(typeof wrapped.now).toBe('function');
		expect(typeof wrapped.now()).toBe('number');
		expect(typeof wrapped.monotonic).toBe('function');
		expect(typeof wrapped.monotonic()).toBe('number');
		expect(typeof wrapped.random.float()).toBe('number');
		expect(typeof wrapped.random.u32()).toBe('number');
		expect(typeof wrapped.random.uuid()).toBe('string');
		expect(wrapped.random.bytes(4)).toHaveLength(4);

		const stamp = wrapped.hlc();
		expect(typeof stamp.wall).toBe('number');
		expect(typeof stamp.logical).toBe('number');
		expect(typeof stamp.nodeId).toBe('string');
		// The fallback HLC keeps the (wall, logical) pair strictly increasing
		// even within a coarse clock tick, the adapter's non-decreasing contract.
		const a = wrapped.hlc();
		const b = wrapped.hlc();
		expect(b.wall > a.wall || (b.wall === a.wall && b.logical > a.logical)).toBe(true);
	});

	it('sharded wrap falls back to native clock / RNG / HLC when the platform omits them', () => {
		const platform = mockPlatform();
		delete platform.now;
		delete platform.monotonic;
		delete platform.random;
		delete platform.hlc;
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);

		expect(typeof wrapped.now).toBe('function');
		expect(typeof wrapped.now()).toBe('number');
		expect(typeof wrapped.monotonic).toBe('function');
		expect(typeof wrapped.monotonic()).toBe('number');
		expect(typeof wrapped.random.float()).toBe('number');
		expect(typeof wrapped.random.u32()).toBe('number');
		expect(typeof wrapped.random.uuid()).toBe('string');
		expect(wrapped.random.bytes(4)).toHaveLength(4);

		const stamp = wrapped.hlc();
		expect(typeof stamp.wall).toBe('number');
		expect(typeof stamp.logical).toBe('number');
		expect(typeof stamp.nodeId).toBe('string');
	});

	it('pubsub wrap prefers the platform clock / RNG / HLC over the fallback when present', () => {
		// The fallback must never shadow a projected member. A seeded harness
		// value set on the platform has to win, so the live reference still flows
		// to cluster handlers (the whole point of forwarding it live).
		const platform = mockPlatform();
		const seededNow = () => 999;
		const seededRandom = { float: () => 0.1, u32: () => 3, uuid: () => 'seed', bytes: (n) => Buffer.alloc(n) };
		const seededHlc = () => ({ wall: 7, logical: 1, nodeId: 'seed' });
		platform.now = seededNow;
		platform.random = seededRandom;
		platform.hlc = seededHlc;
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		expect(wrapped.now).toBe(seededNow);
		expect(wrapped.random).toBe(seededRandom);
		expect(wrapped.hlc).toBe(seededHlc);
	});

	it('pubsub wrap delegates forEachSubscriber to the underlying platform', () => {
		const platform = mockPlatform();
		const walked = [];
		platform.forEachSubscriber = (topic, fn) => { walked.push(topic); fn({ id: 'a' }, {}); };
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		const seen = [];
		wrapped.forEachSubscriber('room:1', (ws) => seen.push(ws.id));
		expect(walked).toEqual(['room:1']);
		expect(seen).toEqual(['a']);
	});

	it('sharded wrap delegates forEachSubscriber to the underlying platform', () => {
		const platform = mockPlatform();
		const walked = [];
		platform.forEachSubscriber = (topic, fn) => { walked.push(topic); fn({ id: 'b' }, {}); };
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);

		const seen = [];
		wrapped.forEachSubscriber('room:2', (ws) => seen.push(ws.id));
		expect(walked).toEqual(['room:2']);
		expect(seen).toEqual(['b']);
	});

	it('pubsub wrap passes publishWire options (excludeWs sender exclusion) through to the platform', () => {
		// The adapter's publishWire accepts { excludeWs } to withhold a publish
		// from the originating socket. The wrap forwards publishWire by
		// reference, so the options argument - the codec too - must reach the
		// underlying platform intact or sender exclusion silently dies in a
		// cluster, where the caller's platform reference IS the wrap result.
		// publishWire is captured at wrap construction (same bind pattern as
		// send / sendCoalesced), so attach the recorder BEFORE wrapping.
		const platform = mockPlatform();
		const calls = [];
		platform.publishWire = (topic, event, data, wire, options) => {
			calls.push({ topic, event, data, wire, options });
			return true;
		};
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		const sender = { id: 'sender' };
		const codec = { capability: 'cursor.protocol:2', encode: () => null };
		wrapped.publishWire('room:1', 'update', { key: 'k', data: { x: 1 } }, codec, { excludeWs: sender });

		expect(calls).toHaveLength(1);
		expect(calls[0].topic).toBe('room:1');
		expect(calls[0].event).toBe('update');
		expect(calls[0].wire).toBe(codec);
		expect(calls[0].options).toEqual({ excludeWs: sender });
		expect(calls[0].options.excludeWs).toBe(sender);
	});

	it('sharded wrap passes publishWire options (excludeWs sender exclusion) through to the platform', () => {
		const platform = mockPlatform();
		const calls = [];
		platform.publishWire = (topic, event, data, wire, options) => {
			calls.push({ topic, event, data, wire, options });
			return true;
		};
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);

		const sender = { id: 'sender' };
		const codec = { capability: 'cursor.protocol:2', encode: () => null };
		wrapped.publishWire('room:2', 'update', { key: 'k', data: { x: 2 } }, codec, { excludeWs: sender });

		expect(calls).toHaveLength(1);
		expect(calls[0].topic).toBe('room:2');
		expect(calls[0].event).toBe('update');
		expect(calls[0].wire).toBe(codec);
		expect(calls[0].options).toEqual({ excludeWs: sender });
		expect(calls[0].options.excludeWs).toBe(sender);
	});

	it('sharded wrap forwards maxPayloadLength / bufferedAmount / onPublishRate', () => {
		const platform = mockPlatform();
		// Methods are captured at wrap-construction (consistent with the
		// existing send / sendCoalesced bind pattern), so override the
		// mock methods BEFORE constructing the wrap.
		platform.bufferedAmount = (ws) => ws.__bytes ?? 0;
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);

		// maxPayloadLength is a live getter, so post-wrap mutations propagate.
		platform.maxPayloadLength = 8 * 1024;
		expect(wrapped.maxPayloadLength).toBe(8 * 1024);

		expect(wrapped.bufferedAmount({ __bytes: 42 })).toBe(42);

		const seen = [];
		wrapped.onPublishRate((events) => seen.push(events));
		platform._emitPublishRate([{ topic: 't', messagesPerSec: 5, bytesPerSec: 5 }]);
		expect(seen).toHaveLength(1);
	});
});

/**
 * Framework conventions stashed on the source platform (e.g.
 * `platform.replay = createReplay(...)`) are NOT adapter Platform members
 * but downstream frameworks discover them by property access on whatever
 * platform reference they hold. When that reference is a `bus.wrap(...)`
 * result (the `configureCron({ bus })` path in svelte-realtime is the
 * known case), the wrap must forward them as live getters; otherwise the
 * framework's discovery falls through and the feature is silently dead.
 *
 * Drift on these forwards has the same shape as the parity gap above,
 * but the contract is wider than `PLATFORM_KEYS` so it lives in its own
 * describe block. Add a new `it()` here when svelte-realtime grows
 * another convention slot the wrap is expected to preserve.
 */
describe('Framework conventions: bus wraps preserve app-stashed properties', () => {
	it('pubsub wrap forwards platform.replay as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);

		expect(wrapped.replay).toBeUndefined();

		const replay = { publish: () => {}, seq: () => 0 };
		platform.replay = replay;
		expect(wrapped.replay).toBe(replay);

		platform.replay = undefined;
		expect(wrapped.replay).toBeUndefined();
	});

	it('sharded wrap forwards platform.replay as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);

		expect(wrapped.replay).toBeUndefined();

		const replay = { publish: () => {}, seq: () => 0 };
		platform.replay = replay;
		expect(wrapped.replay).toBe(replay);
	});

	it('mockPlatform exposes a replay slot defaulting to undefined', () => {
		const platform = mockPlatform();
		expect('replay' in platform).toBe(true);
		expect(platform.replay).toBeUndefined();
	});

	it('pubsub wrap forwards platform.redis as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.redis).toBeUndefined();

		const client = { status: 'ready' };
		platform.redis = client;
		expect(wrapped.redis).toBe(client);
	});

	it('sharded wrap forwards platform.redis as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.redis).toBeUndefined();

		const client = { status: 'ready' };
		platform.redis = client;
		expect(wrapped.redis).toBe(client);
	});

	it('pubsub wrap forwards platform.presence as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createPubSubBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.presence).toBeUndefined();

		const presence = { list: () => [] };
		platform.presence = presence;
		expect(wrapped.presence).toBe(presence);
	});

	it('sharded wrap forwards platform.presence as a live getter', () => {
		const platform = mockPlatform();
		const wrapped = createShardedBus(mockRedisClient('test:')).wrap(platform);
		expect(wrapped.presence).toBeUndefined();

		const presence = { list: () => [] };
		platform.presence = presence;
		expect(wrapped.presence).toBe(presence);
	});
});
