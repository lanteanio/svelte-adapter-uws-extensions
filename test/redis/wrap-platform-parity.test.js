import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform, PLATFORM_KEYS } from '../../testing/mock-platform.js';
import { createPubSubBus } from '../../redis/pubsub.js';
import { createShardedBus } from '../../redis/sharded-pubsub.js';

/**
 * Parity contract: every member listed in `PLATFORM_KEYS` (the canonical
 * set of public Platform members the adapter exposes) MUST be present on:
 *
 *   1. `mockPlatform()` itself -- otherwise the mock has drifted and
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
