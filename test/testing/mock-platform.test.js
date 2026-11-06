// mockPlatform() against the adapter's own test platform: the member kinds it
// declares, and the refusals a test against the mock must meet the way it
// would meet them on the adapter.

import { afterEach, describe, expect, it } from 'vitest';
import { assertPlatformContract, TEST_PLATFORM_CONTRACT } from 'svelte-adapter-uws/platform-contract';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

/** A name ending in a lone high surrogate, at index 4. */
const LONE = 'lone' + String.fromCharCode(0xd800);
const UNPAIRED = 'Topic/event name contains an unpaired surrogate at index 4. Names must be well-formed Unicode.';

describe('mockPlatform and the adapter test platform contract', () => {
	it.each(['worker', 'connection'])('provides every declared member with its declared kind (%s context)', (context) => {
		expect(() => assertPlatformContract(mockPlatform(), TEST_PLATFORM_CONTRACT, { context })).not.toThrow();
	});
});

describe('mockPlatform.processId', () => {
	it('is a UUID, stable, and distinct per mock', () => {
		const a = mockPlatform();
		const b = mockPlatform();
		expect(a.processId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/);
		expect(a.processId).toBe(a.processId);
		expect(b.processId).not.toBe(a.processId);
	});
});

describe('mockPlatform.relayCoalesced', () => {
	it('records the key as the relay carries it and reports that no sibling was reached', () => {
		const p = mockPlatform();
		expect(p.relayCoalesced('room', 'move', { x: 1 }, 7)).toBe(false);
		expect(p.relayCoalesced('room', 'move', { x: 2 })).toBe(false);
		expect(p.relayCoalesced('room', 'move', { x: 3 }, null)).toBe(false);
		expect(p.relayedCoalesced).toEqual([
			{ topic: 'room', event: 'move', data: { x: 1 }, coalesceKey: '7' },
			{ topic: 'room', event: 'move', data: { x: 2 }, coalesceKey: '' },
			{ topic: 'room', event: 'move', data: { x: 3 }, coalesceKey: '' }
		]);
		p.reset();
		expect(p.relayedCoalesced).toEqual([]);
	});

	it('refuses a name the adapter refuses, before recording it', () => {
		const p = mockPlatform();
		expect(() => p.relayCoalesced(5, 'move', 1)).toThrow(new TypeError('Topic/event names must be strings.'));
		expect(() => p.relayCoalesced('a"b', 'move', 1)).toThrow("Topic/event name contains invalid character at index 1: 'a\"b'.");
		expect(() => p.relayCoalesced('room', LONE, 1)).toThrow(UNPAIRED);
		expect(p.relayedCoalesced).toEqual([]);
	});
});

describe('mockPlatform.authorizeDelivery', () => {
	const policy = () => true;

	it('keeps one policy and refuses a second, as the adapter does', () => {
		const p = mockPlatform();
		p.authorizeDelivery(policy, { timeoutMs: 100, maxPendingBytes: 4096 });
		expect(p.deliveryAuthorization).toEqual({ callback: policy, options: { timeoutMs: 100, maxPendingBytes: 4096 } });
		expect(() => p.authorizeDelivery(() => false)).toThrow(new Error('Delivery authorization is already configured'));
		expect(p.deliveryAuthorization.callback).toBe(policy);
		p.reset();
		expect(p.deliveryAuthorization).toBeNull();
		p.authorizeDelivery(policy, { timeoutMs: null });
		expect(p.deliveryAuthorization.callback).toBe(policy);
	});

	it.each([
		['a missing callback', [undefined], 'authorizeDelivery requires a function'],
		['null options', [policy, null], 'authorizeDelivery options must be an object'],
		['array options', [policy, []], 'authorizeDelivery options must be an object'],
		['an unknown option', [policy, { timeout: 5 }], 'Unknown authorizeDelivery option timeout'],
		['a zero byte bound', [policy, { maxPendingBytes: 0 }], 'Invalid authorizeDelivery maxPendingBytes'],
		['a fractional byte bound', [policy, { maxPendingBytes: 1.5 }], 'Invalid authorizeDelivery maxPendingBytes'],
		['a zero timeout', [policy, { timeoutMs: 0 }], 'Invalid authorizeDelivery timeoutMs'],
		['a timeout past the timer ceiling', [policy, { timeoutMs: 2147483648 }], 'Invalid authorizeDelivery timeoutMs']
	])('refuses %s with a TypeError and installs nothing', (_case, args, message) => {
		const p = mockPlatform();
		expect(() => p.authorizeDelivery(...args)).toThrow(new TypeError(message));
		expect(p.deliveryAuthorization).toBeNull();
	});
});

describe('mockPlatform topic and event names', () => {
	const ws = {};
	it.each([
		['publish', (p, t, e) => p.publish(t, e, 1)],
		['publishWire', (p, t, e) => p.publishWire(t, e, 1, {})],
		['publishBatched', (p, t, e) => p.publishBatched([{ topic: 'ok', event: 'ok', data: 1 }, { topic: t, event: e, data: 2 }])],
		['publishWireBatch', (p, t, e) => p.publishWireBatch(t, e, [{ data: 1 }], {})],
		['batch', (p, t, e) => p.batch([{ topic: t, event: e, data: 1 }])],
		['send', (p, t, e) => p.send(ws, t, e, 1)],
		['sendWire', (p, t, e) => p.sendWire(ws, t, e, 1, {})],
		['sendWireBatch', (p, t, e) => p.sendWireBatch(ws, t, e, [{ data: 1 }], {})],
		['sendCoalesced', (p, t, e) => p.sendCoalesced(ws, { key: 'k', topic: t, event: e, data: 1 })],
		['sendTo', (p, t, e) => p.sendTo(() => true, t, e, 1)]
	])('%s refuses what the adapter cannot frame and records nothing', (_member, call) => {
		const p = mockPlatform();
		expect(() => call(p, 5, 'event')).toThrow(new TypeError('Topic/event names must be strings.'));
		expect(() => call(p, 'topic', undefined)).toThrow(new TypeError('Topic/event names must be strings.'));
		expect(() => call(p, 'say "hi"', 'event')).toThrow('Names must not contain quotes, backslashes, or control characters.');
		expect(() => call(p, 'topic', LONE)).toThrow(UNPAIRED);
		expect([p.published, p.publishedBatches, p.sent, p.sentCoalesced]).toEqual([[], [], [], []]);
		// A well-formed surrogate pair is ordinary text.
		expect(() => call(p, 'room-' + String.fromCharCode(0xd83d, 0xde00), 'event')).not.toThrow();
	});

	it('an empty wire batch returns before any name is checked, as on the adapter', () => {
		const p = mockPlatform();
		expect(p.publishWireBatch(LONE, 'event', [], {})).toBe(false);
		expect(p.sendWireBatch(ws, 5, 'event', [], {})).toBe(1);
	});

	it('publishBatched checks only the entries left after a coalesce key replaced an earlier one', () => {
		const p = mockPlatform();
		p.publishBatched([
			{ topic: 'room', event: LONE, data: 1, coalesceKey: 'pos' },
			{ topic: 'room', event: 'move', data: 2, coalesceKey: 'pos' }
		]);
		expect(p.publishedBatches).toHaveLength(1);
		expect(() => p.publishBatched([
			{ topic: 'room', event: 'move', data: 1, coalesceKey: 'pos' },
			{ topic: 'room', event: LONE, data: 2, coalesceKey: 'pos' }
		])).toThrow(UNPAIRED);
		expect(p.publishedBatches).toHaveLength(1);
	});
});

describe('bus.wrap relayCoalesced and an unpaired surrogate', () => {
	const buses = [];
	afterEach(async () => {
		for (const bus of buses.splice(0)) await bus.deactivate();
	});

	it.each([['standard', createPubSubBus], ['sharded', createShardedBus]])('%s bus refuses it as the adapter does', async (_name, create) => {
		const bus = create(mockRedisClient('rc:'));
		buses.push(bus);
		const platform = mockPlatform();
		await bus.activate(platform);
		const wrapped = bus.wrap(platform);
		expect(() => wrapped.relayCoalesced('room', LONE, 1)).toThrow(UNPAIRED);
		expect(() => wrapped.relayCoalesced(LONE, 'move', 1)).toThrow('Topic/event name contains an unpaired surrogate at index 4');
	});
});
