// mockPlatform() against the adapter's own test platform: the member kinds it
// declares, and the refusals a test against the mock must meet the way it
// would meet them on the adapter.

import { describe, expect, it } from 'vitest';
import { assertPlatformContract, TEST_PLATFORM_CONTRACT } from 'svelte-adapter-uws/platform-contract';
import { mockPlatform } from '../../src/testing/mock-platform.js';

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
