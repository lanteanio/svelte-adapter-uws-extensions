import { describe, it, expect } from 'vitest';
import * as extensionsTesting from '../../testing/index.js';
import * as adapterTesting from 'svelte-adapter-uws/testing';

const passthroughs = [
	'esc',
	'completeEnvelope',
	'wrapBatchEnvelope',
	'isValidWireTopic',
	'createScopedTopic',
	'collapseByCoalesceKey',
	'resolveRequestId',
	'createChaosState',
	'WS_SUBSCRIPTIONS',
	'WS_COALESCED',
	'WS_SESSION_ID',
	'WS_PENDING_REQUESTS',
	'WS_STATS',
	'WS_PLATFORM',
	'WS_CAPS',
	'WS_REQUEST_ID_KEY'
];

describe('testing/index re-exports', () => {
	it('exposes the in-memory mocks alongside the adapter pass-throughs', () => {
		expect(typeof extensionsTesting.mockRedisClient).toBe('function');
		expect(typeof extensionsTesting.mockPlatform).toBe('function');
		expect(typeof extensionsTesting.mockWs).toBe('function');
		expect(typeof extensionsTesting.mockPgClient).toBe('function');
	});

	for (const name of passthroughs) {
		it(`re-exports ${name} as the same identity as svelte-adapter-uws/testing`, () => {
			expect(extensionsTesting[name]).toBe(adapterTesting[name]);
		});
	}

	it('does not re-export adapter internals beyond the curated set', () => {
		expect(extensionsTesting.createTestServer).toBeUndefined();
		expect(extensionsTesting.mimes).toBeUndefined();
		expect(extensionsTesting.parse_as_bytes).toBeUndefined();
		expect(extensionsTesting.computePressureReason).toBeUndefined();
	});

	it('WS_REQUEST_ID_KEY is the string carrier the adapter stamps on userData', () => {
		expect(extensionsTesting.WS_REQUEST_ID_KEY).toBe('__adapter_uws_request_id__');
	});

	it('symbol slots are unique per-key', () => {
		const slots = [
			extensionsTesting.WS_SUBSCRIPTIONS,
			extensionsTesting.WS_COALESCED,
			extensionsTesting.WS_SESSION_ID,
			extensionsTesting.WS_PENDING_REQUESTS,
			extensionsTesting.WS_STATS,
			extensionsTesting.WS_PLATFORM,
			extensionsTesting.WS_CAPS
		];
		expect(new Set(slots).size).toBe(slots.length);
		for (const slot of slots) expect(typeof slot).toBe('symbol');
	});
});
