/**
 * Test utilities for svelte-adapter-uws-extensions.
 *
 * In-memory mocks for Redis, Postgres, Platform, and WebSocket that mirror
 * the real APIs closely enough to test extension-consuming code without
 * running any infrastructure, plus pass-throughs of the adapter's curated
 * wire-protocol helpers and userData slot constants so test code can assert
 * on the same shapes the production runtime produces.
 *
 * @module svelte-adapter-uws-extensions/testing
 */

export { mockRedisClient } from './mock-redis.js';
export { mockPlatform } from './mock-platform.js';
export { mockWs } from './mock-ws.js';
export { mockPgClient } from './mock-pg.js';

export {
	esc,
	completeEnvelope,
	wrapBatchEnvelope,
	isValidWireTopic,
	createScopedTopic,
	collapseByCoalesceKey,
	resolveRequestId,
	createChaosState,
	WS_SUBSCRIPTIONS,
	WS_COALESCED,
	WS_SESSION_ID,
	WS_PENDING_REQUESTS,
	WS_STATS,
	WS_PLATFORM,
	WS_CAPS,
	WS_REQUEST_ID_KEY
} from 'svelte-adapter-uws/testing';
