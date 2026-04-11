/**
 * Test utilities for svelte-adapter-uws-extensions.
 *
 * In-memory mocks for Redis, Postgres, Platform, and WebSocket that mirror
 * the real APIs closely enough to test extension-consuming code without
 * running any infrastructure.
 *
 * @module svelte-adapter-uws-extensions/testing
 */

export { mockRedisClient } from './mock-redis.js';
export { mockPlatform } from './mock-platform.js';
export { mockWs } from './mock-ws.js';
export { mockPgClient } from './mock-pg.js';
