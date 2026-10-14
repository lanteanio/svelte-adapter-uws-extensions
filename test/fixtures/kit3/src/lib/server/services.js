import { randomUUID } from 'node:crypto';
import { KIT3_PRIVATE_VALUE, REDIS_URL, DATABASE_URL, TEST_NAMESPACE } from '$app/env/private';
import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';
import { createReplay as createRedisReplay } from 'svelte-adapter-uws-extensions/redis/replay';
import { createReplay as createPgReplay } from 'svelte-adapter-uws-extensions/postgres/replay';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
import { createDistributedSession } from 'svelte-adapter-uws-extensions/redis/session';
import { createConnectionRegistry } from 'svelte-adapter-uws-extensions/redis/registry';

// Read during module evaluation, before any HTTP or WebSocket callback.
export const environment = KIT3_PRIVATE_VALUE;
const redisUrl = REDIS_URL;
const databaseUrl = DATABASE_URL;
const namespace = TEST_NAMESPACE;
if (!environment || !redisUrl || !databaseUrl || !namespace) {
	throw new Error('The server module loaded before its private environment was initialized');
}

export const state = { id: randomUUID(), initialized: false, messages: 0 };
let services;

export function getServices() {
	if (services) return services;
	const redis = createRedisClient({ url: redisUrl, keyPrefix: namespace, autoShutdown: false });
	const pg = createPgClient({ connectionString: databaseUrl, autoShutdown: false });
	const bus = createPubSubBus(redis, { channel: namespace + 'bus' });
	const registry = createConnectionRegistry(redis, {
		identify: (ws) => ws.getUserData().userId,
		heartbeat: 60000
	});
	const redisReplay = createRedisReplay(redis);
	const postgresReplay = createPgReplay(pg, {
		table: namespace.replace(/[^a-zA-Z0-9_]/g, '_') + 'replay',
		cleanupInterval: 0
	});
	const sessions = createDistributedSession(redis);
	services = { redis, pg, bus, registry, redisReplay, postgresReplay, sessions };
	return services;
}

export async function shutdown() {
	if (!services) return;
	await services.registry.destroy();
	await services.bus.deactivate();
	services.postgresReplay.destroy();
	await Promise.all([services.redis.quit(), services.pg.end()]);
	services = undefined;
	state.initialized = false;
}
