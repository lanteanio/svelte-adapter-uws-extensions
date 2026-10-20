import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createDashboardSnapshots } from '../../node_modules/svelte-adapter-uws/src/dev-dashboard.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { createNotifyBridge } from '../../src/postgres/notify.js';
import { createLeader } from '../../src/redis/leader.js';
import { createClockSkewSampler } from '../../src/redis/clock-skew.js';
import { createConnectionRegistry } from '../../src/redis/registry.js';
import { createPresence } from '../../src/redis/presence.js';
import { createRedisClient } from '../../src/redis/index.js';
import { createPgClient } from '../../src/postgres/index.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

const key = Symbol.for('svelte-adapter-uws.dashboard-contributors');
const cleanups = [];
let snapshots;
beforeEach(() => {
	vi.stubEnv('NODE_ENV', 'development');
	delete globalThis[key];
	snapshots = createDashboardSnapshots({ now: () => 1000, introspect: () => ({}), topicCounts: () => new Map() });
});
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
	delete globalThis[key];
	vi.unstubAllEnvs();
});
function section() { return snapshots().sections['cluster-health']; }

describe('cluster dashboard', () => {
	it.each([['pubsub', createPubSubBus], ['sharded-pubsub', createShardedBus]])('reads %s attachment and queue state without backend requests', async (name, createBus) => {
		const client = mockRedisClient('private:');
		const bus = createBus(client);
		cleanups.push(() => bus.deactivate());
		const platform = mockPlatform();
		await bus.activate(platform);
		const publish = vi.spyOn(client.redis, 'publish');
		const info = vi.spyOn(client.redis, 'info');
		const before = snapshots();
		expect(before.sections['cluster-health'][name]).toMatchObject({ backend: 'redis', attached: true, queue: { queued: 0, retained: 0 } });
		expect(snapshots().seq).toBeGreaterThan(before.seq);
		expect(publish).not.toHaveBeenCalled();
		expect(info).not.toHaveBeenCalled();
		await bus.deactivate();
		expect(section()).toBeUndefined();
		await bus.activate(platform);
		expect(section()[name].attached).toBe(true);
	});

	it('replaces a hot-reloaded bus and ignores teardown of the older bus', async () => {
		const first = createPubSubBus(mockRedisClient());
		cleanups.push(() => first.deactivate());
		await first.activate(mockPlatform());
		const previous = globalThis[key].get('cluster-health');
		vi.resetModules();
		const { createPubSubBus: reloaded } = await import('../../src/redis/pubsub.js');
		const second = reloaded(mockRedisClient());
		cleanups.push(() => second.deactivate());
		await second.activate(mockPlatform());
		await first.deactivate();
		expect(globalThis[key].size).toBe(1);
		expect(Object.keys(section())).toEqual(['pubsub']);
		expect(section().pubsub.attached).toBe(true);
		expect(typeof previous).toBe('function');
		await second.deactivate();
		expect(globalThis[key].has('cluster-health')).toBe(false);
	});

	it('reports the notification bridge and removes it on deactivation', async () => {
		const bridge = createNotifyBridge(mockPgClient(), { channel: 'private_channel' });
		cleanups.push(() => bridge.deactivate());
		await bridge.activate(mockPlatform());
		expect(section().notify).toMatchObject({ backend: 'postgres', attached: true, listening: true });
		expect(JSON.stringify(section())).not.toContain('private_channel');
		await bridge.deactivate();
		expect(section()).toBeUndefined();
	});

	it('uses the existing leader and clock readings', async () => {
		const client = mockRedisClient();
		const metrics = createMetrics();
		const leader = createLeader(client, { key: 'private-lock', instanceId: 'private-worker', metrics });
		cleanups.push(() => leader.stop());
		const clock = createClockSkewSampler(client, { immediate: false, samples: 1, metrics });
		cleanups.push(() => clock.stop());
		expect(section().clock.skewMs).toBeNull();
		await clock.sample();
		expect(section().leader.isLeader).toBe(leader.isLeader());
		expect(section().clock.skewMs).toBe(clock.current());
		expect(section().clock.fenced).toBe(clock.fenced());
		expect(JSON.stringify(section())).not.toContain('private');
		await leader.stop();
		await clock.stop();
		expect(section()).toBeUndefined();
	});

	it('reports anonymous membership counts and the existing presence measurements', async () => {
		const client = mockRedisClient();
		const platform = mockPlatform();
		const registry = createConnectionRegistry(client, { identify: ws => ws.getUserData().id, attributes: () => ({ role: 'private-role' }) });
		cleanups.push(() => registry.destroy());
		const presence = createPresence(client, { key: 'id', select: data => ({ id: data.id }), keyspaceNotifications: false });
		cleanups.push(() => presence.destroy());
		const ws = mockWs({ id: 'private-user' });
		ws.getUserData()[WS_SESSION_ID] = 'private-connection';
		await registry.hooks.open(ws, { platform });
		await presence.join(ws, 'private-topic', platform);
		const reading = section();
		expect(reading.registry).toMatchObject({ localConnections: 1, knownUsers: 1, knownInstances: 1, pendingRequests: 0 });
		expect(reading.presence).toMatchObject(presence.metrics());
		expect(reading['presence-subscriber']).toMatchObject({ channels: 1, lagMs: null });
		expect(JSON.stringify(reading)).not.toContain('private');
		await registry.destroy();
		presence.destroy();
		expect(section()).toBeUndefined();
	});

	it('reads client connection and pool pressure without connecting', async () => {
		const redis = createRedisClient({ url: 'redis://127.0.0.1:56419', autoShutdown: false, options: { lazyConnect: true } });
		cleanups.push(() => redis.quit());
		const pool = { query: vi.fn(), totalCount: 5, idleCount: 2, waitingCount: 3, options: { max: 10 } };
		const pg = createPgClient({ pool });
		cleanups.push(() => pg.end());
		expect(section().redis).toEqual({ backend: 'redis', connection: 'wait', duplicates: 0 });
		expect(section().postgres).toEqual({ backend: 'postgres', ended: false, connections: 5, idle: 2, waiters: 3, capacity: 10 });
		pool.waitingCount = 4;
		expect(section().postgres.waiters).toBe(4);
		expect(pool.query).not.toHaveBeenCalled();
		await redis.quit();
		await pg.end();
		expect(section()).toBeUndefined();
	});

	it('isolates failed readings and suppresses custom driver text', async () => {
		const client = mockRedisClient();
		const bus = createPubSubBus(client);
		cleanups.push(() => bus.deactivate());
		await bus.activate(mockPlatform());
		client.redis.status = 'private-address';
		expect(section().pubsub.connection).toBe('unknown');
		const pool = { query() {}, get totalCount() { throw new Error('private-password'); } };
		const pg = createPgClient({ pool });
		cleanups.push(() => pg.end());
		expect(section().postgres).toEqual({ available: false });
		expect(section().pubsub.attached).toBe(true);
		expect(JSON.stringify(section())).not.toContain('private');
	});

	it('retains unrelated contributors during registration and cleanup', async () => {
		const other = () => ({ healthy: true });
		globalThis[key] = new Map([['other', other]]);
		const bus = createPubSubBus(mockRedisClient());
		cleanups.push(() => bus.deactivate());
		await bus.activate(mockPlatform());
		expect(snapshots().sections.other).toEqual({ healthy: true });
		await bus.deactivate();
		expect([...globalThis[key]]).toEqual([['other', other]]);
	});

	it.each(['production', 'test', ''])('does not register outside development (%s)', async mode => {
		vi.stubEnv('NODE_ENV', mode);
		const bus = createPubSubBus(mockRedisClient());
		cleanups.push(() => bus.deactivate());
		await bus.activate(mockPlatform());
		expect(globalThis[key]).toBeUndefined();
	});
});
