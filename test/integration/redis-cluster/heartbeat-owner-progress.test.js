import { afterEach, expect, it, vi } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { resetBackendKeys } from '../helpers/backend.js';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { createPresence } from '../../../src/redis/presence.js';
import { createRedisIo } from '../../../src/redis/cursor/redis-io.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

afterEach(resetRuntimeEnv);

// A RedisClient-shaped wrapper whose duplicates are closed with it.
function clusterRedisClient(keyPrefix) {
	const redis = clusterClient();
	const duplicates = [];
	return {
		redis,
		keyPrefix,
		key: (k) => keyPrefix + k,
		duplicate: (options) => { const duplicate = clusterClient(options); duplicates.push(duplicate); return duplicate; },
		async quit() {
			for (const connection of [redis, ...duplicates]) await connection.quit().catch(() => connection.disconnect());
		}
	};
}

function captureIntervals() {
	const intervals = [];
	setRuntimeEnv({ timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.push(timer); return timer; },
		clearInterval() {}
	} });
	return delay => intervals.find(timer => timer.delay === delay).fn;
}

// Two names whose keys live on different masters.
function onTwoMasters(redis, keyOf) {
	const owner = name => redis.slots[keySlot(keyOf(name))][0];
	const slow = 'name-0';
	for (let i = 1; i < 100; i++) if (owner('name-' + i) !== owner(slow)) return [slow, 'name-' + i];
	throw new Error('every name landed on one master');
}

async function pauseOwnerOf(redis, key) {
	const owner = redis.slots[keySlot(key)][0];
	const mapped = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP)[owner];
	const node = redis.nodes('master').find(candidate => candidate.options.host + ':' + candidate.options.port === owner
		|| (mapped && candidate.options.host === mapped.host && candidate.options.port === mapped.port));
	expect(node).toBeDefined();
	await node.client('PAUSE', 3000, 'ALL');
}

it('keeps registry TTLs fresh on a healthy master while another master is paused', async () => {
	const interval = captureIntervals();
	const client = clusterRedisClient('heartbeat-progress:');
	const redis = client.redis;
	let tracker;
	try {
		await redis.cluster('info');
		const [slow, healthy] = onTwoMasters(redis, user => client.key('conns:' + user));
		tracker = createConnectionRegistry(client, { identify: ws => ws.getUserData().id, heartbeat: 17, ttl: 30 });
		const platform = mockPlatform();
		await tracker.hooks.open(mockWs({ id: slow, [WS_SESSION_ID]: slow }), { platform });
		await tracker.hooks.open(mockWs({ id: healthy, [WS_SESSION_ID]: healthy }), { platform });
		const slowKey = client.key('conns:' + slow);
		const healthyKey = client.key('conns:' + healthy);
		expect(await redis.exists(slowKey)).toBe(1);
		expect(await redis.exists(healthyKey)).toBe(1);
		await pauseOwnerOf(redis, slowKey);
		const tick = interval(17);
		const started = Date.now();
		for (let i = 0; i < 5; i++) {
			await redis.pexpire(healthyKey, 2000);
			tick();
			await vi.waitFor(async () => expect(await redis.pttl(healthyKey)).toBeGreaterThan(25000), { timeout: 400, interval: 10 });
		}
		expect(Date.now() - started).toBeLessThan(2500);
	} finally {
		await tracker?.destroy();
		await resetBackendKeys(client);
		await client.quit();
	}
});

it('keeps presence field TTLs fresh on a healthy master while another master is paused', async () => {
	const interval = captureIntervals();
	const client = clusterRedisClient('presence-progress:');
	const redis = client.redis;
	let presence;
	try {
		await redis.cluster('info');
		const topicKey = topic => client.key('presence:topic:{' + topic + '}');
		const [slow, healthy] = onTwoMasters(redis, topicKey);
		presence = createPresence(client, { key: 'id', heartbeat: 17, ttl: 30 });
		const platform = mockPlatform();
		await presence.join(mockWs({ id: 'u-slow' }), slow, platform);
		await presence.join(mockWs({ id: 'u-healthy' }), healthy, platform);
		const fieldTtl = async () => (await redis.call('HPTTL', topicKey(healthy), 'FIELDS', 1, 'u-healthy'))[0];
		expect(await fieldTtl()).toBeGreaterThan(25000);
		await pauseOwnerOf(redis, topicKey(slow));
		const tick = interval(17);
		const started = Date.now();
		const latencies = [];
		for (let i = 0; i < 5; i++) {
			await redis.hpexpire(topicKey(healthy), 2000, 'FIELDS', 1, 'u-healthy');
			tick();
			await vi.waitFor(async () => expect(await fieldTtl()).toBeGreaterThan(25000), { timeout: 400, interval: 10 });
			latencies.push(presence.metrics().heartbeatLatencyMs);
		}
		expect(Date.now() - started).toBeLessThan(2500);
		// Each tick reads the age of the refresh still waiting on the paused master.
		for (let i = 2; i < 5; i++) expect(latencies[i]).toBeGreaterThan(latencies[i - 1]);
	} finally {
		await presence?.destroy();
		await resetBackendKeys(client);
		await client.quit();
	}
});

it('keeps cursor snapshots and sweeps landing on a healthy master while another master is paused', async () => {
	const interval = captureIntervals();
	const client = clusterRedisClient('cursor-progress:');
	const redis = client.redis;
	let io;
	try {
		await redis.cluster('info');
		const activeTopics = new Set();
		io = createRedisIo({ client, redis, cursorTtl: 1, snapshotIntervalMs: 50, activeTopics });
		const [slow, healthy] = onTwoMasters(redis, topic => io.hashKey(topic));
		activeTopics.add(slow).add(healthy);
		io.startSnapshotTimer();
		io.startCleanupTimer();
		const flush = interval(50);
		const sweep = interval(10000);
		await pauseOwnerOf(redis, io.hashKey(slow));
		const started = Date.now();
		for (let x = 1; x <= 5; x++) {
			io.queueSnapshot(slow, 'k', {}, { x });
			io.queueSnapshot(healthy, 'k', {}, { x });
			flush();
			await vi.waitFor(async () => expect(JSON.parse(await redis.hget(io.hashKey(healthy), 'k')).data).toEqual({ x }), { timeout: 400, interval: 10 });
			await redis.hset(io.hashKey(healthy), 'stale', JSON.stringify({ user: 'gone', data: {}, ts: Date.now() - 5000 }));
			sweep();
			await vi.waitFor(async () => expect(await redis.hexists(io.hashKey(healthy), 'stale')).toBe(0), { timeout: 400, interval: 10 });
		}
		expect(Date.now() - started).toBeLessThan(2500);
	} finally {
		io?.dispose();
		await resetBackendKeys(client);
		await client.quit();
	}
});
