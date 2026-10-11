import { expect, it, vi } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createConnectionRegistry } from '../../../src/redis/registry.js';
import { keySlot } from '../../../src/shared/cluster.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../../src/shared/runtime.js';
import { mockPlatform } from '../../helpers/mock-platform.js';
import { mockWs } from '../../helpers/mock-ws.js';
import { WS_SESSION_ID } from 'svelte-adapter-uws/testing';

it('keeps registry TTLs fresh on a healthy master while another master is paused', async () => {
	const intervals = [];
	setRuntimeEnv({ timers: {
		setInterval(fn, delay) { const timer = { fn, delay, unref() {} }; intervals.push(timer); return timer; },
		clearInterval() {}
	} });
	const client = createBackendClient({ keyPrefix: 'heartbeat-progress:' });
	const redis = client.redis;
	let tracker;
	try {
		await redis.cluster('info');
		let slow, healthy;
		for (let i = 0; i < 100; i++) {
			const user = 'user-' + i;
			if (!slow) slow = user;
			else if (redis.slots[keySlot(client.key('conns:' + slow))][0]
				!== redis.slots[keySlot(client.key('conns:' + user))][0]) { healthy = user; break; }
		}
		expect(healthy).toBeTypeOf('string');
		tracker = createConnectionRegistry(client, { identify: ws => ws.getUserData().id, heartbeat: 17, ttl: 30 });
		const platform = mockPlatform();
		await tracker.hooks.open(mockWs({ id: slow, [WS_SESSION_ID]: slow }), { platform });
		await tracker.hooks.open(mockWs({ id: healthy, [WS_SESSION_ID]: healthy }), { platform });
		const slowKey = client.key('conns:' + slow);
		const healthyKey = client.key('conns:' + healthy);
		expect(await redis.exists(slowKey)).toBe(1);
		expect(await redis.exists(healthyKey)).toBe(1);
		const owner = redis.slots[keySlot(slowKey)][0];
		const mapped = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP)[owner];
		const node = redis.nodes('master').find(candidate => candidate.options.host + ':' + candidate.options.port === owner
			|| (mapped && candidate.options.host === mapped.host && candidate.options.port === mapped.port));
		expect(node).toBeDefined();
		await node.client('PAUSE', 3000, 'ALL');
		const tick = intervals.find(timer => timer.delay === 17).fn;
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
		resetRuntimeEnv();
	}
});
