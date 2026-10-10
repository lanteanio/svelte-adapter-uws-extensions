import { hashFieldTTLSupport } from '../../../src/shared/redis-version.js';
import { isCluster } from '../../../src/shared/cluster.js';
import { createIdempotencyStore } from '../../../src/redis/idempotency.js';
import { ACQUIRE_SCRIPT } from '../../../src/redis/_idempotency-ownership.js';
import { waitRedisMs } from '../helpers/backend-clock.js';
import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { idempotencyErasureContract, pauseIdempotencyCommand } from '../../helpers/idempotency-erasure-contract.js';
let client;
beforeAll(() => { client = createBackendClient({ keyPrefix: 'inttest-idempotency-erasure:' }); });
beforeEach(async () => { await resetBackendKeys(client); });
afterAll(async () => { await client.quit(); });
idempotencyErasureContract(() => client);

const identity = { tenant: 'tenant-a', user: 'alice' };
const indexKey = () => client.key('idem:byuser:v2\0tenant-a\0alice');

it('retains pending ownership beyond a shorter result lifetime', async () => {
	const store = createIdempotencyStore(client, { acquireTtl: 5 });
	const slot = await store.acquire('pending', 1, identity);
	await waitRedisMs(client, 1200);
	expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
	await expect(slot.commit('late')).rejects.toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
	expect(await client.redis.get(client.key('idem:pending'))).toBeNull();
});

it('bounds normal index retention after acknowledgement while covering a late commit', async () => {
	const store = createIdempotencyStore(client, { acquireTtl: 3 });
	const slot = await store.acquire('bounded', 2, identity);
	const remaining = await client.redis.pttl(indexKey());
	expect(remaining).toBeGreaterThan(4000);
	expect(remaining).toBeLessThanOrEqual(5000);
	await waitRedisMs(client, 1100);
	await slot.commit('result');
	const primaryTtl = await client.redis.pttl(client.key('idem:bounded'));
	expect(await client.redis.pttl(indexKey())).toBeGreaterThan(primaryTtl);
	expect(await store.purgeUser('tenant-a', 'alice')).toBe(1);
});

it('keeps another unacknowledged registration durable after older armed entries expire', async () => {
	const store = createIdempotencyStore(client, { acquireTtl: 1 });
	await (await store.acquire('older', 1, identity)).commit('old');
	const pause = pauseIdempotencyCommand(client.redis, ACQUIRE_SCRIPT, args => args[1] === client.key('idem:delayed'));
	const acquiring = store.acquire('delayed', 1, identity).then(value => ({ value }), error => ({ error }));
	try {
		await pause.entered;
		expect(await client.redis.pttl(indexKey())).toBe(-1);
		await waitRedisMs(client, 2200);
		expect(await client.redis.hget(indexKey(), '@unarmed')).toBe('1');
		expect(await client.redis.pttl(indexKey())).toBe(-1);
		expect(await store.purgeUser('tenant-a', 'alice')).toBe(0);
	} finally { pause.release(); pause.restore(); }
	expect((await acquiring).error).toMatchObject({ code: 'IDEMPOTENCY_LEASE_LOST' });
	expect(await client.redis.get(client.key('idem:delayed'))).toBeNull();
});


it('bounds retention when field expiry is denied after the armed record is written', async () => {
	const clustered = isCluster(client.redis);
	const nodes = clustered ? client.redis.nodes('all') : [client.redis];
	const supported = hashFieldTTLSupport(await client.redis.info('server')).supported === true;
	const username = 'idempotency-field-expiry-test', password = 'isolated-integration-only';
	let restricted;
	try {
		await Promise.all(nodes.map(node => node.acl('SETUSER', username, 'reset', 'on', '>' + password, '~*', '+@all', ...(supported ? ['-hpexpire'] : []))));
		restricted = clustered
			? client.redis.duplicate([], { lazyConnect: true, redisOptions: { ...client.redis.options.redisOptions, username, password } })
			: client.redis.duplicate({ lazyConnect: true, username, password });
		await restricted.connect();
		await restricted.ping();
		const store = createIdempotencyStore({ ...client, redis: restricted }, { acquireTtl: 1 });
		const acquiring = store.acquire('field-expiry-failure', 1, identity);
		if (supported) await expect(acquiring).rejects.toThrow(/permission|NOPERM|ACL|can't run/i);
		else expect((await acquiring).acquired).toBe(true);
		expect(await client.redis.hget(indexKey(), '@unarmed')).toBe('0');
		const ttl = await client.redis.pttl(indexKey());
		expect(ttl).toBeGreaterThan(1000);
		expect(ttl).toBeLessThanOrEqual(2000);
		await waitRedisMs(client, 2200);
		expect(await client.redis.exists(indexKey())).toBe(0);
		expect(await client.redis.get(client.key('idem:field-expiry-failure'))).toBeNull();
	} finally {
		restricted?.disconnect();
		await Promise.all(nodes.map(node => node.acl('DELUSER', username)));
	}
});
