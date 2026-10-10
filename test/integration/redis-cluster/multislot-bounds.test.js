import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { execMultiSlot, keySlot } from '../../../src/shared/cluster.js';
import { evalCachedName } from '../../../src/shared/eval-cached.js';
import { scanAndUnlink } from '../../../src/shared/redis-scan.js';
import { MAX_REDIS_PIPELINE_COMMANDS } from '../../../src/shared/caps.js';

let redis;
const prefix = 'multislot-bounds:';
beforeAll(async () => { redis = clusterClient(); await redis.cluster('info'); });
afterAll(async () => { if (redis) { await scanAndUnlink(redis, prefix + '*'); await redis.quit(); } });

it('writes bounded owner pipelines across all masters with index-aligned results', async () => {
	const commands = Array.from({ length: 1800 }, (_, i) => ['set', prefix + i, String(i)]);
	const pipeline = redis.pipeline.bind(redis);
	const sizes = [];
	redis.pipeline = (...args) => {
		const pipe = pipeline(...args);
		const exec = pipe.exec.bind(pipe);
		pipe.exec = (...execArgs) => { sizes.push(pipe.length); return exec(...execArgs); };
		return pipe;
	};
	try {
		expect(await execMultiSlot(redis, commands)).toEqual(commands.map(() => [null, 'OK']));
		expect(Math.max(...sizes)).toBeLessThanOrEqual(MAX_REDIS_PIPELINE_COMMANDS);
		expect(sizes.length).toBeLessThan(15);
		const replies = await execMultiSlot(redis, commands.map(([, key]) => ['get', key]));
		expect(replies).toEqual(commands.map(([, , value]) => [null, value]));
	} finally { redis.pipeline = pipeline; }
});

it('repairs a partially moved pipeline without executing successful writes twice', async () => {
	let left, right;
	for (let i = 0; i < 100; i++) {
		const key = prefix + 'move:' + i;
		if (!left) left = key;
		else if (redis.slots[keySlot(left)][0] !== redis.slots[keySlot(key)][0]) { right = key; break; }
	}
	expect(right).toBeTypeOf('string');
	await redis.del(left); await redis.del(right);
	// Simulate a stale local topology after resharding. The real server returns
	// MOVED for one write while the other succeeds on this pipeline's owner.
	redis.slots[keySlot(right)] = redis.slots[keySlot(left)];
	redis._groupsBySlot[keySlot(right)] = redis._groupsBySlot[keySlot(left)];
	const script = 'return redis.call("INCR", KEYS[1])';
	expect(await execMultiSlot(redis, [['eval', script, 1, left], ['eval', script, 1, right]])).toEqual([[null, 1], [null, 1]]);
	expect(await redis.get(left)).toBe('1');
	expect(await redis.get(right)).toBe('1');
});


it('routes cached scripts in bounded owner pipelines', async () => {
	const commands = Array.from({ length: 900 }, (_, i) => ['set', prefix + 'cached:' + i, String(i)]);
	expect(await execMultiSlot(redis, commands)).toEqual(commands.map(() => [null, 'OK']));
	const name = evalCachedName(redis, 'return redis.call("GET", KEYS[1])');
	expect(await execMultiSlot(redis, commands.map(([, key]) => [name, 1, key])))
		.toEqual(commands.map(([, , value]) => [null, value]));
});

it('completes healthy owner work while another master is paused', async () => {
	// Refresh after the preceding stale-topology test.
	await new Promise(resolve => redis.refreshSlotsCache(resolve));
	let slowKey, fastKey;
	for (let i = 0; i < 100; i++) {
		const key = prefix + '{fair' + i + '}';
		if (!slowKey) slowKey = key;
		else if (redis.slots[keySlot(slowKey)][0] !== redis.slots[keySlot(key)][0]) { fastKey = key; break; }
	}
	expect(fastKey).toBeTypeOf('string');
	const owner = redis.slots[keySlot(slowKey)][0];
	const mapped = JSON.parse(process.env.INTEGRATION_REDIS_CLUSTER_NAT_MAP)[owner];
	const node = redis.nodes('master').find(candidate => candidate.options.host + ':' + candidate.options.port === owner || (mapped && candidate.options.host === mapped.host && candidate.options.port === mapped.port));
	expect(node).toBeDefined();
	const count = MAX_REDIS_PIPELINE_COMMANDS * 20;
	const slow = Array.from({ length: count }, (_, i) => ['set', slowKey + ':' + i, 'slow']);
	const fast = Array.from({ length: count }, (_, i) => ['set', fastKey + ':' + i, 'fast']);
	let done = false;
	await node.client('PAUSE', 2000, 'ALL');
	const pending = execMultiSlot(redis, [...slow, ...fast]).finally(() => { done = true; });
	try {
		await vi.waitFor(async () => expect(await redis.get(fast.at(-1)[1])).toBe('fast'), { timeout: 1000, interval: 10 });
		expect(done).toBe(false);
		expect(await execMultiSlot(redis, [['get', fast[0][1]]])).toEqual([[null, 'fast']]);
		expect(await pending).toEqual([...slow, ...fast].map(() => [null, 'OK']));
	} finally { await pending; }
});
