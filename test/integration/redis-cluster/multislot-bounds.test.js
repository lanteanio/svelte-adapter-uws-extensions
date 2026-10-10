import { afterAll, beforeAll, expect, it } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { execMultiSlot, keySlot } from '../../../src/shared/cluster.js';
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
