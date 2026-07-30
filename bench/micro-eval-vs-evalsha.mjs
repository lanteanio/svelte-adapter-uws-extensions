// Bench: EVAL (full Lua body every call) vs evalCached (EVALSHA via ioredis
// defineCommand). Measures the wire + server-parse saving that routing hot
// scripts through evalCached buys - the win grows with script size and call
// frequency (the per-message rate-limit CONSUME script is the hot target).
//
// REQUIRES A LIVE REDIS (set REDIS_URL, default redis://localhost:6379); it
// self-skips with a message when none is reachable, so it never fails CI. This
// is the before/after harness to run on real iron before trusting the
// perf claim - the mock double cannot measure EVALSHA, only correctness.
//
// Run with: node bench/micro-eval-vs-evalsha.mjs

import { createRedisClient } from '../src/redis/index.js';
import { CONSUME_SCRIPT } from '../src/redis/token-bucket-script.js';
import { benchAsync, compare } from './micro-harness.mjs';

const url = process.env.REDIS_URL || 'redis://localhost:6379';

let client;
try {
	client = createRedisClient({ url, keyPrefix: 'bench:evalsha:', autoShutdown: false, options: { lazyConnect: true, maxRetriesPerRequest: 1 } });
	await client.redis.connect();
	await client.redis.ping();
} catch (err) {
	console.log(`skipped: no reachable Redis at ${url} (${err && err.message}).`);
	console.log('Set REDIS_URL to a live server to measure EVAL vs EVALSHA.');
	if (client) await client.quit().catch(() => {});
	process.exit(0);
}

const redis = client.redis;
const args = (k) => [CONSUME_SCRIPT, 1, client.key(k), 100, 60000, 1, 0];

// Warm: prime the connection and register the evalCached command (first call
// defines it, subsequent calls send only the SHA).
await redis.eval(...args('warm'));
await redis.evalCached(...args('warm'));

console.log(`Redis: ${url}`);
console.log(`CONSUME_SCRIPT length: ${CONSUME_SCRIPT.length} bytes shipped per EVAL call\n`);

const a = await benchAsync('eval       (full body/call)', () => redis.eval(...args('e')), { iters: 20000, warmup: 500 });
const b = await benchAsync('evalCached (EVALSHA/call)', () => redis.evalCached(...args('c')), { iters: 20000, warmup: 500 });
compare('evalCached vs eval', a, b);

await redis.del(client.key('warm'), client.key('e'), client.key('c')).catch(() => {});
await client.quit().catch(() => {});
