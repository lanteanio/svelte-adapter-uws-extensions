// Bench: cursor reconnect snapshot fetch against real Redis.
//
// Current code (cursor.js:158) fires N independent redis.hgetall() promises
// in a for-loop. ioredis auto-pipelines when commands are queued in the same
// tick, but only when autoPipelining is enabled and the connection is idle.
// An explicit redis.pipeline() guarantees one round-trip regardless.
//
// Bench compares the two against the integration stack's Redis. Run as:
//   docker compose -p svelte-bench-cursor -f test/integration/docker-compose.yml up -d --wait
//   node --expose-gc test/_bench/cursor-reconnect.js
//   docker compose -p svelte-bench-cursor -f test/integration/docker-compose.yml down -v

import Redis from 'ioredis';
import { performance } from 'node:perf_hooks';

const PORT = Number(process.env.INTEGRATION_REDIS_HOST_PORT) || 56379;
const TOPIC_COUNTS = [10, 50, 200, 500];
const ITERS = 100;

async function populate(redis, topicCount) {
	const pipe = redis.pipeline();
	for (let t = 0; t < topicCount; t++) {
		const key = `bench:cursor:${t}`;
		for (let i = 0; i < 5; i++) {
			pipe.hset(key, `inst-${i}`, JSON.stringify({ user: { id: i }, data: { x: t, y: i }, ts: Date.now() }));
		}
	}
	await pipe.exec();
}

async function clear(redis, topicCount) {
	const keys = [];
	for (let t = 0; t < topicCount; t++) keys.push(`bench:cursor:${t}`);
	if (keys.length) await redis.unlink(...keys);
}

async function variantConcurrent(redis, topicCount) {
	const promises = [];
	for (let t = 0; t < topicCount; t++) {
		promises.push(redis.hgetall(`bench:cursor:${t}`));
	}
	await Promise.all(promises);
}

async function variantPipeline(redis, topicCount) {
	const pipe = redis.pipeline();
	for (let t = 0; t < topicCount; t++) {
		pipe.hgetall(`bench:cursor:${t}`);
	}
	await pipe.exec();
}

async function timeIt(label, fn, iters) {
	// Warmup
	for (let i = 0; i < 5; i++) await fn();
	const start = performance.now();
	for (let i = 0; i < iters; i++) await fn();
	const elapsed = performance.now() - start;
	const usPerOp = (elapsed * 1000) / iters;
	console.log(`  ${label.padEnd(24)} ${usPerOp.toFixed(1)} µs/call  (${iters} iters, ${elapsed.toFixed(0)} ms)`);
	return usPerOp;
}

async function main() {
	const redis = new Redis({ host: 'localhost', port: PORT, lazyConnect: true });
	try {
		await redis.connect();
		await redis.ping();
	} catch (err) {
		console.error(`Cannot connect to Redis at localhost:${PORT}.`);
		console.error('Spin up the integration stack first:');
		console.error('  docker compose -f test/integration/docker-compose.yml up -d --wait');
		console.error('Original error:', err.message);
		process.exit(1);
	}

	// Match cursor.js's Redis configuration: explicitly disable auto-pipelining
	// when measuring concurrent variant so we test the worst-case path. Then
	// repeat with auto-pipelining on. ioredis exposes this on the constructor.
	for (const autoPipelining of [false, true]) {
		const r = new Redis({
			host: 'localhost', port: PORT,
			enableAutoPipelining: autoPipelining,
			lazyConnect: true
		});
		await r.connect();

		console.log(`\n=== enableAutoPipelining: ${autoPipelining} ===`);
		for (const n of TOPIC_COUNTS) {
			await populate(r, n);
			console.log(`Topics: ${n}`);
			const a = await timeIt('concurrent hgetall', () => variantConcurrent(r, n), ITERS);
			const b = await timeIt('explicit pipeline', () => variantPipeline(r, n), ITERS);
			const delta = ((b - a) / a) * 100;
			const sign = delta > 0 ? '+' : '';
			const verdict = delta < -2 ? 'WIN' : delta > 2 ? 'LOSS' : 'NEUTRAL';
			console.log(`  pipeline vs concurrent: ${sign}${delta.toFixed(1)}% [${verdict}]`);
			await clear(r, n);
		}
		await r.quit();
	}

	await redis.quit();
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
