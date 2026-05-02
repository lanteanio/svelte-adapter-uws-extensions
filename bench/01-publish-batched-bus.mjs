/**
 * A/B bench for `wrapped.publishBatched` vs `wrapped.publish` loop on
 * createPubSubBus. Verifies the credo rule-4 hot-path gate for B16.
 *
 * Three profiles per the B16 spec:
 *   1. 50 events same-topic, two instances     -- bulk-import shape (biggest win)
 *   2. 5 events overlapping topics             -- room-state-reset shape
 *   3. 3 events disjoint topics                -- control: must sit inside noise
 *
 * Usage:
 *   docker compose -f test/integration/docker-compose.yml up -d --wait
 *   BENCH_REDIS_URL=redis://localhost:56379 node bench/01-publish-batched-bus.mjs
 *
 * Environment:
 *   BENCH_REDIS_URL    redis URL (default: integration stack at localhost:56379)
 *   BENCH_ITERATIONS   iterations per profile (default: 500)
 *   BENCH_WARMUP       warmup iterations (default: 50)
 *
 * Output:
 *   Per profile: emit-side wall-clock, end-to-end (publisher emits to subscriber
 *   receives last message), Redis PUBLISH command count, ratio vs baseline.
 */

import { performance } from 'node:perf_hooks';
import { createRedisClient } from '../redis/index.js';
import { createPubSubBus } from '../redis/pubsub.js';
import { mockPlatform } from '../testing/mock-platform.js';

const URL = process.env.BENCH_REDIS_URL || process.env.INTEGRATION_REDIS_URL || 'redis://localhost:56379';
const ITERATIONS = parseInt(process.env.BENCH_ITERATIONS || '500', 10);
const WARMUP = parseInt(process.env.BENCH_WARMUP || '50', 10);

function fmt(n, digits = 2) {
	return n.toFixed(digits).padStart(8);
}

function fmtCount(n) {
	return n.toString().padStart(7);
}

async function readPublishStat(client) {
	// Reads PUBLISH command call count from `INFO commandstats`. Captures
	// pipeline-routed publishes that an in-process spy on `client.redis.publish`
	// would miss (the pipeline submits commands directly to the socket).
	const info = await client.redis.info('commandstats');
	const match = info.match(/cmdstat_publish:calls=(\d+)/);
	return match ? parseInt(match[1], 10) : 0;
}

async function setup(channel) {
	const pubClient = createRedisClient({ url: URL, autoShutdown: false, keyPrefix: 'bench:' });
	const subClient = createRedisClient({ url: URL, autoShutdown: false, keyPrefix: 'bench:' });
	const platformA = mockPlatform();
	const platformB = mockPlatform();
	const busA = createPubSubBus(pubClient, { channel });
	const busB = createPubSubBus(subClient, { channel });
	await busA.activate(platformA);
	await busB.activate(platformB);

	return {
		pubClient, subClient, busA, busB, platformA, platformB,
		wrapped: busA.wrap(platformA)
	};
}

async function teardown(ctx) {
	await ctx.busA.deactivate();
	await ctx.busB.deactivate();
	await ctx.pubClient.quit();
	await ctx.subClient.quit();
}

async function waitFor(fn, timeoutMs = 30000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await new Promise((r) => setImmediate(r));
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

async function runProfile({ name, batchSize, makeMessages, drive }) {
	const channel = 'bench:' + Math.random().toString(16).slice(2);
	const ctx = await setup(channel);

	// Warmup
	for (let i = 0; i < WARMUP; i++) drive(ctx.wrapped, makeMessages(i));
	await waitFor(() => ctx.platformB.published.length >= WARMUP * batchSize);

	ctx.platformA.reset();
	ctx.platformB.reset();

	const pubsBefore = await readPublishStat(ctx.pubClient);
	const expected = ITERATIONS * batchSize;
	const emitStart = performance.now();
	for (let i = 0; i < ITERATIONS; i++) {
		drive(ctx.wrapped, makeMessages(i));
	}
	const emitEnd = performance.now();

	await waitFor(() => ctx.platformB.published.length >= expected);
	const recvEnd = performance.now();
	const pubsAfter = await readPublishStat(ctx.pubClient);

	const result = {
		name,
		emitMs: emitEnd - emitStart,
		e2eMs: recvEnd - emitStart,
		pubCount: pubsAfter - pubsBefore,
		events: expected,
		eventsPerSec: (expected / (recvEnd - emitStart)) * 1000
	};

	await teardown(ctx);
	return result;
}

function report(label, baseline, batched) {
	console.log(`\n${label}`);
	console.log('  variant            emit (ms)   e2e (ms)   redis pubs   events/s');
	console.log('  -----------------  ----------  ---------  -----------  ----------');
	console.log(`  publish loop       ${fmt(baseline.emitMs)}  ${fmt(baseline.e2eMs)}   ${fmtCount(baseline.pubCount)}   ${fmt(baseline.eventsPerSec, 0)}`);
	console.log(`  publishBatched     ${fmt(batched.emitMs)}  ${fmt(batched.e2eMs)}   ${fmtCount(batched.pubCount)}   ${fmt(batched.eventsPerSec, 0)}`);
	const e2eRatio = baseline.e2eMs / batched.e2eMs;
	const pubRatio = baseline.pubCount / batched.pubCount;
	console.log(`  e2e speedup:       ${fmt(e2eRatio)}x   |  redis-publish reduction: ${fmt(pubRatio)}x`);
}

async function main() {
	console.log(`bench: ${ITERATIONS} iterations, ${WARMUP} warmup, redis at ${URL}`);

	// Profile 1: bulk-import shape -- 50 events, same topic, two instances
	const p1a = await runProfile({
		name: 'publish loop x50',
		batchSize: 50,
		makeMessages: () => null,
		drive: (wrapped) => {
			for (let i = 0; i < 50; i++) wrapped.publish('bulk', 'updated', { i });
		}
	});
	const p1b = await runProfile({
		name: 'publishBatched x50',
		batchSize: 50,
		makeMessages: () => Array.from({ length: 50 }, (_, i) => ({
			topic: 'bulk', event: 'updated', data: { i }
		})),
		drive: (wrapped, msgs) => wrapped.publishBatched(msgs)
	});
	report('Profile 1: 50 events same-topic (bulk-import shape)', p1a, p1b);

	// Profile 2: room-state-reset -- 5 events, overlapping topics
	const p2a = await runProfile({
		name: 'publish loop x5 overlap',
		batchSize: 5,
		makeMessages: () => null,
		drive: (wrapped) => {
			wrapped.publish('room:1', 'reset', { v: 1 });
			wrapped.publish('room:1', 'state', { v: 2 });
			wrapped.publish('room:2', 'reset', { v: 3 });
			wrapped.publish('room:2', 'state', { v: 4 });
			wrapped.publish('audit', 'snapshot', { v: 5 });
		}
	});
	const p2b = await runProfile({
		name: 'publishBatched x5 overlap',
		batchSize: 5,
		makeMessages: () => [
			{ topic: 'room:1', event: 'reset', data: { v: 1 } },
			{ topic: 'room:1', event: 'state', data: { v: 2 } },
			{ topic: 'room:2', event: 'reset', data: { v: 3 } },
			{ topic: 'room:2', event: 'state', data: { v: 4 } },
			{ topic: 'audit', event: 'snapshot', data: { v: 5 } }
		],
		drive: (wrapped, msgs) => wrapped.publishBatched(msgs)
	});
	report('Profile 2: 5 events overlapping topics (room-state-reset shape)', p2a, p2b);

	// Profile 3: control -- 3 events, disjoint topics
	const p3a = await runProfile({
		name: 'publish loop x3 disjoint',
		batchSize: 3,
		makeMessages: () => null,
		drive: (wrapped) => {
			wrapped.publish('a', 'x', { i: 1 });
			wrapped.publish('b', 'y', { i: 2 });
			wrapped.publish('c', 'z', { i: 3 });
		}
	});
	const p3b = await runProfile({
		name: 'publishBatched x3 disjoint',
		batchSize: 3,
		makeMessages: () => [
			{ topic: 'a', event: 'x', data: { i: 1 } },
			{ topic: 'b', event: 'y', data: { i: 2 } },
			{ topic: 'c', event: 'z', data: { i: 3 } }
		],
		drive: (wrapped, msgs) => wrapped.publishBatched(msgs)
	});
	report('Profile 3: 3 events disjoint topics (control)', p3a, p3b);

	console.log('\nGate: profiles 1 + 2 must show meaningful e2e speedup; profile 3 must sit inside noise.');
}

main().catch((err) => {
	console.error(err);
	process.exit(1);
});
