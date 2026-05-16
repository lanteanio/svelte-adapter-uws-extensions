/**
 * Mass-disconnect bench for `createPresence` LEAVE_SCRIPT.
 *
 * Exercises the LEAVE Lua script directly under a topic with many existing
 * presence fields. Confirms per-leave Redis-blocked Lua time is independent
 * of the topic's total field count (HLEN check on a per-user hash, not a
 * scan of the per-topic hash).
 *
 * Three profiles, each disconnects N=1000 users from a topic with varying
 * noise field counts:
 *   1. M=1000  noise fields   -- chat room baseline
 *   2. M=10000 noise fields   -- auctions / mid-scale chat
 *   3. M=100000 noise fields  -- pathological / stress
 *
 * For each profile the bench reports:
 *   - pipelined leaveAll wall-clock (production shape)
 *   - per-leave amortized cost
 *   - broadcasts emitted (must equal N - users fully gone after the leave)
 *
 * Usage:
 *   docker compose -f test/integration/docker-compose.yml up -d --wait
 *   BENCH_REDIS_URL=redis://localhost:56379 node bench/02-presence-leave-mass-disconnect.mjs
 *
 * Environment:
 *   BENCH_REDIS_URL    redis URL (default: integration stack at localhost:56379)
 *   BENCH_N            users to mass-disconnect per profile (default: 1000)
 *   BENCH_PROFILES     comma-separated noise sizes (default: 1000,10000,100000)
 *
 * Requires Redis 7.4+ (the LEAVE script uses per-field HEXPIRE TTL primitives
 * via the JOIN side; the LEAVE itself is HDEL + HLEN, no per-field TTL calls).
 */

import { performance } from 'node:perf_hooks';
import Redis from 'ioredis';

const URL = process.env.BENCH_REDIS_URL || process.env.INTEGRATION_REDIS_URL || 'redis://localhost:56379';
const N = parseInt(process.env.BENCH_N || '1000', 10);
const PROFILES = (process.env.BENCH_PROFILES || '1000,10000,100000')
	.split(',')
	.map((s) => parseInt(s.trim(), 10))
	.filter((n) => Number.isFinite(n) && n > 0);

const TTL_SEC = 90;
const TTL_MS = TTL_SEC * 1000;
const INSTANCE = 'bench-instance-aabb';
const PREFIX = 'bench-presence:';
const TOPIC = 'room';
const topicHashKey = PREFIX + 'presence:topic:' + TOPIC;
const userHashKey = (userKey) => PREFIX + 'presence:user:' + TOPIC + ':' + userKey;

const JOIN_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]
local topicHashValue = ARGV[3]
local newTs = tonumber(ARGV[4])
local ttlMs = tonumber(ARGV[5])
if newTs == nil or ttlMs == nil then return redis.error_reply('PRESENCE_JOIN: newTs/ttlMs must be numeric') end
local wasEmpty = (redis.call('HLEN', userKey) == 0)
redis.call('HSET', userKey, instanceId, newTs)
redis.call('HPEXPIRE', userKey, ttlMs, 'FIELDS', 1, instanceId)
local existing = redis.call('HGET', topicKey, userKeyStr)
local shouldWrite = true
if existing then
  local ok, parsed = pcall(cjson.decode, existing)
  local existingTs = ok and parsed and tonumber(parsed.ts) or 0
  if newTs < existingTs then shouldWrite = false end
end
if shouldWrite then
  redis.call('HSET', topicKey, userKeyStr, topicHashValue)
end
redis.call('HPEXPIRE', topicKey, ttlMs, 'FIELDS', 1, userKeyStr)
return wasEmpty and 1 or 0
`;

const LEAVE_SCRIPT = `
local userKey = KEYS[1]
local topicKey = KEYS[2]
local instanceId = ARGV[1]
local userKeyStr = ARGV[2]
redis.call('HDEL', userKey, instanceId)
if redis.call('HLEN', userKey) == 0 then
  redis.call('HDEL', topicKey, userKeyStr)
  return 1
end
return 0
`;

function fmt(n, digits = 2) {
	return n.toFixed(digits).padStart(8);
}

async function wipe(r) {
	let cursor = '0';
	do {
		const [next, keys] = await r.scan(cursor, 'MATCH', PREFIX + '*', 'COUNT', 1000);
		cursor = next;
		if (keys.length > 0) await r.unlink(...keys);
	} while (cursor !== '0');
}

// ioredis buffers the entire pipeline in memory before sending; planting
// 100k EVAL calls in one shot saturates the Node heap and the Redis
// command queue. Chunk in batches so the bench scales smoothly to large
// noise sizes.
const PLANT_BATCH = 2000;

async function plantNoise(r, count) {
	const now = Date.now();
	for (let start = 0; start < count; start += PLANT_BATCH) {
		const end = Math.min(start + PLANT_BATCH, count);
		const pipe = r.pipeline();
		for (let i = start; i < end; i++) {
			const key = userHashKey('noise-user-' + i);
			const value = JSON.stringify({ data: { id: 'noise-user-' + i }, ts: now });
			pipe.eval(
				JOIN_SCRIPT, 2,
				key, topicHashKey,
				'noise-instance-' + i, 'noise-user-' + i, value, String(now), String(TTL_MS)
			);
		}
		await pipe.exec();
	}
}

async function joinUsers(r, count) {
	const now = Date.now();
	for (let start = 0; start < count; start += PLANT_BATCH) {
		const end = Math.min(start + PLANT_BATCH, count);
		const pipe = r.pipeline();
		for (let i = start; i < end; i++) {
			const key = userHashKey('user-' + i);
			const value = JSON.stringify({ data: { id: 'user-' + i, name: 'User ' + i }, ts: now });
			pipe.eval(
				JOIN_SCRIPT, 2,
				key, topicHashKey,
				INSTANCE, 'user-' + i, value, String(now), String(TTL_MS)
			);
		}
		await pipe.exec();
	}
}

async function runProfile(r, noiseCount) {
	await wipe(r);
	await plantNoise(r, noiseCount);
	await joinUsers(r, N);

	const totalFields = await r.hlen(topicHashKey);
	if (totalFields !== noiseCount + N) {
		throw new Error(`setup: expected HLEN ${noiseCount + N}, got ${totalFields}`);
	}

	const pipe = r.pipeline();
	for (let i = 0; i < N; i++) {
		pipe.eval(
			LEAVE_SCRIPT, 2,
			userHashKey('user-' + i), topicHashKey,
			INSTANCE, 'user-' + i
		);
	}

	const t0 = performance.now();
	const results = await pipe.exec();
	const elapsed = performance.now() - t0;

	const broadcasts = results.filter(([err, val]) => !err && val === 1).length;
	return { elapsed, broadcasts, totalFields };
}

async function main() {
	const r = new Redis(URL);
	try {
		await r.ping();
		const info = await r.info('server');
		const versionMatch = /redis_version:([^\r\n]+)/.exec(info);
		const version = versionMatch ? versionMatch[1] : 'unknown';
		const versionParts = /(\d+)\.(\d+)/.exec(version);
		if (versionParts && (parseInt(versionParts[1]) < 7 || (parseInt(versionParts[1]) === 7 && parseInt(versionParts[2]) < 4))) {
			console.error('bench: requires Redis 7.4+, got ' + version);
			process.exit(1);
		}

		console.log('Mass-disconnect bench: presence LEAVE_SCRIPT under noise');
		console.log('Redis: ' + version + ' (' + URL + '), ioredis pipeline, N=' + N + ' users mass-disconnect');
		console.log('');
		console.log('  noise (M)  |  total fields  |  pipelined ms  |  per-leave ms  |  broadcasts');
		console.log('  ---------- + -------------- + -------------- + -------------- + -----------');

		for (const noise of PROFILES) {
			const { elapsed, broadcasts, totalFields } = await runProfile(r, noise);
			console.log(
				'  ' + String(noise).padStart(10) +
				'  |  ' + String(totalFields).padStart(12) +
				'  |  ' + fmt(elapsed, 1) +
				'  |  ' + fmt(elapsed / N, 4) +
				'  |  ' + String(broadcasts).padStart(7) + '/' + N
			);
		}

		await wipe(r);
		console.log('');
		console.log('Expected: per-leave wall-clock is roughly constant across noise sizes - the');
		console.log('LEAVE script does HDEL + HLEN on the per-user hash (typically 1 field), not');
		console.log('a scan of the per-topic hash. Per-leave cost is dominated by ioredis pipeline');
		console.log('overhead, not by topic size.');
	} finally {
		await r.quit();
	}
}

main().catch((e) => {
	console.error('bench failed:', e);
	process.exit(1);
});
