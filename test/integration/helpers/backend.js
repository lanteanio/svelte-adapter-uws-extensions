// DRY env-switched backend for the integration suites.
//
// The same test bodies run against two backends, selected by the
// INTEGRATION_BACKEND env var (default 'solo'):
//   - 'solo'    : a standalone Redis via createRedisClient (the default tier).
//   - 'cluster' : a real Redis Cluster (3 masters + 3 replicas) via clusterClient,
//                 wrapped in the SAME { redis, key, duplicate, quit } shape the
//                 suites already consume, so a single set of suites verifies both
//                 deployments (no duplicated files - credo rule 11).
//
// In 'solo' mode this is byte-for-byte the construction the suites used before,
// so the standalone tier is unchanged. In 'cluster' mode the wrapper routes
// every call to an ioredis.Cluster; the surfaces that Cluster cannot satisfy
// (cross-slot multi-key Lua, cross-slot pipelines, per-node SCAN, keys without a
// shared hash tag) surface as real errors the cluster-mirror run maps and
// documents - "map the gaps first": you cannot fix a cluster bug you have not
// surfaced.
import { createRedisClient } from '../../../redis/index.js';
import { scanAndUnlink } from '../../../shared/redis-scan.js';
import { clusterClient } from './cluster-client.js';

/** The active backend mode. Read lazily so vitest env injection ordering does not matter. */
export function backendMode() {
	return process.env.INTEGRATION_BACKEND === 'cluster' ? 'cluster' : 'solo';
}

/** True when the suites are running against the real Redis Cluster. */
export function isClusterBackend() {
	return backendMode() === 'cluster';
}

/**
 * Cluster-correct per-suite key reset. Routes through the same `scanAndUnlink`
 * the plugins use, which on a cluster fans SCAN across every master node and
 * UNLINKs one key at a time (so it neither misses other-node keys nor
 * cross-slots a batched UNLINK), and on solo is an ordinary SCAN + UNLINK. Use
 * in `beforeEach` instead of a bare `client.redis.scan()` loop so the cluster
 * mirror's gap map reflects plugin behavior, not test-harness cleanup that only
 * swept one node.
 * @param {import('../../../redis/index.js').RedisClient} client
 * @param {string} [pattern] - defaults to every key under the client's prefix
 */
export async function resetBackendKeys(client, pattern) {
	const target = pattern || client.key('*');
	// The cluster reset can hit transient node-connection churn ("Connection is
	// closed" while ioredis re-establishes a master link under the mirror's
	// many-client fan-out); retry a few times so a beforeEach wipe never flakes.
	// scanAndUnlink re-reads nodes('master') each attempt, so a reconnected node
	// is picked up. On solo there is one connection and the first attempt succeeds.
	let lastErr;
	for (let attempt = 0; attempt < 5; attempt++) {
		try {
			await scanAndUnlink(client.redis, target);
			return;
		} catch (err) {
			lastErr = err;
			const msg = (err && err.message) || '';
			if (!/Connection is closed|ECONNRESET|CLUSTERDOWN|Failed to refresh slots|ETIMEDOUT/i.test(msg)) {
				throw err;
			}
			await new Promise((r) => setTimeout(r, 100 * (attempt + 1)));
		}
	}
	throw lastErr;
}

/**
 * Construct a RedisClient-shaped backend for an integration suite.
 *
 * Drop-in for the suites' previous `createRedisClient({ url, keyPrefix,
 * autoShutdown: false })` - pass only `keyPrefix` (plus any ioredis option
 * overrides under `options`); the URL / node list and the autoShutdown:false
 * default are resolved per mode here, so a suite never references
 * INTEGRATION_REDIS_URL directly.
 *
 * @param {{ keyPrefix?: string, options?: import('ioredis').RedisOptions }} [opts]
 * @returns {import('../../../redis/index.js').RedisClient}
 */
export function createBackendClient(opts = {}) {
	const keyPrefix = opts.keyPrefix || '';

	if (backendMode() === 'cluster') {
		if (!process.env.INTEGRATION_REDIS_CLUSTER_NODES) {
			throw new Error(
				'INTEGRATION_BACKEND=cluster but INTEGRATION_REDIS_CLUSTER_NODES is unset; ' +
				'the cluster global-setup did not run'
			);
		}
		// Patient connection settings for the mirror tier. The suite fan-out
		// creates many short-lived cluster clients against the shared 6-node stack,
		// and clusterClient's default test-fast retry (give up after 3 tries) makes
		// a freshly-created client surface a transient "Connection is closed" when
		// its initial slot-refresh races under that contention. A bounded-but-patient
		// retry + ready-check rides out the establishment window without risking a
		// hang (the cluster is already cluster_state:ok by the time global-setup
		// returns). Caller-supplied options still win via the trailing spread.
		const clusterOpts = {
			enableReadyCheck: true,
			clusterRetryStrategy: (times) => (times >= 30 ? null : Math.min(times * 50, 500)),
			...opts.options
		};
		const cluster = clusterClient(clusterOpts);
		/** @type {Set<import('ioredis').Cluster>} */
		const duplicates = new Set();
		return {
			redis: cluster,
			keyPrefix,
			key: (k) => keyPrefix + k,
			duplicate(overrides) {
				const dup = clusterClient({ ...clusterOpts, ...overrides });
				duplicates.add(dup);
				return dup;
			},
			async quit() {
				await Promise.allSettled(
					[cluster, ...duplicates].map((c) => c.quit().catch(() => c.disconnect()))
				);
			}
		};
	}

	const url = process.env.INTEGRATION_REDIS_URL;
	if (!url) {
		throw new Error('INTEGRATION_REDIS_URL not set; the standalone global-setup did not run');
	}
	return createRedisClient({
		url,
		keyPrefix,
		autoShutdown: false,
		...(opts.options ? { options: opts.options } : {})
	});
}
