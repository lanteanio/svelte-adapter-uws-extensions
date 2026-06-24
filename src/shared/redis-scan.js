import { isCluster } from './cluster.js';

/**
 * Iterate `SCAN MATCH pattern COUNT 100` over one node to exhaustion, invoking
 * `onKeys` with each non-empty batch of matched keys. The single definition of
 * the cursor loop, shared by the scan-collect and scan-and-delete helpers.
 *
 * @param {import('ioredis').Redis} node
 * @param {string} pattern
 * @param {(keys: string[]) => Promise<void> | void} onKeys
 */
async function scanNodeBatches(node, pattern, onKeys) {
	let cursor = '0';
	do {
		const [nextCursor, keys] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
		cursor = nextCursor;
		if (keys.length > 0) await onKeys(keys);
	} while (cursor !== '0');
}

/**
 * The cluster-aware set of nodes a keyspace scan must cover. A keyless command
 * like `SCAN` routes to ONE randomly-sampled node on a `Redis.Cluster`, returns
 * a cursor meaningful only for that node, and silently omits keys owned by the
 * others - so a cluster scan must enumerate each master independently. On a
 * standalone the single connection is the only node.
 *
 * @param {import('ioredis').Redis | import('ioredis').Cluster} redis
 * @returns {Array<import('ioredis').Redis>}
 */
function scanTargets(redis) {
	return isCluster(redis) ? /** @type {any} */ (redis).nodes('master') : [redis];
}

/**
 * Collect every key matching `pattern` across the keyspace without modifying
 * anything - the read-only counterpart of `scanAndUnlink`. Same cluster-aware
 * enumeration (one SCAN per master). Returns the matched keys in no particular
 * order; on a standalone the order is the server's SCAN order.
 *
 * @param {import('ioredis').Redis | import('ioredis').Cluster} redis
 * @param {string} pattern
 * @returns {Promise<string[]>}
 */
export async function scanKeys(redis, pattern) {
	const found = [];
	for (const node of scanTargets(redis)) {
		await scanNodeBatches(node, pattern, (keys) => { found.push(...keys); });
	}
	return found;
}

/**
 * SCAN with COUNT 100 across the keyspace and UNLINK every match.
 * Used by `clear()` admin methods to delete all keys for a given pattern
 * without blocking the server with a single `KEYS` call.
 *
 * Cluster awareness: ioredis routes a keyless command like `SCAN` to one
 * randomly-sampled node when called against a `Redis.Cluster` instance
 * (see `cluster/index.js#sendCommand` fallback to `getSampleInstance`).
 * Subsequent calls within the same loop may target different nodes, the
 * returned cursor is meaningful only for the node that produced it, and
 * keys owned by other nodes are silently skipped. Worse, batched UNLINK
 * (`unlink k1 k2 k3`) is a multi-key command and ioredis Cluster rejects
 * it with `CROSSSLOT Keys in request don't hash to the same slot` if the
 * batched keys span hash slots - which they routinely do even within a
 * single node's slot range.
 *
 * Fix: explicitly enumerate master nodes when the client is a Cluster
 * (detected via `typeof redis.nodes === 'function'`, the canonical
 * Cluster API surface) and run an independent SCAN per master. UNLINK
 * one key at a time on Cluster to avoid CROSSSLOT; on standalone, batch
 * UNLINK is still safe and preserves the original throughput shape.
 *
 * @param {import('ioredis').Redis | import('ioredis').Cluster} redis
 * @param {string} pattern
 */
export async function scanAndUnlink(redis, pattern) {
	const cluster = isCluster(redis);
	for (const node of scanTargets(redis)) {
		await scanNodeBatches(node, pattern, async (keys) => {
			if (cluster) {
				// Cluster: UNLINK is multi-key and rejects with CROSSSLOT if the
				// batch spans hash slots. Per-key UNLINK is slower but correct.
				// `clear()` is an admin / test-reset operation, not on the hot
				// path, so the throughput delta does not matter.
				for (const key of keys) await node.unlink(key);
			} else {
				await node.unlink(...keys);
			}
		});
	}
}
