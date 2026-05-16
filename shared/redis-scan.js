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
	const targets = isCluster(redis) ? /** @type {any} */ (redis).nodes('master') : [redis];
	for (const node of targets) {
		await scanOneNode(node, pattern, isCluster(redis));
	}
}

function isCluster(redis) {
	return redis !== null && typeof redis === 'object' && typeof (/** @type {any} */ (redis).nodes) === 'function';
}

async function scanOneNode(node, pattern, perKeyUnlink) {
	let cursor = '0';
	do {
		const [nextCursor, keys] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
		cursor = nextCursor;
		if (keys.length === 0) continue;
		if (perKeyUnlink) {
			// Cluster: UNLINK is multi-key and rejects with CROSSSLOT if the
			// batch spans hash slots. Per-key UNLINK is slower but correct.
			// `clear()` is an admin / test-reset operation, not on the hot
			// path, so the throughput delta does not matter.
			for (const key of keys) {
				await node.unlink(key);
			}
		} else {
			await node.unlink(...keys);
		}
	} while (cursor !== '0');
}
