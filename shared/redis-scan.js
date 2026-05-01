/**
 * SCAN with COUNT 100 across the keyspace and UNLINK every match.
 * Used by `clear()` admin methods to delete all keys for a given pattern
 * without blocking the server with a single `KEYS` call.
 *
 * Cluster note: ioredis routes a single SCAN against one shard's keyspace.
 * Cluster-wide cleanup needs to run this against each master node.
 *
 * @param {import('ioredis').Redis} redis
 * @param {string} pattern
 */
export async function scanAndUnlink(redis, pattern) {
	let cursor = '0';
	do {
		const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
		cursor = nextCursor;
		if (keys.length > 0) {
			await redis.unlink(...keys);
		}
	} while (cursor !== '0');
}
