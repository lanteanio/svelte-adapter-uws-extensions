/**
 * Cluster-aware Redis helpers shared across the redis/ plugins.
 */

/**
 * True when the client is an ioredis Cluster (vs a standalone Redis).
 * Detected via the `nodes()` method, the canonical Cluster API surface.
 *
 * @param {import('ioredis').Redis | import('ioredis').Cluster} redis
 * @returns {boolean}
 */
export function isCluster(redis) {
	return redis !== null && typeof redis === 'object' && typeof (/** @type {any} */ (redis).nodes) === 'function';
}

/**
 * Execute a set of independent single-slot commands and return their results
 * in ioredis pipeline shape: an array of `[err, value]` tuples index-aligned
 * with `commands`. Each command is a tuple `[method, ...args]`, e.g.
 * `['eval', SCRIPT, 2, k1, k2, arg]` or `['hpexpire', key, ms, 'FIELDS', 1, f]`.
 *
 * On a standalone Redis the commands are batched into a single pipeline - one
 * round trip, the original throughput shape. On a Redis Cluster each command is
 * issued on its own so ioredis routes it to the node that owns its keys: a
 * single pipeline spanning multiple hash slots is delivered to just ONE sampled
 * node, which silently no-ops (a per-command MOVED lands in the results array,
 * never a thrown error) every command whose keys live on another node. The
 * per-command form follows MOVED to the owning node, so every command runs.
 *
 * Each command's own keys must still co-locate to one slot (e.g. via a shared
 * `{hash-tag}`); this helper fans commands across nodes, it does not let a
 * single multi-key command cross slots.
 *
 * @param {import('ioredis').Redis | import('ioredis').Cluster} redis
 * @param {Array<[string, ...any]>} commands
 * @returns {Promise<Array<[Error | null, any]>>}
 */
export async function execMultiSlot(redis, commands) {
	if (commands.length === 0) return [];
	if (!isCluster(redis)) {
		const pipe = redis.pipeline();
		for (const [method, ...args] of commands) pipe[/** @type {keyof typeof pipe} */ (method)](...args);
		return pipe.exec();
	}
	return Promise.all(
		commands.map(async ([method, ...args]) => {
			try {
				return [null, await (/** @type {any} */ (redis))[method](...args)];
			} catch (err) {
				return [/** @type {Error} */ (err), undefined];
			}
		})
	);
}
