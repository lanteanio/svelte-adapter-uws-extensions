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

const HASH_SLOTS = 16384;

/**
 * Extract the part of a key Redis hashes, matching its `keyHashSlot` rule
 * exactly: if the key contains a `{` and a `}` follows it with at least one
 * character in between, only that substring is hashed; otherwise the whole key
 * is. `{user}.x` -> `user`, `foo{bar}{zap}` -> `bar`, `foo{{bar}}zap` -> `{bar`,
 * `foo{}{bar}` -> `foo{}{bar}` (the first tag is empty, so the whole key).
 *
 * @param {string} key
 * @returns {string}
 */
function hashTag(key) {
	const open = key.indexOf('{');
	if (open === -1) return key;
	const close = key.indexOf('}', open + 1);
	if (close === -1 || close === open + 1) return key;
	return key.slice(open + 1, close);
}

/**
 * CRC-16/XMODEM (CCITT, polynomial 0x1021, no input/output reflection, zero
 * init) over a byte buffer - the exact variant Redis Cluster uses for slot
 * assignment. The conformance check value for the ASCII string "123456789" is
 * 0x31C3 (12739).
 *
 * @param {Buffer | Uint8Array} bytes
 * @returns {number}
 */
function crc16(bytes) {
	let crc = 0;
	for (let i = 0; i < bytes.length; i++) {
		crc ^= bytes[i] << 8;
		for (let j = 0; j < 8; j++) {
			crc = (crc & 0x8000) ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
		}
	}
	return crc;
}

/**
 * Compute the Redis Cluster hash slot (0 - 16383) for a key, client-side -
 * identical to `CLUSTER KEYSLOT key`, which is `CRC16(hashTag(key)) % 16384`.
 * Lets the sharded plugins resolve a channel's slot with no network round trip.
 *
 * The slot a key maps to is topology-independent and never changes; only the
 * node that currently owns a slot can move on a reshard, which is resolved
 * separately against `CLUSTER SLOTS`. The key is hashed as its UTF-8 bytes,
 * matching how Redis hashes the key bytes on the wire.
 *
 * @param {string} key
 * @returns {number} slot in the range [0, 16383]
 */
export function keySlot(key) {
	return crc16(Buffer.from(hashTag(key), 'utf8')) % HASH_SLOTS;
}
