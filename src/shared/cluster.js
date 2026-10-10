/**
 * Cluster-aware Redis helpers shared across the redis/ plugins.
 */

import { MAX_REDIS_PIPELINE_COMMANDS, MAX_REDIS_INFLIGHT_BATCHES } from './caps.js';
import { isCachedCommand } from './eval-cached.js';

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

const executionQueues = new WeakMap();

// Leave half the client budget available when one Cluster allocation stalls.
const MAX_NODE_BATCHES = Math.max(1, Math.floor(MAX_REDIS_INFLIGHT_BATCHES / 2));
function withExecutionSlot(redis, run, owner = null) {
	let state = executionQueues.get(redis);
	if (!state) { state = { active: 0, owners: new Map(), queue: [] }; executionQueues.set(redis, state); }
	const available = group => group === null || (state.owners.get(group) || 0) < MAX_NODE_BATCHES;
	const drain = () => {
		for (let i = 0; state.active < MAX_REDIS_INFLIGHT_BATCHES && i < state.queue.length;) {
			const next = state.queue[i];
			if (!available(next.owner)) { i++; continue; }
			state.queue.splice(i, 1);
			next.start();
		}
	};
	const start = () => {
		state.active++;
		if (owner !== null) state.owners.set(owner, (state.owners.get(owner) || 0) + 1);
		let result;
		try { result = run(); } catch (error) { result = Promise.reject(error); }
		return Promise.resolve(result).finally(() => {
			state.active--;
			if (owner !== null) {
				const left = state.owners.get(owner) - 1;
				if (left) state.owners.set(owner, left); else state.owners.delete(owner);
			}
			drain();
		});
	};
	if (state.active < MAX_REDIS_INFLIGHT_BATCHES && available(owner)) return start();
	return new Promise((resolve, reject) => {
		state.queue.push({ owner, start: () => start().then(resolve, reject) });
	});
}

const UNKEYED_GROUP = Symbol('unkeyed Redis commands');
const ROUTED_COMMANDS = Symbol('individually routed Redis commands');
const FIRST_KEY_METHODS = new Set(['spublish', 'get', 'set', 'del', 'unlink', 'expire', 'pexpire', 'hgetall', 'hset', 'hdel', 'hpexpire']);
function allocationGroup(redis, command) {
	const method = command[0];
	if (method === 'publish') return UNKEYED_GROUP;
	const key = FIRST_KEY_METHODS.has(method) ? command[1]
		: (method === 'eval' || method === 'evalsha') && Number(command[2]) > 0 ? command[3]
		: isCachedCommand(redis, method) && Number(command[1]) > 0 ? command[2] : null;
	if (typeof key !== 'string' || !Array.isArray(redis.slots)) return null;
	const prefix = redis.options?.keyPrefix || '';
	if (typeof prefix !== 'string') return null;
	const owners = redis.slots[keySlot(prefix + key)];
	return Array.isArray(owners) && owners.length ? owners.join(';') : null;
}

/**
 * Execute a set of independent single-slot commands and return their results
 * in ioredis pipeline shape: an array of `[err, value]` tuples index-aligned
 * with `commands`. Each command is a tuple `[method, ...args]`, e.g.
 * `['eval', SCRIPT, 2, k1, k2, arg]` or `['hpexpire', key, ms, 'FIELDS', 1, f]`.
 *
 * Commands are split into bounded pipelines with one shared concurrency budget
 * per client, with half that budget available to any one known Cluster allocation.
 * Cluster pipelines contain keys assigned to the same current node
 * allocation group; missing topology falls back to individually routed commands.
 * Explicit MOVED/ASK refusals retry only the refused command through Cluster,
 * never successful writes or ambiguous transport failures.
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
	const clustered = isCluster(redis);
	if (!clustered && commands.length <= MAX_REDIS_PIPELINE_COMMANDS) {
		return withExecutionSlot(redis, () => {
			const pipe = redis.pipeline();
			for (const [method, ...args] of commands) pipe[method](...args);
			return pipe.exec();
		}).catch(error => commands.map(() => [error, undefined]));
	}
	const groups = new Map();
	if (!clustered) groups.set('standalone', commands.map((_, i) => i));
	else {
		for (let i = 0; i < commands.length; i++) {
			const group = allocationGroup(redis, commands[i]);
			const id = group === null ? ROUTED_COMMANDS : group;
			let indexes = groups.get(id);
			if (!indexes) { indexes = []; groups.set(id, indexes); }
			indexes.push(i);
		}
	}
	const entries = Array.from(groups, ([group, indexes]) => ({ group, indexes, offset: 0 }));
	let cursor = 0;
	const scheduled = new Map();
	const results = new Array(commands.length);
	async function individual(index) {
		const [method, ...args] = commands[index];
		try { results[index] = [null, await redis[method](...args)]; }
		catch (error) { results[index] = [error, undefined]; }
	}
	async function worker() {
		while (entries.length) {
			let searched = 0;
			while (searched < entries.length && clustered && entries[cursor].group !== ROUTED_COMMANDS && (scheduled.get(entries[cursor].group) || 0) >= MAX_NODE_BATCHES) {
				cursor = (cursor + 1) % entries.length;
				searched++;
			}
			// Active workers resume the remaining groups when their calls settle.
			if (searched === entries.length) return;
			const entry = entries[cursor];
			const { group, indexes, offset } = entry;
			const batch = indexes.slice(offset, offset + (group === ROUTED_COMMANDS ? 1 : MAX_REDIS_PIPELINE_COMMANDS));
			entry.offset += batch.length;
			if (entry.offset === indexes.length) entries.splice(cursor, 1);
			else cursor++;
			if (cursor >= entries.length) cursor = 0;
			scheduled.set(group, (scheduled.get(group) || 0) + 1);
			await withExecutionSlot(redis, async () => {
				if (group === ROUTED_COMMANDS) { await individual(batch[0]); return; }
				let replies;
				try {
					const pipe = redis.pipeline();
					for (const index of batch) {
						const [method, ...args] = commands[index];
						pipe[method](...args);
					}
					replies = await pipe.exec();
				} catch (error) {
					// ioredis rejects this topology mismatch before writing the pipeline.
					if (clustered && error.message === 'All keys in the pipeline should belong to the same slots allocation group') {
						for (const index of batch) await individual(index);
						return;
					}
					for (const index of batch) results[index] = [error, undefined];
					return;
				}
				for (let i = 0; i < batch.length; i++) {
					const reply = replies[i];
					if (clustered && /^(MOVED|ASK) \d+ /.test(reply?.[0]?.message || '')) await individual(batch[i]);
					else results[batch[i]] = reply;
				}
			}, clustered && group !== ROUTED_COMMANDS ? group : null).finally(() => {
				scheduled.set(group, scheduled.get(group) - 1);
			});
		}
	}
	// Workers pull at most one batch each, so one large call never enqueues an
	// unbounded number of promises ahead of unrelated calls sharing the client.
	const settled = await Promise.allSettled(Array.from({ length: Math.min(MAX_REDIS_INFLIGHT_BATCHES, commands.length) }, worker));
	const failed = settled.find(result => result.status === 'rejected');
	if (failed) throw failed.reason;
	return results;
}

/**
 * The first per-command error in a pipeline, transaction or `execMultiSlot`
 * result, or null when every command succeeded. `exec()` resolves even when
 * individual commands fail (READONLY after a failover, OOM, NOPERM): each
 * failure is an `[err, value]` entry, never a rejection, so a caller that only
 * handles the rejection counts those commands as done.
 *
 * @param {Array<[Error | null, any]> | null | undefined} results
 * @returns {Error | null}
 */
export function firstCommandError(results) {
	if (!results) return null;
	for (let i = 0; i < results.length; i++) {
		const err = results[i] && results[i][0];
		if (err) return err;
	}
	return null;
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
