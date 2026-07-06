/**
 * Cached-script eval that works on ANY ioredis instance - a single-node Redis, a
 * Cluster, or the in-process test double - however the client was created (the
 * `createRedisClient` wrapper, or a raw `new Redis.Cluster(...)` a cluster
 * deployment builds itself). Routes a hot Lua script through EVALSHA via ioredis
 * `defineCommand`, which owns the EVALSHA send, the NOSCRIPT reload, and (on a
 * cluster) loading the script on each node, so the ~40-byte SHA ships per call
 * instead of the full body. A drop-in for `redis.eval(script, numKeys,
 * ...keysAndArgs)`: same argument order and reply shape.
 *
 * State is per-instance (a WeakMap keyed by the redis instance): the script text
 * -> defined-command-name map plus a sequence counter, so two clients never
 * collide and the cache is collected with the instance. Each unique script text
 * is registered exactly once per instance, so `defineCommand` is never called
 * twice for the same script.
 *
 * @module svelte-adapter-uws-extensions/shared/eval-cached
 */

/** @type {WeakMap<any, { commands: Map<string, string>, seq: number }>} */
const registries = new WeakMap();

function registryFor(redis) {
	let reg = registries.get(redis);
	if (!reg) {
		reg = { commands: new Map(), seq: 0 };
		registries.set(redis, reg);
	}
	return reg;
}

/**
 * @param {any} redis an ioredis Redis or Cluster instance (or the test double)
 * @param {string} script the Lua source
 * @param {number} numKeys number of KEYS arguments
 * @param {...any} args the KEYS then ARGV values
 * @returns {Promise<any>} the script's reply
 */
export function evalCached(redis, script, numKeys, ...args) {
	const reg = registryFor(redis);
	let name = reg.commands.get(script);
	if (name === undefined) {
		name = 'ecmd' + (reg.seq++);
		redis.defineCommand(name, { lua: script });
		reg.commands.set(script, name);
	}
	return redis[name](numKeys, ...args);
}
