import { EventEmitter } from 'node:events';

// Count logical transport submissions at the existing injected Redis boundary.
// Pipeline members share one submission; Cluster node publishers and every
// connection duplicated from the client (subscribers, per-node connections)
// share this ledger, so SUBSCRIBE / UNSUBSCRIBE traffic counts like any command.
//
// A function an owner assigns onto a connection (the subscriber guard replaces
// subscribe / unsubscribe with versions that track and later send the call) is
// a shim, not a submission: it is returned uncounted, and the method it captured
// before replacing it counts when the shim actually sends.
export function countRedisIO(client) {
	let trips = 0;
	/** @type {string[]} */
	let log = [];
	const wrappers = new WeakMap();
	// Event plumbing and lifecycle calls are not command submissions.
	const local = new Set([...Object.getOwnPropertyNames(EventEmitter.prototype), 'defineCommand', 'quit', 'disconnect', 'connect', 'flushQueue']);
	function count(name) { trips++; log.push(name); }
	function wrap(redis) {
		if (redis === null || typeof redis !== 'object') return redis;
		if (wrappers.has(redis)) return wrappers.get(redis);
		const shims = new Set();
		const proxy = new Proxy(redis, {
			set(target, name, value) {
				if (typeof value === 'function') shims.add(name);
				else shims.delete(name);
				return Reflect.set(target, name, value, target);
			},
			get(target, name) {
				const value = Reflect.get(target, name, target);
				if (typeof value !== 'function') return value;
				if (shims.has(name)) return value;
				if (name === 'nodes') return (...args) => value.apply(target, args).map(wrap);
				if (name === 'duplicate') return (...args) => wrap(value.apply(target, args));
				if (name === 'pipeline' || name === 'multi') return (...args) => {
					const pipe = value.apply(target, args);
					const wrapped = new Proxy(pipe, { get(batch, method) {
						const command = batch[method];
						if (method === 'exec') return (...execArgs) => { count(String(name)); return command.apply(batch, execArgs); };
						if (typeof command !== 'function') return command;
						return (...commandArgs) => { const result = command.apply(batch, commandArgs); return result === batch ? wrapped : result; };
					} });
					return wrapped;
				};
				// A method that returns its own connection (on, once, and the other
				// chainable EventEmitter calls) hands back the meter, so a chained
				// command still counts.
				if (local.has(name) || typeof name !== 'string' || name.startsWith('_')) {
					return (...args) => { const result = value.apply(target, args); return result === target ? proxy : result; };
				}
				return (...args) => { count(name); const result = value.apply(target, args); return result === target ? proxy : result; };
			}
		});
		wrappers.set(redis, proxy);
		return proxy;
	}
	return {
		client: { ...client, redis: wrap(client.redis), duplicate: (...args) => wrap(client.duplicate(...args)) },
		reset() { trips = 0; log = []; },
		read() { return trips; },
		/** Command names (or `pipeline` / `multi`) submitted since the last reset. */
		commands() { return log.slice(); }
	};
}

export function assertConstantIO(small, large, budget) {
	if (small !== budget || large !== budget) {
		throw new Error(`Redis I/O budget ${budget}: observed ${small} and ${large}`);
	}
}
