// Count logical transport submissions at the existing injected Redis boundary.
// Pipeline members share one submission; Cluster node publishers share this ledger.
export function countRedisIO(client) {
	let trips = 0;
	const wrappers = new WeakMap();
	const local = new Set(['on', 'once', 'off', 'removeListener', 'removeAllListeners', 'defineCommand', 'duplicate', 'quit', 'disconnect', 'connect']);
	function wrap(redis) {
		if (wrappers.has(redis)) return wrappers.get(redis);
		const proxy = new Proxy(redis, {
			get(target, name) {
				const value = Reflect.get(target, name, target);
				if (typeof value !== 'function') return value;
				if (name === 'nodes') return (...args) => value.apply(target, args).map(wrap);
				if (name === 'pipeline' || name === 'multi') return (...args) => {
					const pipe = value.apply(target, args);
					const wrapped = new Proxy(pipe, { get(batch, method) {
						const command = batch[method];
						if (method === 'exec') return (...execArgs) => { trips++; return command.apply(batch, execArgs); };
						if (typeof command !== 'function') return command;
						return (...commandArgs) => { const result = command.apply(batch, commandArgs); return result === batch ? wrapped : result; };
					} });
					return wrapped;
				};
				if (local.has(name) || typeof name !== 'string' || name.startsWith('_')) return value.bind(target);
				return (...args) => { trips++; return value.apply(target, args); };
			}
		});
		wrappers.set(redis, proxy);
		return proxy;
	}
	return { client: { ...client, redis: wrap(client.redis) }, reset() { trips = 0; }, read() { return trips; } };
}

export function assertConstantIO(small, large, budget) {
	if (small !== budget || large !== budget) {
		throw new Error(`Redis I/O budget ${budget}: observed ${small} and ${large}`);
	}
}
