// The adapter owns rendering and snapshot sequencing. Readers use only cached
// scalar state, never backend requests, identities, payloads or error text.
const CONTRIBUTORS = Symbol.for('svelte-adapter-uws.dashboard-contributors');
const READERS = Symbol.for('svelte-adapter-uws-extensions.dashboard-readers');

/**
 * Register the latest instance of a component in development. A replacement
 * survives teardown of its predecessor, including bundler-duplicated modules.
 * @param {string} component
 * @param {() => object} read
 * @returns {() => void}
 */
export function registerClusterHealth(component, read) {
	if (typeof process === 'undefined' || process.env.NODE_ENV !== 'development') return () => {};
	let registry = globalThis[CONTRIBUTORS];
	if (registry === undefined) {
		registry = new Map();
		Object.defineProperty(globalThis, CONTRIBUTORS, { value: registry, writable: true, enumerable: true, configurable: true });
	}
	let contribute = registry.get('cluster-health');
	let readers = contribute?.[READERS];
	if (!(readers instanceof Map)) {
		readers = new Map();
		contribute = () => {
			const section = {};
			for (const [name, reader] of readers) {
				try {
					const data = reader();
					// A foreign client getter can throw or return an unserializable
					// value. Keep the other components visible and omit error text.
					section[name] = JSON.parse(JSON.stringify(data));
				} catch { section[name] = { available: false }; }
			}
			return section;
		};
		Object.defineProperty(contribute, READERS, { value: readers });
		registry.set('cluster-health', contribute);
	}
	readers.set(component, read);
	return () => {
		if (readers.get(component) !== read) return;
		readers.delete(component);
		if (readers.size === 0 && registry.get('cluster-health') === contribute) registry.delete('cluster-health');
	};
}

/** Known ioredis states only; custom driver strings can contain addresses. */
export function redisConnectionState(redis) {
	const status = redis?.status;
	return ['wait', 'connecting', 'connect', 'ready', 'close', 'reconnecting', 'end'].includes(status) ? status : 'unknown';
}

export function finiteReading(value) {
	return typeof value === 'number' && Number.isFinite(value) ? value : null;
}
