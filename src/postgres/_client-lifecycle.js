// Internal ownership bridge between the client factory and long-lived consumers.
// Caller-owned pools deliberately do not register a shutdown lifecycle here.
const lifecycles = new WeakMap();

export function registerPgLifecycle(client, lifecycle) {
	lifecycles.set(client, lifecycle);
}

export function onPgShutdown(client, stop) {
	const lifecycle = lifecycles.get(client);
	if (!lifecycle) return () => {};
	if (lifecycle.closing) throw new Error('postgres client is closing');
	lifecycle.stoppers.add(stop);
	return () => lifecycle.stoppers.delete(stop);
}
