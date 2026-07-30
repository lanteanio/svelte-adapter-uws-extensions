export default async function throwHostileStack() {
	const err = new Error('the real handler failure');
	// A source-map or APM hook installing its own `stack` accessor is the
	// ordinary way this arrives. Reading it unguarded while building the
	// transport shape replaces the handler's error with the getter's, on the
	// persisted row AND on the caller's rejection.
	Object.defineProperty(err, 'stack', {
		get() { throw new Error('source-map hook exploded'); }
	});
	throw err;
}
