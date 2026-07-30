export default async function throwUncloneableStack() {
	const err = new Error('the real handler failure');
	// `name`, `message` and `stack` are copied verbatim by the serializer, so
	// they are as able to hold an object as `code` and `cause` are. This one
	// carries a `toJSON`, so it SURVIVES a JSON round-trip and still fails
	// structuredClone - which makes postMessage throw inside the harness, and
	// that loses the reply rather than reporting a task failure.
	Object.defineProperty(err, 'stack', {
		value: { toJSON: () => 'looks-serialisable', fn: () => true },
		writable: true,
		configurable: true
	});
	throw err;
}
