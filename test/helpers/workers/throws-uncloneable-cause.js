export default async function throwUncloneableCause() {
	const err = new Error('bad cause');
	// Functions are not structured-cloneable, so a transport that forwards
	// the cause verbatim makes postMessage throw inside the harness.
	err.cause = { retry: () => true };
	throw err;
}
