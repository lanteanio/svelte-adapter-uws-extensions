export default async function throwUncloneableCode() {
	const err = new Error('bad code');
	// `code` is copied verbatim by the serializer just as `cause` is, so it is
	// equally able to make postMessage throw inside the harness - which turns a
	// task failure into a LOST REPLY the pool can only resolve by timing out.
	err.code = () => true;
	throw err;
}
