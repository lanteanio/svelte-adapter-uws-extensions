export default async function throwWithCause() {
	const err = new Error('upstream 503');
	err.code = 'UPSTREAM';
	// Retry predicates routinely key off the cause rather than the message.
	err.cause = { retryable: true, status: 503 };
	throw err;
}
