// Probe cancellation is internal: callers that abort before a backend verdict
// return their admission without marking the backend healthy or broken.
export const probeCancellation = new WeakMap();
const noCancellation = () => {};

export function guardBreaker(b, key) {
	if (!b) return noCancellation;
	b.guard(key);
	return probeCancellation.get(b)?.(key) ?? noCancellation;
}

