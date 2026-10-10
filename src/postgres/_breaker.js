import { guardBreaker } from '../shared/breaker-admission.js';
export { guardBreaker };

// Local admission never reached PostgreSQL and is not a dependency verdict.
export function pgFailure(breaker, error, cancelProbe) {
	if (error?.code === 'PG_CAPACITY_EXCEEDED') { cancelProbe?.(); return; }
	breaker?.failure(error);
}

export async function withBreaker(breaker, run) {
	if (!breaker) return run();
	const cancelProbe = guardBreaker(breaker);
	try {
		const result = await run();
		breaker.success();
		return result;
	} catch (error) {
		pgFailure(breaker, error, cancelProbe);
		throw error;
	}
}
