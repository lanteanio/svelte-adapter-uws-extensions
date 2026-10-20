/** Generation counters use the same exact signed-64-bit range in both stores. */
export const MAX_ALARM_GENERATION = '9223372036854775807';

/**
 * Upper bound on unresolved retained executions per store namespace. A claim at
 * this bound is refused and its schedule kept; retained work is never evicted.
 * Matches the realtime in-memory pending-alarm bound.
 */
export const MAX_ALARM_EXECUTIONS = 100000;

/** Default and maximum page sizes of `executions({ limit })`. */
export const ALARM_EXECUTIONS_PAGE = 100;
export const MAX_ALARM_EXECUTIONS_PAGE = 1000;

export function assertAlarmGeneration(value, moduleLabel = 'alarm-store') {
	if (typeof value !== 'string' || value.length > MAX_ALARM_GENERATION.length || !/^[1-9][0-9]*$/.test(value) || (value.length === MAX_ALARM_GENERATION.length && value > MAX_ALARM_GENERATION)) {
		throw new TypeError(`${moduleLabel}: generation must be a positive decimal string no greater than ${MAX_ALARM_GENERATION}`);
	}
	return value;
}

export function assertAlarmAttempt(value, moduleLabel = 'alarm-store') {
	if (!Number.isSafeInteger(value) || value < 1) {
		throw new TypeError(`${moduleLabel}: attempt must be a positive safe integer`);
	}
	return value;
}

/** Validate `fail()`'s outcome; raw handler errors are never accepted or persisted. */
export function assertAlarmFailure(failure, moduleLabel = 'alarm-store') {
	if (!failure || typeof failure !== 'object' || failure.errorClass !== 'handler' || typeof failure.failedAt !== 'number' || !Number.isFinite(failure.failedAt)) {
		throw new TypeError(`${moduleLabel}: failure must be { errorClass: 'handler', failedAt: finite epoch-ms }`);
	}
	return Math.trunc(failure.failedAt);
}

/** Validate `requeue()` options and return the next claim's attempt. */
export function assertAlarmRequeue(attempt, options, moduleLabel = 'alarm-store') {
	if (!options || typeof options !== 'object' || typeof options.at !== 'number' || !Number.isFinite(options.at)) {
		throw new TypeError(`${moduleLabel}: requeue requires { at: finite epoch-ms }`);
	}
	if (options.allowAbandoned !== undefined && typeof options.allowAbandoned !== 'boolean') {
		throw new TypeError(`${moduleLabel}: allowAbandoned must be a boolean`);
	}
	if (attempt >= Number.MAX_SAFE_INTEGER) throw new RangeError(`${moduleLabel}: execution attempt exhausted`);
	return { at: options.at, allowAbandoned: options.allowAbandoned === true, next: attempt + 1 };
}

/** Normalize `executions()` options: the page is capped at the maximum, the cursor is exclusive. */
export function alarmExecutionsPage(options, moduleLabel = 'alarm-store') {
	if (options === undefined || options === null) options = {};
	if (typeof options !== 'object') throw new TypeError(`${moduleLabel}: executions options must be an object`);
	const limit = options.limit ?? ALARM_EXECUTIONS_PAGE;
	if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError(`${moduleLabel}: limit must be a positive integer`);
	const after = options.after ?? null;
	if (after !== null) {
		if (typeof after !== 'object' || typeof after.topic !== 'string' || after.topic.length === 0) {
			throw new TypeError(`${moduleLabel}: after must be { topic, generation } or null`);
		}
		assertAlarmGeneration(after.generation, moduleLabel);
	}
	return { limit: Math.min(limit, MAX_ALARM_EXECUTIONS_PAGE), after };
}

/** A claim refused because retained execution storage is full; the schedule is unchanged. */
export function alarmCapacityError(moduleLabel = 'alarm-store') {
	const error = new Error(`${moduleLabel}: retained alarm executions reached capacity; the schedule was kept`);
	error.code = 'ALARM_CAPACITY';
	return error;
}
