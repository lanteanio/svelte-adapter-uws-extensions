/** Generation counters use the same exact signed-64-bit range in both stores. */
export const MAX_ALARM_GENERATION = '9223372036854775807';

export function assertAlarmGeneration(value, moduleLabel = 'alarm-store') {
	if (typeof value !== 'string' || value.length > MAX_ALARM_GENERATION.length || !/^[1-9][0-9]*$/.test(value) || (value.length === MAX_ALARM_GENERATION.length && value > MAX_ALARM_GENERATION)) {
		throw new TypeError(`${moduleLabel}: generation must be a positive decimal string no greater than ${MAX_ALARM_GENERATION}`);
	}
	return value;
}
