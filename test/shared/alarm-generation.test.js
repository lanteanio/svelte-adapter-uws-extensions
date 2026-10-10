import { describe, expect, it } from 'vitest';
import { assertAlarmGeneration, MAX_ALARM_GENERATION } from '../../src/shared/alarm-generation.js';

describe('durable alarm generation validation', () => {
	it.each(['1', '9007199254740993', MAX_ALARM_GENERATION])('preserves exact accepted generation %s', value => {
		expect(assertAlarmGeneration(value)).toBe(value);
	});
	it.each([undefined, null, 1, 1n, '', '0', '-1', '01', ' 1', '1 ', '1.0', '+1', '1e3', '9223372036854775808', '999999999999999999999'])('rejects noncanonical or out-of-range generation %s', value => {
		expect(() => assertAlarmGeneration(value)).toThrow(TypeError);
	});
});
