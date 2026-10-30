import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
	MAX_TIMER_INTERVAL_MS,
	allOf,
	assertWholeExpiry,
	defineOptions,
	interval,
	option,
	suggestOptionKey,
	timerCeiling,
	wholeExpiry
} from '../../src/shared/options.js';

let warn;
beforeEach(() => {
	warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => {
	warn.mockRestore();
});

let nextFactory = 0;
/** A validator with a name of its own, so no other test shares its warn-once memory. */
function validator(schema) {
	const factory = `createThing${nextFactory++}`;
	return { factory, validate: defineOptions({ factory, label: 'thing', types: 'ThingOptions in thing.d.ts' }, schema) };
}

describe('defineOptions: absent values', () => {
	it('never judges undefined or null, whatever the rule', () => {
		const { validate } = validator({
			enabled: option.boolean,
			onError: option.function,
			breaker: option.collaborator,
			heartbeat: interval({ min: 0, exclusiveMin: true }),
			leaseMs: option.positiveInteger
		});
		expect(() => validate({ enabled: null, onError: null, breaker: null, heartbeat: null, leaseMs: null })).not.toThrow();
		expect(() => validate({ enabled: undefined, onError: undefined, breaker: undefined, heartbeat: undefined, leaseMs: undefined })).not.toThrow();
		expect(warn).not.toHaveBeenCalled();
	});

	it('ignores a missing or non-object bag', () => {
		const { validate } = validator({ enabled: option.boolean });
		for (const bag of [undefined, null, 'redis://x', 42]) expect(validate(bag)).toBe(bag);
		expect(warn).not.toHaveBeenCalled();
	});

	it('hands the factory its options without the null keys, so the factory default applies', () => {
		const { validate } = validator({ size: option.known, ttl: option.known, keyPrefix: option.known });
		const proto = { inherited: 1 };
		const bag = Object.assign(Object.create(proto), { size: null, ttl: 5, keyPrefix: null, zz: null });
		const seen = validate(bag);
		expect(seen).not.toBe(bag);
		expect(Object.keys(seen)).toEqual(['ttl']);
		expect(seen.size).toBeUndefined();
		expect(seen.inherited).toBe(1);
		// The caller's object is left as it was.
		expect(bag).toEqual(expect.objectContaining({ size: null, ttl: 5, keyPrefix: null, zz: null }));
	});

	it('returns the caller object itself when nothing is null', () => {
		const { validate } = validator({ size: option.known });
		const bag = { size: 3 };
		expect(validate(bag)).toBe(bag);
		expect(validate({})).toEqual({});
	});

	it('keeps null for a key declared nullable, where null is a setting of its own', () => {
		const { validate } = validator({ systemChannel: option.nullable, size: option.known });
		const seen = validate({ systemChannel: null, size: null });
		expect(seen).toEqual({ systemChannel: null });
		expect(Object.hasOwn(seen, 'systemChannel')).toBe(true);
	});
});

describe('defineOptions: unknown keys', () => {
	it('aggregates every unknown key into one warning with did-you-mean, in the adapter wording', () => {
		const { factory, validate } = validator({ heartbeat: option.known, keyPrefix: option.known });
		validate({ hearbeat: 1, KEYPREFIX: 'x', zz: 1 });
		expect(warn).toHaveBeenCalledTimes(1);
		expect(warn.mock.calls[0][0]).toBe(
			`[extensions] unknown ${factory} option(s): hearbeat (did you mean 'heartbeat'?), ` +
			`KEYPREFIX (did you mean 'keyPrefix'?), zz - not recognized by ${factory} and ignored. ` +
			'Check the spelling against the documented options (ThingOptions in thing.d.ts).'
		);
	});

	it('never throws for an unknown key', () => {
		const { validate } = validator({ heartbeat: option.known });
		expect(() => validate({ fromANewerVersion: true })).not.toThrow();
	});

	it('warns once per distinct unknown-key set, not once per construction', () => {
		const { validate } = validator({ ttl: option.known });
		for (let i = 0; i < 5; i++) validate({ tll: 1 });
		expect(warn).toHaveBeenCalledTimes(1);
		// Order does not make a set distinct.
		validate({ a1: 1, b1: 1 });
		validate({ b1: 1, a1: 1 });
		expect(warn).toHaveBeenCalledTimes(2);
		// A different set is a new mistake and is reported.
		validate({ tll: 1, b1: 1 });
		expect(warn).toHaveBeenCalledTimes(3);
	});

	it('keeps its warn-once memory bounded and still reports after it fills', () => {
		const { validate } = validator({ ttl: option.known });
		validate({ first: 1 });
		for (let i = 0; i < 300; i++) validate({ [`k${i}`]: 1 });
		const before = warn.mock.calls.length;
		expect(before).toBe(301);
		validate({ first: 1 });
		expect(warn.mock.calls.length).toBe(before + 1);
	});
});

describe('defineOptions: value rules', () => {
	it('names the option, the accepted values and the value received', () => {
		const { validate } = validator({ enabled: option.boolean, onError: option.function, meta: option.object });
		expect(() => validate({ enabled: 'false' })).toThrow('thing: enabled must be true or false - got "false" (string).');
		expect(() => validate({ onError: 'log' })).toThrow('thing: onError must be a function - got "log" (string).');
		expect(() => validate({ meta: [1] })).toThrow('thing: meta must be an object - got [1] (object).');
		expect(() => validate({ enabled: 10n })).toThrow('thing: enabled must be true or false - got 10n (bigint).');
	});

	it('throws a plain Error, as the adapter configuration guards do', () => {
		const { validate } = validator({ enabled: option.boolean, heartbeat: timerCeiling(), ttlMs: wholeExpiry() });
		for (const bag of [{ enabled: 'false' }, { heartbeat: MAX_TIMER_INTERVAL_MS + 1 }, { ttlMs: 1.5 }]) {
			let error;
			try { validate(bag); } catch (e) { error = e; }
			expect(error).toBeInstanceOf(Error);
			expect(error.constructor).toBe(Error);
		}
	});

	it('accepts the values it lists', () => {
		const { validate } = validator({
			enabled: option.boolean,
			onError: option.function,
			meta: option.object,
			name: option.string,
			names: option.stringArray,
			buckets: option.numberArray,
			presenceTtlMs: option.positiveNumber,
			leaseMs: option.positiveInteger
		});
		expect(() => validate({
			enabled: false, onError: () => {}, meta: {}, name: '', names: [], buckets: [0.5, 1],
			presenceTtlMs: 0.5, leaseMs: 1
		})).not.toThrow();
		expect(() => validate({ names: ['a', 5] })).toThrow('thing: names must be an array of strings');
		expect(() => validate({ buckets: [1, Infinity] })).toThrow('thing: buckets must be an array of finite numbers');
		expect(() => validate({ presenceTtlMs: 0 })).toThrow('thing: presenceTtlMs must be a number > 0 - got 0 (number).');
		expect(() => validate({ leaseMs: 1.5 })).toThrow('thing: leaseMs must be an integer >= 1 - got 1.5 (number).');
	});

	it('treats every falsy collaborator as off and refuses a truthy non-object', () => {
		const { validate } = validator({ breaker: option.collaborator });
		for (const off of [false, 0, '']) expect(() => validate({ breaker: off })).not.toThrow();
		expect(() => validate({ breaker: {} })).not.toThrow();
		expect(() => validate({ breaker: 'on' })).toThrow('thing: breaker must be an object (or omitted) - got "on" (string).');
	});

	it('judges values before reporting unknown keys, as the adapter does', () => {
		const { validate } = validator({ enabled: option.boolean });
		expect(() => validate({ enabled: 'yes', enabledd: true })).toThrow();
		expect(warn).not.toHaveBeenCalled();
	});
});

describe('timer rules', () => {
	const overflow = (surface, limit, unit = 'milliseconds') => new RegExp(
		`^${surface} must be no greater than ${limit} ${unit}, because Node stores a timer delay in a signed 32-bit integer`
	);

	it('interval(): finite, at or above its floor, and within the 32-bit timer ceiling', () => {
		const check = interval();
		expect(() => check('x: intervalMs', 0)).not.toThrow();
		expect(() => check('x: intervalMs', 12.5)).not.toThrow();
		expect(() => check('x: intervalMs', MAX_TIMER_INTERVAL_MS)).not.toThrow();
		expect(() => check('x: intervalMs', MAX_TIMER_INTERVAL_MS + 1)).toThrow(overflow('x: intervalMs', MAX_TIMER_INTERVAL_MS));
		expect(() => check('x: intervalMs', -1)).toThrow('x: intervalMs must be a number >= 0 (milliseconds) - got -1 (number).');
		expect(() => check('x: intervalMs', '1000')).toThrow('x: intervalMs must be a number >= 0 (milliseconds) - got "1000" (string).');
		expect(() => check('x: intervalMs', Infinity)).toThrow('must be a number >= 0');
		expect(() => check('x: intervalMs', NaN)).toThrow('must be a number >= 0');
		const positive = interval({ min: 0, exclusiveMin: true });
		expect(() => positive('x: heartbeat', 0)).toThrow('x: heartbeat must be a number > 0 (milliseconds) - got 0 (number).');
	});

	it('timerCeiling(): judges only finite numbers, so a factory keeps its own message for the rest', () => {
		const check = timerCeiling();
		for (const value of ['5000', -1, 0, NaN, Infinity, {}, MAX_TIMER_INTERVAL_MS]) {
			expect(() => check('x: heartbeat', value)).not.toThrow();
		}
		expect(() => check('x: heartbeat', MAX_TIMER_INTERVAL_MS + 1)).toThrow(overflow('x: heartbeat', MAX_TIMER_INTERVAL_MS));
	});

	it('timerCeiling(): scales a seconds option or a derived delay into the ceiling it reports', () => {
		const seconds = timerCeiling({ scale: 1000, unit: 'seconds' });
		expect(() => seconds('x: ttl', 2147483)).not.toThrow();
		expect(() => seconds('x: ttl', 2147484)).toThrow(overflow('x: ttl', 2147483, 'seconds'));
		const doubled = timerCeiling({ scale: 2 });
		expect(() => doubled('x: snapshotIntervalMs', Math.floor(MAX_TIMER_INTERVAL_MS / 2))).not.toThrow();
		expect(() => doubled('x: snapshotIntervalMs', Math.floor(MAX_TIMER_INTERVAL_MS / 2) + 1))
			.toThrow(overflow('x: snapshotIntervalMs', Math.floor(MAX_TIMER_INTERVAL_MS / 2)));
	});
});

describe('whole expiries', () => {
	it('refuses a fractional Redis lifetime and names the unit', () => {
		expect(() => assertWholeExpiry('x: ttlMs', 1500.5)).toThrow(
			'x: ttlMs must be a whole number of milliseconds - got 1500.5. Redis refuses a fractional expiry, so every write that sets it would fail.'
		);
		expect(() => assertWholeExpiry('x: ttl', 1.5, { unit: 'seconds' })).toThrow('x: ttl must be a whole number of seconds - got 1.5.');
		expect(() => assertWholeExpiry('x: ttl', 1.0001, { scale: 1000, unit: 'seconds' }))
			.toThrow('x: ttl must be a whole number of milliseconds (seconds with at most three decimals) - got 1.0001.');
		expect(() => assertWholeExpiry('x: ttl', 1.5, { scale: 1000, unit: 'seconds' })).not.toThrow();
		expect(() => assertWholeExpiry('x: ttlMs', 1500)).not.toThrow();
	});

	it('leaves values below its floor and non-numbers to the factory', () => {
		for (const value of [0.5, 0, -1, '1.5', NaN, Infinity]) expect(() => assertWholeExpiry('x: ttlMs', value)).not.toThrow();
		expect(() => assertWholeExpiry('x: blockDuration', 0.5, { min: 0 })).toThrow('x: blockDuration must be a whole number of milliseconds');
		expect(() => assertWholeExpiry('x: blockDuration', 0, { min: 0 })).not.toThrow();
	});

	it('wholeExpiry() and allOf() compose with a timer ceiling', () => {
		const ttl = wholeExpiry({ unit: 'seconds', also: timerCeiling({ scale: 1000, unit: 'seconds' }) });
		expect(() => ttl('x: ttl', 1.5)).toThrow('whole number of seconds');
		expect(() => ttl('x: ttl', 3_000_000)).toThrow('must be no greater than 2147483 seconds');
		expect(() => ttl('x: ttl', 30)).not.toThrow();
		const lease = allOf(option.positiveInteger, timerCeiling({ scale: 1 / 3 }));
		expect(() => lease('x: persistLeaseMs', 0)).toThrow('must be an integer >= 1');
		expect(() => lease('x: persistLeaseMs', 3 * MAX_TIMER_INTERVAL_MS)).not.toThrow();
		expect(() => lease('x: persistLeaseMs', 3 * MAX_TIMER_INTERVAL_MS + 1)).toThrow('must be no greater than 6442450941 milliseconds');
	});
});

describe('suggestOptionKey', () => {
	const known = new Set(['heartbeat', 'ttl', 'maxEnvelopeBytes', 'keyPrefix']);
	it('matches casing exactly first, then a small edit distance', () => {
		expect(suggestOptionKey('TTL', known)).toBe('ttl');
		expect(suggestOptionKey('hearbeat', known)).toBe('heartbeat');
		expect(suggestOptionKey('maxEnvelopBytes', known)).toBe('maxEnvelopeBytes');
	});
	it('names nothing for a short or distant key', () => {
		expect(suggestOptionKey('tl', known)).toBe(null);
		expect(suggestOptionKey('somethingElse', known)).toBe(null);
	});
});
