/**
 * Construction-time option validation shared by every extension factory.
 *
 * The policy is the adapter's (`src/config-guards.js` in svelte-adapter-uws):
 *
 * - A key the factory does not read WARNS, once per distinct set of unknown
 *   keys, with the closest documented key when one is close enough to name.
 *   It never throws: an app pinning an older extensions version must still
 *   start with an options object that carries a newer version's key.
 * - A known key whose VALUE the factory cannot honor THROWS, naming the values
 *   it accepts. A misshaped value is not read as "use the default": most
 *   factories compare numbers with `>` or read flags with `=== true`, so a
 *   string or a negative number silently disables the bound it configures.
 * - `undefined` and `null` are both absent. Neither is judged here, and the
 *   options handed back to the factory leave a `null` out, so the factory
 *   applies its default exactly as for an omitted key.
 * - Errors are plain `Error`s, as the adapter's configuration guards and the
 *   factories' own checks throw.
 *
 * Keys the factory already judges itself are declared with `option.known`
 * so their established messages stay the ones users see. Refinements such as
 * `timerCeiling` add a bound those checks miss without taking the message
 * away from the values the factory already refuses.
 *
 * Internal - not a package export.
 *
 * @module svelte-adapter-uws-extensions/shared/options
 */

/**
 * Milliseconds ceiling of a Node timer delay. `setTimeout` / `setInterval`
 * store the delay in a signed 32-bit integer, and a larger delay overflows to
 * fire every millisecond - so an interval meant to slow a heartbeat or a
 * sampler down would instead run it in the tightest loop the event loop
 * allows.
 */
export const MAX_TIMER_INTERVAL_MS = 0x7fffffff;

/**
 * Distinct unknown-key sets already reported. A per-room or per-tenant
 * factory is constructed many times with the same options, and one typo must
 * not print one warning per construction. Bounded so that an app building
 * option keys dynamically cannot grow it without limit; when full it starts
 * over, which at worst repeats a warning.
 */
const warnedUnknownKeySets = new Set();
const MAX_WARNED_UNKNOWN_KEY_SETS = 256;

/**
 * Render a rejected option value for a message without throwing on the exotic
 * ones. `JSON.stringify` handles most, returns `undefined` for a function or a
 * symbol, and throws on a BigInt or a circular object.
 *
 * @param {unknown} value
 * @returns {string}
 */
export function describeValue(value) {
	if (typeof value === 'bigint') return `${value}n`;
	try {
		const json = JSON.stringify(value);
		return json === undefined ? String(value) : json;
	} catch {
		return String(value);
	}
}

/**
 * @param {string} surface
 * @param {string} expected
 * @param {unknown} value
 * @returns {never}
 */
function refuse(surface, expected, value) {
	throw new Error(`${surface} must be ${expected} - got ${describeValue(value)} (${typeof value}).`);
}

/**
 * @param {string} surface
 * @param {number} ceiling
 * @param {string} unit
 * @param {unknown} value
 * @returns {never}
 */
function throwTimerOverflow(surface, ceiling, unit, value) {
	throw new Error(
		`${surface} must be no greater than ${ceiling} ${unit}, because Node ` +
		`stores a timer delay in a signed 32-bit integer and a larger delay overflows to fire ` +
		`every millisecond - the tight loop an interval this large was meant to avoid - got ` +
		`${describeValue(value)}.`
	);
}

/**
 * A value rule: `test` decides, `expected` names what passes. Every rule is a
 * `(surface, value) => void` that throws on a value it refuses.
 *
 * @param {string} expected
 * @param {(value: any) => boolean} test
 * @returns {(surface: string, value: unknown) => void}
 */
function rule(expected, test) {
	return (surface, value) => {
		if (!test(value)) refuse(surface, expected, value);
	};
}

const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const isFiniteNumber = (value) => typeof value === 'number' && Number.isFinite(value);

/**
 * Refuse a present value that is not a boolean, with the adapter's message.
 * For a flag read as `=== true` or `=== false`, the string `'false'` from an
 * environment variable silently picks the wrong side.
 *
 * @param {string} surface - how the message names the option, e.g. `redis cursor: viewport.enabled`
 * @param {unknown} value
 */
export function assertBooleanOption(surface, value) {
	if (value === undefined || value === null || typeof value === 'boolean') return;
	throw new Error(
		`${surface} must be true or false - got ${describeValue(value)} (${typeof value}). ` +
		'Only false turns it off, so any other value would silently leave it on. If the value ' +
		"comes from the environment, convert it explicitly (e.g. process.env.X !== 'false')."
	);
}

/**
 * Refuse a present value that is not an array of strings, with the adapter's
 * messages. A field list is compared against field names, so a non-string
 * entry can never match and would be dropped without a word.
 *
 * @param {string} surface
 * @param {unknown} value
 */
export function assertStringArrayOption(surface, value) {
	if (value === undefined || value === null) return;
	if (Array.isArray(value)) {
		for (const entry of value) {
			if (typeof entry === 'string') continue;
			throw new Error(
				`${surface} entries must be field-name strings - got ${describeValue(entry)} (${typeof entry}). ` +
				'An entry is matched against field names, so any other type would never match.'
			);
		}
		return;
	}
	throw new Error(`${surface} must be an array of field names - got ${describeValue(value)} (${typeof value}).`);
}

/** An object whose prototype is `Object.prototype` or `null`, so a spread copies it as written. */
const isPlainObject = (value) => {
	if (value === null || typeof value !== 'object') return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
};

export const option = Object.freeze({
	/** Accepted key whose value the factory judges itself. */
	known: () => {},
	/**
	 * Accepted key the factory judges itself, where `null` is a documented
	 * setting rather than "not set" (the pub/sub `systemChannel: null` turns
	 * the health events off), so the validator hands it through.
	 */
	nullable: () => {},
	boolean: rule('true or false', (value) => typeof value === 'boolean'),
	function: rule('a function', (value) => typeof value === 'function'),
	object: rule('an object', isObject),
	/**
	 * Copied with a spread: a string would turn into indexed characters, and a
	 * Map or a Date would flatten to its own enumerable fields.
	 */
	plainObject: rule('a plain object', isPlainObject),
	string: rule('a string', (value) => typeof value === 'string'),
	stringArray: rule('an array of strings', (value) => Array.isArray(value) && value.every((entry) => typeof entry === 'string')),
	/** A list matched against field names, refused as the adapter's plugins refuse it. */
	fieldNames: assertStringArrayOption,
	numberArray: rule('an array of finite numbers', (value) => Array.isArray(value) && value.every(isFiniteNumber)),
	positiveNumber: rule('a number > 0', (value) => isFiniteNumber(value) && value > 0),
	positiveInteger: rule('an integer >= 1', (value) => Number.isSafeInteger(value) && value >= 1),
	/**
	 * A breaker, a metrics registry or a similar collaborator. Only a truthy
	 * non-object is refused: most factories read a falsy one as "none"
	 * (`if (!b)`), so `breaker: false` keeps working where it worked.
	 */
	collaborator: rule('an object (or omitted)', (value) => !value || isObject(value))
});

/**
 * A millisecond delay that feeds a Node timer: a finite number at or above
 * `min`, and no greater than the timer ceiling once multiplied by `scale`
 * (a seconds option feeds its timer `value * 1000`). Fractional milliseconds
 * stay legal; Node timers accept them.
 *
 * @param {{ min?: number, exclusiveMin?: boolean, scale?: number, ceiling?: number, unit?: string }} [bounds]
 */
export function interval({ min = 0, exclusiveMin = false, scale = 1, ceiling = MAX_TIMER_INTERVAL_MS, unit = 'milliseconds' } = {}) {
	const floor = exclusiveMin ? `> ${min}` : `>= ${min}`;
	const base = rule(`a number ${floor} (${unit})`, (value) => isFiniteNumber(value) && (exclusiveMin ? value > min : value >= min));
	const limit = Math.floor(ceiling / scale);
	return (surface, value) => {
		base(surface, value);
		if (/** @type {number} */ (value) * scale > ceiling) throwTimerOverflow(surface, limit, unit, value);
	};
}

/**
 * The timer ceiling alone, for an interval the factory already validates. It
 * judges only finite numbers, so every value the factory's own check refuses
 * keeps that check's message.
 *
 * @param {{ scale?: number, ceiling?: number, unit?: string }} [bounds]
 */
export function timerCeiling({ scale = 1, ceiling = MAX_TIMER_INTERVAL_MS, unit = 'milliseconds' } = {}) {
	const limit = Math.floor(ceiling / scale);
	return (surface, value) => {
		if (isFiniteNumber(value) && /** @type {number} */ (value) * scale > ceiling) throwTimerOverflow(surface, limit, unit, value);
	};
}

/**
 * Refuse a fractional lifetime that Redis receives as a PX / PEXPIRE / EXPIRE
 * argument. Redis refuses a fractional one, so a value such as `1500.5`
 * passes a `>= 1` check and then fails every write that sets the expiry.
 * Only a finite value at or above `min` (1 by default) is judged, leaving
 * every other value to the factory's own check and message.
 *
 * @param {string} surface
 * @param {unknown} value
 * @param {{ scale?: number, unit?: string, min?: number }} [shape] - `scale` converts the
 *   option's unit to the milliseconds Redis receives (1000 for seconds), and
 *   `min` is 0 for a value whose own check already admits zero
 * @returns {void}
 */
export function assertWholeExpiry(surface, value, { scale = 1, unit = 'milliseconds', min = 1 } = {}) {
	if (!isFiniteNumber(value) || /** @type {number} */ (value) < min) return;
	if (Number.isInteger(/** @type {number} */ (value) * scale)) return;
	const whole = scale === 1 ? unit : `milliseconds (${unit} with at most three decimals)`;
	throw new Error(
		`${surface} must be a whole number of ${whole} - got ${describeValue(value)}. ` +
		'Redis refuses a fractional expiry, so every write that sets it would fail.'
	);
}

/**
 * {@link assertWholeExpiry} as a schema rule, optionally followed by another
 * rule (a timer ceiling, for a lifetime that also paces a timer).
 *
 * @param {{ scale?: number, unit?: string, also?: (surface: string, value: unknown) => void }} [shape]
 */
export function wholeExpiry({ scale = 1, unit = 'milliseconds', also } = {}) {
	return (surface, value) => {
		assertWholeExpiry(surface, value, { scale, unit });
		also?.(surface, value);
	};
}

/**
 * Several rules in order; the first that refuses throws.
 *
 * @param {...((surface: string, value: unknown) => void)} checks
 */
export function allOf(...checks) {
	return (surface, value) => {
		for (const check of checks) check(surface, value);
	};
}

/**
 * The closest documented key to an unrecognized one, or `null` when nothing is
 * close enough to name. A case-insensitive exact match wins outright; after
 * that, an edit distance of at most 2 between names of 4+ characters. Anything
 * further apart stays suggestion-free, because a wrong guess in a warning is
 * worse than none.
 *
 * @param {string} key
 * @param {Iterable<string>} known
 * @returns {string | null}
 */
export function suggestOptionKey(key, known) {
	const lower = key.toLowerCase();
	for (const candidate of known) {
		if (candidate.toLowerCase() === lower) return candidate;
	}
	if (key.length < 4) return null;
	let best = null;
	let bestDistance = 3;
	for (const candidate of known) {
		if (candidate.length < 4) continue;
		const distance = boundedEditDistance(lower, candidate.toLowerCase(), 2);
		if (distance !== null && distance < bestDistance) {
			best = candidate;
			bestDistance = distance;
		}
	}
	return best;
}

/**
 * Levenshtein distance capped at `bound`; `null` once the strings are provably
 * further apart, so the scan over a key set stays cheap.
 *
 * @param {string} a
 * @param {string} b
 * @param {number} bound
 * @returns {number | null}
 */
function boundedEditDistance(a, b, bound) {
	if (Math.abs(a.length - b.length) > bound) return null;
	let previous = Array.from({ length: b.length + 1 }, (_, i) => i);
	for (let i = 1; i <= a.length; i++) {
		const row = [i];
		let rowMin = i;
		for (let j = 1; j <= b.length; j++) {
			const cost = a[i - 1] === b[j - 1] ? 0 : 1;
			const value = Math.min(previous[j] + 1, row[j - 1] + 1, previous[j - 1] + cost);
			row.push(value);
			if (value < rowMin) rowMin = value;
		}
		if (rowMin > bound) return null;
		previous = row;
	}
	return previous[b.length] <= bound ? previous[b.length] : null;
}

/**
 * Build the validator for one factory's options. The schema maps every
 * accepted top-level key to its value rule; it is read once here, so a factory
 * constructed per room pays one key walk per construction and nothing more.
 *
 * The validator returns the options the factory reads from. A top-level
 * `null` means "not set", as in the adapter, so it is left out of what the
 * factory sees and every default and established check behaves as if the key
 * were omitted - including checks written as `options.x !== undefined` and
 * destructuring defaults, which `null` would otherwise pass straight through.
 * Without a `null` the caller's object comes back unchanged; with one, a
 * shallow copy (same prototype) without those keys. A key declared with
 * `option.nullable` keeps its `null`, which is a setting of its own there.
 *
 * A section named in `nested` (an options object such as the cursor's
 * `viewport`) has its own keys checked the same way, and an unknown one is
 * reported with its section, as `viewport.padd`.
 *
 * @template T
 * @param {{ factory: string, label: string, types: string }} names
 *   `factory` is the function users call (named in the unknown-key warning),
 *   `label` prefixes value errors the way the factory's own errors do, and
 *   `types` points at the documented options interface.
 * @param {Record<string, (surface: string, value: unknown) => void>} schema
 * @param {{ nested?: Record<string, Set<string>> }} [sections]
 * @returns {(bag: T) => T}
 */
export function defineOptions({ factory, label, types }, schema, { nested } = {}) {
	const known = new Set(Object.keys(schema));
	const keepsNull = new Set(Object.keys(schema).filter((key) => schema[key] === option.nullable));
	const entries = Object.entries(schema);
	return function validateOptions(bag) {
		if (bag === null || typeof bag !== 'object') return bag;
		const options = /** @type {Record<string, unknown>} */ (bag);
		for (const [key, check] of entries) {
			const value = options[key];
			if (value === undefined || value === null) continue;
			check(`${label}: ${key}`, value);
		}
		let unknown = null;
		let absent = null;
		for (const key of Object.keys(options)) {
			if (options[key] === null && !keepsNull.has(key)) (absent ??= []).push(key);
			if (!known.has(key)) {
				(unknown ??= []).push(describeUnknown(key, known, ''));
				continue;
			}
			const inner = nested?.[key];
			const section = options[key];
			if (inner && isObject(section)) {
				for (const sub of Object.keys(/** @type {object} */ (section))) {
					if (!inner.has(sub)) (unknown ??= []).push(describeUnknown(sub, inner, key + '.'));
				}
			}
		}
		if (unknown !== null) warnUnknown(factory, types, unknown);
		if (absent === null) return bag;
		const copy = Object.assign(Object.create(Object.getPrototypeOf(options)), options);
		for (const key of absent) delete copy[key];
		return copy;
	};
}

/**
 * @param {string} key
 * @param {Set<string>} known
 * @param {string} prefix - the section an inner key belongs to, as `viewport.`
 */
function describeUnknown(key, known, prefix) {
	const suggestion = suggestOptionKey(key, known);
	return suggestion === null ? prefix + key : `${prefix}${key} (did you mean '${prefix}${suggestion}'?)`;
}

/**
 * @param {string} factory
 * @param {string} types
 * @param {string[]} described
 */
function warnUnknown(factory, types, described) {
	const id = `${types}\0${factory}\0${described.slice().sort().join('\0')}`;
	if (warnedUnknownKeySets.has(id)) return;
	if (warnedUnknownKeySets.size >= MAX_WARNED_UNKNOWN_KEY_SETS) warnedUnknownKeySets.clear();
	warnedUnknownKeySets.add(id);
	console.warn(
		`[extensions] unknown ${factory} option(s): ${described.join(', ')} - ` +
		`not recognized by ${factory} and ignored. Check the spelling against the ` +
		`documented options (${types}).`
	);
}
