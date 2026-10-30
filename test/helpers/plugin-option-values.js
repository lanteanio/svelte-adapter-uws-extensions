// Values tried against every option the Redis presence, cursor, groups and
// rate limiter share with the adapter's in-memory plugins. They cover the
// shapes an app can plausibly pass: absent, falsy, negative, fractional
// between 0 and 1, timer-overflowing, non-finite, strings, arrays, objects
// and functions.
export const ODD_VALUES = [
	null, undefined, false, true,
	0, -0, -1, 0.5, 1e-9, 0.999, 1, 1.5, 5, 16,
	NaN, Infinity, 2147483647, 2147483648, 3_000_000_000,
	'', 'x', '16', 'id', 'ip', 'tenant', 'principal', 'connection',
	[], ['a'], ['a', 5], ['typing'],
	{}, { enabled: true }, { enabled: true, padding: 1 }, { a: 1 },
	() => ({}), (u) => ({ id: u?.id, name: u?.name }), (ws) => ws?.getUserData?.()?.tenant ?? null
];

/** @param {unknown} value */
export function describeValue(value) {
	if (typeof value === 'function') return `fn(${value.length})/${ODD_VALUES.indexOf(value)}`;
	if (value === undefined) return 'undefined';
	if (Object.is(value, -0)) return '-0';
	if (typeof value === 'number' && !Number.isFinite(value)) return String(value);
	return JSON.stringify(value);
}
