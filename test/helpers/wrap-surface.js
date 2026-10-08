import { PLATFORM_KEYS } from '../../src/testing/mock-platform.js';

/**
 * Members a bus wrap has whatever its source platform has, and why. Every
 * other forwarded member reads undefined on a wrap whose source lacks it.
 */
export const PROVIDED_ANYWAY = Object.freeze({
	publish: 'the member the bus relays',
	topic: 'built on the wrap\'s own publish, so a topic helper relays too',
	now: 'falls back to the runtime clock',
	monotonic: 'falls back to the runtime clock',
	random: 'falls back to the runtime RNG',
	hlc: 'falls back to a process-local HLC',
	protection: 'reads \'normal\'',
	closedWsAborts: 'reads 0',
	traceContext: 'reads null'
});

/** What the wraps forward: the PLATFORM_KEYS floor and the version-gated members outside it. */
export const FORWARDED = Object.freeze([...PLATFORM_KEYS, 'requestTopic', 'publishWire', 'sendWire', 'registerWireCodec']);

/**
 * The forwarded members whose kind differs between a platform and its wrap,
 * given each as `{ member: kind }` (a kind is what `typeof` reads, or 'null';
 * a member missing from the map reads 'undefined'). A member in
 * PROVIDED_ANYWAY that the platform lacks is the wrap's own and not compared.
 *
 * @param {Record<string, string>} platformKinds
 * @param {Record<string, string>} wrapKinds
 * @returns {string[]} one line per mismatch
 */
export function wrapMismatches(platformKinds, wrapKinds) {
	const wrong = [];
	for (const key of FORWARDED) {
		const source = platformKinds[key] ?? 'undefined';
		const forwarded = wrapKinds[key] ?? 'undefined';
		if (source === 'undefined' && Object.hasOwn(PROVIDED_ANYWAY, key)) continue;
		if (forwarded !== source) wrong.push(key + ': platform ' + source + ', wrap ' + forwarded);
	}
	return wrong;
}
