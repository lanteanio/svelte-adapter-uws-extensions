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
