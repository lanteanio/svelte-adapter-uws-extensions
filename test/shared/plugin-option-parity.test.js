// A Redis presence, cursor, group or rate limiter is a drop-in for the
// adapter's in-memory plugin of the same name. For every option both read,
// a value the in-memory plugin accepts must not be refused here, or an app
// that works on one throws when moved to the other. The one exception is
// presence `heartbeat: 0`, which the in-memory plugin reads as "no heartbeat"
// and the Redis presence cannot honor: its heartbeat also renews the Redis
// TTL of every local entry. plugin-option-behaviour.test.js checks that the
// accepted values also behave the same.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createPresence as leadPresence } from 'svelte-adapter-uws/plugins/presence';
import { createCursor as leadCursor } from 'svelte-adapter-uws/plugins/cursor';
import { createGroup as leadGroup } from 'svelte-adapter-uws/plugins/groups';
import { createRateLimit as leadRateLimit } from 'svelte-adapter-uws/plugins/ratelimit';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { ODD_VALUES, describeValue } from '../helpers/plugin-option-values.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createGroup } from '../../src/redis/groups.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';

const plugins = [
	['presence', (o) => leadPresence(o), (redis, o) => createPresence(redis, o),
		['key', 'select', 'heartbeat', 'topicThrottle', 'transient', 'binary', 'clientUpdateFields', 'maxFieldsBytes', 'maxTotalFieldsBytes', 'maxTopicsPerConnection']],
	['cursor', (o) => leadCursor(o), (redis, o) => createCursor(redis, o),
		['select', 'throttle', 'topicThrottle', 'binary', 'dictionary', 'maxDataBytes', 'maxTopicLength', 'minMove', 'position', 'viewport', 'backpressure']],
	['groups', (o) => leadGroup('room', o), (redis, o) => createGroup(redis, 'room', o),
		['meta', 'maxMembers', 'onJoin', 'onLeave', 'onFull', 'onClose']],
	['ratelimit', (o) => leadRateLimit({ points: 2, interval: 1000, ...o }), (redis, o) => createRateLimit(redis, { points: 2, interval: 1000, ...o }),
		['points', 'interval', 'blockDuration', 'keyBy', 'tenant', 'budget']]
];

/** The shared values the Redis side refuses on purpose. */
const exceptions = new Set(['presence.heartbeat=0', 'presence.heartbeat=-0']);

let warn;
let created;
beforeEach(() => {
	warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
	created = [];
});
afterEach(async () => {
	for (const value of created) {
		try { await value?.destroy?.(); } catch { /* best-effort */ }
	}
	warn.mockRestore();
});

function outcome(build) {
	try {
		created.push(build());
		return 'ok';
	} catch {
		return 'throws';
	}
}

const rows = [];
const keys = [];
for (const [name, lead, ours, options] of plugins) {
	for (const key of options) {
		keys.push([`${name}.${key}`, lead, key]);
		for (const value of ODD_VALUES) rows.push([`${name}.${key}=${describeValue(value)}`, lead, ours, key, value]);
	}
}

describe('options shared with the in-memory plugins', () => {
	it.each(rows)('%s: accepted here whenever the in-memory plugin accepts it', (id, lead, ours, key, value) => {
		const leadResult = outcome(() => lead({ [key]: value }));
		const ourResult = outcome(() => ours(mockRedisClient(), { [key]: value }));
		if (exceptions.has(id)) {
			expect([leadResult, ourResult]).toEqual(['ok', 'throws']);
			return;
		}
		if (leadResult === 'ok') expect(ourResult).toBe('ok');
	});

	// The check above only asserts when the in-memory plugin accepts a value,
	// so a plugin that started refusing everything would pass it silently.
	it.each(keys)('%s: the in-memory plugin accepts some of the values tried', (_id, lead, key) => {
		const accepted = ODD_VALUES.filter((value) => outcome(() => lead({ [key]: value })) === 'ok');
		expect(accepted.length).toBeGreaterThanOrEqual(3);
	});

	it('presence heartbeat: 0 names why the Redis presence refuses it', () => {
		expect(() => createPresence(mockRedisClient(), { heartbeat: 0 }))
			.toThrow(/each heartbeat also renews the Redis TTL of every local entry/);
	});
});
