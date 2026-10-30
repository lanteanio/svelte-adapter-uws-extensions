// A Redis presence, cursor, group or rate limiter is a drop-in for the
// adapter's in-memory plugin of the same name. For every option both read,
// a value the in-memory plugin accepts must not be refused here, or an app
// that works on one throws when moved to the other. The one exception is
// presence `heartbeat: 0`, which the in-memory plugin reads as "no heartbeat"
// and the Redis presence cannot honor: its heartbeat also renews the Redis
// TTL of every local entry.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createPresence as leadPresence } from 'svelte-adapter-uws/plugins/presence';
import { createCursor as leadCursor } from 'svelte-adapter-uws/plugins/cursor';
import { createGroup as leadGroup } from 'svelte-adapter-uws/plugins/groups';
import { createRateLimit as leadRateLimit } from 'svelte-adapter-uws/plugins/ratelimit';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createGroup } from '../../src/redis/groups.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';

const big = 3_000_000_000;
const fn = () => ({});
const odd = [null, undefined, false, true, 0, -1, 1.5, '', 'x', '16', 5, big, [], ['a', 5], {}, fn];

const plugins = [
	['presence', (o) => leadPresence(o), (redis, o) => createPresence(redis, o), {
		key: odd, select: odd, heartbeat: odd, topicThrottle: odd, transient: odd, binary: odd,
		clientUpdateFields: [null, undefined, [], ['a'], ['a', 5], 'a', {}],
		maxFieldsBytes: odd, maxTotalFieldsBytes: odd, maxTopicsPerConnection: odd
	}],
	['cursor', (o) => leadCursor(o), (redis, o) => createCursor(redis, o), {
		select: odd, throttle: odd, topicThrottle: odd, binary: odd, dictionary: odd,
		maxDataBytes: odd, maxTopicLength: odd, minMove: odd, position: odd,
		viewport: [...odd, { enabled: true, padding: 1 }], backpressure: [...odd, { enabled: true }]
	}],
	['groups', (o) => leadGroup('room', o), (redis, o) => createGroup(redis, 'room', o), {
		meta: odd, maxMembers: [...odd, Infinity], onJoin: odd, onLeave: odd, onFull: odd, onClose: odd
	}],
	['ratelimit', (o) => leadRateLimit({ points: 2, interval: 1000, ...o }), (redis, o) => createRateLimit(redis, { points: 2, interval: 1000, ...o }), {
		points: odd, interval: odd, blockDuration: odd, keyBy: [...odd, 'ip', 'connection'], tenant: odd, budget: [...odd, 'principal', 'tenant']
	}]
];

/** The one shared value the Redis side refuses on purpose. */
const exceptions = new Set(['presence.heartbeat=0']);

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
	} catch (error) {
		return 'throws';
	}
}

const label = (value) => typeof value === 'function' ? 'fn' : value === undefined ? 'undefined' : JSON.stringify(value);
const rows = [];
for (const [name, lead, ours, options] of plugins) {
	for (const [key, values] of Object.entries(options)) {
		for (const value of values) rows.push([`${name}.${key}=${label(value)}`, lead, ours, key, value]);
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

	it('presence heartbeat: 0 names why the Redis presence refuses it', () => {
		expect(() => createPresence(mockRedisClient(), { heartbeat: 0 }))
			.toThrow(/each heartbeat also renews the Redis TTL of every local entry/);
	});
});
