// Every option the Redis presence, cursor, groups and rate limiter share with
// the adapter's in-memory plugin of the same name must not only be accepted
// for the same values (plugin-option-parity.test.js) but also resolve to the
// same behaviour. For each shared option and every value both sides accept,
// one scenario runs against both implementations and records what an app can
// observe: the roster, which frames went out and when, the codec used, the
// member cap, the rate-limit verdicts. The two records must be equal.
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createPresence as leadPresence } from 'svelte-adapter-uws/plugins/presence';
import { createCursor as leadCursor } from 'svelte-adapter-uws/plugins/cursor';
import { createGroup as leadGroup } from 'svelte-adapter-uws/plugins/groups';
import { createRateLimit as leadRateLimit } from 'svelte-adapter-uws/plugins/ratelimit';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { createCursor } from '../../src/redis/cursor.js';
import { createGroup } from '../../src/redis/groups.js';
import { createRateLimit } from '../../src/redis/ratelimit.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { ODD_VALUES, describeValue } from '../helpers/plugin-option-values.js';
import { createRequire } from 'node:module';
import { realpathSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';

// Both implementations read time through their runtime clock seam: the
// adapter caches wall time on a one-second real timer, and both measure
// throttles with performance.now(). Point every reading at the faked Date so
// the fake timers drive both sides alike.
const leadRoot = dirname(dirname(dirname(createRequire(import.meta.url).resolve('svelte-adapter-uws/plugins/ratelimit'))));
const leadRuntime = await import(pathToFileURL(realpathSync(join(leadRoot, 'runtime/runtime.js'))).href);

let created;
beforeEach(() => {
	vi.spyOn(console, 'warn').mockImplementation(() => {});
	vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'setImmediate', 'clearImmediate', 'Date'] });
	const clock = { now: () => Date.now(), monotonic: () => Date.now(), processMonotonic: () => Date.now(), wallEpoch: () => Date.now() };
	// Node runs a delay below 1 ms or above the 32-bit ceiling after 1 ms; the
	// fake timers would spin on a fractional one, so apply Node's rule here.
	const delay = (ms) => (ms >= 1 && ms <= 0x7fffffff ? ms : 1);
	const timers = {
		set: (cb, ms, ...a) => setTimeout(cb, delay(ms), ...a),
		setInterval: (cb, ms, ...a) => setInterval(cb, delay(ms), ...a)
	};
	leadRuntime.setRuntimeEnv({ clock, timers });
	setRuntimeEnv({ clock, timers });
	created = [];
});
afterEach(async () => {
	for (const value of created) {
		try { await value?.destroy?.(); } catch { /* best-effort */ }
	}
	leadRuntime.resetRuntimeEnv();
	resetRuntimeEnv();
	vi.useRealTimers();
	vi.restoreAllMocks();
});

/** Cursor keys carry a random per-instance prefix on the Redis side. */
const unkey = (v) => JSON.parse(JSON.stringify(v ?? null).replace(/"[0-9a-f]{16}:/g, '"'));

/** A client that negotiated the cursor short-id dictionary. */
const dictionaryClient = {
	getUserData: () => ({ [Symbol.for('adapter-uws.ws.caps')]: new Set(['cursor.protocol:2', 'cursor.protocol:3', 'presence.protocol:1']) })
};

/**
 * A mock platform that also records the wire-codec paths, when each frame
 * went out, and the bytes a dictionary-capable client would receive for each
 * wire frame (encoded through the codec's per-connection state, as the
 * adapter does).
 */
function recordingPlatform() {
	const platform = mockPlatform();
	const start = Date.now();
	for (const list of [platform.published, platform.sent, platform.sentCoalesced]) {
		const push = list.push.bind(list);
		list.push = (...items) => push(...items.map((item) => ({ ...item, at: Date.now() - start })));
	}
	platform.codecs = [];
	platform.encoded = [];
	const states = new Map();
	const encode = (codec, event, data) => {
		if (!codec) return;
		if (!states.has(codec)) states.set(codec, codec.state?.onAttach?.(dictionaryClient) ?? undefined);
		const bytes = codec.encode(event, unkey(data), states.get(codec));
		platform.encoded.push(bytes == null ? null : Buffer.from(bytes).toString('hex'));
	};
	platform.registerWireCodec = () => {};
	platform.publishWire = (topic, event, data, codec, options) => {
		platform.codecs.push(codec?.capability ?? null);
		encode(codec, event, data);
		return platform.publish(topic, event, data, options);
	};
	platform.sendWire = (ws, topic, event, data, codec, options) => {
		platform.codecs.push(codec?.capability ?? null);
		return platform.send(ws, topic, event, data, options);
	};
	return platform;
}

const settle = (ms = 0) => vi.advanceTimersByTimeAsync(ms);
const events = (platform, name) => platform.published.filter((p) => p.event === name).length +
	platform.sent.filter((s) => s.event === name).length;
const sorted = (value) => JSON.parse(JSON.stringify(value, (_k, v) => (
	v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => a.localeCompare(b))) : v
)));

async function attempt(fn) {
	try {
		return await fn();
	} catch (error) {
		return `throws ${error?.constructor?.name}`;
	}
}

/**
 * Presence: each side is `{ make(options), list(p, topic), count(p, topic) }`.
 * Every scenario receives the side and the options and returns a record.
 */
const presenceSides = {
	lead: { make: (o) => leadPresence(o), list: async (p, t) => p.list(t), count: async (p, t) => p.count(t) },
	redis: { make: (o) => createPresence(mockRedisClient(), o), list: (p, t) => p.list(t), count: (p, t) => p.count(t) }
};

const presenceScenarios = {
	async key(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		await p.join(mockWs({ id: 'u1', ip: 'a', x: 'same' }), 'room', platform);
		await p.join(mockWs({ id: 'u1', ip: 'b', x: 'same' }), 'room', platform);
		await settle(100);
		return { count: await side.count(p, 'room') };
	},
	async select(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		const joined = await attempt(() => p.join(mockWs({ id: 'u1', name: 'Ann', token: 's' }), 'room', platform));
		await settle(100);
		return { joined: typeof joined === 'string' ? joined : 'ok', list: sorted(await side.list(p, 'room')) };
	},
	async heartbeat(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		await p.join(mockWs({ id: 'u1' }), 'room', platform);
		// When the first heartbeat goes out, to the millisecond up to 100 ms and
		// then by the default interval.
		let first = null;
		for (let at = 1; at <= 100 && first === null; at++) {
			await settle(1);
			if (events(platform, 'heartbeat') > 0) first = at;
		}
		if (first === null) {
			await settle(31000 - 100);
			first = events(platform, 'heartbeat') > 0 ? 'by 31000' : 'none';
		}
		return { first };
	},
	async topicThrottle(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		await p.join(mockWs({ id: 'u1' }), 'room', platform);
		await settle(1000);
		platform.reset();
		await p.join(mockWs({ id: 'u2' }), 'room', platform);
		const seen = [];
		let elapsed = 0;
		for (const at of [0, 1, 15, 17, 100]) {
			await settle(at - elapsed);
			elapsed = at;
			seen.push(events(platform, 'diff') > 0);
		}
		return { seen };
	},
	async transient(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		const a = mockWs({ id: 'u1' });
		await p.join(a, 'room', platform);
		await p.update(a, 'room', { typing: true, mood: 'ok' }, platform);
		await settle(100);
		const b = mockWs({ id: 'u2' });
		await p.join(b, 'room', platform);
		await settle(100);
		const state = platform.sent.filter((s) => s.ws === b && s.event === 'state').at(-1);
		const entry = state?.data?.u1 ?? null;
		return { fields: entry ? Object.keys(entry).sort() : null };
	},
	async binary(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		await p.join(mockWs({ id: 'u1' }), 'room', platform);
		await p.join(mockWs({ id: 'u2' }), 'room', platform);
		await settle(100);
		return { codecs: [...new Set(platform.codecs)].sort() };
	},
	async clientUpdateFields(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		const a = mockWs({ id: 'u1' });
		await p.join(a, 'room', platform);
		const updated = await attempt(() => p.update(a, 'room', { a: 1, b: 2 }, platform));
		await settle(100);
		const entry = (await side.list(p, 'room'))[0] ?? null;
		return { updated: typeof updated === 'string' ? updated : 'ok', fields: entry ? Object.keys(entry).sort() : null };
	},
	async maxFieldsBytes(side, options) {
		return fieldSizes(side, options, (n) => ({ a: 'x'.repeat(n) }));
	},
	async maxTotalFieldsBytes(side, options) {
		return fieldSizes(side, options, (n) => ({ a: 'x'.repeat(n), b: 'y'.repeat(n) }));
	},
	async maxTopicsPerConnection(side, options) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		const a = mockWs({ id: 'u1' });
		// One past the default of 100.
		for (let i = 0; i < 101; i++) await attempt(() => p.join(a, 't' + i, platform));
		await settle(100);
		const counts = [];
		for (let i = 0; i < 101; i++) counts.push(await side.count(p, 't' + i));
		return { counts };
	}
};

async function fieldSizes(side, options, fields) {
	const kept = [];
	// Sizes either side of the defaults (8192 per field, 65536 in total).
	for (const n of [1, 100, 5000, 8150, 8250, 32700, 33000]) {
		const p = side.make(options);
		created.push(p);
		const platform = recordingPlatform();
		const a = mockWs({ id: 'u1' });
		await p.join(a, 'room', platform);
		const updated = await attempt(() => p.update(a, 'room', fields(n), platform));
		await settle(100);
		const entry = (await side.list(p, 'room'))[0] ?? {};
		kept.push(typeof updated === 'string' ? updated : Object.keys(entry).filter((k) => k !== 'id').sort().join(','));
	}
	return { kept };
}

/** Cursor: `{ make(options), list(c, topic) }`. */
const cursorSides = {
	lead: { make: (o) => leadCursor(o), list: async (c, t) => c.list(t) },
	redis: { make: (o) => createCursor(mockRedisClient(), o), list: (c, t) => c.list(t) }
};

async function cursorUpdates(side, options, { topic = 'room', moves = [{ x: 1, y: 2 }], gap = 0, sockets = 1, checkpoints = [0, 1, 15, 17, 100] } = {}) {
	const c = side.make(options);
	created.push(c);
	const platform = recordingPlatform();
	const wss = Array.from({ length: sockets }, (_, i) => mockWs({ id: 'u' + i, name: 'n' + i }));
	let outcome = 'ok';
	for (const ws of wss) {
		for (const move of moves) {
			const r = await attempt(() => c.update(ws, topic, move, platform));
			if (typeof r === 'string') outcome = r;
			if (gap) await settle(gap);
		}
	}
	const seen = [];
	let elapsed = 0;
	for (const at of checkpoints) {
		await settle(at - elapsed);
		elapsed = at;
		seen.push([platform.published.length, platform.sent.length, platform.sentCoalesced.length]);
	}
	const frames = [
		...platform.published.map((p) => ['publish', p.at, p.topic.length, p.event, sorted(unkey(p.data))]),
		...platform.sent.map((p) => ['send', p.at, p.event, sorted(unkey(p.data))]),
		...platform.sentCoalesced.map((p) => ['coalesced', p.at, p.event, sorted(unkey(p.data))])
	];
	return { outcome, seen, codecs: [...new Set(platform.codecs)].sort(), encoded: platform.encoded, frames };
}

const cursorScenarios = {
	select: (side, o) => cursorUpdates(side, o),
	throttle: (side, o) => cursorUpdates(side, o, { moves: [{ x: 1, y: 1 }, { x: 50, y: 50 }, { x: 99, y: 99 }], gap: 6 }),
	topicThrottle: (side, o) => cursorUpdates(side, o, { sockets: 3 }),
	binary: (side, o) => cursorUpdates(side, o),
	dictionary: (side, o) => cursorUpdates(side, o, { sockets: 2 }),
	// Either side of the 8192-byte default.
	maxDataBytes: (side, o) => cursorUpdates(side, o, { moves: [{ x: 1, y: 1, note: 'x'.repeat(60) }, { x: 2, y: 2, note: 'y'.repeat(5000) }, { x: 3, y: 3, note: 'z'.repeat(8150) }, { x: 4, y: 4, note: 'w'.repeat(8200) }, { x: 5, y: 5 }], gap: 50 }),
	maxTopicLength: async (side, o) => ({
		short: await cursorUpdates(side, o, { topic: 't'.repeat(5) }),
		edge: await cursorUpdates(side, o, { topic: 't'.repeat(256) }),
		long: await cursorUpdates(side, o, { topic: 't'.repeat(257) })
	}),
	minMove: (side, o) => cursorUpdates(side, o, { moves: [{ x: 0, y: 0 }, { x: 0.5, y: 0 }, { x: 3, y: 0 }, { x: 20, y: 0 }], gap: 50, checkpoints: [0, 100] }),
	// minMove is measured on what position() returns, so it decides which moves go out.
	position: (side, o) => cursorUpdates(side, { minMove: 5, ...o }, { moves: [{ x: 0, y: 0 }, { x: 1, y: 0 }, { x: 10, y: 0 }, { x: 11, y: 0 }], gap: 50 }),
	async viewport(side, o) {
		const c = side.make(o);
		created.push(c);
		const platform = recordingPlatform();
		const a = mockWs({ id: 'a' });
		const b = mockWs({ id: 'b' });
		await c.update(a, 'room', { x: 0, y: 0 }, platform);
		await c.update(b, 'room', { x: 0, y: 0 }, platform);
		await settle(50);
		const reported = await attempt(() => c.viewport(a, 'room', { x: 5000, y: 5000, w: 100, h: 100, zoom: 1 }));
		platform.reset();
		for (let i = 1; i < 5; i++) {
			await c.update(b, 'room', { x: i * 10, y: i * 10 }, platform);
			await settle(40);
		}
		await settle(200);
		return {
			reported: typeof reported === 'string' ? reported : 'ok',
			published: platform.published.map((p) => p.event),
			sent: platform.sent.map((p) => [p.ws?.getUserData?.().id, p.event]),
			coalesced: platform.sentCoalesced.map((p) => [p.ws?.getUserData?.().id, p.event])
		};
	},
	backpressure: (side, o) => cursorUpdates(side, o, { sockets: 2 })
};

/** Groups: `{ make(options), meta(g), join(g, ws, platform) }`. */
const groupSides = {
	lead: { make: (o) => leadGroup('room', o), meta: async (g) => g.meta },
	redis: { make: (o) => createGroup(mockRedisClient(), 'room', o), meta: (g) => g.getMeta() }
};

async function groupJoins(side, options, count = 3) {
	const g = side.make(options);
	created.push(g);
	const platform = recordingPlatform();
	const joins = [];
	for (let i = 0; i < count; i++) joins.push(await attempt(() => g.join(mockWs({ id: 'u' + i }), platform)));
	await settle(100);
	return { g, joins, platform };
}

const groupScenarios = {
	async meta(side, options) {
		const g = side.make(options);
		created.push(g);
		await settle(10);
		// Redis stores hash fields as strings, so compare the string forms.
		const meta = await side.meta(g);
		return { meta: Object.fromEntries(Object.entries(meta ?? {}).map(([k, v]) => [k, String(v)]).sort(([a], [b]) => a.localeCompare(b))) };
	},
	async maxMembers(side, options) {
		const { g, joins } = await groupJoins(side, options);
		return { maxMembers: g.maxMembers, joins };
	},
	onJoin: groupHooks,
	onLeave: groupHooks,
	onFull: groupHooks,
	onClose: groupHooks
};

/**
 * Wrap each hook in the options so every call is recorded with its
 * arguments (a socket as its id) and what the hook returned, then run a
 * fixed sequence on a group capped at two: a joins, b joins as admin, c is
 * refused, a joins again, b leaves twice, the group closes.
 */
async function groupHooks(side, options) {
	const calls = [];
	const wrapped = { maxMembers: 2, ...options };
	for (const name of ['onJoin', 'onLeave', 'onFull', 'onClose']) {
		const hook = wrapped[name];
		if (typeof hook !== 'function') continue;
		wrapped[name] = (...args) => {
			const result = hook(...args);
			calls.push([name, ...args.map((a) => (a && typeof a.getUserData === 'function' ? 'ws:' + a.getUserData().id : a)), sorted(result ?? null)]);
			return result;
		};
	}
	const g = side.make(wrapped);
	created.push(g);
	const platform = recordingPlatform();
	const a = mockWs({ id: 'a' });
	const b = mockWs({ id: 'b' });
	const c = mockWs({ id: 'c' });
	const joins = [
		await attempt(() => g.join(a, platform)),
		await attempt(() => g.join(b, platform, 'admin')),
		await attempt(() => g.join(c, platform)),
		await attempt(() => g.join(a, platform))
	];
	await settle(50);
	await attempt(() => g.leave(b, platform));
	await attempt(() => g.leave(b, platform));
	await settle(50);
	await attempt(() => g.close(platform));
	await settle(50);
	return { joins, calls };
}

/** Rate limit: the verdict sequence over time, in the default window mode. */
const rateSides = {
	lead: (o) => leadRateLimit({ points: 2, interval: 1000, ...o }),
	redis: (o) => createRateLimit(mockRedisClient(), { points: 2, interval: 1000, ...o })
};

async function verdicts(side, options) {
	const limiter = side(options);
	created.push(limiter);
	const sockets = [mockWs({ ip: '1.1.1.1', tenant: 't1' }), mockWs({ ip: '1.1.1.1', tenant: 't2' }), mockWs({ ip: '2.2.2.2', tenant: 't1' })];
	const out = [];
	for (const at of [0, 0, 0, 0, 1500, 4000]) {
		if (at) await settle(at);
		for (const ws of sockets) {
			const r = await attempt(() => limiter.consume(ws));
			out.push(typeof r === 'string' ? r : `${r.allowed}/${r.remaining}`);
		}
	}
	return { out };
}

const rateScenarios = Object.fromEntries(['points', 'interval', 'blockDuration', 'keyBy', 'tenant', 'budget'].map((k) => [k, verdicts]));

const plugins = [
	['presence', presenceSides, presenceScenarios],
	['cursor', cursorSides, cursorScenarios],
	['groups', groupSides, groupScenarios],
	['ratelimit', { lead: rateSides.lead, redis: rateSides.redis }, rateScenarios]
];

/**
 * Rows where the two sides are known to differ, each with the reason. A
 * keyBy that returns a fresh object every call: the in-memory limiter keys
 * its Map by that object's identity, so every call gets a new bucket and
 * nothing is ever limited; the Redis limiter turns the key into a string,
 * so every such call shares one bucket.
 */
const divergent = new Map([
	['ratelimit.keyBy=fn(0)/' + ODD_VALUES.findIndex((v) => typeof v === 'function'), 'object key'],
	['ratelimit.keyBy=fn(1)/' + (ODD_VALUES.findIndex((v) => typeof v === 'function') + 1), 'object key']
]);

/** Values a side accepts at construction, judged without running a scenario. */
function constructs(side, key, value) {
	try {
		const made = (side.make ?? side)({ [key]: value });
		created.push(made);
		return true;
	} catch {
		return false;
	}
}

const rows = [];
for (const [name, sides, scenarios] of plugins) {
	for (const [key, scenario] of Object.entries(scenarios)) {
		for (const value of ODD_VALUES) rows.push([`${name}.${key}=${describeValue(value)}`, sides, scenario, key, value]);
	}
}

const scenarioKeys = plugins.flatMap(([name, sides, scenarios]) => Object.entries(scenarios).map(([key, scenario]) => [`${name}.${key}`, sides, scenario, key]));

describe('options shared with the in-memory plugins behave the same', () => {
	// A scenario that records the same thing for every value cannot tell two
	// readings of the option apart.
	it.each(scenarioKeys)('%s: the scenario tells the in-memory plugin\'s values apart', async (_id, sides, scenario, key) => {
		const records = new Set();
		for (const value of ODD_VALUES) {
			if (!constructs(sides.lead, key, value)) continue;
			records.add(JSON.stringify(await scenario(sides.lead, { [key]: value })));
		}
		expect(records.size).toBeGreaterThanOrEqual(2);
	});

	it.each(rows)('%s', async (_id, sides, scenario, key, value) => {
		if (!constructs(sides.lead, key, value) || !constructs(sides.redis, key, value)) return;
		const lead = await scenario(sides.lead, { [key]: value });
		const redis = await scenario(sides.redis, { [key]: value });
		if (divergent.has(_id)) expect(redis).not.toEqual(lead);
		else expect(redis).toEqual(lead);
	});
});
