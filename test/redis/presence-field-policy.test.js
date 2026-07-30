import { describe, it, expect, vi } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { mockWs } from '../../src/testing/mock-ws.js';
import { createPresence } from '../../src/redis/presence.js';
import { isReservedPresenceField, mergeFields, newFieldMap } from '../../src/redis/presence/field-policy.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const opts = { select: (u) => ({ id: u.id }) };

/**
 * Wait until the join diff has actually flushed. The flush is armed with
 * setTimeout(0), which a loaded machine can starve past a fixed sleep(30) -
 * then the join diff lands AFTER platform.reset() and a "no diff frames"
 * assertion counts the join itself. Poll for the frame instead of racing it.
 */
async function waitForDiffFlush(platform, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (platform.sent.some((f) => f.event === 'diff') || platform.published.some((f) => f.event === 'diff')) return;
		await sleep(5);
	}
	throw new Error('join diff never flushed');
}

describe('presence field policy', () => {
	it('agrees with the adapter on which names are reserved', () => {
		for (const k of ['__proto__', 'constructor', 'prototype', '__admin', '__internal']) {
			expect(isReservedPresenceField(k)).toBe(true);
		}
		for (const k of ['color', 'isAdmin', 'x', 'a__b', 'proto']) {
			expect(isReservedPresenceField(k)).toBe(false);
		}
	});

	it('mergeFields never reaches a prototype setter', () => {
		const target = newFieldMap();
		mergeFields(target, JSON.parse('{"__proto__":{"polluted":true},"color":"red"}'));
		expect(target.color).toBe('red');
		expect(({}).polluted).toBeUndefined();
		expect(Object.getPrototypeOf(target)).toBe(null);
	});
});

describe('presence update ingress', () => {
	it('drops a reserved field so change detection stays intact', async () => {
		const client = mockRedisClient('p1:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const attacker = mockWs({ id: 'mallory' });
		await presence.join(attacker, 'room', platform);

		// Assigning '__proto__' into a plain object invokes the prototype
		// setter rather than storing data, so a later legitimate update whose
		// value equals the planted one compares equal and is never relayed.
		await presence.update(attacker, 'room', JSON.parse('{"__proto__":{"isAdmin":true}}'), platform);
		await sleep(30);

		// The null-prototype container alone makes the change-detection half
		// of this pass, because `__proto__` then lands as an ordinary own key
		// and no longer compares equal to a later real value. The reserved-name
		// PREDICATE is what stops it being stored and re-served at all, so
		// assert that too: a `__proto__` key must never reach a roster frame.
		const planted = JSON.stringify(platform.sent) + JSON.stringify(platform.published);
		expect(planted).not.toContain('__proto__');
		expect(planted).not.toContain('isAdmin');

		platform.reset();
		await presence.update(attacker, 'room', { isAdmin: true }, platform);
		await sleep(30);

		expect(JSON.stringify(platform.sent) + JSON.stringify(platform.published)).toContain('isAdmin');
		await presence.destroy?.();
	});

	it('drops a reserved field that the container alone would happily store', async () => {
		// `__proto__` is the wrong probe for the PREDICATE: a null-prototype
		// container plus defineProperty already neutralizes it, so that case
		// passes with the reserved-name check deleted. These names are reserved
		// but carry no prototype hazard at all - nothing but the predicate stops
		// them being stored and re-served, and a field accepted here rides
		// publicData() into every roster, diff and heartbeat frame the topic
		// emits.
		const client = mockRedisClient('p1b:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const attacker = mockWs({ id: 'mallory' });
		await presence.join(attacker, 'room', platform);
		platform.reset();

		await presence.update(attacker, 'room', {
			__admin: true,
			__internal: 'x',
			constructor: 'c',
			prototype: 'p'
		}, platform);
		await sleep(30);

		const emitted = JSON.stringify(platform.sent) + JSON.stringify(platform.published);
		for (const k of ['__admin', '__internal']) expect(emitted, k).not.toContain(k);
		// `constructor` / `prototype` as ordinary string keys - present in the
		// frame only if the predicate let them through.
		expect(emitted).not.toContain('"constructor"');
		expect(emitted).not.toContain('"prototype"');

		// And the roster read back from Redis agrees, so nothing was merely
		// withheld from the live frame while being stored durably.
		const list = await presence.list('room');
		for (const entry of list) {
			for (const k of ['__admin', '__internal', 'constructor', 'prototype']) {
				expect(Object.prototype.hasOwnProperty.call(entry, k), k).toBe(false);
			}
		}
		await presence.destroy?.();
	});

	it('keeps ordinary fields working', async () => {
		const client = mockRedisClient('p2:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const ws = mockWs({ id: 'alice' });
		await presence.join(ws, 'room', platform);
		platform.reset();
		await presence.update(ws, 'room', { color: 'red' }, platform);
		await sleep(30);
		expect(JSON.stringify(platform.sent) + JSON.stringify(platform.published)).toContain('red');
		await presence.destroy?.();
	});
});

describe('presence cross-instance bus ingress', () => {
	it('drops malformed envelope, event, key, and data shapes', async () => {
		const client = mockRedisClient('p-shape:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		await presence.join(mockWs({ id: 'local' }), 'room', platform);
		await waitForDiffFlush(platform);
		platform.reset();

		const channel = 'p-shape:presence:events:room';
		const invalid = [
			// instanceId and envelope topic are part of the canonical envelope,
			// not optional metadata.
			{ instanceId: 123, topic: 'room', event: 'join', payload: { key: 'phantom', data: { id: 'phantom' } } },
			{ instanceId: 'foreign', event: 'join', payload: { key: 'phantom', data: { id: 'phantom' } } },
			{ instanceId: 'foreign', topic: 'other-room', event: 'join', payload: { key: 'phantom', data: { id: 'phantom' } } },
			// Event enum, payload container, key and event-specific data shapes.
			{ instanceId: 'foreign', topic: 'room', event: 'bogus', payload: { key: 'phantom', data: { id: 'phantom' } } },
			{ instanceId: 'foreign', topic: 'room', event: 'join', payload: null },
			{ instanceId: 'foreign', topic: 'room', event: 'join', payload: { key: { coerced: true }, data: { id: 'phantom' } } },
			{ instanceId: 'foreign', topic: 'room', event: 'join', payload: { key: 'phantom', data: 'PHANTOM' } },
			{ instanceId: 'foreign', topic: 'room', event: 'leave', payload: { key: 'real', data: [] } },
			{ instanceId: 'foreign', topic: 'room', event: 'fields', payload: { key: 'real', durable: [], transient: {} } },
			{ instanceId: 'foreign', topic: 'room', event: 'fields', payload: { key: 'real', durable: {}, transient: null } }
		];
		for (const envelope of invalid) {
			await client.redis.publish(channel, JSON.stringify(envelope));
		}
		await sleep(30);
		presence.flushDiffs();
		expect([...platform.sent, ...platform.published].filter((f) => f.event === 'diff')).toHaveLength(0);

		// Positive control: the canonical shape still reaches the observer.
		await client.redis.publish(channel, JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'join',
			payload: { key: 'remote', data: { id: 'remote' } }
		}));
		await sleep(30);
		presence.flushDiffs();
		const diff = [...platform.sent, ...platform.published].find((f) => f.event === 'diff');
		expect(diff?.data?.joins?.remote).toEqual({ id: 'remote' });
		await presence.destroy?.();
	});

	it('drops an oversized envelope before JSON.parse', async () => {
		const client = mockRedisClient('p-cap:');
		const platform = mockPlatform();
		const presence = createPresence(client, { ...opts, maxEnvelopeBytes: 256 });
		await presence.join(mockWs({ id: 'local' }), 'room', platform);
		await waitForDiffFlush(platform);
		platform.reset();

		const channel = 'p-cap:presence:events:room';
		const raw = JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'join',
			payload: { key: 'oversized', data: { id: 'oversized', blob: 'x'.repeat(512) } }
		});
		const realParse = JSON.parse;
		let parsedOversizedEnvelope = false;
		JSON.parse = function trackedParse(value, ...args) {
			if (value === raw) parsedOversizedEnvelope = true;
			return realParse(value, ...args);
		};
		try {
			await client.redis.publish(channel, raw);
			await sleep(20);
		} finally {
			JSON.parse = realParse;
		}

		presence.flushDiffs();
		expect(parsedOversizedEnvelope).toBe(false);
		expect([...platform.sent, ...platform.published].filter((f) => f.event === 'diff')).toHaveLength(0);
		await presence.destroy?.();
	});

	it('warns and drops an outbound envelope that every peer would reject', async () => {
		const client = mockRedisClient('p-outbound:');
		const platform = mockPlatform();
		const publish = vi.spyOn(client.redis, 'publish');
		const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
		const presence = createPresence(client, {
			maxEnvelopeBytes: 256,
			select: (u) => ({ id: u.id, blob: 'x'.repeat(512) })
		});
		try {
			await presence.join(mockWs({ id: 'large' }), 'room', platform);
			expect(publish.mock.calls.some(([channel]) => channel === 'p-outbound:presence:events:room')).toBe(false);
			expect(warn).toHaveBeenCalledWith(expect.stringMatching(/exceeds maxEnvelopeBytes/));
		} finally {
			await presence.destroy?.();
			warn.mockRestore();
			publish.mockRestore();
		}
	});

	it('rejects invalid maxEnvelopeBytes at construction', () => {
		for (const value of [0, -1, 1.5, NaN, '256']) {
			expect(
				() => createPresence(mockRedisClient('p-invalid-cap:'), { ...opts, maxEnvelopeBytes: value })
			).toThrow(/maxEnvelopeBytes must be a positive integer/);
		}
	});

	it('applies the same field rule as the local path', async () => {
		const client = mockRedisClient('p3:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const member = mockWs({ id: 'alice' });
		await presence.join(member, 'room', platform);

		// Let the join diff FLUSH before forging. While it is still pending,
		// bufferUpdate folds any update for a locally-presented user into that
		// join and discards it - which masks the field predicate under test.
		await waitForDiffFlush(platform);
		platform.reset();

		// A forged FIELDS envelope must not land a field name the client
		// path refuses. Storing it safely (as own data) is not enough: it
		// then rides publicData() into every roster and heartbeat frame,
		// which makes the bus a phantom-field injector.
		await client.redis.publish('p3:presence:events:room', JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'fields',
			payload: { key: 'alice', durable: JSON.parse('{"__proto__":{"isAdmin":true},"__admin":1}'), transient: {} }
		}));
		await sleep(30);

		// The bus merge lands in live in-memory state, so the observable is
		// what this instance then emits to its own subscribers.
		expect(JSON.stringify(platform.sent) + JSON.stringify(platform.published)).not.toContain('__admin');
		expect(({}).isAdmin).toBeUndefined();

		// And change detection still works for a real update.
		platform.reset();
		await presence.update(member, 'room', { isAdmin: true }, platform);
		await sleep(30);
		expect(JSON.stringify(platform.sent) + JSON.stringify(platform.published)).toContain('isAdmin');
		await presence.destroy?.();
	});

	it('emits no field updates for an all-reserved envelope', async () => {
		const client = mockRedisClient('p5:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const member = mockWs({ id: 'alice' });
		await presence.join(member, 'room', platform);
		// Let the join diff FLUSH before resetting. While it is still pending,
		// bufferUpdate folds any update for a locally-presented user into that
		// join and discards it - which masks the behaviour under test entirely.
		// Poll for the flush rather than sleeping a fixed budget: a starved
		// timer phase otherwise lands the join diff after the reset and the
		// frame count below flaps between 0 and 1.
		await waitForDiffFlush(platform);
		platform.reset();

		// Emptiness was tested on the RAW envelope, so an envelope carrying
		// only rejected names still counted as non-empty and fanned an
		// `update` diff - containing nothing - out to every local subscriber.
		await client.redis.publish('p5:presence:events:room', JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'fields',
			payload: { key: 'alice', durable: { __admin: 1, constructor: 2 }, transient: { prototype: 3 } }
		}));
		await sleep(30);

		const frames = [...platform.sent, ...platform.published];
		// Presence emits ONE event name, `diff`, and carries the changed
		// fields in `data.updates`. Filtering on an `update` / `updates`
		// EVENT name matches nothing presence has ever emitted, so the
		// assertion held whatever the subscriber did with the envelope.
		expect(frames.filter((f) => f.event === 'diff')).toHaveLength(0);
		await presence.destroy?.();
	});

	it('relays an ordinary cross-instance field update', async () => {
		const client = mockRedisClient('p4:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const member = mockWs({ id: 'alice' });
		await presence.join(member, 'room', platform);
		platform.reset();
		await client.redis.publish('p4:presence:events:room', JSON.stringify({
			instanceId: 'foreign',
			topic: 'room',
			event: 'fields',
			payload: { key: 'alice', durable: { color: 'blue' }, transient: {} }
		}));
		await sleep(30);
		expect(JSON.stringify(platform.sent) + JSON.stringify(platform.published)).toContain('blue');
		await presence.destroy?.();
	});
});

describe('presence roster keys', () => {
	it('does not let a reserved name become the roster key', async () => {
		const client = mockRedisClient('p5:');
		const platform = mockPlatform();
		// A roster key is a plain-object property name in every snapshot and
		// diff downstream, so '__proto__' there targets the prototype and the
		// entry silently vanishes from state.
		const presence = createPresence(client, { keyField: 'id', select: (u) => ({ id: u.id }) });
		const ws = mockWs({ id: '__proto__' });
		await presence.join(ws, 'room', platform);
		await sleep(20);

		// Read the roster the way a subscriber does: sync() emits the keyed
		// state map, which is exactly the plain object a '__proto__' key
		// would disappear into.
		platform.reset();
		const observer = mockWs({ id: 'observer' });
		await presence.sync(observer, 'room', platform);
		const stateFrame = platform.sent.find((e) => e.event === 'state');
		expect(stateFrame).toBeDefined();
		const keys = Object.keys(stateFrame.data);
		expect(keys).toHaveLength(1);
		expect(keys[0]).not.toBe('__proto__');
		expect(keys[0].startsWith('__conn:')).toBe(true);
		await presence.destroy?.();
	});
});

describe('presence read-back from Redis', () => {
	it('applies the field rule to durable fields already stored', async () => {
		const client = mockRedisClient('p6:');
		const platform = mockPlatform();
		const presence = createPresence(client, opts);
		const ws = mockWs({ id: 'alice' });
		await presence.join(ws, 'room', platform);
		await sleep(20);

		// Simulate what a pre-fix instance mid-rolling-upgrade (or anything
		// else with Redis write access) leaves behind: reserved names stored
		// durably. Read back unfiltered they ride into every roster frame,
		// and '__proto__' lands on the emitted object's PROTOTYPE.
		const key = 'p6:presence:topic:{room}';
		const all = await client.redis.hgetall(key);
		const field = Object.keys(all)[0];
		const entry = JSON.parse(all[field]);
		entry.fields = JSON.parse('{"__admin":true,"__proto__":{"polluted":1},"color":"red"}');
		await client.redis.hset(key, field, JSON.stringify(entry));

		platform.reset();
		await presence.sync(mockWs({ id: 'observer' }), 'room', platform);
		const state = platform.sent.find((e) => e.event === 'state');
		expect(state).toBeDefined();
		const emitted = JSON.stringify(state.data);
		expect(emitted).not.toContain('__admin');
		expect(emitted).toContain('red');
		for (const v of Object.values(state.data)) expect(v.polluted).toBeUndefined();
		await presence.destroy?.();
	});
});
