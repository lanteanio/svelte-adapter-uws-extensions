import { describe, expect, it } from 'vitest';
import { PLATFORM_MEMBERS } from 'svelte-adapter-uws/platform-contract';
import { mockPlatform, PLATFORM_KEYS } from '../../src/testing/mock-platform.js';
import { mockWs } from '../helpers/mock-ws.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

const forwarded = [
	'adviseReconnect', 'bumpTopicEpoch', 'diagnostic', 'grantPublish', 'introspect',
	'isWarmupRequest', 'metricsSnapshot', 'publishGame', 'publishGrant', 'revokePublish',
	'publishWireBatch', 'sendWireBatch'
];

describe('canonical Platform inventory', () => {
	it('uses the exported adapter inventory directly', () => {
		expect(PLATFORM_KEYS).toBe(PLATFORM_MEMBERS);
		expect(Object.isFrozen(PLATFORM_KEYS)).toBe(true);
	});

	for (const create of [createPubSubBus, createShardedBus]) {
		for (const traced of [false, true]) {
			it(`${create.name} forwards available methods with tracing ${traced}`, () => {
				const platform = mockPlatform();
				platform.trace.enabled = traced;
				const args = [{ opaque: true }, ['entry'], { options: true }];
				const result = Promise.resolve(Symbol('source result'));
				for (const member of forwarded) {
					platform[member] = function (...actual) {
						expect(this).toBe(platform);
						expect(actual).toEqual(args);
						return result;
					};
				}
				const wrapped = create(mockRedisClient()).wrap(platform);
				for (const member of forwarded) expect(wrapped[member](...args)).toBe(result);
			});
		}

		it(`${create.name} exposes live metrics and assertion references`, () => {
			const platform = mockPlatform();
			const wrapped = create(mockRedisClient()).wrap(platform);
			expect(wrapped.metrics).toBe(null);
			expect(wrapped.assertions).toBe(platform.assertions);
			platform.metrics = { opaque: true };
			platform.assertions = new Map([['count', 2]]);
			expect(wrapped.metrics).toBe(platform.metrics);
			expect(wrapped.assertions).toBe(platform.assertions);
		});
	}

	it('records the logical wire fallback, preserving per-entry options', () => {
		const platform = mockPlatform();
		const socket = mockWs();
		const codec = { capability: 'fixture', schemaVersion: 1, encode: () => null };
		const first = { value: 1 }, second = { value: 2 };
		expect(platform.publishWireBatch('room', 'update', [
			{ data: first, seq: 7, excludeWs: socket }, { data: second }
		], codec, { seq: false, compress: true })).toBe(true);
		expect(platform.published).toEqual([
			{ topic: 'room', event: 'update', data: first, options: { seq: 7, compress: true, excludeWs: socket } },
			{ topic: 'room', event: 'update', data: second, options: { seq: false, compress: true } }
		]);
		expect(platform.sendWireBatch(socket, 'room', 'update', [{ data: first }, { data: second }], codec, { compress: false })).toBe(1);
		expect(platform.sent.map(({ data }) => data)).toEqual([first, second]);
		platform.reset();
		platform.publishWireBatch('room', 'update', [{ data: first, excludeWs: null }], codec, { seq: false, excludeWs: socket });
		expect(platform.published[0].options.excludeWs).toBe(socket);
	});

	it.each(['publishWire', 'sendWire', 'publishWireBatch', 'sendWireBatch'])('records the wire compression default and explicit choices for %s', method => {
		const platform = mockPlatform();
		const socket = mockWs();
		const codec = { capability: 'fixture', schemaVersion: 1, encode: () => null };
		const data = { value: 1 };
		const invoke = options => {
			if (method === 'publishWire') return platform.publishWire('room', 'update', data, codec, options);
			if (method === 'sendWire') return platform.sendWire(socket, 'room', 'update', data, codec, options);
			if (method === 'publishWireBatch') return platform.publishWireBatch('room', 'update', [{ data }], codec, options);
			return platform.sendWireBatch(socket, 'room', 'update', [{ data }], codec, options);
		};
		for (const options of [undefined, null, {}, { compress: undefined }, { compress: false }, { compress: true }]) {
			platform.reset();
			if (options) Object.freeze(options);
			expect(invoke(options)).toBe(method.startsWith('publish') ? true : 1);
			const records = method.startsWith('publish') ? platform.published : platform.sent;
			expect(records).toHaveLength(1);
			expect(records[0].data).toBe(data);
			expect(records[0].options.compress).toBe(options?.compress === true);
		}
		platform.reset();
		const options = Object.freeze({ relay: false, seq: false, excludeWs: socket, compress: undefined });
		invoke(options);
		const records = method.startsWith('publish') ? platform.published : platform.sent;
		const expected = { relay: false, seq: false, excludeWs: socket, compress: false };
		expect(records[0].options).toEqual(expected);
		expect(options.compress).toBeUndefined();
	});

	it.each(['publishWire', 'sendWire'])('reads an inherited compression choice once in %s', method => {
		const platform = mockPlatform();
		const socket = mockWs();
		let reads = 0;
		const options = Object.create({ get compress() { reads++; return true; } });
		const codec = { capability: 'fixture', schemaVersion: 1, encode: () => null };
		if (method === 'publishWire') platform.publishWire('room', 'update', {}, codec, options);
		else platform.sendWire(socket, 'room', 'update', {}, codec, options);
		expect(reads).toBe(1);
		expect((method === 'publishWire' ? platform.published : platform.sent)[0].options.compress).toBe(true);
	});

	it('keeps grants per socket, excludes game senders, and advances topic epochs', () => {
		const platform = mockPlatform();
		const sender = mockWs(), peer = mockWs();
		platform.forEachSubscriber = (_topic, visit) => { visit(sender); visit(peer); };
		expect(platform.publishGrant(sender)).toBe(null);
		expect(platform.grantPublish(sender, 'room')).toBe(true);
		expect(platform.publishGrant(sender)).toBe('room');
		expect(platform.publishGrant(peer)).toBe(null);
		expect(platform.publishGame(sender, 'room', 'input', { value: 1 }, 'client-id')).toEqual({ seq: 1, delivered: 1 });
		expect(platform.sent).toEqual([{ ws: peer, topic: 'room', event: 'input', data: { value: 1 }, options: { seq: 1 }, id: 'client-id' }]);
		expect(platform.revokePublish(sender)).toBe(true);
		expect(platform.revokePublish(sender)).toBe(false);
		platform.random.u32 = () => 0;
		expect(platform.topicEpoch('room')).toBe(0);
		expect(platform.bumpTopicEpoch('room')).toBe(1);
		expect(platform.topicEpoch('room')).toBe(1);
		expect(platform.topicEpoch('other')).toBe(0);
	});

	it('provides the complete published pressure placeholder through each wrapper', () => {
		const platform = mockPlatform();
		expect(platform.pressure).toEqual({
			sampledAt: null, active: false, value: 0,
			subscriberRatio: 0, publishRate: 0, memoryMB: 0, reason: 'NONE',
			psi: null, cpuThrottle: null, maxBufferedBytes: 0,
			backpressuredConnections: 0, droppedFrames: 0, droppedBytes: 0,
			egress: { deliveries: 0, bytes: 0, refusedTopic: 0, refusedTenant: 0 },
			topPublishers: []
		});
		for (const create of [createPubSubBus, createShardedBus]) {
			const wrapped = create(mockRedisClient()).wrap(platform);
			expect(wrapped.pressure).toBe(platform.pressure);
			expect(wrapped.pressure.egress.bytes).toBe(0);
			expect(wrapped.pressure.topPublishers).toEqual([]);
		}
	});

	it('refuses grants on a freed handle without exposing its former binding', () => {
		const platform = mockPlatform();
		let freed = false;
		const socket = { getUserData() { if (freed) throw new Error('freed WebSocket'); return {}; } };
		expect(platform.grantPublish(socket, 'room')).toBe(true);
		expect(platform.publishGrant(socket)).toBe('room');
		freed = true;
		expect(platform.publishGrant(socket)).toBe(null);
		expect(platform.revokePublish(socket)).toBe(false);
		expect(platform.closedWsAborts).toBe(0);
		expect(platform.grantPublish(socket, 'replacement')).toBe(false);
		expect(platform.closedWsAborts).toBe(1);
	});

	it.each([createPubSubBus, createShardedBus])('retains native closed-grant refusals through %s', create => {
		const platform = mockPlatform();
		const wrapped = create(mockRedisClient()).wrap(platform);
		const socket = { getUserData() { throw new Error('freed WebSocket'); } };
		expect(wrapped.grantPublish(socket, 'room')).toBe(false);
		expect(wrapped.publishGrant(socket)).toBe(null);
		expect(wrapped.revokePublish(socket)).toBe(false);
		expect(wrapped.closedWsAborts).toBe(1);
	});

	it('returns empty diagnostic and metric state without leaking topic labels', async () => {
		const platform = mockPlatform();
		platform.pressure.topPublishers = [{ topic: 'private-topic' }];
		platform.assertions.set('example', 1);
		const snapshot = platform.introspect();
		expect(snapshot.assertions).toEqual({ example: 1 });
		expect(JSON.stringify(snapshot)).not.toContain('private-topic');
		expect(snapshot.diagnostics).toEqual({ retained: 0, recent: [] });
		expect(platform.diagnostic('unknown')).toBe(null);
		expect(await platform.metricsSnapshot()).toBe(null);
		expect(await platform.requestTopic('room', 'query')).toEqual([]);
	});
});
