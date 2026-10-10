// What a bus carries of a publish's seq choice, on a single worker (the
// multi-worker side is test/shared/cluster-sequence-authority.test.js). An
// authoritative number crosses as `s` so every instance stamps the same seq;
// `seq: false` crosses as `s: false` so the frame stays without one
// everywhere; anything else carries nothing and each instance stamps its own
// counter, as before. Null disables sequencing and bigint authorities cross
// as numbers. On one worker the local publish gets
// the caller's options untouched.

import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../../src/testing/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { MULTI_WORKER_RUNTIME, carriedSeq, receivedSeq } from '../../src/shared/sequence-authority.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function until(check, timeoutMs = 2000) {
	const start = Date.now();
	while (!check()) {
		if (Date.now() - start > timeoutMs) throw new Error('condition never held');
		await sleep(5);
	}
}

function wirePlatform() {
	const p = mockPlatform();
	p.publishedWire = [];
	p.publishWire = (topic, event, data, wire, options) => {
		p.publishedWire.push({ topic, event, data, wire, options });
		return true;
	};
	p.registerWireCodec = () => {};
	return p;
}

const sharedCodec = {
	capability: 'test.shared:1',
	schemaVersion: 1,
	shared: true,
	encode: (event, data) => new TextEncoder().encode(JSON.stringify({ event, data }))
};

it('runs on a single worker', () => {
	expect(MULTI_WORKER_RUNTIME).toBe(false);
});

describe('carriedSeq and receivedSeq', () => {
	it('carry an authority as a number, seq: false as false, and anything else as nothing', () => {
		expect(carriedSeq(9)).toBe(9);
		expect(carriedSeq(false)).toBe(false);
		expect(carriedSeq(9n)).toBe(9);
		expect(carriedSeq(null)).toBe(false);
		for (const seq of [true, undefined, '9']) expect(carriedSeq(seq), String(seq)).toBeUndefined();
	});

	it('take a positive safe integer or false, and leave anything else to the counter', () => {
		expect(receivedSeq(9)).toBe(9);
		expect(receivedSeq(false)).toBe(false);
		for (const s of [undefined, null, true, 0, -1, 1.5, NaN, Infinity, 2 ** 60, '7', {}]) {
			expect(receivedSeq(s), String(s)).toBeUndefined();
		}
	});
});

const BUSES = [
	['pubsub bus', (client) => createPubSubBus(client), async () => {},
		(client, envelope) => client.redis.publish('uws:pubsub', JSON.stringify(envelope))],
	['sharded bus', (client) => createShardedBus(client), async (bus, topic) => { await bus.follow(topic); },
		(client, envelope) => client.redis.spublish('uws:sharded:chat', JSON.stringify(envelope))]
];

// Each bus with and without a tracing provider: wrap() picks the traced or the
// untraced publish paths once, from the platform it wraps.
describe.each(BUSES.flatMap(([name, ...rest]) => [[name, ...rest, false], [name + ', traced', ...rest, true]]))('the %s on a single worker', (_name, createBus, follow, inject, traced) => {
	async function pair(prefix, makePlatform = mockPlatform) {
		const client = mockRedisClient(prefix);
		const busA = createBus(client);
		const busB = createBus(client);
		const a = makePlatform();
		const b = makePlatform();
		a.trace.enabled = traced;
		b.trace.enabled = traced;
		await busA.activate(a);
		await busB.activate(b);
		await follow(busB, 'chat');
		return { client, busA, busB, a, b, wrapped: busA.wrap(a), close: async () => { await busA.deactivate(); await busB.deactivate(); } };
	}

	it.each([
		['no options', undefined, { relay: false }],
		['seq: true', { seq: true }, { relay: false }],
		['seq: false', { seq: false }, { relay: false, seq: false }],
		['seq: null', { seq: null }, { relay: false, seq: false }],
		['seq: 12', { seq: 12 }, { relay: false, seq: 12 }],
		['seq: 12n', { seq: 12n }, { relay: false, seq: 12 }],
		['compress: false', { compress: false }, { relay: false, compress: false }],
		['compress: true', { compress: true }, { relay: false, compress: true }],
		['jitterMs and seq', { seq: false, jitterMs: 40 }, { relay: false, seq: false, jitterMs: 40 }]
	])('a publish with %s reaches the peer with the same seq choice', async (_label, options, delivered) => {
		const { b, a, wrapped, close } = await pair('bs1:');
		try {
			wrapped.publish('chat', 'msg', 1, options);
			await until(() => b.published.length === 1);
		} finally {
			await close();
		}
		// One worker: the caller's own options object, unchanged.
		expect(a.published[0].options).toBe(options);
		expect(b.published[0].options).toEqual(delivered);
	});

	it(traced ? 'runs the traced publish paths' : 'runs the untraced publish paths', async () => {
		const { a, wrapped, close } = await pair('bs0:');
		try {
			wrapped.publish('chat', 'one', 1, { seq: false });
		} finally {
			await close();
		}
		expect(a.traceOperations.length > 0).toBe(traced);
	});

	it('takes the seq choice per entry from a batch and a wire-batched publish', async () => {
		const { a, b, wrapped, close } = await pair('bs2:');
		const batched = [
			{ topic: 'chat', event: 'a', data: 1, options: { seq: false } },
			{ topic: 'chat', event: 'b', data: 2, options: { seq: 5, jitterMs: 30 } },
			{ topic: 'chat', event: 'c', data: 3 }
		];
		try {
			wrapped.publishBatched(batched);
			wrapped.batch([{ topic: 'chat', event: 'd', data: 4, options: { seq: 6 } }]);
			await until(() => b.published.length === 4);
		} finally {
			await close();
		}
		expect(a.publishedBatches[0].messages).toBe(batched);
		expect(Object.fromEntries(b.published.map((f) => [f.event, f.options]))).toEqual({
			a: { relay: false, seq: false },
			b: { relay: false, seq: 5, jitterMs: 30 },
			c: { relay: false },
			d: { relay: false, seq: 6 }
		});
	});

	it.each([undefined, { compress: false }, { compress: true }])('carries batch compression %j to local and peer platforms', async (options) => {
		const { a, b, wrapped, close } = await pair('batch-compression:');
		try {
			wrapped.publishBatched([{ topic: 'chat', event: 'msg', data: 1 }], options);
			await until(() => b.publishedBatches.length === 1);
			expect(a.publishedBatches[0].options).toBe(options);
			expect(b.publishedBatches[0].options).toEqual(options?.compress ? { compress: true } : undefined);
			wrapped.publishBatched([{ topic: 'chat', event: 'local', data: 2, options: { relay: false } }], options);
			expect(a.publishedBatches[1].options).toBe(options);
		} finally {
			await close();
		}
	});

	it.each([undefined, { compress: false }, { compress: true }])('keeps compression %j when a peer falls back from a batch to individual publishes', async options => {
		const { b, wrapped, close } = await pair('batch-compression-fallback:');
		b.publishBatched = undefined;
		try {
			wrapped.publishBatched([{ topic: 'chat', event: 'msg', data: 1, options: { seq: false } }], options);
			await until(() => b.published.length === 1);
			expect(b.published[0].options).toEqual({ relay: false, seq: false, compress: options?.compress === true });
		} finally {
			await close();
		}
	});

	it('preserves per-entry compression for ordinary batch publishes', async () => {
		const { b, wrapped, close } = await pair('batch-entry-compression:');
		try {
			wrapped.batch([false, true, undefined].map((compress, data) => ({ topic: 'chat', event: 'msg', data, options: { compress } })));
			await until(() => b.published.length === 3);
			expect(b.published.map(entry => entry.options)).toEqual([
				{ relay: false, compress: false }, { relay: false, compress: true }, { relay: false }
			]);
		} finally { await close(); }
	});

	it.each([false, true])('coalesces before relay filtering and shard routing (individual fallback=%s)', async fallback => {
		const { a, b, busB, wrapped, close } = await pair('batch-coalesce:');
		await follow(busB, 'other');
		if (fallback) b.publishBatched = undefined;
		try {
			wrapped.publishBatched([
				{ topic: 'chat', event: 'msg', data: 0, coalesceKey: 'position' },
				{ topic: 'chat', event: 'msg', data: 1 },
				{ topic: 'chat', event: 'msg', data: 2, coalesceKey: 'position' },
				{ topic: 'chat', event: 'msg', data: 3, coalesceKey: 'local' },
				{ topic: 'chat', event: 'msg', data: 4, coalesceKey: 'local', options: { relay: false } },
				{ topic: 'other', event: 'msg', data: 5, coalesceKey: 'cross-topic' },
				{ topic: 'chat', event: 'msg', data: 6, coalesceKey: 'cross-topic' }
			]);
			await until(() => b.published.length >= 3);
			await sleep(20);
			expect(b.published.map(entry => entry.data)).toEqual([1, 2, 6]);
			expect(a.published.map(entry => entry.data)).toEqual([1, 2, 4, 6]);
		} finally { await close(); }
	});

	it('forwards malformed entries even after finding a coalescing key', async () => {
		const { a, wrapped, close } = await pair('batch-coalesce-malformed:');
		const rejection = new TypeError('platform rejected malformed batch');
		a.publishBatched = () => { throw rejection; };
		try {
			expect(() => wrapped.publishBatched([
				{ topic: 'chat', event: 'msg', data: 0, coalesceKey: 'position' },
				null,
				{ topic: 'chat', event: 'msg', data: 1, coalesceKey: 'position' }
			])).toThrow(rejection);
		} finally { await close(); }
	});

	it('carries the seq choice of a shared-codec publishWire', async () => {
		const { a, b, wrapped, busB, close } = await pair('bs3:', wirePlatform);
		busB.wrap(b).registerWireCodec(sharedCodec);
		try {
			wrapped.publishWire('chat', 'snap', { n: 1 }, sharedCodec, { seq: false });
			await until(() => b.publishedWire.length === 1);
		} finally {
			await close();
		}
		expect(a.publishedWire[0].options).toEqual({ seq: false });
		expect(b.publishedWire[0].options).toEqual({ relay: false, seq: false });
	});

	it.each([undefined, false, true])('keeps codec compression %j when a peer uses JSON fallback', async compress => {
		const { b, wrapped, close } = await pair('wire-compression-fallback:', wirePlatform);
		try {
			wrapped.publishWire('chat', 'snap', 1, sharedCodec, { compress });
			await until(() => b.published.length === 1);
			expect(b.published[0].options).toEqual({ relay: false, compress: compress === true });
		} finally { await close(); }
	});

	it.each([0, -3, 1.5, '7', 2 ** 60, true, null])('stamps the counter for a peer envelope whose s is %j', async (s) => {
		const client = mockRedisClient('bs4:');
		const bus = createBus(client);
		const b = mockPlatform();
		b.trace.enabled = traced;
		await bus.activate(b);
		await follow(bus, 'chat');
		try {
			await inject(client, { instanceId: 'peer', topic: 'chat', event: 'msg', data: 1, s });
			await inject(client, { instanceId: 'peer', batch: [{ topic: 'chat', event: 'b', data: 2, s }] });
			await until(() => b.published.length === 2);
		} finally {
			await bus.deactivate();
		}
		for (const f of b.published) expect(f.options).toEqual({ relay: false });
	});

	it('keeps a peer envelope\'s s: false', async () => {
		const client = mockRedisClient('bs5:');
		const bus = createBus(client);
		const b = mockPlatform();
		b.trace.enabled = traced;
		await bus.activate(b);
		await follow(bus, 'chat');
		try {
			await inject(client, { instanceId: 'peer', topic: 'chat', event: 'msg', data: 1, s: false });
			await until(() => b.published.length === 1);
		} finally {
			await bus.deactivate();
		}
		expect(b.published[0].options).toEqual({ relay: false, seq: false });
	});

	it('gives topic() the adapter\'s helpers: options forwarded, counters default to 1, results returned', async () => {
		const { a, b, wrapped, close } = await pair('bs6:');
		try {
			const chat = wrapped.topic('chat');
			expect(chat.increment()).toBe(true);
			expect(chat.decrement(2, { seq: false })).toBe(true);
			expect(chat.publish('custom', { x: 1 }, { seq: 3 })).toBe(true);
			await until(() => b.published.length === 3);
		} finally {
			await close();
		}
		expect(a.published.map((f) => [f.event, f.data, f.options])).toEqual([
			['increment', 1, undefined],
			['decrement', 2, { seq: false }],
			['custom', { x: 1 }, { seq: 3 }]
		]);
		expect(b.published.map((f) => f.options)).toEqual([{ relay: false }, { relay: false, seq: false }, { relay: false, seq: 3 }]);
	});
});

describe('mockPlatform', () => {
	it('forwards each batch entry\'s options, as the adapter\'s batch does', () => {
		const p = mockPlatform();
		p.batch([{ topic: 't', event: 'e', data: 1, options: { seq: false } }]);
		expect(p.published[0].options).toEqual({ seq: false });
	});

	it('gives topic() the adapter\'s helpers', () => {
		const p = mockPlatform();
		expect(p.topic('t').increment()).toBe(true);
		expect(p.topic('t').created({ id: 1 }, { seq: false })).toBe(true);
		expect(p.published.map((f) => [f.event, f.data, f.options])).toEqual([
			['increment', 1, undefined],
			['created', { id: 1 }, { seq: false }]
		]);
	});
});
