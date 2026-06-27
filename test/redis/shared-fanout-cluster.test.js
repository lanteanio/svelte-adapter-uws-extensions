import { describe, it, expect } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../../src/testing/mock-platform.js';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';

// Cross-instance fan-out for a stateless `shared: true` wire codec. The adapter
// already fans a shared codec out via native cohort topics on a single instance
// (and across worker threads via its IPC relay). The bus mirror makes the SAME
// thing happen across a Redis-bus cluster: a shared publishWire relays the
// logical event + capability as JSON (never the binary frame nor the per-process
// wire-id), and every other instance re-derives its codec by capability and runs
// its own native cohort split locally. Stateful codecs (cursor/presence) carry
// `shared` falsy and keep their own per-plugin relay, so they never relay here.

// A wire-capable mock platform: the default mock omits publishWire/sendWire/
// registerWireCodec (the version-gated binary surface), so capture them here -
// the same pattern the cursor/presence binary-wire suites use.
function wirePlatform() {
	const p = mockPlatform();
	p.publishedWire = [];
	p.sentWire = [];
	p.registeredCodecs = [];
	p.publishWire = (topic, event, data, wire, options) => {
		p.publishedWire.push({ topic, event, data, wire, options });
		return true;
	};
	p.sendWire = (ws, topic, event, data, wire, options) => {
		p.sentWire.push({ ws, topic, event, data, wire, options });
		return 1;
	};
	p.registerWireCodec = (wire) => {
		p.registeredCodecs.push(wire);
	};
	return p;
}

function sharedCodec() {
	return {
		capability: 'test.shared:1',
		schemaVersion: 1,
		shared: true,
		encode: (event, data) => new TextEncoder().encode(JSON.stringify({ event, data }))
	};
}

// A stateful (non-shared) codec, like cursor/presence: the wrap must NOT relay it.
function statefulCodec() {
	return {
		capability: 'test.stateful:1',
		schemaVersion: 1,
		encode: () => new Uint8Array([1, 2, 3])
	};
}

const tick = () => new Promise((r) => setTimeout(r, 5));

describe('pubsub bus: shared binary fan-out relay', () => {
	describe('send side', () => {
		it('a shared publishWire fans out locally AND relays the logical event over the bus', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push({ ch, parsed: JSON.parse(msg) }); return orig.call(client.redis, ch, msg); };

			const codec = sharedCodec();
			wrapped.publishWire('world', 'snapshot', { tick: 1 }, codec);
			await tick();

			// Local fan-out still happened (the wrap forwards to the platform).
			expect(platform.publishedWire).toHaveLength(1);
			expect(platform.publishedWire[0].wire).toBe(codec);

			// The logical event was relayed, carrying the capability token only.
			expect(calls).toHaveLength(1);
			expect(calls[0].ch).toBe('uws:pubsub');
			expect(calls[0].parsed.wire).toBe(true);
			expect(calls[0].parsed.capability).toBe('test.shared:1');
			expect(calls[0].parsed.topic).toBe('world');
			expect(calls[0].parsed.event).toBe('snapshot');
			expect(calls[0].parsed.data).toEqual({ tick: 1 });
			expect(typeof calls[0].parsed.instanceId).toBe('string');
			// The codec object and the binary frame never cross the bus.
			expect(calls[0].parsed.encode).toBeUndefined();
		});

		it('a non-shared (stateful) codec fans out locally but does NOT relay', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishWire('room', 'update', { x: 1 }, statefulCodec());
			await tick();

			expect(platform.publishedWire).toHaveLength(1);
			expect(calls).toHaveLength(0); // no bus relay for a stateful codec
		});

		it('relay: false suppresses the bus relay for a shared codec', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishWire('world', 'snapshot', { tick: 2 }, sharedCodec(), { relay: false });
			await tick();

			expect(platform.publishedWire).toHaveLength(1); // local only
			expect(calls).toHaveLength(0);
		});

		it('excludeWs still relays a shared publish (other instances have no excluded socket)', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			const sender = { id: 's' };
			wrapped.publishWire('world', 'snapshot', { tick: 3 }, sharedCodec(), { excludeWs: sender });
			await tick();

			// Local fan-out honors excludeWs; the relay carries no excludeWs.
			expect(platform.publishedWire[0].options).toEqual({ excludeWs: sender });
			expect(calls).toHaveLength(1);
			expect(calls[0].wire).toBe(true);
			expect(calls[0].excludeWs).toBeUndefined();
		});

		it('carries the compress intent on the relay envelope when set', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishWire('world', 'snapshot', { tick: 4 }, sharedCodec(), { compress: true });
			await tick();

			expect(calls).toHaveLength(1);
			expect(calls[0].compress).toBe(true);
		});

		it('registerWireCodec forwards to the underlying platform', () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const wrapped = createPubSubBus(client).wrap(platform);

			const codec = sharedCodec();
			wrapped.registerWireCodec(codec);

			expect(platform.registeredCodecs).toContain(codec);
		});
	});

	describe('receive side', () => {
		it('re-derives the registered codec and re-runs publishWire with relay: false', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const bus = createPubSubBus(client);
			const codec = sharedCodec();
			bus.wrap(platform).registerWireCodec(codec); // populate the bus codec map
			await bus.activate(platform);

			const env = JSON.stringify({
				instanceId: 'other-instance', wire: true, capability: 'test.shared:1',
				topic: 'world', event: 'snapshot', data: { tick: 9 }
			});
			await client.redis.publish('uws:pubsub', env);

			// Re-encoded locally with the registered codec, relay: false stops a loop.
			expect(platform.publishedWire).toHaveLength(1);
			expect(platform.publishedWire[0].topic).toBe('world');
			expect(platform.publishedWire[0].event).toBe('snapshot');
			expect(platform.publishedWire[0].data).toEqual({ tick: 9 });
			expect(platform.publishedWire[0].wire).toBe(codec);
			expect(platform.publishedWire[0].options).toEqual({ relay: false });
			// NOT the JSON publish path.
			expect(platform.published.filter((p) => p.event === 'snapshot')).toHaveLength(0);
		});

		it('re-applies the compress intent on the re-encode', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const bus = createPubSubBus(client);
			bus.wrap(platform).registerWireCodec(sharedCodec());
			await bus.activate(platform);

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance', wire: true, capability: 'test.shared:1',
				topic: 'world', event: 'snapshot', data: { tick: 1 }, compress: true
			}));

			expect(platform.publishedWire).toHaveLength(1);
			expect(platform.publishedWire[0].options).toEqual({ relay: false, compress: true });
		});

		it('falls back to a JSON publish when the codec is not registered on this instance', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const bus = createPubSubBus(client);
			// Deliberately do NOT register the codec (a pure-subscriber cold instance).
			await bus.activate(platform);

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance', wire: true, capability: 'test.shared:1',
				topic: 'world', event: 'snapshot', data: { tick: 9 }
			}));

			expect(platform.publishedWire).toHaveLength(0);
			expect(platform.published).toHaveLength(1);
			expect(platform.published[0]).toMatchObject({
				topic: 'world', event: 'snapshot', data: { tick: 9 }, options: { relay: false }
			});
		});

		it('drops a wire envelope with a non-string capability', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const bus = createPubSubBus(client);
			bus.wrap(platform).registerWireCodec(sharedCodec());
			await bus.activate(platform);

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: 'other-instance', wire: true, capability: 123,
				topic: 'world', event: 'snapshot', data: { tick: 9 }
			}));

			expect(platform.publishedWire).toHaveLength(0);
			expect(platform.published).toHaveLength(0);
		});

		it('echo-suppresses a shared relay carrying this instance own id', async () => {
			const client = mockRedisClient();
			const platform = wirePlatform();
			const bus = createPubSubBus(client);
			const wrapped = bus.wrap(platform);
			wrapped.registerWireCodec(sharedCodec());
			await bus.activate(platform);

			const calls = [];
			const orig = client.redis.publish;
			client.redis.publish = async (ch, msg) => { calls.push(JSON.parse(msg)); return orig.call(client.redis, ch, msg); };

			wrapped.publishWire('world', 'snapshot', { tick: 1 }, sharedCodec());
			await tick();
			const ownId = calls[0].instanceId;
			platform.publishedWire.length = 0; // ignore the local send fan-out

			await client.redis.publish('uws:pubsub', JSON.stringify({
				instanceId: ownId, wire: true, capability: 'test.shared:1',
				topic: 'world', event: 'snapshot', data: { tick: 2 }
			}));

			expect(platform.publishedWire).toHaveLength(0); // own relay is suppressed
		});
	});

	describe('two instances over one bus (end to end)', () => {
		it('B re-encodes binary locally from a shared publish on A', async () => {
			const client = mockRedisClient();
			const busA = createPubSubBus(client);
			const busB = createPubSubBus(client);
			const platformA = wirePlatform();
			const platformB = wirePlatform();
			const codec = sharedCodec();

			await busA.activate(platformA);
			await busB.activate(platformB);
			busB.wrap(platformB).registerWireCodec(codec); // B can re-derive the codec

			busA.wrap(platformA).publishWire('world', 'snapshot', { tick: 7 }, codec);
			await tick();

			// A fanned out locally.
			expect(platformA.publishedWire).toHaveLength(1);
			// B received the relay and re-ran publishWire with its own registered codec.
			expect(platformB.publishedWire).toHaveLength(1);
			expect(platformB.publishedWire[0].topic).toBe('world');
			expect(platformB.publishedWire[0].wire).toBe(codec);
			expect(platformB.publishedWire[0].options).toEqual({ relay: false });
			expect(platformB.published.filter((p) => p.event === 'snapshot')).toHaveLength(0);
		});
	});
});

describe('sharded bus: shared binary fan-out relay', () => {
	const shardKey = (t) => t.split(':')[0];

	it('a shared publishWire relays on the topic shard channel; B re-encodes locally', async () => {
		const client = mockRedisClient();
		const busA = createShardedBus(client, { shardKey });
		const busB = createShardedBus(client, { shardKey });
		const platformA = wirePlatform();
		const platformB = wirePlatform();
		const codec = sharedCodec();

		await busA.activate(platformA);
		await busB.activate(platformB);
		await busB.follow('world:1');
		busB.wrap(platformB).registerWireCodec(codec);

		busA.wrap(platformA).publishWire('world:1', 'snapshot', { tick: 5 }, codec);
		await tick();

		expect(platformA.publishedWire).toHaveLength(1);
		expect(platformB.publishedWire).toHaveLength(1);
		expect(platformB.publishedWire[0].topic).toBe('world:1');
		expect(platformB.publishedWire[0].wire).toBe(codec);
		expect(platformB.publishedWire[0].options).toEqual({ relay: false });

		await busA.deactivate();
		await busB.deactivate();
	});

	it('a non-shared codec does NOT relay on the shard channel', async () => {
		const client = mockRedisClient();
		const busA = createShardedBus(client, { shardKey });
		const busB = createShardedBus(client, { shardKey });
		const platformA = wirePlatform();
		const platformB = wirePlatform();

		await busA.activate(platformA);
		await busB.activate(platformB);
		await busB.follow('world:1');

		busA.wrap(platformA).publishWire('world:1', 'update', { x: 1 }, statefulCodec());
		await tick();

		expect(platformA.publishedWire).toHaveLength(1);
		expect(platformB.publishedWire).toHaveLength(0); // not relayed
		expect(platformB.published).toHaveLength(0);

		await busA.deactivate();
		await busB.deactivate();
	});

	it('falls back to a JSON publish on B when the codec is not registered there', async () => {
		const client = mockRedisClient();
		const busA = createShardedBus(client, { shardKey });
		const busB = createShardedBus(client, { shardKey });
		const platformA = wirePlatform();
		const platformB = wirePlatform();

		await busA.activate(platformA);
		await busB.activate(platformB);
		await busB.follow('world:1');
		// B does NOT register the codec.

		busA.wrap(platformA).publishWire('world:1', 'snapshot', { tick: 8 }, sharedCodec());
		await tick();

		expect(platformB.publishedWire).toHaveLength(0);
		expect(platformB.published).toHaveLength(1);
		expect(platformB.published[0]).toMatchObject({
			topic: 'world:1', event: 'snapshot', data: { tick: 8 }, options: { relay: false }
		});

		await busA.deactivate();
		await busB.deactivate();
	});
});
