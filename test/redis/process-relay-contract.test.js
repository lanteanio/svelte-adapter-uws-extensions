import { afterEach, describe, expect, it, vi } from 'vitest';
import { createPubSubBus } from '../../src/redis/pubsub.js';
import { createShardedBus } from '../../src/redis/sharded-pubsub.js';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { mockPlatform } from '../helpers/mock-platform.js';
import { MULTI_WORKER_RUNTIME } from '../../src/shared/sequence-authority.js';
import { assertClusterSequenceAuthority } from '../../node_modules/svelte-adapter-uws/src/runtime/handler/cluster-sequence-policy.js';

vi.mock('node:worker_threads', async importOriginal => ({ ...await importOriginal(), workerData: { totalWorkers: 2 } }));

const buses = [];
afterEach(async () => {
	for (const bus of buses.splice(0)) await bus.deactivate();
});

for (const [name, factory] of [['standard', createPubSubBus], ['sharded', createShardedBus]]) {
	for (const tracing of [false, true]) describe(`${name} process-aware relay, tracing ${tracing}`, () => {
		async function setup({ processAware = true } = {}) {
			const client = mockRedisClient();
			const source = mockPlatform();
			const sibling = mockPlatform();
			const remote = mockPlatform();
			if (processAware) {
				source.processId = sibling.processId = 'process-one';
				remote.processId = 'process-two';
			} else {
				// An adapter that predates processId does not define the member.
				for (const platform of [source, sibling, remote]) delete platform.processId;
			}
			source.trace.enabled = tracing;
			const native = source.publish;
			source.publish = (topic, event, data, options) => {
				assertClusterSequenceAuthority(options, { totalWorkers: 2 });
				const result = native(topic, event, data, options);
				if (options?.relay !== false) sibling.publish(topic, event, data, { relay: false, seq: false });
				return result;
			};
			source.publishWire = (topic, event, data, codec, options) => source.publish(topic, event, data, options);
			const sourceBatch = source.publishBatched;
			source.publishBatched = (messages, options) => {
				for (const entry of messages) assertClusterSequenceAuthority(entry.options, { totalWorkers: 2 });
				sourceBatch(messages, options);
				const relayed = messages.filter(entry => entry.options?.relay !== false);
				if (relayed.length) sibling.publishBatched(relayed.map(entry => ({ ...entry, options: { relay: false, seq: false } })), options);
			};
			const nodes = [source, sibling, remote].map(platform => {
				const bus = factory(client);
				buses.push(bus);
				return { bus, platform };
			});
			for (const { bus, platform } of nodes) {
				await bus.activate(platform);
				if (bus.follow) await bus.follow('room');
			}
			return { source, sibling, remote, wrapped: nodes[0].bus.wrap(source), client };
		}

		it('uses Redis for sibling delivery when ordinary native admission is unobservable', async () => {
			expect(MULTI_WORKER_RUNTIME).toBe(true);
			const { source, sibling, remote, wrapped } = await setup();
			expect(wrapped.processId).toBe('process-one');
			wrapped.publish('room', 'event', 1, { seq: false });
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(source.published[0].options).toEqual({ seq: false, relay: false });
			expect(sibling.published).toHaveLength(1);
			expect(remote.published).toHaveLength(1);
		});

		it('keeps every mixed-batch entry on Redis without inferring native admission', async () => {
			const { source, sibling, remote, wrapped } = await setup();
			wrapped.publishBatched([
				{ topic: 'room', event: 'volatile', data: 1, options: { seq: false } },
				{ topic: 'room', event: 'durable', data: 2, options: { seq: 7 } }
			]);
			await new Promise(resolve => setTimeout(resolve, 10));
			const entries = source.publishedBatches[0].messages;
			expect(entries[0].options).toEqual({ seq: false, relay: false });
			expect(entries[1].options).toEqual({ seq: 7, relay: false });
			expect(sibling.published.map(entry => entry.event).sort()).toEqual(['durable', 'volatile']);
			expect(remote.published.map(entry => entry.event).sort()).toEqual(['durable', 'volatile']);
		});

		it.each(['batch', 'wire'])('uses Redis for sibling delivery on the %s lane', async lane => {
			const { source, sibling, remote, wrapped } = await setup();
			if (lane === 'batch') wrapped.batch([{ topic: 'room', event: 'event', data: 1, options: { seq: false } }]);
			else wrapped.publishWire('room', 'event', 1, { shared: true, capability: 'sample:1' }, { seq: false });
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(source.published[0].options).toEqual({ seq: false, relay: false });
			expect(sibling.published).toHaveLength(1);
			expect(remote.published).toHaveLength(1);
		});

		it.each(['publish', 'batch', 'wire', 'publishBatched'])('still reaches siblings when the local %s path refuses admission', async lane => {
			const { source, sibling, remote, wrapped } = await setup();
			source.publish = () => false;
			source.publishBatched = () => {};
			const entries = [
				{ topic: 'room', event: 'volatile', data: 1, options: { seq: false } },
				{ topic: 'room', event: 'durable', data: 2, options: { seq: 7 } }
			];
			if (lane === 'publish') wrapped.publish('room', 'volatile', 1, { seq: false });
			else if (lane === 'wire') wrapped.publishWire('room', 'volatile', 1, { shared: true, capability: 'sample:1' }, { seq: false });
			else wrapped[lane](entries);
			await new Promise(resolve => setTimeout(resolve, 10));
			const expected = lane === 'publish' || lane === 'wire' ? ['volatile'] : ['durable', 'volatile'];
			expect(sibling.published.map(entry => entry.event).sort()).toEqual(expected);
			expect(remote.published.map(entry => entry.event).sort()).toEqual(expected);
		});

		it.each(['publish', 'batch', 'wire'])('does not infer native relay from an optimistic deferred %s result', async lane => {
			const { source, sibling, remote, wrapped } = await setup();
			// A delivery policy may defer this operation and return true before
			// egress admission eventually refuses it. Redis remains its carrier.
			source.publish = () => true;
			if (lane === 'publish') wrapped.publish('room', 'event', 1, { seq: false });
			else if (lane === 'wire') wrapped.publishWire('room', 'event', 1, { shared: true, capability: 'sample:1' }, { seq: false });
			else wrapped.batch([{ topic: 'room', event: 'event', data: 1, options: { seq: false } }]);
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(sibling.published).toHaveLength(1);
			expect(remote.published).toHaveLength(1);
		});

		it('preserves sibling order between single publishes and coalesced values', async () => {
			const { source, sibling, wrapped } = await setup();
			const received = [];
			const publish = sibling.publish;
			sibling.publish = (...args) => { received.push(args[1]); return publish(...args); };
			const socket = {};
			sibling.forEachSubscriber = (_, visit) => visit(socket);
			const sendCoalesced = sibling.sendCoalesced;
			sibling.sendCoalesced = (ws, payload) => { received.push(payload.event); return sendCoalesced(ws, payload); };
			// A second transport can make progress before Redis's timer flushes.
			source.relayCoalesced = (topic, event, data, key) => {
				sibling.sendCoalesced(socket, { key, topic, event, data });
				return true;
			};
			wrapped.publish('room', 'first', 1, { seq: false });
			wrapped.relayCoalesced('room', 'second', 2, 'cursor');
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(received).toEqual(['first', 'second']);
		});

		it('retains the Redis sibling route on adapters without a process id', async () => {
			const { source, sibling, remote, wrapped } = await setup({ processAware: false });
			expect(wrapped.processId).toBeUndefined();
			wrapped.publish('room', 'event', 1, { seq: false });
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(source.published[0].options).toEqual({ seq: false, relay: false });
			expect(sibling.published).toHaveLength(1);
			expect(remote.published).toHaveLength(1);
		});

		it('keeps coalesced values on Redis without serializing application values twice', async () => {
			const { source, sibling, remote, wrapped } = await setup();
			const socket = {};
			sibling.forEachSubscriber = remote.forEachSubscriber = (_, visit) => visit(socket);
			let nativeCalls = 0;
			source.relayCoalesced = (topic, event, data, key) => {
				nativeCalls++;
				sibling.sendCoalesced(socket, { key: topic + '\0' + key, topic, event, data });
				return true;
			};
			let calls = 0;
			expect(wrapped.relayCoalesced('room', 'move', { toJSON() { calls++; return { x: 2 }; } }, 'cursor')).toBe(true);
			await new Promise(resolve => setTimeout(resolve, 10));
			expect(calls).toBe(1);
			expect(nativeCalls).toBe(0);
			expect(sibling.sentCoalesced).toHaveLength(1);
			expect(remote.sentCoalesced).toHaveLength(1);
		});
	});
}
