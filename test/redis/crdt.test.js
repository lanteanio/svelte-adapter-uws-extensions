// Unit tests for the CRDT cluster coordinator (redis/crdt.js): the relay
// transport (publish/subscribe with echo suppression), the cold-join sync
// request/reply roundtrip (targeted at the requester), and the per-topic
// persist lease (one writer at a time). Two coordinators share ONE mock Redis
// client so a publish on one reaches the other's subscriber - the same
// single-process two-instance shape the cursor/pubsub unit tests use. The
// true multi-instance convergence is covered on real Redis in
// test/integration/redis/crdt.test.js.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createCrdtCluster } from '../../src/redis/crdt.js';

/** Let the deferred subscribe + publish + synchronous delivery settle. */
const settle = async () => {
	for (let i = 0; i < 5; i++) await Promise.resolve();
	await new Promise((r) => setTimeout(r, 0));
	for (let i = 0; i < 5; i++) await Promise.resolve();
};

/** A recording handler set for one coordinator. */
function recorder() {
	const updates = [];
	const syncRequests = [];
	const syncReplies = [];
	return {
		updates, syncRequests, syncReplies,
		handlers: {
			onUpdate: (declKey, topic, bytes) => updates.push({ declKey, topic, bytes }),
			onSyncRequest: (declKey, topic, sv, from) => syncRequests.push({ declKey, topic, sv, from }),
			onSyncReply: (declKey, topic, bytes) => syncReplies.push({ declKey, topic, bytes })
		}
	};
}

describe('createCrdtCluster relay', () => {
	let client;
	let a;
	let b;
	let ra;
	let rb;

	beforeEach(() => {
		client = mockRedisClient('crdttest:');
		a = createCrdtCluster(client);
		b = createCrdtCluster(client);
		ra = recorder();
		rb = recorder();
		a.onMessage(ra.handlers);
		b.onMessage(rb.handlers);
	});
	afterEach(() => {
		a.destroy();
		b.destroy();
	});

	it('gives each instance a distinct relay identity', () => {
		expect(typeof a.instanceId).toBe('string');
		expect(a.instanceId).not.toBe(b.instanceId);
	});

	it('relays an applied update to the OTHER instance and echo-suppresses its own', async () => {
		a.relayUpdate('dk1', 'board:1', [1, 2, 3]);
		await settle();
		expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes: [1, 2, 3] }]);
		expect(ra.updates).toEqual([]); // sender does not receive its own relay
	});

	it('routes a cold-join sync request to peers and a targeted reply back to the requester only', async () => {
		// B cold-joins: broadcasts a sync request carrying its state vector.
		b.requestSync('dk1', 'board:1', [7]);
		await settle();
		expect(ra.syncRequests).toEqual([{ declKey: 'dk1', topic: 'board:1', sv: [7], from: b.instanceId }]);
		expect(rb.syncRequests).toEqual([]); // requester does not answer itself

		// A replies with the missing structs, targeted at B.
		a.sendSyncReply('dk1', 'board:1', [9, 9], b.instanceId);
		await settle();
		expect(rb.syncReplies).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes: [9, 9] }]);
		expect(ra.syncReplies).toEqual([]); // not addressed to A
	});

	it('does not deliver a sync reply addressed to a different instance', async () => {
		a.sendSyncReply('dk1', 'board:1', [1], 'some-other-instance');
		await settle();
		expect(rb.syncReplies).toEqual([]);
		expect(ra.syncReplies).toEqual([]);
	});

	it('stops delivering after destroy', async () => {
		b.destroy();
		a.relayUpdate('dk1', 'board:1', [1]);
		await settle();
		expect(rb.updates).toEqual([]);
	});
});

describe('createCrdtCluster per-topic persist lease', () => {
	let client;
	let a;
	let b;

	beforeEach(() => {
		client = mockRedisClient('leasetest:');
		a = createCrdtCluster(client);
		b = createCrdtCluster(client);
	});
	afterEach(() => {
		a.destroy();
		b.destroy();
	});

	it('grants the lease to one instance and denies the other (one writer per topic)', async () => {
		expect(await a.acquirePersist('board:1')).toBe(true); // first acquires
		expect(await b.acquirePersist('board:1')).toBe(false); // someone else holds it
		expect(await a.acquirePersist('board:1')).toBe(true); // holder renews
	});

	it('leases are per-topic (one instance can hold different topics independently)', async () => {
		expect(await a.acquirePersist('board:1')).toBe(true);
		expect(await b.acquirePersist('board:2')).toBe(true); // a different topic, free
		expect(await b.acquirePersist('board:1')).toBe(false); // still held by a
	});

	it('a destroyed coordinator declines to write', async () => {
		a.destroy();
		expect(await a.acquirePersist('board:1')).toBe(false);
	});
});
