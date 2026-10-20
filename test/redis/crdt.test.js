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
import { createCircuitBreaker } from '../../src/shared/breaker.js';
import { SUBSCRIBER_RESTORED } from '../../src/shared/redis-subscriber.js';
import { createDocInstance, createBackend, itemsOf } from '../helpers/crdt-replica-harness.js';

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

describe('createCrdtCluster repair after a lost relay', () => {
	const TOPIC = 'doc:1';
	/** @type {any[]} */
	let instances;
	let client;
	let backend;

	const wait = (ms) => new Promise((r) => setTimeout(r, ms));
	async function until(fn, timeoutMs = 3000) {
		const start = Date.now();
		while (!(await fn())) {
			if (Date.now() - start > timeoutMs) throw new Error('condition not met within ' + timeoutMs + 'ms');
			await wait(5);
		}
	}
	function instance(options, schedule) {
		const inst = createDocInstance(client, backend, { persistLeaseMs: 600, ...options }, schedule);
		instances.push(inst);
		return inst;
	}
	/** Cut an instance's relay subscriber off the channel while `fn` runs. */
	async function deaf(inst, fn) {
		const channels = inst.subscribers[0]._subscribedChannels;
		const held = [...channels];
		channels.clear();
		try { await fn(); } finally { for (const ch of held) channels.add(ch); }
	}

	beforeEach(() => {
		client = mockRedisClient('crdtrepair:');
		backend = createBackend();
		instances = [];
	});
	afterEach(() => {
		for (const inst of instances) inst.destroy();
	});

	it('delivers an edit the open breaker refused once the breaker closes, so the persister stores both edits', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		// a stores nothing during the test, so b is the sole persister.
		const a = instance({ breaker }, { debounceWait: 60000, debounceMaxWait: 60000 });
		const b = instance();
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true);

		breaker.failure(new Error('redis down'));
		a.edit(TOPIC, 'u1', 1); // relay refused by the open breaker
		b.edit(TOPIC, 'u2', 2);
		await settle();
		expect(b.items(TOPIC)).toEqual({ u2: 2 });

		breaker.reset();
		await until(() => 'u1' in b.items(TOPIC));
		await b.authority.persistNow(TOPIC);
		expect(itemsOf(backend.blob)).toEqual({ u1: 1, u2: 2 });
	});

	it('retries a relay Redis failed without any breaker until the peers have it', async () => {
		const a = instance();
		const b = instance();
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		const publish = client.redis.publish;
		let failures = 0;
		client.redis.publish = function (channel, message) {
			if (failures === 0 && message.includes(a.crdt.instanceId) && message.includes('"k":"u"')) {
				failures++;
				return Promise.reject(new Error('connection lost'));
			}
			return publish.call(this, channel, message);
		};
		a.edit(TOPIC, 'u1', 1);
		await settle();
		expect(failures).toBe(1);
		expect(b.items(TOPIC)).toEqual({});
		await until(() => 'u1' in b.items(TOPIC));
		await b.authority.persistNow(TOPIC);
		expect(itemsOf(backend.blob)).toEqual({ u1: 1 });
	});

	it('syncs a freshly acquired persist lease with the peers before the write is allowed', async () => {
		// Neither instance stores during the test, so only the lease decides.
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({}, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		await deaf(b, async () => {
			a.edit(TOPIC, 'u1', 1);
			await settle();
		});
		expect(b.items(TOPIC)).toEqual({}); // b never received u1
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true); // fresh lease
		expect(b.items(TOPIC)).toEqual({ u1: 1 }); // merged before the write
		// A renewal with nothing to repair does not wait for a round.
		const started = Date.now();
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true);
		expect(Date.now() - started).toBeLessThan(100);
	});

	it('resyncs every held topic when the subscriber restores its channel', async () => {
		const a = instance();
		const b = instance();
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		await deaf(b, async () => {
			a.edit(TOPIC, 'u1', 1);
			await settle();
		});
		expect(b.items(TOPIC)).toEqual({});
		b.subscribers[0]._listeners.get(SUBSCRIBER_RESTORED)();
		await until(() => 'u1' in b.items(TOPIC));
	});

	it('keeps a replica holding edits the peers lack loaded until the relay delivered them', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const a = instance({ breaker });
		const b = instance();
		// b is the persister before a holds anything it could store.
		await b.join(TOPIC);
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true);
		await a.join(TOPIC);
		await settle();

		breaker.failure(new Error('redis down'));
		a.edit(TOPIC, 'u1', 1);
		a.authority.release(TOPIC); // last local subscriber leaves: final store
		await wait(100);
		expect(a.authority.has(TOPIC)).toBe(true); // not unloaded with u1 unrelayed
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toThrow();

		breaker.reset();
		await until(() => 'u1' in b.items(TOPIC));
		await until(() => !a.authority.has(TOPIC));
	});

	it('ends a lease sync round as soon as every peer answered, including peers that hold nothing', async () => {
		const a = instance({ persistLeaseMs: 30000 });
		const b = instance({ persistLeaseMs: 30000 });
		await a.join(TOPIC);
		// b holds nothing; it only answers with an empty reply.
		await settle();
		const started = Date.now();
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
		expect(Date.now() - started).toBeLessThan(2000); // the window is 10s
	});

	it('waits at most a third of the lease for a peer that never answers', async () => {
		const a = instance({ persistLeaseMs: 600 });
		await a.join(TOPIC);
		await settle();
		// A subscriber on the channel that never answers, like a peer that died.
		const silent = client.duplicate();
		await silent.subscribe(client.key('crdt:events'));
		const started = Date.now();
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
		const took = Date.now() - started;
		expect(took).toBeGreaterThanOrEqual(150);
		expect(took).toBeLessThan(1000);
	});

	it('keeps persisting when only a sync reply was refused, and answers the requester again once the relay recovers', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const b = instance({ breaker }, quiet);
		await b.join(TOPIC);
		await settle();
		b.edit(TOPIC, 'k', 1);
		await settle();
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true);

		breaker.failure(new Error('redis down'));
		const a = instance({}, quiet);
		await a.join(TOPIC); // b's cold-join reply is refused
		await settle();
		expect(a.items(TOPIC)).toEqual({});
		// No local edit of b's is missing anywhere, so its writes go on.
		expect(await b.crdt.acquirePersist(TOPIC)).toBe(true);

		breaker.reset();
		await until(() => 'k' in a.items(TOPIC));
	});
});

describe('createCrdtCluster relay retry', () => {
	it('re-sends the full state of a topic whose relay failed, on a backoff with no other signal', async () => {
		const client = mockRedisClient('crdtretry:');
		const a = createCrdtCluster(client);
		const b = createCrdtCluster(client);
		const received = [];
		// a's replica holds [7, 7]; b records what reaches it.
		a.onMessage({ onSyncRequest: (declKey, topic, sv, from) => a.sendSyncReply(declKey, topic, [7, 7], from) });
		b.onMessage({ onUpdate: (declKey, topic, bytes) => received.push(bytes) });
		await settle();
		const publish = client.redis.publish;
		let failed = false;
		client.redis.publish = function (channel, message) {
			if (!failed) {
				failed = true;
				return Promise.reject(new Error('connection lost'));
			}
			return publish.call(this, channel, message);
		};
		try {
			a.relayUpdate('dk1', 'board:1', [7]);
			await settle();
			expect(received).toEqual([]);
			await new Promise((r) => setTimeout(r, 1300));
			expect(received).toEqual([[7, 7]]);
		} finally {
			a.destroy();
			b.destroy();
		}
	});
});

describe('createCrdtCluster breaker reporting', () => {
	it('reports a relay admitted as the breaker probe, so a working relay closes the breaker', async () => {
		const client = mockRedisClient('crdtprobe:');
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 10 });
		const a = createCrdtCluster(client, { breaker });
		try {
			breaker.failure(new Error('redis down'));
			await new Promise((r) => setTimeout(r, 40));
			expect(breaker.state).toBe('probing');
			a.relayUpdate('dk1', 'board:1', [1]);
			await settle();
			expect(breaker.state).toBe('healthy');
		} finally {
			a.destroy();
		}
	});
});

describe('createCrdtCluster subscriber recovery', () => {
	it('re-subscribes after a refused subscribe without waiting for traffic, then resyncs the held topics', async () => {
		const client = mockRedisClient('crdtresub:');
		const b = createCrdtCluster(client);
		const requests = [];
		b.onMessage({ onSyncRequest: (declKey, topic, sv, from) => requests.push({ topic, from }) });
		const recording = Object.create(client);
		let opened = 0;
		recording.duplicate = (...args) => {
			const conn = client.duplicate(...args);
			if (opened++ === 0) conn.subscribe = () => Promise.reject(new Error('NOPERM'));
			return conn;
		};
		const a = createCrdtCluster(recording, { onError: () => {} });
		a.onMessage({ onSyncRequest: (declKey, topic, sv, from) => a.sendSyncReply(declKey, topic, [3], from) });
		try {
			a.requestSync('dk1', 'board:1', []); // cold join while the subscribe is refused
			await settle();
			expect(requests.filter((r) => r.from === a.instanceId)).toHaveLength(1);
			await new Promise((r) => setTimeout(r, 1300));
			expect(opened).toBe(2);
			// The fresh subscriber missed the cold-join reply, so a resync asks again.
			expect(requests.filter((r) => r.from === a.instanceId)).toHaveLength(2);
		} finally {
			a.destroy();
			b.destroy();
		}
	});
});
