// Unit tests for the CRDT cluster coordinator (redis/crdt.js): the relay
// transport (publish/subscribe with echo suppression), the cold-join sync
// request/reply roundtrip (targeted at the requester), and the per-topic
// persist lease (one writer at a time). Two coordinators share ONE mock Redis
// client so a publish on one reaches the other's subscriber - the same
// single-process two-instance shape the cursor/pubsub unit tests use. The
// true multi-instance convergence is covered on real Redis in
// test/integration/redis/crdt.test.js.

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
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

	it('moves no document bytes when a fresh lease finds the replicas converged', async () => {
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const all = [instance({}, quiet), instance({}, quiet), instance({}, quiet)];
		for (const inst of all) await inst.join(TOPIC);
		await settle();
		all[0].edit(TOPIC, 'big', 'x'.repeat(50000));
		await settle();
		const sent = [];
		const publish = client.redis.publish;
		client.redis.publish = function (channel, message) {
			sent.push(JSON.parse(message));
			return publish.call(this, channel, message);
		};
		let applied = 0;
		for (const inst of all) {
			const apply = inst.authority.applyUpdate.bind(inst.authority);
			inst.authority.applyUpdate = (topic, bytes) => { applied++; return apply(topic, bytes); };
		}
		expect(await all[1].crdt.acquirePersist(TOPIC)).toBe(true); // fresh lease
		expect(sent.map((env) => env.k).sort()).toEqual(['srep', 'srep', 'sreq']);
		expect(sent.filter((env) => Array.isArray(env.b) && env.b.length > 0)).toEqual([]);
		expect(applied).toBe(0);
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

	/** Hold every update `inst` publishes until the returned release() runs. */
	function holdUpdates(inst) {
		const publish = client.redis.publish;
		const held = [];
		client.redis.publish = function (channel, message) {
			if (message.includes(inst.crdt.instanceId) && message.includes('"k":"u"')) {
				return new Promise((resolve, reject) => held.push(() => publish.call(this, channel, message).then(resolve, reject)));
			}
			return publish.call(this, channel, message);
		};
		return {
			held,
			release() {
				client.redis.publish = publish;
				for (const send of held.splice(0)) send();
			}
		};
	}

	it('refuses persistence while Redis has not confirmed the relay of a local edit', async () => {
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({}, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
		const hold = holdUpdates(a);
		a.edit(TOPIC, 'u1', 1); // the publish hangs, like a command queued while offline
		await settle();
		expect(hold.held).toHaveLength(1);
		const started = Date.now();
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(Date.now() - started).toBeLessThan(1000); // bounded by the sync window
		hold.release();
		await until(() => 'u1' in b.items(TOPIC));
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
	});

	it('keeps a topic marked until Redis confirmed the resync that carries its lost edit', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({ breaker }, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		breaker.failure(new Error('redis down'));
		a.edit(TOPIC, 'u1', 1); // refused: the topic is marked
		await settle();
		const hold = holdUpdates(a);
		breaker.reset(); // the resync pushes the full state, which hangs
		await settle();
		expect(hold.held).toHaveLength(1);
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(b.items(TOPIC)).toEqual({});
		hold.release();
		await until(() => 'u1' in b.items(TOPIC));
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
	});

	it('repairs a replica whose full state exceeds maxEnvelopeBytes', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({ breaker, maxEnvelopeBytes: 20000 }, quiet);
		const b = instance({ maxEnvelopeBytes: 20000 }, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		a.edit(TOPIC, 'big', 'y'.repeat(4000)); // each edit fits one envelope
		b.edit(TOPIC, 'big2', 'z'.repeat(4000));
		await settle();
		breaker.failure(new Error('redis down'));
		a.edit(TOPIC, 'u1', 1);
		await settle();
		breaker.reset(); // the resync pushes a full state larger than the cap
		await until(() => 'u1' in b.items(TOPIC));
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
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

	it('hands the breaker probe back when a resync finds a topic no longer held', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 50 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({ breaker }, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		breaker.failure(new Error('redis down'));
		await a.join('doc:gone'); // its cold-join request is refused
		await settle();
		a.authority.release('doc:gone');
		await wait(300); // the probing breaker resyncs doc:gone, which a no longer holds
		expect(breaker.state).toBe('probing');
		a.edit(TOPIC, 'after', 1);
		await until(() => 'after' in b.items(TOPIC));
		expect(breaker.state).toBe('healthy');
	});

	it('reports one failure to the breaker for a resync round whose publishes both fail', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 2, resetTimeout: 60000 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({ breaker }, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		breaker.failure(new Error('redis down'));
		breaker.failure(new Error('redis down'));
		a.edit(TOPIC, 'u1', 1); // refused: the round pushes the state and asks the peers
		await settle();
		const publish = client.redis.publish;
		let failed = 0;
		client.redis.publish = function (channel, message) {
			if (message.includes(a.crdt.instanceId)) {
				failed++;
				return Promise.reject(new Error('connection lost'));
			}
			return publish.call(this, channel, message);
		};
		breaker.reset();
		await settle();
		client.redis.publish = publish;
		expect(failed).toBe(2);
		expect(breaker.failures).toBe(1);
		expect(breaker.state).toBe('healthy');
	});

	it('refuses the first write of a lease whose sync round the breaker refused, until a round reaches the peers', async () => {
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const b = instance({}, quiet);
		await b.join(TOPIC);
		await settle();
		b.edit(TOPIC, 'onlyb', 1);
		await settle();
		const a = instance({ breaker }, quiet);
		breaker.failure(new Error('redis down'));
		await a.join(TOPIC); // the cold-join request is refused
		await settle();
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(a.items(TOPIC)).toEqual({});
		breaker.reset();
		await until(() => 'onlyb' in a.items(TOPIC));
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
	});

	it('refuses the first write of a fresh lease while a local edit made during its sync round is unconfirmed', async () => {
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({}, quiet);
		await a.join(TOPIC);
		await settle();
		// A peer that never answers keeps the round open for the whole window.
		const silent = client.duplicate();
		await silent.subscribe(client.key('crdt:events'));
		const pending = a.crdt.acquirePersist(TOPIC);
		await wait(50);
		const hold = holdUpdates(a);
		a.edit(TOPIC, 'u1', 1);
		await expect(pending).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(hold.held).toHaveLength(1);
		hold.release();
	});

	it('refuses the first write of a fresh lease whose sync request Redis has not confirmed', async () => {
		const quiet = { debounceWait: 60000, debounceMaxWait: 60000 };
		const a = instance({}, quiet);
		const b = instance({}, quiet);
		await a.join(TOPIC);
		await b.join(TOPIC);
		await settle();
		const publish = client.redis.publish;
		const held = [];
		client.redis.publish = function (channel, message) {
			if (message.includes(a.crdt.instanceId) && message.includes('"k":"sreq"')) {
				return new Promise((resolve, reject) => held.push(() => publish.call(this, channel, message).then(resolve, reject)));
			}
			return publish.call(this, channel, message);
		};
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(held).toHaveLength(1);
		// Renewing the lease it now holds does not skip the sync either.
		await expect(a.crdt.acquirePersist(TOPIC)).rejects.toMatchObject({ code: 'CRDT_RELAY_PENDING' });
		expect(held).toHaveLength(2);
		client.redis.publish = publish;
		for (const send of held.splice(0)) send();
		await settle();
		expect(await a.crdt.acquirePersist(TOPIC)).toBe(true);
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
		b.onMessage({ onSyncRequest: () => {} });
		const requests = [];
		const publish = client.redis.publish;
		client.redis.publish = function (channel, message) {
			const env = JSON.parse(message);
			if (env.k === 'sreq') requests.push({ topic: env.t, from: env.i });
			return publish.call(this, channel, message);
		};
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

describe('createCrdtCluster recovery cost', () => {
	const wait = (ms) => new Promise((r) => setTimeout(r, ms));

	it('stops a recovery at the first round the probing breaker refuses, before reading another replica', async () => {
		const client = mockRedisClient('crdtburst:');
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 30 });
		const a = createCrdtCluster(client, { breaker });
		let reads = 0;
		a.onMessage({
			onSyncRequest: (declKey, topic, sv, from) => {
				if (from.startsWith('local:')) reads++;
				a.sendSyncReply(declKey, topic, [1], from);
			}
		});
		try {
			await settle();
			breaker.failure(new Error('redis down'));
			for (let i = 0; i < 20; i++) a.relayUpdate('dk1', 'doc:' + i, [i + 1]); // refused: 20 topics marked
			client.redis.publish = () => new Promise(() => {}); // the probe never answers
			await wait(100);
			expect(breaker.state).toBe('probing');
			// One round took the probe; the rest wait instead of each encoding
			// its replica only to be refused.
			expect(reads).toBe(1);
		} finally {
			a.destroy();
		}
	});

	it('sweeps the remembered topics a bounded slice per event-loop turn', async () => {
		const client = mockRedisClient('crdtsweep:');
		const a = createCrdtCluster(client);
		let reads = 0;
		a.onMessage({
			onSyncRequest: (declKey, topic, sv, from) => {
				if (!from.startsWith('local:')) return;
				reads++;
				a.sendSyncReply(declKey, topic, [1], from);
			}
		});
		try {
			for (let i = 0; i <= 1024; i++) a.requestSync('dk1', 'doc:' + i, []); // one past the sweep threshold
			await new Promise((r) => setImmediate(r));
			expect(reads).toBeGreaterThan(0);
			expect(reads).toBeLessThanOrEqual(64);
			for (let turn = 0; turn < 40 && reads < 1025; turn++) await new Promise((r) => setImmediate(r));
			expect(reads).toBe(1025);
		} finally {
			a.destroy();
		}
	});
});

describe('createCrdtCluster envelopes over maxEnvelopeBytes', () => {
	const bytesOf = (n) => Array.from({ length: n }, (_, i) => (i * 7) % 256);

	it('carries an update larger than maxEnvelopeBytes to the peers in parts that each fit', async () => {
		const client = mockRedisClient('crdtparts:');
		const a = createCrdtCluster(client, { maxEnvelopeBytes: 2000 });
		const b = createCrdtCluster(client, { maxEnvelopeBytes: 2000 });
		const rb = recorder();
		b.onMessage(rb.handlers);
		a.onMessage({});
		await settle();
		const sizes = [];
		const publish = client.redis.publish;
		client.redis.publish = function (channel, message) {
			sizes.push(Buffer.byteLength(message));
			return publish.call(this, channel, message);
		};
		try {
			const bytes = bytesOf(3000);
			a.relayUpdate('dk1', 'board:1', bytes);
			await settle();
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes }]);
			expect(sizes.length).toBeGreaterThan(1);
			for (const size of sizes) expect(size).toBeLessThanOrEqual(2000);
		} finally {
			a.destroy();
			b.destroy();
		}
	});

	it('rebuilds an envelope whose parts arrive out of order', async () => {
		const client = mockRedisClient('crdtpartorder:');
		const a = createCrdtCluster(client, { maxEnvelopeBytes: 1000 });
		const b = createCrdtCluster(client, { maxEnvelopeBytes: 1000 });
		const rb = recorder();
		b.onMessage(rb.handlers);
		a.onMessage({});
		await settle();
		const publish = client.redis.publish;
		const captured = [];
		client.redis.publish = async (channel, message) => { captured.push([channel, message]); return 1; };
		try {
			const bytes = bytesOf(2000);
			a.relayUpdate('dk1', 'board:1', bytes);
			await settle();
			client.redis.publish = publish;
			expect(captured.length).toBeGreaterThan(2);
			for (const [channel, message] of captured.reverse()) await client.redis.publish(channel, message);
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes }]);
		} finally {
			a.destroy();
			b.destroy();
		}
	});

	it('drops a partly received envelope once its parts stop arriving', async () => {
		const client = mockRedisClient('crdtpartexpiry:');
		const b = createCrdtCluster(client);
		const rb = recorder();
		b.onMessage(rb.handlers);
		await settle();
		const channel = client.key('crdt:events');
		const data = Buffer.from([1, 2, 3, 4, 5, 6]).toString('base64');
		const part = (n, extra, p = 'p1') => JSON.stringify({ i: 'peer', k: 'part', d: 'dk1', t: 'board:1', p, n, of: 2, x: n === 0 ? data.slice(0, 4) : data.slice(4), ...extra });
		vi.useFakeTimers();
		try {
			await client.redis.publish(channel, part(0, { e: { k: 'u' } }));
			vi.advanceTimersByTime(30001);
			await client.redis.publish(channel, part(1));
			expect(rb.updates).toEqual([]); // the first part expired
			// The late part does not start an assembly of its own.
			await client.redis.publish(channel, part(0, { e: { k: 'u' } }));
			expect(rb.updates).toEqual([]);
			await client.redis.publish(channel, part(0, { e: { k: 'u' } }, 'p2'));
			await client.redis.publish(channel, part(1, {}, 'p2'));
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes: [1, 2, 3, 4, 5, 6] }]);
		} finally {
			vi.useRealTimers();
			b.destroy();
		}
	});
});

describe('createCrdtCluster topics the relay cannot carry', () => {
	it('resyncs the other topics when one topic cannot be relayed at any size, and reports that one', async () => {
		const client = mockRedisClient('crdtoversize:');
		const breaker = createCircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });
		const errors = [];
		const a = createCrdtCluster(client, { breaker, maxEnvelopeBytes: 400, onError: (err) => errors.push(err) });
		const b = createCrdtCluster(client, { maxEnvelopeBytes: 400 });
		const rb = recorder();
		b.onMessage(rb.handlers);
		// Too long a name leaves no room for any part of its envelope.
		const huge = 'doc:' + 'h'.repeat(300);
		const state = { [huge]: Array.from({ length: 50 }, (_, i) => i), 'doc:small': [9, 9] };
		a.onMessage({
			onSyncRequest: (declKey, topic, sv, from) => {
				if (from.startsWith('local:') && state[topic]) a.sendSyncReply(declKey, topic, state[topic], from);
			}
		});
		try {
			await settle();
			breaker.failure(new Error('redis down'));
			a.relayUpdate('dk1', huge, state[huge]); // cannot be carried: marked first
			a.relayUpdate('dk1', 'doc:small', [9]); // refused: marked
			await settle();
			breaker.reset();
			await settle();
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'doc:small', bytes: [9, 9] }]);
			expect(errors.length).toBeGreaterThan(0);
			expect(errors.every((err) => err.code === 'RELAY_ENVELOPE_TOO_LARGE')).toBe(true);
		} finally {
			a.destroy();
			b.destroy();
		}
	});
});

describe('createCrdtCluster reassembly capacity', () => {
	const wait = (ms) => new Promise((r) => setTimeout(r, ms));

	/** A receiver holding `topics`, recording updates and the sync requests it sends. */
	async function receiver(prefix, topics) {
		const client = mockRedisClient(prefix);
		const b = createCrdtCluster(client);
		const rb = recorder();
		b.onMessage({
			...rb.handlers,
			onSyncRequest: (declKey, topic, sv, from) => {
				if (from.startsWith('local:')) b.sendSyncReply(declKey, topic, [1], from);
			}
		});
		for (const topic of topics) b.requestSync('dk1', topic, []);
		await settle();
		const requests = [];
		const publish = client.redis.publish;
		client.redis.publish = function (channel, message) {
			const env = JSON.parse(message);
			if (env.i === b.instanceId && env.k === 'sreq') requests.push(env);
			return publish.call(this, channel, message);
		};
		return { client, b, rb, requests, channel: client.key('crdt:events') };
	}

	const part = (from, id, topic, n, of, x, extra = {}) =>
		JSON.stringify({ i: from, k: 'part', d: 'dk1', t: topic, p: id, n, of, x, ...(n === 0 ? { e: { k: 'u' } } : {}), ...extra });
	const small = Buffer.from([4, 5, 6]).toString('base64');

	it('makes room for another peer when one peer fills the buffer, and resyncs the topic it drops', async () => {
		const { client, b, rb, requests, channel } = await receiver('crdtpartfair:', ['doc:big', 'doc:small']);
		try {
			// One peer's unfinished envelope fills the whole buffer.
			const total = 64 * 1024 * 1024;
			const slice = 1000000;
			let held = 0;
			for (let n = 0; held < total; n++) {
				const size = Math.min(slice, total - held);
				await client.redis.publish(channel, part('peerA', 'big', 'doc:big', n, 100, 'A'.repeat(size)));
				held += size;
			}
			await client.redis.publish(channel, part('peerB', 's1', 'doc:small', 0, 2, small.slice(0, 2)));
			await client.redis.publish(channel, part('peerB', 's1', 'doc:small', 1, 2, small.slice(2)));
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'doc:small', bytes: [4, 5, 6] }]);
			// peerA's envelope is gone, so b asks the peers for the topic again.
			for (let i = 0; i < 60 && !requests.some((r) => r.t === 'doc:big' && r.a === 1); i++) await wait(50);
			expect(requests.some((r) => r.t === 'doc:big' && r.a === 1)).toBe(true);
		} finally {
			b.destroy();
		}
	});

	it('makes room for another peer when one peer holds every assembly slot', async () => {
		const { client, b, rb, channel } = await receiver('crdtpartslots:', ['doc:small']);
		try {
			for (let k = 0; k < 256; k++) await client.redis.publish(channel, part('peerA', 'a' + k, 'doc:other', 0, 2, 'AAAA'));
			await client.redis.publish(channel, part('peerB', 's1', 'doc:small', 0, 2, small.slice(0, 2)));
			await client.redis.publish(channel, part('peerB', 's1', 'doc:small', 1, 2, small.slice(2)));
			expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'doc:small', bytes: [4, 5, 6] }]);
		} finally {
			b.destroy();
		}
	});
});
