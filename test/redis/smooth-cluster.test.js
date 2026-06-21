// Unit tests for the smooth cluster coordinator (redis/smooth-cluster.js): the
// relay transport (command-forward, correlated sync request/reply, broadcast
// fan-out, targeted ack, leave - all with echo suppression) and the per-topic
// ownership lease (acquire / renew / release / read, one owner at a time). Two
// coordinators share ONE mock Redis client so a publish on one reaches the
// other's subscriber - the same single-process two-instance shape the
// cursor/pubsub/crdt unit tests use. True multi-instance behavior and lease TTL
// handoff are covered on real Redis in test/integration/redis/smooth-cluster.test.js.

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createSmoothCluster } from '../../src/redis/smooth-cluster.js';

/** Let the deferred subscribe + publish + synchronous delivery settle. */
const settle = async () => {
	for (let i = 0; i < 5; i++) await Promise.resolve();
	await new Promise((r) => setTimeout(r, 0));
	for (let i = 0; i < 5; i++) await Promise.resolve();
};

/** A recording handler set for one coordinator. */
function recorder() {
	const commands = [];
	const syncs = [];
	const syncReplies = [];
	const broadcasts = [];
	const acks = [];
	const leaves = [];
	const shoots = [];
	return {
		commands, syncs, syncReplies, broadcasts, acks, leaves, shoots,
		handlers: {
			onCommand: (wireTopic, identity, originInstance, batch) => commands.push({ wireTopic, identity, originInstance, batch }),
			onSync: (wireTopic, identity, originInstance, corr) => syncs.push({ wireTopic, identity, originInstance, corr }),
			onSyncReply: (wireTopic, corr, payload) => syncReplies.push({ wireTopic, corr, payload }),
			onBroadcast: (wireTopic, event, data, excludeIdentity, seq, ownerInstance) => broadcasts.push({ wireTopic, event, data, excludeIdentity, seq, ownerInstance }),
			onAck: (wireTopic, identity, payload) => acks.push({ wireTopic, identity, payload }),
			onLeave: (wireTopic, identity, originInstance) => leaves.push({ wireTopic, identity, originInstance }),
			onShoot: (wireTopic, identity, originInstance, payload) => shoots.push({ wireTopic, identity, originInstance, payload })
		}
	};
}

describe('createSmoothCluster relay', () => {
	let client;
	let a;
	let b;
	let ra;
	let rb;

	beforeEach(() => {
		client = mockRedisClient('smoothtest:');
		a = createSmoothCluster(client);
		b = createSmoothCluster(client);
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

	it('forwards a command batch to the OTHER instance and echo-suppresses its own', async () => {
		a.relayCommand('__smooth:room:1', 'player-1', a.instanceId, [{ id: 1 }, { id: 2 }]);
		await settle();
		expect(rb.commands).toEqual([
			{ wireTopic: '__smooth:room:1', identity: 'player-1', originInstance: a.instanceId, batch: [{ id: 1 }, { id: 2 }] }
		]);
		expect(ra.commands).toEqual([]); // sender does not receive its own relay
	});

	it('forwards a shot to the OTHER instance (durations payload) and echo-suppresses its own', async () => {
		const payload = { cmd: { aim: 0.5 }, reach: 120, rewindAge: 47, detect: { minUplink: 10, maxUplink: 12, divergence: 2 } };
		b.relayShoot('__smooth:room:1', 'shooter-9', b.instanceId, payload);
		await settle();
		expect(ra.shoots).toEqual([
			{ wireTopic: '__smooth:room:1', identity: 'shooter-9', originInstance: b.instanceId, payload }
		]);
		expect(rb.shoots).toEqual([]); // the forwarding edge does not receive its own relay
	});

	it('ignores a forwarded shot with a non-object payload', async () => {
		a.relayShoot('__smooth:room:1', 'shooter-9', a.instanceId, 'not-an-object');
		await settle();
		expect(rb.shoots).toEqual([]);
	});

	it('routes a sync request to peers and a correlated, targeted reply back to the requester only', async () => {
		// B cold-joins: requests the catalog with a correlation id.
		b.requestSync('__smooth:room:1', 'player-2', b.instanceId, 'corr-1');
		await settle();
		expect(ra.syncs).toEqual([
			{ wireTopic: '__smooth:room:1', identity: 'player-2', originInstance: b.instanceId, corr: 'corr-1' }
		]);
		expect(rb.syncs).toEqual([]); // requester does not answer itself

		// A (the owner) replies, targeted at B and correlated by corr-1.
		a.sendSyncReply('__smooth:room:1', 'corr-1', b.instanceId, { ack: 7, states: [{ key: 'k', data: { x: 1 } }] });
		await settle();
		expect(rb.syncReplies).toEqual([
			{ wireTopic: '__smooth:room:1', corr: 'corr-1', payload: { ack: 7, states: [{ key: 'k', data: { x: 1 } }] } }
		]);
		expect(ra.syncReplies).toEqual([]); // not addressed to A
	});

	it('does not deliver a sync reply addressed to a different instance', async () => {
		a.sendSyncReply('__smooth:room:1', 'corr-x', 'some-other-instance', { ack: 1, states: [] });
		await settle();
		expect(rb.syncReplies).toEqual([]);
		expect(ra.syncReplies).toEqual([]);
	});

	it('fans out a broadcast to the other instance with its event, data, exclude, and seq', async () => {
		a.relayBroadcast('__smooth:room:1', 'update', { key: 'k', data: { hp: 50 } }, 'player-1', 42);
		await settle();
		expect(rb.broadcasts).toEqual([
			{ wireTopic: '__smooth:room:1', event: 'update', data: { key: 'k', data: { hp: 50 } }, excludeIdentity: 'player-1', seq: 42, ownerInstance: a.instanceId }
		]);
		expect(ra.broadcasts).toEqual([]); // owner emits locally; echo-suppressed on the relay
	});

	it('carries an undefined excludeIdentity through a broadcast (global event)', async () => {
		a.relayBroadcast('__smooth:room:1', 'event', { kind: 'kill' }, undefined, 1);
		await settle();
		expect(rb.broadcasts).toHaveLength(1);
		expect(rb.broadcasts[0].excludeIdentity).toBeUndefined();
		expect(rb.broadcasts[0].seq).toBe(1);
	});

	it('routes an ack to the target instance only', async () => {
		// A (owner) acks player-2, whose client is on B.
		a.relayAck('__smooth:room:1', 'player-2', b.instanceId, { ack: 9 });
		await settle();
		expect(rb.acks).toEqual([
			{ wireTopic: '__smooth:room:1', identity: 'player-2', payload: { ack: 9 } }
		]);
		expect(ra.acks).toEqual([]); // not addressed to A
	});

	it('does not deliver an ack addressed to a different instance', async () => {
		a.relayAck('__smooth:room:1', 'player-2', 'some-other-instance', { ack: 9 });
		await settle();
		expect(rb.acks).toEqual([]);
		expect(ra.acks).toEqual([]);
	});

	it('relays a leave to the OTHER instance (the owner)', async () => {
		b.relayLeave('__smooth:room:1', 'player-2', b.instanceId);
		await settle();
		expect(ra.leaves).toEqual([
			{ wireTopic: '__smooth:room:1', identity: 'player-2', originInstance: b.instanceId }
		]);
		expect(rb.leaves).toEqual([]); // sender echo-suppressed
	});

	it('accepts a `__smooth:`-prefixed wire topic (not rejected by a system-topic gate)', async () => {
		// Regression guard for the validator divergence: the entity wire topic is
		// `__`-prefixed and travels as an envelope field, so it must pass the
		// shape check rather than be dropped by the `__`-prefix system-topic gate.
		a.relayCommand('__smooth:lobby', 'p', a.instanceId, [{ id: 1 }]);
		await settle();
		expect(rb.commands).toHaveLength(1);
		expect(rb.commands[0].wireTopic).toBe('__smooth:lobby');
	});

	it('stops delivering after destroy', async () => {
		b.destroy();
		a.relayCommand('__smooth:room:1', 'p', a.instanceId, [{ id: 1 }]);
		await settle();
		expect(rb.commands).toEqual([]);
	});
});

describe('createSmoothCluster per-topic ownership lease', () => {
	let client;
	let a;
	let b;

	beforeEach(() => {
		client = mockRedisClient('smoothlease:');
		a = createSmoothCluster(client);
		b = createSmoothCluster(client);
	});
	afterEach(() => {
		a.destroy();
		b.destroy();
	});

	it('grants ownership to one instance and denies the other (one owner per topic)', async () => {
		expect(await a.acquireOwner('__smooth:room:1')).toBe(true); // first acquires
		expect(await b.acquireOwner('__smooth:room:1')).toBe(false); // someone else holds it
		expect(await a.acquireOwner('__smooth:room:1')).toBe(true); // holder re-acquires (renew-if-ours)
	});

	it('renewOwner refreshes the holder and refuses a non-owner', async () => {
		expect(await a.acquireOwner('__smooth:room:1')).toBe(true);
		expect(await a.renewOwner('__smooth:room:1')).toBe(true); // owner renews
		expect(await b.renewOwner('__smooth:room:1')).toBe(false); // not ours - never acquires via renew
		// renewOwner must NOT acquire a free lease.
		expect(await b.renewOwner('__smooth:room:2')).toBe(false);
		expect(await b.currentOwner('__smooth:room:2')).toBeNull();
	});

	it('releaseOwner frees the lease for a sibling; a non-owner cannot release it', async () => {
		expect(await a.acquireOwner('__smooth:room:1')).toBe(true);
		expect(await b.releaseOwner('__smooth:room:1')).toBe(false); // compare-and-delete guard: not ours
		expect(await b.acquireOwner('__smooth:room:1')).toBe(false); // still held by a
		expect(await a.releaseOwner('__smooth:room:1')).toBe(true); // owner releases
		expect(await a.currentOwner('__smooth:room:1')).toBeNull();
		expect(await b.acquireOwner('__smooth:room:1')).toBe(true); // now free, b takes over
	});

	it('currentOwner reports the holder instance id', async () => {
		expect(await a.currentOwner('__smooth:room:1')).toBeNull();
		await a.acquireOwner('__smooth:room:1');
		expect(await a.currentOwner('__smooth:room:1')).toBe(a.instanceId);
		expect(await b.currentOwner('__smooth:room:1')).toBe(a.instanceId);
	});

	it('leases are per-topic (one instance can own different topics independently)', async () => {
		expect(await a.acquireOwner('__smooth:room:1')).toBe(true);
		expect(await b.acquireOwner('__smooth:room:2')).toBe(true); // a different topic, free
		expect(await b.acquireOwner('__smooth:room:1')).toBe(false); // still held by a
	});

	it('a destroyed coordinator declines to own or report', async () => {
		a.destroy();
		expect(await a.acquireOwner('__smooth:room:1')).toBe(false);
		expect(await a.renewOwner('__smooth:room:1')).toBe(false);
		expect(await a.releaseOwner('__smooth:room:1')).toBe(false);
		expect(await a.currentOwner('__smooth:room:1')).toBeNull();
	});
});

describe('createSmoothCluster snapshot (warm handoff)', () => {
	let client;
	let a;
	let b;

	beforeEach(() => {
		client = mockRedisClient('smoothsnap:');
		a = createSmoothCluster(client);
		b = createSmoothCluster(client);
	});
	afterEach(() => {
		a.destroy();
		b.destroy();
	});

	it('round-trips a catalog payload from the writing owner to a reader', async () => {
		const catalog = [
			{ key: 'player-1', state: { x: 3, y: 7, hp: 90 } },
			{ key: 'player-2', state: { x: -1, y: 0, hp: 100 } }
		];
		await a.writeSnapshot('__smooth:room:1', catalog);
		// A different coordinator (the would-be new owner) reads it back verbatim.
		expect(await b.readSnapshot('__smooth:room:1')).toEqual(catalog);
	});

	it('readSnapshot returns null for a topic that was never written', async () => {
		expect(await a.readSnapshot('__smooth:room:absent')).toBeNull();
	});

	it('snapshots are per-topic and a later write replaces the earlier one', async () => {
		await a.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 1 } }]);
		await a.writeSnapshot('__smooth:room:2', [{ key: 'p', state: { v: 2 } }]);
		await a.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 3 } }]); // overwrite room:1
		expect(await b.readSnapshot('__smooth:room:1')).toEqual([{ key: 'p', state: { v: 3 } }]);
		expect(await b.readSnapshot('__smooth:room:2')).toEqual([{ key: 'p', state: { v: 2 } }]);
	});

	it('a destroyed coordinator neither writes nor reads', async () => {
		await a.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 1 } }]);
		a.destroy();
		// A destroyed write is a no-op (does not throw, does not change the value).
		await a.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 999 } }]);
		expect(await a.readSnapshot('__smooth:room:1')).toBeNull(); // destroyed read -> null
		expect(await b.readSnapshot('__smooth:room:1')).toEqual([{ key: 'p', state: { v: 1 } }]); // unchanged
	});

	it('an open circuit breaker skips the write and read (the warm handoff degrades, not the authority)', async () => {
		const open = { guard() { throw new Error('breaker open'); } };
		const guarded = createSmoothCluster(client, { breaker: open });
		// A control coordinator seeds a value the guarded one must neither clobber nor read.
		await a.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 1 } }]);
		await guarded.writeSnapshot('__smooth:room:1', [{ key: 'p', state: { v: 2 } }]); // skipped
		expect(await guarded.readSnapshot('__smooth:room:1')).toBeNull(); // read skipped -> null
		expect(await a.readSnapshot('__smooth:room:1')).toEqual([{ key: 'p', state: { v: 1 } }]); // write was skipped
		guarded.destroy();
	});

	// NOTE: SET PX TTL is a no-op in the mock (the flag is accepted and ignored),
	// so snapshot expiry is asserted only against real Redis in
	// test/integration/redis/smooth-cluster.test.js.
});
