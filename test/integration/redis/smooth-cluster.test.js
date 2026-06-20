/**
 * Integration tests for the smooth cluster coordinator (redis/smooth-cluster.js)
 * against a real Redis server. Two coordinators on separate connections share
 * one Redis, modeling two server instances. The high-value scenarios the
 * in-memory mock cannot prove on the wire: real cross-connection command
 * forwarding, the correlated sync request/reply roundtrip across connections,
 * broadcast fan-out and targeted ack delivery, and the per-topic ownership lease
 * (SET NX PX + compare-and-pexpire renew + compare-and-delete release) enforcing
 * exactly one owner per topic, including TTL handoff after an owner goes quiet.
 *
 * Runs in both backend tiers (standalone + cluster-mirror) via the shared
 * backend helper. The mock suite at test/redis/smooth-cluster.test.js stays the
 * exhaustive behavior surface; this file focuses on what only a real server can prove.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createSmoothCluster } from '../../../src/redis/smooth-cluster.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(fn, timeoutMs = 2000) {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (await fn()) return;
		await wait(10);
	}
	throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

describe('redis smooth cluster coordinator (integration)', () => {
	let client;
	const coordinators = [];

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-smooth:' });
	});

	beforeEach(async () => {
		await resetBackendKeys(client);
	});

	afterEach(() => {
		while (coordinators.length > 0) {
			const c = coordinators.pop();
			try { c.destroy(); } catch { /* ignore */ }
		}
	});

	afterAll(async () => {
		if (client && typeof client.quit === 'function') await client.quit().catch(() => {});
	});

	function track(c) {
		coordinators.push(c);
		return c;
	}

	function recorder(c) {
		const commands = [];
		const syncs = [];
		const syncReplies = [];
		const broadcasts = [];
		const acks = [];
		const leaves = [];
		c.onMessage({
			onCommand: (wireTopic, identity, originInstance, batch) => commands.push({ wireTopic, identity, originInstance, batch }),
			onSync: (wireTopic, identity, originInstance, corr) => syncs.push({ wireTopic, identity, originInstance, corr }),
			onSyncReply: (wireTopic, corr, payload) => syncReplies.push({ wireTopic, corr, payload }),
			onBroadcast: (wireTopic, event, data, excludeIdentity, seq, ownerInstance) => broadcasts.push({ wireTopic, event, data, excludeIdentity, seq, ownerInstance }),
			onAck: (wireTopic, identity, payload) => acks.push({ wireTopic, identity, payload }),
			onLeave: (wireTopic, identity, originInstance) => leaves.push({ wireTopic, identity, originInstance })
		});
		return { commands, syncs, syncReplies, broadcasts, acks, leaves };
	}

	/** Force both subscribers to attach before the first asserted publish. */
	async function warmup(a, b, ra, rb) {
		a.requestSync('__smooth:warmup', 'w', a.instanceId, 'w');
		b.requestSync('__smooth:warmup', 'w', b.instanceId, 'w');
		await wait(100);
		ra.syncs.length = 0; rb.syncs.length = 0;
	}

	it('forwards a command batch across connections, echo-suppressing the sender', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		const ra = recorder(a);
		const rb = recorder(b);
		await warmup(a, b, ra, rb);

		b.relayCommand('__smooth:room:1', 'player-2', b.instanceId, [{ id: 1 }, { id: 2 }]);
		await waitFor(() => ra.commands.length > 0);
		expect(ra.commands[0]).toEqual({
			wireTopic: '__smooth:room:1', identity: 'player-2', originInstance: b.instanceId, batch: [{ id: 1 }, { id: 2 }]
		});
		expect(rb.commands).toEqual([]); // sender echo-suppressed
	});

	it('round-trips a correlated sync request and a targeted reply', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		const ra = recorder(a);
		const rb = recorder(b);
		await warmup(a, b, ra, rb);

		// B cold-joins room:9.
		b.requestSync('__smooth:room:9', 'player-2', b.instanceId, 'corr-9');
		await waitFor(() => ra.syncs.length > 0);
		expect(ra.syncs[0]).toMatchObject({ wireTopic: '__smooth:room:9', identity: 'player-2', originInstance: b.instanceId, corr: 'corr-9' });

		// A answers, targeted at B and correlated by corr-9.
		a.sendSyncReply('__smooth:room:9', 'corr-9', b.instanceId, { ack: 3, states: [{ key: 'k', data: { x: 1 } }] });
		await waitFor(() => rb.syncReplies.length > 0);
		expect(rb.syncReplies[0]).toEqual({ wireTopic: '__smooth:room:9', corr: 'corr-9', payload: { ack: 3, states: [{ key: 'k', data: { x: 1 } }] } });
		expect(ra.syncReplies).toEqual([]); // not addressed to A
	});

	it('fans out a broadcast and targets an ack across connections', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		const ra = recorder(a);
		const rb = recorder(b);
		await warmup(a, b, ra, rb);

		a.relayBroadcast('__smooth:room:1', 'update', { key: 'k', data: { hp: 50 } }, 'player-1', 7);
		await waitFor(() => rb.broadcasts.length > 0);
		expect(rb.broadcasts[0]).toEqual({ wireTopic: '__smooth:room:1', event: 'update', data: { key: 'k', data: { hp: 50 } }, excludeIdentity: 'player-1', seq: 7, ownerInstance: a.instanceId });

		a.relayAck('__smooth:room:1', 'player-2', b.instanceId, { ack: 7 });
		await waitFor(() => rb.acks.length > 0);
		expect(rb.acks[0]).toEqual({ wireTopic: '__smooth:room:1', identity: 'player-2', payload: { ack: 7 } });
		expect(ra.acks).toEqual([]); // not addressed to A
	});

	it('grants the per-topic ownership lease to exactly one instance', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		expect(await a.acquireOwner('__smooth:room:lease')).toBe(true);
		expect(await b.acquireOwner('__smooth:room:lease')).toBe(false); // a owns it
		expect(await a.renewOwner('__smooth:room:lease')).toBe(true);     // holder renews
		expect(await b.renewOwner('__smooth:room:lease')).toBe(false);    // contender cannot
		expect(await b.currentOwner('__smooth:room:lease')).toBe(a.instanceId);
	});

	it('lets a sibling take over after release without waiting out the TTL', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		expect(await a.acquireOwner('__smooth:room:rel')).toBe(true);
		expect(await a.releaseOwner('__smooth:room:rel')).toBe(true);
		expect(await b.acquireOwner('__smooth:room:rel')).toBe(true); // free immediately after release
	});

	it('lets a different instance acquire after the owner goes quiet and the lease TTL expires', async () => {
		const a = track(createSmoothCluster(client, { leaseMs: 300 }));
		const b = track(createSmoothCluster(client, { leaseMs: 300 }));
		expect(await a.acquireOwner('__smooth:room:ttl')).toBe(true);
		expect(await b.acquireOwner('__smooth:room:ttl')).toBe(false);
		// Let A's lease lapse without renewal; B can then take over with a fresh authority.
		await wait(450);
		expect(await b.acquireOwner('__smooth:room:ttl')).toBe(true);
	});

	/**
	 * A minimal model of a realtime smooth instance around a coordinator: it owns
	 * a set of topics, applies forwarded commands to a tiny authority with the
	 * same drop-stale guard the realtime layer uses, relays the resulting update +
	 * event + ack, and on the receive side dedups broadcasts by per-owner seq.
	 * Enough to prove the end-to-end correctness invariants (apply-once,
	 * drop-stale, fire-once, clean handoff) over a REAL Redis - without importing
	 * the realtime singleton (which cannot run two instances in one process).
	 */
	function instance(opts) {
		const sm = track(createSmoothCluster(client, opts));
		const owned = new Set();
		const authority = new Map(); // topic -> Map(identity -> { state, lastAckedId })
		const seqByTopic = new Map();
		const seen = new Map();      // topic -> { owner, seq } receive watermark
		const firedEvents = [];      // { topic, key } events actually delivered here
		const appliedUpdates = [];   // { topic, key, state } updates applied here
		const auth = (t) => { let m = authority.get(t); if (!m) { m = new Map(); authority.set(t, m); } return m; };
		const nextSeq = (t) => { const s = seqByTopic.get(t) || 0; seqByTopic.set(t, s + 1); return s; };
		sm.onMessage({
			onCommand: (t, identity, origin, batch) => {
				if (!owned.has(t)) return;             // only the owner applies
				const m = auth(t);
				let e = m.get(identity);
				if (!e) { e = { state: 0, lastAckedId: 0 }; m.set(identity, e); }
				const fresh = batch.filter((c) => c.id > e.lastAckedId);
				if (fresh.length === 0) return;        // drop-stale: nothing fresh, no relay
				for (const c of fresh) { e.state += c.delta; e.lastAckedId = c.id; }
				sm.relayBroadcast(t, 'update', { key: identity, data: e.state }, undefined, nextSeq(t));
				sm.relayBroadcast(t, 'event', { type: 'tick', key: identity + ':' + e.lastAckedId }, undefined, nextSeq(t));
				sm.relayAck(t, identity, origin, { id: e.lastAckedId, state: e.state });
			},
			onSync: (t, identity, origin, corr) => {
				if (!owned.has(t)) return;
				const m = auth(t);
				if (!m.has(identity)) m.set(identity, { state: 0, lastAckedId: 0 });
				sm.sendSyncReply(t, corr, origin, { ack: m.get(identity).lastAckedId, states: [...m].map(([k, v]) => ({ key: k, state: v.state })) });
			},
			onSyncReply: () => {},
			onBroadcast: (t, event, data, _exclude, seq, owner) => {
				let w = seen.get(t);
				if (!w || w.owner !== owner) { w = { owner, seq: -1 }; seen.set(t, w); } // reset on handoff
				if (seq <= w.seq) return;              // per-owner seq dedup
				w.seq = seq;
				if (event === 'event') firedEvents.push({ topic: t, key: data.key });
				else if (event === 'update') appliedUpdates.push({ topic: t, key: data.key, state: data.data });
			},
			onAck: () => {},
			onLeave: () => {}
		});
		return {
			sm, firedEvents, appliedUpdates,
			async acquire(t) { const ok = await sm.acquireOwner(t); if (ok) owned.add(t); return ok; },
			async release(t) { owned.delete(t); return sm.releaseOwner(t); },
			forward(t, identity, batch) { sm.relayCommand(t, identity, sm.instanceId, batch); },
			authState: (t, identity) => { const m = authority.get(t); const e = m && m.get(identity); return e ? e.state : undefined; }
		};
	}

	it('end-to-end: forwarded commands apply once, broadcasts dedup, and ownership hands off cleanly', async () => {
		const T = '__smooth:e2e:1';
		const a = instance();
		const b = instance();
		await wait(100); // let both relay subscribers attach

		// A owns T; B is a non-owner holding a local client 'p'.
		expect(await a.acquire(T)).toBe(true);
		expect(await b.acquire(T)).toBe(false);

		// B forwards two commands for 'p' -> A (owner) applies each once and relays
		// the authoritative update + event back; B receives them deduped.
		b.forward(T, 'p', [{ id: 1, delta: 5 }]);
		b.forward(T, 'p', [{ id: 2, delta: 3 }]);
		await waitFor(() => b.appliedUpdates.length >= 2);
		expect(a.authState(T, 'p')).toBe(8);                                  // applied once each: 5 + 3
		expect(b.appliedUpdates[b.appliedUpdates.length - 1].state).toBe(8);  // converged on B

		// Redelivery of an already-acked command is dropped (drop-stale): no extra
		// update reaches B and the authoritative state is unchanged.
		const updatesBefore = b.appliedUpdates.length;
		b.forward(T, 'p', [{ id: 1, delta: 999 }]);
		await wait(200);
		expect(b.appliedUpdates.length).toBe(updatesBefore);
		expect(a.authState(T, 'p')).toBe(8);

		// Every one-shot event fired exactly once (no double-fire): keys are unique.
		const keys = b.firedEvents.map((e) => e.key);
		expect(new Set(keys).size).toBe(keys.length);
		expect(keys).toEqual(['p:1', 'p:2']);

		// Handoff: A releases ownership; B acquires a fresh authority and becomes the
		// owner. A (now a non-owner) forwards a command for a NEW client 'q', which
		// B applies once in its fresh authority - the old 'p' entity is not carried
		// over (reset-on-handoff) and nothing double-applies across the transition.
		await a.release(T);
		expect(await b.acquire(T)).toBe(true);
		a.forward(T, 'q', [{ id: 1, delta: 4 }]);
		await waitFor(() => a.appliedUpdates.some((u) => u.key === 'q'));
		expect(b.authState(T, 'q')).toBe(4);
		expect(a.appliedUpdates.filter((u) => u.key === 'q').map((u) => u.state)).toEqual([4]);
	});

	it('persists a topic snapshot one connection can read after another wrote it', async () => {
		const a = track(createSmoothCluster(client));
		const b = track(createSmoothCluster(client));
		const T = '__smooth:snap:1';
		const catalog = [
			{ key: 'p1', state: { x: 3, y: 7 } },
			{ key: 'p2', state: { x: -1, y: 0 } }
		];
		await a.writeSnapshot(T, catalog);
		expect(await b.readSnapshot(T)).toEqual(catalog); // a different connection reads it
		expect(await b.readSnapshot('__smooth:snap:absent')).toBeNull();
	});

	it('expires a snapshot after snapshotTtlMs (real PX, not testable on the mock)', async () => {
		const a = track(createSmoothCluster(client, { snapshotTtlMs: 300 }));
		const T = '__smooth:snap:ttl';
		await a.writeSnapshot(T, [{ key: 'p', state: { v: 1 } }]);
		expect(await a.readSnapshot(T)).toEqual([{ key: 'p', state: { v: 1 } }]); // present now
		await wait(450);
		expect(await a.readSnapshot(T)).toBeNull(); // self-expired
	});

	it('warm handoff: a fresh owner recovers the previous owner snapshot across a TTL lapse', async () => {
		const a = track(createSmoothCluster(client, { leaseMs: 300 }));
		const b = track(createSmoothCluster(client, { leaseMs: 300 }));
		const T = '__smooth:snap:handoff';
		const catalog = [{ key: 'p1', state: { x: 9 } }, { key: 'p2', state: { x: 4 } }];

		// A owns the topic and debounce-persists its catalog.
		expect(await a.acquireOwner(T)).toBe(true);
		await a.writeSnapshot(T, catalog);

		// A goes quiet; its lease lapses and B takes over with a fresh authority.
		await wait(450);
		expect(await b.acquireOwner(T)).toBe(true);

		// The fresh owner recovers the previous owner's state instead of starting empty.
		expect(await b.readSnapshot(T)).toEqual(catalog);
	});
});
