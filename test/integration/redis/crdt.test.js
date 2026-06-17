/**
 * Integration tests for the CRDT cluster coordinator (redis/crdt.js) against a
 * real Redis server. Two coordinators on separate connections share one Redis,
 * modeling two server instances. The high-value scenarios the in-memory mock
 * cannot prove on the wire: real cross-connection relay delivery, the
 * cold-join sync request/reply roundtrip across connections, and the per-topic
 * persist lease (SET NX PX + compare-and-pexpire renew) enforcing exactly one
 * writer per topic.
 *
 * Runs in both backend tiers (standalone + cluster-mirror) via the shared
 * backend helper. The mock suite at test/redis/crdt.test.js stays the
 * exhaustive behavior surface; this file focuses on what only a real server
 * can prove.
 */
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { createCrdtCluster } from '../../../src/redis/crdt.js';

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

describe('redis crdt cluster coordinator (integration)', () => {
	let client;
	const coordinators = [];

	beforeAll(() => {
		client = createBackendClient({ keyPrefix: 'inttest-crdt:' });
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
		const updates = [];
		const syncRequests = [];
		const syncReplies = [];
		c.onMessage({
			onUpdate: (declKey, topic, bytes) => updates.push({ declKey, topic, bytes }),
			onSyncRequest: (declKey, topic, sv, from) => syncRequests.push({ declKey, topic, sv, from }),
			onSyncReply: (declKey, topic, bytes) => syncReplies.push({ declKey, topic, bytes })
		});
		return { updates, syncRequests, syncReplies };
	}

	it('relays an applied update across connections, echo-suppressing the sender', async () => {
		const a = track(createCrdtCluster(client));
		const b = track(createCrdtCluster(client));
		const ra = recorder(a);
		const rb = recorder(b);
		// Let both subscribers attach before publishing.
		a.requestSync('warmup', 'warmup', []);
		b.requestSync('warmup', 'warmup', []);
		await wait(100);
		ra.syncRequests.length = 0; rb.syncRequests.length = 0;

		a.relayUpdate('dk1', 'board:1', [1, 2, 3, 4]);
		await waitFor(() => rb.updates.length > 0);
		expect(rb.updates).toEqual([{ declKey: 'dk1', topic: 'board:1', bytes: [1, 2, 3, 4] }]);
		expect(ra.updates).toEqual([]); // sender echo-suppressed
	});

	it('round-trips a cold-join sync request and a targeted reply', async () => {
		const a = track(createCrdtCluster(client));
		const b = track(createCrdtCluster(client));
		const ra = recorder(a);
		const rb = recorder(b);
		a.requestSync('warmup', 'warmup', []);
		b.requestSync('warmup', 'warmup', []);
		await wait(100);
		ra.syncRequests.length = 0; rb.syncRequests.length = 0;

		// B cold-joins board:9.
		b.requestSync('dk1', 'board:9', [5]);
		await waitFor(() => ra.syncRequests.length > 0);
		expect(ra.syncRequests[0]).toMatchObject({ declKey: 'dk1', topic: 'board:9', sv: [5], from: b.instanceId });

		// A answers, targeted at B.
		a.sendSyncReply('dk1', 'board:9', [42, 7], b.instanceId);
		await waitFor(() => rb.syncReplies.length > 0);
		expect(rb.syncReplies[0]).toEqual({ declKey: 'dk1', topic: 'board:9', bytes: [42, 7] });
		expect(ra.syncReplies).toEqual([]); // not addressed to A
	});

	it('grants the per-topic persist lease to exactly one instance', async () => {
		const a = track(createCrdtCluster(client));
		const b = track(createCrdtCluster(client));
		const first = await a.acquirePersist('board:lease');
		const second = await b.acquirePersist('board:lease');
		expect(first).toBe(true);
		expect(second).toBe(false); // a holds the lease
		// The holder renews; the contender still cannot acquire.
		expect(await a.acquirePersist('board:lease')).toBe(true);
		expect(await b.acquirePersist('board:lease')).toBe(false);
	});

	it('lets a different instance acquire after the lease TTL expires', async () => {
		const a = track(createCrdtCluster(client, { persistLeaseMs: 300 }));
		const b = track(createCrdtCluster(client, { persistLeaseMs: 300 }));
		expect(await a.acquirePersist('board:ttl')).toBe(true);
		expect(await b.acquirePersist('board:ttl')).toBe(false);
		// Let A's lease lapse without renewal; B can then take over.
		await wait(450);
		expect(await b.acquirePersist('board:ttl')).toBe(true);
	});
});
