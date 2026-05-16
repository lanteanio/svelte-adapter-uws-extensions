/**
 * Pins the regression that `scanAndUnlink` must work against a real Redis
 * Cluster: SCAN must run per-master (the cursor is meaningful only on the
 * node that produced it) and UNLINK must run one key at a time (a multi-
 * key UNLINK that spans hash slots fails with CROSSSLOT).
 *
 * The pre-fix code path used `redis.scan(cursor, ...)` (sample-one-node
 * fallback) + batched `redis.unlink(...keys)` (CROSSSLOT-prone). The fix
 * iterates `redis.nodes('master')` and runs per-key UNLINK on Cluster.
 *
 * This test is the seed of the cluster integration tier; future Cluster-
 * correctness work lands additional test files next to this one.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { clusterClient } from '../helpers/cluster-client.js';
import { scanAndUnlink } from '../../../shared/redis-scan.js';

describe('scanAndUnlink (cluster)', () => {
	/** @type {import('ioredis').Cluster} */
	let cluster;

	beforeAll(async () => {
		cluster = clusterClient();
		// Force initial slot map fetch so the first test's writes go to
		// the right node without waiting for a refresh.
		await cluster.cluster('info');
	});

	afterAll(async () => {
		if (cluster) {
			// Wipe any leftover keys under our test prefix on every master.
			await scanAndUnlink(cluster, 'cluster-scan-test:*').catch(() => {});
			await cluster.quit().catch(() => cluster.disconnect());
		}
	});

	beforeEach(async () => {
		await scanAndUnlink(cluster, 'cluster-scan-test:*');
	});

	it('removes 100 distinct keys with default (no-hashtag) naming - keys spread across all masters', async () => {
		// 100 keys with no hashtag means slot = CRC16(key) % 16384, which
		// naturally distributes across all three masters' slot ranges.
		// We can't use a single pipeline here (ioredis rejects cross-slot
		// pipelines with "All keys in the pipeline should belong to the
		// same slots allocation group"); a Promise.all of individual SETs
		// routes each call to the slot's owning master independently.
		const sets = [];
		for (let i = 0; i < 100; i++) {
			sets.push(cluster.set('cluster-scan-test:k' + i, 'v' + i));
		}
		await Promise.all(sets);

		// Sanity: every master holds at least one key (statistically
		// near-certain at N=100 across 3 slot ranges).
		const masters = cluster.nodes('master');
		expect(masters).toHaveLength(3);
		let totalBefore = 0;
		for (const m of masters) {
			const dbsize = await m.dbsize();
			totalBefore += dbsize;
		}
		expect(totalBefore).toBe(100);

		// The actual fix-under-test: scanAndUnlink must remove every key
		// without throwing CROSSSLOT.
		await expect(scanAndUnlink(cluster, 'cluster-scan-test:*')).resolves.toBeUndefined();

		let totalAfter = 0;
		for (const m of masters) {
			totalAfter += await m.dbsize();
		}
		expect(totalAfter).toBe(0);
	});

	it('removes 100 hash-tagged keys (all keys in one slot) without falling back to a single-node scan', async () => {
		// `{tag}:rest` syntax: only the tag participates in slot hashing,
		// so all keys land in the same slot (and thus the same master).
		// The fix must STILL iterate every master because SCAN may have
		// returned 0 keys from the empty masters but we cannot tell that
		// from a single-node call.
		const pipe = cluster.pipeline();
		for (let i = 0; i < 100; i++) {
			pipe.set('cluster-scan-test:{shared-tag}:k' + i, 'v' + i);
		}
		await pipe.exec();

		const masters = cluster.nodes('master');
		let nonEmptyMasters = 0;
		for (const m of masters) {
			if ((await m.dbsize()) > 0) nonEmptyMasters++;
		}
		// All 100 keys land in the same slot -> exactly one master holds them.
		expect(nonEmptyMasters).toBe(1);

		await expect(scanAndUnlink(cluster, 'cluster-scan-test:*')).resolves.toBeUndefined();

		for (const m of masters) {
			expect(await m.dbsize()).toBe(0);
		}
	});

	it('mixed tagged + non-tagged keys: every key under the pattern is removed regardless of slot', async () => {
		// Tagged keys go to one master; non-tagged keys spread across all
		// three. scanAndUnlink must catch both. Individual SETs avoid the
		// ioredis cross-slot-pipeline restriction.
		const sets = [];
		for (let i = 0; i < 50; i++) {
			sets.push(cluster.set('cluster-scan-test:plain:' + i, 'v' + i));
			sets.push(cluster.set('cluster-scan-test:{shared-tag}:' + i, 'v' + i));
		}
		await Promise.all(sets);

		const masters = cluster.nodes('master');
		let totalBefore = 0;
		for (const m of masters) totalBefore += await m.dbsize();
		expect(totalBefore).toBe(100);

		await expect(scanAndUnlink(cluster, 'cluster-scan-test:*')).resolves.toBeUndefined();

		for (const m of masters) {
			expect(await m.dbsize()).toBe(0);
		}
	});

	it('does NOT raise CROSSSLOT when the underlying SCAN returns multiple slot-spanning keys in one batch', async () => {
		// Pre-fix code path would HGETALL-like-batched the SCAN result into
		// `redis.unlink(k1, k2, k3, ...)`. Two non-tagged keys are
		// near-guaranteed to span different slots, so a 100-key batch would
		// reliably trigger `CROSSSLOT Keys in request don't hash to the
		// same slot`. The fix runs per-key UNLINK on Cluster, which avoids
		// the multi-key restriction entirely.
		//
		// Plant a larger batch (300 keys) to make slot-spanning effectively
		// certain within the SCAN COUNT 100 batches. Individual SETs (not
		// pipeline) so each key routes to its owning slot.
		const sets = [];
		for (let i = 0; i < 300; i++) {
			sets.push(cluster.set('cluster-scan-test:dense:' + i, 'v'));
		}
		await Promise.all(sets);

		// scanAndUnlink resolves without throwing CROSSSLOT.
		let caught = null;
		try {
			await scanAndUnlink(cluster, 'cluster-scan-test:dense:*');
		} catch (err) {
			caught = err;
		}
		expect(caught).toBeNull();

		const masters = cluster.nodes('master');
		for (const m of masters) {
			expect(await m.dbsize()).toBe(0);
		}
	});

	it('does not touch keys outside the pattern', async () => {
		await cluster.set('cluster-scan-test:keep', 'survivor');
		await cluster.set('cluster-other-pattern:k1', 'survivor');

		await scanAndUnlink(cluster, 'cluster-other-pattern:*');

		expect(await cluster.get('cluster-scan-test:keep')).toBe('survivor');
		expect(await cluster.get('cluster-other-pattern:k1')).toBeNull();

		// Clean up the survivor for the next beforeEach
		await cluster.del('cluster-scan-test:keep');
	});
});
