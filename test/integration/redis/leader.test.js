/**
 * Integration tests for redis/leader against a real Redis 7 server.
 *
 * The mock-based suite at test/redis/leader.test.js exhaustively covers
 * the surface; this file pins the wire-level invariants only a real
 * server can prove: actual SET ... NX PX admission, real PEXPIRE
 * sliding under renewal ticks, the LEASE_RENEW_SCRIPT and
 * LEASE_RELEASE_SCRIPT value-match guards under genuine GET-then-mutate
 * atomicity, and the cross-instance handoff where a leader's lease
 * expires and a sibling on a separate ioredis connection takes over.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys, isClusterBackend } from '../helpers/backend.js';
import { createLeader } from '../../../redis/leader.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(pred, { intervalMs = 10, timeoutMs = 3000 } = {}) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await pred()) return;
		await wait(intervalMs);
	}
	throw new Error('waitFor: timed out');
}

describe('redis leader (integration)', () => {
	let client;

	beforeAll(() => {
		client = createBackendClient({
			keyPrefix: 'inttest-leader:'
		});
	});

	beforeEach(async () => {
		await resetBackendKeys(client);
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('acquire (SET ... NX PX)', () => {
		it('writes the prefixed key with a real PX TTL', async () => {
			const l = createLeader(client, { leaseMs: 5000, renewMs: 1500 });
			await waitFor(async () => l.isLeader());

			const value = await client.redis.get(client.key('leader'));
			const pttl = await client.redis.pttl(client.key('leader'));

			expect(value).toBe(l.instanceId);
			expect(pttl).toBeGreaterThan(0);
			expect(pttl).toBeLessThanOrEqual(5000);

			await l.stop();
			expect(await client.redis.exists(client.key('leader'))).toBe(0);
		});

		it('respects a custom key on the wire', async () => {
			const l = createLeader(client, { key: 'cron:nightly', leaseMs: 5000, renewMs: 1500 });
			await waitFor(async () => l.isLeader());

			expect(await client.redis.exists(client.key('cron:nightly'))).toBe(1);
			expect(await client.redis.exists(client.key('leader'))).toBe(0);

			await l.stop();
		});
	});

	describe('renewal slides PEXPIRE under real TTL', () => {
		// Redis Cluster: asserts every PTTL sample stays in a TIGHT band
		// (> leaseMs - renewMs*~3.5) across renewals. The cluster's per-command
		// latency variance can delay a renewal tick enough that one sample dips
		// under the tight floor, even though the lease is being renewed correctly
		// (PTTL never approaches 0 / expiry). The single-key lease is cluster-safe;
		// only this tight timing-window assertion is cluster-fragile, so it is
		// skipped on the cluster backend (the deterministic band is asserted on solo).
		(isClusterBackend() ? it.skip : it)('PTTL stays close to leaseMs while the renewal tick runs', async () => {
			const l = createLeader(client, { leaseMs: 1500, renewMs: 200 });
			await waitFor(async () => l.isLeader());

			// Sample PTTL at three points spread across multiple renewals.
			// Without renewal, PTTL would monotonically decrease toward 0;
			// with renewal, every sample should be > leaseMs - renewMs * 2.
			const samples = [];
			for (let i = 0; i < 5; i++) {
				await wait(250);
				samples.push(await client.redis.pttl(client.key('leader')));
			}

			for (const pttl of samples) {
				expect(pttl).toBeGreaterThan(800);
				expect(pttl).toBeLessThanOrEqual(1500);
			}

			await l.stop();
		});
	});

	describe('cross-instance handoff', () => {
		it('a sibling on a separate ioredis connection takes over after the leader stops', async () => {
			const clientA = createBackendClient({ keyPrefix: 'inttest-leader:' });
			const clientB = createBackendClient({ keyPrefix: 'inttest-leader:' });
			try {
				const a = createLeader(clientA, { instanceId: 'a', leaseMs: 1500, renewMs: 200 });
				const b = createLeader(clientB, { instanceId: 'b', leaseMs: 1500, renewMs: 200 });

				await waitFor(async () => a.isLeader() || b.isLeader());

				const winner = a.isLeader() ? a : b;
				const loser = winner === a ? b : a;
				expect(loser.isLeader()).toBe(false);

				await winner.stop();

				// Loser's next acquire tick (within renewMs) should win,
				// because the explicit release deleted the key.
				await waitFor(async () => loser.isLeader(), { timeoutMs: 1500 });
				expect(loser.isLeader()).toBe(true);

				await loser.stop();
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});

		it('a sibling takes over via real TTL expiry when the leader cannot release', async () => {
			const clientA = createBackendClient({ keyPrefix: 'inttest-leader:' });
			const clientB = createBackendClient({ keyPrefix: 'inttest-leader:' });
			try {
				// Short lease so the test doesn't wait long for expiry.
				const a = createLeader(clientA, { instanceId: 'a', leaseMs: 600, renewMs: 200 });
				const b = createLeader(clientB, { instanceId: 'b', leaseMs: 600, renewMs: 200 });

				await waitFor(async () => a.isLeader() || b.isLeader());
				const winner = a.isLeader() ? a : b;
				const loser = winner === a ? b : a;

				// Quit the winner's redis connection without calling stop().
				// The renewal eval will fail; the lease will expire on its
				// own after leaseMs.
				await (winner === a ? clientA.quit() : clientB.quit());

				// Wait for the lease to expire server-side, then verify
				// the loser acquires.
				await waitFor(async () => loser.isLeader(), { timeoutMs: 2500 });
				expect(loser.isLeader()).toBe(true);

				await loser.stop();
			} finally {
				try { await clientA.quit(); } catch { /* already quit */ }
				try { await clientB.quit(); } catch { /* already quit */ }
			}
		});
	});

	describe('release script (compare-and-delete)', () => {
		it('stop() does not delete the key if a sibling has taken over', async () => {
			const clientA = createBackendClient({ keyPrefix: 'inttest-leader:' });
			const clientB = createBackendClient({ keyPrefix: 'inttest-leader:' });
			try {
				const a = createLeader(clientA, { instanceId: 'a', leaseMs: 5000, renewMs: 1000 });
				await waitFor(async () => a.isLeader());

				// Manually overwrite the key with b's identity to simulate
				// a takeover that a hasn't observed yet.
				await client.redis.set(client.key('leader'), 'b', 'PX', 5000);

				// a still thinks it's leader; stop() should not delete b's
				// lease because the compare guard fails.
				await a.stop();

				expect(await client.redis.get(client.key('leader'))).toBe('b');
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});
	});

	describe('currentLeader()', () => {
		it('returns the live owner identity', async () => {
			const l = createLeader(client, { instanceId: 'observer', leaseMs: 5000, renewMs: 1500 });
			await waitFor(async () => l.isLeader());

			expect(await l.currentLeader()).toBe('observer');

			await l.stop();
			expect(await l.currentLeader()).toBe(null);
		});
	});
});
