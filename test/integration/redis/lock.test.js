/**
 * Integration tests for redis/lock against a real Redis 7 server.
 *
 * The mock-based suite at test/redis/lock.test.js exhaustively covers
 * the surface; this file pins the wire-level invariants only a real
 * server can prove: actual `SET key val NX PX` admission, real PEXPIRE
 * sliding under heartbeat ticks, the HEARTBEAT_SCRIPT and RELEASE_SCRIPT
 * value-match guards under genuine GET-then-mutate atomicity, and the
 * cross-instance acquire serialization where two physically separate
 * ioredis connections compete for the same key.
 */
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { createBackendClient, resetBackendKeys } from '../helpers/backend.js';
import { waitRedisMs, redisNowMs } from '../helpers/backend-clock.js';
import {
	createDistributedLock,
	LockAcquireTimeoutError,
	LockLostError
} from '../../../src/redis/lock.js';

function wait(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

describe('redis distributed lock (integration)', () => {
	let client;

	beforeAll(() => {
		client = createBackendClient({
			keyPrefix: 'inttest-lock:'
		});
	});

	beforeEach(async () => {
		// Wipe under our prefix so each test starts clean.
		await resetBackendKeys(client);
	});

	afterAll(async () => {
		await client.quit();
	});

	describe('acquire (SET ... NX PX)', () => {
		it('writes the prefixed key with a real PX TTL while held', async () => {
			const lock = createDistributedLock(client, { defaultTtlMs: 5000 });
			let observedPttl;
			let observedValue;
			await lock.withLock('order-42', async () => {
				observedValue = await client.redis.get(client.key('lock:order-42'));
				observedPttl = await client.redis.pttl(client.key('lock:order-42'));
			});
			expect(observedValue).toMatch(/^[0-9a-f]{32}$/);
			expect(observedPttl).toBeGreaterThan(0);
			expect(observedPttl).toBeLessThanOrEqual(5000);
			// Released after fn returns.
			expect(await client.redis.exists(client.key('lock:order-42'))).toBe(0);
		});

		it('respects a custom keyPrefix on the wire', async () => {
			const lock = createDistributedLock(client, { keyPrefix: 'mutex:' });
			await lock.withLock('foo', async () => {
				expect(await client.redis.exists(client.key('mutex:foo'))).toBe(1);
				expect(await client.redis.exists(client.key('lock:foo'))).toBe(0);
			});
		});
	});

	describe('cross-instance mutual exclusion', () => {
		// Runs on both backends. On solo the 5ms head start makes A acquire first, so
		// the order is fixed (A then B). On a cluster, connect/redirect latency makes
		// which connection wins the contended key first nondeterministic, so we assert
		// the weaker-but-true invariant the lock actually guarantees: MUTUAL EXCLUSION
		// - the two critical sections serialize in one order or the other, never
		// interleave. The single-key lock is cluster-safe either way.
		it('serializes two physically separate ioredis connections on the same key', async () => {
			const clientA = createBackendClient({ keyPrefix: 'inttest-lock:' });
			const clientB = createBackendClient({ keyPrefix: 'inttest-lock:' });
			try {
				const lockA = createDistributedLock(clientA, { retryDelayMs: 10, maxWaitMs: 5000 });
				const lockB = createDistributedLock(clientB, { retryDelayMs: 10, maxWaitMs: 5000 });

				const order = [];
				// Hand off on A's ACQUISITION, not on a fixed sleep. A 5ms gap
				// asserted that A's SET NX round-trip beats 5ms, which is a
				// statement about the machine, not about the lock - and it lost
				// that race under full-suite load. Waiting for A to be inside
				// the critical section makes "A holds it when B asks" a fact,
				// so the strict order below is the lock excluding B rather than
				// A happening to be first.
				let aHoldsLock;
				const aAcquired = new Promise((resolve) => { aHoldsLock = resolve; });
				const a = lockA.withLock('shared', async () => {
					order.push('a-start');
					aHoldsLock();
					await wait(80);
					order.push('a-end');
				});
				await aAcquired;
				const b = lockB.withLock('shared', async () => {
					order.push('b-start');
					order.push('b-end');
				});

				await Promise.all([a, b]);
				// One serialization is now the only correct outcome on both
				// tiers: B asked while A demonstrably held the lock, so anything
				// other than B running entirely after A is a failure to exclude.
				expect(order).toEqual(['a-start', 'a-end', 'b-start', 'b-end']);
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});

		it('different keys do not block across instances', async () => {
			const clientA = createBackendClient({ keyPrefix: 'inttest-lock:' });
			const clientB = createBackendClient({ keyPrefix: 'inttest-lock:' });
			try {
				const lockA = createDistributedLock(clientA);
				const lockB = createDistributedLock(clientB);

				let aDone = false;
				const a = lockA.withLock('k1', async () => { await wait(100); aDone = true; });
				await wait(5);
				// B targets a different key and must complete before A.
				await lockB.withLock('k2', async () => { /* immediate */ });
				expect(aDone).toBe(false);
				await a;
			} finally {
				await Promise.all([clientA.quit(), clientB.quit()]);
			}
		});
	});

	describe('heartbeat (value-matched PEXPIRE)', () => {
		it('refreshes the PTTL while the lock is held, allowing fn to outlive defaultTtlMs', async () => {
			// defaultTtlMs is intentionally short; heartbeat at 1/3 keeps refreshing.
			const lock = createDistributedLock(client, {
				defaultTtlMs: 400,
				heartbeatMs: 80
			});
			let observed = false;
			await lock.withLock('long-running', async () => {
				// Hold past the original TTL window, measured on Redis's OWN
				// clock (TIME) where the PEXPIRE decays, so the window really
				// elapsed despite host/VM clock drift. If the heartbeat were
				// not refreshing PEXPIRE, the key would have elapsed and our
				// release script's compare-and-delete would no-op.
				await waitRedisMs(client, 900);
				const pttl = await client.redis.pttl(client.key('lock:long-running'));
				expect(pttl).toBeGreaterThan(0);
				expect(pttl).toBeLessThanOrEqual(400);
				observed = true;
			});
			expect(observed).toBe(true);
			// Heartbeat-refreshed lock was released cleanly.
			expect(await client.redis.exists(client.key('lock:long-running'))).toBe(0);
		});
	});

	describe('force-takeover detection', () => {
		it('aborts via signal when an external DEL yanks the key out from under the holder', async () => {
			const lock = createDistributedLock(client, {
				defaultTtlMs: 5000,
				heartbeatMs: 30
			});

			let lostError;
			const result = lock.withLock('contested', async (signal) => {
				signal.addEventListener('abort', () => {
					lostError = signal.reason;
				});
				// Stay mid-flight long enough for several heartbeat ticks
				// (one of which will observe the deletion).
				await wait(200);
			});

			// External operator yanks the key while we're still inside fn.
			await wait(50);
			await client.redis.del(client.key('lock:contested'));

			await result;
			expect(lostError).toBeInstanceOf(LockLostError);
			expect(lostError.key).toBe('contested');
		});

		it('release script does not delete when a foreign fence value owns the key', async () => {
			// Plant a foreign holder, then have our withLock observe its TTL
			// elapse and acquire afterward. The foreign value is preserved
			// when our (different) acquirer attempts release at the wrong key.
			//
			// Cleaner shape: drive RELEASE_SCRIPT directly via withLock under
			// a force-takeover, where the holder finishes fn AFTER a foreign
			// value swap. Our compare-and-delete refuses to delete it.
			const lock = createDistributedLock(client, {
				defaultTtlMs: 5000,
				// Disable observable heartbeat firing inside the short fn so
				// the `lost` flag stays false and release runs against the
				// foreign-occupied key. heartbeatMs > fn duration achieves this.
				heartbeatMs: 60_000
			});

			await lock.withLock('takeover', async () => {
				// Mid-flight foreign takeover via XX (only overwrites existing).
				await client.redis.set(client.key('lock:takeover'), 'foreign-token', 'XX');
			});

			// Our release ran (heartbeat never fired, lost===false) but its
			// compare-and-delete refused: foreign value still in place.
			expect(await client.redis.get(client.key('lock:takeover'))).toBe('foreign-token');
		});
	});

	describe('acquire timeout against a real foreign holder', () => {
		it('throws LockAcquireTimeoutError after maxWaitMs and leaves the foreign key intact', async () => {
			// Plant a foreign holder with a long PX so it will not elapse
			// during the test's wait window.
			await client.redis.set(client.key('lock:held'), 'foreign-fence', 'NX', 'PX', 60_000);

			const lock = createDistributedLock(client, { retryDelayMs: 20, maxWaitMs: 120 });

			let ranFn = false;
			const start = Date.now();
			await expect(
				lock.withLock('held', async () => { ranFn = true; })
			).rejects.toBeInstanceOf(LockAcquireTimeoutError);
			const elapsed = Date.now() - start;

			expect(ranFn).toBe(false);
			expect(elapsed).toBeGreaterThanOrEqual(120);
			// Foreign holder is untouched.
			expect(await client.redis.get(client.key('lock:held'))).toBe('foreign-fence');
		});

		it('acquires immediately once the foreign holder TTL elapses', async () => {
			// Foreign holder with a short PX; withLock retries past it.
			await client.redis.set(client.key('lock:freeing'), 'foreign-fence', 'NX', 'PX', 200);

			const lock = createDistributedLock(client, { retryDelayMs: 20, maxWaitMs: 2000 });

			// The elapsed wait is measured on the Redis clock - the same clock
			// the foreign PX lives on - so a host/VM clock step can neither
			// shrink nor stretch the bound being asserted.
			const start = await redisNowMs(client.redis);
			let ranFn = false;
			await lock.withLock('freeing', async () => {
				ranFn = true;
				const v = await client.redis.get(client.key('lock:freeing'));
				expect(v).toMatch(/^[0-9a-f]{32}$/);
				expect(v).not.toBe('foreign-fence');
			});
			const elapsed = (await redisNowMs(client.redis)) - start;

			expect(ranFn).toBe(true);
			// We waited for the foreign TTL (~200ms) but well under maxWaitMs.
			expect(elapsed).toBeGreaterThanOrEqual(150);
			expect(elapsed).toBeLessThan(2000);
			expect(await client.redis.exists(client.key('lock:freeing'))).toBe(0);
		});
	});

	describe('external signal cancellation', () => {
		it('aborts mid acquire-loop when the user signal fires', async () => {
			await client.redis.set(client.key('lock:k'), 'foreign-fence', 'NX', 'PX', 60_000);
			const lock = createDistributedLock(client, { retryDelayMs: 25, maxWaitMs: 5000 });

			const ac = new AbortController();
			const result = lock.withLock('k', async () => { /* never reached */ }, { signal: ac.signal });
			await wait(40);
			ac.abort(new Error('user-cancel'));

			await expect(result).rejects.toThrow(/user-cancel|aborted/);
			// Foreign holder remains untouched; no recovery side effect.
			expect(await client.redis.get(client.key('lock:k'))).toBe('foreign-fence');
		});
	});
});
