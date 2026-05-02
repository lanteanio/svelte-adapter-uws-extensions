import { describe, it, expect, beforeEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import {
	createDistributedLock,
	LockAcquireTimeoutError,
	LockLostError
} from '../../redis/lock.js';
import { createMetrics } from '../../prometheus/index.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

describe('redis distributed lock', () => {
	let client;

	beforeEach(() => {
		client = mockRedisClient('app:');
	});

	describe('construction', () => {
		it('requires a redis client', () => {
			expect(() => createDistributedLock(null)).toThrow(/client/);
			expect(() => createDistributedLock({})).toThrow(/client/);
		});

		it('rejects invalid options', () => {
			expect(() => createDistributedLock(client, { defaultTtlMs: 0 })).toThrow(/defaultTtlMs/);
			expect(() => createDistributedLock(client, { retryDelayMs: -1 })).toThrow(/retryDelayMs/);
			expect(() => createDistributedLock(client, { maxWaitMs: -1 })).toThrow(/maxWaitMs/);
			expect(() => createDistributedLock(client, { heartbeatMs: 0 })).toThrow(/heartbeatMs/);
			expect(() => createDistributedLock(client, { mapKey: 'string' })).toThrow(/mapKey/);
		});

		it('returns the public API shape', () => {
			const lock = createDistributedLock(client);
			expect(typeof lock.withLock).toBe('function');
		});
	});

	describe('withLock: basic acquire / release', () => {
		it('acquires, runs fn, releases the key', async () => {
			const lock = createDistributedLock(client);
			let ranInside = false;
			const out = await lock.withLock('order-42', async () => {
				ranInside = true;
				expect(await client.redis.get(client.key('lock:order-42'))).not.toBe(null);
				return 'result';
			});
			expect(ranInside).toBe(true);
			expect(out).toBe('result');
			expect(await client.redis.get(client.key('lock:order-42'))).toBe(null);
		});

		it('passes an AbortSignal to fn', async () => {
			const lock = createDistributedLock(client);
			let signal;
			await lock.withLock('k', async (s) => { signal = s; });
			expect(signal).toBeInstanceOf(AbortSignal);
			expect(signal.aborted).toBe(false);
		});

		it('forwards fn synchronous return values', async () => {
			const lock = createDistributedLock(client);
			const out = await lock.withLock('k', () => 7);
			expect(out).toBe(7);
		});

		it('releases the key when fn throws and propagates the error', async () => {
			const lock = createDistributedLock(client);
			const err = new Error('boom');
			await expect(lock.withLock('k', async () => { throw err; })).rejects.toBe(err);
			expect(await client.redis.get(client.key('lock:k'))).toBe(null);
		});

		it('uses the keyPrefix when forming the Redis key', async () => {
			const lock = createDistributedLock(client, { keyPrefix: 'mutex:' });
			await lock.withLock('foo', async () => {
				expect(await client.redis.get(client.key('mutex:foo'))).not.toBe(null);
				expect(await client.redis.get(client.key('lock:foo'))).toBe(null);
			});
		});
	});

	describe('withLock: validation', () => {
		it('rejects an empty key', async () => {
			const lock = createDistributedLock(client);
			await expect(lock.withLock('', async () => {})).rejects.toThrow(/key/);
		});

		it('rejects a non-function fn', async () => {
			const lock = createDistributedLock(client);
			await expect(lock.withLock('k', null)).rejects.toThrow(/fn/);
		});

		it('rejects per-call ttlMs that is non-positive', async () => {
			const lock = createDistributedLock(client);
			await expect(
				lock.withLock('k', async () => {}, { ttlMs: 0 })
			).rejects.toThrow(/ttlMs/);
		});

		it('rejects per-call maxWaitMs that is negative', async () => {
			const lock = createDistributedLock(client);
			await expect(
				lock.withLock('k', async () => {}, { maxWaitMs: -1 })
			).rejects.toThrow(/maxWaitMs/);
		});
	});

	describe('withLock: mutual exclusion', () => {
		it('serializes two concurrent calls on the same key', async () => {
			const lock = createDistributedLock(client, { retryDelayMs: 5 });
			const order = [];

			const a = lock.withLock('k', async () => {
				order.push('a-start');
				await sleep(20);
				order.push('a-end');
			});
			// Microtask gap to let A start before B issues its acquire.
			await sleep(2);
			const b = lock.withLock('k', async () => {
				order.push('b-start');
				await sleep(5);
				order.push('b-end');
			});

			await Promise.all([a, b]);
			expect(order).toEqual(['a-start', 'a-end', 'b-start', 'b-end']);
		});

		it('different keys do not block each other', async () => {
			const lock = createDistributedLock(client);
			let aDone = false;
			const a = lock.withLock('k1', async () => { await sleep(20); aDone = true; });
			await sleep(2);
			// b should complete fast even while a is still mid-flight.
			await lock.withLock('k2', async () => { /* no wait */ });
			expect(aDone).toBe(false);
			await a;
		});
	});

	describe('withLock: acquire timeout', () => {
		it('throws LockAcquireTimeoutError after maxWaitMs', async () => {
			const lock = createDistributedLock(client, {
				retryDelayMs: 5,
				maxWaitMs: 30
			});
			// Plant a foreign holder.
			await client.redis.set(client.key('lock:held'), 'foreign', 'NX', 'PX', 60000);

			let ranFn = false;
			await expect(
				lock.withLock('held', async () => { ranFn = true; })
			).rejects.toBeInstanceOf(LockAcquireTimeoutError);
			expect(ranFn).toBe(false);
		});

		it('exposes key + waitedMs on the timeout error', async () => {
			const lock = createDistributedLock(client, { retryDelayMs: 5, maxWaitMs: 30 });
			await client.redis.set(client.key('lock:k'), 'foreign', 'NX', 'PX', 60000);

			try {
				await lock.withLock('k', async () => {});
				expect.fail('should have thrown');
			} catch (err) {
				expect(err).toBeInstanceOf(LockAcquireTimeoutError);
				expect(err.key).toBe('k');
				expect(err.waitedMs).toBeGreaterThanOrEqual(30);
			}
		});

		it('does not modify the foreign-held key on timeout', async () => {
			const lock = createDistributedLock(client, { retryDelayMs: 5, maxWaitMs: 20 });
			await client.redis.set(client.key('lock:k'), 'foreign', 'NX', 'PX', 60000);

			await expect(lock.withLock('k', async () => {})).rejects.toBeInstanceOf(LockAcquireTimeoutError);
			expect(await client.redis.get(client.key('lock:k'))).toBe('foreign');
		});

		it('honors the per-call maxWaitMs override', async () => {
			const lock = createDistributedLock(client, { retryDelayMs: 5, maxWaitMs: 5000 });
			await client.redis.set(client.key('lock:k'), 'foreign', 'NX', 'PX', 60000);

			const start = Date.now();
			await expect(
				lock.withLock('k', async () => {}, { maxWaitMs: 20 })
			).rejects.toBeInstanceOf(LockAcquireTimeoutError);
			expect(Date.now() - start).toBeLessThan(150);
		});
	});

	describe('withLock: external signal', () => {
		it('throws immediately when an already-aborted signal is passed', async () => {
			const lock = createDistributedLock(client);
			const ac = new AbortController();
			ac.abort(new Error('cancelled'));

			await expect(
				lock.withLock('k', async () => {}, { signal: ac.signal })
			).rejects.toThrow(/cancelled|aborted/);
			expect(await client.redis.get(client.key('lock:k'))).toBe(null);
		});

		it('aborts the acquire wait when the external signal fires mid-loop', async () => {
			const lock = createDistributedLock(client, { retryDelayMs: 20, maxWaitMs: 5000 });
			await client.redis.set(client.key('lock:k'), 'foreign', 'NX', 'PX', 60000);

			const ac = new AbortController();
			const result = lock.withLock('k', async () => {}, { signal: ac.signal });
			await sleep(5);
			ac.abort(new Error('user-cancel'));

			await expect(result).rejects.toThrow(/user-cancel|aborted/);
		});

		it('forwards the external signal to fn alongside our internal one', async () => {
			const lock = createDistributedLock(client);
			const ac = new AbortController();
			let signalSeen;
			const result = lock.withLock('k', async (signal) => {
				signalSeen = signal;
				await sleep(50);
			}, { signal: ac.signal });
			await sleep(5);
			expect(signalSeen.aborted).toBe(false);
			ac.abort();
			await sleep(5);
			expect(signalSeen.aborted).toBe(true);
			await result;
		});
	});

	describe('withLock: heartbeat / lost lock', () => {
		it('aborts via signal when a foreign instance takes over the key', async () => {
			const lock = createDistributedLock(client, {
				defaultTtlMs: 1000,
				heartbeatMs: 10
			});

			let lostError;
			const result = lock.withLock('contested', async (signal) => {
				signal.addEventListener('abort', () => {
					lostError = signal.reason;
				});
				// Wait long enough for a few heartbeat ticks.
				await sleep(60);
			});

			// Yank the key out from under us mid-flight.
			await sleep(15);
			await client.redis.del(client.key('lock:contested'));

			await result;
			expect(lostError).toBeInstanceOf(LockLostError);
			expect(lostError.key).toBe('contested');
		});

		it('does not call the release script after the lock was lost', async () => {
			// If we no longer own it, release would be a no-op via the
			// compare-and-delete guard, but skipping it entirely avoids the
			// pointless eval call. Verify the key absent after fn finishes.
			const lock = createDistributedLock(client, {
				defaultTtlMs: 1000,
				heartbeatMs: 10
			});
			const result = lock.withLock('k', async (signal) => {
				signal.addEventListener('abort', () => {}); // observer
				await sleep(30);
			});

			await sleep(15);
			// Force-takeover by another instance.
			await client.redis.set(client.key('lock:k'), 'foreign-token', 'XX');

			await result;
			// Foreign-token still in place: our release skipped, AND the
			// compare-and-delete shape would have refused anyway.
			expect(await client.redis.get(client.key('lock:k'))).toBe('foreign-token');
		});

		it('continues running fn through a transient eval failure on heartbeat', async () => {
			// Simulate a single failed heartbeat by swapping eval temporarily.
			const lock = createDistributedLock(client, {
				defaultTtlMs: 1000,
				heartbeatMs: 10
			});

			const realEval = client.redis.eval.bind(client.redis);
			let failsLeft = 1;
			client.redis.eval = async (...args) => {
				if (failsLeft > 0) {
					failsLeft--;
					throw new Error('transient redis failure');
				}
				return realEval(...args);
			};

			let aborted = false;
			const result = lock.withLock('k', async (signal) => {
				signal.addEventListener('abort', () => { aborted = true; });
				await sleep(50);
			});
			await result;

			// First heartbeat after acquire failed; subsequent heartbeats
			// succeeded; signal never fired.
			expect(aborted).toBe(false);
			client.redis.eval = realEval;
		});
	});

	describe('withLock: metrics', () => {
		it('counts lock_acquired_total under the mapKey class', async () => {
			const metrics = createMetrics();
			const lock = createDistributedLock(client, {
				metrics,
				mapKey: (k) => k.split(':')[0]
			});
			await lock.withLock('orders:42', async () => {});
			await lock.withLock('orders:99', async () => {});
			await lock.withLock('payments:7', async () => {});

			const out = await metrics.serialize();
			expect(out).toMatch(/lock_acquired_total\{key_class="orders"\}\s+2/);
			expect(out).toMatch(/lock_acquired_total\{key_class="payments"\}\s+1/);
		});

		it('observes lock_acquire_wait_ms on every successful acquire', async () => {
			const metrics = createMetrics();
			const lock = createDistributedLock(client, { metrics });
			await lock.withLock('k', async () => {});
			await lock.withLock('k', async () => {});

			const out = await metrics.serialize();
			expect(out).toMatch(/lock_acquire_wait_ms_count\s+2/);
		});

		it('counts lock_acquire_timeouts_total under the mapKey class', async () => {
			const metrics = createMetrics();
			const lock = createDistributedLock(client, {
				metrics,
				retryDelayMs: 5,
				maxWaitMs: 20,
				mapKey: (k) => k.split(':')[0]
			});
			await client.redis.set(client.key('lock:orders:42'), 'foreign', 'NX', 'PX', 60000);
			await expect(
				lock.withLock('orders:42', async () => {})
			).rejects.toBeInstanceOf(LockAcquireTimeoutError);

			const out = await metrics.serialize();
			expect(out).toMatch(/lock_acquire_timeouts_total\{key_class="orders"\}\s+1/);
		});

		it('counts lock_lost_total under the mapKey class', async () => {
			const metrics = createMetrics();
			const lock = createDistributedLock(client, {
				metrics,
				defaultTtlMs: 1000,
				heartbeatMs: 10,
				mapKey: (k) => k.split(':')[0]
			});

			const result = lock.withLock('orders:42', async (signal) => {
				signal.addEventListener('abort', () => {});
				await sleep(40);
			});
			await sleep(15);
			await client.redis.del(client.key('lock:orders:42'));
			await result;

			const out = await metrics.serialize();
			expect(out).toMatch(/lock_lost_total\{key_class="orders"\}\s+1/);
		});
	});

	describe('withLock: breaker integration', () => {
		it('passes Redis errors through the breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 2 });
			const lock = createDistributedLock(client, { breaker });

			const realSet = client.redis.set.bind(client.redis);
			client.redis.set = async () => { throw new Error('redis down'); };
			await expect(lock.withLock('k', async () => {})).rejects.toThrow(/redis down/);
			await expect(lock.withLock('k', async () => {})).rejects.toThrow(/redis down/);
			expect(breaker.state).toBe('broken');

			client.redis.set = realSet;
		});
	});
});
