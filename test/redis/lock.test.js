import { describe, it, expect, beforeEach, vi } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import {
	createDistributedLock,
	LockAcquireTimeoutError,
	LockLostError
} from '../../src/redis/lock.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { setRuntimeEnv, resetRuntimeEnv } from '../../src/shared/runtime.js';
import { createCircuitBreaker } from '../../src/shared/breaker.js';

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

	describe('withLock: retry-delay jitter (thundering-herd avoidance)', () => {
		it('retry sleeps are jittered, not all identical, and bounded by 1.25x retryDelayMs', async () => {
			// Plant a foreign holder so every acquire attempt fails and the
			// jittered sleep runs. Sample sleeps; verify they are not all
			// identical (the pre-fix behavior) and the upper bound holds.
			// The lower bound is not asserted because the sleep is clamped
			// to (callMaxWaitMs - elapsed), so the final retries before
			// timeout will fall below the [0.75x] floor.
			const RETRY_DELAY = 40;
			const lock = createDistributedLock(client, {
				retryDelayMs: RETRY_DELAY,
				maxWaitMs: RETRY_DELAY * 10
			});
			await client.redis.set(client.key('lock:contested'), 'foreign', 'NX', 'PX', 60000);

			const samples = [];
			const origSetTimeout = global.setTimeout;
			/** @type {any} */
			global.setTimeout = function (fn, ms) {
				if (typeof ms === 'number' && ms > 0 && ms <= RETRY_DELAY * 1.25 + 1) samples.push(ms);
				return origSetTimeout(fn, ms);
			};

			try {
				await lock.withLock('contested', async () => {}).catch(() => {});
			} finally {
				global.setTimeout = origSetTimeout;
			}

			expect(samples.length).toBeGreaterThan(2);
			const max = RETRY_DELAY * 1.25;
			for (const s of samples) {
				expect(s).toBeLessThanOrEqual(max);
			}
			// Confirm at least two samples differ (no jitter would mean every
			// non-clamped sample equals RETRY_DELAY exactly).
			const distinct = new Set(samples);
			expect(distinct.size).toBeGreaterThan(1);
		});

		it('jitter never produces a negative or out-of-budget sleep', async () => {
			// When elapsed approaches callMaxWaitMs, the sleep is clamped to
			// the remaining budget. Verify the clamp still works under jitter.
			const lock = createDistributedLock(client, {
				retryDelayMs: 100,
				maxWaitMs: 10
			});
			await client.redis.set(client.key('lock:tight'), 'foreign', 'NX', 'PX', 60000);

			let maxSeen = 0;
			const origSetTimeout = global.setTimeout;
			/** @type {any} */
			global.setTimeout = function (fn, ms) {
				if (typeof ms === 'number' && ms > 0) maxSeen = Math.max(maxSeen, ms);
				return origSetTimeout(fn, ms);
			};

			try {
				await lock.withLock('tight', async () => {}).catch(() => {});
			} finally {
				global.setTimeout = origSetTimeout;
			}

			// Every captured sleep must be within the maxWaitMs budget.
			expect(maxSeen).toBeLessThanOrEqual(10);
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
				expect(err).toMatchObject({ code: 'LOCK_ACQUIRE_TIMEOUT' });
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

	describe('monotonic fences', () => {
		it('is opt-in and preserves the one-argument callback by default', async () => {
			const increment = vi.spyOn(client.redis, 'incr');
			const lock = createDistributedLock(client);
			const callback = vi.fn(() => 'ok');
			await expect(lock.withLock('plain', callback)).resolves.toBe('ok');
			expect(callback.mock.calls[0]).toHaveLength(1);
			expect(increment).not.toHaveBeenCalled();
		});

		it('increments across independent factories and permits a per-call override', async () => {
			const first = createDistributedLock(client, { fence: true });
			const second = createDistributedLock(client);
			const read = (signal, fence) => fence;
			await expect(first.withLock('ordered', read)).resolves.toBe(1);
			await expect(second.withLock('ordered', read, { fence: true })).resolves.toBe(2);
			await expect(first.withLock('ordered', read, { fence: false })).resolves.toBeUndefined();
			await expect(second.withLock('ordered', read, { fence: true })).resolves.toBe(3);
			expect(await client.redis.get(client.key('lock-fences:' + Buffer.from(client.key('lock:ordered')).toString('base64url')))).toBe('3');
		});

		it('shares a counter between factory prefixes that name the same lease', async () => {
			const first = createDistributedLock(client, { fence: true });
			const alias = createDistributedLock(client, { keyPrefix: '', fence: true });
			const read = (_, fence) => fence;
			await expect(first.withLock('alias', read)).resolves.toBe(1);
			await expect(alias.withLock('lock:alias', read)).resolves.toBe(2);
			await expect(first.withLock('alias', read)).resolves.toBe(3);
		});

		it('keeps suffix-named leases separate from persistent counters', async () => {
			const lock = createDistributedLock(client, { fence: true });
			const read = (_, fence) => fence;
			await expect(lock.withLock('x', read)).resolves.toBe(1);
			await expect(lock.withLock('x:fence', read)).resolves.toBe(1);
			await expect(lock.withLock('x', read)).resolves.toBe(2);
			const unprefixed = createDistributedLock(client, { keyPrefix: '' });
			await expect(unprefixed.withLock('lock-fences:any', () => {})).rejects.toThrow(/reserved/);
			await expect(createDistributedLock(client, { keyPrefix: 'lock-' }).withLock('fences:any', () => {})).rejects.toThrow(/reserved/);
		});

		it('releases its lease without calling user code when increment fails', async () => {
			const failure = new Error('counter unavailable');
			vi.spyOn(client.redis, 'incr').mockRejectedValueOnce(failure);
			const callback = vi.fn();
			await expect(createDistributedLock(client, { fence: true }).withLock('failed', callback)).rejects.toBe(failure);
			expect(callback).not.toHaveBeenCalled();
			expect(await client.redis.get(client.key('lock:failed'))).toBeNull();
		});

		it('refuses unsafe integer counters without invoking user code', async () => {
			await client.redis.set(client.key('lock-fences:' + Buffer.from(client.key('lock:overflow')).toString('base64url')), String(Number.MAX_SAFE_INTEGER));
			const callback = vi.fn();
			await expect(createDistributedLock(client, { fence: true }).withLock('overflow', callback)).rejects.toBeInstanceOf(RangeError);
			expect(callback).not.toHaveBeenCalled();
			expect(await client.redis.get(client.key('lock:overflow'))).toBeNull();
		});

		it('refuses a delayed former holder even when its counter exceeds its successor', async () => {
			const increment = client.redis.incr.bind(client.redis);
			let unblock, entered;
			const admitted = new Promise(resolve => { entered = resolve; });
			const gate = new Promise(resolve => { unblock = resolve; });
			vi.spyOn(client.redis, 'incr').mockImplementationOnce(async key => { entered(); await gate; return increment(key); });
			const callback = vi.fn();
			const first = createDistributedLock(client, { fence: true }).withLock('delayed', callback);
			const verdict = expect(first).rejects.toBeInstanceOf(LockLostError);
			await admitted;
			await client.redis.del(client.key('lock:delayed'));
			await expect(createDistributedLock(client, { fence: true }).withLock('delayed', (_, fence) => fence)).resolves.toBe(1);
			unblock();
			await verdict;
			expect(callback).not.toHaveBeenCalled();
			expect(await client.redis.get(client.key('lock-fences:' + Buffer.from(client.key('lock:delayed')).toString('base64url')))).toBe('2');
		});

		it('refuses a counter reply outside the conservative lease deadline', async () => {
			let clock = 0;
			setRuntimeEnv({ clock: { monotonic: () => clock } });
			const increment = client.redis.incr.bind(client.redis);
			vi.spyOn(client.redis, 'incr').mockImplementationOnce(async key => { clock = 100; return increment(key); });
			const callback = vi.fn();
			try {
				await expect(createDistributedLock(client, { fence: true, defaultTtlMs: 50 }).withLock('expired', callback)).rejects.toBeInstanceOf(LockLostError);
				expect(callback).not.toHaveBeenCalled();
			} finally { resetRuntimeEnv(); }
		});

		it('rejects non-boolean fence options', async () => {
			expect(() => createDistributedLock(client, { fence: 'yes' })).toThrow(TypeError);
			await expect(createDistributedLock(client).withLock('x', () => {}, { fence: 1 })).rejects.toThrow(TypeError);
		});

		it('accepts decimal-string counter replies from stringNumbers clients', async () => {
			const increment = client.redis.incr.bind(client.redis);
			vi.spyOn(client.redis, 'incr').mockImplementation(async key => String(await increment(key)));
			const lock = createDistributedLock(client, { fence: true });
			await expect(lock.withLock('strings', (_, fence) => fence)).resolves.toBe(1);
			await expect(lock.withLock('strings', (_, fence) => fence)).resolves.toBe(2);
		});

		it('refuses a decimal-string counter past the safe integer range', async () => {
			vi.spyOn(client.redis, 'incr').mockResolvedValueOnce('9007199254740992');
			const callback = vi.fn();
			await expect(createDistributedLock(client, { fence: true }).withLock('strings-overflow', callback)).rejects.toBeInstanceOf(RangeError);
			expect(callback).not.toHaveBeenCalled();
			expect(await client.redis.get(client.key('lock:strings-overflow'))).toBeNull();
		});

		it('refuses a non-canonical decimal counter reply', async () => {
			vi.spyOn(client.redis, 'incr').mockResolvedValueOnce('01');
			const callback = vi.fn();
			await expect(createDistributedLock(client, { fence: true }).withLock('canonical', callback)).rejects.toBeInstanceOf(RangeError);
			expect(callback).not.toHaveBeenCalled();
		});

		it('measures acquisition wait up to the lease, not through the counter step', async () => {
			let now = 0;
			setRuntimeEnv({ clock: { monotonic: () => now } });
			const metrics = createMetrics();
			const set = client.redis.set.bind(client.redis);
			client.redis.set = async (...args) => { now += 5; return set(...args); };
			const increment = client.redis.incr.bind(client.redis);
			vi.spyOn(client.redis, 'incr').mockImplementationOnce(async key => { now += 400; return increment(key); });
			try {
				await expect(createDistributedLock(client, { fence: true, metrics }).withLock('timed', (_, fence) => fence)).resolves.toBe(1);
				expect(metrics.serialize()).toContain('lock_acquire_wait_seconds_sum 0.005');
			} finally {
				resetRuntimeEnv();
			}
		});

		it('counts an ownership loss found after the counter step without failing the breaker', async () => {
			const metrics = createMetrics();
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			vi.spyOn(client.redis, 'get').mockResolvedValueOnce('another-holder');
			const lock = createDistributedLock(client, { fence: true, metrics, breaker, mapKey: (k) => k.split(':')[0] });
			const callback = vi.fn();
			await expect(lock.withLock('orders:7', callback)).rejects.toBeInstanceOf(LockLostError);
			expect(callback).not.toHaveBeenCalled();
			const out = await metrics.serialize();
			expect(out).toMatch(/lock_acquired_total\{key_class="orders"\}\s+1/);
			expect(out).toMatch(/lock_lost_total\{key_class="orders"\}\s+1/);
			expect(breaker.state).toBe('healthy');
		});

		it('does not count an unusable counter value against the breaker', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 1 });
			vi.spyOn(client.redis, 'incr').mockResolvedValueOnce(0);
			await expect(createDistributedLock(client, { fence: true, breaker }).withLock('zero', vi.fn())).rejects.toBeInstanceOf(RangeError);
			expect(breaker.state).toBe('healthy');
		});
	});

	describe('withLock: heartbeat / lost lock', () => {
		it('does not claim hard exclusion after a stale holder ignores its abort signal', async () => {
			const holder = createDistributedLock(client, {
				defaultTtlMs: 1000,
				heartbeatMs: 5
			});
			const successor = createDistributedLock(client, {
				retryDelayMs: 1,
				maxWaitMs: 1000
			});
			let releaseHolder;
			let holderRunning = false;
			let holderSignal;

			const staleRun = holder.withLock('cooperative-boundary', async (signal) => {
				holderSignal = signal;
				holderRunning = true;
				await new Promise((resolve) => { releaseHolder = resolve; });
				holderRunning = false;
			});

			while (!holderRunning) await sleep(1);
			await client.redis.del(client.key('lock:cooperative-boundary'));
			for (let i = 0; i < 50 && !holderSignal.aborted; i++) await sleep(2);

			let observedOverlap = false;
			try {
				await successor.withLock('cooperative-boundary', async () => {
					observedOverlap = holderRunning;
				});
			} finally {
				releaseHolder();
				await expect(staleRun).rejects.toBeInstanceOf(LockLostError);
			}

			expect(holderSignal.aborted).toBe(true);
			expect(observedOverlap).toBe(true);
		});

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

			await expect(result).rejects.toBeInstanceOf(LockLostError);
			expect(lostError).toBeInstanceOf(LockLostError);
			expect(lostError).toMatchObject({ code: 'LOCK_LOST' });
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

			await expect(result).rejects.toBeInstanceOf(LockLostError);
			// Foreign-token still in place: our release skipped, AND the
			// compare-and-delete shape would have refused anyway.
			expect(await client.redis.get(client.key('lock:k'))).toBe('foreign-token');
		});


		it('rejects a successful callback when the final release discovers a takeover', async () => {
			const lock = createDistributedLock(client, { heartbeatMs: 60000 });
			await expect(lock.withLock('late', async () => {
				await client.redis.set(client.key('lock:late'), 'successor', 'XX');
				return 'finished';
			})).rejects.toBeInstanceOf(LockLostError);
			expect(await client.redis.get(client.key('lock:late'))).toBe('successor');
		});

		it('preserves a callback error when cleanup also confirms ownership loss', async () => {
			const original = new Error('operation failed');
			const lock = createDistributedLock(client, { heartbeatMs: 60000 });
			await expect(lock.withLock('late', async () => {
				await client.redis.del(client.key('lock:late'));
				throw original;
			})).rejects.toBe(original);
		});

		it('waits for a pending heartbeat verdict before reporting callback completion', async () => {
			const lock = createDistributedLock(client, { defaultTtlMs: 1000, heartbeatMs: 5 });
			const realEval = client.redis.eval.bind(client.redis);
			let entered, finishHeartbeat, finishCallback;
			const started = new Promise((resolve) => { entered = resolve; });
			const heartbeatGate = new Promise((resolve) => { finishHeartbeat = resolve; });
			client.redis.eval = async (script, ...args) => {
				if (script.includes("'PEXPIRE'")) {
					entered();
					await heartbeatGate;
				}
				return realEval(script, ...args);
			};
			const pending = lock.withLock('pending', () => new Promise((resolve) => { finishCallback = resolve; }));
			let settled = false;
			void pending.then(() => { settled = true; }, () => { settled = true; });
			await started;
			await client.redis.set(client.key('lock:pending'), 'successor', 'XX');
			finishCallback('finished');
			await sleep(1);
			expect(settled).toBe(false);
			finishHeartbeat();
			await expect(pending).rejects.toBeInstanceOf(LockLostError);
			expect(await client.redis.get(client.key('lock:pending'))).toBe('successor');
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

		it('heartbeat refreshes never overlap when an eval is slower than heartbeatMs', async () => {
			// Pre-fix: setInterval(async () => ...) fired a new eval every
			// heartbeatMs regardless of whether the previous eval was still in
			// flight. With heartbeatMs=5 and eval taking 40ms, the pre-fix
			// code would have 8 concurrent heartbeat evals at any moment.
			// Post-fix: each tick is chained off the previous promise, so the
			// number of CONCURRENT heartbeat evals (eval calls carrying the
			// HEARTBEAT_SCRIPT first arg) never exceeds 1.
			const lock = createDistributedLock(client, {
				defaultTtlMs: 1000,
				heartbeatMs: 5
			});

			const realEval = client.redis.eval.bind(client.redis);
			let inFlightHeartbeats = 0;
			let maxConcurrentHeartbeats = 0;
			client.redis.eval = async (script, ...args) => {
				const isHeartbeat = script.includes("'PEXPIRE'");
				if (isHeartbeat) {
					inFlightHeartbeats++;
					maxConcurrentHeartbeats = Math.max(maxConcurrentHeartbeats, inFlightHeartbeats);
				}
				try {
					if (isHeartbeat) {
						// Make every heartbeat eval take significantly longer
						// than heartbeatMs. Several setInterval ticks fire
						// during a single in-flight heartbeat.
						await sleep(40);
					}
					return await realEval(script, ...args);
				} finally {
					if (isHeartbeat) inFlightHeartbeats--;
				}
			};

			const result = lock.withLock('slow', async () => {
				// Hold long enough for many heartbeat ticks to schedule onto
				// the in-flight chain. Pre-fix this would land in 8+
				// concurrent evals; post-fix the chain serializes them to 1.
				await sleep(200);
			});
			await result;

			expect(maxConcurrentHeartbeats).toBeLessThanOrEqual(1);
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

		it('observes lock_acquire_wait_seconds on every successful acquire', async () => {
			const metrics = createMetrics();
			const lock = createDistributedLock(client, { metrics });
			await lock.withLock('k', async () => {});
			await lock.withLock('k', async () => {});

			const out = await metrics.serialize();
			expect(out).toMatch(/lock_acquire_wait_seconds_count\s+2/);
		});

		it('records acquisition wait in seconds with explicit bounds despite registry defaults', async () => {
			const metrics = createMetrics({ defaultBuckets: [100, 1000] });
			const lock = createDistributedLock(client, { metrics });
			let now = 0;
			setRuntimeEnv({ clock: { monotonic: () => now } });
			const set = client.redis.set.bind(client.redis);
			client.redis.set = async (...args) => { now += 5; return set(...args); };
			try {
				await lock.withLock('k', () => {});
				const out = metrics.serialize();
				expect(out).toContain('lock_acquire_wait_seconds_sum 0.005');
				expect(out).toContain('lock_acquire_wait_seconds_bucket{le="0.001"} 0');
				expect(out).toContain('lock_acquire_wait_seconds_bucket{le="0.005"} 1');
				expect(out).not.toContain('lock_acquire_wait_ms');
			} finally {
				resetRuntimeEnv();
			}
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
			await expect(result).rejects.toBeInstanceOf(LockLostError);

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
