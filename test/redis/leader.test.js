import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mockRedisClient } from '../helpers/mock-redis.js';
import { createLeader } from '../../redis/leader.js';
import { createMetrics } from '../../prometheus/index.js';
import { createCircuitBreaker } from '../../shared/breaker.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Wait until `pred()` returns truthy, polling `intervalMs` up to `timeoutMs`. */
async function waitFor(pred, { intervalMs = 5, timeoutMs = 1000 } = {}) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (pred()) return;
		await sleep(intervalMs);
	}
	throw new Error('waitFor: timed out');
}

describe('redis leader', () => {
	let client;
	let leaders;

	beforeEach(() => {
		client = mockRedisClient('app:');
		leaders = [];
	});

	afterEach(async () => {
		for (const l of leaders) await l.stop();
	});

	function track(l) {
		leaders.push(l);
		return l;
	}

	describe('construction', () => {
		it('requires a redis client', () => {
			expect(() => createLeader(null)).toThrow(/client/);
			expect(() => createLeader({})).toThrow(/client/);
		});

		it('rejects invalid options', () => {
			expect(() => createLeader(client, { key: '' })).toThrow(/key/);
			expect(() => createLeader(client, { instanceId: '' })).toThrow(/instanceId/);
			expect(() => createLeader(client, { leaseMs: 0 })).toThrow(/leaseMs/);
			expect(() => createLeader(client, { renewMs: 0 })).toThrow(/renewMs/);
			expect(() => createLeader(client, { leaseMs: 100, renewMs: 100 })).toThrow(/renewMs must be < leaseMs/);
			expect(() => createLeader(client, { leaseMs: 100, renewMs: 200 })).toThrow(/renewMs must be < leaseMs/);
			expect(() => createLeader(client, { onError: 'nope' })).toThrow(/onError/);
			expect(() => createLeader(client, { mapKey: 'nope' })).toThrow(/mapKey/);
		});

		it('returns the public API shape', () => {
			const l = track(createLeader(client));
			expect(typeof l.isLeader).toBe('function');
			expect(typeof l.currentLeader).toBe('function');
			expect(typeof l.stop).toBe('function');
			expect(typeof l.instanceId).toBe('string');
			expect(l.instanceId.length).toBeGreaterThan(0);
			expect(typeof l.key).toBe('string');
			expect(l.key).toBe('app:leader');
		});

		it('honors a custom instanceId', () => {
			const l = track(createLeader(client, { instanceId: 'worker-7' }));
			expect(l.instanceId).toBe('worker-7');
		});

		it('honors a custom key', () => {
			const l = track(createLeader(client, { key: 'cron:nightly' }));
			expect(l.key).toBe('app:cron:nightly');
		});

		it('isLeader() is false synchronously at construction (acquire is async)', () => {
			const l = track(createLeader(client));
			expect(l.isLeader()).toBe(false);
		});
	});

	describe('single-worker acquire / renew / stop', () => {
		it('acquires within one tick and isLeader() flips true', async () => {
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 200 }));
			await waitFor(() => l.isLeader());
			expect(l.isLeader()).toBe(true);
			expect(await client.redis.get('app:leader')).toBe(l.instanceId);
		});

		it('renews periodically while held', async () => {
			const m = createMetrics();
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 60, metrics: m }));
			await waitFor(() => l.isLeader());

			// Wait long enough for at least 3 renewal ticks.
			await sleep(250);

			const out = m.serialize();
			const match = out.match(/leader_renewals_total\{key_class="leader"\}\s+(\d+)/);
			expect(match).not.toBeNull();
			expect(Number(match[1])).toBeGreaterThanOrEqual(2);
		});

		it('stop() releases the lease via compare-and-delete', async () => {
			const l = createLeader(client, { leaseMs: 600, renewMs: 60 });
			await waitFor(() => l.isLeader());
			expect(await client.redis.get('app:leader')).toBe(l.instanceId);

			await l.stop();

			expect(l.isLeader()).toBe(false);
			expect(await client.redis.get('app:leader')).toBe(null);
		});

		it('stop() is idempotent', async () => {
			const l = createLeader(client, { leaseMs: 600, renewMs: 60 });
			await waitFor(() => l.isLeader());

			await l.stop();
			await l.stop();
			await l.stop();
			expect(l.isLeader()).toBe(false);
		});

		it('stop() before first acquire completes still resolves cleanly', async () => {
			const l = createLeader(client, { leaseMs: 600, renewMs: 60 });
			await l.stop();
			expect(l.isLeader()).toBe(false);
		});
	});

	describe('multi-worker mutual exclusion', () => {
		it('only one of two workers holds leadership at any moment', async () => {
			const a = track(createLeader(client, { instanceId: 'a', leaseMs: 600, renewMs: 60 }));
			const b = track(createLeader(client, { instanceId: 'b', leaseMs: 600, renewMs: 60 }));

			await waitFor(() => a.isLeader() || b.isLeader());

			// Sample several times -- mutual exclusion is the invariant.
			for (let i = 0; i < 5; i++) {
				expect(a.isLeader() && b.isLeader()).toBe(false);
				await sleep(20);
			}

			const owner = await client.redis.get('app:leader');
			expect(['a', 'b']).toContain(owner);
		});

		it('the loser becomes leader after the winner stops', async () => {
			const a = track(createLeader(client, { instanceId: 'a', leaseMs: 600, renewMs: 30 }));
			const b = track(createLeader(client, { instanceId: 'b', leaseMs: 600, renewMs: 30 }));

			await waitFor(() => a.isLeader() || b.isLeader());

			const winner = a.isLeader() ? a : b;
			const loser = winner === a ? b : a;
			expect(loser.isLeader()).toBe(false);

			await winner.stop();

			// Loser's next renew-tick (within ~30ms) sees the empty key and
			// acquires.
			await waitFor(() => loser.isLeader(), { timeoutMs: 500 });
			expect(loser.isLeader()).toBe(true);
		});
	});

	describe('lease expiry', () => {
		// The mock redis does not simulate PX-based TTL expiry; cross-worker
		// expiry-then-handoff is exercised end-to-end in the integration
		// suite where real Redis enforces TTL. Here we cover the
		// self-healing recovery path: when the lease vanishes (we simulate
		// expiry by deleting the key), the renewal loop notices via the
		// compare-and-pexpire script returning 0, transitions to
		// non-leader, then re-acquires on its next tick.
		it('leader re-acquires on its next tick once the lease key is gone', async () => {
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 30 }));
			await waitFor(() => l.isLeader());

			await client.redis.del('app:leader');

			// One tick observes the missing key (renew returns 0,
			// _isLeader flips false), the next tick re-acquires via SET NX.
			await sleep(120);
			expect(l.isLeader()).toBe(true);
			expect(await client.redis.get('app:leader')).toBe(l.instanceId);
		});
	});

	describe('redis failure', () => {
		it('isLeader() flips false on first renewal failure and onError fires', async () => {
			const errors = [];
			const l = track(createLeader(client, {
				leaseMs: 600,
				renewMs: 50,
				onError: (err) => errors.push(err)
			}));
			await waitFor(() => l.isLeader());

			client.redis.eval = async () => { throw new Error('redis down'); };

			await waitFor(() => !l.isLeader(), { timeoutMs: 500 });
			expect(l.isLeader()).toBe(false);
			expect(errors.length).toBeGreaterThanOrEqual(1);
			expect(errors[0].message).toBe('redis down');
		});

		it('renewal-interval errors do not throw out of the worker', async () => {
			// If a setInterval callback rejects synchronously, vitest sees
			// an unhandled rejection. We assert the run completes normally.
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 30 }));
			await waitFor(() => l.isLeader());
			client.redis.eval = async () => { throw new Error('boom'); };
			client.redis.set = async () => { throw new Error('boom-set'); };

			await sleep(150);
			// Surviving the sleep without an unhandled rejection is the test.
			expect(l.isLeader()).toBe(false);
		});

		it('on Redis recovery, normal acquire resumes', async () => {
			const l = track(createLeader(client, { leaseMs: 400, renewMs: 30 }));
			await waitFor(() => l.isLeader());

			const origEval = client.redis.eval;
			const origSet = client.redis.set;
			client.redis.eval = async () => { throw new Error('down'); };
			client.redis.set = async () => { throw new Error('down'); };

			await waitFor(() => !l.isLeader(), { timeoutMs: 500 });

			// Recover the connection and clear the stale key (simulating
			// server-side TTL expiry, which the mock does not enforce).
			client.redis.eval = origEval;
			client.redis.set = origSet;
			await client.redis.del('app:leader');

			await waitFor(() => l.isLeader(), { timeoutMs: 1000 });
			expect(l.isLeader()).toBe(true);
		});

		it('onError listener that throws does not crash the renewal loop', async () => {
			const l = track(createLeader(client, {
				leaseMs: 600,
				renewMs: 30,
				onError: () => { throw new Error('listener buggy'); }
			}));
			await waitFor(() => l.isLeader());

			client.redis.eval = async () => { throw new Error('boom'); };

			await sleep(120);
			// The renewal loop survived a throwing listener.
			expect(l.isLeader()).toBe(false);
		});
	});

	describe('currentLeader()', () => {
		it('returns the owner instanceId when held', async () => {
			const l = track(createLeader(client, { instanceId: 'owner-1', leaseMs: 600, renewMs: 60 }));
			await waitFor(() => l.isLeader());

			expect(await l.currentLeader()).toBe('owner-1');
		});

		it('returns null when unowned', async () => {
			const l = track(createLeader(client));
			// Don't wait for acquire; just delete the key.
			await client.redis.del('app:leader');
			expect(await l.currentLeader()).toBe(null);
		});

		it('returns null and calls onError on Redis failure', async () => {
			const errors = [];
			const l = track(createLeader(client, { onError: (err) => errors.push(err) }));
			client.redis.get = async () => { throw new Error('get failed'); };

			expect(await l.currentLeader()).toBe(null);
			expect(errors.length).toBe(1);
		});
	});

	describe('breaker integration', () => {
		it('counts a breaker failure on renewal error', async () => {
			const breaker = createCircuitBreaker({ failureThreshold: 5 });
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 30, breaker }));
			await waitFor(() => l.isLeader());

			client.redis.eval = async () => { throw new Error('down'); };

			await waitFor(() => breaker.failures > 0, { timeoutMs: 500 });
			expect(breaker.failures).toBeGreaterThan(0);

			breaker.destroy();
		});

	});

	describe('metrics', () => {
		it('emits leader_acquired_total on initial acquire', async () => {
			const m = createMetrics();
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 60, metrics: m }));
			await waitFor(() => l.isLeader());

			const out = m.serialize();
			expect(out).toMatch(/leader_acquired_total\{key_class="leader"\} 1/);
		});

		it('emits leader_lost_total on transition to non-leader', async () => {
			const m = createMetrics();
			const l = track(createLeader(client, { leaseMs: 600, renewMs: 30, metrics: m }));
			await waitFor(() => l.isLeader());

			client.redis.eval = async () => { throw new Error('down'); };
			await waitFor(() => !l.isLeader(), { timeoutMs: 500 });

			const out = m.serialize();
			expect(out).toMatch(/leader_lost_total\{key_class="leader"\} 1/);
		});

		it('mapKey controls the key_class label', async () => {
			const m = createMetrics();
			const l = track(createLeader(client, {
				key: 'cron:nightly',
				leaseMs: 600,
				renewMs: 60,
				metrics: m,
				mapKey: () => 'cron'
			}));
			await waitFor(() => l.isLeader());

			const out = m.serialize();
			expect(out).toMatch(/leader_acquired_total\{key_class="cron"\} 1/);
		});
	});
});
