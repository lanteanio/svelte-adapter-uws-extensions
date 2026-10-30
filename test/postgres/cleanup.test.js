import { EventEmitter } from 'node:events';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { runCleanupPass, scheduleCleanup } from '../../src/postgres/_cleanup.js';
import { resetRuntimeEnv, setRuntimeEnv } from '../../src/shared/runtime.js';
import { createMetrics } from '../../src/prometheus/index.js';
import { mockPgClient } from '../helpers/mock-pg.js';
import { createIdempotencyStore } from '../../src/postgres/idempotency.js';

function checkout({ acquired = true, remove = 250 } = {}) {
	const tx = Object.assign(new EventEmitter(), { release: vi.fn(), query: vi.fn(async ({ text }) => ({ rows: text.includes('advisory') ? [{ acquired }] : [], rowCount: text === 'DELETE' ? remove : 0 })) });
	return { tx, client: { connect: vi.fn(async () => tx) } };
}
afterEach(() => { resetRuntimeEnv(); vi.useRealTimers(); });

describe('bounded PostgreSQL maintenance', () => {
	it('admits only the advisory-lock winner and releases a skipped checkout cleanly', async () => {
		const { tx, client } = checkout({ acquired: false }); const work = vi.fn();
		expect(await runCleanupPass(client, 'cache', work)).toEqual({ result: 'contended', rows: 0 });
		expect(work).not.toHaveBeenCalled(); expect(tx.release).toHaveBeenCalledExactlyOnceWith(undefined);
		expect(tx.query.mock.calls.at(-1)[0].text).toBe('COMMIT'); expect(tx.listenerCount('error')).toBe(0);
	});
	it('limits a persistent backlog to four 250-row deletes and commits only once', async () => {
		const { tx, client } = checkout();
		expect(await runCleanupPass(client, 'cache', async budget => {
			while (budget.available) { expect(budget.limit).toBe(250); await budget.remove('DELETE', []); }
		})).toEqual({ result: 'budget', rows: 1000 });
		expect(tx.query.mock.calls.filter(([q]) => q.text === 'DELETE')).toHaveLength(4);
		expect(tx.query.mock.calls.filter(([q]) => q.text === 'COMMIT')).toHaveLength(1);
	});
	it('destroys uncertain connections after a query failure instead of returning an open transaction', async () => {
		const { tx, client } = checkout(); const failure = new Error('statement timeout');
		await expect(runCleanupPass(client, 'cache', async () => { throw failure; })).rejects.toBe(failure);
		expect(tx.release).toHaveBeenCalledExactlyOnceWith(failure);
		expect(tx.query.mock.calls.some(([q]) => q.text === 'COMMIT')).toBe(false);
	});
	it('handles checked-out connection errors without an unhandled EventEmitter error', async () => {
		const { tx, client } = checkout(); const failure = new Error('connection ended');
		await expect(runCleanupPass(client, 'cache', async () => { tx.emit('error', failure); })).rejects.toBe(failure);
		expect(tx.release).toHaveBeenCalledExactlyOnceWith(failure); expect(tx.listenerCount('error')).toBe(0);
	});
	it('bounds checkout waits even for a legacy client and releases the late connection once', async () => {
		vi.useFakeTimers(); let resolve; const pending = new Promise(r => { resolve = r; }); const release = vi.fn();
		const work = vi.fn(); const client = { pool: { connect: vi.fn(() => pending) } }; const operation = runCleanupPass(client, 'cache', work);
		const assertion = expect(operation).rejects.toThrow('checkout deadline');
		await vi.advanceTimersByTimeAsync(1000); await assertion;
		expect(await runCleanupPass(client, 'cache', work)).toEqual({ result: 'waiting', rows: 0 });
		expect(client.pool.connect).toHaveBeenCalledTimes(1); resolve({ release }); await Promise.resolve();
		expect(release).toHaveBeenCalledExactlyOnceWith(); expect(work).not.toHaveBeenCalled();
	});
	it('leaves time to commit when the wall-time work budget ends', async () => {
		let clock = 0; setRuntimeEnv({ clock: { monotonic: () => clock } });
		const { tx, client } = checkout();
		expect(await runCleanupPass(client, 'cache', async budget => { await budget.remove('DELETE'); clock = 901; expect(budget.available).toBe(false); })).toEqual({ result: 'budget', rows: 250 });
		expect(tx.query.mock.calls.at(-1)[0]).toMatchObject({ text: 'COMMIT', query_timeout: 99 });
	});
	it('jitter schedules once after settlement and stop prevents further passes', async () => {
		const scheduled = []; const cleared = []; const { client } = checkout(); const metrics = createMetrics();
		setRuntimeEnv({ rng: { float: () => 0.25 }, timers: { set: (cb, ms) => { const timer = { cb, ms }; scheduled.push(timer); return timer; }, clear: t => cleared.push(t) } });
		const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'idempotency', interval: 1000, metrics, ready: async () => {}, work: async b => { await b.remove('DELETE'); } });
		expect(scheduled[0].ms).toBe(250); await scheduled[0].cb(); expect(scheduled.at(-1).ms).toBe(250);
		expect(metrics.serialize()).toContain('postgres_cleanup_rows_total{store="idempotency"} 250');
		cleanup.stop(); const calls = client.connect.mock.calls.length; await scheduled.at(-1).cb(); expect(client.connect).toHaveBeenCalledTimes(calls);
		expect(cleared).toContain(scheduled.at(-1));
	});
	it('write-triggered maintenance does not create extra timers and shares local ownership', async () => {
		const scheduled = []; const { client } = checkout(); let finish;
		const pending = new Promise(resolve => { finish = resolve; });
		const work = vi.fn(() => pending);
		setRuntimeEnv({ timers: { set: (cb, ms) => { const timer = { cb, ms }; scheduled.push(timer); return timer; }, clear() {} } });
		const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'dead-letter', interval: 1000, ready: async () => {}, work });
		try {
			const first = cleanup.run();
			await vi.waitFor(() => expect(work).toHaveBeenCalledTimes(1));
			await cleanup.run();
			expect(client.connect).toHaveBeenCalledTimes(1);
			finish(); await first;
			const timers = scheduled.length;
			await cleanup.run();
			// Only the checkout deadline is added, not another recurring timer.
			expect(scheduled).toHaveLength(timers + 1);
		} finally { finish(); cleanup.stop(); }
	});
	it('queues one pass per pass that saw writes, ends when writes stop, and stop cancels it', async () => {
		vi.useFakeTimers();
		const { client } = checkout(); let cleanup; let writes = 3;
		// Each of the first passes sees one write arrive while it runs, the last
		// write landing during a queued pass.
		const work = vi.fn(async () => { if (writes-- > 0) await cleanup.run(); });
		cleanup = scheduleCleanup({ client, table: 'cache', kind: 'dead-letter', interval: 0, coalesce: true, ready: async () => {}, work });
		try {
			await cleanup.run();
			expect(work).toHaveBeenCalledTimes(1);
			await vi.advanceTimersByTimeAsync(1);
			expect(work).toHaveBeenCalledTimes(2);
			await vi.advanceTimersByTimeAsync(1);
			await vi.advanceTimersByTimeAsync(1);
			expect(work).toHaveBeenCalledTimes(4);
			expect(vi.getTimerCount()).toBe(0);
			writes = 1; await cleanup.run();
			expect(work).toHaveBeenCalledTimes(5);
			cleanup.stop(); await vi.advanceTimersByTimeAsync(1000);
			expect(work).toHaveBeenCalledTimes(5);
		} finally { cleanup.stop(); }
	});
	it('follows a coalesced pass that exhausted its budget, met another owner or timed out on a lock', async () => {
		vi.useFakeTimers();
		for (const [outcome, delay] of [['budget', 1], ['contended', 100], ['lock timeout', 100]]) {
			const { client, tx } = checkout({ acquired: outcome !== 'contended', remove: 250 });
			let passes = 0;
			const work = vi.fn(async budget => {
				passes++;
				if (passes > 1) return;
				if (outcome === 'budget') while (budget.available) await budget.remove('DELETE');
				if (outcome === 'lock timeout') throw Object.assign(new Error('canceling statement due to lock timeout'), { code: '55P03' });
			});
			const failure = vi.fn();
			const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'dead-letter', interval: 0, coalesce: true, ready: async () => {}, work, failure });
			try {
				await cleanup.run();
				if (outcome === 'contended') tx.query.mockImplementation(async ({ text }) => ({ rows: text.includes('advisory') ? [{ acquired: true }] : [], rowCount: 0 }));
				const before = client.connect.mock.calls.length;
				await vi.advanceTimersByTimeAsync(delay - 1);
				expect(client.connect, outcome).toHaveBeenCalledTimes(before);
				await vi.advanceTimersByTimeAsync(1);
				expect(client.connect, outcome).toHaveBeenCalledTimes(before + 1);
				// The follow-up completed with nothing left, so the chain ends.
				await vi.advanceTimersByTimeAsync(1000);
				expect(client.connect, outcome).toHaveBeenCalledTimes(before + 1);
				expect(vi.getTimerCount(), outcome).toBe(0);
			} finally { cleanup.stop(); }
		}
	});
	it('backs off retries while another owner keeps the pass, up to a cap, and resets after a pass runs', async () => {
		vi.useFakeTimers();
		const { client, tx } = checkout({ acquired: false });
		const at = [];
		const connect = client.connect;
		client.connect = vi.fn(async () => { at.push(Date.now()); return connect(); });
		const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'dead-letter', interval: 0, coalesce: true, ready: async () => {}, work: async () => {} });
		try {
			await cleanup.run();
			await vi.advanceTimersByTimeAsync(100 + 200 + 400 + 800 + 1600 + 3200 + 6400 * 3);
			expect(at.slice(1).map((time, index) => time - at[index])).toEqual([100, 200, 400, 800, 1600, 3200, 6400, 6400, 6400]);
			// A pass that runs ends the chain and resets the delay.
			tx.query.mockImplementation(async ({ text }) => ({ rows: text.includes('advisory') ? [{ acquired: true }] : [], rowCount: 0 }));
			await vi.advanceTimersByTimeAsync(6400);
			expect(vi.getTimerCount()).toBe(0);
			tx.query.mockImplementation(async ({ text }) => ({ rows: text.includes('advisory') ? [{ acquired: false }] : [], rowCount: 0 }));
			at.length = 0;
			await cleanup.run();
			await vi.advanceTimersByTimeAsync(100);
			expect(at.slice(1).map((time, index) => time - at[index])).toEqual([100]);
		} finally { cleanup.stop(); }
	});
	it('does not follow a pass of a store that does not coalesce', async () => {
		vi.useFakeTimers();
		const { client } = checkout();
		const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'idempotency', interval: 0, ready: async () => {}, work: async budget => { while (budget.available) await budget.remove('DELETE'); } });
		try {
			await cleanup.run();
			await vi.advanceTimersByTimeAsync(1000);
			expect(client.connect).toHaveBeenCalledTimes(1);
		} finally { cleanup.stop(); }
	});
	it('the public idempotency store retains live entries while draining expired rows over multiple bounded ticks', async () => {
		let tick; setRuntimeEnv({ rng: { float: () => 1 }, timers: { set: cb => { tick = cb; return {}; }, clear: () => {} } });
		const client = mockPgClient(); const store = createIdempotencyStore(client, { cleanupInterval: 1000 }); await store.ready();
		for (let i = 0; i < 1500; i++) client._getIdemRows().set('expired-' + i, { expires_at: 0 });
		client._getIdemRows().set('live', { expires_at: Infinity });
		try { await tick(); expect(client._getIdemRows().size).toBe(501); await tick(); expect([...client._getIdemRows().keys()]).toEqual(['live']); }
		finally { store.destroy(); }
	});
	it('does not reset breaker health when another maintenance checkout is still pending', async () => {
		let resolve; const pending = new Promise(r => { resolve = r; });
		const { tx } = checkout(); const client = { connect: () => pending }; const success = vi.fn();
		setRuntimeEnv({ timers: { set: () => ({}), clear: () => {} } });
		const options = { client, table: 'cache', kind: 'idempotency', interval: 1000, ready: async () => {}, work: async () => {}, success };
		const first = scheduleCleanup(options), second = scheduleCleanup(options);
		try {
			const active = first.tick(); await Promise.resolve(); await second.tick();
			expect(success).not.toHaveBeenCalled(); resolve(tx); await active; expect(success).toHaveBeenCalledTimes(1);
		} finally { first.stop(); second.stop(); }
	});

	it('separates slow cleanup durations above one second', async () => {
		let clock = 0, elapsed = 1500;
		setRuntimeEnv({ clock: { monotonic: () => clock } });
		const { client } = checkout(), metrics = createMetrics();
		const cleanup = scheduleCleanup({ client, table: 'cache', kind: 'idempotency', interval: 0, metrics,
			ready: async () => { clock += elapsed; }, work: async () => {} });
		try {
			await cleanup.run(); elapsed = 6000; await cleanup.run();
			const text = metrics.serialize();
			expect(text).toContain('postgres_cleanup_duration_seconds_bucket{store="idempotency",le="1"} 0');
			expect(text).toContain('postgres_cleanup_duration_seconds_bucket{store="idempotency",le="2.5"} 1');
			expect(text).toContain('postgres_cleanup_duration_seconds_bucket{store="idempotency",le="10"} 2');
			expect(text).toContain('postgres_cleanup_duration_seconds_bucket{store="idempotency",le="60"} 2');
		} finally { cleanup.stop(); }
	});

});
