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

});
