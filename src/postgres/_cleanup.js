import { clearTimer, monotonicNow, randomFloat, setTimer } from '../shared/runtime.js';

const ROW_BUDGET = 1000;
const PASS_MS = 1000;
const BATCH_SIZE = 250;
const pendingCheckouts = new WeakSet();

// Only retention work uses this transaction namespace. The table OID resolves
// through the same search_path as its DELETE, including custom table names.
export async function runCleanupPass(client, table, work, stopped = () => false) {
	if (pendingCheckouts.has(client)) return { result: 'waiting', rows: 0 };
	const controller = new AbortController();
	const timeout = setTimer(() => controller.abort(), PASS_MS);
	timeout?.unref?.();
	let tx;
	let failure;
	const onError = (error) => { failure ??= error; };
	try {
		// Keep the eventual underlying checkout owned even after our deadline.
		// A never-settling pool cannot accumulate repeated cleanup waiters.
		tx = await new Promise((resolve, reject) => {
			let settled = false;
			const abort = () => { settled = true; reject(new Error('postgres cleanup: checkout deadline exceeded')); };
			controller.signal.addEventListener('abort', abort, { once: true });
			pendingCheckouts.add(client);
			let pending;
			try { pending = typeof client.connect === 'function' ? client.connect() : client.pool.connect(); }
			catch (error) { pending = Promise.reject(error); }
			Promise.resolve(pending).then(connection => {
				pendingCheckouts.delete(client);
				controller.signal.removeEventListener('abort', abort);
				if (settled) { try { connection.release(); } catch { /* expired checkout cannot be reused here */ } return; }
				settled = true; resolve(connection);
			}, error => {
				pendingCheckouts.delete(client);
				controller.signal.removeEventListener('abort', abort);
				if (!settled) { settled = true; reject(error); }
			});
		});
		tx.on?.('error', onError);
		clearTimer(timeout);
		if (stopped()) return { result: 'stopped', rows: 0 };
		const deadline = monotonicNow() + PASS_MS;
		const workDeadline = deadline - 100;
		const query = (text, values) => tx.query({ text, values, query_timeout: Math.max(1, Math.ceil(deadline - monotonicNow())) });
		await query('BEGIN');
		await query("SELECT set_config('statement_timeout', '1000', true), set_config('lock_timeout', '100', true)");
		const lock = await query('SELECT pg_try_advisory_xact_lock(1937134697, $1::regclass::oid::integer) AS acquired', [table]);
		if (!lock.rows[0]?.acquired) {
			await query('COMMIT');
			return { result: 'contended', rows: 0 };
		}
		let rows = 0;
		let queries = 0;
		const budget = {
			get available() { return !stopped() && rows < ROW_BUDGET && queries < 128 && monotonicNow() < workDeadline; },
			get limit() { return Math.min(BATCH_SIZE, ROW_BUDGET - rows); },
			async query(text, values) {
				queries++;
				const remaining = Math.max(1, Math.ceil(workDeadline - monotonicNow()));
				await query("SELECT set_config('statement_timeout', $1, true)", [String(remaining)]);
				return query(text, values);
			},
			async remove(text, values) {
				const result = await this.query(text, values);
				rows += result.rowCount || 0;
				return result.rowCount || 0;
			}
		};
		await work(budget);
		if (failure) throw failure;
		const exhausted = !budget.available;
		await query('COMMIT');
		return { result: exhausted ? 'budget' : 'complete', rows };
	} catch (error) {
		failure = error;
		throw error;
	} finally {
		clearTimer(timeout);
		// A query timeout can precede server cancellation. Destroy the checkout
		// on every failure so its transaction and advisory lock cannot be reused.
		tx?.removeListener?.('error', onError);
		tx?.release(failure);
	}
}

export function scheduleCleanup({ client, table, kind, interval, metrics, ready, healthy, success, failure, work, coalesce = false }) {
	if (!Number.isInteger(interval) || interval < 0 || interval > 2147483647) {
		throw new Error('postgres cleanup: cleanupInterval must be an integer between 0 and 2147483647');
	}
	const runs = metrics?.counter('postgres_cleanup_runs_total', 'PostgreSQL retention passes by outcome', ['store', 'result']);
	const removed = metrics?.counter('postgres_cleanup_rows_total', 'Rows committed by PostgreSQL retention cleanup', ['store']);
	const duration = metrics?.histogram('postgres_cleanup_duration_seconds', 'PostgreSQL retention pass duration in seconds', ['store']);
	let stopped = false;
	let timer;
	let trailingTimer;
	let trailingRequested = false;
	let running = false;
	function schedule() {
		if (stopped || interval === 0) return;
		timer = setTimer(tick, Math.max(1, Math.ceil(randomFloat() * interval)));
		timer?.unref?.();
	}
	async function run(trailing = false) {
		if (stopped) return;
		if (running) {
			if (coalesce) trailingRequested = true;
			return;
		}
		// A new leading pass also covers writes awaiting the queued pass.
		if (!trailing) { clearTimer(trailingTimer); trailingTimer = undefined; }
		running = true;
		const start = monotonicNow();
		try {
			if (healthy && !healthy()) return;
			await ready();
			if (stopped) return;
			const result = await runCleanupPass(client, table, work, () => stopped);
			runs?.inc({ store: kind, result: result.result });
			if (result.rows) removed?.inc({ store: kind }, result.rows);
			if (result.result !== 'waiting' && result.result !== 'stopped') success?.();
		} catch (error) {
			runs?.inc({ store: kind, result: 'error' });
			failure?.(error);
		} finally {
			duration?.observe({ store: kind }, (monotonicNow() - start) / 1000);
			running = false;
			// A leading pass may queue one extra bounded pass. Writes during that
			// trailing pass cannot extend the chain or the original caller's wait.
			if (!stopped && !trailing && trailingRequested) {
				trailingTimer = setTimer(() => { trailingTimer = undefined; return run(true); }, 1);
				trailingTimer?.unref?.();
			}
			trailingRequested = false;
		}
	}
	async function tick() {
		await run();
		schedule();
	}
	schedule();
	return { tick, run, stop() { stopped = true; clearTimer(timer); clearTimer(trailingTimer); } };
}
