import { describe, it, expect, vi } from 'vitest';

// Replace the `Worker` constructor with a controllable mock that lets the
// test drive what the "worker" postMessages back to the parent. The mock
// exposes `_fireMessage(msg)` so a test can simulate a worker emitting
// arbitrary content, including content claiming to be a result for an id
// that was actually dispatched to a different worker. Real Worker threads
// behave correctly (the harness only posts results for ids it received);
// the parent-side defense is for the case where the handler file is
// compromised, misbehaving, or a future protocol change introduces a new
// message shape.
//
// Implementation note: the mock factory deliberately avoids importing
// other modules (EventEmitter, etc.) because vi.mock factories are
// hoisted to before any module imports run. Using a minimal per-event
// listener array is the simplest way to stay self-contained.
vi.mock('node:worker_threads', () => {
	const instances = [];
	class MockWorker {
		constructor() {
			this._listeners = { message: [], error: [], exit: [] };
			this.terminated = false;
			this.dispatched = [];
			instances.push(this);
		}
		on(event, listener) {
			if (this._listeners[event]) this._listeners[event].push(listener);
			return this;
		}
		postMessage(msg) {
			this.dispatched.push(msg);
		}
		terminate() {
			this.terminated = true;
			for (const fn of this._listeners.exit) fn(0);
			return Promise.resolve();
		}
		unref() {}
		_fireMessage(msg) {
			for (const fn of this._listeners.message) fn(msg);
		}
	}
	MockWorker.instances = instances;
	return { Worker: MockWorker };
});

const { createWorkerPool } = await import('../../postgres/_tasks-worker-pool.js');
const wt = await import('node:worker_threads');
const MockWorker = /** @type {any} */ (wt.Worker);

function freshPool(size = 2) {
	MockWorker.instances.length = 0;
	// `file:` URL is required by createWorkerPool's scheme guard; the mock
	// Worker never actually imports this path, so any file: URL works.
	return createWorkerPool({ path: 'file:///mock/handler.js', pool: { size, idleTimeout: 0 } }, 'test');
}

describe('worker pool IPC origin check', () => {
	it('drops a message from worker A that claims to resolve a task dispatched to worker B', async () => {
		const pool = freshPool(2);

		// Dispatch two tasks in parallel. Each spawns a fresh worker.
		const pA = pool.run({ input: 'A', idempotencyKey: 'a', fence: 'fA', attempt: 1, signal: null });
		const pB = pool.run({ input: 'B', idempotencyKey: 'b', fence: 'fB', attempt: 1, signal: null });

		await new Promise((r) => setImmediate(r));

		expect(MockWorker.instances).toHaveLength(2);
		const [wA, wB] = MockWorker.instances;
		const idA = wA.dispatched[0].id;
		const idB = wB.dispatched[0].id;
		expect(idA).not.toBe(idB);

		// Compromised worker A fires a forged result for the id that was
		// actually dispatched to worker B. Pre-fix, this would resolve
		// pB with the poison value and the eventual real result from B
		// would be silently dropped.
		wA._fireMessage({ type: 'result', id: idB, result: { hijacked: true } });

		// Now worker B emits its legitimate result. This must be the
		// value pB resolves to, not the forged one.
		wB._fireMessage({ type: 'result', id: idB, result: { genuine: true } });

		// Worker A also emits its own legitimate result.
		wA._fireMessage({ type: 'result', id: idA, result: { from: 'A' } });

		const [rA, rB] = await Promise.all([pA, pB]);
		expect(rB).toEqual({ genuine: true });
		expect(rA).toEqual({ from: 'A' });
	});

	it('drops a forged error event from worker A targeting worker B\'s id', async () => {
		const pool = freshPool(2);
		const pA = pool.run({ input: 'A', idempotencyKey: 'a', fence: 'fA', attempt: 1, signal: null });
		const pB = pool.run({ input: 'B', idempotencyKey: 'b', fence: 'fB', attempt: 1, signal: null });
		await new Promise((r) => setImmediate(r));

		const [wA, wB] = MockWorker.instances;
		const idB = wB.dispatched[0].id;

		// A fires a forged error claiming to reject B's task.
		wA._fireMessage({ type: 'error', id: idB, error: { message: 'forged' } });

		// B fires its real result.
		wB._fireMessage({ type: 'result', id: idB, result: 'okB' });
		wA._fireMessage({ type: 'result', id: wA.dispatched[0].id, result: 'okA' });

		await expect(pB).resolves.toBe('okB');
		await expect(pA).resolves.toBe('okA');
	});

	it('still drops messages for unknown ids (pre-existing if-not-slot guard)', async () => {
		const pool = freshPool(1);
		const pA = pool.run({ input: 'A', idempotencyKey: 'a', fence: 'fA', attempt: 1, signal: null });
		await new Promise((r) => setImmediate(r));
		const [wA] = MockWorker.instances;
		const idA = wA.dispatched[0].id;

		// Message for an unknown id is silently dropped (slot is undefined).
		expect(() => wA._fireMessage({ type: 'result', id: 'unknown', result: 'x' })).not.toThrow();
		// The real task still resolves correctly.
		wA._fireMessage({ type: 'result', id: idA, result: 'okA' });
		await expect(pA).resolves.toBe('okA');
	});
});

describe('worker pool path scheme guard', () => {
	it('rejects a data: URL at registration time', () => {
		expect(() => createWorkerPool(
			{ path: 'data:text/javascript,export default function(){}', pool: { size: 1 } },
			'malicious'
		)).toThrow(/must be a file: URL/);
	});

	it('rejects an http: URL at registration time', () => {
		expect(() => createWorkerPool(
			{ path: 'http://attacker.example/handler.js', pool: { size: 1 } },
			'malicious'
		)).toThrow(/must be a file: URL/);
	});

	it('rejects a bare relative-path string', () => {
		expect(() => createWorkerPool(
			{ path: './handler.js', pool: { size: 1 } },
			'mistake'
		)).toThrow(/must be a file: URL/);
	});

	it('accepts a file: URL string', () => {
		// Construction succeeds; the mock Worker never imports the path.
		expect(() => createWorkerPool(
			{ path: 'file:///abs/handler.js', pool: { size: 1, idleTimeout: 0 } },
			'ok'
		)).not.toThrow();
	});

	it('accepts a URL instance with file: protocol', () => {
		expect(() => createWorkerPool(
			{ path: new URL('file:///abs/handler.js'), pool: { size: 1, idleTimeout: 0 } },
			'ok'
		)).not.toThrow();
	});

	it('rejects a URL instance with data: protocol', () => {
		expect(() => createWorkerPool(
			{ path: new URL('data:text/javascript,export default function(){}'), pool: { size: 1 } },
			'malicious'
		)).toThrow(/must be a file: URL/);
	});
});
