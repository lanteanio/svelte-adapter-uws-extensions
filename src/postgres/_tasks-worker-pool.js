/**
 * Worker-thread pool for tasks registered with the `worker` option.
 *
 * Workers are spawned on demand up to `size`; each handles one run at a
 * time. When a worker has been idle for `idleTimeout` ms it is terminated
 * and replaced on the next run. `AbortSignal` aborts are forwarded as
 * messages to the worker, which translates them back into a local
 * AbortController.signal that the user handler sees.
 *
 * @module svelte-adapter-uws-extensions/postgres/_tasks-worker-pool
 */

import { Worker } from 'node:worker_threads';
import { randomUuid, setTimer, clearTimer } from '../shared/runtime.js';
import { deserialiseError } from './_tasks-errors.js';
import { TimeoutError } from '../shared/errors.js';

const HARNESS_URL = new URL('./_worker-harness.js', import.meta.url);

/**
 * @param {{ path: URL | string, pool?: { size?: number, idleTimeout?: number } }} workerOption
 * @param {string} taskName
 */
export function createWorkerPool(workerOption, taskName, { metrics, maxPayloadBytes = 256 * 1024 } = {}) {
	const handlerPath = workerOption.path instanceof URL ? workerOption.path.href : String(workerOption.path);
	// Reject non-file URL schemes. Node's dynamic import() still accepts
	// `data:` URLs (which execute inline code) on the current LTS line;
	// `http:` / `https:` are blocked by Node, but blocking them explicitly
	// here surfaces a clearer error and is future-proof if Node ever
	// re-enables network imports. Any string we accept must point at a
	// file the developer wrote and intends to load - paths derived from
	// user input (cookies, query strings, header values, message bodies)
	// have no business reaching this option.
	if (!handlerPath.startsWith('file://')) {
		throw new Error(
			`postgres tasks: worker.path for "${taskName}" must be a file: URL ` +
			`(typically \`new URL('./handler.js', import.meta.url)\` or the string returned ` +
			`by \`import.meta.resolve('./handler.js')\`). Received: ${JSON.stringify(handlerPath.slice(0, 80))}.`
		);
	}
	const poolCfg = workerOption.pool || {};
	const size = poolCfg.size !== undefined ? poolCfg.size : 1;
	const idleTimeout = poolCfg.idleTimeout !== undefined ? poolCfg.idleTimeout : 30000;

	if (!Number.isInteger(size) || size < 1) {
		throw new Error(`postgres tasks: worker.pool.size for "${taskName}" must be a positive integer`);
	}
	if (!Number.isInteger(idleTimeout) || idleTimeout < 0) {
		throw new Error(`postgres tasks: worker.pool.idleTimeout for "${taskName}" must be a non-negative integer`);
	}

	const maxJobs = size + 256;
	const maxBytes = Math.max(8 * 1024 * 1024, maxPayloadBytes);
	const queueTimeout = 30000;
	const mQueued = metrics?.gauge('tasks_worker_queue_jobs', 'Worker jobs waiting for a thread');
	const mBytes = metrics?.gauge('tasks_worker_reserved_bytes', 'Worker input bytes reserved by admission');
	const mActive = metrics?.gauge('tasks_worker_active', 'Worker jobs executing in threads');
	const mRejected = metrics?.counter('tasks_worker_rejected_total', 'Worker jobs refused or cancelled before execution', ['reason']);
	const workers = new Set();
	const idle = new Set();
	const inFlight = new Map();
	const queue = new Map();
	const permits = new Set();
	const idleTimers = new WeakMap();
	const retireWorkers = new WeakMap();
	const retirementErrors = new WeakMap();
	let reservedBytes = 0;
	let destroyed = false;

	function capacity(bytes) {
		return destroyed ? 0 : Math.max(0, Math.min(maxJobs - permits.size, Math.floor((maxBytes - reservedBytes) / Math.max(1, bytes))));
	}
	function reserve(bytes) {
		if (destroyed) throw new Error('task runner destroyed');
		if (permits.size >= maxJobs || bytes > maxBytes - reservedBytes) {
			mRejected?.inc({ reason: 'capacity' });
			throw new RangeError('postgres tasks: worker admission capacity exceeded');
		}
		const permit = { bytes, release() {
			if (!permits.delete(permit)) return;
			reservedBytes -= bytes;
			mBytes?.inc(-bytes);
		} };
		permits.add(permit);
		reservedBytes += bytes;
		mBytes?.inc(bytes);
		return permit;
	}
	function clearIdleTimer(worker) {
		const timer = idleTimers.get(worker);
		if (timer) { clearTimer(timer); idleTimers.delete(worker); }
	}
	function unqueue(job) {
		if (!queue.delete(job.id)) return false;
		clearTimer(job.timer);
		job.timer = null;
		mQueued?.inc(-1);
		return true;
	}
	function continueWorker(worker) {
		if (destroyed || !workers.has(worker) || retirementErrors.has(worker)) return;
		const next = queue.size ? queue.values().next().value : undefined;
		if (next) { unqueue(next); dispatch(worker, next); return; }
		idle.add(worker);
		if (idleTimeout === 0) return;
		const timer = setTimer(() => {
			if (!idle.delete(worker)) return;
			workers.delete(worker);
			idleTimers.delete(worker);
			worker.terminate().catch(() => {});
		}, idleTimeout);
		timer.unref?.();
		idleTimers.set(worker, timer);
	}
	function dispatch(worker, job) {
		clearIdleTimer(worker);
		idle.delete(worker);
		job.worker = worker;
		inFlight.set(job.id, job);
		mActive?.inc(1);
		try {
			worker.postMessage({ type: 'run', id: job.id, input: job.ctx.input,
				idempotencyKey: job.ctx.idempotencyKey, fence: job.ctx.fence, attempt: job.ctx.attempt });
		} catch (error) {
			inFlight.delete(job.id);
			mActive?.inc(-1);
			job.reject(error);
			continueWorker(worker);
		}
	}
	function spawnWorker() {
		const worker = new Worker(HARNESS_URL, { workerData: { handlerPath } });
		workers.add(worker);
		worker.unref?.();
		worker.on('message', message => {
			if (message?.type !== 'result' && message?.type !== 'error') return;
			const job = inFlight.get(message.id);
			if (!job || job.worker !== worker) return;
			inFlight.delete(message.id);
			mActive?.inc(-1);
			if (message.type === 'result') job.resolve(message.result);
			else job.reject(deserialiseError(message.error));
			continueWorker(worker);
		});
		function retire(error) {
			if (!workers.delete(worker)) return;
			idle.delete(worker);
			clearIdleTimer(worker);
			for (const [id, job] of inFlight) if (job.worker === worker) {
				inFlight.delete(id);
				mActive?.inc(-1);
				job.reject(error);
			}
			if (workers.size === 0) for (const job of queue.values()) {
				unqueue(job);
				job.reject(error);
			}
		}
		retireWorkers.set(worker, retire);
		worker.on('error', retire);
		worker.on('exit', () => retire(retirementErrors.get(worker) || new Error('postgres tasks: worker exited before completing its task')));
		return worker;
	}
	return {
		capacity,
		reserve,
		run(ctx, permit) {
			if (destroyed) return Promise.reject(new Error('task runner destroyed'));
			if (ctx.signal?.aborted) return Promise.reject(ctx.signal.reason || new Error('aborted'));
			const ownsPermit = !permit;
			try {
				permit ??= reserve(Buffer.byteLength(JSON.stringify(ctx.input ?? null)));
				if (!permits.has(permit)) throw new Error('postgres tasks: worker admission expired');
			} catch (error) { return Promise.reject(error); }
			return new Promise((resolve, reject) => {
				const id = randomUuid();
				const cleanup = () => { ctx.signal?.removeEventListener('abort', onAbort); if (ownsPermit) permit.release(); };
				const job = { id, ctx, timer: null, worker: null,
					resolve(value) { cleanup(); resolve(value); },
					reject(error) { cleanup(); reject(error); } };
				const onAbort = () => {
					if (unqueue(job)) {
						mRejected?.inc({ reason: 'abort' });
						job.reject(ctx.signal.reason || new Error('aborted'));
					} else {
						const active = inFlight.get(id);
						if (active) {
							try { active.worker.postMessage({ type: 'abort', id }); }
							catch (error) {
								const worker = active.worker;
								retirementErrors.set(worker, error);
								// Keep the active reservation until the thread has stopped.
								worker.terminate().then(() => retireWorkers.get(worker)(error), () => {});
							}
						}
					}
				};
				ctx.signal?.addEventListener('abort', onAbort, { once: true });
				try {
					const worker = idle.values().next().value;
					if (worker) dispatch(worker, job);
					else if (workers.size < size) dispatch(spawnWorker(), job);
					else {
						queue.set(id, job);
						mQueued?.inc(1);
						job.timer = setTimer(() => {
							if (!unqueue(job)) return;
							mRejected?.inc({ reason: 'timeout' });
							job.reject(new TimeoutError('postgres tasks', 'worker queue', queueTimeout));
						}, queueTimeout);
						job.timer.unref?.();
					}
				} catch (error) { job.reject(error); }
			});
		},
		destroy() {
			if (destroyed) return;
			destroyed = true;
			for (const job of inFlight.values()) { mActive?.inc(-1); job.reject(new Error('task runner destroyed')); }
			inFlight.clear();
			for (const job of queue.values()) { unqueue(job); job.reject(new Error('task runner destroyed')); }
			for (const worker of workers) { clearIdleTimer(worker); worker.terminate().catch(() => {}); }
			workers.clear();
			idle.clear();
			for (const permit of permits) permit.release();
		}
	};
}
