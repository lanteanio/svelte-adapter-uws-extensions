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

import { randomUUID } from 'node:crypto';
import { Worker } from 'node:worker_threads';
import { deserialiseError } from './_tasks-errors.js';

const HARNESS_URL = new URL('./_worker-harness.js', import.meta.url);

/**
 * @param {{ path: URL | string, pool?: { size?: number, idleTimeout?: number } }} workerOption
 * @param {string} taskName
 */
export function createWorkerPool(workerOption, taskName) {
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

	/** @type {Set<Worker>} */
	const idle = new Set();
	/** @type {Map<string, { worker: Worker, resolve: Function, reject: Function, ctx: any }>} */
	const inFlight = new Map();
	/** @type {Array<{ id: string, ctx: any, resolve: Function, reject: Function }>} */
	const queue = [];
	/** @type {WeakMap<Worker, NodeJS.Timeout>} */
	const idleTimers = new WeakMap();
	let workerCount = 0;
	let destroyed = false;

	function clearIdleTimer(worker) {
		const t = idleTimers.get(worker);
		if (t) {
			clearTimeout(t);
			idleTimers.delete(worker);
		}
	}

	function scheduleIdleTimeout(worker) {
		if (idleTimeout === 0) return;
		const t = setTimeout(() => {
			if (idle.has(worker)) {
				idle.delete(worker);
				workerCount -= 1;
				worker.terminate().catch(() => {});
			}
		}, idleTimeout);
		if (t.unref) t.unref();
		idleTimers.set(worker, t);
	}

	function dispatch(worker, job) {
		clearIdleTimer(worker);
		idle.delete(worker);
		inFlight.set(job.id, { worker, resolve: job.resolve, reject: job.reject, ctx: job.ctx });
		worker.postMessage({
			type: 'run',
			id: job.id,
			input: job.ctx.input,
			idempotencyKey: job.ctx.idempotencyKey,
			fence: job.ctx.fence,
			attempt: job.ctx.attempt
		});
	}

	function spawnWorker() {
		const w = new Worker(HARNESS_URL, {
			workerData: { handlerPath }
		});
		workerCount += 1;
		if (w.unref) w.unref();
		w.on('message', (msg) => {
			const slot = inFlight.get(msg.id);
			if (!slot) return;
			// Reject messages that reference an in-flight id this worker
			// was not dispatched to. The worker IS user code (handler
			// path is user-supplied); a misbehaving or compromised
			// handler that postMessages a forged result for another
			// worker's id could otherwise hijack that task's resolve()
			// path. Mirrors the existing slot.worker === w check on the
			// 'error' listener below.
			if (slot.worker !== w) return;
			inFlight.delete(msg.id);
			if (msg.type === 'result') {
				slot.resolve(msg.result);
			} else if (msg.type === 'error') {
				slot.reject(deserialiseError(msg.error));
			}
			const next = queue.shift();
			if (next) {
				dispatch(w, next);
			} else {
				idle.add(w);
				scheduleIdleTimeout(w);
			}
		});
		w.on('error', (err) => {
			for (const [id, slot] of inFlight) {
				if (slot.worker === w) {
					inFlight.delete(id);
					slot.reject(err);
				}
			}
			idle.delete(w);
			clearIdleTimer(w);
			workerCount -= 1;
		});
		w.on('exit', () => {
			idle.delete(w);
			clearIdleTimer(w);
		});
		return w;
	}

	return {
		run(ctx) {
			if (destroyed) {
				return Promise.reject(new Error('task runner destroyed'));
			}
			return new Promise((resolve, reject) => {
				const id = randomUUID();
				const job = { id, ctx, resolve, reject };

				const onAbort = () => {
					const slot = inFlight.get(id);
					if (slot) slot.worker.postMessage({ type: 'abort', id });
				};
				const cleanup = () => {
					if (ctx.signal && !ctx.signal.aborted) {
						ctx.signal.removeEventListener('abort', onAbort);
					}
				};
				const wrappedResolve = (v) => { cleanup(); resolve(v); };
				const wrappedReject = (e) => { cleanup(); reject(e); };
				job.resolve = wrappedResolve;
				job.reject = wrappedReject;

				if (ctx.signal) {
					if (ctx.signal.aborted) {
						wrappedReject(ctx.signal.reason || new Error('aborted'));
						return;
					}
					ctx.signal.addEventListener('abort', onAbort, { once: true });
				}

				let w = null;
				for (const candidate of idle) { w = candidate; break; }
				if (w) {
					dispatch(w, job);
				} else if (workerCount < size) {
					dispatch(spawnWorker(), job);
				} else {
					queue.push(job);
				}
			});
		},

		destroy() {
			destroyed = true;
			for (const slot of inFlight.values()) {
				slot.reject(new Error('task runner destroyed'));
			}
			inFlight.clear();
			for (const job of queue) {
				job.reject(new Error('task runner destroyed'));
			}
			queue.length = 0;
			for (const w of idle) {
				clearIdleTimer(w);
				w.terminate().catch(() => {});
			}
			idle.clear();
		}
	};
}
