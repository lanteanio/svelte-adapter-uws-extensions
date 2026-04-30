/**
 * Worker-thread harness for createTaskRunner.
 *
 * Loaded as the entry point of every worker thread spawned by a task
 * registered with the `worker` option. Responsibilities:
 *
 *   - Import the user's handler file (the file whose default export is
 *     the handler) once at startup.
 *   - For each `run` message from the main thread, build a fresh
 *     AbortController, invoke the handler with the standard context,
 *     and post back either `result` or `error`.
 *   - For each `abort` message, fire the AbortController of the matching
 *     in-flight run.
 *
 * This file is internal. The leading underscore in the filename is a
 * signal to consumers that they should not import it directly. It is
 * shipped in the package because the main module spawns workers from its
 * URL.
 *
 * @module svelte-adapter-uws-extensions/postgres/_worker-harness
 */

import { parentPort, workerData } from 'node:worker_threads';

if (!parentPort) {
	throw new Error('worker harness: must be loaded as a worker thread entry');
}
if (!workerData || typeof workerData.handlerPath !== 'string') {
	throw new Error('worker harness: workerData.handlerPath is required');
}

const handlerPromise = (async () => {
	const mod = await import(workerData.handlerPath);
	const fn = mod.default;
	if (typeof fn !== 'function') {
		throw new Error(`worker harness: ${workerData.handlerPath} must default-export a function`);
	}
	return fn;
})();

/** @type {Map<string, AbortController>} */
const controllers = new Map();

function serialiseError(err) {
	if (!err || typeof err !== 'object') {
		return { message: String(err), name: 'Error' };
	}
	const out = {
		name: err.name || 'Error',
		message: err.message || String(err)
	};
	if (err.stack) out.stack = err.stack;
	if (err.code !== undefined) out.code = err.code;
	if (err.cause !== undefined) {
		try {
			out.cause = err.cause instanceof Error ? serialiseError(err.cause) : err.cause;
		} catch { /* skip un-serialisable cause */ }
	}
	return out;
}

parentPort.on('message', async (msg) => {
	if (msg && msg.type === 'run') {
		const id = msg.id;
		const controller = new AbortController();
		controllers.set(id, controller);

		let handler;
		try {
			handler = await handlerPromise;
		} catch (err) {
			controllers.delete(id);
			parentPort.postMessage({ type: 'error', id, error: serialiseError(err) });
			return;
		}

		try {
			const result = await handler({
				input: msg.input,
				idempotencyKey: msg.idempotencyKey,
				fence: msg.fence,
				signal: controller.signal,
				attempt: msg.attempt
			});
			parentPort.postMessage({ type: 'result', id, result });
		} catch (err) {
			parentPort.postMessage({ type: 'error', id, error: serialiseError(err) });
		} finally {
			controllers.delete(id);
		}
		return;
	}

	if (msg && msg.type === 'abort') {
		const c = controllers.get(msg.id);
		if (c) c.abort(new Error('fence lost'));
		return;
	}
});
