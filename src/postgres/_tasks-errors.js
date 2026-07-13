/**
 * Internal task-runner errors and structured serialisation helpers.
 *
 * `TaskInFlightError` and `UnknownTaskError` are re-exported from `./tasks.js`
 * as part of the public API; consumers should import them from there.
 *
 * @module svelte-adapter-uws-extensions/postgres/_tasks-errors
 */

/**
 * Thrown when a task is acquired by another worker (idempotency store
 * reports the slot as pending). Caller may retry after a backoff.
 */
export class TaskInFlightError extends Error {
	constructor(idempotencyKey) {
		super(`task in flight for idempotency key "${idempotencyKey}"`);
		this.name = 'TaskInFlightError';
		this.idempotencyKey = idempotencyKey;
	}
}

/**
 * Thrown when run() is called for an unregistered task name. The recovery
 * loop logs but does not throw on unknown names (the handler may live on a
 * different deployment).
 */
export class UnknownTaskError extends Error {
	constructor(name) {
		super(`no handler registered for task "${name}"`);
		this.name = 'UnknownTaskError';
		this.taskName = name;
	}
}

/**
 * Thrown by `run()` when this worker's fence was superseded by another worker
 * before it could record its outcome, and the canonical terminal result did
 * not become durable within `awaitTimeout`. The task is NOT lost - the winning
 * worker owns it - but this caller cannot report a canonical value, so it
 * throws rather than returning its own stale local attempt (which could differ
 * from the durable row and, via the idempotency store, be cached). Retry, or
 * `await(taskId)`, to read the canonical outcome once it lands.
 *
 * Stable contract: `err.code === 'TASK_FENCE_LOST'`.
 */
export class TaskFenceLostError extends Error {
	constructor(taskId, taskName, lastStatus) {
		super(`task "${taskId}" (${taskName}) was fenced out by another worker and its canonical outcome did not become terminal within awaitTimeout (last status="${lastStatus}")`);
		this.name = 'TaskFenceLostError';
		this.code = 'TASK_FENCE_LOST';
		this.taskId = taskId;
		this.taskName = taskName;
	}
}

/**
 * Convert an Error (or anything else) into a JSON-safe shape for storage
 * in the tasks table or transport across a worker-thread boundary.
 *
 * @param {unknown} err
 * @returns {{ name: string, message: string, stack?: string, code?: unknown, cause?: unknown }}
 */
export function serialiseError(err) {
	if (!err || typeof err !== 'object') {
		return { message: String(err), name: 'Error' };
	}
	const out = {
		name: err.name || 'Error',
		message: err.message || String(err)
	};
	if (err.stack) out.stack = err.stack;
	if (err.cause !== undefined) {
		try {
			out.cause = err.cause instanceof Error ? serialiseError(err.cause) : err.cause;
		} catch { /* skip un-serialisable cause */ }
	}
	if (err.code !== undefined) out.code = err.code;
	return out;
}

/**
 * Reverse of `serialiseError`. Reconstructs an Error from the stored shape.
 *
 * @param {unknown} payload
 * @returns {Error}
 */
export function deserialiseError(payload) {
	if (!payload || typeof payload !== 'object') {
		return new Error(String(payload));
	}
	const err = new Error(payload.message);
	err.name = payload.name || 'Error';
	if (payload.stack) err.stack = payload.stack;
	if (payload.cause !== undefined) err.cause = payload.cause;
	if (payload.code !== undefined) err.code = payload.code;
	return err;
}
