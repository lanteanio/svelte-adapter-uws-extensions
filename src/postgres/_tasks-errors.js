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
export function serialiseError(err, opts) {
	if (!err || typeof err !== 'object') {
		return { message: String(err), name: 'Error' };
	}
	const out = {
		name: err.name || 'Error',
		message: err.message || String(err)
	};
	if (err.stack) out.stack = err.stack;
	// cause is opt-in: handlers routinely attach config-bearing
	// causes (connection errors carry DSNs/credentials) and the persisted
	// row is re-served to dashboards via await()/list().
	if (opts && opts.includeCause === true && err.cause !== undefined) {
		try {
			out.cause = err.cause instanceof Error ? serialiseError(err.cause, opts) : err.cause;
		} catch { /* skip un-serialisable cause */ }
	}
	if (err.code !== undefined) out.code = err.code;
	return out;
}

/**
 * Read one property once, and only accept it if it is already a string.
 *
 * Reading twice - a `typeof` check and then a `.slice()` - is not the same
 * as reading once: an accessor is free to return a string to the check and
 * an object with its own `slice` to the use, which puts an arbitrary value
 * back into a shape whose whole purpose is that it cannot fail to encode.
 *
 * @param {any} obj
 * @param {string} key
 * @param {number} max
 * @returns {string | undefined}
 */
function readString(obj, key, max) {
	let raw;
	try { raw = obj[key]; } catch { return undefined; }
	if (typeof raw !== 'string') return undefined;
	return raw.length > max ? raw.slice(0, max) : raw;
}

/**
 * Last-resort shape for an error that cannot be serialised at all.
 * Every field is a primitive read once and coerced, so the result is
 * always JSON-encodable.
 *
 * @param {unknown} err
 * @returns {Record<string, any>}
 */
export function unserialisableErrorShape(err) {
	/** @type {Record<string, any>} */
	const out = { name: 'Error', message: 'error could not be serialised', unserialisable: true };
	if (err && typeof err === 'object') {
		const name = readString(err, 'name', 256);
		if (name !== undefined) out.name = name;
		const message = readString(err, 'message', 2048);
		if (message !== undefined) out.message = message;
		let code;
		try { code = /** @type {any} */ (err).code; } catch { code = undefined; }
		const t = typeof code;
		if (t === 'string') out.code = code.length > 256 ? code.slice(0, 256) : code;
		else if (t === 'number' || t === 'boolean') out.code = code;
		else if (t === 'bigint') out.code = String(code);
	}
	return out;
}

/**
 * Total wrapper around `serialiseError`: never throws, for any input.
 *
 * `serialiseError` reads `name`, `message`, `stack`, `code` and `cause`
 * straight off the error, and any of those can be an accessor that throws -
 * a source-map or APM hook installing its own `stack` getter is the ordinary
 * way it happens. BOTH surfaces that re-serve a handler error go through
 * here, because a throw on either one is silent: on the persistence path it
 * strands the row, and on the event path it skips the terminal transition
 * and the failure counter while replacing the caller's error with the
 * getter's.
 *
 * @param {unknown} err
 * @param {{ includeCause?: boolean }} [opts]
 * @returns {Record<string, any>}
 */
export function serialiseErrorSafe(err, opts) {
	let out;
	try {
		out = serialiseError(err, opts);
	} catch {
		return unserialisableErrorShape(err);
	}
	// Not throwing is not enough. `serialiseError` can SUCCEED and still hand
	// back an object JSON cannot encode - `code` is copied verbatim, and so is
	// a non-Error `cause` when opted in - and every consumer of this shape
	// encodes it: the row stringifies it, and a listener that JSON-logs the
	// state-change event throws inside a callback whose errors are swallowed,
	// so the failure event vanishes with only "listener threw" in its place.
	try {
		JSON.stringify(out);
	} catch {
		return unserialisableErrorShape(err);
	}
	return out;
}

/**
 * Serialise for the in-process worker_threads transport.
 *
 * This is a different problem from the persisted row. Nothing here is
 * stored or re-served to a dashboard, so the reason `cause` is withheld on
 * the persistence path does not apply: dropping it across the thread
 * boundary only breaks the caller's own `retry.on(err => err.cause...)`
 * predicate, and does so silently, on the one code path where the runner's
 * `serializeErrorCause` option cannot reach.
 *
 * Every field is made structured-clone-safe, not just `cause`. A handler can
 * attach anything anywhere, and an uncloneable value makes `postMessage`
 * throw inside the harness - turning a task failure into a LOST REPLY, which
 * the pool can only resolve by timing out. `code` is copied verbatim by
 * `serialiseError` and is just as able to hold a function or a symbol as
 * `cause` is, and reading the error at all can throw, so the whole build goes
 * through the total path first.
 *
 * @param {unknown} err
 * @returns {{ name: string, message: string, stack?: string, code?: unknown, cause?: unknown }}
 */
export function serialiseErrorForTransport(err) {
	const out = serialiseErrorSafe(err, { includeCause: true });
	// Checked as a WHOLE, not field by field. `name`, `message` and `stack`
	// are copied verbatim by `serialiseError` and are just as able to hold an
	// object as `code` and `cause` are - and an object carrying a `toJSON`
	// passes a JSON round-trip while still failing structuredClone. Any single
	// survivor makes `postMessage` throw INSIDE the harness, which never
	// surfaces as a task failure: it loses the reply, and the pool can only
	// resolve that by timing out.
	if (isCloneable(out)) return out;

	/** @type {Record<string, any>} */
	const repaired = { name: forceString(out.name, 'Error'), message: forceString(out.message, '') };
	if (typeof out.stack === 'string') repaired.stack = out.stack;
	for (const key of ['code', 'cause']) {
		const v = out[key];
		if (v === undefined) continue;
		if (isCloneable(v)) { repaired[key] = v; continue; }
		let coerced;
		try { coerced = JSON.parse(JSON.stringify(v)); } catch { coerced = undefined; }
		if (coerced !== undefined && isCloneable(coerced)) { repaired[key] = coerced; continue; }
		// Keep an identifying trace of a `code`; a `cause` has no compact
		// stand-in worth inventing.
		if (key === 'code') {
			const s = forceString(v, '');
			if (s) repaired.code = s.slice(0, 256);
		}
	}
	return isCloneable(repaired)
		? repaired
		: { name: 'Error', message: 'error could not be transported across the worker boundary' };
}

/**
 * @param {unknown} v
 * @returns {boolean}
 */
function isCloneable(v) {
	try { structuredClone(v); return true; } catch { return false; }
}

/**
 * @param {unknown} v
 * @param {string} fallback
 * @returns {string}
 */
function forceString(v, fallback) {
	if (typeof v === 'string') return v;
	if (v === undefined || v === null) return fallback;
	try { return String(v); } catch { return fallback; }
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
