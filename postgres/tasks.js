/**
 * Postgres-backed durable task runner.
 *
 * Wraps an effectful operation in a state machine that survives process
 * crashes and naturally fans across cluster instances. Three guarantees:
 *
 *   1. Caller-retry idempotency: a stable `idempotencyKey` paired with an
 *      idempotency store ensures retries from the original caller return
 *      the cached result rather than re-executing.
 *   2. Worker-crash recovery: every attempt holds a fence (UUID + expiry).
 *      The conditional commit `UPDATE ... WHERE fence = $current` is
 *      atomic, so a stuck attempt that comes back from the dead cannot
 *      overwrite a completed attempt's result. A periodic recovery sweep
 *      reclaims rows whose fence has expired and re-drives the handler.
 *   3. External-service idempotency: the `idempotencyKey` is passed
 *      through to the handler, who forwards it to Stripe / SendGrid / S3
 *      so the side-effect target de-duplicates across retries too.
 *
 * Schema (auto-created):
 *
 *   svti_tasks (
 *     svti_tasks_id        UUID         PRIMARY KEY,
 *     name                 TEXT         NOT NULL,
 *     input                JSONB,
 *     svti_idempotency_key TEXT,
 *     status               TEXT         NOT NULL,  -- 'running' | 'committed' | 'failed'
 *     result               JSONB,
 *     error                JSONB,
 *     fence                UUID         NOT NULL,
 *     fence_expires_at     TIMESTAMPTZ  NOT NULL,
 *     attempts             INT          NOT NULL DEFAULT 1,
 *     created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
 *     updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
 *   )
 *
 * The fence and execution-context paths are factored behind seams so
 * a Redis fence provider (`createRedisFence` from the redis entry) or a
 * worker-thread execution context can be swapped in without touching
 * the state machine.
 *
 * @module svelte-adapter-uws-extensions/postgres/tasks
 */

import { randomUUID } from 'node:crypto';
import {
	TaskInFlightError,
	UnknownTaskError,
	deserialiseError
} from './_tasks-errors.js';
import { createWorkerPool } from './_tasks-worker-pool.js';
import { createTaskSql } from './_tasks-sql.js';
import { withBreaker } from '../shared/breaker.js';
import { MAX_TASK_HANDLERS } from '../shared/caps.js';

export { TaskInFlightError, UnknownTaskError };

/**
 * @typedef {Object} TaskRunnerOptions
 * @property {string} [table='svti_tasks'] - Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`.
 * @property {import('../redis/idempotency.js').RedisIdempotencyStore | import('./idempotency.js').PgIdempotencyStore} [idempotency] - Optional cache for committed results.
 * @property {import('../redis/fence.js').RedisFenceProvider} [fence] - Optional external fence provider (e.g. `createRedisFence`). When set, the runner pairs Postgres heartbeats with the provider's heartbeat so a force-takeover via the external store is detected immediately.
 * @property {number} [fenceTtl=60] - Per-attempt fence lifetime in seconds. Heartbeat extends it while the handler is running.
 * @property {number} [heartbeatInterval] - ms between fence heartbeats. Defaults to fenceTtl * 1000 / 3.
 * @property {number} [recoveryInterval=30000] - ms between recovery sweeps. 0 disables.
 * @property {number} [recoveryBatchSize=10] - Max rows reclaimed per sweep.
 * @property {number} [dispatchInterval=5000] - ms between dispatch sweeps (claim pending rows). 0 disables.
 * @property {number} [dispatchBatchSize=10] - Max pending rows claimed per sweep.
 * @property {number} [awaitPollInterval=500] - ms between row reads while awaiting a task's terminal state.
 * @property {number} [awaitTimeout=60000] - ms after which await() rejects if the task is still not terminal. 0 = no timeout.
 * @property {number} [cleanupInterval=3600000] - ms between cleanup sweeps. 0 disables.
 * @property {number} [rowTtl=604800] - Seconds to keep terminal rows (committed/failed) before deletion. Default 7 days.
 * @property {boolean} [autoMigrate=true] - Auto-create the table on first use.
 * @property {import('../shared/breaker.js').CircuitBreaker} [breaker] - Optional circuit breaker.
 * @property {any} [metrics] - Optional Prometheus metrics registry.
 */

/**
 * @typedef {Object} TaskHandlerContext
 * @property {unknown} input - The input passed to run().
 * @property {string|undefined} idempotencyKey - Stable retry key, typically forwarded to external services.
 * @property {string} fence - This attempt's fence UUID. Read-only.
 * @property {AbortSignal} signal - Aborts when the fence is lost (recovery loop took over).
 * @property {number} attempt - 1-based attempt counter.
 */

/**
 * @typedef {Object} TaskRetryPolicy
 * @property {number} maxAttempts - Total attempts including the first try. >= 1.
 * @property {(attempt: number, err: unknown) => number} [backoff] - ms to wait before the next attempt.
 * @property {(err: unknown) => boolean} [on] - Predicate; return false to skip retries for this error.
 */

/**
 * @typedef {Object} TaskRegistration
 * @property {(ctx: TaskHandlerContext) => Promise<unknown>} handler
 * @property {TaskRetryPolicy} [retry]
 */

/**
 * @typedef {Object} TaskRunOptions
 * @property {unknown} [input] - JSON-serialisable input. Defaults to null.
 * @property {string} [idempotencyKey] - Stable retry key.
 */

/**
 * @typedef {Object} TaskRunner
 * @property {(name: string, handler: (ctx: TaskHandlerContext) => Promise<unknown>, options?: { retry?: TaskRetryPolicy }) => void} register
 * @property {(name: string, options: TaskRunOptions) => Promise<unknown>} run
 * @property {() => void} destroy - Stop recovery and cleanup timers.
 */

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_-]*$/;
const TABLE_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function defaultBackoff(attempt) {
	return Math.min(1000 * 2 ** (attempt - 1), 60000);
}

function delay(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a Postgres-backed task runner.
 *
 * @param {import('./index.js').PgClient} client
 * @param {TaskRunnerOptions} [options]
 * @returns {TaskRunner}
 */
export function createTaskRunner(client, options = {}) {
	if (options.fenceTtl !== undefined) {
		if (typeof options.fenceTtl !== 'number' || options.fenceTtl < 1 || !Number.isInteger(options.fenceTtl)) {
			throw new Error(`postgres tasks: fenceTtl must be a positive integer, got ${options.fenceTtl}`);
		}
	}
	if (options.recoveryInterval !== undefined) {
		if (typeof options.recoveryInterval !== 'number' || options.recoveryInterval < 0 || !Number.isInteger(options.recoveryInterval)) {
			throw new Error(`postgres tasks: recoveryInterval must be a non-negative integer, got ${options.recoveryInterval}`);
		}
	}
	if (options.recoveryBatchSize !== undefined) {
		if (typeof options.recoveryBatchSize !== 'number' || options.recoveryBatchSize < 1 || !Number.isInteger(options.recoveryBatchSize)) {
			throw new Error(`postgres tasks: recoveryBatchSize must be a positive integer, got ${options.recoveryBatchSize}`);
		}
	}
	if (options.dispatchInterval !== undefined) {
		if (typeof options.dispatchInterval !== 'number' || options.dispatchInterval < 0 || !Number.isInteger(options.dispatchInterval)) {
			throw new Error(`postgres tasks: dispatchInterval must be a non-negative integer, got ${options.dispatchInterval}`);
		}
	}
	if (options.dispatchBatchSize !== undefined) {
		if (typeof options.dispatchBatchSize !== 'number' || options.dispatchBatchSize < 1 || !Number.isInteger(options.dispatchBatchSize)) {
			throw new Error(`postgres tasks: dispatchBatchSize must be a positive integer, got ${options.dispatchBatchSize}`);
		}
	}
	if (options.awaitPollInterval !== undefined) {
		if (typeof options.awaitPollInterval !== 'number' || options.awaitPollInterval < 1 || !Number.isInteger(options.awaitPollInterval)) {
			throw new Error(`postgres tasks: awaitPollInterval must be a positive integer, got ${options.awaitPollInterval}`);
		}
	}
	if (options.awaitTimeout !== undefined) {
		if (typeof options.awaitTimeout !== 'number' || options.awaitTimeout < 0 || !Number.isInteger(options.awaitTimeout)) {
			throw new Error(`postgres tasks: awaitTimeout must be a non-negative integer, got ${options.awaitTimeout}`);
		}
	}
	if (options.cleanupInterval !== undefined) {
		if (typeof options.cleanupInterval !== 'number' || options.cleanupInterval < 0 || !Number.isInteger(options.cleanupInterval)) {
			throw new Error(`postgres tasks: cleanupInterval must be a non-negative integer, got ${options.cleanupInterval}`);
		}
	}
	if (options.rowTtl !== undefined) {
		if (typeof options.rowTtl !== 'number' || options.rowTtl < 1 || !Number.isInteger(options.rowTtl)) {
			throw new Error(`postgres tasks: rowTtl must be a positive integer, got ${options.rowTtl}`);
		}
	}

	const table = options.table || 'svti_tasks';
	if (!TABLE_PATTERN.test(table)) {
		throw new Error(`postgres tasks: invalid table name "${table}"`);
	}

	const fenceTtl = options.fenceTtl || 60;
	const heartbeatInterval = options.heartbeatInterval || Math.max(1000, Math.floor(fenceTtl * 1000 / 3));
	const recoveryInterval = options.recoveryInterval !== undefined ? options.recoveryInterval : 30000;
	const recoveryBatchSize = options.recoveryBatchSize || 10;
	const dispatchInterval = options.dispatchInterval !== undefined ? options.dispatchInterval : 5000;
	const dispatchBatchSize = options.dispatchBatchSize || 10;
	const awaitPollInterval = options.awaitPollInterval !== undefined ? options.awaitPollInterval : 500;
	const awaitTimeout = options.awaitTimeout !== undefined ? options.awaitTimeout : 60000;
	const cleanupInterval = options.cleanupInterval !== undefined ? options.cleanupInterval : 3600000;
	const rowTtl = options.rowTtl || 7 * 24 * 3600;
	const autoMigrate = options.autoMigrate !== false;
	const idempotency = options.idempotency || null;
	const fenceProvider = options.fence || null;
	if (fenceProvider !== null) {
		if (typeof fenceProvider !== 'object' ||
			typeof fenceProvider.acquire !== 'function' ||
			typeof fenceProvider.heartbeat !== 'function' ||
			typeof fenceProvider.release !== 'function') {
			throw new Error('postgres tasks: fence must implement acquire, heartbeat, and release');
		}
	}

	const b = options.breaker;
	const m = options.metrics;
	const mRunStart = m?.counter('tasks_started_total', 'Tasks started', ['name']);
	const mRunCommit = m?.counter('tasks_committed_total', 'Tasks committed', ['name']);
	const mRunFail = m?.counter('tasks_failed_total', 'Tasks failed', ['name']);
	const mRetry = m?.counter('tasks_retried_total', 'Task retries', ['name']);
	const mRecovered = m?.counter('tasks_recovered_total', 'Tasks reclaimed by recovery sweep', ['name']);
	const mDispatched = m?.counter('tasks_dispatched_total', 'Pending tasks claimed by dispatch sweep', ['name']);
	const mEnqueued = m?.counter('tasks_enqueued_total', 'Tasks enqueued', ['name']);
	const mFenceLost = m?.counter('tasks_fence_lost_total', 'Attempts whose fence was taken over mid-run', ['name']);
	const mIdemHit = m?.counter('tasks_idempotency_hits_total', 'Cached results returned without running');

	/** @type {Map<string, TaskRegistration>} */
	const handlers = new Map();
	let recoveryTimer = null;
	let recoveryRunning = false;
	let dispatchTimer = null;
	let dispatchRunning = false;
	let cleanupTimer = null;
	let cleanupRunning = false;
	let destroyed = false;

	const sql = createTaskSql({ client, table, fenceTtl, rowTtl, autoMigrate });

	async function executeAttempt(name, input, idempotencyKey, taskId, fence, attempt, requestId) {
		const reg = handlers.get(name);
		if (!reg) throw new UnknownTaskError(name);

		const controller = new AbortController();
		let heartbeatId = null;
		let heartbeatLost = false;

		if (heartbeatInterval > 0) {
			heartbeatId = setInterval(async () => {
				try {
					if (fenceProvider) {
						const externalOk = await fenceProvider.heartbeat(taskId, fence, fenceTtl);
						if (!externalOk) {
							heartbeatLost = true;
							mFenceLost?.inc({ name });
							controller.abort(new Error('fence lost'));
							clearInterval(heartbeatId);
							return;
						}
					}
					const stillOurs = await sql.heartbeatFence(taskId, fence);
					if (!stillOurs) {
						heartbeatLost = true;
						mFenceLost?.inc({ name });
						controller.abort(new Error('fence lost'));
						clearInterval(heartbeatId);
					}
				} catch {
					// transient heartbeat failure: skip this tick.  If the
					// fence truly expires another worker will reclaim.
				}
			}, heartbeatInterval);
			if (heartbeatId.unref) heartbeatId.unref();
		}

		try {
			return await reg.executor({
				input,
				idempotencyKey,
				requestId: requestId ?? null,
				fence,
				signal: controller.signal,
				attempt
			});
		} finally {
			if (heartbeatId) clearInterval(heartbeatId);
			if (!controller.signal.aborted) controller.abort();
		}
	}

	async function runRegisteredTask(name, input, idempotencyKey, taskId, startingAttempt, startingFence, requestId) {
		const reg = handlers.get(name);
		if (!reg) throw new UnknownTaskError(name);

		let attempt = startingAttempt;
		let fence = startingFence;
		let lastError;

		while (true) {
			let result;
			let handlerError;
			await withBreaker(b, async () => {
				if (fence === null || fence === undefined) {
					// Entry path from run(): row does not exist yet.
					fence = randomUUID();
					await sql.insertAttempt(taskId, name, input, idempotencyKey, fence, requestId);
				} else if (attempt > startingAttempt) {
					// Retry within the loop: rotate fence and rearm the existing row.
					fence = randomUUID();
					await sql.rearmAttempt(taskId, fence, attempt);
				}
				// else: dispatch/recovery first iteration uses the fence assigned by the
				// claim CTE; no row mutation needed.
				if (fenceProvider) {
					await fenceProvider.acquire(taskId, fence, fenceTtl);
				}
			});

			mRunStart?.inc({ name });

			try {
				result = await executeAttempt(name, input, idempotencyKey, taskId, fence, attempt, requestId);
			} catch (err) {
				handlerError = err;
			}

			if (handlerError === undefined) {
				const committed = await sql.commitRow(taskId, fence, result);
				if (committed && fenceProvider) {
					try { await fenceProvider.release(taskId, fence); } catch { /* best-effort */ }
				}
				if (!committed) {
					// Our fence was lost; another worker took over.  Read the
					// row to learn the canonical outcome.
					const row = await sql.readRow(taskId);
					if (row && row.status === 'committed') {
						mRunCommit?.inc({ name });
						return row.result;
					}
					if (row && row.status === 'failed') {
						mRunFail?.inc({ name });
						throw deserialiseError(row.error);
					}
					// Row is still running under someone else's fence: yield
					// the result we just produced; the canonical commit will
					// land via that other worker.
					mRunCommit?.inc({ name });
					return result;
				}
				mRunCommit?.inc({ name });
				return result;
			}

			lastError = handlerError;

			const retry = reg.retry;
			const canRetry =
				retry &&
				attempt < retry.maxAttempts &&
				(typeof retry.on !== 'function' || retry.on(handlerError) !== false);

			if (!canRetry) {
				await sql.failRow(taskId, fence, handlerError);
				if (fenceProvider) {
					try { await fenceProvider.release(taskId, fence); } catch { /* best-effort */ }
				}
				mRunFail?.inc({ name });
				throw handlerError;
			}

			mRetry?.inc({ name });
			const backoff = retry.backoff || defaultBackoff;
			const ms = backoff(attempt, handlerError);
			if (ms > 0) await delay(ms);
			attempt += 1;
		}
	}

	async function recoveryTick() {
		if (recoveryRunning || destroyed) return;
		if (b && !b.isHealthy) return;
		recoveryRunning = true;
		try {
			await sql.ensureTable();
			const reclaimed = await sql.reclaimStuck(recoveryBatchSize);
			b?.success();
			for (const row of reclaimed) {
				const reg = handlers.get(row.name);
				if (!reg) {
					// Unknown handler in this process.  Leave the row in
					// 'running' state with our reclaimed fence; another
					// instance with the handler registered will pick it up
					// when our fence expires, or the next sweep here will
					// retry.  No-op rather than fail-the-row-permanently.
					continue;
				}
				mRecovered?.inc({ name: row.name });
				// Run in the background; do not await all reclaimed rows
				// serially or one slow handler stalls the sweep.
				runRegisteredTask(row.name, row.input, row.idempotency_key, row.id, row.attempts, row.fence, row.request_id).catch(() => {
					// Failure is recorded on the row; nothing else to do here.
				});
			}
		} catch (err) {
			b?.failure(err);
		} finally {
			recoveryRunning = false;
		}
	}

	async function dispatchTick() {
		if (dispatchRunning || destroyed) return;
		if (b && !b.isHealthy) return;
		dispatchRunning = true;
		try {
			await sql.ensureTable();
			const claimed = await sql.claimPending(dispatchBatchSize);
			b?.success();
			for (const row of claimed) {
				if (!handlers.has(row.name)) {
					// Unknown handler in this process.  Leave the row in
					// 'running' state with our claimed fence; another instance
					// with the handler registered will pick it up when our
					// fence expires.
					continue;
				}
				mDispatched?.inc({ name: row.name });
				runRegisteredTask(row.name, row.input, row.idempotency_key, row.id, row.attempts, row.fence, row.request_id).catch(() => {
					// Failure is recorded on the row; nothing else to do here.
				});
			}
		} catch (err) {
			b?.failure(err);
		} finally {
			dispatchRunning = false;
		}
	}

	async function cleanupTick() {
		if (cleanupRunning || destroyed) return;
		if (b && !b.isHealthy) return;
		cleanupRunning = true;
		try {
			await sql.ensureTable();
			await sql.deleteOldTerminal();
			b?.success();
		} catch (err) {
			b?.failure(err);
		} finally {
			cleanupRunning = false;
		}
	}

	if (recoveryInterval > 0) {
		recoveryTimer = setInterval(recoveryTick, recoveryInterval);
		if (recoveryTimer.unref) recoveryTimer.unref();
	}
	if (dispatchInterval > 0) {
		dispatchTimer = setInterval(dispatchTick, dispatchInterval);
		if (dispatchTimer.unref) dispatchTimer.unref();
	}
	if (cleanupInterval > 0) {
		cleanupTimer = setInterval(cleanupTick, cleanupInterval);
		if (cleanupTimer.unref) cleanupTimer.unref();
	}

	return {
		register(name, handler, registrationOptions = {}) {
			if (typeof name !== 'string' || !NAME_PATTERN.test(name)) {
				throw new Error(`postgres tasks: invalid task name "${name}"`);
			}
			if (handlers.has(name)) {
				throw new Error(`postgres tasks: task "${name}" is already registered`);
			}
			if (handlers.size >= MAX_TASK_HANDLERS) {
				throw new Error(
					`postgres tasks: registered task count exceeded ${MAX_TASK_HANDLERS} ` +
					`-- registration is bootstrap-time, this number is already pathological`
				);
			}

			const workerOption = registrationOptions.worker;
			if (workerOption !== undefined) {
				if (handler !== null && handler !== undefined) {
					throw new Error(`postgres tasks: handler for "${name}" must be null/undefined when worker is provided (the handler lives in the worker file)`);
				}
				const path = workerOption instanceof URL || typeof workerOption === 'string'
					? workerOption
					: workerOption.path;
				if (!(path instanceof URL) && typeof path !== 'string') {
					throw new Error(`postgres tasks: worker.path for "${name}" must be a URL or string`);
				}
			} else {
				if (typeof handler !== 'function') {
					throw new Error(`postgres tasks: handler for "${name}" must be a function`);
				}
			}

			const retry = registrationOptions.retry;
			if (retry !== undefined) {
				if (!retry || typeof retry !== 'object') {
					throw new Error(`postgres tasks: retry must be an object`);
				}
				if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
					throw new Error(`postgres tasks: retry.maxAttempts must be a positive integer`);
				}
				if (retry.backoff !== undefined && typeof retry.backoff !== 'function') {
					throw new Error(`postgres tasks: retry.backoff must be a function`);
				}
				if (retry.on !== undefined && typeof retry.on !== 'function') {
					throw new Error(`postgres tasks: retry.on must be a function`);
				}
			}

			let executor;
			let pool = null;
			if (workerOption !== undefined) {
				const normalised = workerOption instanceof URL || typeof workerOption === 'string'
					? { path: workerOption }
					: workerOption;
				pool = createWorkerPool(normalised, name);
				executor = (ctx) => pool.run(ctx);
			} else {
				executor = (ctx) => handler(ctx);
			}

			handlers.set(name, { executor, retry, pool });
		},

		async run(name, runOptions = {}) {
			if (typeof name !== 'string' || !NAME_PATTERN.test(name)) {
				throw new Error(`postgres tasks: invalid task name "${name}"`);
			}
			if (!handlers.has(name)) {
				throw new UnknownTaskError(name);
			}
			const input = runOptions.input ?? null;
			const idempotencyKey = runOptions.idempotencyKey;
			if (idempotencyKey !== undefined && (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0)) {
				throw new Error(`postgres tasks: idempotencyKey must be a non-empty string`);
			}
			const requestId = resolveRequestId(runOptions);

			await sql.ensureTable();

			if (idempotency && idempotencyKey) {
				const slot = await idempotency.acquire(idempotencyKey);
				if (slot.acquired) {
					try {
						const result = await runWithoutCache(name, input, idempotencyKey, requestId);
						await slot.commit(result);
						return result;
					} catch (err) {
						await slot.abort();
						throw err;
					}
				}
				if (slot.pending) {
					throw new TaskInFlightError(idempotencyKey);
				}
				mIdemHit?.inc();
				return slot.result;
			}

			return runWithoutCache(name, input, idempotencyKey, requestId);
		},

		async enqueue(name, runOptions = {}) {
			if (typeof name !== 'string' || !NAME_PATTERN.test(name)) {
				throw new Error(`postgres tasks: invalid task name "${name}"`);
			}
			const input = runOptions.input ?? null;
			const idempotencyKey = runOptions.idempotencyKey;
			if (idempotencyKey !== undefined && (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0)) {
				throw new Error(`postgres tasks: idempotencyKey must be a non-empty string`);
			}
			const requestId = resolveRequestId(runOptions);

			await sql.ensureTable();

			const taskId = randomUUID();
			await withBreaker(b, () => sql.insertPending(taskId, name, input, idempotencyKey, requestId));
			mEnqueued?.inc({ name });
			return taskId;
		},

		async await(taskId, awaitOptions = {}) {
			if (typeof taskId !== 'string' || taskId.length === 0) {
				throw new Error('postgres tasks: taskId must be a non-empty string');
			}
			const pollInterval = awaitOptions.pollInterval !== undefined ? awaitOptions.pollInterval : awaitPollInterval;
			const timeout = awaitOptions.timeout !== undefined ? awaitOptions.timeout : awaitTimeout;
			if (!Number.isInteger(pollInterval) || pollInterval < 1) {
				throw new Error('postgres tasks: awaitPollInterval must be a positive integer');
			}
			if (!Number.isInteger(timeout) || timeout < 0) {
				throw new Error('postgres tasks: awaitTimeout must be a non-negative integer');
			}

			await sql.ensureTable();
			const start = Date.now();

			while (true) {
				const row = await sql.readRow(taskId);
				if (!row) {
					throw new Error(`postgres tasks: task "${taskId}" not found`);
				}
				if (row.status === 'committed') return row.result;
				if (row.status === 'failed') throw deserialiseError(row.error);

				if (timeout > 0 && Date.now() - start >= timeout) {
					throw new Error(`postgres tasks: await timeout for task "${taskId}" (waited ${timeout}ms, status=${row.status})`);
				}

				const remainingTimeout = timeout > 0 ? Math.max(1, timeout - (Date.now() - start)) : pollInterval;
				await delay(Math.min(pollInterval, remainingTimeout));
			}
		},

		destroy() {
			destroyed = true;
			if (recoveryTimer) {
				clearInterval(recoveryTimer);
				recoveryTimer = null;
			}
			if (dispatchTimer) {
				clearInterval(dispatchTimer);
				dispatchTimer = null;
			}
			if (cleanupTimer) {
				clearInterval(cleanupTimer);
				cleanupTimer = null;
			}
			for (const reg of handlers.values()) {
				if (reg.pool) reg.pool.destroy();
			}
		}
	};

	function runWithoutCache(name, input, idempotencyKey, requestId) {
		const taskId = randomUUID();
		return runRegisteredTask(name, input, idempotencyKey, taskId, 1, null, requestId);
	}
}

// Resolve a request id from run / enqueue options. Explicit `requestId`
// takes precedence over the platform-extracted one so callers can override
// when running outside an HTTP/WS request context (cron, background sweep).
function resolveRequestId(opts) {
	if (typeof opts.requestId === 'string' && opts.requestId.length > 0) {
		return opts.requestId;
	}
	const fromPlatform = opts.platform && opts.platform.requestId;
	if (typeof fromPlatform === 'string' && fromPlatform.length > 0) {
		return fromPlatform;
	}
	return null;
}
