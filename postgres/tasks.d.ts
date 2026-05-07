import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { RedisIdempotencyStore } from '../redis/idempotency.js';
import type { PgIdempotencyStore } from './idempotency.js';

export type TaskStatus = 'pending' | 'running' | 'committed' | 'failed';

/**
 * Serialised error stored on failed task rows. JSON-safe; pass through
 * directly to a UI / dashboard without further normalisation. Reconstruct
 * a live `Error` instance with `Object.assign(new Error(e.message), e)` if
 * needed.
 */
export interface TaskError {
	name: string;
	message: string;
	stack?: string;
	code?: unknown;
	cause?: unknown;
}

export interface TaskRow {
	id: string;
	name: string;
	input: unknown;
	status: TaskStatus;
	result: unknown;
	error: TaskError | null;
	attempts: number;
	requestId: string | null;
	createdAt: Date;
	updatedAt: Date;
	fenceExpiresAt: Date;
}

export interface TaskCounts {
	pending: number;
	running: number;
	committed: number;
	failed: number;
	total: number;
}

export interface TaskStateChangeEvent {
	taskId: string;
	name: string;
	/** `null` for the initial insert (the row didn't exist before). */
	oldStatus: 'pending' | 'running' | null;
	newStatus: TaskStatus;
	/** 0 for `pending` rows just inserted by `enqueue`, 1+ for `running` and terminal. */
	attempt: number;
	requestId: string | null;
	/** Present when `newStatus === 'committed'`. */
	result?: unknown;
	/** Present when `newStatus === 'failed'`. */
	error?: TaskError;
}

export interface TaskRunnerOptions {
	/** Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. @default 'svti_tasks' */
	table?: string;
	/** Optional cache for committed results. Pass an idempotency store to deduplicate caller retries. */
	idempotency?: RedisIdempotencyStore | PgIdempotencyStore;
	/** Optional external fence provider (e.g. createRedisFence). Pairs with the Postgres heartbeat for force-takeover detection. */
	fence?: import('../redis/fence.js').FenceProvider;
	/**
	 * Local-worker callback fired AFTER each state-machine transition
	 * commits. Use for UI dashboards, metrics, structured logs, webhooks
	 * -- anything the runner already knows about that today requires
	 * polling or wiring `postgres/notify`.
	 *
	 * Errors thrown from the callback (or rejected promises) are caught
	 * and logged; they do NOT roll back the state machine. Listeners
	 * never block the runner -- async listeners are fired-and-forgotten.
	 *
	 * Cluster-wide fan-out is a separate concern: this callback fires on
	 * the worker that performed the transition. Use `postgres/notify`
	 * if every instance needs to react.
	 *
	 * Fired on:
	 * - `null -> pending` (after `enqueue` inserts the row)
	 * - `null -> running` (after `run` inserts the first attempt)
	 * - `pending -> running` (dispatch sweep claims the row)
	 * - `running -> running` (retry rearm OR recovery sweep, with `attempt` bumped)
	 * - `running -> committed` (handler succeeded)
	 * - `running -> failed` (handler errored past `retry.maxAttempts`)
	 */
	onStateChange?(event: TaskStateChangeEvent): void | Promise<void>;
	/** Per-attempt fence lifetime in seconds. Heartbeat extends it while the handler is running. @default 60 */
	fenceTtl?: number;
	/** ms between fence heartbeats. @default fenceTtl * 1000 / 3 */
	heartbeatInterval?: number;
	/** ms between recovery sweeps. 0 disables. @default 30000 */
	recoveryInterval?: number;
	/** Max rows reclaimed per sweep. @default 10 */
	recoveryBatchSize?: number;
	/** ms between dispatch sweeps (claim pending rows enqueued via enqueue()). 0 disables. @default 5000 */
	dispatchInterval?: number;
	/** Max pending rows claimed per dispatch sweep. @default 10 */
	dispatchBatchSize?: number;
	/** ms between row reads while await() polls for the terminal state. @default 500 */
	awaitPollInterval?: number;
	/** ms after which await() rejects if the task is still not terminal. 0 = no timeout. @default 60000 */
	awaitTimeout?: number;
	/** ms between cleanup sweeps. 0 disables. @default 3600000 (1 hour) */
	cleanupInterval?: number;
	/** Seconds to keep terminal rows (committed/failed) before deletion. @default 604800 (7 days) */
	rowTtl?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface TaskHandlerContext<TInput = unknown> {
	/** The input passed to run(). */
	input: TInput;
	/** Stable retry key. Forward to external services so they de-duplicate too. */
	idempotencyKey: string | undefined;
	/**
	 * The originating request id captured at run() / enqueue() time. Persists
	 * on the row so a handler running on a different worker thread or
	 * instance (recovery sweep, dispatch claim) can correlate logs back to
	 * the originating WS / HTTP request. `null` when the task was started
	 * outside a request context (cron, direct invocation, recovery without
	 * an upstream).
	 */
	requestId: string | null;
	/** This attempt's fence UUID. Read-only. */
	fence: string;
	/** Aborts when the fence is lost (recovery loop took over mid-run). */
	signal: AbortSignal;
	/** 1-based attempt counter. */
	attempt: number;
}

export interface TaskRetryPolicy {
	/** Total attempts including the first try. Must be >= 1. */
	maxAttempts: number;
	/** ms to wait before the next attempt. @default exponential 2^(attempt-1) capped at 60s */
	backoff?: (attempt: number, err: unknown) => number;
	/** Predicate; return false to skip retries for a given error. */
	on?: (err: unknown) => boolean;
}

/**
 * Run the handler in a worker thread instead of the main thread.
 *
 * The handler must live in a separate file whose default export is the
 * handler function. The worker file boots its own database/Redis clients
 * since native connection pools cannot be shared across threads. Use this
 * for CPU-bound work (image resize, hashing, large JSON.parse) that would
 * otherwise block the event loop.
 */
export interface TaskWorkerOption {
	/** URL or absolute path to the worker file. Default-export the handler. */
	path: URL | string;
	pool?: {
		/** Max worker threads spawned for this task. @default 1 */
		size?: number;
		/** ms after which an idle worker is terminated. @default 30000 (0 keeps workers forever) */
		idleTimeout?: number;
	};
}

export interface TaskRegistrationOptions {
	retry?: TaskRetryPolicy;
	/**
	 * Run the handler in a worker thread. When provided, the `handler`
	 * argument to `register` must be null/undefined (the actual handler
	 * lives in the worker file).
	 *
	 * Shorthand: pass a URL or string directly instead of `{ path }`.
	 */
	worker?: URL | string | TaskWorkerOption;
}

export interface TaskRunOptions<TInput = unknown> {
	/** JSON-serialisable input. Defaults to null. */
	input?: TInput;
	/** Stable retry key. */
	idempotencyKey?: string;
	/**
	 * Request id to persist on the task row, exposed to the handler as
	 * `ctx.requestId`. Takes precedence over `platform.requestId` when both
	 * are provided.
	 */
	requestId?: string;
	/**
	 * Convenience for callers inside a request handler: when `platform` is
	 * passed, `platform.requestId` is captured automatically. Pass the
	 * `event.platform` from your SvelteKit handler. Ignored if `requestId`
	 * is also set.
	 */
	platform?: { requestId?: string };
}

export interface TaskAwaitOptions {
	/** Override the runner-level awaitPollInterval (ms). */
	pollInterval?: number;
	/** Override the runner-level awaitTimeout (ms). 0 = no timeout. */
	timeout?: number;
}

export interface TaskRunner {
	/**
	 * Register a task handler. Throws if the name is taken or invalid.
	 *
	 * Default execution: the handler runs in the current process and
	 * shares its scope (database clients, in-memory state). Pass
	 * `options.worker` to run the handler in a worker thread; the handler
	 * argument must then be null/undefined and the actual handler lives
	 * in the worker file.
	 */
	register<TInput = unknown, TResult = unknown>(
		name: string,
		handler: ((ctx: TaskHandlerContext<TInput>) => Promise<TResult>) | null | undefined,
		options?: TaskRegistrationOptions
	): void;

	/**
	 * Run a registered task inline. Awaits the handler's result. The
	 * current process is the worker; if it dies the recovery sweep on
	 * any live instance reclaims the row and re-drives the handler.
	 */
	run<TResult = unknown>(name: string, options: TaskRunOptions): Promise<TResult>;

	/**
	 * Enqueue a task without running it inline. Returns the taskId. The
	 * dispatch sweep on any live instance picks up the row and runs the
	 * handler in the background. Pair with `await(taskId)` to block on
	 * completion, or fire-and-forget for jobs whose result no caller is
	 * waiting on.
	 */
	enqueue(name: string, options: TaskRunOptions): Promise<string>;

	/**
	 * Block until the given task reaches a terminal status. Returns the
	 * committed result, throws the stored error, or rejects with a
	 * timeout error if the task is still pending/running past the
	 * configured awaitTimeout.
	 */
	await<TResult = unknown>(taskId: string, options?: TaskAwaitOptions): Promise<TResult>;

	/**
	 * Resolves once the runner's auto-migration has completed (or
	 * immediately if `autoMigrate: false`). The migration is kicked off
	 * at construction so a separate code path can read the runner's
	 * table before the first `run()` / `enqueue()` / `await()` call --
	 * UI dashboards polling the table on a tick, status pages reading
	 * counts at boot, etc.
	 *
	 * Idempotent. Subsequent calls return the same promise (or a
	 * resolved one once the migration has landed).
	 */
	ready(): Promise<void>;

	/**
	 * List recent rows. Filters compose with AND. Newest first by
	 * `created_at`. Default `limit` is 50, max 1000.
	 *
	 * Returns rows shaped for the public API (camelCase keys, `Date`
	 * instances, parsed JSON, `error` as the JSON-safe shape from
	 * `TaskError`). The internal `fence` UUID is intentionally
	 * excluded -- it has no caller value and exposing it invites misuse.
	 */
	list(options?: {
		name?: string;
		status?: TaskStatus;
		limit?: number;
		offset?: number;
	}): Promise<TaskRow[]>;

	/**
	 * Status counts grouped by status, optionally filtered by `name`.
	 * Always returns the full bucket set (`pending`, `running`,
	 * `committed`, `failed`, `total`) even when one or more buckets is
	 * zero, so callers don't have to normalise.
	 */
	counts(options?: { name?: string }): Promise<TaskCounts>;

	/**
	 * Force a running attempt to abort. Expires the Postgres fence AND
	 * (when a Redis fence provider is configured) releases the Redis
	 * mirror key, so the in-flight handler's heartbeat aborts via
	 * `AbortSignal` on its very next tick instead of waiting for the
	 * Postgres deadline. The recovery sweep on any live instance then
	 * reclaims the row and re-drives the handler under the registered
	 * retry policy.
	 *
	 * Returns `true` if a row was running and got taken over, `false`
	 * if the row is no longer running (already terminal, never
	 * existed at this status, or somebody else expired it first).
	 *
	 * Use case: "drain this instance" -- kick its in-flight tasks off
	 * so the recovery sweep on a healthy instance picks them up faster
	 * than waiting for the Postgres fence deadline.
	 */
	takeover(taskId: string): Promise<boolean>;

	/** Stop the recovery, dispatch, and cleanup timers. */
	destroy(): void;
}

/**
 * Thrown by run() when the idempotency store reports the slot as pending
 * (another caller is mid-flight for the same key). Caller may retry after
 * a backoff or surface a 409 to the upstream HTTP request.
 */
export class TaskInFlightError extends Error {
	idempotencyKey: string;
}

/**
 * Thrown by run() when no handler is registered for the given name.
 * Recovery does not throw on unknown names because the handler may live
 * on a different deployment.
 */
export class UnknownTaskError extends Error {
	taskName: string;
}

/**
 * Create a Postgres-backed durable task runner.
 */
export function createTaskRunner(client: PgClient, options?: TaskRunnerOptions): TaskRunner;
