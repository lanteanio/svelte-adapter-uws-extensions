import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface JobQueueOptions {
	/** Table name. @default 'svti_jobs' */
	table?: string;
	/** Auto-create table on first use. @default true */
	autoMigrate?: boolean;
	/**
	 * Default ms a claim is held before another worker can re-claim
	 * the job. Per-call override via `claim(queue, { visibilityTimeoutMs })`.
	 * @default 30000
	 */
	visibilityTimeout?: number;
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface Job {
	id: string | number;
	queue: string;
	payload: unknown;
	attempts: number;
	created_at: Date;
}

export interface ClaimOptions {
	/** Max number of jobs to return. @default 1 */
	batchSize?: number;
	/** Override the default visibility timeout (ms). */
	visibilityTimeoutMs?: number;
}

export interface JobQueue {
	/** Insert a job; returns the job id. */
	enqueue(queue: string, payload: unknown): Promise<string | number>;

	/**
	 * Atomically claim up to `batchSize` jobs from the named queue
	 * via `SELECT ... FOR UPDATE SKIP LOCKED`. Increments `attempts`
	 * on each claimed row. Concurrent claimers see disjoint subsets.
	 */
	claim(queue: string, opts?: ClaimOptions): Promise<Job[]>;

	/** Delete the claimed job(s). Use after successful processing. */
	complete(idOrIds: (string | number) | (string | number)[]): Promise<void>;

	/**
	 * Release the claim so another worker can re-claim. Use after a
	 * recoverable processing failure when the caller wants the job
	 * to retry.
	 */
	fail(idOrIds: (string | number) | (string | number)[]): Promise<void>;

	/** Extend the visibility deadline for jobs that need more time. */
	extend(idOrIds: (string | number) | (string | number)[], additionalMs: number): Promise<void>;

	/** Count of pending (unclaimed) jobs in `queue`, or across all queues if omitted. */
	pending(queue?: string): Promise<number>;

	/** Delete all jobs in `queue`, or across all queues if omitted. */
	clear(queue?: string): Promise<void>;

	/** Reserved for symmetry with other extensions; no-op currently. */
	destroy(): void;
}

/**
 * Create a Postgres-backed `SELECT ... FOR UPDATE SKIP LOCKED` job
 * queue. Works on vanilla Postgres 9.5+; no extensions required.
 *
 * Pairs with `createTaskRunner` as the lighter "ingest event,
 * defer work" producer; for pgmq deployments, prefer the future
 * `createPgmqWorker` primitive.
 */
export function createJobQueue(client: PgClient, options?: JobQueueOptions): JobQueue;
