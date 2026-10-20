import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

/** Resolver metadata persisted with an alarm (opaque to the store). */
export interface AlarmMeta {
	/** Stable resolver identity. When present, recovery never falls back to `path`. */
	alarmId?: string;
	path?: string;
	tenantId?: string | null;
}

/** One overdue alarm returned by `due(now)` for the realtime recovery poll. */
export interface AlarmRow {
	topic: string;
	at: number;
	meta: AlarmMeta | null;
	/** Positive decimal-string generation allocated by this store. */
	generation: string;
}

/** Exact retained execution identity, also the exclusive `executions()` cursor. */
export interface AlarmExecutionRef {
	topic: string;
	generation: string;
}

/** A claimed alarm, retained until completion or explicit disposition. */
export interface AlarmExecution extends AlarmRow {
	/** Starts at 1; each explicit requeue's next claim increments it. */
	attempt: number;
	status: 'claimed' | 'failed';
	claimedAt: number;
	/** Null until `fail` records the outcome. */
	failedAt: number | null;
	/** Stable failure classification; never the handler's raw error or payload. */
	errorClass: 'handler' | null;
}

export interface RedisAlarmStoreOptions {
	/**
	 * Prefix prepended (after the client `keyPrefix`) to the alarm keys. Lets
	 * two logical alarm stores on one ioredis client stay separate.
	 * @default 'alarm:'
	 */
	keyPrefix?: string;

	/**
	 * Max alarms returned per `due(now)` sweep. The leader recovery poll drains any
	 * remainder on subsequent ticks, so this just bounds one round-trip.
	 * @default 100
	 */
	dueBatch?: number;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

/**
 * A Redis-backed durable alarm store implementing the realtime `AlarmStore` seam
 * (`configureAlarm({ store })`). Pure data-access - no background loop; the
 * leader-gated recovery poll lives in the realtime layer.
 */
export interface RedisAlarmStore {
	/** Reserve a never-reused generation, leaving the current schedule unchanged. Rejects at signed BIGINT exhaustion. */
	nextGeneration(topic: string): Promise<string>;
	/** Read the current scheduled row, or null. Adopts legacy generation authority on first read. */
	get(topic: string): Promise<AlarmRow | null>;
	/** Commit only the latest reservation above the retained committed watermark. */
	set(topic: string, at: number, meta: AlarmMeta | null, generation: string): Promise<boolean>;
	/**
	 * Remove only the matching scheduled generation. Allocation and committed
	 * watermarks survive deletion so delayed writes cannot resurrect it.
	 */
	delete(topic: string, generation: string): Promise<boolean>;
	/**
	 * Atomically move the matching scheduled generation into retained execution
	 * storage and return its attempt, or null when it is absent, replaced or
	 * already claimed. At execution capacity it rejects with code
	 * `ALARM_CAPACITY` and leaves the schedule intact; retained work is never
	 * evicted.
	 */
	claim(topic: string, generation: string): Promise<{ attempt: number } | null>;
	/** Remove only this exact retained attempt (success, a misfire skip, or operator disposal). */
	complete(topic: string, generation: string, attempt: number): Promise<boolean>;
	/** Mark this exact attempt failed, keeping its metadata and deadline; false for an obsolete attempt. */
	fail(topic: string, generation: string, attempt: number, failure: { errorClass: 'handler'; failedAt: number }): Promise<boolean>;
	/** Read one retained execution, or null. Scheduled rows stay available through `get`. */
	getExecution(topic: string, generation: string): Promise<AlarmExecution | null>;
	/**
	 * Page retained executions ordered by topic then generation (byte order),
	 * after the exclusive cursor. Default limit 100, capped at 1000. Restrict to
	 * trusted operators: topics and metadata carry tenant scope.
	 */
	executions(options?: { limit?: number; after?: AlarmExecutionRef | null }): Promise<{ items: AlarmExecution[]; next: AlarmExecutionRef | null }>;
	/**
	 * Explicit retry: move a failed attempt back to the schedule at `at`, keeping
	 * its generation; the next claim gets `attempt + 1`. Returns false without
	 * changes when the attempt does not match, a schedule exists or a newer
	 * generation was reserved. A `claimed` record needs `allowAbandoned: true`,
	 * after confirming its worker stopped. Writes the schedule only; the
	 * realtime recovery poll dispatches it.
	 */
	requeue(topic: string, generation: string, attempt: number, options: { at: number; allowAbandoned?: boolean }): Promise<boolean>;
	/** Return the alarms whose `at <= nowMs` (capped at `dueBatch`) for the poll. */
	due(nowMs: number): Promise<AlarmRow[]>;
	/** Maintenance operation: remove schedules and invalidate reservations, traversing retained topic history atomically. Retained executions are kept. */
	clear(): Promise<void>;
	/** No-op (no background loop); present for parity with the other factories. */
	destroy(): Promise<void>;
}

/**
 * Create a Redis-backed durable alarm store. Wire it into the realtime layer with
 * `configureAlarm({ store: createAlarmStore(client), leader: () => leader.isLeader() })`
 * so `live.alarm` survives a restart and fires once cluster-wide.
 */
export function createAlarmStore(
	client: RedisClient,
	options?: RedisAlarmStoreOptions
): RedisAlarmStore;
