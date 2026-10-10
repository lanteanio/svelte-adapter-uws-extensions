import type { PostgresStoreReadiness } from './index.js';
import type { PgClient } from './index.js';
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
	/** Exact positive decimal string; never convert it to a JavaScript number. */
	generation: string;
}

export interface PgAlarmStoreOptions {
	/** Table name. Must match `[a-zA-Z_][a-zA-Z0-9_]*`. @default 'svti_alarms' */
	table?: string;
	/** Max alarms returned per `due(now)` sweep. @default 100 */
	dueBatch?: number;
	/** Auto-create the table on first use. @default true */
	autoMigrate?: boolean;
	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

/**
 * A Postgres-backed durable alarm store implementing the realtime `AlarmStore` seam
 * (`configureAlarm({ store })`). Pure data-access - no background loop; the
 * leader-gated recovery poll lives in the realtime layer.
 */
export interface PgAlarmStore extends PostgresStoreReadiness {
	/** Migrate and validate, or validate read-only when autoMigrate is false. */
	ready(): Promise<void>;
	/** Reserve a durable generation without changing the scheduled row; rejects at signed-64-bit exhaustion. */
	nextGeneration(topic: string): Promise<string>;
	/** Return the current row with its generation, or null. */
	get(topic: string): Promise<AlarmRow | null>;
	/** Commit only the latest reservation newer than the retained watermark; false for stale writes. */
	set(topic: string, at: number, meta: AlarmMeta | null | undefined, generation: string): Promise<boolean>;
	/**
	 * Remove only the matching current generation. Returns whether THIS call removed a present row
	 * - the atomic claim guaranteeing single-fire between the in-memory timer and
	 * the recovery poll.
	 */
	delete(topic: string, generation: string): Promise<boolean>;
	/** Return the alarms whose `at <= nowMs` (capped at `dueBatch`) for the poll. */
	due(nowMs: number): Promise<AlarmRow[]>;
	/** Remove every pending alarm and invalidate reservations, retaining generation authority. */
	clear(): Promise<void>;
	/** No-op (no background loop); present for parity with the other factories. */
	destroy(): Promise<void>;
}

/**
 * Create a Postgres-backed durable alarm store. Wire it into the realtime layer with
 * `configureAlarm({ store: createAlarmStore(client), leader: () => leader.isLeader() })`
 * so `live.alarm` survives a restart and fires once cluster-wide.
 */
export function createAlarmStore(
	client: PgClient,
	options?: PgAlarmStoreOptions
): PgAlarmStore;
