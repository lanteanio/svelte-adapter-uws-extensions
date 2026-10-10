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
	/** Return the alarms whose `at <= nowMs` (capped at `dueBatch`) for the poll. */
	due(nowMs: number): Promise<AlarmRow[]>;
	/** Maintenance operation: remove schedules and invalidate reservations, traversing retained topic history atomically. */
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
