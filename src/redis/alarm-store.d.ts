import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

/** Resolver metadata persisted with an alarm (opaque to the store). */
export interface AlarmMeta {
	path?: string;
	tenantId?: string | null;
}

/** One overdue alarm returned by `due(now)` for the realtime recovery poll. */
export interface AlarmRow {
	topic: string;
	at: number;
	meta: AlarmMeta | null;
}

export interface RedisAlarmStoreOptions {
	/**
	 * Prefix prepended (after the client `keyPrefix`) to the two alarm keys. Lets
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
	/** Persist (or replace) the alarm for `topic`, due at epoch-ms `at`. */
	set(topic: string, at: number, meta?: AlarmMeta | null): Promise<void>;
	/**
	 * Remove the alarm for `topic`. Returns whether THIS call removed a present row
	 * - the atomic claim guaranteeing single-fire between the in-memory timer and
	 * the recovery poll.
	 */
	delete(topic: string): Promise<boolean>;
	/** Return the alarms whose `at <= nowMs` (capped at `dueBatch`) for the poll. */
	due(nowMs: number): Promise<AlarmRow[]>;
	/** Remove every persisted alarm. */
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
