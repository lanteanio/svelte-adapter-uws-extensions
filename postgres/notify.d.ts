import type { Platform } from 'svelte-adapter-uws';
import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface NotifyBridgeOptions {
	/** Postgres LISTEN channel name. Required. */
	channel: string;

	/**
	 * Parse the notification payload into a publish call.
	 * Return null to skip the notification.
	 * Defaults to JSON.parse expecting `{ topic, event, data }`.
	 */
	parse?: (payload: string, channel: string) => { topic: string; event: string; data?: unknown } | null;

	/** Reconnect on connection loss. @default true */
	autoReconnect?: boolean;

	/** ms between reconnect attempts. @default 3000 */
	reconnectInterval?: number;

	/**
	 * `'all'` (default) — every replica opens its own LISTEN connection
	 * and forwards locally with `relay: false`.
	 *
	 * `'advisory'` — every replica polls `pg_try_advisory_lock(lockId)`
	 * on a dedicated connection. The replica that wins the lock holds
	 * the LISTEN connection and forwards notifications *with* relay so
	 * the cross-instance pub/sub bus fans out to non-leader replicas.
	 * If the leader's connection drops, the lock auto-releases and the
	 * next poll on another replica picks it up. Requires `lockId`.
	 *
	 * Use `'advisory'` to avoid N LISTEN connections in an N-replica
	 * deployment. The platform must be wrapped by a cross-instance bus
	 * (e.g. `createPubSubBus`) so non-leader replicas receive the
	 * leader's publishes.
	 *
	 * @default 'all'
	 */
	multiListener?: 'all' | 'advisory';

	/**
	 * Postgres advisory lock id. Required when `multiListener` is
	 * `'advisory'`. Choose a stable value across deployments (e.g.
	 * a CRC32 of a channel-scoped string).
	 */
	lockId?: number;

	/** ms between leader-election polls. @default 5000 */
	pollInterval?: number;

	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

export interface NotifyBridge {
	/** Start listening. Forwards notifications to platform.publish(). Idempotent. */
	activate(platform: Platform): Promise<void>;

	/** Stop listening and release the connection. */
	deactivate(): Promise<void>;
}

/**
 * Create a Postgres LISTEN/NOTIFY bridge.
 */
export function createNotifyBridge(client: PgClient, options: NotifyBridgeOptions): NotifyBridge;
