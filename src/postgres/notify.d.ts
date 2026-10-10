import type { Platform } from 'svelte-adapter-uws';
import type { PgClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { TraceContext } from '../shared/trace-context.js';

export interface NotifyEnvelope {
	topic: string;
	event: string;
	data?: unknown;
	/** Explicit restored context. Invalid/null explicitly suppresses carrier fallback. */
	traceContext?: TraceContext | null;
	/** W3C carrier fields accepted by the default parser and custom parsers. */
	traceparent?: string;
	tracestate?: string;
}

export interface NotifyBridgeOptions {
	/** Postgres LISTEN channel name. Required. */
	channel: string;

	/**
	 * Parse the notification payload into a publish call.
	 * Return null to skip the notification.
	 * Defaults to JSON.parse expecting `{ topic, event, data }`.
	 */
	parse?: (payload: string, channel: string) => NotifyEnvelope | null;

	/** Reconnect on connection loss. @default true */
	autoReconnect?: boolean;

	/** Reconnect baseline in ms; retries wait a jittered delay from this value up to twice it. @default 3000 */
	reconnectInterval?: number;

	/**
	 * `'all'` (default) - every replica opens its own LISTEN connection
	 * and forwards locally with `relay: false`.
	 *
	 * `'advisory'` - every replica polls `pg_try_advisory_lock(lockId)`
	 * on a dedicated connection. The replica that wins the lock holds
	 * the LISTEN connection and forwards notifications *with* relay so
	 * the cross-instance pub/sub bus fans out to non-leader replicas.
	 * If the leader's connection drops, the lock auto-releases and the
	 * next poll on another replica picks it up. Requires `lockId`.
	 *
	 * Use `'advisory'` to avoid N LISTEN consumers in an N-replica
	 * deployment. Every contender still owns one dedicated database session. Use a direct or session-pooled DSN, never transaction pooling. The platform must be wrapped by a cross-instance bus
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

	/**
	 * Reject inbound `NOTIFY` payloads larger than this many bytes before
	 * `JSON.parse` runs. Defends against a buggy or hostile trigger
	 * flooding the bridge with oversized payloads.
	 * @default 1048576 (1 MB)
	 */
	maxEnvelopeBytes?: number;

	/**
	 * When `false` (default), parsed envelopes addressed to `__`-prefixed
	 * topics are dropped before the publish call. Closes the
	 * NOTIFY-injection class where a foreign publisher (or hostile DBA)
	 * could otherwise inject forged `__signal:*` / `__rpc` /
	 * plugin-internal frames via `pg_notify`. Apps that legitimately
	 * bridge user-defined `__`-prefixed topics (rare) can opt back in
	 * via `allowSystemTopics: true`.
	 * @default false
	 */
	allowSystemTopics?: boolean;
}

export interface NotifyBridge {
	/** Await session setup before serving traffic. A healthy advisory follower resolves; connection/setup failures reject. Concurrent calls share startup. */
	activate(platform: Platform): Promise<void>;

	/** Await timer cancellation and session closure. Owned PgClient.end() calls this before closing its pool. */
	deactivate(): Promise<void>;
}

/**
 * Create a Postgres LISTEN/NOTIFY bridge.
 */
export function createNotifyBridge(client: PgClient, options: NotifyBridgeOptions): NotifyBridge;
