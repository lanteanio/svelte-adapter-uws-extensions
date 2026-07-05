import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RedisPresenceOptions {
	/** Field in selected data for user dedup. @default 'id' */
	key?: string;
	/** Extract public fields from userData. @default identity */
	select?: (userData: any) => Record<string, any>;
	/** Heartbeat interval in ms (refresh TTL). @default 30000 */
	heartbeat?: number;
	/** TTL in seconds for presence hash entries. @default 90 */
	ttl?: number;
	/**
	 * Subscribe to `__keyevent@*__:expired` so a topic's local subscribers
	 * receive an empty `list` event the moment its presence hash expires.
	 * Catches the instance-died scenario where a sync-only observer would
	 * otherwise show stale data forever.
	 *
	 * Requires `CONFIG SET notify-keyspace-events Ex` (or any flagset
	 * including `K`/`E` and `x`) on the Redis server. If the psubscribe
	 * fails the failure is logged once and the rest of the tracker keeps
	 * working without the keyspace branch.
	 *
	 * @default false
	 */
	keyspaceNotifications?: boolean;
	/**
	 * Dynamic field names (set via `update()`) that are broadcast live but
	 * NEVER persisted to Redis and NEVER included in the `state` snapshot or
	 * the heartbeat roster. A (re)joining or swept-then-readded client never
	 * inherits a possibly-stale transient value (e.g. a disconnected typer
	 * leaves no stuck indicator). Durable `update()` fields not listed here
	 * persist and ride the snapshot. Matches the bundled in-memory presence
	 * plugin. Default: none (every `update()` field is durable).
	 */
	transient?: string[];
	/**
	 * Fleet-level self-preservation guard for correlated mass disconnects
	 * (on by default). When one heartbeat tick finds `threshold` (default
	 * 15%) or more of this instance's tracked sockets simultaneously dead -
	 * and at least `minPopulation` (default 8) sockets are tracked, so a
	 * small population cannot read as a ratio - that is treated as a network
	 * event, not that many users leaving at once: those evictions are HELD
	 * (cluster TTLs kept refreshed, no leave broadcast) for up to `holdMaxMs`
	 * (default 90000), giving clients time to reconnect without a leave/join
	 * flap across every roster. A held socket whose user never reconnects is
	 * evicted when its hold expires. Individually-dying sockets below the
	 * threshold evict immediately, exactly as before. `onChange(active,
	 * { held, total })` fires on activation and release. Set `false` to
	 * restore unconditional immediate eviction.
	 * @default true
	 */
	selfPreservation?: boolean | {
		threshold?: number;
		minPopulation?: number;
		holdMaxMs?: number;
		onChange?: (active: boolean, info: { held: number; total: number }) => void;
	};
	/** Prometheus metrics registry. */
	metrics?: MetricsRegistry;
	/** Circuit breaker instance. */
	breaker?: CircuitBreaker;
}

/**
 * Wire shape clients see on `__presence:{topic}`. Mirrors the adapter's
 * bundled `createPresence` plugin so a single client decoder works for
 * both single-instance and cluster deployments.
 */
export type PresenceWireEvent =
	| { event: 'state'; data: Record<string, Record<string, any>> }
	| { event: 'diff'; data: { joins: Record<string, Record<string, any>>; leaves: Record<string, Record<string, any>>; updates?: Record<string, Record<string, any>> } }
	| { event: 'heartbeat'; data: Record<string, Record<string, any>> };

export interface PresenceMetricsSnapshot {
	/** Sum of unique-users-per-topic across all topics this instance is locally tracking. */
	totalOnline: number;
	/** Duration of the most recent heartbeat tick in milliseconds. */
	heartbeatLatencyMs: number;
	/** Cumulative count of stale fields removed by the heartbeat-driven cleanup script since startup. */
	staleCleanedTotal: number;
}

/**
 * Thrown by `join()` when the websocket closes during an async gap before
 * the join can commit. Server-side state is fully rolled back before the
 * throw. Catch on `err.code === 'WS_CLOSED'` rather than the class - the
 * same code is shared with `cursor.attach` and any future RPC-shaped
 * operation in this package.
 */
export class WsClosedError extends Error {
	name: 'WsClosedError';
	code: 'WS_CLOSED';
	operation: string;
	topic: string;
}

export interface RedisPresenceTracker {
	/**
	 * Add a connection to a topic's presence.
	 * Ignores `__`-prefixed topics. Idempotent.
	 *
	 * @throws {WsClosedError} (`err.code === 'WS_CLOSED'`) if the websocket
	 *   closes during one of the internal async gaps (subscribe, Redis eval,
	 *   snapshot fetch, ws.subscribe). Server state is rolled back before
	 *   the throw; callers do not need to compensate.
	 */
	join(ws: any, topic: string, platform: Platform): Promise<void>;

	/** Remove a connection from a specific topic, or all topics if omitted. */
	leave(ws: any, platform: Platform, topic?: string): Promise<void>;

	/** Send current presence list without joining. */
	sync(ws: any, topic: string, platform: Platform): Promise<void>;

	/**
	 * Set dynamic fields on the present user (typing, a selection range, a lock
	 * map) as a field-level delta: only fields whose value actually changed are
	 * merged into the user and broadcast in the next `diff` under
	 * `updates[key]`. Durable fields are persisted to Redis so a cross-instance
	 * `state` read includes them; fields named in the `transient` option are
	 * broadcast live but never persisted or snapshotted. The update applies to
	 * the user (per dedup key), so any of a multi-tab user's connections - on
	 * any instance - may call it. A connection not present on the topic is a
	 * silent no-op. No-op if no field actually changed. Mirrors the in-memory
	 * `presence.update`.
	 */
	update(ws: any, topic: string, fields: Record<string, any>, platform: Platform): Promise<void>;

	/** Get the current presence list for a topic. */
	list(topic: string): Promise<Record<string, any>[]>;

	/** Get the number of unique users present on a topic. */
	count(topic: string): Promise<number>;

	/**
	 * Right-to-erasure: remove a user from every presence topic across the
	 * cluster (DEL the per-user hash + HDEL the topic hash + broadcast a leave).
	 * Matches when the configured `key` field makes the presence key equal the
	 * userId. Returns the number of topics the user was removed from.
	 */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;

	/**
	 * Snapshot of local presence health metrics. Synchronous; reads
	 * in-memory state only. The same numbers are exposed as Prometheus
	 * gauges (`presence_total_online`, `presence_heartbeat_latency_ms`)
	 * when a metrics registry is attached.
	 */
	metrics(): PresenceMetricsSnapshot;

	/**
	 * Live state of the mass-disconnect self-preservation guard: whether the
	 * hold is active, since when (epoch ms), and how many dead sockets it
	 * is currently holding from eviction. Synchronous - safe to poll from a
	 * health endpoint or render as a degraded-banner source. Also exposed as
	 * the gauges `presence_self_preservation_active` / `_held` and the
	 * counter `presence_self_preservation_activations_total`.
	 */
	selfPreservation(): { enabled: boolean; active: boolean; since: number | null; held: number };

	/**
	 * Drain the pending diff buffer synchronously. The diff buffer
	 * normally flushes on the next microtask after a join / leave /
	 * update; call this when a test or graceful-shutdown path needs
	 * the `diff` to land before the await chain continues.
	 */
	flushDiffs(): void;

	/** Clear all presence state. */
	clear(): Promise<void>;

	/** Stop heartbeat timer and Redis subscriber. */
	destroy(): void;

	/**
	 * Ready-made WebSocket hooks for zero-config presence.
	 *
	 * `subscribe` handles both regular topics (calls `join`) and `__presence:*`
	 * topics (calls `sync` so the client gets the current list immediately).
	 * `unsubscribe` removes presence from a single topic when the client
	 * unsubscribes (requires core adapter v0.4.0+).
	 * `close` calls `leave`.
	 *
	 * @example
	 * ```js
	 * import { presence } from '$lib/server/presence';
	 * export const { subscribe, unsubscribe, close } = presence.hooks;
	 * ```
	 */
	hooks: {
		subscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		unsubscribe(ws: any, topic: string, ctx: { platform: Platform }): Promise<void>;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};
}

/**
 * Create a Redis-backed presence tracker.
 */
export function createPresence(client: RedisClient, options?: RedisPresenceOptions): RedisPresenceTracker;
