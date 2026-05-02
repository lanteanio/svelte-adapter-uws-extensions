import type { Platform } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

export interface RegistryOptions {
	/**
	 * Extract the user identity from a WebSocket. Return `null` /
	 * `undefined` for anonymous connections; the registry skips them.
	 */
	identify(ws: any): string | null | undefined;

	/**
	 * Extract per-user attributes captured at registration time. Used by
	 * `sendTo(criteria, ...)` for tenant- / role- / cohort-scoped
	 * broadcasts. Shallow values only -- string, number, boolean.
	 * Numbers and booleans round-trip via `String()` for index-key
	 * consistency. Compound queries (regex, array containment, nested
	 * objects) are deliberately out of scope.
	 *
	 * Required for `sendTo(...)` -- callers using only `request`,
	 * `send`, and `sendCoalesced` can omit it; the events channel and
	 * secondary index are skipped when no `attributes` is provided.
	 */
	attributes?(ws: any): Record<string, string | number | boolean> | null | undefined;

	/**
	 * Prefix prepended to all registry keys and channels. Stacks with
	 * the underlying client's `keyPrefix`.
	 * @default ''
	 */
	keyPrefix?: string;

	/**
	 * Expiry on registry entries in seconds. Should be greater than
	 * `heartbeat * 3` so a missed beat doesn't drop a live user.
	 * @default 90
	 */
	ttl?: number;

	/**
	 * Refresh interval in ms. Each tick `EXPIRE`s every locally-owned
	 * entry back to `ttl`.
	 * @default 30000
	 */
	heartbeat?: number;

	/**
	 * Default timeout for `request(...)` calls.
	 * @default 5000
	 */
	requestTimeoutMs?: number;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

/**
 * Stored registry entry. Most-recent-connection-wins; a second device
 * replaces the first.
 */
export interface RegistryEntry {
	instanceId: string;
	sessionId: string;
	ts: number;
	/** Attributes captured by the optional `attributes(ws)` option. Empty when none were configured. */
	attrs: Record<string, string>;
}

export interface ConnectionRegistry {
	/** Stable id for this instance, also the name of its push channel. */
	readonly instanceId: string;

	/**
	 * Resolve a userId to its current owning instance, or `null` if the
	 * user is offline (no live entry in Redis).
	 */
	lookup(userId: string): Promise<RegistryEntry | null>;

	/**
	 * Cluster-routed request/reply. Looks up the owning instance, forwards
	 * the request envelope on the per-instance push channel, and resolves
	 * with the reply when the owning instance answers via the origin's own
	 * push channel.
	 *
	 * Self-targeting (the origin instance owns the user) short-circuits to
	 * a local `platform.request(ws, ...)` without round-tripping Redis.
	 *
	 * Rejects on:
	 *  - the target user being offline (no entry in Redis)
	 *  - the request timing out (`timeoutMs` exceeded waiting for reply)
	 *  - the owning instance reporting a handler error
	 *
	 * @example
	 * ```js
	 * const reply = await registry.request('user-123', 'confirm-action', { op: 'delete' }, {
	 *   timeoutMs: 5000
	 * });
	 * if (reply.confirmed) await actuallyDelete();
	 * ```
	 */
	request<TReply = unknown>(
		target: string,
		event: string,
		data?: unknown,
		options?: { timeoutMs?: number }
	): Promise<TReply>;

	/**
	 * Cluster-routed coalesce-by-key send. Fire-and-forget: routes to the
	 * owning instance, which calls `platform.sendCoalesced(ws, message)`
	 * locally. Per-`(connection, key)` replacement happens on the receiver
	 * side via the adapter's existing coalesce semantics, so a duplicate
	 * or out-of-order envelope from a flaky link is collapsed on arrival.
	 *
	 * Self-targeting (the origin instance owns the user) short-circuits
	 * to a local `platform.sendCoalesced` with no Redis hop.
	 *
	 * Ordering is preserved within a `(user, key)` tuple as long as the
	 * user does not move instances mid-flight; instance migration triggers
	 * one transient out-of-order moment that the per-connection coalesce
	 * collapses on the new instance.
	 *
	 * @example
	 * ```js
	 * registry.sendCoalesced('user-123', {
	 *   key: 'cursor:doc-7',
	 *   topic: 'doc:doc-7',
	 *   event: 'cursor',
	 *   data: { x: 410, y: 220 }
	 * });
	 * ```
	 */
	sendCoalesced(
		target: string,
		message: { key: string; topic: string; event: string; data?: unknown }
	): Promise<void>;

	/**
	 * Cluster-routed counterpart to `platform.send(ws, topic, event, data)`.
	 * Fire-and-forget: routes to the owning instance, which calls
	 * `platform.send(ws, topic, event, data)` locally.
	 *
	 * Self-targeting (the origin instance owns the user) short-circuits
	 * to a local `platform.send` with no Redis hop.
	 *
	 * Edge cases:
	 *  - **User offline:** silently drops. Callers who want a status signal
	 *    should use `request(...)` instead.
	 *  - **Mid-flight migration:** sender's envelope lands on the old owner,
	 *    which no longer has the `ws`; drops with a
	 *    `push_sends_total{result="late"}` increment on the receiver side.
	 *
	 * @example
	 * ```js
	 * registry.send('user-123', 'notifications', 'incoming', { id: 42 });
	 * ```
	 */
	send(target: string, topic: string, event: string, data?: unknown): Promise<void>;

	/**
	 * Cluster-wide attribute-filtered broadcast. Resolves matching userIds
	 * via the local secondary index (built from the `attributes(ws)`
	 * option), groups them by their owning instance, and ships one
	 * envelope per owning instance on the existing per-instance push
	 * channel. Each receiver re-resolves its own local matches and calls
	 * `platform.send(ws, topic, event, data)` for each.
	 *
	 * Match shape is shallow equality only: one literal value per
	 * attribute key, AND across keys. No regex, no array containment, no
	 * nested-object queries. The filter-function escape hatch from
	 * `platform.sendTo(filter, ...)` is deliberately not supported --
	 * functions don't serialize across instances.
	 *
	 * Eventual consistency: a user reconnecting on a different instance
	 * between the sender's index lookup and the receive can produce a
	 * single best-effort missed delivery while the events channel
	 * propagates the migration. Callers who need stronger guarantees
	 * should use `request(...)` per userId or fan out via topic
	 * subscribers.
	 *
	 * Throws when no `attributes` option was supplied at construction,
	 * when criteria is empty, or when topic / event are missing.
	 *
	 * @example
	 * ```js
	 * registry.sendTo({ tenantId: 't42' }, 'announcements', 'created', payload);
	 * registry.sendTo({ tenantId: 't42', role: 'admin' }, 'audit', 'created', payload);
	 * ```
	 */
	sendTo(
		criteria: Record<string, string | number | boolean>,
		topic: string,
		event: string,
		data?: unknown
	): Promise<void>;

	/** Count of users registered to THIS instance (local view, scrape-time). */
	size(): number;

	hooks: {
		open(ws: any, ctx: { platform: Platform }): Promise<void>;
		close(ws: any, ctx: { platform: Platform }): Promise<void>;
	};

	/** Stop the heartbeat timer and Redis subscriber. */
	destroy(): Promise<void>;
}

/**
 * Create a cluster-aware connection registry + push-with-reply primitive.
 *
 * Pairs with the adapter's `platform.request(ws, ...)`: that is single-
 * instance because it takes a local `ws`; this is the cluster-routed
 * counterpart.
 */
export function createConnectionRegistry(
	client: RedisClient,
	options: RegistryOptions
): ConnectionRegistry;
