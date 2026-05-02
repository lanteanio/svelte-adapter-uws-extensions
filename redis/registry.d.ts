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
