import type { UpgradeContext, CloseContext } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';

/**
 * The subset of the adapter `hooks.ws` handler that `withHooks` composes with.
 * Your `upgrade` runs first (returning `false` rejects the connection and skips
 * the session load); your `close` runs after the session is persisted.
 */
export interface SessionWsHooks {
	upgrade?(ctx: UpgradeContext): unknown;
	close?(ws: any, ctx: CloseContext): void | Promise<void>;
}

export interface DistributedSessionOptions {
	/**
	 * Prefix prepended (after the client `keyPrefix`) to every session key.
	 * @default 'sess:'
	 */
	keyPrefix?: string;

	/**
	 * Time to live in milliseconds. Each `set` refreshes to `ttlMs`. By
	 * default `get` and `touch` also refresh (sliding window). Default
	 * 24 hours.
	 * @default 86400000
	 */
	ttlMs?: number;

	/**
	 * Whether `get(token)` extends the TTL on a hit. Set to `false` for
	 * read-only flows where reads should not act as liveness signals.
	 * @default true
	 */
	refreshOnGet?: boolean;

	/**
	 * Extract the session token from the WebSocket upgrade context (typically a
	 * cookie). Required for `withHooks` / `of` to load a session; without it -
	 * or when it returns a falsy/non-string value - the connection is anonymous.
	 * Runs at the HTTP upgrade, where the cookie is available (there is no
	 * socket yet, which is why this takes the ctx, not a ws).
	 */
	identify?: (ctx: UpgradeContext) => string | null | undefined;

	/**
	 * Absolute session lifetime in milliseconds, independent of the sliding
	 * `ttlMs`. A session older than this (by its server-minted creation time) is
	 * treated as expired on load and the connection becomes anonymous, forcing
	 * re-authentication regardless of activity. Off by default. Only the
	 * lifecycle layer (`create` / `withHooks` / `of`) stamps and enforces it.
	 */
	maxAgeMs?: number;

	/**
	 * What `withHooks` does when the session store is unreachable (Redis down,
	 * breaker open) at connect, or `identify` throws. `'reject'` (default) fails
	 * closed - the upgrade is refused; `'anonymous'` fails open - the connection
	 * proceeds with no session. An unknown or expired token is NOT an error: it
	 * is always a clean anonymous connection (load-only never creates a session).
	 * @default 'reject'
	 */
	onLoadError?: 'reject' | 'anonymous';

	/**
	 * Right-to-erasure: extract the owning userId from session data at write time
	 * so `live.forget` can revoke every session a user holds. Without it, sessions
	 * are not user-purgeable (the token is opaque to the store).
	 */
	forgetUserId?: (data: T) => string | null | undefined;

	breaker?: CircuitBreaker;
	metrics?: MetricsRegistry;
}

export interface DistributedSession<T = unknown> {
	/**
	 * Look up by token. Returns the stored data if present and not yet
	 * expired, else `null`. By default refreshes the TTL on a hit
	 * (sliding window); disable via `refreshOnGet: false`.
	 *
	 * Returns `null` for a missing token, an expired key, or a corrupt
	 * (non-JSON) entry. The corrupt-entry path is treated as a miss so
	 * the next `set` cleanly overwrites.
	 */
	get(token: string): Promise<T | null>;

	/**
	 * Store or replace data for `token`. Resets the TTL via `SET PX`.
	 * The data must be JSON-serializable.
	 */
	set(token: string, data: T): Promise<void>;

	/**
	 * Extend TTL without reading data. Returns `true` if the entry was
	 * present and refreshed, `false` if the token was missing or already
	 * expired.
	 */
	touch(token: string): Promise<boolean>;

	/**
	 * Remove an entry. Returns `true` if the token was present, `false`
	 * if it was missing or already expired.
	 */
	delete(token: string): Promise<boolean>;

	/**
	 * Remove every session under this store's `keyPrefix`. SCAN-based
	 * cleanup - cluster-wide cost scales with total session count.
	 * Not a hot-path operation; use for graceful-shutdown teardowns,
	 * test harnesses, or operator-initiated wipes.
	 */
	/** Right-to-erasure: revoke every session a user holds (needs `forgetUserId`). */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;
	clear(): Promise<void>;

	/**
	 * Mint a NEW session server-side and return its opaque 256-bit CSPRNG token.
	 * Call at login (or in the adapter's `authenticate` hook) and set the
	 * returned token as an httpOnly + secure + sameSite cookie; `identify` then
	 * reads it on connect. Minting tokens only server-side - never trusting a
	 * client-presented token - is what makes the lifecycle immune to session
	 * fixation. Stores a wrapped record (data + creation time); use the lifecycle
	 * layer (`create` / `withHooks` / `of`) consistently rather than mixing raw
	 * `set` / `get` on a minted token.
	 */
	create(data?: T): Promise<string>;

	/**
	 * Compose session load + persist into the adapter `hooks.ws`. Returns
	 * `{ upgrade, close }` to spread into your WS handler. Your own `upgrade`
	 * runs first (a `false` rejection short-circuits with no session load) and
	 * your `close` runs after the session is persisted. The session is loaded
	 * LOAD-ONLY (an unknown token never creates one) and attached to the
	 * connection for `of(ws)`; on close the (mutated) session is persisted once.
	 */
	withHooks(userHooks?: SessionWsHooks): {
		upgrade(ctx: UpgradeContext): Promise<unknown>;
		close(ws: any, ctx: CloseContext): Promise<void>;
	};

	/**
	 * The live, mutable session data for a connection wired via `withHooks`, or
	 * `null` for an anonymous connection (no/unknown token) or a closed socket.
	 * Mutate the returned object in place; it is persisted once, on disconnect.
	 */
	of(ws: any): T | null;
}

/**
 * Create a Redis-backed session store with sliding TTL. Mirrors the
 * shape of the adapter's bundled `createSession` plugin
 * (`svelte-adapter-uws/plugins/session`) but cluster-wide.
 *
 * Pairs with `createConnectionRegistry`: when both are wired,
 * `session` provides the durable per-user state and `registry`
 * provides the live "where are they right now" pointer.
 */
export function createDistributedSession<T = unknown>(
	client: RedisClient,
	options?: DistributedSessionOptions
): DistributedSession<T>;
