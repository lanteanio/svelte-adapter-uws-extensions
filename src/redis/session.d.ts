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

export interface DistributedSessionOptions<T = unknown> {
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
	 * breaker open) at connect, or `identify` throws or returns a reserved `byuser:` token. `'reject'` (default) fails
	 * closed - the upgrade is refused; `'anonymous'` fails open - the connection
	 * proceeds with no session. An unknown or expired token is NOT an error: it
	 * is always a clean anonymous connection (load-only never creates a session).
	 * A failed erasure-index refresh is not a load error either: the session
	 * loads, and its TTL is not extended.
	 * @default 'reject'
	 */
	onLoadError?: 'reject' | 'anonymous';

	/**
	 * Right-to-erasure: extract the owning userId from session data so
	 * `live.forget` can revoke every session a user holds. Without it, sessions
	 * are not user-purgeable (the token is opaque to the store).
	 *
	 * The store keeps a per-user index of tokens. Every write (`set`, `create`,
	 * the close-time persist) writes the owner's index field before the record,
	 * and every refresh (`touch`, a sliding `get` or lifecycle load) refreshes
	 * it before extending the record's TTL; the field outlives the record by a
	 * fixed minute. The extractor receives application data only: for a
	 * lifecycle record (`create` / `withHooks`) it gets the session data, never
	 * the stored wrapper. A raw `set` value that is exactly an object with `d`
	 * and a numeric `c` has the lifecycle record's shape and is attributed by
	 * its `d` in the same way.
	 *
	 * Writes and `touch` reject with `code: 'ERASURE_INDEX_INCOMPLETE'` and
	 * `erasureIndexIncomplete: true` when the extractor throws or returns a
	 * non-string, or the index write fails; the record is then neither written
	 * nor extended. A missing, `null` or empty owner leaves the session
	 * unindexed. Reads never reject because of the index.
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
	 *
	 * Rejects only when Redis fails the read. With `forgetUserId`, the TTL
	 * refresh first refreshes the owner's erasure-index field; when that
	 * fails, `get` still resolves the data and leaves the TTL unchanged.
	 */
	get(token: string): Promise<T | null>;

	/**
	 * Store or replace data for `token`. Resets the TTL via `SET PX`.
	 * All token operations reject the reserved, case-sensitive `byuser:` prefix.
	 * The data must be JSON-serializable.
	 *
	 * With `forgetUserId`, the owner's erasure-index field is written first, and
	 * an extractor or index failure rejects with `code: 'ERASURE_INDEX_INCOMPLETE'`
	 * before the record is written.
	 */
	set(token: string, data: T): Promise<void>;

	/**
	 * Extend TTL. Returns `true` if the entry was present and refreshed, `false`
	 * if the token was missing or already expired.
	 *
	 * With `forgetUserId`, touch reads the record to find its owner and
	 * refreshes the owner's erasure-index field before extending the TTL. An
	 * extractor or index failure rejects with `code: 'ERASURE_INDEX_INCOMPLETE'`
	 * and leaves the TTL unchanged; a corrupt (non-JSON) record returns `false`
	 * and is not extended.
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
	clear(): Promise<void>;

	/**
	 * Revoke every session a user holds (needs `forgetUserId`; a no-op
	 * returning 0 without it). Reads only this user's erasure index, never the
	 * rest of the keyspace, so the cost scales with the user's own sessions.
	 * An indexed record is deleted only while its stored bytes still name this
	 * user; a record replaced for another user, or for none, is kept and only
	 * leaves this user's index.
	 *
	 * Rejects with `code: 'ERASURE_INDEX_INCOMPLETE'` and
	 * `erasureIndexIncomplete: true` when Redis fails, one of this user's
	 * indexed records cannot be parsed or attributed, or a record keeps
	 * changing during the purge. The user's other sessions are still revoked,
	 * and the unresolved index fields stay for a retry. Rejects before deleting
	 * anything when the index holds a reserved `byuser:` token. User IDs are
	 * globally unique; tenantId does not partition this legacy store.
	 */
	purgeUser(tenantId: string | null, userId: string): Promise<number>;

	/**
	 * Mint a NEW session server-side and return its opaque 256-bit CSPRNG token.
	 * Call at login (or in the adapter's `authenticate` hook) and set the
	 * returned token as an httpOnly + secure + sameSite cookie; `identify` then
	 * reads it on connect. Minting tokens only server-side - never trusting a
	 * client-presented token - is what makes the lifecycle immune to session
	 * fixation. Stores a wrapped record (data + creation time); use the lifecycle
	 * layer (`create` / `withHooks` / `of`) consistently rather than mixing raw
	 * `set` / `get` on a minted token. Rejects like `set` when the erasure index
	 * cannot be written, and then stores no session.
	 */
	create(data?: T): Promise<string>;

	/**
	 * Compose session load + persist into the adapter `hooks.ws`. Returns
	 * `{ upgrade, close }` to spread into your WS handler. Your own `upgrade`
	 * runs first (a `false` rejection short-circuits with no session load) and
	 * your `close` runs after the session is persisted. The session is loaded
	 * LOAD-ONLY (an unknown token never creates one) and attached to the
	 * connection for `of(ws)`; on close the (mutated) session is persisted once.
	 * The close-time persist is best-effort: when it fails, including an
	 * erasure-index failure, the stored session keeps its previous data.
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
	options?: DistributedSessionOptions<T>
): DistributedSession<T>;
