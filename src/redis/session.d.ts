import type { UpgradeContext, CloseContext } from 'svelte-adapter-uws';
import type { RedisClient } from './index.js';
import type { MetricsRegistry } from '../prometheus/index.js';
import type { CircuitBreaker } from '../shared/breaker.js';
import type { ForgetIdentity } from '../shared/forget-identity.js';

export type { ForgetIdentity };

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
	 * fixed minute. The extractor receives application data only, as stored
	 * after a JSON round trip, both when a session is indexed and when
	 * `purgeUser` checks it (no class methods or prototype getters, dates as
	 * strings): for a lifecycle record (`create` / `withHooks`) it gets the
	 * session data, never the stored wrapper. A raw `set` value whose stored
	 * JSON is exactly an object with `d` and a numeric `c` has the lifecycle
	 * record's shape and is attributed by its `d` in the same way.
	 *
	 * `set`, `create` and `touch` reject (the close-time persist skips its
	 * write) with `code: 'ERASURE_INDEX_INCOMPLETE'` and
	 * `erasureIndexIncomplete: true` when the extractor throws or returns
	 * anything other than a string, `null` or `undefined`, or the index write
	 * fails; the record is then neither written nor extended. A missing,
	 * `null` or empty owner leaves the session unindexed. Reads never reject
	 * because of the index.
	 */
	forgetUserId?: (data: T) => string | null | undefined;

	/**
	 * Tenant-qualified alternative to `forgetUserId`; configuring both throws.
	 * Receives the same application data and returns the exact
	 * `{ tenantId, userId }` owning it (`tenantId` is server-trusted: 1 to 64
	 * characters of `[a-zA-Z0-9_-]`, or `null` for the untenanted scope), or
	 * `null` / `undefined` for an unattributed session, which is stored
	 * without an index entry and is not reachable by `purgeUser`.
	 *
	 * The identity is extracted once per write and stored beside the record
	 * as `{"identity:v1":[tenantId,userId],"r":<record>}`; refreshes,
	 * replacements and purges read it from there. The index is
	 * `sess:identity:v1:byuser:<base64url(JSON [tenantId,userId])>`, written
	 * before the record, as with `forgetUserId`. A replacement for another
	 * identity drops the token from the previous identity's index. A throwing
	 * or malformed extractor (a missing `tenantId`, an invalid tenant, an empty
	 * `userId`) rejects `set` and `create` with
	 * `code: 'ERASURE_INDEX_INCOMPLETE'` (the close-time persist skips its
	 * write), and nothing is written.
	 *
	 * A record written before the option was enabled is adopted where a
	 * legacy store would slide it (`touch`, and a `get` or lifecycle load with
	 * `refreshOnGet`): its identity is extracted, indexed, and the record is
	 * rewritten in the qualified layout with a fresh TTL. One whose identity
	 * cannot be extracted stays readable but is not extended, and `touch`
	 * resolves `false` for it.
	 */
	forgetIdentity?: (data: T) => ForgetIdentity | null | undefined;

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
	 * All token operations reject the reserved, case-sensitive `byuser:` and
	 * `identity:` prefixes.
	 * The data must be JSON-serializable.
	 *
	 * With `forgetUserId`, the owner's erasure-index field is written first, and
	 * an extractor or index failure rejects with `code: 'ERASURE_INDEX_INCOMPLETE'`
	 * before the record is written. Without `forgetIdentity`, a `set`,
	 * `create` or `touch` that is due to renew the `identity:v1:legacy-active`
	 * coverage key waits for the renewal, and rejects with its Redis error,
	 * writing nothing, when it fails; a refreshing `get` or load then returns
	 * the session without extending it.
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
	 * Revoke every session a user holds (needs `forgetUserId` or
	 * `forgetIdentity`; a no-op
	 * returning 0 without it). Reads only this user's erasure index, never the
	 * rest of the keyspace, so the cost scales with the user's own sessions.
	 * An indexed record is deleted only while its stored bytes still name this
	 * user; a record replaced for another user, or for none, is kept and only
	 * leaves this user's index.
	 *
	 * Rejects with `code: 'ERASURE_INDEX_INCOMPLETE'` and
	 * `erasureIndexIncomplete: true` when Redis fails, one of this user's
	 * indexed records cannot be parsed or attributed, or a record keeps
	 * changing during the purge. The user's other reachable sessions are still
	 * revoked (none are when the index itself cannot be read), and the
	 * unresolved index fields stay for a retry. Rejects before deleting
	 * anything when the index holds a reserved `byuser:` token. With
	 * `forgetUserId`, user IDs are globally unique and tenantId does not
	 * partition the store.
	 *
	 * With `forgetIdentity` (needed instead of `forgetUserId`), the purge
	 * reads only the index of exactly `(tenantId, userId)`, `null` being only
	 * the untenanted scope, and deletes only records stored with that
	 * identity. An invalid `tenantId` throws. Records written before
	 * qualified mode name no tenant and have no qualified index entry, so
	 * while one may still exist it rejects with `code: 'ERASURE_INCOMPLETE'`,
	 * `legacyResidue: true` and the `removed` count. The first qualified purge
	 * scans the keyspace once, for at most 5 seconds, and keeps the answer at
	 * `sess:identity:v1:legacy`: none found opens no window; some found, a
	 * scan out of time, or Cluster slots moving during it opens one for
	 * `ttlMs`. A store without `forgetIdentity` keeps the key
	 * `sess:identity:v1:legacy-active` alive, on the server's clock, for
	 * `1.5 * ttlMs` after its last renewal (which a write or slide due for one
	 * waits for, and which fails that write when it fails); purges report
	 * residue while it
	 * exists.
	 * Records still listed by the `forgetUserId` index of the same raw user
	 * id are residue as well. A write still in flight can land after a purge.
	 * Every rejection carries `removed`, the sessions this call revoked.
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
	 * The close-time persist only updates a record that still exists, so a
	 * session deleted or erased while the socket was open stays gone. It is
	 * best-effort: when it fails, including an erasure-index failure, the
	 * stored session keeps its previous data.
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
