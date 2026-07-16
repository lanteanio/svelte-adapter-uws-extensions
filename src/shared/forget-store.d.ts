/**
 * Durable right-to-erasure store for `live.forget`. Composes wired per-backend
 * stores (registry, idempotency, presence, ...) into the single duck-typed
 * `{ purgeUser }` the realtime layer consumes via `configureForget({ store })`.
 *
 * With `options.redis`, the purge also erases the user from the cluster-wide
 * room state in Redis (room-owner hashes and presence rosters, tenant-scoped)
 * and resolves the owner-succession envelope so the realtime layer can
 * announce each room's successor on its `:owner` stream.
 *
 * @module svelte-adapter-uws-extensions/forget-store
 */

export interface ForgetableStore {
	purgeUser(
		tenantId: string | null,
		userId: string,
		cascade?: unknown
	): Promise<number | Record<string, number>> | number | Record<string, number>;
}

/**
 * One room's ownership change produced by the cluster-wide owner eviction.
 * `topic` is the WIRE data topic (tenant prefix included); `owner` is the
 * successor, or `null` with reason `'vacated'` when the room emptied.
 */
export interface ForgetOwnerSuccession {
	topic: string;
	owner: string | null;
	reason: 'succeeded' | 'vacated';
}

/**
 * The envelope resolved when the store was built with `options.redis`:
 * the per-store removal counts (the built-in legs report under `roomOwners`
 * and `presenceRoster`) plus the ownership changes the eviction produced.
 */
export interface ForgetEnvelope {
	rowsAffected: Record<string, number>;
	ownerSuccessions: ForgetOwnerSuccession[];
}

export interface ForgetStoreOptions {
	/**
	 * The same Redis client the app stashes on `platform.redis` (a raw ioredis
	 * instance, or a client wrapper exposing one on `.redis`). Opts the purge
	 * into the cluster-wide room-owner force-evict (atomic per-room script
	 * mirroring the realtime leave transition's successor rule, ownership
	 * checked in-script) and the presence-roster field erasure, both scoped to
	 * the purged tenant. Switches the resolved shape to {@link ForgetEnvelope}.
	 */
	redis?: unknown;
}

export interface ForgetStore {
	/**
	 * Erase a user across every composed store. Attempts all stores even if one
	 * fails, then rejects if any failed (so the realtime layer can surface the
	 * incomplete erasure for retry). Resolves to a per-store removal-count
	 * breakdown - or, when the store was built with `options.redis`, the
	 * owner-succession envelope.
	 *
	 * On rejection the thrown error still carries `ownerSuccessions` (the
	 * `ForgetOwnerSuccession[]` for rooms whose eviction ALREADY committed
	 * before a sibling room failed) plus `failures` and `partialCounts`. The
	 * realtime layer announces those committed successions even on the failure
	 * path, because a retry re-drives only the failed rooms and would not
	 * re-report an already-handed-off room.
	 */
	purgeUser(
		tenantId: string | null,
		userId: string,
		cascade?: unknown
	): Promise<Record<string, number> | ForgetEnvelope>;
}

/**
 * Compose wired stores into one `{ purgeUser }`. Accepts a named map for a
 * labelled breakdown, or an array. Entries without a `purgeUser` are skipped.
 */
export function createForgetStore(
	stores: Record<string, ForgetableStore | null | undefined> | Array<ForgetableStore | null | undefined>,
	options?: ForgetStoreOptions
): ForgetStore;
