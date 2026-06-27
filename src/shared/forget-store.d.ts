/**
 * Durable right-to-erasure store for `live.forget`. Composes wired per-backend
 * stores (registry, idempotency, presence, ...) into the single duck-typed
 * `{ purgeUser }` the realtime layer consumes via `configureForget({ store })`.
 *
 * @module svelte-adapter-uws-extensions/forget-store
 */

export interface ForgetableStore {
	purgeUser(tenantId: string | null, userId: string, cascade?: unknown): Promise<number> | number;
}

export interface ForgetStore {
	/**
	 * Erase a user across every composed store. Attempts all stores even if one
	 * fails, then rejects if any failed (so the realtime layer can surface the
	 * incomplete erasure for retry). Resolves to a per-store removal-count
	 * breakdown.
	 */
	purgeUser(tenantId: string | null, userId: string, cascade?: unknown): Promise<Record<string, number>>;
}

/**
 * Compose wired stores into one `{ purgeUser }`. Accepts a named map for a
 * labelled breakdown, or an array. Entries without a `purgeUser` are skipped.
 */
export function createForgetStore(
	stores: Record<string, ForgetableStore | null | undefined> | Array<ForgetableStore | null | undefined>
): ForgetStore;
