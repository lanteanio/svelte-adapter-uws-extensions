/**
 * Durable right-to-erasure store for `live.forget`.
 *
 * Composes the per-backend stores an app already wired (connection registry,
 * idempotency, presence, cursor, ...) into the single duck-typed store the
 * realtime layer consumes via `configureForget({ store })`. Each composed store
 * exposes `purgeUser(tenantId, userId, cascade) => Promise<number>`; this
 * fan-out calls them all, returns a per-store breakdown, and - critically for a
 * GDPR erasure - attempts EVERY store even if one fails, then rejects if any
 * failed so the realtime layer surfaces the incomplete erasure for retry
 * (`live.forget` maps a durable rejection to `FORGET_STORE_FAILED`).
 *
 * The realtime layer never imports this; it only sees `{ purgeUser }`.
 *
 * @example
 * ```js
 * import { createForgetStore } from 'svelte-adapter-uws-extensions/forget-store';
 * import { configureForget } from 'svelte-realtime/server';
 *
 * configureForget({
 *   store: createForgetStore({ registry, idempotency }),
 *   platform // for the in-memory presence cluster-roster leg
 * });
 * ```
 *
 * @module svelte-adapter-uws-extensions/forget-store
 */

/**
 * @typedef {Object} ForgetableStore
 * @property {(tenantId: string | null, userId: string, cascade?: any) => Promise<number> | number} purgeUser
 */

/**
 * Compose wired stores into one `{ purgeUser }`. Accepts a named map
 * (`{ registry, idempotency }`) for a labelled breakdown, or an array (labelled
 * `store0`, `store1`, ...). Entries without a `purgeUser` are skipped, so it is
 * safe to pass a store mix that predates forget support.
 *
 * @param {Record<string, ForgetableStore | null | undefined> | Array<ForgetableStore | null | undefined>} stores
 * @returns {{ purgeUser: (tenantId: string | null, userId: string, cascade?: any) => Promise<Record<string, number>> }}
 */
export function createForgetStore(stores) {
	if (!stores || typeof stores !== 'object') {
		throw new Error('createForgetStore: pass a map or array of stores');
	}
	const raw = Array.isArray(stores)
		? stores.map((s, i) => [`store${i}`, s])
		: Object.entries(stores);
	const entries = raw.filter(([, s]) => s && typeof (/** @type {any} */ (s).purgeUser) === 'function');

	return {
		async purgeUser(tenantId, userId, cascade) {
			if (typeof userId !== 'string' || userId.length === 0) return {};
			const settled = await Promise.allSettled(
				entries.map(([name, s]) =>
					Promise.resolve(/** @type {any} */ (s).purgeUser(tenantId, userId, cascade)).then((n) => ({ name, n }))
				)
			);
			/** @type {Record<string, number>} */
			const counts = {};
			const failures = [];
			for (const r of settled) {
				if (r.status === 'fulfilled') {
					const { name, n } = r.value;
					// A store may report a number or a nested breakdown; flatten to
					// a total so the realtime layer can sum the whole result.
					counts[name] = typeof n === 'number' && n > 0 ? n
						: (n && typeof n === 'object' ? Object.values(n).reduce((a, v) => a + (typeof v === 'number' && v > 0 ? v : 0), 0) : 0);
				} else {
					failures.push(r.reason);
				}
			}
			if (failures.length) {
				const err = new Error(
					'createForgetStore: ' + failures.length + ' of ' + entries.length +
					' store(s) failed to purge; the erasure is incomplete and must be retried'
				);
				/** @type {any} */ (err).failures = failures;
				/** @type {any} */ (err).partialCounts = counts;
				throw err;
			}
			return counts;
		}
	};
}
