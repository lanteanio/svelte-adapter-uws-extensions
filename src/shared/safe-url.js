/**
 * SSRF-defence URL validator.
 *
 * Single-sourced: the implementation lives in the adapter
 * (`svelte-adapter-uws/safe-url`), the layer this package already depends on,
 * and is re-exported here so the public `svelte-adapter-uws-extensions/safe-url`
 * entry is unchanged for consumers. There is intentionally one canonical copy,
 * so the SSRF logic cannot drift between packages. See the adapter module for
 * the full contract (`isSafeUrl` / `checkUrl` / `checkUrlResolved`, the
 * address-level `classifyAddress` / `isAddressSafe`, the blocked ranges, and the
 * `strict` / `allowlist` / `off` modes).
 *
 * The `.d.ts` re-exports the whole surface via `export *`, so the runtime named
 * re-export below MUST list every symbol or it resolves to `undefined` at
 * runtime while still type-checking.
 *
 * @module svelte-adapter-uws-extensions/shared/safe-url
 */

export { isSafeUrl, checkUrl, checkUrlResolved, classifyAddress, isAddressSafe } from 'svelte-adapter-uws/safe-url';
