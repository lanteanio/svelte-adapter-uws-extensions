/**
 * SSRF-defence URL validator. Single-sourced from the adapter
 * (`svelte-adapter-uws/safe-url`) and re-exported here, so the public
 * `svelte-adapter-uws-extensions/safe-url` entry is unchanged for consumers
 * while there is only one canonical implementation to maintain. See the
 * adapter module for the full contract.
 */
export * from 'svelte-adapter-uws/safe-url';
