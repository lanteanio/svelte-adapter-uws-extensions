# Redis session tokens

[Package overview](../README.md) | [Public entry points](public-entries.md)

The exact, case-sensitive token prefix `byuser:` belongs to the right-to-erasure
index. `get`, `set`, `touch` and `delete` reject such tokens before Redis access,
including with a custom or empty store `keyPrefix`. Lifecycle cookie loads apply
`onLoadError`: the default rejects the upgrade; `anonymous` loads no session.
These validation failures do not count as Redis circuit-breaker failures.
Server-minted tokens from `create()` remain valid. A token containing `byuser:`
later in its value is allowed; this rule does not replace token authorization.

Normal session and index layouts stay unchanged. Applications that previously
issued reserved tokens must stop doing so. The guard cannot repair an index
already overwritten or removed by an older caller. A legacy index containing a
reserved token makes `purgeUser` reject before deleting any indexed sessions.
With writers paused, inspect the affected key types and reconcile tokens and
ownership mappings against trusted application records. Do not blindly delete
an index hash or treat the upgrade as proof of historical erasure. Revoke affected
sessions through an operator-led recovery or wait for their actual TTLs to expire
without refresh before retiring the old records.
