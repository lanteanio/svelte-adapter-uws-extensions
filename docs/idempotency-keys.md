# Redis idempotency keys

The Redis store reserves keys beginning with the exact, case-sensitive prefix
`byuser:` for its right-to-erasure reverse index. `acquire()` and `purge()` reject
such caller keys before issuing Redis commands, including when a custom or empty
store `keyPrefix` is configured. This prevents a caller from replacing an index
hash with a cache string or deleting the index while its cached results remain.

Derive application keys from trusted identity and validated operation components.
Encoding a tuple with `JSON.stringify` keeps component boundaries distinct.
Tenant-qualified framework keys remain supported; a component containing
`byuser:` is valid when the complete caller key does not begin with that prefix.
This namespace guard does not replace authorization or isolate overlapping store
prefixes configured by the application.

## Existing data

Normal cache and index keys retain their existing Redis layout. There is no bulk
rewrite, scan, or automatic deletion during construction or rejected calls.
Applications that previously used caller keys beginning with `byuser:` must stop
issuing those keys and choose an application-owned key format. Drain pending work
and account for cached-result TTLs before retrying operations under new keys;
renaming an idempotency key can allow an effect to execute again.

The guard cannot reconstruct a reverse index already overwritten or removed by an
older caller. With writers paused, inspect the exact affected key and its Redis
TYPE, and reconcile its cached results against authoritative application records.
Do not delete a legitimate hash or treat a successful namespace upgrade as proof
that historical erasure is complete. Restore ownership mappings from trusted
records, or wait for the affected cached results to expire under their actual
configured TTLs before retiring obsolete data. Retained legacy values are left
untouched for that operator-led recovery.
