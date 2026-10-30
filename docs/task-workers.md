# Task worker admission

[Package overview](../README.md) | [Public entry points](public-entries.md)

Worker threads run one handler at a time and spawn lazily up to the registered
pool size. The pool admits at most `size + 256` jobs, including capacity reserved
while database claims, execution, retries and commits settle. Reserved input
bytes are limited to 8 MiB per registered pool, or the runner's `maxPayloadBytes`
when that is larger. These are internal safety bounds; `size` and `idleTimeout`
remain the pool configuration options.

An inline `run()` that cannot reserve capacity throws `RangeError` before its
row is inserted. Use `enqueue()` for bursts that should wait durably in Postgres.
Dispatch and recovery reserve idle or spawnable threads before claiming worker
rows; saturated pools leave rows eligible for another compatible worker. Inline
handlers keep their remaining sweep budget when a worker pool has little capacity available.
Reservations span retries and are released on success, failure and SQL errors.

A foreground job waiting for a thread for 30 seconds rejects with `TimeoutError`.
Its handler is never started after that deadline. Queued jobs whose signal aborts are removed
and rejected immediately. Active jobs receive the abort signal cooperatively;
a handler that ignores it continues occupying capacity until it returns or its
worker exits. A durable claim retains its thread reservation through retries and
database settlement. A crashed worker rejects its active job; a replacement
continues unrelated queued jobs in order. Retiring threads count against the pool
size until they stop. Destroying a runner terminates active and idle worker threads.

With the runner's metrics registry enabled, `tasks_worker_queue_jobs`,
`tasks_worker_active` and `tasks_worker_reserved_bytes` expose current pressure.
`tasks_worker_rejected_total` counts `capacity`, `timeout` and `abort` outcomes.
These metrics aggregate registered pools and have no task-input or user labels.

## Fence renewal confidence

Heartbeats coalesce: one renewal runs at a time and at most one tick waits behind
it. SQL renewal has a driver deadline bounded by the remaining confidence budget.
The watchdog measures elapsed time from before the database claim or renewal,
including any external fence acquisition and renewal latency. It aborts the handler
before that conservative budget expires, even if a backend promise never settles.
A slow response cannot grant a fresh lifetime after time already spent waiting.

Use a positive integer `heartbeatInterval` below `fenceTtl * 1000`; zero retains
the default of one third of the lease lifetime. An interval close to the lifetime
leaves little room for backend latency. The watchdog is conservative confidence,
not a replacement for the database's atomic fence checks: expiry makes a row
eligible for recovery, and a different token prevents the old worker's commit.

A handler that ignores a lost-fence signal cannot commit its returned success.
Loss of renewal confidence abandons that attempt without marking its durable row
failed or consuming the handler retry ladder. The row remains running until its
PostgreSQL lease expires, then a recovery worker may claim a new fence. A confirmed
successor's terminal result still wins over the old attempt's local outcome.
Active worker capacity remains reserved until the handler actually settles.
Handler exceptions retain their original errors. When an external fence renewal
is already pending at handler completion, success waits for its result or the
watchdog deadline; late ownership loss is not silently discarded. Shutdown stops
future ticks and late external replies cannot start another database renewal.

## Fair dispatch across tenants

A dispatch sweep claims pending rows by tenant scope, not by global creation
order. A scope is one tenant of rows stamped by `forgetIdentity`, the untenanted
scope of stamped rows with a `null` tenant, or all rows written without
`forgetIdentity`. Each claim takes the oldest row of every chosen scope before
a second row of any scope, and keeps creation order inside a scope. When more
scopes have pending work than the claim's limit, the next claim for the same
handler names continues after the last scope served, so every tenant with
pending work is reached within `ceil(scopes / dispatchBatchSize)` sweeps however
large another tenant's backlog is. A sweep that claims separately for worker
pool and inline handlers keeps one rotation for each. Rotation state is per
runner. Only `forgetIdentity` gives a row a tenant: without it every row is in
one scope and dispatch is first in, first out.

The claim reads the `idx_svti_tasks_pending_scope` partial index
`(identity_version, name, tenant_id, created_at) WHERE status = 'pending'`
with equality on each registered handler name, so rows of names this runner
does not handle are never read. For `n` registered names, finding the scopes
costs at most `n * (2 * dispatchBatchSize + 2)` single-entry index probes, and
claiming reads at most `n * k * (dispatchBatchSize - k + 1)` index entries for
`k` chosen scopes, 30 per name at the default batch of 10. Neither grows with
any tenant's backlog of either registered or other names. The first-in,
first-out claim it replaces read one creation-time index and filtered names as
it went, so a backlog of other names lay in its path. The rows each read takes
are locked with `FOR UPDATE SKIP LOCKED`: a claim that overlaps another passes
over that claim's rows and takes the next ones, and a row another sweep claimed
meanwhile is re-checked and left alone, so no row is claimed twice. Rows read
but not claimed stay pending; their locks end with the claim's statement.
