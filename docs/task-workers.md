# Task worker admission

Worker threads run one handler at a time and spawn lazily up to the registered
pool size. The pool admits at most `size + 256` jobs, including capacity reserved
while database claims, execution, retries and commits settle. Reserved input
bytes are limited to 8 MiB per registered pool, or the runner's `maxPayloadBytes`
when that is larger. These are internal safety bounds; `size` and `idleTimeout`
remain the pool configuration options.

An inline `run()` that cannot reserve capacity throws `RangeError` before its
row is inserted. Use `enqueue()` for bursts that should wait durably in Postgres.
Dispatch and recovery reserve capacity before claiming worker rows; saturated
pools leave rows eligible for another compatible worker. Inline handlers keep
their remaining sweep budget when a worker pool has little capacity available.
Reservations span retries and are released on success, failure and SQL errors.

A job waiting for a thread for 30 seconds rejects with `TimeoutError`. Its handler
is never started after that deadline. Queued jobs whose signal aborts are removed
and rejected immediately. Active jobs receive the abort signal cooperatively;
a handler that ignores it continues occupying capacity until it returns or its
worker exits. Worker errors, unexpected exits and failed IPC messages settle
waiting callers. Destroying a runner terminates active and idle worker threads.

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
Active worker capacity remains reserved until the handler actually settles.
Handler exceptions retain their original errors. When an external fence renewal
is already pending at handler completion, success waits for its result or the
watchdog deadline; late ownership loss is not silently discarded. Shutdown stops
future ticks and late external replies cannot start another database renewal.
