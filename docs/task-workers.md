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
