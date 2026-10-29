# Operational error codes

[Package overview](../README.md) | [Public entry points](public-entries.md)

Branch on an error's `code` when deciding how to handle a recoverable failure. Messages are for people and can change independently of these codes. Existing error names, causes and operation fields remain available. Registry offline messages omit target identifiers. A diagnostic ID in the [error reference](./errors.md) identifies a logging site; it is separate from an error object's `code`.

| Code | Produced by | Meaning |
| --- | --- | --- |
| `CONNECTION_FAILED` | Redis and PostgreSQL client connection wrappers | The connection could not be established. |
| `OPERATION_TIMEOUT` | Redis and PostgreSQL client timeout wrappers | The operation did not complete before its deadline. |
| `CIRCUIT_BROKEN` | Circuit breaker | The breaker refused the operation while open. |
| `REPLICATION_TIMEOUT` | Redis replay | Fewer replicas acknowledged than required. The primary may already hold the write. |
| `REPLAY_STORAGE_FAILED` | Redis and PostgreSQL replay | Replay storage failed; inspect the cause when diagnosing the backend. |
| `REPLAY_SERIALIZATION_FAILED` | Redis and PostgreSQL replay | The supplied data could not be serialized. Correct the input before retrying. |
| `LOCK_ACQUIRE_TIMEOUT` | Redis locks | The lock was not acquired before the wait deadline. |
| `LOCK_LOST` | Redis locks | The lease is no longer owned; stop work that requires ownership. |
| `TASK_IN_FLIGHT` | PostgreSQL tasks | Another call holds the idempotency slot for this task. |
| `UNKNOWN_TASK` | PostgreSQL task execution | This runner has no registered handler for the task name. |
| `TASK_NOT_FOUND` | PostgreSQL task `await()` | No row exists for the requested task ID. |
| `TASK_AWAIT_TIMEOUT` | PostgreSQL task `await()` | The row exists but did not become terminal within the wait deadline. |
| `TASK_FENCE_LOST` | PostgreSQL task execution | Another worker owns the task and its canonical outcome was not available before the deadline. |
| `IDEMPOTENCY_RESULT_TOO_LARGE` | Redis and PostgreSQL idempotency | The encoded result exceeds the configured byte cap. |
| `IDEMPOTENCY_LEASE_LOST` | Redis and PostgreSQL idempotency | The pending slot is no longer owned by this call. |
| `ERASURE_INDEX_INCOMPLETE` | Redis sessions with `forgetUserId` | `set`, `create` or `touch` stored or extended nothing because `forgetUserId` threw or returned a non-string, or the erasure index write failed; or `purgeUser` could not erase every indexed session. `err.cause` holds the underlying error; retry after repairing it. |
| `WS_CLOSED` | Presence and cursor operations | The socket closed during the operation; the operation rolled back. |
| `SUBSCRIBE_DENIED` | Presence and cursor operations | The platform refused access to the underlying topic. |
| `WEBHOOK_CIRCUIT_OPEN` | Redis webhook controls | Delivery was refused while the target's breaker was open. |
| `PG_CAPACITY_EXCEEDED` | PostgreSQL client wrapper `query()` and `connect()` admission | The client's `maxInFlight` limit on outstanding operations is full, so the call never reached PostgreSQL; apply backpressure. This is a ConnectionError with a capacity-specific code. Store breakers and the notification bridge do not count it as a dependency failure. |
| `PG_CLIENT_CLOSING` | PostgreSQL lifecycle registration | The client is closing and cannot own another long-lived consumer. |
| `PG_NOTIFY_SESSION_UNSTABLE` | PostgreSQL notification bridge `activate()` | The new notification session could not be confirmed: its probe notification, sent through the client's query route, never arrived (a transaction- or statement-pooling proxy in the session route, or a query route reaching a different database or server), or the query route is read-only. Fix the routing; the bridge's breaker does not count it as a dependency failure, and with `autoReconnect` the bridge keeps retrying. |
| `REGISTRY_TARGET_OFFLINE` | Registry request and requestSession | No live owner or local socket exists. The message excludes the target identifier. |
| `REGISTRY_PENDING_LIMIT` | Registry request and requestSession | The instance pending-request limit is full; wait for existing requests to settle. |
| `REGISTRY_REQUEST_TIMEOUT` | Registry request and requestSession | No reply arrived before the deadline; the remote outcome is unknown. |
| `REGISTRY_REMOTE_ERROR` | Registry remote replies | The remote handler or owner rejected the request. The existing wire format carries its message, not its error class or code. |
| `REGISTRY_DESTROYED` | Registry request and requestSession | The registry was destroyed before the request settled, or before it could open its reply subscription. |
| `TASK_WORKER_CAPACITY_EXCEEDED` | PostgreSQL task worker admission | No job, byte or thread allowance is available. The error remains a RangeError. |
| `TASK_RUNNER_DESTROYED` | PostgreSQL task workers | The worker pool has stopped; active, queued and new work is rejected. |
| `TASK_WORKER_EXITED` | PostgreSQL task workers | The worker exited before its task completed; the operation outcome may be unknown. |
| `TASK_WORKER_ADMISSION_EXPIRED` | PostgreSQL task workers | The supplied reservation was released before execution. |
| `TASK_WORKER_RESERVATION_IN_USE` | PostgreSQL task workers | The supplied reservation already has an executing task. |
| `OPERATION_ABORTED` | PostgreSQL task worker fallback | An abort without a supplied reason cancelled the work; supplied abort reasons remain unchanged. |
| `ERASURE_INCOMPLETE` | Composed forget store | One or more stores failed. Inspect failures and partialCounts, repair the cause and retry. |
| `REDIS_CONNECTION_CLOSED` | Managed Redis subscribers | The owned connection ended and cannot complete the queued operation. Native driver errors retain their own codes. |
| `REDIS_CLUSTER_PUBLISHER_UNAVAILABLE` | Redis pub/sub relay | No ready master can publish this relay batch. |
| `BUS_DEACTIVATED` | Redis pub/sub and sharded bus lifecycle | Deactivation interrupted activation or a pending shard follow. |
| `BUS_NOT_ACTIVE` | Sharded bus follows | Activate the bus before following topics. |
| `SHARDED_OWNER_UNAVAILABLE` | Sharded bus topology | The slot owner is not available in the current topology. |
| `SHARDED_OWNER_CLOSED` | Sharded bus subscription | The owning connection ended while subscribing. |
| `SHARDED_OPERATION_TIMEOUT` | Sharded bus topology and subscription | A bounded topology or subscription operation did not settle before its deadline. |
| `SHARDED_TOPIC_CAPACITY_EXCEEDED` | Sharded bus follow | The instance distinct-topic allowance is full. |
| `SHARDED_REDIS_VERSION_INVALID` | Sharded bus activation | The server version could not be read from INFO server. |
| `SHARDED_REDIS_VERSION_UNSUPPORTED` | Sharded bus activation | The server is older than the required Redis 7. |
| `CURSOR_TOPIC_CAPACITY_EXCEEDED` | Redis cursor update | The instance local-topic allowance is full. |
| `CURSOR_CONNECTION_CAPACITY_EXCEEDED` | Redis cursor connection state | The instance local-connection allowance is full. |
| `RELAY_ENVELOPE_TOO_LARGE` | Registry and Redis bus relay admission; Redis CRDT coordinator `onError` | The complete encoded envelope exceeds the configured peer limit. Bus batch failures remain RangeError; registry failures remain Error. The CRDT coordinator reports an envelope too large even for parts once per held document (the document layer reports each failed store separately). Raising `maxEnvelopeBytes` on every instance helps only when the topic name leaves no room; the part limit, a little under 36 MiB of state, is fixed. |
| `CRDT_RELAY_PENDING` | Redis CRDT coordinator `acquirePersist` | Local edits to the document have not reached the other instances, because a relay failed or Redis has not confirmed it, or the replica has not synced with them after the relay lost traffic, or a document too large for the relay may differ between instances (`cause` is the `RELAY_ENVELOPE_TOO_LARGE` error; a stored snapshot is replaced, not merged, so writing a replica that differs would erase the other instances' edits, and once a peer holds a different state, or a sync request or reply cannot be sent, this lasts until the instance restarts). The persistence schedule retries; the replica stays loaded. |
| `RELAY_QUEUE_FULL` | Redis bus publishBatched admission | The relay queue cannot admit the whole call; nothing was delivered locally or relayed. When queued relays leave too little of the queue's envelope or byte allowance, retry after Redis drains the queue or reduce publish pressure. A sharded-bus call whose shard envelopes together exceed `max(8 MiB, maxEnvelopeBytes)` or number more than 8,192 is refused on every attempt, even on an idle queue; split it. The error remains a RangeError. |

Task handler errors retain their code through worker-thread transport, state-change events, persisted JSON and `runner.await(taskId)`. Reconstructed errors preserve `name`, `message` and `code`, but are generic `Error` objects: do not rely on `instanceof LockLostError`, for example, after a worker or database boundary. Handler-supplied string codes are preserved.

Task lookup and await timeout errors retain the name `Error`. Public named error classes expose literal `code` types in their TypeScript declarations. Connection and timeout wrappers are internal classes returned as failures from public clients; no new constructor exports are required to catch their codes.

These codes do not mean that retrying every failure is safe. A replication timeout may follow a successful primary write, and a lost lease must not be treated as permission to keep mutating protected state. Use the operation's existing retry and idempotency contract. Driver errors and invalid-option errors can have their own codes or no code.
