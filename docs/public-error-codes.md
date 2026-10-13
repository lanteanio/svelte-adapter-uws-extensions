# Operational error codes

[Package overview](../README.md) | [Public entry points](public-entries.md)

Branch on an error's `code` when deciding how to handle a recoverable failure. Messages are for people and can change independently of these codes. The existing error names, messages, causes and operation fields remain available. A diagnostic ID in the [error reference](./errors.md) identifies a logging site; it is separate from an error object's `code`.

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

Task handler errors retain their code through worker-thread transport, state-change events, persisted JSON and `runner.await(taskId)`. Reconstructed errors preserve `name`, `message` and `code`, but are generic `Error` objects: do not rely on `instanceof LockLostError`, for example, after a worker or database boundary. Handler-supplied string codes are preserved.

Task lookup and await timeout errors retain the name `Error`. Public named error classes expose literal `code` types in their TypeScript declarations. Connection and timeout wrappers are internal classes returned as failures from public clients; no new constructor exports are required to catch their codes.

These codes do not mean that retrying every failure is safe. A replication timeout may follow a successful primary write, and a lost lease must not be treated as permission to keep mutating protected state. Use the operation's existing retry and idempotency contract. Driver errors and invalid-option errors can have their own codes or no code.
