# Durable enqueue receipts

[Package overview](../README.md) | [Migration guide](../MIGRATION.md)

`enqueueOnce()` deduplicates task submissions in PostgreSQL. It is separate
from `enqueue()`, which inserts a fresh task ID on every call and only forwards
`idempotencyKey` to the handler, and from the optional `run()` result cache.

## Surface and scope

```ts
interface TaskEnqueueReceipt {
  taskId: string;
  dedupExpiresAt: number; // database epoch milliseconds
}
interface TaskEnqueueOnceOptions extends TaskRunOptions {
  tenantId: string | null;
  idempotencyKey: string;
  dedupTtlMs?: number;
}
interface TaskEnqueueReceiptQuery {
  tenantId: string | null;
  idempotencyKey: string;
}
// Methods on the task runner:
enqueueOnce(name: string, options: TaskEnqueueOnceOptions): Promise<TaskEnqueueReceipt>;
lookupEnqueueReceipt(name: string, query: TaskEnqueueReceiptQuery): Promise<TaskEnqueueReceipt | null>;
```

A key is scoped by the task table (and so its database and schema), the exact
tenant, the task name and the idempotency key. `null` is the untenanted scope,
distinct from every string. `tenantId` follows the
[erasure identity rule](./tenant-erasure-identity.md): 1 to 64 ASCII letters,
digits, `_` or `-`, or `null`; a missing `tenantId` is invalid. Keys are
nonempty strings of at most 256 characters. The same key under another task
name or tenant is independent. Pass the tenant from trusted server context and
authorize receipt lookup yourself: a key is not an access capability, and no
client request may choose a tenant by supplying this option.

`dedupTtlMs` defaults to 24 hours and accepts a positive integer up to 30 days.
The first insertion fixes `dedupExpiresAt` from database time. Retries never
extend it or replace the original tracing, owner, input or scheduling
metadata, and a duplicate with a different TTL returns the original expiry.
Choose a window longer than your producer retries and recovery. This bounds
duplicate submissions; it does not make handler effects exactly-once.

## Input identity and concurrent insertion

The input is encoded once with the runner's payload validation, and those exact
JSON bytes are both the task input and the receipt's identity. Equal bytes are
equal input: object key order and `toJSON` output are significant, so
canonicalize before submission when semantic equality is what you need.

The receipt in `<table>_receipts` stores the task ID, the input bytes, the
erasure owner stamp, creation time and expiry. Its primary key is the
JSON-encoded `[tenantId, name, idempotencyKey]` array, which has no delimiter
ambiguity and no null-uniqueness hole. A live receipt with different input or
a different owner rejects with `TaskEnqueueConflictError`
(`TASK_ENQUEUE_CONFLICT`): nothing is inserted, the original task ID is not
returned, and no handler runs. With `forgetIdentity`, a non-null extracted
owner must belong to the enqueue tenant or the call throws; a `null` owner is
unattributed data with the coverage limits of the identity contract.

One statement inserts the pending task row and its receipt, or neither.
Concurrent producers of one scope serialize on the primary key: one task wins
and every equal retry returns its committed receipt. Workers and later
processes read the same receipt from the same rows. Task execution keeps the
existing fencing and retry rules and can repeat external effects, so make those
effects idempotent in the handler.

## Lost replies and recovery

A receipt is returned only after the insert is confirmed or a committed mapping
is read back from the primary. When the reply is lost and that read cannot
establish the outcome, the call rejects with `TaskEnqueueOutcomeUnknownError`
(`TASK_ENQUEUE_OUTCOME_UNKNOWN`), which carries `tenantId`, `taskName` and
`idempotencyKey` but never a task ID. Recover by retrying the same submission
with the same key; do not generate a replacement key. The runner never falls
back to `enqueue()` or runs the handler inline. A statement error PostgreSQL
reports (which rolls the statement back), a validation failure and a local
admission refusal are ordinary failures with no receipt.

`lookupEnqueueReceipt()` reads the primary through the runner's client: it
resolves the live receipt, `null` when none is visible, or rejects when the
read fails. `null` does not prove that an in-flight submission cannot still
commit. Retrying `enqueueOnce()` with the same input is safe while the window
is live because the unique insert settles that race.

## Retention, expiry and erasure

Task cleanup never deletes a live receipt; there is no foreign key to cascade.
After a completed task's row is cleaned up, `enqueueOnce()` and
`lookupEnqueueReceipt()` still return its receipt, and `await()` on its task ID
reports `TASK_NOT_FOUND`, never a cached result or a replacement task. The
cleanup sweep deletes only receipts whose expiry has passed in database time.

Once expired, lookup resolves `null` and the next `enqueueOnce()` replaces the
receipt and creates a new task in the same statement, even while the previous
task still exists or runs. Concurrent replacements serialize on the same key.
A receipt is never refreshed implicitly. Past expiry, recovery cannot tell an
old lost success from no submission; reconcile business state before retrying
when a duplicate is unacceptable.

`purgeUser()` deletes the identity's receipts, including their input copy and
owner stamp, in the same transaction as its task rows, and counts both. An
`enqueueOnce()` stamped with that identity takes the shared identity lock the
erasure takes exclusively, so an insertion in flight when the erasure starts
commits first and is erased with the rest. With `forgetIdentity`, unstamped
receipts that may hold the user make the erasure report
`ERASURE_INCOMPLETE` with `legacyResidue: true`, as unstamped task rows do.
Revoke the identity's write authority before erasure and keep it revoked: a
later authorized request creates a new task, because erasure removes its
deduplication memory. No tombstone is retained.

The `run()` result cache neither supplies nor consumes receipts.
`enqueueOnce()` forwards the original key to its handler like `enqueue()`;
scope downstream effect keys yourself when another service shares a broader
key namespace.

## Migration

Tasks schema version 5 creates `<table>_receipts`, its checks and its expiry
and owner indexes through the migration ledger. With `autoMigrate: false`,
apply it (`migrate(client, { store: 'tasks' })`) before deploying this release:
the cleanup sweep and `purgeUser()` read the table, and `ready()` reports a
missing one. Historic `enqueue()` rows get no receipts, since repeated keys
were legal and there is no unique winner. To roll back, stop the new producers
and keep the receipts table, so receipts already returned stay recoverable
until they expire.
