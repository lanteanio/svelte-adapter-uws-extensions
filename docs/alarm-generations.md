# Durable alarm generations

[Package overview](../README.md) | [Public entry points](public-entries.md)

Redis and PostgreSQL alarm stores reserve an ordered generation before each
schedule. Keep generation values as strings; converting them to JavaScript
numbers loses precision. The supported range is `1` through
`9223372036854775807`. Allocation rejects at exhaustion and never wraps.

```js
const generation = await store.nextGeneration(topic);
const committed = await store.set(topic, at, meta, generation);
const current = await store.get(topic); // { topic, at, meta, generation } or null
if (current) await store.delete(topic, current.generation);
```

Reservation leaves the current alarm unchanged. `set` returns true only when its
generation is the latest reservation and exceeds the committed/deleted watermark.
A newer reservation fences an older pending writer even before the newer writer
commits. Repeated or stale writes return false. A failed write does not commit its
generation, so the latest reservation can be retried after correcting the cause.

`get` and `due` return the payload and its generation together. `delete` claims
only the matching current generation and returns true to one claimant. A stale
timer cannot delete a replacement. Deletion preserves watermarks and permits a
newer pending reservation to commit; it cannot resurrect an old committed alarm.
`clear` removes current alarms and invalidates all reservations observed before
its commit, retaining allocation history. Neither operation resets counters.

These are durable storage claims. A process crash after claiming can still lose
handler delivery; execution receipts and retries are a separate runtime concern.

## PostgreSQL rollout and recovery

Stop all old topic-only writers before migration and restart only realtime
builds implementing this generation contract. Old three-argument `set` and
topic-only `delete` are incompatible. Await the store's `ready()` gate.

Migration version 3 preserves versions 1 and 2 and adds `generation BIGINT NOT
NULL` to the alarm table without a default. Its `${table}_generations` companion
has `topic TEXT PRIMARY KEY`, `reserved BIGINT NOT NULL DEFAULT 0` and `committed
BIGINT NOT NULL DEFAULT 0`. Valid authority satisfies
`reserved >= committed >= 0`; current alarm generations are positive. Existing
alarms receive generation 1, with both watermarks set to 1. Empty topics first
reserve 1. Backfill takes bounded locks and refuses incompatible existing
authority instead of resetting it. Rehearse the migration on a restored copy.

Grant runtime SELECT, INSERT, UPDATE and DELETE on the alarm table and SELECT,
INSERT and UPDATE on its authority table, plus schema USAGE and ledger SELECT
as described in [PostgreSQL readiness](./postgres-support.md). With
`autoMigrate: false`, deployment must apply version 3 before runtime readiness.
Administrative `clear()` uses a dedicated `READ COMMITTED` transaction, including
writes that commit while it waits even if the connection defaults to stronger
isolation. Authority is locked before the alarm table, checkout is bounded to ten seconds, lock waits to five seconds,
and each statement to ten seconds. Failure rolls back and discards the checkout;
rollback waits at most one second before the connection is discarded.
Clear scans retained authority but updates only outstanding reservations, avoiding
row rewrites and WAL for settled history. Its scan cost still grows with lifetime
topic cardinality; run it during planned maintenance rather than request handling.

Restore the alarm table, authority table and migration ledger from the same
recovery point. Retained authority includes deleted topics and reservations that
have not yet committed, so an empty alarm table does not mean its authority can
be discarded. Do not truncate or expire generation records during cleanup.
Restoring alarms without their authority can otherwise reuse old tokens. Rehearse
that a deleted topic's next reservation exceeds its restored watermark. Mutations
refuse an active alarm whose generation disagrees with its retained authority;
repair a partial restore from a consistent backup rather than resetting counters.

There is no safe mixed-writer rollout or destructive downgrade. Stop producers
and timers before changing binaries; preserve both tables and reconcile pending
alarms and external handler effects before reverting a scheduler.

## Redis rollout and recovery

For Redis upgrades, stop older writers that call topic-only `set` or `delete` before starting the generation-aware store. Existing scheduled rows acquire generation `1` atomically on first use, including `get()` and `due()`; these reads can write authority fields during adoption, while preserving metadata bytes. Keep all five keys together in backups and restores. `clear()` removes schedules and invalidates outstanding reservations, but retains generation authority. Deleted topics keep their counters so delayed calls and restarted processes cannot reuse a generation; this history has no automatic expiry. `clear()` traverses that full history synchronously in Redis, so its cost grows with lifetime topic cardinality. Use it for planned maintenance, away from request handling. Do not erase the authority hashes while any old calls can still arrive. Rolling back to a topic-only writer requires quiescing writers and coordinating the matching realtime version.
