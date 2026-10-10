# Redis idempotency ownership and erasure

[Package overview](../README.md)

`acquire(key, ttl, { tenant, user })` registers its owner before creating a pending
primary value. An acquisition returns a handle only after checking that its
registration has not been revoked. `purgeUser(tenant, user)` revokes
registered owners, then deletes each primary only if it still belongs to that
owner. A committed result retains the same owner token. A delayed commit must
still match the pending primary, so it cannot recreate a successfully erased
value. A stale owner cannot erase or overwrite a successor's result.

Erasure also revokes pending claims. Their later `commit` rejects with
`IDEMPOTENCY_LEASE_LOST`, as it does after ordinary lease expiry. Erasure does not
cancel a handler or undo external effects; applications must coordinate those
with their own durable operation and authorization records. Calls without user
metadata remain outside user erasure. Tenant/user identity and application keys
must come from trusted application context. Prevent new operations for an erased
user in application authorization: registrations made after revocation are new
operations, not a permanent user tombstone.

## Retention and failures

An initial registration has no expiry until the primary acquisition is
acknowledged. After acknowledgement, its retention bound is `acquireTtl + ttl`
using the result TTL captured by that handle, measured from the later index
operation. A successful commit must occur before the pending primary expires;
it can extend the primary only by that captured result TTL. The bound therefore
covers every result that the owner can still commit, regardless of handler
runtime. A handler outliving its pending lease cannot commit successfully.

Redis 7.4+ and Valkey 9+ apply this bound per index field. Older servers retain
fields until the user's maximum index deadline; a busy user may retain stale
references longer. Unacknowledged registrations keep the shared hash persistent,
so another completed claim cannot expire their erasure record. Once all initial
acquisitions are acknowledged or revoked and removed, normal expiry resumes.

Registration and commit errors reject the call. Erasure errors reject rather
than reporting a successful zero count. A primary write whose acknowledgement
was lost can leave a durable unarmed registration; an unacknowledged deletion
keeps its revoked registration for a retry. Retry `purgeUser` after recovery to
finish the requested erasure. Erasure processes at most eight owners concurrently,
acknowledging each primary deletion before removing its index record. On the
first observed failure it stops starting owners and waits for all started work
to settle before rejecting; no erasure writes continue in the background after
that rejection. Unstarted owners and owners without an acknowledged primary
deletion remain indexed for retry; retry also removes metadata left by a
partially completed index cleanup. Repair records can outlive ordinary TTLs;
include them in operator reconciliation instead of expiring them speculatively.
Do not interpret an operation timeout as proof that a write did not happen.

Ownership scripts require ACL access to `GET`, `SET`, `DEL`, `HMGET`, `HGET`,
`HGETALL`, `HSET`, `HDEL`, `HLEN`, `EXISTS`, `PERSIST`, `PEXPIREAT` and `TIME`
through `EVAL`/`EVALSHA`; legacy cleanup also uses `HKEYS`. Permit the store's
primary keys and both index namespaces. When the version probe reports field
expiry support, permit `HPEXPIRE` too: denying it rejects acquisition, even
though the safe whole-hash deadline is installed first. Keep these permissions
with the application deployment rather than treating index errors as optional.

## Performance and work bounds

Durable registration and acknowledgement add Redis round trips to a cold
user-owned acquisition. Erasure acknowledges a primary deletion and its index
cleanup for each owner, with at most eight owners in flight. These operations
provide the ordering described above; do not budget this as a latency-neutral
change.

A local Redis 7.4 Docker comparison used 1 KiB string payloads, seven alternating
rounds, 256 operations per workload and a separate unchanged-versus-unchanged
control. The table reports median elapsed milliseconds divided by operation
count; concurrency-eight rows describe throughput, not individual request
latency. The purge row measures deletion of 100 retained owners.

| Workload | Previous store | Ownership protocol | Change | Unchanged control |
| --- | ---: | ---: | ---: | ---: |
| Cold user acquire + commit, sequential | 1.987 | 3.456 | +74% | +6% |
| Cold user acquire + commit, concurrency 8 | 0.456 | 0.626 | +37% | -5% |
| Cached result, sequential | 0.634 | 0.633 | -0.2% | -1% |
| Cached result, concurrency 8 | 0.145 | 0.143 | -2% | -14% |
| Anonymous acquire + commit, sequential | 1.344 | 1.210 | -10% | -0.8% |
| Purge 100 owners | 13.33 | 33.89 | +154% | +17% |

Cache-hit changes were within control variation. The anonymous row uses a
separate nine-round, 512-operation comparison. Anonymous calls issue the same
two Redis commands, but their committed values also retain an owner envelope so
a legacy index cannot erase a new anonymous successor. Sample times varied
from 1.13 to 1.63 ms, so the measured improvement is not a latency guarantee.
These local figures are diagnostic, not deployment targets: measure with your
Redis round-trip time and workload.

## Stored format and coordinated upgrade

Primary keys keep their existing locations. Committed values now have the
`__idem_result_v2__:` prefix followed by a JSON owner/result envelope. New
per-user indexes use `byuser:v2\0<tenant>\0<user>` after the configured store
prefix; their fields are owner tokens, with internal state and expiry metadata.
The two NUL delimiters keep this namespace disjoint from the legacy one-NUL
index layout. The existing reserved `byuser:` caller-key rule still applies.

1. Back up primary values, both index namespaces and their expiry state together.
   Stop old writers and drain their acquired handlers before changing versions.
   Do not run old and new writers against the same store during this transition.
2. Deploy the new readers and writers together. Legacy JSON results remain
   readable. Erasure also follows retained legacy indexes, deleting only the
   exact legacy value observed and preserving any new owner that replaced it.
3. Reconcile legacy missing or damaged indexes against authoritative application
   records. Old pending work was not indexed; the new protocol cannot discover
   historical ownership that was never recorded. Do not certify historical
   erasure until old handlers are drained and affected records are reconciled or
   their actual TTLs have elapsed without refresh.
4. Verify pending-owner erasure, failed-write retry and tenant isolation in your
   backend tier before resuming traffic. A renamed idempotency key can repeat an
   effect; keep primary key derivation stable throughout recovery.

Older readers cannot decode new envelopes and older erasure code cannot follow
new indexes. Rollback therefore also requires a stopped-writer recovery plan,
not a mixed-version rollout. Do not delete the new cache to make rollback appear
successful: losing result receipts can cause completed effects to run again.
Retain or reconcile results from authoritative operation records before retiring
any format. Backups must restore primary values and ownership indexes from the
same point in time, then reapply subsequent erasures before serving traffic.
