# Distributed lock internals

[Package overview](../README.md) | [Public entry points](public-entries.md)

This guide describes how `createDistributedLock` holds its lease, what it guarantees when a holder stalls, the opt-in monotonic fence, and the shared lease scripts. Setup, options, errors and metrics are in the [Distributed lock](../README.md#distributed-lock) section of the package overview.

## Lease protocol

1. **Acquire.** `SET <fullKey> <leaseToken> NX PX <ttlMs>` is an atomic test-and-set with a TTL. The lease token is a per-call random value, so a stale heartbeat or release from a previous holder can never affect a new holder.
2. **Retry.** When another holder has the key, the call sleeps `retryDelayMs` (with +/-25% jitter) and tries again. After `maxWaitMs` of total wait it throws `LockAcquireTimeoutError`.
3. **Hold.** While `fn` runs, a heartbeat every `heartbeatMs` (default `defaultTtlMs / 3`) refreshes the TTL with a Lua-atomic `if get == leaseToken then pexpire end`. A monotonic deadline starts when each acquire or renewal is sent. If no renewal is confirmed before that deadline, or Redis reports lost ownership, the supplied `AbortSignal` fires with `LockLostError`; a delayed reply cannot revive it.
4. **Release.** When `fn` settles, with a result or an error, a Lua-atomic `if get == leaseToken then del end` releases the key. It is skipped after a confirmed loss and is a no-op through the compare guard either way. A heartbeat already in flight settles before the call finishes, bounded by its last confirmed lease deadline. Confirmed loss, or a callback completing past its deadline with an unconfirmed release, rejects with `LockLostError`; a callback error keeps precedence. Cleanup delays alone do not invalidate work completed within its confirmed lease. If a refresh is still unanswered at that deadline, release runs best-effort without extending the cleanup wait.

The `AbortSignal` is how a holder learns about loss: when a heartbeat detects it or the local lease deadline expires, your code can stop instead of continuing to change state another holder now owns. Listen for the `abort` event or check `signal.aborted` at long-running checkpoints.

## Stalled holders

`withLock` is a TTL lease with cooperative release, not a fencing mutex. The lease expires after `ttlMs` so a crashed holder cannot block the cluster forever, and that same expiry means a holder that stalls past `ttlMs` (a long GC pause, a blocked event loop, a paused debugger) loses the lease while still executing. A successor can then acquire it and run the same critical section concurrently. The `AbortSignal` tells the stalled holder to stop, but it is advisory: code that ignores the signal, or is blocked and never observes it, keeps running.

The lease token is internal and never passed to `fn`; it guards only the release and the heartbeat, so a stale holder cannot free or refresh a new holder's key. It does not protect the resource `fn` changes. `withLock` therefore gives mutual exclusion while holders stay responsive and no pause exceeds `ttlMs`. When a critical section must be single-writer even across a stall, the protected resource itself has to reject stale writes: keep the operation idempotent, have the resource check a [monotonic fence](#monotonic-fencing), or use `createTaskRunner`, whose Postgres fence enforces at-most-once commits at the database.

## Monotonic fencing

With `fence: true` on the factory or on one call, the callback receives a monotonically increasing integer as its second argument. A per-call `fence: false` overrides a factory default of `true`, and callbacks without fencing receive only the signal.

The fence protects data only when the resource you write to enforces it. That resource must store the fence atomically with each write and reject any write whose fence is not greater than the last one it accepted. A single conditional `UPDATE ... WHERE fence < $1`, a compare-and-set script, or a version column checked in the same transaction all qualify. The integer cannot stop a stalled holder from executing code; it lets the resource refuse that holder's writes once a newer holder has written.

**Allocation.** After the lease is acquired, Redis increments a counter for that lease key. The callback starts only after the call has confirmed it still owns the lease and is still inside the deadline it computed before acquiring, so a former holder whose increment arrives late never sees a fence greater than its successor's and still runs. Allocation failures reject with the Redis error and counter values outside the positive safe-integer range with `RangeError`, both before user code is called and after a best-effort release of the lease; a lost ownership check rejects with `LockLostError`. A failed attempt can leave a gap in the sequence; the resource only needs the values to increase.

**Keys.** Counters live at `<clientPrefix>lock-fences:<base64url(fullLeaseKey)>`, where the full lease key includes the client and factory prefixes. The client-relative `lock-fences:` namespace is reserved: a lock key that would resolve under it rejects with `TypeError`, even for calls without fencing. Factories whose prefixes resolve to the same physical lease key share one counter. Every holder protecting the same resource must use the same Redis client `keyPrefix`, or they draw from different counters.

**Storage.** Each distinct fenced lease key keeps one counter forever; counters have no TTL. Fencing a high-cardinality key space, such as one lock per order id, keeps one small string per key ever fenced, so size Redis memory for it or fence a bounded set of keys. Counters must not be evicted: under an `allkeys-*` `maxmemory-policy` Redis can drop them, and a counter that restarts at 1 makes the resource reject every write until the counter passes the last fence it accepted. Use `noeviction` or a `volatile-*` policy on the database that holds them.

**Failover and restore.** A replica promoted after an asynchronous failover can lack the most recent increments, and a restored backup can be older than the resource's history. In both cases the next fences can be at or below one the resource already accepted. The resource then rejects those writes rather than accepting a stale one, but fenced work is refused until the counter passes that value. Keep counters with the resource's accepted-fence history through backup, restore and failover. Never delete or roll them back while that history survives, and after a restore set any lagging counter to at least the highest fence the resource accepted before fenced writers resume. The [storage recovery guide](recovery.md#restore-scope) lists them in the restore scope.

## Choosing a coordination primitive

- **`createDistributedLock`** when business logic needs "at most one responsive instance runs this critical section at a time": dedicated lookups against rate-limited APIs, periodic cluster work that should not overlap while holders observe their leases, transactional state machines that do not fit the task runner's shape. The lease is held while one function runs, and its return value is forwarded only without lease loss. It is cooperative, not a hard mutex across a [holder stall](#stalled-holders); strict single-writer effects require downstream fencing or idempotency.
- **`createLeader`** when the question is "which one of N workers should fire this scheduled job right now", not "who runs this critical section". The leader holds a long-lived lease, and the consumer polls the synchronous `isLeader()` at every tick.
- **`createTaskRunner`** when work needs durable result tracking and retries across crashes. Handler execution is at least once after lease expiry: the Postgres fence rejects a stale canonical-result commit, but it cannot undo an external effect already started by that attempt. Observe the abort signal and pass the stable `idempotencyKey` or a fencing token to the downstream system.
- **`createIdempotencyStore`** when the contract is "this operation has a result, and a retry within the TTL must return the same result". Mutual exclusion is not the goal; caching the outcome is.

The lock and the leader run the same Lua scripts with the same lease semantics. They differ in consumer shape: a scoped serializer around one function versus a long-lived observer.

## Raw lease scripts

`svelte-adapter-uws-extensions/redis/lease-scripts` exports the value-guarded Lua scripts that the lock, leader and smooth-cluster primitives run, for tooling that deliberately calls `redis.eval`. Most applications should use `createDistributedLock`, `createLeader` or another lease-backed primitive instead.

`LEASE_RENEW_SCRIPT` takes one key, the expected holder value and a TTL in milliseconds. `LEASE_RELEASE_SCRIPT` takes one key and the expected holder value. Each returns `1` only when the stored value still belongs to that holder, otherwise `0`, so a direct caller shares the primitives' value-guarded semantics.
