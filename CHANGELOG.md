# Changelog

All notable changes to `svelte-adapter-uws-extensions` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Idempotency store** (`svelte-adapter-uws-extensions/redis/idempotency` and `svelte-adapter-uws-extensions/postgres/idempotency`). Caches the result of an effectful operation under a stable key so retries within `ttl` return the original outcome rather than re-executing. Three-state `acquire(key)` returns `acquired` (caller runs the work, then `commit(result)` or `abort()`), `pending` (another caller is mid-flight), or `result` (cached). Two TTLs: `acquireTtl` (default 60s) bounds a pending slot to prevent crashed-owner deadlocks; `ttl` (default 48h) governs result cache lifetime. Redis backend uses a single-round-trip Lua script with `SET NX EX`. Postgres backend uses an atomic `INSERT ... ON CONFLICT DO UPDATE` with `expires_at` and a periodic cleanup sweep. Both backends share the same contract; the adapter's in-memory `createDedup` plugin is the zero-config single-instance fallback. Optional `breaker` and `metrics` options match the rest of the extensions.
- **Durable task runner** (`svelte-adapter-uws-extensions/postgres/tasks`). Wraps an effectful operation in a state machine that survives process crashes and naturally fans across cluster instances. `register(name, handler, { retry })` declares a task and its retry policy at registration time; `run(name, { input, idempotencyKey })` awaits the result. Three guarantees: caller-retry idempotency (pair with the idempotency store via the `idempotency` option to cache committed results), worker-crash recovery (every attempt holds a fence UUID; the conditional commit `UPDATE ... WHERE fence = $current` is atomic, and a periodic recovery sweep reclaims rows whose fence has expired), and external-service idempotency (the `idempotencyKey` is forwarded to the handler so it can be passed to Stripe / SendGrid / S3). Per-attempt heartbeat extends the fence while the handler is running; if the heartbeat fails, the handler's `AbortSignal` fires so it can bail gracefully. Per-handler retry config (`maxAttempts`, `backoff`, `on` predicate) ships in this entry; default is no retry on handler-thrown errors so non-idempotent tasks stay safe. Auto-creates a `svti_tasks` table; periodic cleanup deletes terminal rows past `rowTtl` (default 7 days). Optional `breaker` and `metrics` options.
- **Task runner worker-thread execution** (`worker` option on `register`). For CPU-bound handlers that would otherwise block the event loop, opt in by passing a URL/path to a separate file whose default export is the handler. The runner spawns a per-task thread pool (default size 1, idle timeout 30s, both configurable via `worker.pool`); each thread imports the handler file once and reuses it across runs. The `signal` in the handler context still fires on fence loss and is forwarded across the thread boundary so the worker handler can bail gracefully. Database/Redis pools cannot be shared across threads, so the worker file boots its own. I/O-bound tasks should stay in-process; this is targeted at sync CPU work like image processing or hashing.
- **Redis fence provider for the task runner** (`svelte-adapter-uws-extensions/redis/fence`). Optional `fence` option on `createTaskRunner` adds a second source of truth for "is this attempt's fence still alive." The Postgres row stays canonical; the provider mirrors the fence value to a Redis key with a short TTL refreshed by heartbeat. On every heartbeat tick the runner consults both sources, and either reporting "lost" aborts the handler. Two atomic Lua scripts back the path: heartbeat is `if get == fence then pexpire end`, release is `if get == fence then del end` -- no fence held by another owner can be released or refreshed by accident. Primary value is force-takeover detection (drain an instance, immediately kick its in-flight tasks off without waiting for the Postgres deadline) rather than raw heartbeat throughput.
- **Task runner async path** (`enqueue` + `await` on `createTaskRunner`). `enqueue(name, opts)` inserts a row with `status='pending'` and returns the `taskId` immediately without running the handler. A new dispatch sweep (default every 5s, configurable via `dispatchInterval` / `dispatchBatchSize`) claims pending rows via `FOR UPDATE SKIP LOCKED`, transitions them to `running`, and runs the handler in the background on any live instance with the handler registered. `await(taskId, opts?)` polls the row until terminal and returns the result or throws the stored error; runner-level `awaitPollInterval` / `awaitTimeout` defaults are overridable per call. Use cases: HTTP handlers that respond 202 immediately, cross-instance work distribution where the web tier enqueues and a worker tier processes, fire-and-forget background jobs. Pending rows whose handler is unknown locally are left in `running` for another instance to reclaim via the recovery sweep. Internal cleanup: `runRegisteredTask` was refactored to accept an optional starting fence so dispatch and recovery skip the redundant rearm UPDATE that B2a's recovery path was doing.
- **Replay buffers: `gap(topic, lastSeenSeq)` probe** on both Redis and Postgres backends. Returns `{ truncated, missingFrom }` so a consumer can decide between an incremental `since()` fetch and a full reload without driving a WebSocket replay. The same truncation logic that the existing `replay()` method uses internally is now callable: a non-zero `lastSeenSeq` whose successor is no longer in the buffer (either the buffer was trimmed past it, or the buffer is empty and the seq counter has advanced) returns `truncated: true` with `missingFrom = lastSeenSeq + 1`; otherwise `truncated: false, missingFrom: null`. `lastSeenSeq` of 0 short-circuits to "not truncated" (a fresh client has no history to lose). The Redis path uses `ZRANGEBYSCORE` from `lastSeenSeq + 1` and skips corrupt entries the same way `replay()` does; the Postgres path uses an indexed `SELECT seq ... WHERE seq >= $2 ORDER BY seq ASC LIMIT 1` lookup on `(topic, seq)`. README also gains an "Aggregate vs broadcast topics" subsection: replay buffers should be sized per-aggregate (`auction:a1b2`, `chat:room-7`), not per broadcast channel, so the size budget reflects a real history window per aggregate and gap detection is actionable.
- **Pub/sub bus: first-class degraded / recovered events** (`systemChannel`, `onDegraded`, `onRecovered` options on `createPubSubBus`). When the bus shares a circuit breaker with the rest of the extensions, the bus now subscribes to the breaker and auto-emits `degraded` / `recovered` events on a configurable system topic (default `'__realtime'`) so connected clients can show a stale-data banner without any user-side wiring. The event payload is `{ at: <epoch ms> }`. Auto-emission is local-only -- Redis is what's degraded, so the event reaches local clients via the underlying platform without attempting a relay; each instance reports its own breaker state to its own clients. Set `systemChannel: null` or `false` to disable auto-emission; the `onDegraded` / `onRecovered` callbacks remain available for server-side reactions (logging, alerts) regardless. Replaces the manually wired `breaker.onStateChange` -> `distributed.publish('__system', ...)` pattern that the README previously documented; the manual pattern still works for users who want different topics, custom payloads, or cross-instance forwarding.
- **Circuit breaker: `subscribe(handler)`** on `createCircuitBreaker`. Returns an unsubscribe function and supports multiple listeners simultaneously. The constructor's `onStateChange` keeps working (it is added as an initial listener internally) so existing call sites are unchanged. A throwing listener can no longer break the others -- listener errors are caught and swallowed inside `transition()`. This lifts the single-callback constraint that previously blocked layered consumers like the pub/sub bus's degraded-event emitter from coexisting with a user-supplied `onStateChange`.
- **Presence: `metrics()` snapshot** on `createPresence`. Synchronous accessor returning `{ totalOnline, heartbeatLatencyMs, staleCleanedTotal }`. `totalOnline` sums unique-users-per-topic across topics this instance is locally tracking. `heartbeatLatencyMs` is the duration of the most recent heartbeat tick, useful as a rough Redis-health indicator. `staleCleanedTotal` is the cumulative count of stale fields removed by the heartbeat-driven cleanup script since startup. The same numbers are exposed as Prometheus gauges (`presence_total_online{topic}`, `presence_heartbeat_latency_ms`) when a metrics registry is attached. The heartbeat tick now always observes the cleanup script result so the internal counter stays accurate even when no Prometheus registry is wired.
- **Presence: `keyspaceNotifications: true` mode** on `createPresence`. Optional opt-in that `psubscribe`-s to `__keyevent@*__:expired` so a sync-only observer of a topic gets an empty `list` event the moment that topic's presence hash expires in Redis -- catches the instance-died scenario where the only tracker crashed and the broadcast `leave` event never fired. Requires `CONFIG SET notify-keyspace-events Ex` (or any flagset including `K`/`E` and `x`); `psubscribe` failure is logged once and the rest of the tracker keeps working without the keyspace branch. Scope is hash-key expiry only; per-field expiry of stale entries continues to run via the existing heartbeat cleanup script. Per-field hash TTLs via Redis 7.4 `HEXPIRE` are deliberately out of scope (different storage layout, future item).
- **Replay buffer: `publishIdempotent` (stream backend only)**. Caller-provided idempotency via `{ producerId, requestId }`. Implemented as a Lua-atomic `HGET` (cache lookup) + `INCR` + `XADD` + `HSET` -- on a repeat tuple within `idempotencyTtl` (default 48 hours), returns the cached seq, skips the XADD, and skips the local broadcast. The seq counter only advances on fresh writes so duplicate retries do not trigger false-positive truncation. Dedup cache is keyed `{prefix}replay:idmp:{producerId}:{topic}`. Pairs with the durable task runner: a task that publishes to replay can pass its task id as `requestId` so worker-crash retries don't double-publish. Native Redis 8.6+ `XADD IDMP` was evaluated and rejected for our seq-counter architecture (pre-INCR before XADD wastes seqs on duplicates and causes false-positive truncation; the manual Lua approach has no such waste).
- **Replay buffer: `storage: 'stream'` backend** on `createReplay`. Opt-in dispatch to a Redis Streams implementation (`XADD`/`XRANGE` with `<seq>-0` IDs) parallel to the default sorted-set one. Same external contract -- same `publish` / `seq` / `gap` / `since` / `replay` / `clear` methods, same `durability: 'replicated'` mode, same metrics. Listpack encoding is more compact than sorted-set encoding, and `XRANGE` against `(seq-0` filters natively by sequence number. Both backends use the same seq counter but different buf-key prefixes (`replay:buf:` vs `replay:streambuf:`) so they can coexist on the same Redis without WRONGTYPE collisions. No built-in migration helper between backends; a single topic should pick one and stay there.
- **Postgres job queue** (`svelte-adapter-uws-extensions/postgres/jobs`). `createJobQueue(pgClient)` is a minimal `SELECT ... FOR UPDATE SKIP LOCKED` queue for vanilla Postgres 9.5+ -- no extensions required. `enqueue(queue, payload)` / `claim(queue, { batchSize, visibilityTimeoutMs })` / `complete(idOrIds)` / `fail(idOrIds)` / `extend(idOrIds, ms)` / `pending(queue?)` / `clear(queue?)`. Visibility timeout means a worker that crashes mid-processing has its claim auto-expire so another worker can pick the job up. Max-attempts and dead-letter behavior are deliberately NOT baked in -- the `attempts` counter is exposed on every claim, callers track it and decide when to give up. Auto-creates the `svti_jobs` table with two partial indexes (queue+id where unclaimed, claimed_until where claimed). Pairs with `createTaskRunner` as the lighter "ingest event, defer work" producer.
- **Redis Functions wrapper** (`svelte-adapter-uws-extensions/redis/functions`). `createFunctionLibrary(client, code)` parses the library name from the `#!lua name=<libname>` shebang and exposes `load()` (FUNCTION LOAD REPLACE), `call(funcName, { keys, args })` (FCALL), and `delete()` (FUNCTION DELETE). Versioned, hot-reloadable server-side scripts: roll forward without an app deploy. Requires Redis 7+; activate runs `INFO server` and throws on older servers (no EVALSHA fallback -- maintaining each function in two forms is more cost than benefit; on Redis 6 use `redis.eval` directly).
- **Sharded pub/sub bus** (`svelte-adapter-uws-extensions/redis/sharded-pubsub`). `createShardedBus` is the SPUBLISH/SSUBSCRIBE variant of `createPubSubBus` for Redis Cluster deployments with many fine-grained topics. Per-topic channels, dynamic subscription via `follow(topic)` / `unfollow(topic)` (or `bus.hooks` against WebSocket subscribe/unsubscribe), refcounted SSUBSCRIBE on first follower per channel and SUNSUBSCRIBE on last out. Optional `shardKey` groups topics into the same channel (e.g. `(topic) => topic.split(':')[0]`). Requires Redis 7+; activate runs `INFO server` and throws on older servers (use `createPubSubBus` for Redis 6 / older Valkey). Most apps still want the simpler `createPubSubBus` -- the sharded bus is for cluster + narrow-audience topics where bandwidth saving outweighs the management overhead.
- **Redis replay: `durability: 'replicated'` mode** on `createReplay`. After each write, runs `WAIT minReplicas replicationTimeoutMs`; throws `ReplicationTimeoutError` and skips the local broadcast when fewer replicas ack within the timeout. The data is on the master regardless of outcome (other instances doing `replay()` see it); only the local broadcast is suppressed so live consumers aren't committed to state that could be lost if the master fails before replicas catch up. WAIT command errors bubble up as breaker failures; under-acked timeouts do not trip the breaker -- it's a separate signal layer.
- **LISTEN/NOTIFY: leader-elected single-listener mode** (`multiListener: 'advisory'` on `createNotifyBridge`). Each replica polls `pg_try_advisory_lock(lockId)` on a dedicated connection; the winner holds the LISTEN connection and forwards notifications, others stay idle followers. On leader connection loss the session-scoped lock auto-releases and another replica picks up on its next poll. The leader publishes *with* relay so a cross-instance pub/sub bus fans out to non-leader replicas; the existing default (`multiListener: 'all'`) keeps publishing with `relay: false` because every replica has its own LISTEN. Avoids N LISTEN connections in an N-replica deployment.
- **Admission control** (`svelte-adapter-uws-extensions/admission`). Pressure-aware companion to the circuit breaker. `createAdmissionControl({ classes })` accepts a per-class block rule (either a list of pressure reasons or a predicate function) and exposes `shouldAccept(className, platform)`, which reads `platform.pressure` from the adapter and returns `false` when the class should be shed. Where the breaker answers "is the backend up?", admission control answers "are we OK to take more work right now?" -- using the worker-local memory / publish-rate / subscriber-ratio signals to gate non-critical work before it reaches a backend. Requires `svelte-adapter-uws >= 0.5.0-next.1` for the `platform.pressure` getter. The reason-precedence math (memory > publish rate > subscribers) lives in the adapter; this controller just maps the resolved reason to a per-class accept/reject decision. Optional Prometheus metrics: `admission_accepted_total{class}` and `admission_rejected_total{class, reason}` track admission decisions for ops dashboards.

### Metrics

- `pubsub_degraded_total` and `pubsub_recovered_total` counters track auto-emitted system events on the bus.
- `presence_total_online{topic}` gauge mirrors the per-topic unique-user count in real time.
- `presence_heartbeat_latency_ms` gauge tracks heartbeat tick duration.
- `presence_keyspace_cleanups_total` counter tracks how many topic hash expiries triggered a local empty-list emit (only meaningful when `keyspaceNotifications: true`).
- `admission_accepted_total{class}` and `admission_rejected_total{class, reason}` counters track admission decisions per class, with the rejected counter labeled by the pressure reason that caused rejection.

### Changed

- **Postgres table prefix is now `svti_`** (was `ws_`). Default table names: `svti_replay`, `svti_replay_seq`, `svti_idempotency`, `svti_jobs`, `svti_tasks`. Override the prefix via the `table` option on each module. The new prefix lines up with the `svti.me` short-link domain so the schema is greppable back to the docs.
- **Primary key columns follow the `[tablename]_id` rule.** `svti_replay_id` (was `id` via the `pkCol` option), `svti_jobs_id` (was `id`), `svti_tasks_id` (was `task_id`). The natural-key PK on idempotency renamed to `svti_idempotency_key` (was `key`); the FK column on tasks matches it (was `idempotency_key`). JS-level field names on row objects stay ergonomic via SQL `AS` aliases: `job.id`, `task.id`, `task.idempotency_key` are unchanged in code that reads from claim/dispatch results.
- **Idempotency API parameter renamed `key` -> `idempotencyKey`.** `idempotency.acquire(idempotencyKey)` and `idempotency.purge(idempotencyKey)` now match the existing `idempotencyKey` option name on the task runner so the term is consistent across the package.
- **Replay timestamp column renamed `created_date` -> `created_at`** for consistency with `created_at` / `updated_at` columns elsewhere.
- **Race-safe `CREATE TABLE IF NOT EXISTS`.** Concurrent first-use of a fresh module instance no longer fails on `pg_class_relname_nsp_index` collisions when two connections both pass the existence check. Each `CREATE TABLE` / `CREATE INDEX` is wrapped per-statement in a try/catch for SQLSTATE `23505`, `42P07`, `42710`. Affects `replay`, `idempotency`, `jobs`, `tasks`.
- **TOCTOU fix in Redis `presence.destroy()` and `groups.destroy()`.** `subscriber` is now captured into a local before being nulled, so a `subscriber.quit()` rejection that fires after the null assignment no longer throws `TypeError` from inside the `.catch()` handler.

### Migration

Existing deployments with default table names need to either:

1. **Override the `table` option** on each module to keep the old `ws_*` names: `createReplay(pg, { table: 'ws_replay' })`, etc. Zero-downtime, no SQL needed.
2. **Rename tables in place** before deploying:
   ```sql
   ALTER TABLE ws_replay RENAME TO svti_replay;
   ALTER TABLE ws_replay RENAME COLUMN id TO svti_replay_id;
   ALTER TABLE ws_replay RENAME COLUMN created_date TO created_at;
   ALTER TABLE ws_replay_seq RENAME TO svti_replay_seq;
   ALTER TABLE ws_idempotency RENAME TO svti_idempotency;
   ALTER TABLE svti_idempotency RENAME COLUMN key TO svti_idempotency_key;
   ALTER TABLE ws_jobs RENAME TO svti_jobs;
   ALTER TABLE svti_jobs RENAME COLUMN id TO svti_jobs_id;
   ALTER TABLE ws_tasks RENAME TO svti_tasks;
   ALTER TABLE svti_tasks RENAME COLUMN task_id TO svti_tasks_id;
   ALTER TABLE svti_tasks RENAME COLUMN idempotency_key TO svti_idempotency_key;
   ```

The JS API for `enqueue()` / `claim()` / `await()` results is unchanged (SQL aliases preserve the field names callers read from rows). The only JS-level source change is the `idempotency.acquire(key)` parameter, now `idempotency.acquire(idempotencyKey)` -- a function-argument rename, transparent at call sites.

---

## [0.4.2] - 2026-04-11

### Added

- **Testing entry point** (`svelte-adapter-uws-extensions/testing`). Exports the same in-memory mocks used by the extensions' own test suite: `mockRedisClient`, `mockPlatform`, `mockWs`, `mockPgClient`. Enables downstream projects to test extension-consuming code without running Redis or Postgres.

### Changed

- **Rate limiter: versioned Redis keys.** Bucket keys now include a version prefix (`v1:ratelimit:{key}` instead of `ratelimit:{key}`). When the Lua script algorithm changes in a future release, the version will be bumped so rolling deployments with different script versions use separate key spaces. Old-version keys expire naturally via their existing TTL. No migration needed -- existing unversioned keys will expire on their own.
- **Notify bridge: warn on custom parse errors.** When a custom `parse` function throws, the bridge now logs a `console.warn` with the channel name and error message. Previously, custom parse errors were counted in the `notify_parse_errors_total` metric but produced no log output, making them invisible without Prometheus.
- **README: degradation notification pattern.** Added a "Notifying clients of degradation" subsection under Failure handling, showing how to wire `onStateChange` to publish a system-level event so clients can surface a stale-data banner when Redis pub/sub fails.

---

## [0.4.1] - 2026-04-10

### Fixed

- **Redis replay: gap detection when buffer is empty.** When all buffered entries had been trimmed (by size cap or TTL expiry) but the sequence counter had advanced past the client's `sinceSeq`, `replay()` did not fire a `truncated` event. The client would receive only an `end` marker and assume it was caught up, silently missing messages. Now falls back to checking the seq counter when the sorted set is empty, matching the Postgres replay behavior that already handled this case.

### Changed

- **README: failure handling section.** Added a top-level table describing per-extension degradation behavior when the circuit breaker trips.
- **README: pub/sub bus.** Documented echo suppression mechanism and microtask batching (multiple publishes in one tick become a single Redis pipeline).
- **README: presence.** Documented staged join with rollback on failure, atomic leave check via Lua script, and zombie cleanup via heartbeat probing (`getBufferedAmount()`) and server-side Lua stale field removal.
- **README: replay buffer (Redis and Postgres).** Documented atomic sequence numbering, buffer trimming behavior, and gap detection (including the empty-buffer edge case).
- **README: LISTEN/NOTIFY bridge.** Strengthened 8KB payload limit documentation and added guidance on when to use LISTEN/NOTIFY vs Redis pub/sub.

---

## [0.4.0] - 2025-03-19

### Breaking Changes

#### Replay (Redis and Postgres)

- **`replay()` end event data changed from `null` to `{ reqId }`.** Previously the end marker sent `null` as its data. Now sends `{ reqId: undefined }` (or `{ reqId: 'some-id' }` when a correlation ID is passed). **Action:** update any client-side check from `data === null` to an object check.
- **`replay()` signature accepts optional `reqId` parameter.** `replay(ws, topic, sinceSeq, platform, reqId?)`. Not breaking if you don't pass it, but the wire format above is.
- **New `truncated` event on `__replay:{topic}`.** Sent before replay messages when the buffer has been trimmed past the client's `sinceSeq`. **Action:** handle or ignore this new event in your replay store.

#### Presence

- **Default `select` strips `__`-prefixed and sensitive keys.** Previously the identity function. Now strips keys starting with `__` (e.g. `__subscriptions`, `remoteAddress` injected by core v0.4.0) and keys matching `/token|secret|password|auth|session|cookie|jwt|credential/i`. **Action:** if you relied on these fields in presence data, pass an explicit `select` function.
- **`hooks` now includes `unsubscribe`.** Hooks destructure should become `export const { subscribe, unsubscribe, close } = presence.hooks`. Not strictly breaking (a missing hook is never called), but required for correct single-topic leave on unsubscribe.

#### Cursor

- **Default `select` strips `__`-prefixed and sensitive keys.** Same behavior change as presence above.

#### Groups

- **JOIN_SCRIPT returns array `[status, ...liveMembers]` instead of single integer.** Internal change but affects anyone calling the Lua script directly.

#### Package

- **Peer dependency changed from `svelte-adapter-uws >=0.2.0` to `>=0.4.0`.** Core adapter must be upgraded first.

### Added

#### New Modules

- **Prometheus metrics** (`svelte-adapter-uws-extensions/prometheus`). Zero-dependency metrics registry in Prometheus text exposition format. Zero overhead when not enabled -- every extension uses optional chaining on a nullish reference. Includes `createMetrics()`, counter/gauge/histogram primitives, `mapTopic` for cardinality control, and a built-in uWS HTTP handler.
- **Circuit breaker** (`svelte-adapter-uws-extensions/breaker`). Three-state breaker (healthy/broken/probing) that prevents thundering herd when a backend goes down. All extensions accept an optional `breaker` option. Awaited operations fail fast; fire-and-forget operations (heartbeat, relay, cursor broadcast) are skipped entirely.
- **Shared utilities** -- `shared/time.js` (cached `Date.now()`), `shared/scripts.js` (deduplicated Lua scripts), `shared/errors.js` (`ConnectionError`, `TimeoutError`).

#### All Extensions

- `metrics` option on every extension for Prometheus instrumentation.
- `breaker` option on every extension for circuit breaker integration.

#### Cursor

- `snapshot(ws, topic, platform)` -- send all current cursor positions to a single connection.
- `hooks` helper -- ready-made `subscribe`, `message`, `close` hooks for zero-config cursor tracking.

#### Groups

- `hooks` helper -- ready-made `subscribe`, `unsubscribe`, `close` hooks for zero-config group membership.

#### Presence

- `unsubscribe` hook for single-topic leave when the client unsubscribes (requires core v0.4.0+).
- `updated` event -- broadcast when a user's data changes (deep equality check), instead of silently overwriting.

#### Pub/Sub

- Wrapped platform now relays `batch()` calls to Redis, not just `publish()`.

#### Rate Limiting

- Atomic `ban()` via Lua script -- single round trip instead of multi-step `hget` + `hmset`.

### Changed (Under the Hood)

#### Performance

- Presence heartbeat, cursor broadcast, and groups heartbeat use Redis pipelines instead of individual commands.
- PubSub relay batching via microtask -- multiple publishes within one tick coalesced into a single pipelined round trip.
- Presence `list()` and `count()` use server-side Lua scripts (`LIST_SCRIPT`, `COUNT_DEDUP_SCRIPT`) -- avoids transferring the full hash to Node.
- Groups `count()` uses server-side Lua (`COUNT_SCRIPT`).
- Cached `Date.now()` via `shared/time.js` for hot paths (heartbeats, throttle checks, staleness).
- Replay Lua avoids double `cjson.encode` -- builds JSON payload via string concat.
- Rate limit Lua uses `hmget` (one round trip) instead of 3x `hget`; `hset` instead of deprecated `hmset`.
- Postgres replay publish uses a CTE to combine sequence increment + insert into one query.
- Postgres replay trim is sequence-based (O(1) cutoff calculation) instead of `COUNT(*)` + subquery.
- Postgres replay uses prepared (named) statements.
- Postgres periodic cleanup query rewritten to use `OFFSET`-based cutoff instead of `ROW_NUMBER()` window function.
- `redis.del()` replaced with `redis.unlink()` across all `clear()` methods (non-blocking delete).
- Lua scripts deduplicated into `shared/scripts.js`.

#### Reliability

- Presence join has full rollback (`undoJoin()`) that reverts local state, Redis writes, and compensating events on any failure during the async flow.
- Groups join rolls back the Redis member entry if `ws.subscribe()` fails after the Lua insert.
- `ws.unsubscribe()` wrapped in try/catch throughout groups, presence, and cursor for already-closed connections.
- Presence subscriber auto-closes after 30s of no active topics (idle timeout).
- Cursor cleanup timer is lazy -- starts on first topic, stops when last topic is removed.
- Presence and cursor filter out timestamp-less entries as stale (previously only filtered if timestamp was present and expired).
- Redis client `duplicates` tracking uses `Set` with auto-cleanup on `close`/`end` events.
- Postgres replay cleanup guarded against concurrent runs.

#### Validation and Safety

- Input validation on all construction options (`reconnectInterval`, `throttle`, `ttl`, `memberTtl`, `heartbeat`, `select`, `parse`, etc.).
- `select()` results validated as JSON-serializable at join/update time.
- Sensitive data warnings -- presence and cursor warn once if userData contains keys matching `token`, `secret`, `password`, etc.
- Notify bridge warns when payload approaches Postgres ~8000 byte NOTIFY limit.

---

## [0.1.9] and earlier

See [git history](../../commits/main) for changes prior to 0.4.0.
