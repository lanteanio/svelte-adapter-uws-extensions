# Changelog

All notable changes to `svelte-adapter-uws-extensions` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
