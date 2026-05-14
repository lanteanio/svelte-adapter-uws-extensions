# svelte-adapter-uws-extensions

Redis and Postgres extensions for [svelte-adapter-uws](https://github.com/lanteanio/svelte-adapter-uws).

The core adapter keeps everything in-process memory. That works great for single-server deployments, but the moment you scale to multiple instances you need shared state. This package provides drop-in replacements backed by Redis and Postgres, with the same API shapes you already know from the core plugins.

## What you get

- **Distributed pub/sub** - `platform.publish()` reaches all instances, not just the local one
- **Persistent replay buffers** - messages survive restarts, backed by Redis sorted sets or a Postgres table
- **Cross-instance presence** - who's online across your entire fleet, with multi-tab dedup
- **Distributed rate limiting** - token bucket enforced across all instances via atomic Lua script
- **Distributed broadcast groups** - named groups with membership and roles that span instances
- **Shared cursor state** - ephemeral positions (cursors, selections, drawing strokes) visible across instances
- **Database change notifications** - Postgres LISTEN/NOTIFY forwarded straight to WebSocket clients
- **Prometheus metrics** - expose extension metrics for scraping, zero overhead when disabled

**Upgrading from 0.4.x?** See the [migration guide](./MIGRATION.md) for every breaking change between 0.4.x and 0.5.x.

---

## Table of contents

**Getting started**
- [Installation](#installation)

**Clients**
- [Redis client](#redis-client)
- [Postgres client](#postgres-client)

**Redis extensions**
- [Pub/sub bus](#pubsub-bus)
- [Sharded pub/sub bus](#sharded-pubsub-bus)
- [Replay buffer (Redis)](#replay-buffer-redis)
- [Presence](#presence)
- [Rate limiting](#rate-limiting)
- [Broadcast groups](#broadcast-groups)
- [Cursor](#cursor)

**Postgres extensions**
- [Replay buffer (Postgres)](#replay-buffer-postgres)
- [LISTEN/NOTIFY bridge](#listennotify-bridge)
- [Job queue](#job-queue)

**Cross-backend**
- [Idempotency store](#idempotency-store)
- [Distributed lock](#distributed-lock)
- [Distributed session](#distributed-session)
- [Task runner](#task-runner)

**Observability**
- [Prometheus metrics](#prometheus-metrics)

**Reliability**
- [Failure handling](#failure-handling)
- [Circuit breaker](#circuit-breaker)
- [Admission control](#admission-control)
- [Redis Functions](#redis-functions)

**Operations**
- [Graceful shutdown](#graceful-shutdown)
- [Testing](#testing)

**More**
- [Related projects](#related-projects)
- [License](#license)

---

**Getting started**

## Version compatibility

The three ecosystem packages move together. Bump them as a group:

| `svelte-adapter-uws` | `svelte-realtime` | `svelte-adapter-uws-extensions` | Notes |
|---|---|---|---|
| `^0.4.x` | `^0.4.x` | `^0.4.x` | Legacy stable |
| `^0.5.0` | `^0.5.0` | `^0.5.0` | Current. Node 22+ required. Redis 7+ for `createShardedBus` / `createFunctionLibrary`. See `MIGRATION.md` if upgrading from 0.4. |

Mixed-version installs are rejected at install time with a peer-dep warning.

## Installation

```bash
npm install svelte-adapter-uws-extensions ioredis
```

Postgres support is optional:

```bash
npm install pg
```

Requires `svelte-adapter-uws >= 0.2.0` as a peer dependency.

---

**Clients**

## Redis client

Factory that wraps [ioredis](https://github.com/redis/ioredis) with lifecycle management. All Redis extensions accept this client.

```js
// src/lib/server/redis.js
import { createRedisClient } from 'svelte-adapter-uws-extensions/redis';

export const redis = createRedisClient({
  url: 'redis://localhost:6379',
  keyPrefix: 'myapp:'   // optional, prefixes all keys
});
```

#### Options

| Option | Default | Description |
|---|---|---|
| `url` | `'redis://localhost:6379'` | Redis connection URL |
| `keyPrefix` | `''` | Prefix for all keys |
| `autoShutdown` | `true` | Disconnect on `sveltekit:shutdown` |
| `options` | `{}` | Extra ioredis options |

#### API

| Method | Description |
|---|---|
| `redis.redis` | The underlying ioredis instance |
| `redis.key(k)` | Returns `keyPrefix + k` |
| `redis.duplicate(overrides?)` | New connection with same config. Pass ioredis options to override defaults. |
| `redis.quit()` | Gracefully disconnect all connections |

---

## Postgres client

Factory that wraps [pg](https://github.com/brianc/node-postgres) Pool with lifecycle management. Two construction modes:

```js
// src/lib/server/pg.js
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';

// (1) Owned-pool: the client constructs and owns its own pg.Pool.
export const pg = createPgClient({
  connectionString: 'postgres://localhost:5432/mydb'
});

// (2) Wrapped-pool: pass an existing pg.Pool to share with raw `pg` use
//     elsewhere, another framework integration, or a different module
//     in the same app - single connection footprint against the database.
import pg from 'pg';
const pool = new pg.Pool({ connectionString: env.DATABASE_URL });

export const wrapped = createPgClient({ pool });
// pg.end() is a no-op now - the caller owns `pool`'s lifecycle.
```

#### Options

| Option | Default | Description |
|---|---|---|
| `connectionString` | - | Postgres connection string. Required UNLESS `pool` is provided. |
| `pool` | - | An existing `pg.Pool` to wrap. When provided, `autoShutdown` defaults to `false` and `end()` becomes a no-op. Pass `connectionString` alongside `pool` if you also need `createClient()`. |
| `autoShutdown` | `true` (owned pool) / `false` (wrapped pool) | Disconnect on `sveltekit:shutdown`. |
| `options` | `{}` | Extra pg Pool options. Ignored when `pool` is provided. |

#### API

| Method | Description |
|---|---|
| `pg.pool` | The underlying pg Pool (provided or owned). |
| `pg.query(text, values?)` | Run a query. |
| `pg.createClient()` | New standalone pg.Client with the same config. Throws when only `pool` was provided - pass `connectionString` alongside `pool` to enable this path. |
| `pg.end()` | Gracefully close the pool. No-op when wrapping an externally-provided pool. |

---

**Redis extensions**

## Pub/sub bus

Distributes `platform.publish()` calls across multiple server instances via Redis pub/sub. Each instance publishes locally AND to Redis. Incoming Redis messages are forwarded to the local platform with echo suppression (messages originating from the same instance are dropped on receive, keyed by a per-process instance ID).

Multiple `publish()` calls within the same event-loop tick are coalesced into a single Redis pipeline via microtask batching. This means a form action that publishes to three topics results in one pipelined round trip, not three independent commands.

#### Setup

```js
// src/lib/server/bus.js
import { redis } from './redis.js';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';

export const bus = createPubSubBus(redis);
```

#### Usage

```js
// src/hooks.ws.js
import { bus } from '$lib/server/bus';

let distributed;

export async function open(ws, ctx) {
  // Activates the Redis subscriber (idempotent) AND subscribes the
  // connection to the bus's systemChannel so degraded / recovered
  // events reach it. One line, zero ceremony.
  await bus.hooks.open(ws, ctx);
  // Wrapped platform: publish() reaches local clients AND every other instance.
  distributed = bus.wrap(ctx.platform);
}

export function message(ws, { data, platform }) {
  const msg = JSON.parse(Buffer.from(data).toString());
  distributed.publish('chat', 'message', msg);
}
```

If your open hook does nothing else, drop the ceremony entirely:

```js
// src/hooks.ws.js
import { bus } from '$lib/server/bus';

export const { open } = bus.hooks;
```

#### Wire-batched publish (`publishBatched`)

When a single request publishes many events at once - bulk imports, room state resets, audit fanouts - use `publishBatched` instead of a `publish` loop. It ships **one** Redis envelope for the whole batch, and receivers fan out via `platform.publishBatched` so each subscriber sees **one** WebSocket frame per call.

```js
distributed.publishBatched([
  { topic: 'org:42:items', event: 'updated', data: a },
  { topic: 'org:42:items', event: 'updated', data: b },
  { topic: 'org:42:audit',  event: 'created', data: c }
]);
```

Trade-offs vs `wrapped.publish` in a tight loop:

- **Redis publish count drops linearly with batch size.** A 50-message batch is one PUBLISH on the wire instead of 50 - around 50x reduction on the bulk-import profile, ~3x on small disjoint batches (measured in `bench/01-publish-batched-bus.mjs`).
- **Per-subscriber wire frames drop too.** A subscriber receives one frame containing N events instead of N frames containing one event each.
- **Per-message `relay: false` is honored.** Flagged messages still publish locally but are excluded from the Redis envelope.

`wrapped.batch(messages)` is left in place for callers that explicitly want N independent publishes; it does not produce wire batching. Use `publishBatched` whenever you want one frame per subscriber per call.

#### Options

| Option | Default | Description |
|---|---|---|
| `channel` | `'uws:pubsub'` | Redis channel name |
| `systemChannel` | `'__realtime'` | Topic for auto-emitted `degraded` / `recovered` events. `null` or `false` to disable. Requires a `breaker` |
| `onDegraded` | - | Server-side handler invoked once when the breaker leaves the healthy state |
| `onRecovered` | - | Server-side handler invoked once when the breaker returns to the healthy state |
| `maxEnvelopeBytes` | `1048576` (1 MB) | Reject inbound bus envelopes larger than this before `JSON.parse` runs. Defends against a hostile co-tenant or compromised peer flooding the bus with oversized payloads. |
| `allowSystemTopics` | `false` | When `false` (default), inbound envelopes addressed to `__`-prefixed topics are dropped; the configured `systemChannel` (default `__realtime`) remains in an explicit allowlist so the bus's own degraded / recovered events still flow. Closes the bus-injection class in shared-Redis deployments where a foreign publisher could otherwise inject forged `__signal:*` / `__rpc` / plugin-internal frames into the local platform. Apps that legitimately bus-relay user-defined `__`-prefixed topics (rare) can opt back in with `true`. |

See [Notifying clients of degradation](#notifying-clients-of-degradation) for the full pattern.

#### API

| Method | Description |
|---|---|
| `bus.wrap(platform)` | Returns a new Platform whose `publish()`, `batch()`, and `publishBatched()` send to Redis + local. Other Platform methods (`send`, `sendCoalesced`, `request`, `pressure`, etc.) pass through unchanged |
| `bus.hooks` | Ready-made WebSocket hooks. `open(ws, ctx)` activates the Redis subscriber (idempotent) AND subscribes `ws` to the bus's `systemChannel` so `degraded` / `recovered` events are delivered. Destructure for one-line `hooks.ws.js` wiring: `export const { open } = bus.hooks;` |
| `bus.activate(platform)` | Start the Redis subscriber (idempotent). Equivalent to the subscriber half of `bus.hooks.open`; prefer `bus.hooks.open` for new code |
| `bus.deactivate()` | Stop the subscriber |

---

## Sharded pub/sub bus

`createPubSubBus` uses one channel for every message, so in a Redis Cluster every node receives every publish via the cluster bus. For deployments with many fine-grained topics where each subscriber only cares about a small subset (chat with millions of rooms, per-document collaboration, per-user feeds), most of that fan-out is wasted bandwidth.

`createShardedBus` (`svelte-adapter-uws-extensions/redis/sharded-pubsub`) is the SPUBLISH/SSUBSCRIBE variant: per-topic channels, dynamic subscription via `follow(topic)` / `unfollow(topic)`, no wildcards. In Redis Cluster, messages stay on the shard that owns each channel rather than fanning out to every node.

**Requires Redis 7+.** `activate()` runs `INFO server` and throws on older servers; use `createPubSubBus` for Redis 6 / older Valkey.

#### Setup

```js
import { createShardedBus } from 'svelte-adapter-uws-extensions/redis/sharded-pubsub';

export const bus = createShardedBus(redis, {
  shardKey: (topic) => topic.split(':')[0]  // optional grouping
});
```

#### Usage

```js
// src/hooks.ws.js
import { bus } from '$lib/server/bus';

let distributed;

export async function open(ws, { platform }) {
  await bus.activate(platform);
  distributed = bus.wrap(platform);
}

// Wire follow / unfollow against WebSocket subscribe / unsubscribe:
export const { subscribe, unsubscribe, close } = bus.hooks;

// Or manually:
// await bus.follow('chat:room-7');
// distributed.publish('chat:room-7', 'msg', { text: 'hi' });
// await bus.unfollow('chat:room-7');
```

`bus.hooks` is the recommended path - it tracks per-`ws` subscription state and refcounts so the bus only `SSUBSCRIBE`s on the first follower per channel and `SUNSUBSCRIBE`s on the last one out.

#### Bulk follow (`followBatch`, `bus.hooks.subscribeBatch`)

`bus.followBatch(topics)` groups the input topics by shard channel and `SSUBSCRIBE`s any new channels in one round trip. Pairs with the adapter's `subscribeBatch` hook (`hooks.ws.subscribeBatch`) so an N-topic subscribe batch lands as one round-trip-per-channel rather than one round-trip-per-topic. With the adapter's client-side coalescing (next.7+), the win covers initial-mount subscribes too, not just reconnect resubscribes.

```js
// Today (works, but N round trips):
export const subscribeBatch = async (ws, topics) => {
  for (const topic of topics) await bus.follow(topic);
};

// Recommended - one round trip per shard channel:
export const { subscribeBatch } = bus.hooks;
```

Single `follow` / `unfollow` keep their existing semantics; `followBatch` is purely additive. Refcount semantics for individual topics match `follow`: each call to `followBatch` bumps every input topic's refcount by 1, and only channel transitions trigger Redis traffic. Empty arrays no-op; duplicate topics in the input collapse to one refcount bump.

`bus.hooks.subscribeBatch` skips `__`-prefixed topics like the per-topic `subscribe` hook does, and skips topics this `ws` is already following so a duplicate batch from a flaky reconnect doesn't leak refcount.

#### Wire-batched publish (`publishBatched`)

`distributed.publishBatched(messages)` ships **one** SPUBLISH envelope per shard channel per call. Receivers fan out via `platform.publishBatched` so each follower sees **one** WebSocket frame per call.

```js
distributed.publishBatched([
  { topic: 'chat:room1', event: 'msg',     data: a },
  { topic: 'chat:room2', event: 'msg',     data: b },
  { topic: 'audit:org1', event: 'created', data: c }
]);
// With shardKey: (t) => t.split(':')[0], this is two SPUBLISH envelopes
// (one to the 'chat' shard channel, one to 'audit') instead of three.
```

Same trade-offs as the unsharded bus: linear Redis-publish-count reduction with batch size, per-subscriber wire frames drop from N to 1, per-message `relay: false` honored. Use `publishBatched` for bulk operations; `wrapped.batch(messages)` remains a per-event loop for callers that explicitly want N separate publishes.

#### Options

| Option | Default | Description |
|---|---|---|
| `channelPrefix` | `'uws:sharded:'` | Prefix for sharded pub/sub channels |
| `shardKey` | `(topic) => topic` | Map a topic to a shard label. The channel is `channelPrefix + shardKey(topic)`. Default: identity (one channel per topic). |
| `maxEnvelopeBytes` | `1048576` (1 MB) | Reject inbound bus envelopes larger than this before `JSON.parse` runs. Defends against a hostile co-tenant or compromised peer flooding the cluster bus with oversized payloads. |
| `allowSystemTopics` | `false` | When `false` (default), inbound envelopes addressed to `__`-prefixed topics are dropped. Closes the bus-injection class in shared-Redis deployments. Apps that legitimately bus-relay user-defined `__`-prefixed topics (rare) can opt back in with `true`. |

#### When to use which bus

| | `createPubSubBus` | `createShardedBus` |
|---|---|---|
| Redis version | any | 7+ |
| Topology | standalone or cluster | meaningful only in cluster |
| Channel model | one shared channel | per-topic (or per shard) |
| Subscription | every instance auto-subscribes | dynamic via `follow` / `hooks` |
| Best fit | most apps; broad-interest topics | many fine-grained topics with narrow audiences |

If you don't have a concrete cluster + fine-grained-topics use case, `createPubSubBus` is simpler and sufficient.

---

## Replay buffer (Redis)

Same API as the core `createReplay` plugin, but backed by Redis sorted sets. Messages survive restarts and are shared across instances.

Sequence numbers are incremented atomically via a Lua script (`INCR` + `ZADD` + trim in a single `EVAL`), so concurrent publishes from multiple instances produce strictly ordered, gap-free sequences per topic. When the buffer exceeds `size`, the oldest entries are removed inside the same Lua script - no second round trip required.

When a client requests replay, the buffer checks whether the client's last-seen sequence is older than the oldest buffered entry. If it is (the buffer was trimmed past the client's position), a `truncated` event fires on `__replay:{topic}` before any `msg` events, so the client knows it missed messages and can do a full reload. This also fires when the buffer is completely empty but the sequence counter has advanced past the client's position (e.g. all entries expired via TTL).

The same gap state is exposed as a callable: `gap(topic, lastSeenSeq)` returns `{ truncated, missingFrom }` without driving a full WebSocket replay. Useful for SSR loaders that want to decide between an incremental `since()` fetch and a full reload before the page even opens its socket.

#### Aggregate vs broadcast topics

Replay buffers track one sequence per topic, so the topic boundary is also the gap-detection boundary. Map topics to **aggregates** (`auction:a1b2`, `chat:room-7`, `doc:abc123`) rather than broadcast channels (`auctions:all`, `chat:everyone`). With one topic per aggregate, the buffer size budget covers a real history window per aggregate and gap detection is meaningful: if a client missed seq 14 of `chat:room-7`, you know exactly which room to refetch. With one broadcast topic, a hot aggregate can rotate the buffer past every other aggregate's history within seconds, so any reconnecting client looks "truncated" even when the aggregate they care about hasn't changed in an hour.

#### Setup

```js
// src/lib/server/replay.js
import { redis } from './redis.js';
import { createReplay } from 'svelte-adapter-uws-extensions/redis/replay';

export const replay = createReplay(redis, {
  size: 500,
  ttl: 3600  // expire after 1 hour
});
```

#### Usage

```js
// In a form action or API route
export const actions = {
  send: async ({ request, platform }) => {
    const data = Object.fromEntries(await request.formData());
    const msg = await db.createMessage(data);
    await replay.publish(platform, 'chat', 'created', msg);
  }
};
```

```js
// In +page.server.js
export async function load() {
  const messages = await db.getRecentMessages();
  return { messages, seq: await replay.seq('chat') };
}
```

```js
// In hooks.ws.js - handle replay requests
export async function message(ws, { data, platform }) {
  const msg = JSON.parse(Buffer.from(data).toString());
  if (msg.type === 'replay') {
    await replay.replay(ws, msg.topic, msg.since, platform);
    return;
  }
}
```

#### Session resumption (`resumeHook`)

The adapter's WebSocket `resume` hook fires on reconnect when the client presents per-topic `lastSeenSeqs` from `sessionStorage`. `resumeHook()` returns a hook function that drives gap-fill across every topic the client cared about, in one line:

```js
// src/lib/server/replay.js
export const replay = createReplay(redis);

// src/hooks.ws.js
import { replay } from '$lib/server/replay';
export const resume = replay.resumeHook();
```

The returned hook iterates the client's `lastSeenSeqs` and calls `replay.replay(ws, topic, sinceSeq, platform)` per topic. Per-topic truncation detection still happens inside `replay()` - a client whose buffer rolled gets a `truncated` event on `__replay:{topic}` so it can do a full reload for that aggregate while other topics continue with incremental gap-fill.

For finer control - custom truncation handling, gathering several gap-fills before flushing, mixing in other resume work - compose by hand:

```js
export async function resume(ws, { lastSeenSeqs, platform }) {
  for (const [topic, sinceSeq] of Object.entries(lastSeenSeqs)) {
    await replay.replay(ws, topic, sinceSeq, platform);
  }
  // ... your own resume work alongside replay
}
```

The same `resumeHook()` is available on the Postgres backend; behavior is identical.

#### Options

| Option | Default | Description |
|---|---|---|
| `storage` | `'sortedset'` | Backend: `'sortedset'` (default) uses ZADD; `'stream'` uses XADD. See [Stream backend](#stream-backend). |
| `size` | `1000` | Max messages per topic |
| `ttl` | `0` | Key expiry in seconds (0 = never) |
| `durability` | - | Set to `'replicated'` for per-publish replication signalling. See [Replicated durability](#replicated-durability). |
| `minReplicas` | `1` | Minimum replicas that must ack (only with `durability: 'replicated'`). |
| `replicationTimeoutMs` | `1000` | Per-publish replication timeout in ms. `0` blocks indefinitely (Redis WAIT semantics). |

#### Stream backend

`storage: 'stream'` dispatches to a Redis Streams implementation (`XADD`/`XRANGE`) instead of the default sorted-set one. Same external contract - the same `publish` / `seq` / `gap` / `since` / `replay` / `clear` methods, same `durability: 'replicated'` mode, same metrics. Two changes under the hood:

- Listpack encoding is more compact than sorted-set encoding for typical message shapes - meaningfully smaller memory for buffers in the thousands of entries per topic.
- Stream IDs are `<seq>-0` where `seq` is the same INCR counter the sorted-set backend uses. `XRANGE` against `(seq-0` filters natively by sequence number, so range queries skip the app-side scan the sorted-set backend does for some paths.

```js
const replay = createReplay(redis, {
  storage: 'stream',
  size: 10000
});
```

Both backends use the same seq-counter key (`{prefix}replay:seq:{topic}`) but different buf-key prefixes (`replay:buf:{topic}` for sorted-set, `replay:streambuf:{topic}` for stream), so they can coexist on the same Redis without WRONGTYPE collisions. A single topic should pick one backend at startup and stay there - there is no built-in migration helper for switching an existing topic from one backend to the other (greenfield deployments don't need it; if you have one in flight and need to migrate, drain consumers, copy entries with a one-off script, and switch).

The stream backend works on Redis 5+; listpack encoding is the Redis 7+ default that delivers the memory win.

#### Idempotent publish (stream backend only)

For producers that need at-most-once semantics under retry, the stream backend exposes `publishIdempotent`:

```js
const replay = createReplay(redis, { storage: 'stream' });

const { seq, isDuplicate } = await replay.publishIdempotent(platform, 'orders', 'created', order, {
  producerId: 'order-service',
  requestId: order.clientOrderId  // stable per-operation id supplied by the caller
});
```

On a fresh `(producerId, requestId)` tuple, the call performs the same INCR + XADD + broadcast as `publish()` and stashes `seq` in a per-(producer, topic) dedup hash. On a repeat tuple within `idempotencyTtl` (default 48 hours), the call returns the cached seq, skips the XADD, and skips the local broadcast - the original publish already broadcast to live consumers, and reconnecting consumers pick the entry up via `replay()` from the buffer.

The `seq` counter only advances on fresh writes, so duplicate retries do not introduce gaps that would trigger false-positive truncation events on consumers.

The dedup cache key is `{prefix}replay:idmp:{producerId}:{topic}` - topic-scoped so the same `requestId` can be reused across topics without collision. Override the TTL per call via `opts.idempotencyTtl`, or globally via `idempotencyTtl` on `createReplay`.

This pairs with the durable task runner (`postgres/tasks`): a task that publishes to the replay buffer can pass its task id as `requestId` so worker-crash retries don't double-publish.

#### Replicated durability

For loss-sensitive flows (audit logs, financial events) opt in with `durability: 'replicated'`. After the write to the master, `publish()` runs `WAIT minReplicas replicationTimeoutMs`. If fewer than `minReplicas` replicas ack within the timeout, `publish()` throws `ReplicationTimeoutError` and skips the local broadcast - the data is on the master only, and broadcasting would commit live consumers to state that could be lost if the master fails before replicas catch up.

```js
import { createReplay, ReplicationTimeoutError } from 'svelte-adapter-uws-extensions/redis/replay';

const replay = createReplay(redis, {
  durability: 'replicated',
  minReplicas: 1,
  replicationTimeoutMs: 1000
});

try {
  await replay.publish(platform, 'orders', 'created', order);
} catch (err) {
  if (err instanceof ReplicationTimeoutError) {
    // err.ack, err.minReplicas, err.timeoutMs available for logging
    // Caller decides: retry, fail the request, or accept best-effort
  }
  throw err;
}
```

The data is in the buffer on the master regardless of the WAIT outcome - other instances doing `replay()` will still see it. Only the local broadcast is suppressed when the durability signal fails. WAIT command-level errors (network/protocol) bubble up as the original error and DO count as a circuit-breaker failure; an under-acked timeout is a separate signal layer and does NOT trip the breaker.

#### API

All methods are async (they hit Redis). The API otherwise matches the core plugin exactly:

| Method | Description |
|---|---|
| `publish(platform, topic, event, data)` | Store + broadcast. May throw `ReplicationTimeoutError` when `durability: 'replicated'`. |
| `seq(topic)` | Current sequence number |
| `gap(topic, lastSeenSeq)` | Probe for a buffer gap. Returns `{ truncated, missingFrom }` |
| `since(topic, seq)` | Messages after a sequence |
| `replay(ws, topic, sinceSeq, platform)` | Send missed messages to one client |
| `clear()` | Delete all replay data |
| `clearTopic(topic)` | Delete replay data for one topic |

---

## Presence

Same API as the core `createPresence` plugin, but backed by Redis hashes. Presence state is shared across instances with cross-instance join/leave notifications via Redis pub/sub.

#### Wire shape

Clients see three event types on `__presence:{topic}`. Mirrors the adapter's bundled `createPresence` plugin so a single client decoder handles both single-instance and cluster deployments:

| Event | When | Payload | Direction |
|---|---|---|---|
| `presence_state` | Once on subscribe | `{[userKey]: data}` - flat snapshot of current presence | Server -> single connection |
| `presence_diff` | Microtask-batched after joins / leaves / updates | `{joins: {[key]: data}, leaves: {[key]: data}}` | Server -> topic subscribers |
| `heartbeat` | Per heartbeat interval | `string[]` - array of currently-known user keys | Server -> topic subscribers |

`presence_diff` collapses by key per-tick: if the same user joins and leaves in the same microtask, only the latest op survives on the wire. An update (same user re-joins with different data) appears as a `joins` entry carrying the new data, since clients overwrite their `Map.set` on the same key.

Cross-instance traffic on the dedicated `presence:events:{topic}` Redis pub/sub channel is `{instanceId, topic, event, payload}` with `event` in `'join' | 'leave' | 'updated'`. Receivers route inbound events into their local diff buffer for client fan-out, so clients only ever see the unified `presence_state` / `presence_diff` shape regardless of which instance the change originated on.

Joins are staged with full rollback on failure: local state is set up first, then the Redis hash field is written, then the WebSocket is subscribed. If any step fails (circuit breaker trips, Redis is down, WebSocket closed during an async gap), all prior steps are undone - local maps, the Redis field, and any buffered diff entry are reversed. Compensating join+leave ops on the same key in the same tick collapse to nothing on the wire.

Leaves use an atomic Lua script (`LEAVE_SCRIPT`) that removes this instance's field from the hash and then scans remaining fields for the same user key, ignoring stale entries. Leave is only buffered into the diff when no other instance holds a live entry for that user, preventing premature "user left" notifications in multi-instance deployments.

Zombie cleanup runs on the heartbeat interval. Each tick, every tracked WebSocket is probed via `getBufferedAmount()` - if the call throws, the socket is dead and its presence is removed synchronously before the heartbeat writes to Redis. The heartbeat then refreshes timestamps on all live entries via a Redis pipeline and runs a server-side Lua cleanup script (`CLEANUP_SCRIPT`) that scans the hash and removes any fields whose timestamp exceeds the TTL. This handles crashed instances whose close handlers never fired.

#### Setup

```js
// src/lib/server/presence.js
import { redis } from './redis.js';
import { createPresence } from 'svelte-adapter-uws-extensions/redis/presence';

export const presence = createPresence(redis, {
  key: 'id',
  select: (userData) => ({ id: userData.id, name: userData.name }),
  heartbeat: 30000,
  ttl: 90
});
```

#### Usage

```js
// src/hooks.ws.js
import { presence } from '$lib/server/presence';

export async function subscribe(ws, topic, { platform }) {
  await presence.join(ws, topic, platform);
}

export async function close(ws, { platform }) {
  await presence.leave(ws, platform);
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `key` | `'id'` | Field for user dedup (multi-tab) |
| `select` | strips `__`-prefixed keys | Extract public fields from userData |
| `heartbeat` | `30000` | TTL refresh interval in ms |
| `ttl` | `90` | Per-entry expiry in seconds. Entries from crashed instances expire individually after this period, even if other instances are still active on the same topic. |
| `keyspaceNotifications` | `false` | Subscribe to Redis `__keyevent@*__:expired`. When a presence hash key expires (instance-died scenario), this instance's local subscribers receive an empty `presence_state` event. See [Keyspace cleanup mode](#keyspace-cleanup-mode). |

#### API

| Method | Description |
|---|---|
| `join(ws, topic, platform)` | Add connection to presence |
| `leave(ws, platform, topic?)` | Remove from a specific topic, or all topics if omitted |
| `sync(ws, topic, platform)` | Send list without joining |
| `list(topic)` | Get current users |
| `count(topic)` | Count unique users |
| `metrics()` | Synchronous snapshot: `{ totalOnline, heartbeatLatencyMs, staleCleanedTotal }`. See [Metrics snapshot](#metrics-snapshot). |
| `flushDiffs()` | Drain the pending `presence_diff` buffer synchronously. Use in graceful-shutdown paths or tests that need the diff to land before the await chain continues. |
| `clear()` | Reset all presence state |
| `destroy()` | Stop heartbeat and subscriber |
| `hooks` | `{ subscribe, close }` - ready-made WebSocket hooks. Destructure for one-line `hooks.ws.js` setup. |

#### Metrics snapshot

`presence.metrics()` returns a synchronous snapshot of in-memory state:

| Field | Description |
|---|---|
| `totalOnline` | Sum of unique-users-per-topic across all topics this instance is locally tracking. The same user in two topics counts twice; per-topic counts sum cleanly. |
| `heartbeatLatencyMs` | Duration of the most recent heartbeat tick in milliseconds. Useful as a rough Redis-health indicator - a tick that suddenly takes longer than usual is likely waiting on a slow Redis. |
| `staleCleanedTotal` | Cumulative count of stale fields removed by the heartbeat-driven cleanup script since this instance started. A non-zero rate means crashed sibling instances' presence is being cleaned up; a zero rate is the healthy steady state. |

The same numbers are exposed as Prometheus when a `metrics` registry is attached: `presence_total_online{topic="..."}` (gauge), `presence_heartbeat_latency_ms` (gauge), `presence_stale_cleaned_total` (counter, already shipped pre-0.5.0).

Two additional counters track the diff-protocol behavior:

| Metric | Description |
|---|---|
| `presence_diff_frames_total{topic="..."}` | `presence_diff` frames published to topic subscribers. Compared against `presence_joins_total` + `presence_leaves_total` it tells you how much per-tick coalescing the buffer is doing - the bigger the gap, the more bandwidth saved versus per-event broadcast. |
| `presence_diff_coalesced_total{topic="..."}` | Buffered diff entries overwritten by a later op in the same tick. A non-zero rate confirms the same-key collapse is working (e.g. a user reconnecting fast enough to leave-then-join in one tick). Zero is also a valid state under steady traffic. |

#### Keyspace cleanup mode

By default a sync-only observer (a connection that called `presence.sync()` to watch a room without joining it) only learns about leaves when the tracking instance broadcasts a `presence_diff` with the user in `leaves`. If the tracking instance crashes, the broadcast never fires and the observer's UI shows stale data until the page is reloaded.

`keyspaceNotifications: true` closes that gap by `psubscribe`-ing to `__keyevent@*__:expired`. When the presence hash key for a topic expires (which happens once no instance is heartbeating the topic anymore - typically because the only tracker crashed), this instance emits an empty `presence_state` event on `__presence:<topic>` so local subscribers can replace their entire local map with "no one here."

```js
const presence = createPresence(redis, {
  key: 'id',
  keyspaceNotifications: true
});
```

**Operator burden:** Redis must be configured to publish keyspace events:

```
CONFIG SET notify-keyspace-events Ex
```

(or any flagset that includes both `K`/`E` and `x` - e.g. `Ex`, `KEA`, etc.) If the `psubscribe` call fails because keyspace events are off, the failure is logged once and the rest of the tracker keeps working without the keyspace branch.

**Scope:** this is hash-key expiry (whole topic gone), not per-field expiry. Per-field cleanup of stale entries from crashed instances continues to run via the heartbeat-driven cleanup script. Per-field hash TTLs would require Redis 7.4+ `HEXPIRE` and a different storage layout; that's a future evolution, not part of this mode.

#### Zero-config hooks

Instead of writing `subscribe` and `close` handlers manually, destructure `presence.hooks`:

```js
// src/hooks.ws.js
import { presence } from '$lib/server/presence';
export const { subscribe, close } = presence.hooks;
```

`subscribe` handles both regular topics (calls `join`) and `__presence:*` topics (calls `sync` so the client gets the current list). `close` calls `leave`.

If you need custom logic (auth gating, logging), wrap the hooks:

```js
import { presence } from '$lib/server/presence';

export async function subscribe(ws, topic, ctx) {
  if (!ctx.platform.getUserData(ws).authenticated) return;
  await presence.hooks.subscribe(ws, topic, ctx);
}

export const { close } = presence.hooks;
```

---

## Connection registry

The adapter's `platform.request(ws, ...)` is single-instance: it takes a local `ws` reference, so it only works against connections owned by the calling instance. `createConnectionRegistry` is the cluster-routed counterpart - a `userId -> {instanceId, sessionId, ts}` map in Redis plus a per-instance push channel that lets any instance route a request to whichever one currently owns a given user's WebSocket.

#### Setup

```js
// src/lib/server/registry.js
import { redis } from './redis.js';
import { createConnectionRegistry } from 'svelte-adapter-uws-extensions/redis/registry';

export const registry = createConnectionRegistry(redis, {
  identify: (ws) => ws.getUserData()?.userId
});
```

Wire the open / close hooks so each connection is tracked cluster-wide:

```js
// src/hooks.ws.js
import { registry } from '$lib/server/registry';

export const open = registry.hooks.open;
export const close = registry.hooks.close;
```

The `identify` function returns the user identity for a WebSocket (return `null` / `undefined` for anonymous connections; the registry skips them). The registry reads the per-connection `WS_SESSION_ID` slot the adapter stamps on userData, so no other configuration is required.

#### Cluster-routed request/reply

```js
// From any instance:
const reply = await registry.request('user-123', 'confirm-action', { op: 'delete' }, {
  timeoutMs: 5000
});
if (reply.confirmed) await actuallyDelete();
```

The lookup resolves which instance currently owns `user-123`'s connection. If that's the calling instance, the request short-circuits to a local `platform.request(ws, ...)` - no Redis hop. Otherwise the request envelope ships across the per-instance push channel, the owning instance calls `platform.request` locally, and the reply ships back on the origin's push channel.

Wire envelopes (internal):

| Direction | Channel | Payload |
|---|---|---|
| Origin -> owner | `{prefix}__push:{ownerInstanceId}` | `{type:'request', ref, sessionId, event, data, replyTo, timeoutMs}` |
| Owner -> origin | `{prefix}__push:{originInstanceId}` | `{type:'reply', ref, data}` or `{type:'reply', ref, error}` |

`request(...)` rejects on:

- the target user being offline (no entry in Redis)
- the request timing out (`timeoutMs` exceeded)
- the owning instance reporting a handler error from its `platform.request`

Mid-flight migration (user reconnects to a different instance between lookup and reply) surfaces as a timeout on the origin; the owning instance's late reply lands on a missing pending entry and is dropped with a `push_late_replies_total` increment. See [Edge cases](#registry-edge-cases) below.

#### Storage shape

| Key | Shape | Notes |
|---|---|---|
| `{prefix}conns:{userId}` | Hash `{instanceId, sessionId, ts, attrs?}` | Most-recent-connection-wins. A second device on the same `userId` replaces the first; targeting from `request(...)` always reaches the most recent connection. `attrs` is a JSON-encoded snapshot of the optional `attributes(ws)` callback's return value - present only when `attributes` is wired and returns at least one value. |
| `{prefix}__push:{instanceId}` | Pub/sub channel | Each instance subscribes to its own channel. Inbound messages dispatch by `envelope.type`. |
| `{prefix}__registry-events` | Pub/sub channel | Shared across all instances. Carries `{type:'open' | 'close', userId, instanceId, attrs?}` envelopes so each instance can maintain a live secondary index for `sendTo(...)`. Skipped when no `attributes` option was supplied. |

Compare-and-delete on `close`: a Lua-atomic check ensures the close hook only removes the registry entry when the stored `instanceId` still matches this instance. Prevents a stale close from clobbering a registration that already migrated to another instance via a fast laptop-then-phone reconnect.

#### Options

| Option | Default | Description |
|---|---|---|
| `identify` | (required) | `(ws) => userId | null`. Anonymous connections are skipped. |
| `attributes` | - | `(ws) => Record<string, string | number | boolean>`. Required for `sendTo(...)`. Captures per-user attributes at registration time for tenant- / role- / cohort-scoped broadcasts. Numbers and booleans are coerced to strings for index-key consistency; nested objects, arrays, and `null` values are dropped (shallow values only per credo rule 1). |
| `keyPrefix` | `''` | Prefix prepended to all registry keys and channels. Stacks with the underlying client's `keyPrefix`. |
| `ttl` | `90` | Expiry on registry entries in seconds. Should be > `heartbeat * 3` so a missed beat doesn't drop a live user. |
| `heartbeat` | `30000` | TTL refresh interval in ms. Each tick `EXPIRE`s every locally-owned entry. |
| `requestTimeoutMs` | `5000` | Default timeout for `request(...)` calls. Overridable per call via `options.timeoutMs`. |
| `breaker` | - | Optional circuit breaker for Redis ops. |
| `metrics` | - | Optional Prometheus metrics registry. |

#### API

| Method | Description |
|---|---|
| `lookup(userId)` | Resolve a userId to its current entry (`{instanceId, sessionId, ts}`) or `null`. |
| `request(target, event, data?, opts?)` | Cluster-routed request/reply. Resolves with the reply. |
| `send(target, topic, event, data?)` | Cluster-routed `platform.send` counterpart. Fire-and-forget. See [Targeted sends](#registry-targeted-sends) below. |
| `sendCoalesced(target, message)` | Cluster-routed coalesce-by-key send. Fire-and-forget. See [Coalesced sends](#registry-coalesced-sends) below. |
| `sendTo(criteria, topic, event, data?)` | Attribute-targeted broadcast across the cluster. Requires `attributes` option. See [Attribute-targeted broadcast](#registry-sendto) below. |
| `size()` | Count of users registered to THIS instance (local view, scrape-time). |
| `instanceId` | Stable id for this instance, also the name of its push channel. |
| `hooks.open` / `hooks.close` | Wire as ready-made WebSocket hooks. |
| `destroy()` | Stop the heartbeat timer and Redis subscriber. |

#### Targeted sends {#registry-targeted-sends}

`registry.send(target, topic, event, data)` is the cluster-routed counterpart to the adapter's `platform.send(ws, topic, event, data)`. Lookup resolves the owning instance, self-targeting short-circuits to a local `platform.send`, otherwise a fire-and-forget envelope `{type:'send', sessionId, topic, event, data}` ships on the owner's push channel.

```js
registry.send('user-123', 'notifications', 'incoming', { id: 42 });
```

Fire-and-forget: no Promise<reply>, no acknowledgement. Callers who need a delivery signal should use `request(...)` instead. A user offline at lookup-time silently drops with `push_sends_total{result="offline"}`. Mid-flight migration (user disconnects between lookup and arrival) drops on the receiver with `push_sends_total{result="late"}`.

#### Coalesced sends {#registry-coalesced-sends}

`registry.sendCoalesced(target, { key, topic, event, data })` is the cluster-routed counterpart to the adapter's `platform.sendCoalesced(ws, ...)` - one slot per `(connection, key)` tuple, latest-value-wins. Fire-and-forget; no reply path, no `Promise<reply>`.

```js
registry.sendCoalesced('user-123', {
  key: 'cursor:doc-7',
  topic: 'doc:7',
  event: 'cursor',
  data: { x: 410, y: 220 }
});
```

Routing follows the same shape as `request(...)`: lookup the owning instance, self-target short-circuits to a local `platform.sendCoalesced(ws, ...)`, otherwise a fire-and-forget envelope `{type:'coalesced', sessionId, key, topic, event, data}` ships on the owner's push channel and the receiver calls `platform.sendCoalesced` locally.

Per-`(connection, key)` replacement happens on the receiver via the adapter's existing coalesce semantics, so a duplicate or out-of-order envelope from a flaky link is collapsed on arrival rather than producing a stutter on the wire. Ordering is preserved within a `(user, key)` tuple as long as the user does not move instances mid-flight; instance migration triggers one transient out-of-order moment that the per-connection coalesce collapses on the new instance.

Best fit: targeted latest-value streams where the target is a *user*, not a topic. Cursor positions inside a doc, typing indicators between two users, presence-state pushes from a moderator to a single subscriber. Topic-broadcast coalesce (every subscriber sees the same stream) already works cluster-wide via `bus.wrap(platform).publish(...)` on either bus and per-receiver A1 logic; this method covers the remaining gap.

#### Attribute-targeted broadcast {#registry-sendto}

`registry.sendTo(criteria, topic, event, data)` is the cluster-routed counterpart to the adapter's `platform.sendTo(filter, ...)`. Captures per-user attributes at registration time via the `attributes(ws)` option, indexes them in memory on every instance, and resolves a match into one envelope per owning instance. Keys are entirely user-defined - whatever you return from `attributes(ws)` is what `sendTo` can match against. The adapter's `platform.sendTo` examples use `userData.role === 'admin'`; the cluster version follows the same shape:

```js
import { createConnectionRegistry } from 'svelte-adapter-uws-extensions/redis/registry';

const registry = createConnectionRegistry(redis, {
  identify: (ws) => ws.getUserData()?.userId,
  attributes: (ws) => {
    const ud = ws.getUserData();
    return { role: ud.role, region: ud.region };
  }
});

// Broadcast to every connection where attributes.role === 'admin':
registry.sendTo({ role: 'admin' }, 'alerts', 'warning', { message: 'High load' });

// Compound match (AND across keys):
registry.sendTo({ role: 'admin', region: 'eu' }, 'audit', 'created', payload);
```

`platform.sendTo(filter, topic, event, data)` accepts a filter function - functions don't serialize across instances, so the filter-function escape hatch is deliberately not lifted here. The cluster shape is shallow equality only: one literal value per attribute key, AND across keys. No regex, no array containment, no nested-object queries. Apps that need richer queries should publish to a dedicated topic and route their own broadcasts.

How it works:

1. Each instance maintains an in-memory secondary index: `Map<attrKey, Map<attrValue, Set<userId>>>` plus a shadow `userId -> attrs` map and a `userId -> instanceId` map.
2. Updates ride a shared `{prefix}__registry-events` Redis pub/sub channel. Every successful `hooks.open` publishes `{type:'open', userId, instanceId, attrs}`; every successful close publishes `{type:'close', userId, instanceId}` (compare-and-delete already protects against a stale close from a previous owner).
3. When a new instance starts up, it bootstraps the index by SCAN-ing the existing `{prefix}conns:*` hashes and calling HGETALL on each to populate from the live registry state. Subscribe-first / SCAN-second keeps the bootstrap race-tolerant under set semantics.
4. `sendTo(criteria, ...)` resolves matching userIds via the local index, groups them by their owning instance, fires once per owning instance on the existing `{prefix}__push:` channel: `{type:'sendTo', criteria, topic, event, data}`.
5. Each receiving instance re-resolves matches against its own authoritative local index and calls `platform.send(ws, topic, event, data)` for each match. Authoritative-on-receiver matching tolerates one round of sender-index staleness from a fast user migration.

`sendTo` is fire-and-forget. No reply, no acknowledgement, no per-target outcome. The single `push_sendto_total` counter records sender-side outcomes:

| `result` label | Meaning |
|---|---|
| `ok` | At least one match resolved; envelopes published successfully (or only self-matches that delivered locally without Redis traffic). |
| `empty` | No matches resolved by the local index. The call no-ops. |
| `error` | At least one Redis publish failed; partial delivery still occurred for the successful publishes and any local self-matches. |

Eventual consistency caveats:

- **Mid-flight migration.** A user reconnecting on a different instance between the sender's index lookup and the receive can produce a single best-effort missed delivery while the registry-events channel propagates the migration.
- **Pubsub disconnect.** If the registry's subscriber connection drops, in-memory indexes will not update until reconnect; the `attrs` field on the Redis hash stays authoritative for `lookup(userId)`. The next call to `ensureSubscriber()` (e.g., on the next `hooks.open`) re-bootstraps via SCAN.
- **Topics outside the bus.** Only topics published via `platform.send`/`platform.publish` reach the receiving connections; matched users with no active subscription on the targeted topic still receive the event since `platform.send` is per-connection, not per-topic.

For exact targeting (audit log, billing, transactional broadcasts), use `request(...)` per userId or fan out via topic subscribers with the bus.

#### Metrics

| Metric | Description |
|---|---|
| `push_requests_total{result="ok|offline|timeout|error"}` | Outcomes for `request(...)` calls. `ok` is a successful reply; `offline` is a missing or local-ws-gone entry; `timeout` is no reply within `timeoutMs`; `error` is a handler-thrown error or a Redis publish failure. |
| `push_reply_latency_ms` | Histogram of request-publish to reply-receive in milliseconds (success path). |
| `push_registry_size` | Gauge: connections registered to this instance. Scrape-time, no continuous accounting. |
| `push_late_replies_total` | Counter: replies that arrived after their request expired or migrated. |
| `push_coalesced_total{result="ok|self|offline|late|error"}` | Outcomes for `sendCoalesced(...)`. `ok` is a successful cross-instance publish; `self` is a successful self-target; `offline` is a missing entry or local-ws-gone; `late` is a receive-side miss (sessionId not in the local map - target migrated/closed between dispatch and arrival); `error` is a Redis publish failure or a thrown `platform.sendCoalesced` on either side. |
| `push_sends_total{result="ok|self|offline|late|error"}` | Outcomes for `send(...)`. Same result space as `push_coalesced_total` - `ok` cross-instance, `self` short-circuited locally, `offline` entry missing or local-ws-gone, `late` receive-side miss after migration, `error` Redis publish or local `platform.send` threw. |
| `push_sendto_total{result="ok|empty|error"}` | Outcomes for `sendTo(...)`. `ok` is "at least one match resolved, all publishes succeeded (or only self-matches)"; `empty` is "no matches resolved by the local index"; `error` is "at least one Redis publish failed." Per-call counter, not per-target - one `sendTo` produces exactly one increment regardless of how many matches it fans out to. |

#### Registry edge cases

- **User reconnects to a different instance mid-request.** The origin's pending entry waits on the OLD instance's reply channel. The user's new connection won't reply on that channel; the request times out. Any late reply from the old instance after teardown lands on a missing pending entry and is silently dropped (`push_late_replies_total` increments).
- **Owning instance crashes between request publish and local `platform.request`.** Same shape as above - request times out. The Redis entry remains until the TTL expires (sliding heartbeat cleared by the dead instance), after which subsequent `request(...)` calls see `result="offline"` from the lookup.
- **Self-targeting** (the origin instance owns the user). Short-circuits to a local `platform.request(ws, ...)` without round-tripping Redis. One conditional in the dispatcher; not a special case at the API surface.
- **Anonymous connections.** `identify(ws)` returning `null` / `undefined` makes the open / close hooks no-op. Anonymous users are not addressable through the registry by design.

---

## Rate limiting

Same API as the core `createRateLimit` plugin, but backed by Redis using an atomic Lua script. Rate limits are enforced across all server instances with exactly one Redis roundtrip per `consume()` call.

#### Setup

```js
// src/lib/server/ratelimit.js
import { redis } from './redis.js';
import { createRateLimit } from 'svelte-adapter-uws-extensions/redis/ratelimit';

export const limiter = createRateLimit(redis, {
  points: 10,
  interval: 1000,
  blockDuration: 30000
});
```

#### Usage

```js
// src/hooks.ws.js
import { limiter } from '$lib/server/ratelimit';

export async function message(ws, { data, platform }) {
  const { allowed } = await limiter.consume(ws);
  if (!allowed) return; // drop the message
  // ... handle message
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `points` | *required* | Tokens available per interval |
| `interval` | *required* | Refill interval in ms |
| `blockDuration` | `0` | Auto-ban duration in ms (0 = no ban) |
| `keyBy` | `'ip'` | `'ip'`, `'connection'`, or a function |

#### API

All methods are async (they hit Redis). The API otherwise matches the core plugin:

| Method | Description |
|---|---|
| `consume(ws, cost?)` | Attempt to consume tokens. `cost` must be a positive integer. |
| `reset(key)` | Clear the bucket for a key |
| `ban(key, duration?)` | Manually ban a key |
| `unban(key)` | Remove a ban |
| `clear()` | Reset all state |

---

## Broadcast groups

Same API as the core `createGroup` plugin, but membership is stored in Redis so groups work across multiple server instances. Local WebSocket tracking is maintained per-instance, and cross-instance events are relayed via Redis pub/sub.

#### Setup

```js
// src/lib/server/lobby.js
import { redis } from './redis.js';
import { createGroup } from 'svelte-adapter-uws-extensions/redis/groups';

export const lobby = createGroup(redis, 'lobby', {
  maxMembers: 50,
  meta: { game: 'chess' }
});
```

Note: the API signature is `createGroup(client, name, options)` instead of `createGroup(name, options)` - the Redis client is the first argument.

#### Usage

```js
// src/hooks.ws.js
import { lobby } from '$lib/server/lobby';

export async function subscribe(ws, topic, { platform }) {
  if (topic === 'lobby') await lobby.join(ws, platform);
}

export async function close(ws, { platform }) {
  await lobby.leave(ws, platform);
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `maxMembers` | `Infinity` | Maximum members allowed (enforced atomically) |
| `meta` | `{}` | Initial group metadata |
| `memberTtl` | `120` | Member entry TTL in seconds. Entries from crashed instances expire after this period. |
| `onJoin` | - | Called after a member joins |
| `onLeave` | - | Called after a member leaves |
| `onFull` | - | Called when a join is rejected (full) |
| `onClose` | - | Called when the group is closed |

#### API

| Method | Description |
|---|---|
| `join(ws, platform, role?)` | Add a member (returns false if full/closed) |
| `leave(ws, platform)` | Remove a member |
| `publish(platform, event, data, role?)` | Broadcast to all or filter by role |
| `send(platform, ws, event, data)` | Send to a single member |
| `localMembers()` | Members on this instance |
| `count()` | Total members across all instances |
| `has(ws)` | Check membership on this instance |
| `getMeta()` / `setMeta(meta)` | Read/write group metadata |
| `close(platform)` | Dissolve the group |
| `destroy()` | Stop the Redis subscriber |

---

## Cursor

Same API as the core `createCursor` plugin, but cursor positions are shared across instances via Redis. Each instance throttles locally (same leading/trailing edge logic as the core), then relays broadcasts through Redis pub/sub so subscribers on other instances see cursor updates.

Out of the box the broadcast path is a 60Hz world-state tick: each topic emits at most one frame per `topicThrottle` window, carrying the latest position for every cursor that moved. Bandwidth per peer scales with active-mover count, not with mover-count times per-mover rate - 100 cursors moving at 60Hz cost the same per peer as 100 cursors moving at 1Hz, because every peer receives one bulk frame per tick regardless. For high-density rooms (>200 active movers) where the bulk-frame size becomes the bottleneck, lower the tick by raising `topicThrottle` to 33 (30Hz).

Hash entries have a TTL so stale cursors from crashed instances get cleaned up automatically.

#### Setup

```js
// src/lib/server/cursors.js
import { redis } from './redis.js';
import { createCursor } from 'svelte-adapter-uws-extensions/redis/cursor';

export const cursors = createCursor(redis, {
  select: (userData) => ({ id: userData.id, name: userData.name, color: userData.color }),
  ttl: 30
});
```

#### Usage

Cursor publishes go to the internal `__cursor:{topic}` channel. Clients receive those frames only if the extension has subscribed them server-side - the adapter's wire-level `__`-prefix gate intentionally denies client-sent `__cursor:` subscribe frames. Call `cursors.attach(ws, topic, platform)` from your "join room" RPC (mirroring `presence.join`); without it, every `update` fans out to an empty subscriber set.

```js
// src/lib/server/rpc.js (or wherever you handle joinBoard / leaveBoard)
import { cursors } from '$lib/server/cursors';

export async function joinBoard(ws, { topic, platform }) {
  await cursors.attach(ws, topic, platform);   // subscribes ws + sends snapshot
}

export function leaveBoard(ws, { topic, platform }) {
  cursors.detach(ws, topic, platform);          // unsubscribes (only needed if the user stays connected)
}
```

```js
// src/hooks.ws.js
import { cursors } from '$lib/server/cursors';

export function message(ws, { data, platform }) {
  const msg = JSON.parse(Buffer.from(data).toString());
  if (msg.type === 'cursor') {
    cursors.update(ws, msg.topic, msg.position, platform);
  }
}

export function close(ws, { platform }) {
  cursors.remove(ws, platform);
  // No per-topic detach needed - uWS releases all subscriptions on disconnect.
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `throttle` | `16` | Minimum ms between broadcasts per user per topic. 60Hz default matches the world-state tick so an individual cursor stays smooth at the per-peer wire rate. |
| `topicThrottle` | `16` | World-state tick rate in ms. Each topic emits at most one bulk frame per window, carrying the latest position for every cursor that moved. Raise to 33 (30Hz) for high-density rooms; 0 disables the tick. |
| `select` | strips `__`-prefixed keys | Extract user data to broadcast alongside position |
| `ttl` | `30` | Per-entry TTL in seconds (auto-refreshed on each broadcast). Stale entries from crashed instances are filtered out individually, even if other instances are still active on the same topic. |
| `maxEnvelopeBytes` | `1048576` (1 MB) | Reject inbound cursor envelopes larger than this before `JSON.parse` runs. The inner topic is always validated against the `__` denylist (the module constructs its own `__cursor:` wrapper prefix), so no `allowSystemTopics` knob is needed here. |

#### API

| Method | Description |
|---|---|
| `attach(ws, topic, platform)` | Opt the connection into receiving cursor updates for `topic` (subscribes to `__cursor:{topic}` server-side and sends a snapshot). Required for any client to see cursor frames. |
| `detach(ws, topic, platform)` | Stop the connection from receiving updates for `topic`. Only needed for explicit leave; uWS handles disconnect cleanup. |
| `update(ws, topic, data, platform)` | Broadcast cursor position (throttled per user per topic) |
| `remove(ws, platform, topic?)` | Remove from a specific topic, or all topics if omitted |
| `list(topic)` | Get current positions across all instances |
| `clear()` | Reset all local and Redis state |
| `destroy()` | Stop the Redis subscriber and clear timers |

---

**Postgres extensions**

## Replay buffer (Postgres)

Same API as the Redis replay buffer, but backed by a Postgres table. Best suited for durable audit trails or history that needs to survive longer than Redis TTLs. Sequence numbers are generated atomically via a dedicated `_seq` table using `INSERT ... ON CONFLICT DO UPDATE`, so concurrent publishes from multiple instances produce strictly ordered sequences with no duplicates or gaps.

Buffer trimming runs after each publish by deleting rows with `seq <= currentSeq - maxSize`. If the trim query fails, the publish still succeeds - the periodic background cleanup (configurable via `cleanupInterval`) catches any excess rows later.

Same gap detection behavior as the Redis replay buffer: if the client's last-seen sequence is older than the oldest buffered row, or the buffer is empty but the sequence counter has advanced, a `truncated` event fires before replay. The standalone `gap(topic, lastSeenSeq)` probe is also available with the same `{ truncated, missingFrom }` shape; the gap query uses the `(topic, seq)` index for an O(log n) seek rather than scanning the buffer.

The aggregate-vs-broadcast guidance from the [Redis replay section](#aggregate-vs-broadcast-topics) applies equally here - one topic per aggregate keeps the buffer size budget meaningful and gap detection actionable.

`resumeHook()` is available with identical semantics to the Redis backend; see [Session resumption](#session-resumption-resumehook).

#### Setup

```js
// src/lib/server/replay.js
import { pg } from './pg.js';
import { createReplay } from 'svelte-adapter-uws-extensions/postgres/replay';

export const replay = createReplay(pg, {
  table: 'svti_replay',
  size: 1000,
  ttl: 86400,       // 24 hours
  autoMigrate: true  // auto-create table
});
```

#### Schema

The table is created automatically on first use (if `autoMigrate` is true):

```sql
CREATE TABLE IF NOT EXISTS svti_replay (
  svti_replay_id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  seq BIGINT NOT NULL,
  event TEXT NOT NULL,
  data JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_svti_replay_topic_seq ON svti_replay (topic, seq);

CREATE TABLE IF NOT EXISTS svti_replay_seq (
  topic TEXT PRIMARY KEY,
  seq BIGINT NOT NULL DEFAULT 0
);
```

#### Options

| Option | Default | Description |
|---|---|---|
| `table` | `'svti_replay'` | Table name |
| `size` | `1000` | Max messages per topic |
| `ttl` | `0` | Row expiry in seconds (0 = never) |
| `autoMigrate` | `true` | Auto-create table |
| `cleanupInterval` | `60000` | Periodic cleanup interval in ms (0 to disable) |

#### API

Same as [Replay buffer (Redis)](#api-3), plus:

| Method | Description |
|---|---|
| `destroy()` | Stop the cleanup timer |

---

## LISTEN/NOTIFY bridge

Listens on a Postgres channel for notifications and forwards them to `platform.publish()`. You provide the trigger on your table - this module handles the listening side.

Uses a standalone connection (not from the pool) since LISTEN requires a persistent connection that stays open for the lifetime of the bridge.

#### Setup

```js
// src/lib/server/notify.js
import { pg } from './pg.js';
import { createNotifyBridge } from 'svelte-adapter-uws-extensions/postgres/notify';

export const bridge = createNotifyBridge(pg, {
  channel: 'table_changes',
  parse: (payload) => {
    const row = JSON.parse(payload);
    return { topic: row.table, event: row.op, data: row.data };
  }
});
```

#### Usage

```js
// src/hooks.ws.js
import { bridge } from '$lib/server/notify';

let activated = false;

export function open(ws, { platform }) {
  if (!activated) {
    activated = true;
    bridge.activate(platform);
  }
}
```

#### Setting up the trigger

Create a trigger function and attach it to your table:

```sql
CREATE OR REPLACE FUNCTION notify_table_change() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('table_changes', json_build_object(
    'table', TG_TABLE_NAME,
    'op', lower(TG_OP),
    'data', CASE TG_OP
      WHEN 'DELETE' THEN row_to_json(OLD)
      ELSE row_to_json(NEW)
    END
  )::text);
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER messages_notify
  AFTER INSERT OR UPDATE OR DELETE ON messages
  FOR EACH ROW EXECUTE FUNCTION notify_table_change();
```

Now any INSERT, UPDATE, or DELETE on the `messages` table will fire a notification. The bridge parses it and calls `platform.publish()`, which reaches all connected WebSocket clients subscribed to the topic.

The client side needs no changes - the core `crud('messages')` store already handles `created`, `updated`, and `deleted` events.

#### Options

| Option | Default | Description |
|---|---|---|
| `channel` | *required* | Postgres LISTEN channel name |
| `parse` | JSON with `{ topic, event, data }` | Parse notification payload into a publish call. Return null to skip. |
| `autoReconnect` | `true` | Reconnect on connection loss |
| `reconnectInterval` | `3000` | ms between reconnect attempts |
| `multiListener` | `'all'` | `'all'`: every replica opens its own LISTEN (current default). `'advisory'`: leader-elected via `pg_try_advisory_lock`. See [Single-listener mode](#single-listener-mode). |
| `lockId` | - | Advisory lock id. Required when `multiListener: 'advisory'`. |
| `pollInterval` | `5000` | ms between leader-election polls (advisory mode only). |
| `maxEnvelopeBytes` | `1048576` (1 MB) | Reject inbound `NOTIFY` payloads larger than this before `JSON.parse` runs. Defends against a buggy or hostile trigger flooding the bridge with oversized payloads. |
| `allowSystemTopics` | `false` | When `false` (default), parsed envelopes addressed to `__`-prefixed topics are dropped before the publish call. Closes the NOTIFY-injection class where a foreign publisher (or hostile DBA) could inject forged `__signal:*` / `__rpc` / plugin-internal frames via `pg_notify`. Apps that legitimately bridge user-defined `__`-prefixed topics (rare) can opt back in with `true`. |

#### Single-listener mode

By default each replica in an N-replica deployment opens its own LISTEN connection. That's N persistent Postgres connections doing the same work, plus N copies of the same notification.

`multiListener: 'advisory'` elects a single leader via Postgres advisory locks. One replica wins `pg_try_advisory_lock(lockId)` and holds the LISTEN connection; others poll for the lock every `pollInterval` ms. If the leader's connection drops, the session-scoped lock auto-releases and another replica picks it up on its next poll.

```js
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
import { createNotifyBridge } from 'svelte-adapter-uws-extensions/postgres/notify';

const bus = createPubSubBus(redis);

const bridge = createNotifyBridge(pg, {
  channel: 'table_changes',
  multiListener: 'advisory',
  lockId: 0x6e6f7466  // any stable 32-bit id; e.g. CRC32 of the channel name
});

export function open(ws, { platform }) {
  bus.activate(platform);
  bridge.activate(bus.wrap(platform));
}
```

**Requires a cross-instance pub/sub bus.** In `'all'` mode the bridge passes `relay: false` because every replica's local LISTEN already delivers the notification. In `'advisory'` mode only the leader has LISTEN active, so the leader publishes *with* relay - the bus fans out to non-leader replicas via Redis. Without a bus the leader's local clients receive notifications but other replicas' clients don't.

**Choosing a `lockId`.** Pick a stable 32-bit signed integer that's unique per channel within your deployment. CRC32 of the channel name is a reasonable hash; multiple channels in the same database need distinct ids.

#### API

| Method | Description |
|---|---|
| `activate(platform)` | Start listening (idempotent) |
| `deactivate()` | Stop listening and release the connection |

#### Limitations

- **Payload is hard-limited to ~8000 bytes by Postgres** (`pg_notify` silently truncates or errors above this). This is a Postgres constraint, not a library limitation. The bridge warns at 7500 bytes. For large rows, send the row ID in the notification and let the client fetch the full row via an API call.
- Only fires from triggers. Changes made outside your app (manual SQL, migrations) are invisible unless you add triggers for those tables too.
- This is not logical replication. It is simpler, works on every Postgres provider, and needs no extensions or superuser access.

#### When to use this instead of Redis pub/sub

If your real-time events are driven by database writes and you do not need Redis for other extensions (presence, rate limiting, groups, cursors), the LISTEN/NOTIFY bridge is a simpler deployment: no Redis infrastructure, no separate pub/sub channel management, and your notifications are inherently tied to committed transactions. Use the Redis pub/sub bus when you need to broadcast events that do not originate from database writes, or when you are already running Redis for other extensions.

---

## Job queue

`createJobQueue` (`svelte-adapter-uws-extensions/postgres/jobs`) is a minimal `SELECT ... FOR UPDATE SKIP LOCKED` queue that works on vanilla Postgres 9.5+ - no extensions required.

The shape: enqueue jobs into a named queue, claim batches atomically, mark complete (delete) or fail (release for retry). Visibility timeout means a worker that crashes mid-processing has its claim auto-expire so another worker can pick the job up. Max-attempts and dead-letter behavior are intentionally NOT baked in - the `attempts` counter is exposed on every claim, callers track it and decide when to give up.

#### Setup

```js
// src/lib/server/jobs.js
import { pg } from './pg.js';
import { createJobQueue } from 'svelte-adapter-uws-extensions/postgres/jobs';

export const jobs = createJobQueue(pg, {
  visibilityTimeout: 60000  // 60s default; per-call override on claim()
});
```

#### Producer

```js
// In a request handler:
const id = await jobs.enqueue(
  'email',
  { to: 'user@example.com', subject: 'Welcome' },
  { platform }   // captures platform.requestId onto the row, surfaced as job.requestId on claim
);
```

The third argument is an options bag; `platform` (the SvelteKit `event.platform`) auto-captures the originating request id, or pass `requestId` explicitly to override. The captured id surfaces on `job.requestId` when the row is claimed, so the worker can correlate logs back to the request that enqueued the job.

`enqueue()` returns the row id verbatim from `pg`, which serialises `BIGINT`/`BIGSERIAL` columns as **strings** by default to avoid precision loss past `Number.MAX_SAFE_INTEGER`. Pass it through to `claim()`/`complete()`/`fail()`/`extend()` as-is. If you want a JS number for logging or comparison, coerce explicitly with `Number(id)` (safe up to 2^53 - still ~9 quadrillion rows of headroom).

#### Consumer (worker loop)

```js
// In a separate worker process or background loop:
async function workerLoop() {
  while (running) {
    const batch = await jobs.claim('email', { batchSize: 5, visibilityTimeoutMs: 30000 });
    if (batch.length === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      continue;
    }
    for (const job of batch) {
      try {
        await sendEmail(job.payload);
        await jobs.complete(job.id);
      } catch (err) {
        if (job.attempts >= 5) {
          await jobs.complete(job.id);  // give up after 5 tries
          await logToDeadLetter(job, err);
        } else {
          await jobs.fail(job.id);  // release for retry
        }
      }
    }
  }
}
```

For long-running jobs that need more visibility headroom, call `jobs.extend(id, additionalMs)` periodically while processing.

#### Schema

The table is created automatically on first use (if `autoMigrate` is true):

```sql
CREATE TABLE IF NOT EXISTS svti_jobs (
  svti_jobs_id  BIGSERIAL   PRIMARY KEY,
  queue         TEXT        NOT NULL,
  payload       JSONB,
  request_id    TEXT,                       - originating platform.requestId, or null
  claimed_at    TIMESTAMPTZ,
  claimed_until TIMESTAMPTZ,
  attempts      INTEGER     NOT NULL DEFAULT 0,
  created_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_svti_jobs_queue_pending
    ON svti_jobs (queue, svti_jobs_id) WHERE claimed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_svti_jobs_visibility
    ON svti_jobs (claimed_until) WHERE claimed_at IS NOT NULL;
```

Existing 0.5.0-next.1 deployments forward-migrate via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS request_id TEXT` on first use; idempotent and zero-downtime.

#### Options

| Option | Default | Description |
|---|---|---|
| `table` | `'svti_jobs'` | Table name |
| `autoMigrate` | `true` | Auto-create the table on first use |
| `visibilityTimeout` | `30000` | Default ms a claim is held before another worker can re-claim |

#### API

| Method | Description |
|---|---|
| `enqueue(queue, payload, opts?)` | Insert a job; returns the job id. Opts: `{ requestId?, platform? }` - `platform.requestId` is captured automatically when `platform` is passed |
| `claim(queue, opts?)` | `SELECT ... FOR UPDATE SKIP LOCKED` claim; opts: `{ batchSize?, visibilityTimeoutMs? }`. Each returned job carries `id, queue, payload, requestId, attempts, created_at` |
| `complete(idOrIds)` | Delete the job(s) on success |
| `fail(idOrIds)` | Release the claim for retry |
| `extend(idOrIds, ms)` | Push back the visibility deadline |
| `pending(queue?)` | Count of unclaimed jobs |
| `clear(queue?)` | Delete all jobs (useful for tests) |

#### When to use this vs createTaskRunner

| | `createJobQueue` | `createTaskRunner` |
|---|---|---|
| Surface | minimal: claim / complete / fail | full state machine: idempotency + fence + retry + result tracking |
| Best fit | "ingest event, defer work" with caller-driven retry | tasks that must complete exactly once with cross-instance recovery |
| Result tracking | none (caller tracks via DB writes) | yes (`run` / `await`) |

If your handler must run exactly once and you want the runtime to track the result, reach for `createTaskRunner`. If you want a lighter producer/consumer split with caller-driven retry, this is the simpler shape.

---

**Cross-backend**

## Idempotency store

Caches the result of an effectful operation under a stable key so retries within `ttl` return the original outcome rather than re-executing. Use it for HTTP/RPC retries, webhook redeliveries, and any handler where the caller may legitimately repeat a request that must execute at most once - charge-customer, send-email, create-order.

The store exposes three states via `acquire(key)`:

- **acquired** - you own the slot. Run the work, then call `commit(result)` on success or `abort()` on failure.
- **pending** - another caller acquired the slot and has not committed yet. Decide locally whether to return a 409, retry later, or wait.
- **result** - a previous run committed. The cached value is returned.

A short `acquireTtl` (default 60 seconds) bounds how long a pending slot can hold the key, so a crashed owner cannot deadlock retries forever. On `commit` the longer `ttl` (default 48 hours) replaces the sentinel and governs the cache lifetime.

Two backends share the same contract: pick whichever your stack already runs. The adapter's in-memory `Dedup` plugin is the zero-config fallback for single-instance deployments.

#### Setup (Redis)

```js
// src/lib/server/idempotency.js
import { redis } from './redis.js';
import { createIdempotencyStore } from 'svelte-adapter-uws-extensions/redis/idempotency';

export const idempotency = createIdempotencyStore(redis, {
  keyPrefix: 'idem:',
  ttl: 48 * 3600,    // result cache lifetime (48h)
  acquireTtl: 60     // pending-slot lifetime (60s)
});
```

Backed by a single Redis string per key. The acquire path is one Lua-script round trip.

#### Setup (Postgres)

```js
// src/lib/server/idempotency.js
import { pg } from './pg.js';
import { createIdempotencyStore } from 'svelte-adapter-uws-extensions/postgres/idempotency';

export const idempotency = createIdempotencyStore(pg, {
  table: 'svti_idempotency',
  ttl: 48 * 3600,
  acquireTtl: 60,
  autoMigrate: true
});
```

The Postgres backend periodically deletes expired rows (configurable via `cleanupInterval`, default 60s, 0 to disable). Stale pending rows clear on the next sweep without manual intervention.

The Postgres table is created automatically on first use:

```sql
CREATE TABLE IF NOT EXISTS svti_idempotency (
  svti_idempotency_key TEXT        PRIMARY KEY,
  status               TEXT        NOT NULL,
  result               JSONB,
  expires_at           TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_svti_idempotency_expires_at ON svti_idempotency (expires_at);
```

#### Usage

```js
// Wrap an effectful handler.  The caller passes a stable key per logical
// operation; identical retries return the cached result.
export async function placeOrder(input, ctx) {
  const idempotencyKey = `order:${ctx.user.id}:${input.clientOrderId}`;

  const slot = await idempotency.acquire(idempotencyKey);
  if (slot.acquired) {
    try {
      const order = await db.createOrder(input);
      await slot.commit(order);
      return order;
    } catch (err) {
      await slot.abort();
      throw err;
    }
  }
  if (slot.pending) {
    throw new Error('duplicate request in flight');
  }
  return slot.result;
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `keyPrefix` (Redis) | `'idem:'` | Prepended to every Redis key after the client keyPrefix |
| `table` (Postgres) | `'svti_idempotency'` | Table name |
| `ttl` | `172800` (48h) | Result cache lifetime in seconds |
| `acquireTtl` | `60` | Pending-slot lifetime in seconds (anti-deadlock) |
| `autoMigrate` (Postgres) | `true` | Auto-create the table on first use |
| `cleanupInterval` (Postgres) | `60000` | Periodic expired-row cleanup interval in ms (0 to disable) |
| `breaker` | - | Circuit breaker; bypassed when broken |
| `metrics` | - | Prometheus registry; emits `idempotency_*_total` counters |

#### API

| Method | Description |
|---|---|
| `acquire(key)` | Returns `{acquired, commit, abort}` or `{acquired: false, pending: true}` or `{acquired: false, result}` |
| `purge(key)` | Drop a single cached result |
| `clear()` | Drop every key under the configured prefix / table |
| `destroy()` (Postgres only) | Stop the cleanup timer |

#### Choosing acquireTtl

`acquireTtl` is the upper bound on how long a single execution of the wrapped operation can run before retries see the slot as available again. Set it longer than your worst expected latency for the wrapped handler, but short enough that a crashed instance does not block retries for too long. The default (60 seconds) suits most HTTP and RPC handlers; bump it for long-running tasks (large file uploads, multi-step workflows) and trim it for tight read-heavy paths.

#### Pairing with the Dedup plugin

The adapter's in-memory `createDedup` plugin (`svelte-adapter-uws/plugins/dedup`) is the single-instance fallback for the same contract. The shape is identical, so swapping the backend is a one-line change. Use the in-memory plugin for tests and single-process deployments; reach for this store the moment a second instance enters the picture.

---

## Distributed lock

Cluster-wide mutual-exclusion primitive. The adapter ships an in-memory `Lock` plugin that serializes `withLock(key, fn)` per key on a single instance via a `Map<string, Promise>`; this is the Redis-backed swap for multi-instance deployments. Distinct from `redis/fence` (B2c): the fence module is task-runner-specific (one fence per `taskId`, paired with the Postgres state machine); this is a general-purpose mutex any user code can grab.

#### Setup

```js
import { redis } from './redis.js';
import { createDistributedLock } from 'svelte-adapter-uws-extensions/redis/lock';

export const lock = createDistributedLock(redis, {
  defaultTtlMs: 30_000,
  maxWaitMs: 5000
});
```

#### Use

```js
import { lock } from '$lib/server/lock';

await lock.withLock('order-42', async (signal) => {
  // serialized cluster-wide. Bail when `signal.aborted` if the heartbeat
  // detects we lost ownership mid-flight.
  if (signal.aborted) return;
  await processOrder(42);
});
```

`withLock(key, fn, options?)` runs `fn` while holding a cluster-wide mutex on `key`. The user fn cannot forget to release - the lock module owns the release path. `fn`'s return value is forwarded through; errors thrown by `fn` propagate after the lock is released.

#### How it works

1. **Acquire.** `SET <fullKey> <fenceToken> NX PX <ttlMs>` - atomic test-and-set with a TTL. The fence token is a per-call random UUID so a stale heartbeat or release from a previous holder can never affect a new holder.
2. **Retry.** On collision (someone else holds the key), sleep `retryDelayMs` and try again. After `maxWaitMs` total wait, throw `LockAcquireTimeoutError`.
3. **Hold.** While `fn` is running, a heartbeat tick every `heartbeatMs` (defaults to `defaultTtlMs / 3`) refreshes the TTL via Lua-atomic `if get == fenceToken then pexpire end`. If the heartbeat returns 0 (we no longer own the key - operator force-takeover, TTL elapsed before we could refresh), the supplied `AbortSignal` fires with a `LockLostError` and the heartbeat stops.
4. **Release.** On `fn`'s completion (success or error), Lua-atomic `if get == fenceToken then del end` releases the key. Skipped if we already lost ownership (no-op via the compare guard regardless).

The `AbortSignal` shape is the cluster-correctness story: when the heartbeat detects loss, your code learns immediately and can bail instead of continuing to mutate state another holder now thinks it owns. Listen for the `abort` event or check `signal.aborted` at long-running checkpoints.

#### Options

| Option | Default | Description |
|---|---|---|
| `keyPrefix` | `'lock:'` | Prefix prepended (after the client `keyPrefix`) to every lock key. |
| `defaultTtlMs` | `30000` | TTL on a held lock in milliseconds. The heartbeat refreshes this back to the original value before it elapses; if the holder dies and stops heartbeating, the lock auto-expires after at most `ttlMs` so cluster work is not permanently blocked. |
| `retryDelayMs` | `50` | Sleep between acquire retries when the key is held by another instance. Constant retry; no exponential backoff. |
| `maxWaitMs` | `5000` | Total time to wait before throwing `LockAcquireTimeoutError`. Override per call via `withLock(key, fn, { maxWaitMs })`. |
| `heartbeatMs` | `defaultTtlMs / 3` | Heartbeat refresh interval. Default keeps margin for one missed beat. |
| `mapKey` | identity | Map lock key names to bounded label values for metric cardinality. |
| `breaker` | - | Optional circuit breaker for the Redis ops. |
| `metrics` | - | Optional Prometheus metrics registry. |

#### Per-call options

| Option | Description |
|---|---|
| `ttlMs` | Override the holder TTL for this call. |
| `maxWaitMs` | Override the maximum acquire wait for this call. |
| `signal` | External cancellation signal. Aborts the acquire loop and the inner-`fn` execution; the lock is still released cleanly on the way out (we held it; we give it back). |

#### Errors

- **`LockAcquireTimeoutError`** - thrown by `withLock` when `maxWaitMs` elapses without a successful acquire. Properties: `key`, `waitedMs`.
- **`LockLostError`** - surfaced via `signal.reason` (and `controller.abort(...)` on the user fn's signal) when the heartbeat detects we no longer own the key. The user fn keeps running through this; it's up to your code to react. Property: `key`.

#### Metrics

| Metric | Description |
|---|---|
| `lock_acquired_total{key_class}` | Counter of successful acquires. `key_class` is `mapKey(key)`. |
| `lock_acquire_wait_ms` | Histogram of time waited from `withLock` call to successful acquire (ms). Observed only on success. |
| `lock_acquire_timeouts_total{key_class}` | Counter of `withLock` calls that exceeded `maxWaitMs` without acquiring. |
| `lock_lost_total{key_class}` | Counter of locks lost mid-flight via heartbeat detection. A non-zero rate signals operator force-takeover or holder TTL elapsing - both indicate `defaultTtlMs` should be raised or work should be split into smaller chunks. |

#### When to use which

- **`createDistributedLock`** when business logic needs "only one instance runs this critical section at a time" - dedicated lookups against rate-limited APIs, periodic cluster work that must not double-fire, transactional state machines that don't fit `createTaskRunner`'s shape.
- **`createTaskRunner`** when the work is a durable side-effect that must finish exactly once across crashes (charge customer, send email). The runner pairs a Postgres state machine with the Redis fence to guarantee at-most-one and at-least-once delivery.
- **`createIdempotencyStore`** when the contract is "this operation has a result, and a retry within the TTL must return the same result." Mutex semantics are not the goal - caching the outcome is.
- **`createLeader`** (next section) when the question is "which one of N workers should fire this scheduled job right now," not "who runs this critical section." Distinct from `withLock`: leader is a long-lived synchronous observer, lock is a request-scoped serializer.

---

## Leader election

Cluster-wide leader-election primitive via Redis lease. One worker across the cluster holds the lease at any moment; the synchronous `isLeader()` getter is microsecond-cost and cached. Use for cluster-wide singletons (cron schedulers, periodic cleanup, health probes that should run from one place) where firing N times across N workers would be wrong. Designed to plug into `svelte-realtime`'s `live.configureCron({ leader })` hook.

#### Setup

```js
// src/lib/server/leader.js
import { redis } from './redis.js';
import { createLeader } from 'svelte-adapter-uws-extensions/redis/leader';

export const leader = createLeader(redis);
```

#### Use

```js
// src/hooks.ws.js
import { leader } from '$lib/server/leader';
import { configureCron } from 'svelte-realtime/server';

export function init() {
  configureCron({ leader: leader.isLeader });
}

export async function shutdown() {
  // Best-effort release so a sibling takes over within `renewMs`
  // instead of waiting for the full lease to expire.
  await leader.stop();
}
```

`isLeader()` is the only call on the hot path. It reads a cached boolean (no Redis I/O, microsecond cost) and is safe to call at every cron tick / scheduled-job entry. Falsy means "another worker holds the lease" or "no Redis, fail closed" - in both cases the consumer skips.

#### How it works

1. **Acquire.** `SET <fullKey> <instanceId> NX PX <leaseMs>` on construction. If the key was free, this worker holds the lease and `_isLeader` flips true.
2. **Renew.** Every `renewMs` (default `leaseMs / 3`), Lua-atomic `if get == instanceId then pexpire end`. Returns 1 -> still ours, refreshed; returns 0 -> we lost it (force-takeover or TTL elapsed before renewal could land), `_isLeader` flips false.
3. **Re-acquire.** When `_isLeader` is false, the same renewal tick attempts a fresh `SET NX`. As soon as the holder releases or the lease expires server-side, the next non-leader to tick wins.
4. **Release.** `stop()` runs Lua-atomic `if get == instanceId then del end`. Skipped if we already lost ownership; the compare guard means we never accidentally delete a sibling's lease.

The compare-on-mutate guard on both renew and release means a stale tick from a worker that already lost leadership cannot extend or release somebody else's lease. Same shape and the same shared Lua scripts as `redis/lock`'s heartbeat - intentionally identical so the two primitives can't drift on lease semantics.

#### Failure model: fail-closed

A renewal that throws (Redis disconnect, breaker open, network partition) drops `_isLeader` to false and surfaces the error to `onError`. The renewal interval keeps ticking so leadership can recover when Redis recovers. Errors never escape the interval - a Redis blip cannot crash the worker.

Across the cluster, a partitioned Redis means the lease expires server-side and no worker holds leadership until the partition heals - jobs miss ticks rather than double-fire. Better-safe-than-double-fire is the deliberate choice: across most cron consumers, missing one tick is acceptable while running a job twice is not.

GC-pause caveat: a long stop-the-world pause on the leader can cause brief overlap with a freshly-elected successor. Make jobs idempotent at the consumer; this primitive does not provide fencing tokens (consumer sinks for cron-style work rarely have the machinery to consume them anyway).

#### Options

| Option | Default | Description |
|---|---|---|
| `key` | `'leader'` | Redis key for the lease (prefixed by the client `keyPrefix`). |
| `instanceId` | random hex | This worker's identity. Override only if you want a stable identity for diagnostics; correctness does not depend on it. |
| `leaseMs` | `30000` | TTL on the lease in milliseconds. Worst-case window between leader death and successor takeover. |
| `renewMs` | `leaseMs / 3` | Renewal interval, also the interval at which non-leaders attempt fresh acquire. Must be `< leaseMs`. |
| `onError` | - | Called on every Redis failure. Use for structured logging. Errors never escape the renewal interval regardless. |
| `mapKey` | identity | Map the lease key to a bounded label value for cardinality control on the four `leader_*` counters. |
| `breaker` | - | Optional circuit breaker. Renewal failures count via `breaker.failure(err)`; successes via `breaker.success()`. |
| `metrics` | - | Optional Prometheus metrics registry. |

#### API

| Method | Description |
|---|---|
| `isLeader()` | Synchronous cached check. Microsecond-cost. Call at the top of every scheduled-job entry. |
| `currentLeader()` | Single-GET diagnostic read of the current owner's `instanceId`. Returns `null` if unowned or on Redis failure. |
| `stop()` | Stop the renewal interval and best-effort release the lease via compare-and-delete. Idempotent. Never throws. |
| `instanceId` | This worker's identity (provided or generated). |
| `key` | The fully-prefixed lease key (useful for diagnostics). |

#### Metrics

| Metric | Description |
|---|---|
| `leader_acquired_total{key_class}` | Counter of successful acquires (transitions to leader). |
| `leader_lost_total{key_class}` | Counter of leadership losses (transitions to non-leader, including renewal failure). |
| `leader_renewals_total{key_class}` | Counter of successful renewals. |
| `leader_renewal_failures_total{key_class}` | Counter of renewal calls that threw or returned 0 (lease vanished or taken over). |

#### When to use which

- **`createLeader`** for "exactly one of N workers fires this scheduled job." Cluster-wide singleton observation. The primitive holds long-lived state (the lease); the consumer polls `isLeader()` synchronously at every tick.
- **`createDistributedLock`** for "only one of N workers runs this critical section right now." Per-call mutual exclusion around a function that returns. The primitive holds the lock only while the function is running and forwards the function's return value.

Both use the same backing Lua scripts and the same lease semantics; they differ in consumer shape (long-lived observer vs. scoped serializer).

---

## Distributed session

Cluster-wide session store with sliding TTL. The adapter ships an in-memory `Session` plugin (`svelte-adapter-uws/plugins/session`) that holds `Map<token, data>` in process memory; this is the Redis-backed swap for multi-instance deployments where a session created on instance A must be readable from instance B after a load-balancer hop.

Pairs with [Connection registry](#connection-registry): when both are wired, the session provides the durable per-user state (survives disconnect, persists across reconnect, available before a WS even opens), while the registry provides the live "where is this user right now" pointer (only meaningful while the WS is connected). They answer different questions and compose without overlap.

#### Setup

```js
import { redis } from './redis.js';
import { createDistributedSession } from 'svelte-adapter-uws-extensions/redis/session';

export const sessions = createDistributedSession(redis, {
  ttlMs: 30 * 60 * 1000   // 30 minutes
});
```

#### Use

```js
// On login (HTTP handler):
await sessions.set(token, { userId: 42, role: 'admin' });

// On WS upgrade or HTTP request:
const data = await sessions.get(token);
if (!data) return error(401, 'session expired');

// Refresh without reading data:
await sessions.touch(token);

// Logout:
await sessions.delete(token);
```

#### Storage shape

Each session is one Redis string at `{prefix}sess:{token}` containing the JSON-encoded data, with a TTL of `ttlMs`. Single round-trip for every operation: `SET key json PX ttl` to write, `GET key` plus `PEXPIRE key ttl` to read with sliding refresh, `PEXPIRE` to touch, `UNLINK` to delete. JSON blob keeps the whole record atomic on replace; partial-field updates are not a feature - callers do `set(token, { ...await get(token), changed: 'value' })` for that.

#### Sliding TTL

Every `set` resets the TTL to `ttlMs`. By default `get` and `touch` also extend the TTL on a hit (the adapter's bundled `Session` plugin does the same). Disable read-time refresh via `refreshOnGet: false` for read-only flows where reads should not act as liveness signals.

`touch(token)` and `delete(token)` return `true` if the entry was present and the operation succeeded, `false` if the entry was missing or already expired. `get(token)` returns `null` for missing, expired, or corrupt (non-JSON) entries; the corrupt-entry path is treated as a miss so the next `set` cleanly overwrites.

#### Options

| Option | Default | Description |
|---|---|---|
| `keyPrefix` | `'sess:'` | Prefix prepended (after the client `keyPrefix`) to every session key. |
| `ttlMs` | `86_400_000` (24h) | Sliding TTL window in milliseconds. |
| `refreshOnGet` | `true` | Whether `get(token)` extends the TTL on a hit. |
| `breaker` | - | Optional circuit breaker for the Redis ops. |
| `metrics` | - | Optional Prometheus metrics registry. |

#### Pairing with the bundled Session plugin

The shape is identical: same `get` / `set` / `delete` / `touch` / `clear` names, same sliding-TTL semantics. The Redis-backed methods return `Promise<T>` while the bundled plugin's are synchronous, so a swap requires `await` on the call sites - but otherwise the contract is shared. Use the bundled plugin for tests and single-process deployments; reach for this store the moment a second instance enters the picture.

#### Pairing with the connection registry

Sessions and the [Connection registry](#connection-registry) answer different questions:

- **Session:** "What does the system know about this user across HTTP and WS?" Survives disconnect; persists across reconnect; available even when the user has no live WS.
- **Registry:** "Which instance currently owns this user's WS, and what attributes did the connection register with?" Lives only while the WS is connected; cleared on close (compare-and-delete).

Wire them together by storing the auth token both on the WS userData (so the registry can identify the user) and using the same token to look up session data:

```js
// hooks.ws.js
import { sessions } from '$lib/server/sessions';
import { registry } from '$lib/server/registry';

export async function upgrade({ cookies }) {
  const token = cookies.get('session_id');
  if (!token) return false;
  const session = await sessions.get(token);
  if (!session) return false;
  return { token, userId: session.userId };
}

export const open = registry.hooks.open;
export const close = registry.hooks.close;

export async function message(ws) {
  await sessions.touch(ws.getUserData().token);
}
```

#### Metrics

| Metric | Description |
|---|---|
| `session_get_total{result="hit|miss"}` | Counter of get calls. Hit rate signals cache freshness; a miss spike under steady traffic suggests TTL is too short. |
| `session_set_total` | Counter of set calls. |
| `session_delete_total{result="present|absent"}` | Counter of delete calls. `present` = the entry existed; `absent` = the token was already gone (idempotent logout). |
| `session_touch_total{result="present|absent"}` | Counter of touch calls. `absent` = the entry was missing or already expired. |

#### `clear()` and operational notes

`clear()` removes every session under the configured `keyPrefix` via SCAN + UNLINK. Cluster-wide cost scales with total session count - not a hot-path operation. Use for graceful-shutdown teardowns, test harnesses, or operator-initiated wipes (e.g., post-incident "log everyone out"). Other keys outside the prefix are untouched.

There is intentionally no `size()` method: a cluster-wide count of session keys requires SCAN every time, and exposing it as a synchronous-looking accessor would be misleading. Apps that want session counts should track a separate Redis SET on `set` / `delete` and `SCARD` it.

---

## Task runner

Wraps an effectful operation in a state machine that survives process crashes and naturally fans across cluster instances. Use it for background work that absolutely must finish exactly once: charging a customer, sending a transactional email, posting to a webhook, kicking off a long-running pipeline.

**Requires Postgres 13+.** Uses the built-in `gen_random_uuid()` function (added to core in 13; older versions need the `pgcrypto` extension explicitly enabled, which the runner does not do for you).

**Task names must match `/^[a-zA-Z][a-zA-Z0-9_-]*$/`** - start with a letter, then letters/digits/underscores/hyphens. Names starting with `_` or a digit are rejected at `register()` time. Trips test fixtures most often (`__noop` -> `noop`).

Three guarantees:

- **Caller-retry idempotency.** Pair the runner with the idempotency store via the `idempotency` option. When a caller passes the same `idempotencyKey` twice, the second call returns the cached result instead of re-running the handler.
- **Worker-crash recovery.** Every attempt holds a fence UUID and a `fence_expires_at` timestamp. The conditional commit `UPDATE ... WHERE fence = $current_fence` is atomic, so a stuck attempt that comes back from the dead cannot overwrite a completed attempt's result. A periodic recovery sweep reclaims rows whose fence has expired and re-drives the handler in any live instance.
- **External-service idempotency.** The `idempotencyKey` is forwarded to the handler, where you pass it on to Stripe / SendGrid / S3 so the side-effect target de-duplicates retries too.

#### Setup

```js
// src/lib/server/tasks.js
import { pg } from './pg.js';
import { idempotency } from './idempotency.js';
import { createTaskRunner } from 'svelte-adapter-uws-extensions/postgres/tasks';

export const tasks = createTaskRunner(pg, {
  idempotency,           // optional but recommended
  fenceTtl: 60,          // seconds; per-attempt fence lifetime
  recoveryInterval: 30000,
  cleanupInterval: 3600000,
  rowTtl: 7 * 24 * 3600  // keep terminal rows for 7 days
});

tasks.register('charge-customer', async ({ input, idempotencyKey, requestId, signal }) => {
  log.info({ requestId, customerId: input.customerId }, 'charging customer');
  return await stripe.paymentIntents.create(
    { amount: input.amount, customer: input.customerId },
    { idempotencyKey, signal }
  );
}, {
  retry: {
    maxAttempts: 5,
    backoff: (attempt) => Math.min(1000 * 2 ** (attempt - 1), 60000),
    on: (err) => err.type === 'StripeAPIError'
  }
});
```

#### Usage

```js
// In a form action, RPC handler, anywhere with an awaited result
import { tasks } from '$lib/server/tasks';

export const actions = {
  pay: async ({ request, locals, platform }) => {
    const { amount } = Object.fromEntries(await request.formData());
    const result = await tasks.run('charge-customer', {
      input: { amount, customerId: locals.user.stripeCustomerId },
      idempotencyKey: `charge-${locals.user.id}-${request.headers.get('idempotency-key')}`,
      platform   // captures platform.requestId onto the row, exposed as ctx.requestId in the handler
    });
    return { success: true, paymentIntentId: result.id };
  }
};
```

Pass `platform` (the SvelteKit `event.platform`) to capture the originating request id automatically - it lands on `svti_tasks.request_id` and surfaces as `ctx.requestId` in the handler so logs from inside the task correlate back to the WS / HTTP request that started it. Override explicitly via the `requestId` option for non-request contexts (cron, recovery, manual invocation).

#### Schema

The table is created automatically on first use (if `autoMigrate` is true):

```sql
CREATE TABLE IF NOT EXISTS svti_tasks (
  svti_tasks_id        UUID         PRIMARY KEY,
  name                 TEXT         NOT NULL,
  input                JSONB,
  svti_idempotency_key TEXT,
  request_id           TEXT,                    - originating platform.requestId, or null
  status               TEXT         NOT NULL,  - 'running' | 'committed' | 'failed'
  result               JSONB,
  error                JSONB,
  fence                UUID         NOT NULL,
  fence_expires_at     TIMESTAMPTZ  NOT NULL,
  attempts             INT          NOT NULL DEFAULT 1,
  created_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_svti_tasks_running_fence
    ON svti_tasks (fence_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_svti_tasks_terminal_updated
    ON svti_tasks (updated_at) WHERE status IN ('committed', 'failed');
```

Existing 0.5.0-next.1 deployments forward-migrate via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS request_id TEXT` on first use; idempotent and zero-downtime.

#### Options

| Option | Default | Description |
|---|---|---|
| `table` | `'svti_tasks'` | Table name |
| `idempotency` | - | An idempotency store ([above](#idempotency-store)). When provided, results are cached per `idempotencyKey`. Strongly recommended. |
| `fenceTtl` | `60` | Per-attempt fence lifetime in seconds. Heartbeat extends it while the handler runs. |
| `heartbeatInterval` | `fenceTtl * 1000 / 3` | ms between fence heartbeats |
| `recoveryInterval` | `30000` | ms between recovery sweeps. 0 disables. |
| `recoveryBatchSize` | `10` | Max rows reclaimed per sweep |
| `dispatchInterval` | `5000` | ms between dispatch sweeps that claim `enqueue`d pending rows. 0 disables. |
| `dispatchBatchSize` | `10` | Max pending rows claimed per dispatch sweep |
| `awaitPollInterval` | `500` | ms between row reads while `await()` waits |
| `awaitTimeout` | `60000` | ms after which `await()` rejects if the task is still not terminal. 0 = no timeout. |
| `cleanupInterval` | `3600000` | ms between cleanup sweeps. 0 disables. |
| `rowTtl` | `604800` (7 days) | Seconds to keep terminal rows before deletion |
| `autoMigrate` | `true` | Auto-create the table on first use. Migration is kicked off at construction; `await tasks.ready()` to block until it lands. |
| `onStateChange` | - | Local-worker callback fired AFTER each state-machine transition commits. See [Live observation](#live-observation) below. |
| `breaker` | - | Circuit breaker; bypassed when broken |
| `metrics` | - | Prometheus registry; emits `tasks_*_total` counters |

#### Live observation

The runner owns every state-machine transition; expose what it knows via the `onStateChange` callback or read directly with `list()` / `counts()`. Both work without standing up `postgres/notify`.

```js
const tasks = createTaskRunner(pg, {
  onStateChange(event) {
    // event.{ taskId, name, oldStatus, newStatus, attempt, requestId, result?, error? }
    metrics.inc(`tasks.${event.name}.${event.newStatus}`);
    if (event.newStatus === 'committed') {
      bus.publish(`task:${event.taskId}`, 'committed', { result: event.result });
    }
  }
});

// Wait for migration before polling.
await tasks.ready();

// Dashboard / admin polling tick.
const recent = await tasks.list({ limit: 30 });
const summary = await tasks.counts({ name: 'charge-customer' });
// summary = { pending, running, committed, failed, total }
```

Transitions fired:

- `null -> pending` - `enqueue()` inserts the row.
- `null -> running` - `run()` inserts the first attempt directly.
- `pending -> running` - the dispatch sweep claims the row.
- `running -> running` - retry rearm (within the same `run()` loop) OR recovery sweep (a sibling instance reclaims an expired fence). `attempt` bumps either way.
- `running -> committed` - handler succeeded. `event.result` carries the value.
- `running -> failed` - handler errored past `retry.maxAttempts`. `event.error` carries the JSON-safe `{ name, message, stack?, code?, cause? }` shape.

Errors thrown from the callback (sync or via promise rejection) are caught and `console.warn`ed; they do NOT roll back the state machine. Listeners run on the same tick as the SQL commit but are awaited fire-and-forget; the runner never blocks on them.

Cluster-wide fan-out is a separate concern: `onStateChange` fires on the worker that performed the transition, not across the cluster. Use `postgres/notify` if every instance needs to react.

#### Operator force-takeover

```js
// Drain this instance: kick its in-flight tasks off so the recovery sweep
// on a healthy instance picks them up faster than waiting for the
// fence_expires_at deadline.
const took = await tasks.takeover(taskId);
// took === true if a row was running and got taken over.
```

`takeover(taskId)` expires the Postgres fence AND (when a Redis fence provider is configured) releases the Redis mirror key, cutting abort latency from O(`heartbeatInterval`) to one tick. The recovery sweep then reclaims the row and re-drives the handler under the registered retry policy.

#### Per-handler retry config

Retry is declared at registration so the policy travels with the handler, not with each call site:

```js
tasks.register('flaky-webhook', handler, {
  retry: {
    maxAttempts: 5,
    backoff: (attempt, err) => Math.min(1000 * 2 ** (attempt - 1), 60000),
    on: (err) => !(err instanceof PermanentError)
  }
});
```

Default is no retry on handler-thrown errors - safe for non-idempotent tasks. Stuck recovery (fence-expired-while-running) is always on; it is not the same thing as retry-on-failure.

#### Handler context

```ts
{
  input: TInput,                    // the input passed to run()
  idempotencyKey: string | undefined, // forward to external services
  fence: string,                    // this attempt's fence UUID, read-only
  signal: AbortSignal,              // aborts when the fence is lost
  attempt: number                   // 1-based attempt counter
}
```

The `signal` fires when the heartbeat detects another worker has reclaimed the row (your fence_expires_at passed and the recovery sweep took over). Pass it to `fetch`, Stripe, anything that supports cancellation - the handler should bail gracefully when it fires rather than racing the new owner.

#### Errors

`run()` throws three error shapes:

- `UnknownTaskError` - no handler registered for that name in this process. Recovery does not throw on unknown names because the handler may live on a different deployment.
- `TaskInFlightError` - the idempotency store reports the slot as pending (another caller is mid-flight for the same key). Caller may surface a 409 to the upstream HTTP request or retry after a backoff.
- The handler's own thrown error, after retries are exhausted. Errors are serialised to `{name, message, stack, code, cause}` for the failed-row record and reconstructed as a plain `Error` if a sibling caller reads the row.

#### Async path: `enqueue` + `await`

`run()` blocks the calling process until the handler finishes. Two more verbs let you decouple submission from completion:

- **`enqueue(name, opts)`** - fire-and-forget. Inserts the row with `status='pending'` and returns the `taskId` immediately. A dispatch sweep on any live instance picks up the row and runs the handler in the background.
- **`await(taskId, opts?)`** - block until the task reaches a terminal status. Returns the committed result, throws the stored error, or rejects with a timeout if the task is still pending/running past `awaitTimeout`.

```js
import { tasks } from '$lib/server/tasks';

// Submit a job that will be processed elsewhere
const taskId = await tasks.enqueue('send-welcome-email', {
  input: { userId: locals.user.id },
  idempotencyKey: `welcome-${locals.user.id}`
});

// Optionally block on completion (or fire-and-forget by skipping this)
const result = await tasks.await(taskId, { timeout: 30000 });
```

Use cases:

- **HTTP handler returns 202 quickly**: `enqueue` and respond with the `taskId`. The client polls a status endpoint that reads the row.
- **Cross-instance work distribution**: enqueue from a web tier; a worker tier with the handlers registered picks the row up via dispatch.
- **Decoupling submission from result**: the dispatch sweep also picks up rows whose handler is unknown locally and leaves them in `running` until another instance reclaims them.

The dispatch loop runs every `dispatchInterval` ms (default 5000) and claims up to `dispatchBatchSize` pending rows per sweep via `FOR UPDATE SKIP LOCKED`. Set `dispatchInterval: 0` to disable dispatch entirely (use only `run()` paths).

`await` polls the task row every `awaitPollInterval` ms (default 500) until terminal or `awaitTimeout` ms (default 60000). For most use cases the runner-level defaults are fine; per-call overrides are available:

```js
await tasks.await(taskId, { pollInterval: 100, timeout: 10000 });
```

Errors thrown by the handler are reconstructed from the stored row (`name`, `message`, `stack`, `code`, `cause`) when `await` reads a `failed` row. The reconstructed error is a plain `Error`; the original prototype chain does not survive the JSON round-trip.

#### Worker thread execution

By default the handler runs in the current process. For CPU-bound work that would otherwise block the event loop - image resize, hashing, large JSON parse, anything that genuinely consumes a CPU core for long enough to matter - you can opt in to running the handler in a worker thread.

The handler lives in a separate file whose default export is the handler. The runner spawns a thread pool per task; each thread imports the handler file once at startup and reuses the import across runs. Database and Redis clients cannot be shared across worker threads (native handles do not cross thread boundaries), so the worker file boots its own.

```js
// src/lib/server/workers/resize.js
import sharp from 'sharp';

export default async function resize({ input, signal }) {
  return await sharp(input.imageBuffer, { signal })
    .resize(input.width, input.height)
    .toBuffer();
}
```

```js
// src/lib/server/tasks.js
import { tasks } from './tasks.js';

tasks.register('resize-image', null, {
  worker: new URL('./workers/resize.js', import.meta.url)
});

// Or with explicit pool config:
tasks.register('resize-image', null, {
  worker: {
    path: new URL('./workers/resize.js', import.meta.url),
    pool: { size: 4, idleTimeout: 30000 }
  }
});
```

When `worker` is set, the `handler` argument to `register` must be `null` or omitted - the handler argument exists in the worker file, not at the registration site. Pool defaults: `size: 1`, `idleTimeout: 30000` ms (set to `0` to keep workers warm forever). Workers spawn lazily on first run; idle workers past `idleTimeout` are terminated.

The `signal` in the handler context fires when the runner detects a fence loss, exactly as for in-process handlers. The runner forwards an abort message to the worker; the worker translates it into a local `AbortController.abort()` so the handler can bail.

When *not* to use this:

- I/O-bound tasks (HTTP, database, Stripe). Workers add startup cost (~10ms cold, ~50MB memory) and connection-pool duplication; the event loop already handles I/O concurrency natively.
- Anything that needs to share in-memory state with the main process. Workers have separate memory.

When this *is* the right tool: a single handler that synchronously consumes the event loop for tens of milliseconds or more, where blocking the cluster instance's other work is unacceptable.

#### Redis fence provider (force-takeover detection)

Pass an optional `fence` provider to add a second source of truth for "is this attempt's fence still alive". The Postgres row remains the canonical record of task state; the provider mirrors the fence value to an external store with a short TTL refreshed by heartbeat. On every heartbeat tick the runner consults both sources - either reporting "lost" aborts the handler.

The primary value is force-takeover detection. If an operator manually deletes the fence key (or another instance forcibly releases it), the heartbeat sees the divergence and bails immediately, even if the Postgres `fence_expires_at` would still pass. Useful for ops scenarios like "drain this instance, kick its in-flight tasks off so the recovery sweep on a healthy instance picks them up faster than waiting for the Postgres deadline."

```js
import { createRedisFence } from 'svelte-adapter-uws-extensions/redis/fence';
import { createTaskRunner } from 'svelte-adapter-uws-extensions/postgres/tasks';

export const tasks = createTaskRunner(pg, {
  idempotency,
  fence: createRedisFence(redis, { keyPrefix: 'fence:' })
});
```

The provider exposes `acquire`/`heartbeat`/`release`. The runner pairs each with the matching Postgres operation: `acquire` runs after the row is inserted/rearmed; `heartbeat` runs before the Postgres heartbeat each tick and short-circuits the abort path on its own; `release` is best-effort after a terminal commit/fail.

`createRedisFence` options:

| Option | Default | Description |
|---|---|---|
| `keyPrefix` | `'fence:'` | Prefix prepended (after the client keyPrefix) to every fence key |

The Redis side uses two atomic Lua scripts: heartbeat is `if get == fence then pexpire end`, release is `if get == fence then del end`. No fence held by another owner can be released or refreshed by accident.

---

**Observability**

## Prometheus metrics

Exposes extension metrics in Prometheus text exposition format. No external dependencies. Zero overhead when not enabled - every metric call uses optional chaining on a nullish reference, so V8 short-circuits on a single pointer check.

#### Setup

```js
// src/lib/server/metrics.js
import { createMetrics } from 'svelte-adapter-uws-extensions/prometheus';

export const metrics = createMetrics({
  prefix: 'myapp_',
  mapTopic: (topic) => topic.startsWith('room:') ? 'room:*' : topic
});
```

Pass the `metrics` object to any extension via its options:

```js
import { metrics } from './metrics.js';
import { redis } from './redis.js';
import { createPresence } from 'svelte-adapter-uws-extensions/redis/presence';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';
import { createReplay } from 'svelte-adapter-uws-extensions/redis/replay';
import { createRateLimit } from 'svelte-adapter-uws-extensions/redis/ratelimit';
import { createGroup } from 'svelte-adapter-uws-extensions/redis/groups';
import { createCursor } from 'svelte-adapter-uws-extensions/redis/cursor';

export const bus = createPubSubBus(redis, { metrics });
export const presence = createPresence(redis, { metrics, key: 'id' });
export const replay = createReplay(redis, { metrics });
export const limiter = createRateLimit(redis, { points: 10, interval: 1000, metrics });
export const lobby = createGroup(redis, 'lobby', { metrics });
export const cursors = createCursor(redis, { metrics });
```

#### Mounting the endpoint

With uWebSockets.js:

```js
app.get('/metrics', metrics.handler);
```

Or use `metrics.serialize()` to get the raw text and serve it however you like.

#### Options

| Option | Default | Description |
|---|---|---|
| `prefix` | `''` | Prefix for all metric names |
| `mapTopic` | identity | Map topic names to bounded label values for cardinality control |
| `defaultBuckets` | `[1, 5, 10, 25, 50, 100, 250, 500, 1000]` | Default histogram buckets |

Metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*` and label names must match `[a-zA-Z_][a-zA-Z0-9_]*` (no `__` prefix). Invalid names throw at registration time. HELP text containing backslashes or newlines is escaped automatically.

#### Cardinality control

If your topics are user-generated (e.g. `room:abc123`), per-topic labels will grow unbounded. Use `mapTopic` to collapse them:

```js
const metrics = createMetrics({
  mapTopic: (topic) => {
    if (topic.startsWith('room:')) return 'room:*';
    if (topic.startsWith('user:')) return 'user:*';
    return topic;
  }
});
```

#### WebSocket observability helpers

Two drop-in wirers for adapter telemetry:

```js
import {
  createMetrics,
  wirePublishRateMetrics,
  connectionMetricsHook
} from 'svelte-adapter-uws-extensions/prometheus';

export const metrics = createMetrics({ prefix: 'myapp_' });

// In setup, once you have a `platform`:
wirePublishRateMetrics(platform, metrics, { topN: 10 });

// In hooks.ws.js:
export const close = connectionMetricsHook(metrics);
```

`wirePublishRateMetrics` registers `ws_topic_publish_rate{topic="..."}` and `ws_topic_publish_bytes{topic="..."}` gauges that read `platform.pressure.topPublishers` at scrape time - no continuous accounting on the publish hot path. The `topN` cap (default 10) bounds gauge cardinality; the registry's `mapTopic` (or an inline `mapTopic` option) can further collapse user-generated topic names.

`connectionMetricsHook(metrics, userClose?)` returns a close-hook that emits per-connection histograms (`ws_connection_duration_seconds`, `ws_connection_messages_in` / `_out`, `ws_connection_bytes_in` / `_out`) plus a `ws_connection_close_total{code}` counter from the close-ctx fields the adapter populates. Compose with your own close logic by passing a function as the second argument; it runs after the metrics are recorded:

```js
export const close = connectionMetricsHook(metrics, async (ws, ctx) => {
  // your own teardown - runs after metrics, with the same ctx
});
```

Requires `svelte-adapter-uws >= 0.5.0-next.4`: the `topPublishers` field on the pressure snapshot and the duration / messages / bytes fields on the close ctx are only populated by that version.

#### Metrics reference

**Pub/sub bus**

| Metric | Type | Description |
|---|---|---|
| `pubsub_messages_relayed_total` | counter | Messages relayed to Redis |
| `pubsub_messages_received_total` | counter | Messages received from Redis |
| `pubsub_echo_suppressed_total` | counter | Messages dropped by echo suppression |
| `pubsub_parse_errors_total` | counter | Malformed envelopes dropped on receive |
| `pubsub_relay_batch_size` | histogram | Relay batch size per flush |
| `pubsub_degraded_total` | counter | Auto-emitted `degraded` events |
| `pubsub_recovered_total` | counter | Auto-emitted `recovered` events |

**Sharded pub/sub bus**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `sharded_pubsub_messages_relayed_total` | counter | `topic` | Messages SPUBLISHed |
| `sharded_pubsub_messages_received_total` | counter | `topic` | Messages received via SSUBSCRIBE |
| `sharded_pubsub_echo_suppressed_total` | counter | | Sharded messages dropped by echo suppression |
| `sharded_pubsub_parse_errors_total` | counter | | Malformed envelopes dropped on receive |
| `sharded_pubsub_ssubscribes_total` | counter | | SSUBSCRIBE calls (first follower per channel) |
| `sharded_pubsub_sunsubscribes_total` | counter | | SUNSUBSCRIBE calls (last follower out) |

**Adapter telemetry (`wirePublishRateMetrics` + `connectionMetricsHook`)**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ws_topic_publish_rate` | gauge | `topic` | Messages per second sampled from `platform.pressure.topPublishers` (top N) |
| `ws_topic_publish_bytes` | gauge | `topic` | Bytes per second sampled from `platform.pressure.topPublishers` (top N) |
| `ws_connection_duration_seconds` | histogram | | Connection duration in seconds at close |
| `ws_connection_messages_in` | histogram | | Messages received per connection at close |
| `ws_connection_messages_out` | histogram | | Messages sent per connection at close |
| `ws_connection_bytes_in` | histogram | | Bytes received per connection at close |
| `ws_connection_bytes_out` | histogram | | Bytes sent per connection at close |
| `ws_connection_close_total` | counter | `code` | Connections closed by close code |

**Presence**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `presence_joins_total` | counter | `topic` | Join events |
| `presence_leaves_total` | counter | `topic` | Leave events |
| `presence_heartbeats_total` | counter | | Heartbeat refresh cycles |
| `presence_stale_cleaned_total` | counter | | Stale entries removed by cleanup |
| `presence_total_online` | gauge | `topic` | Unique users present per topic on this instance |
| `presence_heartbeat_latency_ms` | gauge | | Duration of the most recent heartbeat tick in ms |
| `presence_keyspace_cleanups_total` | counter | | Topic hash expiries that triggered an empty-list emit (keyspace mode only) |

**Replay buffer (Redis and Postgres)**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `replay_publishes_total` | counter | `topic` | Messages published |
| `replay_messages_replayed_total` | counter | `topic` | Messages replayed to clients |
| `replay_truncations_total` | counter | `topic` | Truncation events detected |
| `replay_replications_total` | counter | | Publishes confirmed replicated within timeout (Redis only, `durability: 'replicated'` mode) |
| `replay_replication_timeouts_total` | counter | | Publishes that did not reach `minReplicas` within timeout |
| `replay_idmp_hits_total` | counter | `topic` | `publishIdempotent` calls served from the dedup cache (no XADD) |
| `replay_idmp_writes_total` | counter | `topic` | `publishIdempotent` calls that produced a new entry |

**Rate limiting**

| Metric | Type | Description |
|---|---|---|
| `ratelimit_allowed_total` | counter | Requests allowed |
| `ratelimit_denied_total` | counter | Requests denied |
| `ratelimit_bans_total` | counter | Bans applied |

**Broadcast groups**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `group_joins_total` | counter | `group` | Join events |
| `group_joins_rejected_total` | counter | `group` | Joins rejected (full) |
| `group_leaves_total` | counter | `group` | Leave events |
| `group_publishes_total` | counter | `group` | Publish events |

**Cursor**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cursor_updates_total` | counter | `topic` | Cursor update calls |
| `cursor_broadcasts_total` | counter | `topic` | Broadcasts actually sent |
| `cursor_throttled_total` | counter | `topic` | Updates deferred by throttle |

**LISTEN/NOTIFY bridge**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `notify_received_total` | counter | `channel` | Notifications received |
| `notify_parse_errors_total` | counter | `channel` | Parse failures |
| `notify_reconnects_total` | counter | | Reconnect attempts |

**Admission control**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `admission_accepted_total` | counter | `class` | `shouldAccept` calls that returned `true` |
| `admission_rejected_total` | counter | `class`, `reason` | `shouldAccept` calls that returned `false`, labeled with the pressure reason that caused rejection |

**Job queue**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `jobs_enqueued_total` | counter | `queue` | Jobs enqueued |
| `jobs_claimed_total` | counter | `queue` | Jobs claimed (rows returned by `claim`) |
| `jobs_completed_total` | counter | `queue` | Jobs completed (deleted) |
| `jobs_failed_total` | counter | `queue` | Jobs released via `fail()` for retry |

**Redis Functions**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `redis_function_loads_total` | counter | `library` | `FUNCTION LOAD` calls |
| `redis_function_calls_total` | counter | `library`, `function` | `FCALL` calls |
| `redis_function_errors_total` | counter | `library`, `function` | `FCALL` calls that threw |

---

**Reliability**

## Capacity model

Every internal `Map` / `Set` / queue of factory-or-module-level scope declares an explicit upper bound and a documented saturation behavior, so a runaway publisher, a subscribe-in-loop bug, or a topic-cardinality leak can no longer exhaust process memory silently. Mirrors the adapter's `0.5.0-next.8` capacity-cap pattern, scaled one tier up: where the adapter caps **per-connection** at 1M, we cap **per-instance** at 10M (an instance can hold ~1M concurrent uWS connections with ~10 entries of state per connection on average), and **cluster-wide** warn-only caps land at 100M (beyond which the in-memory index itself becomes a real memory concern, ~3.2GB on each instance maintaining the index).

Saturation behavior is matched to each data structure's contract:

- **State the protocol depends on** (`userToInstance` for `sendTo` routing, the secondary index buckets, the cluster-wide user index): **warn-only**. Eviction would corrupt routing or matching, so a single structured `console.warn` fires the first time the cap is crossed - surfacing the leak shape - and the index keeps growing.
- **State with overwrite-by-key contract**: not currently used in extensions (this saturation shape lives in the adapter's `WS_COALESCED` and the throttle/debounce plugins).
- **State bounded by the caller** (registry pending requests, presence ws, cursor ws, sharded bus topics, groups local members): **reject new** - the adding caller surfaces an explicit error, and the saturated entry is not added.
- **Per-tick microtask batches** (sharded bus relay, pubsub relay): **warn-only** - the batch is drained every microtask, so reaching the cap in one tick means a synchronous burst leak; the warn surfaces it without dropping any in-flight publishes.

Caps live as named constants in `shared/caps.js`. They are not currently configurable per-instance; if a real workload needs a tighter or looser bound for a specific module, raise an issue and we'll add a per-factory option.

| Cap | Default | Saturation | Notes |
|---|---|---|---|
| `MAX_REGISTRY_SESSIONS_PER_INSTANCE` | 10_000_000 | reject new | `hooks.open` skips the registration past the cap |
| `MAX_REGISTRY_PENDING_REQUESTS` | 10_000_000 | reject new | `request(...)` rejects with "pending requests exceeded" |
| `MAX_REGISTRY_USER_INDEX` | 100_000_000 | warn-only | cluster-wide; eviction would mis-route `sendTo` |
| `MAX_REGISTRY_INDEX_VALUES_PER_KEY` | 10_000_000 | warn-only | per-attribute-key bucket in the secondary index |
| `MAX_SHARDED_BUS_TOPICS` | 10_000_000 | reject new | `follow` / `followBatch` reject distinct-new past the cap |
| `MAX_SHARDED_BUS_BATCH_CHANNELS_PER_TICK` | 1_000_000 | warn-only | per-microtask outbound batch |
| `MAX_PUBSUB_RELAY_BATCH_PER_TICK` | 1_000_000 | warn-only | per-microtask outbound batch |
| `MAX_PRESENCE_WS` | 10_000_000 | reject new | per-instance ws joins |
| `MAX_PRESENCE_TOPICS` | 10_000_000 | reject new | per-instance topic count |
| `MAX_CURSOR_WS` | 10_000_000 | reject new | per-instance ws cursor activity |
| `MAX_CURSOR_TOPICS` | 10_000_000 | reject new | per-instance cursor topic count |
| `MAX_GROUPS_LOCAL_MEMBERS` | 10_000_000 | reject new | treated as "group full" |
| `MAX_TASK_HANDLERS` | 10_000 | reject new | bootstrap-time `register(name, handler)` calls |
| `MAX_REDIS_DUPLICATES_PER_CLIENT` | 1_000 | warn-only | duplicate ioredis connections per client wrapper |
| `MAX_AGGREGATOR_REMOTE_INSTANCES` | 10_000 | warn-only | sibling instances on the publish-rate aggregator |
| `MAX_BREAKER_LISTENERS` | 10_000 | reject new | listeners on a single breaker |

Aggregate-memory protection still belongs to the adapter's `upgradeAdmission.maxConcurrent` (see the adapter's "Layered admission" section); per-instance caps are not the right place to defend against a 10M-connection DoS.

---

## Production assertions

Critical invariants across the extensions are checked at runtime via a two-tier assertion helper that mirrors the adapter's `0.5.0-next.8` shape. Two helpers, `assert(cond, category, context)` and `devAssert(cond, message, context)`, live in `shared/assert.js` and fire at ~10 invariant sites today (envelope shape on inbound pubsub frames, registry session-shadow consistency, registry events-channel payload type, secondary-index consistency, lock heartbeat-vs-signal-aborted invariant, etc).

#### How violations surface

| Mode | `assert` behavior | `devAssert` behavior |
|---|---|---|
| **Production** (`NODE_ENV === 'production'`) | counter++, structured `[extensions/assert]` log line, **does NOT throw** - a thrown exception inside a Redis pubsub callback or a publish hot-path microtask could leave a half-applied transaction or a corrupted local index | full no-op |
| **Test** (`process.env.VITEST` or `NODE_ENV === 'test'`) | counter++, log, and throws so vitest surfaces the failure as a test error | log only, never throws |
| **Development** (otherwise) | counter++, log, no throw | log only |

`devAssert` is for cosmetic / DX hints (schema-mismatch warnings, etc); `assert` is for invariants whose violation indicates corrupted internal state. The DX framing matches the adapter's: hard assertions never fire on healthy code; if they fire, something is genuinely wrong, and the metric + log surface it without taking the worker down.

#### Wiring the metric

```js
import { createMetrics } from 'svelte-adapter-uws-extensions/prometheus';
import { wireAssertionMetrics } from 'svelte-adapter-uws-extensions/prometheus';

const metrics = createMetrics();
wireAssertionMetrics(metrics);
```

After wiring, every `assert` violation increments `extensions_assertion_violations_total{category}`. The label cardinality is bounded by the number of distinct categories declared in the source - not user-input-driven.

If a counter goes non-zero in production: file an issue with the category name, the log entries, and a description of the workload. The category names follow the convention `<module>.<invariant>` (e.g. `registry.session-shadow.consistency`, `pubsub.envelope.shape`).

#### Reading counters in process

`getAssertionCounters()` from `shared/assert.js` returns the live counter Map. Mirrors the adapter's `platform.assertions` shape - the Map is the live state, not a snapshot, so consumers holding the reference see updates automatically.

```js
import { getAssertionCounters } from 'svelte-adapter-uws-extensions/assert';

if (getAssertionCounters().size > 0) {
  console.warn('extensions: invariant violations detected', getAssertionCounters());
}
```

---

## Failure handling

Every Redis and Postgres extension accepts an optional `breaker` option - a shared [circuit breaker](#circuit-breaker) that tracks backend health across all extensions wired to it. When the breaker trips, each extension degrades differently depending on whether the operation is critical or best-effort:

| Extension | Awaited operations (join, consume, publish) | Fire-and-forget operations |
|---|---|---|
| **Pub/sub bus** | `wrap().publish()` queues to local platform only; relay to Redis is skipped silently | Microtask relay flush is skipped entirely |
| **Presence** | `join()` / `leave()` throw `CircuitBrokenError` | Heartbeat refresh and stale cleanup are skipped |
| **Replay buffer** | `publish()` / `replay()` / `seq()` throw `CircuitBrokenError` | - |
| **Rate limiting** | `consume()` throws `CircuitBrokenError` (fail-closed - requests are blocked, not allowed through) | - |
| **Broadcast groups** | `join()` / `leave()` throw `CircuitBrokenError` | Heartbeat refresh is skipped |
| **Cursor** | - | Hash writes and cross-instance relay are skipped; local throttle continues |
| **LISTEN/NOTIFY** | `activate()` throws; auto-reconnect retries on its own interval | - |

The breaker is a three-state machine: **healthy** (all requests pass through) -> **broken** after N consecutive failures (all requests fail fast via `CircuitBrokenError`) -> **probing** after a timeout (one request is allowed through to test recovery) -> back to **healthy** on success. See [Circuit breaker](#circuit-breaker) for configuration.

#### Notifying clients of degradation

When Redis pub/sub fails, live streams on other replicas stop receiving updates. Connected clients continue showing stale data with no indication that the stream is degraded. The pub/sub bus emits this directly: when the shared breaker leaves the healthy state, a `degraded` event fires on the bus's `systemChannel` (default `'__realtime'`); when it returns to healthy, a `recovered` event fires.

```js
// src/lib/server/bus.js
import { createCircuitBreaker } from 'svelte-adapter-uws-extensions/breaker';
import { createPubSubBus } from 'svelte-adapter-uws-extensions/redis/pubsub';

export const breaker = createCircuitBreaker({ failureThreshold: 5, resetTimeout: 30000 });

export const bus = createPubSubBus(redis, {
  breaker,
  // optional handlers for server-side reactions (logging, alerts):
  onDegraded: () => console.warn('pubsub bus degraded'),
  onRecovered: () => console.info('pubsub bus recovered')
});
```

```js
// src/hooks.ws.js
import { bus } from '$lib/server/bus';

// bus.hooks.open subscribes every connection to the systemChannel.
// Without this wiring, degraded / recovered events publish into an
// empty subscriber set and clients never hear about the outage.
export const { open } = bus.hooks;
```

On the client, subscribe to the `__realtime` topic and show a banner when the `degraded` event fires. On `recovered`, dismiss the banner and refetch stale data. The event payload is `{ at: <epoch ms> }` so a client can show "lost connection 12s ago".

Both the topic name and the auto-emission are configurable:

| Option | Default | Description |
|---|---|---|
| `systemChannel` | `'__realtime'` | Topic used for `degraded` / `recovered` events. Set to `null` or `false` to disable auto-emission. |
| `onDegraded` | - | Server-side handler invoked once on the healthy -> non-healthy transition |
| `onRecovered` | - | Server-side handler invoked once on the non-healthy -> healthy transition |

Auto-emission is local-only - Redis is what's degraded, so the event reaches local clients via the underlying platform without attempting a relay. Each instance reports its own breaker state to its own clients. If you need different semantics (cross-instance forwarding, custom payload, filtering by failure type), use `breaker.subscribe(handler)` to register your own listener and emit through whichever channel you prefer.

---

## Cluster publish-rate aggregator

`createPublishRateAggregator` (`svelte-adapter-uws-extensions/redis/publish-rate`) gives every instance a cluster-wide view of which topics are hottest across the whole deployment. Each instance broadcasts its own `platform.pressure.topPublishers` slice on a Redis pub/sub channel; every instance maintains a sliding-window view of all instances' slices and merges them into a cluster-wide top-N. No leader election - each instance is its own aggregator. Storage cost is `O(instances * topN)` per instance, bounded and small.

#### Setup

```js
// src/lib/server/publish-rate.js
import { redis } from './redis.js';
import { createPublishRateAggregator } from 'svelte-adapter-uws-extensions/redis/publish-rate';

export const aggregator = createPublishRateAggregator(redis, {
  publishInterval: 5000,
  staleAfter: 12000,
  topN: 20
});

// In your open hook (or any startup path with a platform reference):
export async function open(ws, { platform }) {
  await aggregator.activate(platform);
}
```

#### API

| Member | Description |
|---|---|
| `instanceId` | Stable id for this instance, used as the from-tag on outbound slice envelopes. |
| `topPublishers` | Cluster-wide top publishers, merged from this instance's local slice (read fresh from `platform.pressure.topPublishers`) and the cached remote slices (stale entries dropped). Sorted descending by `messagesPerSec`, capped at `topN`. Each entry is `{topic, messagesPerSec, bytesPerSec, contributingInstances}`. Pure memory computation. |
| `rateOf(topic)` | Cluster-wide messagesPerSec for a topic, or 0 if not in the merged top-N. Used by the `clusterTopPublisher` admission rule. |
| `subscribersOf(topic)` | Cluster-wide subscriber count for a topic, summed across this instance's live local count (from the optional `subjects` callback) and cached non-stale remote contributions. Returns 0 when no `subjects` is wired and no remote instance has reported the topic. The sharded bus's `bus.subscribers(topic)` delegates here when an aggregator is wired. |
| `activate(platform)` | Open the subscriber and start the broadcast timer. Idempotent. |
| `deactivate()` | Stop the timer, drop the subscriber, clear cached slices. |

#### Wire envelope (internal)

```
Channel: {channel}                          (default: 'uws:pressure:rates')
Payload: {instanceId, ts, slice: [{topic, messagesPerSec, bytesPerSec}, ...], subs?: [{topic, count}, ...]}
```

Receivers merge into a per-`instanceId` map keyed on the broadcasting instance; entries older than `staleAfter` are dropped on the next merge. The `subs` field is omitted when no `subjects` callback is configured. Aggregators on either side of a version skew tolerate envelopes with or without `subs` (forward and backward compatible).

#### Options

| Option | Default | Description |
|---|---|---|
| `channel` | `'uws:pressure:rates'` | Redis channel for slice broadcasts. |
| `publishInterval` | `5000` | How often this instance broadcasts its slice (ms). |
| `staleAfter` | `12000` | Drop a remote instance's slice if no fresher one arrives within this window (ms). Should be at least `2 * publishInterval`. |
| `topN` | `20` | Cap on per-instance slice and merged result. Bounds storage cost. Also caps the `subs` slice (sorted descending by `count`). |
| `subjects` | - | Optional `() => Array<{topic, count}>` contributor for cluster-wide subscriber counts. Called fresh on every broadcast tick. When wired, the envelope grows a `subs` field and `subscribersOf(topic)` returns the merged sum. Pair with the sharded bus via `subjects: () => bus.localSubjects(platform)`. |
| `breaker` | - | Optional circuit breaker for the publish call. |
| `metrics` | - | Optional Prometheus metrics registry. |

#### Pairing with admission control

`createAdmissionControl({ aggregator, classes: { hot: { clusterTopPublisher: { threshold } } } })` consults `aggregator.rateOf(topic)` on every `shouldAccept` call. Memory-only lookup, no Redis traffic on the hot path. See [Cluster-aware shedding](#cluster-aware-shedding-clustertoppublisher-rule).

#### Pairing with the sharded bus (cluster subscriber counts)

The aggregator's `subjects` option is the channel for cluster-wide subscriber counts. Wire the sharded bus's `localSubjects(platform)` helper as the contributor; the bus exposes a matching `bus.subscribers(topic)` that delegates to `aggregator.subscribersOf(topic)`:

```js
import { createShardedBus } from 'svelte-adapter-uws-extensions/redis/sharded-pubsub';
import { createPublishRateAggregator } from 'svelte-adapter-uws-extensions/redis/publish-rate';

const bus = createShardedBus(redis);
const aggregator = createPublishRateAggregator(redis, {
  subjects: () => bus.localSubjects(platform)
});

// One option to also pass subscribersAggregator into the bus so
// bus.subscribers(topic) returns the cluster-wide count rather than
// just the local one:
const busWithCluster = createShardedBus(redis, { subscribersAggregator: aggregator });

await bus.activate(platform);
await aggregator.activate(platform);

const cluster = busWithCluster.subscribers('chat:room-7');
// Local count + sum from non-stale remote instances.
```

Without an aggregator wired, `bus.subscribers(topic)` returns the local count only - same number `platform.subscribers(topic)` reports. Eventually-consistent within `publishInterval` for the remote contribution; the local read is always live. For exact counts (audit log, billing), track a Redis SET cluster-wide on subscribe / unsubscribe and `SCARD` it.

The unsharded `createPubSubBus` does not track per-topic state (it forwards every topic through a single Redis channel), so it does not expose `localSubjects` / `subscribers`. Apps that need cluster-wide subscriber counts on the unsharded bus thread their own per-topic state into the aggregator's `subjects` callback.

#### Pairing with prometheus

`wireClusterPublishRateMetrics(aggregator, metrics, { topN })` registers two gauges that scrape the merged top-N at collect time:

- `cluster_topic_publish_rate{topic}` - cluster-wide messagesPerSec, summed across instances
- `cluster_topic_publish_bytes{topic}` - cluster-wide bytesPerSec, summed across instances

Both wirers (per-instance via `wirePublishRateMetrics`, cluster via `wireClusterPublishRateMetrics`) can be active simultaneously. The local view shows hot-shard pressure; the cluster view shows global capacity.

#### Aggregator metrics

| Metric | Description |
|---|---|
| `cluster_publish_rate_broadcasts_total` | Slice envelopes published by this instance. |
| `cluster_publish_rate_received_total` | Slice envelopes received from sibling instances. |
| `cluster_publish_rate_parse_errors_total` | Malformed envelopes dropped on receive. |
| `cluster_publish_rate_instance_count` | Gauge: sibling instances contributing slices (excluding self) at scrape time. Useful for cluster-size monitoring. |

---

## Circuit breaker

Prevents thundering herd when a backend goes down. When Redis or Postgres becomes unreachable, every extension that uses the breaker fails fast instead of queueing up timeouts, and fire-and-forget operations (heartbeats, relay flushes, cursor broadcasts) are skipped entirely.

Three states:
- **healthy** - everything works, requests go through
- **broken** - too many failures, requests fail fast via `CircuitBrokenError`
- **probing** - one request is allowed through to test if the backend is back

#### Setup

```js
// src/lib/server/breaker.js
import { createCircuitBreaker } from 'svelte-adapter-uws-extensions/breaker';

export const breaker = createCircuitBreaker({
  failureThreshold: 5,
  resetTimeout: 30000,
  onStateChange: (from, to) => console.log(`circuit: ${from} -> ${to}`)
});
```

Pass the same breaker to all extensions that share a backend:

```js
import { breaker } from './breaker.js';

export const bus = createPubSubBus(redis, { breaker });
export const presence = createPresence(redis, { breaker, key: 'id' });
export const replay = createReplay(redis, { breaker });
export const limiter = createRateLimit(redis, { points: 10, interval: 1000, breaker });
```

Failures from any extension contribute to the same breaker. When one trips it, all others fail fast.

#### Options

| Option | Default | Description |
|---|---|---|
| `failureThreshold` | `5` | Consecutive failures before breaking |
| `resetTimeout` | `30000` | Ms before transitioning from broken to probing |
| `onStateChange` | - | Called on state transitions: `(from, to) => void` |

#### API

| Method / Property | Description |
|---|---|
| `breaker.state` | `'healthy'`, `'broken'`, or `'probing'` |
| `breaker.isHealthy` | `true` only when state is `'healthy'` |
| `breaker.failures` | Current consecutive failure count |
| `breaker.guard()` | Throws `CircuitBrokenError` if the circuit is broken |
| `breaker.success()` | Record a successful operation |
| `breaker.failure()` | Record a failed operation |
| `breaker.reset()` | Force back to healthy |
| `breaker.destroy()` | Clear internal timers |

#### How extensions use it

Awaited operations (join, consume, publish) call `guard()` before the Redis/Postgres call, `success()` after, and `failure()` in the catch block. When the circuit is broken, `guard()` throws `CircuitBrokenError` and the operation never reaches the backend.

Fire-and-forget operations (heartbeat refresh, relay flush, cursor broadcast) check `isHealthy` and skip entirely when the circuit is not healthy. This prevents piling up commands on a dead connection.

#### Error handling

```js
import { CircuitBrokenError } from 'svelte-adapter-uws-extensions/breaker';

try {
  await replay.publish(platform, 'chat', 'msg', data);
} catch (err) {
  if (err instanceof CircuitBrokenError) {
    // Backend is down - degrade gracefully
    platform.publish('chat', 'msg', data); // local-only delivery
  }
}
```

---

## Admission control

Pressure-aware companion to the [circuit breaker](#circuit-breaker). Where the breaker answers "is the backend up?", admission control answers "are we OK to take more work right now?" - using the adapter's `platform.pressure` signal (memory, publish rate, subscriber ratio) to gate non-critical work before it ever reaches a backend.

Requires `svelte-adapter-uws >= 0.5.0-next.1` (the version that ships `platform.pressure`).

#### Setup

```js
// src/lib/server/admission.js
import { createAdmissionControl } from 'svelte-adapter-uws-extensions/admission';

export const ac = createAdmissionControl({
  classes: {
    critical:   ['MEMORY'],                              // refuse only on memory pressure
    normal:     ['MEMORY', 'PUBLISH_RATE'],              // refuse on memory or publish rate
    background: ['MEMORY', 'PUBLISH_RATE', 'SUBSCRIBERS'] // refuse on any pressure
  }
});
```

#### Usage

```js
// In a server endpoint or RPC handler:
import { ac } from '$lib/server/admission';

export async function POST({ platform, request }) {
  if (!ac.shouldAccept('background', platform)) {
    return new Response('busy', { status: 503 });
  }
  // ...proceed with the request...
}
```

Each class is independently configured. The adapter has already collapsed concurrent signals (memory, publish rate, subscribers) into a single most-urgent `reason` - this controller just maps the resolved reason to a per-class accept/reject decision.

#### Class rule shapes

A class rule is either an array of pressure reasons that should block this class, or a predicate function:

```js
classes: {
  // Array form: block when reason is in this list
  critical: ['MEMORY'],

  // Predicate form: block when the predicate returns truthy
  streaming: (snapshot) => snapshot.subscriberRatio > 50
}
```

Predicates receive the full `PressureSnapshot` so they can apply custom thresholds (e.g. block above a specific publish-rate that's tighter than the adapter's). Array form is the simple-case shorthand and is what 90% of callers should use.

Valid reason strings: `'NONE'`, `'PUBLISH_RATE'`, `'SUBSCRIBERS'`, `'MEMORY'`. Including `'NONE'` in a block list means "always block this class," which is occasionally useful for kill-switching a class without removing the wiring.

#### Options

| Option | Default | Description |
|---|---|---|
| `classes` | (required) | Map of class name to admission rule. Must define at least one class. |
| `metrics` | - | Prometheus metrics registry. |

#### API

| Method | Description |
|---|---|
| `shouldAccept(className, platform)` | Returns `true` to admit, `false` to shed. Throws on unknown class name (typo defense) or missing `platform.pressure`. |

`shouldAccept` reads `platform.pressure` via a property access - no I/O, safe to call on every request hot path. The reason-precedence math (memory > publish rate > subscribers > none) lives in the adapter; this method only checks the resolved `reason` against the configured rule.

#### Composition with the breaker

Admission control and the circuit breaker check independent signals. Use them together:

```js
export async function POST({ platform, request }) {
  // Local pressure check first - cheaper, no Redis call.
  if (!ac.shouldAccept('normal', platform)) {
    return new Response('busy', { status: 503 });
  }
  // Then attempt the backend call. CircuitBrokenError surfaces if the
  // breaker is open.
  try {
    await replay.publish(platform, 'chat', 'msg', data);
  } catch (err) {
    if (err instanceof CircuitBrokenError) {
      return new Response('backend unavailable', { status: 503 });
    }
    throw err;
  }
}
```

The two layers complement each other: admission control prevents new work from piling up under server-local pressure; the breaker prevents thundering-herd retries against a dead backend.

#### Cluster-aware shedding (`clusterTopPublisher` rule)

`platform.pressure.topPublishers` is per-instance. In an N-instance cluster, a topic that's hot across every instance simultaneously looks the same locally as one that's only hot here - but it warrants a different response. The `clusterTopPublisher` rule consults a `createPublishRateAggregator` (`redis/publish-rate`) to shed at the cluster layer:

```js
import { createPublishRateAggregator } from 'svelte-adapter-uws-extensions/redis/publish-rate';
import { createAdmissionControl } from 'svelte-adapter-uws-extensions/admission';

const aggregator = createPublishRateAggregator(redis);
await aggregator.activate(platform);

export const ac = createAdmissionControl({
  aggregator,
  classes: {
    nonCritical: { clusterTopPublisher: { threshold: 5000 } }
  }
});

// In a request handler that publishes to a hot topic:
if (!ac.shouldAccept('nonCritical', platform, { topic: 'org:42:audit' })) {
  return new Response('busy', { status: 503 });
}
```

The rule's check is a memory-only lookup (`aggregator.rateOf(topic)` against the merged top-N); no Redis traffic on the hot path. Rejected admissions surface in `admission_rejected_total{class, reason="CLUSTER_TOP_PUBLISHER"}`. See [Cluster publish-rate aggregator](#cluster-publish-rate-aggregator) for aggregator setup.

#### Two-tier admission (handshake + message)

The adapter ships a separate admission layer at the WebSocket handshake path - before any TLS / header work - via the `upgradeAdmission` option on its `wsOptions` (and on `createTestServer` for test harnesses). The two layers operate at different points in the connection lifecycle and are configured independently:

| Layer | Where | Sheds | Configured via |
|---|---|---|---|
| Handshake | Adapter, before `res.upgrade()` | Concurrent in-flight upgrades and per-tick handshake budget | `wsOptions.upgradeAdmission = { maxConcurrent, perTickBudget }` |
| Message / RPC | Extensions, in your handler | Per-class load-shedding against `platform.pressure` | `createAdmissionControl({ classes })` plus a `shouldAccept(...)` check |

Wire both for full coverage of "new connections under storm" and "established connections under pressure":

```js
// svelte.config.js (or wherever you configure the adapter)
import adapter from 'svelte-adapter-uws';

export default {
  kit: {
    adapter: adapter({
      websocket: {
        upgradeAdmission: { maxConcurrent: 200, perTickBudget: 50 }
      }
    })
  }
};
```

```js
// In your message / RPC handler
import { ac } from '$lib/server/admission';

export function message(ws, { data, platform }) {
  if (!ac.shouldAccept('background', platform)) {
    ws.send(JSON.stringify({ error: 'overloaded' }));
    return;
  }
  // ...handle the message...
}
```

Connections that make it past the handshake are not exempt from message-tier shedding, and message-tier shedding cannot rescue a connection that lost the handshake race - the layers compose without overlap. See the adapter's [Layered admission](https://github.com/lanteanio/svelte-adapter-uws#layered-admission) section for the handshake-tier reference.

---

## Redis Functions

`createFunctionLibrary` (`svelte-adapter-uws-extensions/redis/functions`) is a thin wrapper over Redis 7+ `FUNCTION LOAD` / `FCALL`. Versioned, hot-reloadable server-side scripts: ship a new library version and `load()` swaps it in atomically without an app redeploy.

The library code is plain Lua and must start with `#!lua name=<libname>` - the wrapper parses the name from the shebang. Inside the library, declare functions via `redis.register_function(...)`. Function names are global on the Redis server (not namespaced by library), which is why `call(funcName, ...)` keys on function name only.

```js
import { createFunctionLibrary } from 'svelte-adapter-uws-extensions/redis/functions';

const lib = createFunctionLibrary(redis, `#!lua name=ws-presence
redis.register_function('cleanup', function(keys, args)
  - args[1] = now (ms), args[2] = ttl (ms)
  local now = tonumber(args[1])
  local ttl = tonumber(args[2])
  local removed = 0
  - ... iterate hash fields, HDEL stale ...
  return removed
end)
`);

await lib.load();
const removed = await lib.call('cleanup', {
  keys: ['presence:room1'],
  args: [Date.now(), 90000]
});

await lib.delete();  // FUNCTION DELETE
```

#### Options

| Option | Default | Description |
|---|---|---|
| `metrics` | - | Prometheus metrics registry. |
| `breaker` | - | Circuit breaker instance. |

#### API

| Method / Property | Description |
|---|---|
| `lib.name` | Library name parsed from the shebang |
| `lib.load()` | `FUNCTION LOAD REPLACE`. Runs `INFO server` on first call and throws on Redis < 7. Idempotent. |
| `lib.call(funcName, { keys?, args? })` | `FCALL` - returns the function's return value |
| `lib.delete()` | `FUNCTION DELETE <libname>` |

#### When to use this vs `redis.eval`

`redis.eval` is fine for one-off scripts that ship inside the app code. Use `createFunctionLibrary` when:

- Scripts have meaningful versions and ops want to roll forward without an app deploy.
- Multiple scripts belong together as a coherent library (shared helpers, etc.).
- Scripts are large enough that parsing + caching them per `eval` becomes a measurable cost.

Requires Redis 7+. There is no built-in fallback to `EVALSHA` for older servers because that would require maintaining each function in two forms (library function + plain Lua); on Redis 6 just call `redis.eval` directly with your own SHA caching.

---

**Operations**

## Graceful shutdown

All clients listen for the `sveltekit:shutdown` event and disconnect cleanly by default. You can disable this with `autoShutdown: false` and manage the lifecycle yourself.

```js
// Manual shutdown
await redis.quit();
await pg.end();
presence.destroy();
```

---

## Testing

This repo runs tests in two layers. Both stay green; you can run either independently.

```bash
npm test                  # mock layer (24 files, 861 tests, no services needed)
npm run test:integration  # integration layer (real Redis 7 + Postgres 16 in Docker)
```

### Mock layer (`test/`)

In-memory mocks for Redis and Postgres that mirror the public APIs closely enough to drive the extensions through their happy paths and edge cases. Fast feedback (~15s for the full suite), no Docker required.

The mocks live at `testing/mock-redis.js` and `testing/mock-pg.js` and are exported as `svelte-adapter-uws-extensions/testing` so consumers of this package can use them too. See [Testing your own code](#testing-your-own-code) below.

### Integration layer (`test/integration/`)

Exercises the same modules against real services in `docker compose`. Picks up cases the mocks can only approximate: Lua atomicity inside Redis EVAL, Postgres LISTEN/NOTIFY cross-connection delivery, real TTL/EXPIRE behaviour, partial-index plans on the job queue.

The compose stack at [`test/integration/docker-compose.yml`](test/integration/docker-compose.yml) binds non-default host ports so it does not clash with a locally running Postgres/Redis: **Postgres on `55432`**, **Redis on `56379`**. `test/integration/global-setup.js` runs `docker compose up -d --wait` before the suite, exports `INTEGRATION_REDIS_URL` / `INTEGRATION_POSTGRES_URL` for the tests to read, and tears the stack down with `docker compose down -v` afterwards.

The host ports and compose project name are env-var overridable for running multiple stacks side-by-side on the same machine:

```bash
INTEGRATION_REDIS_HOST_PORT=56380 \
INTEGRATION_POSTGRES_HOST_PORT=55433 \
INTEGRATION_COMPOSE_PROJECT=my-slice \
npm run test:integration
```

Project name auto-derives from the port pair when overridden, so unique ports also mean unique container names.

#### Adding a new integration test

1. Drop a `*.test.js` file under `test/integration/redis/` or `test/integration/postgres/`.
2. In `beforeAll`, build a real client:

   ```js
   import { createRedisClient } from '../../../redis/index.js';
   // or: import { createPgClient } from '../../../postgres/index.js';

   beforeAll(() => {
     client = createRedisClient({
       url: process.env.INTEGRATION_REDIS_URL,
       keyPrefix: 'inttest-yourmodule:',  // namespace per test file
       autoShutdown: false                // tests own the lifecycle
     });
   });
   ```
3. In `beforeEach`, wipe state under your prefix (Redis: `SCAN MATCH prefix* + UNLINK`; Postgres: `TRUNCATE` your test tables, or use distinct channels for LISTEN/NOTIFY).
4. In `afterAll`, `await client.quit()` / `await client.end()`.

The integration layer is additive: the mock-based test for a module stays in place when you add the integration counterpart. They cover different failure modes.

### Testing your own code

The `svelte-adapter-uws-extensions/testing` entry point exports the same in-memory mocks used by the extensions' own test suite. Use them to test your extension-consuming code without running Redis or Postgres:

```js
import { mockRedisClient, mockPlatform, mockWs } from 'svelte-adapter-uws-extensions/testing';
import { createPresence } from 'svelte-adapter-uws-extensions/redis/presence';
import { createRateLimit } from 'svelte-adapter-uws-extensions/redis/ratelimit';
import { describe, it, expect } from 'vitest';

describe('presence', () => {
  it('tracks users across topics', async () => {
    const client = mockRedisClient();
    const platform = mockPlatform();
    const presence = createPresence(client, { key: 'id' });

    const ws = mockWs({ id: 'user-1', name: 'Alice' });
    await presence.join(ws, 'room:lobby', platform);

    expect(await presence.count('room:lobby')).toBe(1);
    expect(platform.published.some(p => p.event === 'join')).toBe(true);

    presence.destroy();
  });
});

describe('rate limiting', () => {
  it('blocks after exhausting points', async () => {
    const client = mockRedisClient();
    const limiter = createRateLimit(client, { points: 3, interval: 10000 });
    const ws = mockWs({ remoteAddress: '1.2.3.4' });

    for (let i = 0; i < 3; i++) {
      expect((await limiter.consume(ws)).allowed).toBe(true);
    }
    expect((await limiter.consume(ws)).allowed).toBe(false);
  });
});
```

#### Available mocks

| Export | What it mocks | Supports |
|---|---|---|
| `mockRedisClient(prefix?)` | `createRedisClient()` | Strings, hashes, sorted sets, pub/sub, pipelines, scan, Lua eval for all extension scripts |
| `mockPlatform()` | Platform API | `publish()`, `send()`, `batch()`, `topic()` - records all calls in `.published` and `.sent` |
| `mockWs(userData?)` | uWS WebSocket | `subscribe()`, `unsubscribe()`, `getUserData()`, `getBufferedAmount()`, `close()` |
| `mockPgClient()` | `createPgClient()` | SQL parsing for replay buffer operations, sequence counters |

The circuit breaker (`createCircuitBreaker()`) is pure logic with no I/O - use it directly in tests, no mock needed.

#### Adapter wire-shape helpers

`svelte-adapter-uws-extensions/testing` also re-exports the curated wire-protocol helpers and `userData` slot constants the adapter exposes from `svelte-adapter-uws/testing`, so test code asserting on the wire format or per-connection state has one import location alongside the mocks:

```js
import {
  // Wire-protocol helpers
  esc, completeEnvelope, wrapBatchEnvelope, isValidWireTopic, createScopedTopic,
  // Behavior helpers
  collapseByCoalesceKey, resolveRequestId, createChaosState,
  // userData slot constants
  WS_SUBSCRIPTIONS, WS_COALESCED, WS_SESSION_ID, WS_PENDING_REQUESTS,
  WS_STATS, WS_PLATFORM, WS_CAPS, WS_REQUEST_ID_KEY,
  // Plus the in-memory mocks
  mockRedisClient, mockPlatform, mockWs, mockPgClient
} from 'svelte-adapter-uws-extensions/testing';
```

The re-exported names are the exact same identities as the adapter source (`expect(extensionsTesting.wrapBatchEnvelope).toBe(adapterTesting.wrapBatchEnvelope)`); a surface-lock test in this package pins the set so a future adapter refactor that drops one fails here. See the adapter's [testing entry point docs](https://github.com/lanteanio/svelte-adapter-uws#testing-your-own-handlers) for the per-helper reference.

`createTestServer` is intentionally not re-exported - it boots a real uWebSockets.js instance, which is the adapter's responsibility; import it directly from `svelte-adapter-uws/testing` if you need it.

The adapter's `__chaos` harness on `createTestServer` covers the WS-frame outbound path (drop / delay frames going to connected clients). It does **not** reach traffic on other transports - ioredis, pg, NATS, custom HTTP backends - because each of those goes through its own client, not through the test server's outbound chokepoint. To inject faults at those wires, wrap the transport client in a chaos proxy: `createChaosState` is re-exported above and composes with any client method via a small `Proxy` wrapper. See the adapter's [Wrap your own transport for cross-wire chaos](https://github.com/lanteanio/svelte-adapter-uws#wrap-your-own-transport-for-cross-wire-chaos) section for the pattern. The `__chaos` JSDoc on the adapter's [testing.d.ts](https://github.com/lanteanio/svelte-adapter-uws/blob/main/testing.d.ts) names the WS-only scope explicitly so tests reaching for cross-wire coverage see the boundary at the type level.

---

## Related projects

- [svelte-adapter-uws](https://github.com/lanteanio/svelte-adapter-uws) - The core adapter this package extends. Single-process WebSocket pub/sub, presence, replay, and more for SvelteKit on uWebSockets.js.
- [svelte-realtime](https://github.com/lanteanio/svelte-realtime) - Opinionated full-stack starter built on the adapter. Auth, database, real-time CRUD, and deployment config out of the box.
- [svelte-realtime-demo](https://github.com/lanteanio/svelte-realtime-demo) - Live demo of svelte-realtime. [Try it here.](https://svelte-realtime-demo.lantean.io/)

---

## License

MIT
