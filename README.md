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

**Cross-backend**
- [Idempotency store](#idempotency-store)
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

Factory that wraps [pg](https://github.com/brianc/node-postgres) Pool with lifecycle management.

```js
// src/lib/server/pg.js
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';

export const pg = createPgClient({
  connectionString: 'postgres://localhost:5432/mydb'
});
```

#### Options

| Option | Default | Description |
|---|---|---|
| `connectionString` | *required* | Postgres connection string |
| `autoShutdown` | `true` | Disconnect on `sveltekit:shutdown` |
| `options` | `{}` | Extra pg Pool options |

#### API

| Method | Description |
|---|---|
| `pg.pool` | The underlying pg Pool |
| `pg.query(text, values?)` | Run a query |
| `pg.createClient()` | New standalone pg.Client with same config (not from the pool) |
| `pg.end()` | Gracefully close the pool |

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

export function open(ws, { platform }) {
  // Start subscriber (idempotent, only subscribes once)
  bus.activate(platform);
  // Get a wrapped platform that publishes to Redis + local
  distributed = bus.wrap(platform);
}

export function message(ws, { data, platform }) {
  const msg = JSON.parse(Buffer.from(data).toString());
  // This publish reaches local clients AND all other instances
  distributed.publish('chat', 'message', msg);
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `channel` | `'uws:pubsub'` | Redis channel name |
| `systemChannel` | `'__realtime'` | Topic for auto-emitted `degraded` / `recovered` events. `null` or `false` to disable. Requires a `breaker` |
| `onDegraded` | -- | Server-side handler invoked once when the breaker leaves the healthy state |
| `onRecovered` | -- | Server-side handler invoked once when the breaker returns to the healthy state |

See [Notifying clients of degradation](#notifying-clients-of-degradation) for the full pattern.

#### API

| Method | Description |
|---|---|
| `bus.wrap(platform)` | Returns a new Platform whose `publish()` sends to Redis + local |
| `bus.activate(platform)` | Start the Redis subscriber (idempotent) |
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

`bus.hooks` is the recommended path -- it tracks per-`ws` subscription state and refcounts so the bus only `SSUBSCRIBE`s on the first follower per channel and `SUNSUBSCRIBE`s on the last one out.

#### Options

| Option | Default | Description |
|---|---|---|
| `channelPrefix` | `'uws:sharded:'` | Prefix for sharded pub/sub channels |
| `shardKey` | `(topic) => topic` | Map a topic to a shard label. The channel is `channelPrefix + shardKey(topic)`. Default: identity (one channel per topic). |

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

Sequence numbers are incremented atomically via a Lua script (`INCR` + `ZADD` + trim in a single `EVAL`), so concurrent publishes from multiple instances produce strictly ordered, gap-free sequences per topic. When the buffer exceeds `size`, the oldest entries are removed inside the same Lua script -- no second round trip required.

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

#### Options

| Option | Default | Description |
|---|---|---|
| `storage` | `'sortedset'` | Backend: `'sortedset'` (default) uses ZADD; `'stream'` uses XADD. See [Stream backend](#stream-backend). |
| `size` | `1000` | Max messages per topic |
| `ttl` | `0` | Key expiry in seconds (0 = never) |
| `durability` | -- | Set to `'replicated'` for per-publish replication signalling. See [Replicated durability](#replicated-durability). |
| `minReplicas` | `1` | Minimum replicas that must ack (only with `durability: 'replicated'`). |
| `replicationTimeoutMs` | `1000` | Per-publish replication timeout in ms. `0` blocks indefinitely (Redis WAIT semantics). |

#### Stream backend

`storage: 'stream'` dispatches to a Redis Streams implementation (`XADD`/`XRANGE`) instead of the default sorted-set one. Same external contract -- the same `publish` / `seq` / `gap` / `since` / `replay` / `clear` methods, same `durability: 'replicated'` mode, same metrics. Two changes under the hood:

- Listpack encoding is more compact than sorted-set encoding for typical message shapes -- meaningfully smaller memory for buffers in the thousands of entries per topic.
- Stream IDs are `<seq>-0` where `seq` is the same INCR counter the sorted-set backend uses. `XRANGE` against `(seq-0` filters natively by sequence number, so range queries skip the app-side scan the sorted-set backend does for some paths.

```js
const replay = createReplay(redis, {
  storage: 'stream',
  size: 10000
});
```

Both backends use the same seq-counter key (`{prefix}replay:seq:{topic}`) but different buf-key prefixes (`replay:buf:{topic}` for sorted-set, `replay:streambuf:{topic}` for stream), so they can coexist on the same Redis without WRONGTYPE collisions. A single topic should pick one backend at startup and stay there -- there is no built-in migration helper for switching an existing topic from one backend to the other (greenfield deployments don't need it; if you have one in flight and need to migrate, drain consumers, copy entries with a one-off script, and switch).

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

On a fresh `(producerId, requestId)` tuple, the call performs the same INCR + XADD + broadcast as `publish()` and stashes `seq` in a per-(producer, topic) dedup hash. On a repeat tuple within `idempotencyTtl` (default 48 hours), the call returns the cached seq, skips the XADD, and skips the local broadcast -- the original publish already broadcast to live consumers, and reconnecting consumers pick the entry up via `replay()` from the buffer.

The `seq` counter only advances on fresh writes, so duplicate retries do not introduce gaps that would trigger false-positive truncation events on consumers.

The dedup cache key is `{prefix}replay:idmp:{producerId}:{topic}` -- topic-scoped so the same `requestId` can be reused across topics without collision. Override the TTL per call via `opts.idempotencyTtl`, or globally via `idempotencyTtl` on `createReplay`.

This pairs with the durable task runner (`postgres/tasks`): a task that publishes to the replay buffer can pass its task id as `requestId` so worker-crash retries don't double-publish.

#### Replicated durability

For loss-sensitive flows (audit logs, financial events) opt in with `durability: 'replicated'`. After the write to the master, `publish()` runs `WAIT minReplicas replicationTimeoutMs`. If fewer than `minReplicas` replicas ack within the timeout, `publish()` throws `ReplicationTimeoutError` and skips the local broadcast -- the data is on the master only, and broadcasting would commit live consumers to state that could be lost if the master fails before replicas catch up.

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

The data is in the buffer on the master regardless of the WAIT outcome -- other instances doing `replay()` will still see it. Only the local broadcast is suppressed when the durability signal fails. WAIT command-level errors (network/protocol) bubble up as the original error and DO count as a circuit-breaker failure; an under-acked timeout is a separate signal layer and does NOT trip the breaker.

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

Joins are staged with full rollback on failure: local state is set up first, then the Redis hash field is written, then the WebSocket is subscribed. If any step fails (circuit breaker trips, Redis is down, WebSocket closed during an async gap), all prior steps are undone -- local maps, the Redis field, and any broadcast join event are reversed. This prevents ghost entries that would show a user as online when they never fully connected.

Leaves use an atomic Lua script (`LEAVE_SCRIPT`) that removes this instance's field from the hash and then scans remaining fields for the same user key, ignoring stale entries. Leave is only broadcast when no other instance holds a live entry for that user, preventing premature "user left" notifications in multi-instance deployments.

Zombie cleanup runs on the heartbeat interval. Each tick, every tracked WebSocket is probed via `getBufferedAmount()` -- if the call throws, the socket is dead and its presence is removed synchronously before the heartbeat writes to Redis. The heartbeat then refreshes timestamps on all live entries via a Redis pipeline and runs a server-side Lua cleanup script (`CLEANUP_SCRIPT`) that scans the hash and removes any fields whose timestamp exceeds the TTL. This handles crashed instances whose close handlers never fired.

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
| `keyspaceNotifications` | `false` | Subscribe to Redis `__keyevent@*__:expired`. When a presence hash key expires (instance-died scenario), this instance's local subscribers receive an empty `list` event. See [Keyspace cleanup mode](#keyspace-cleanup-mode). |

#### API

| Method | Description |
|---|---|
| `join(ws, topic, platform)` | Add connection to presence |
| `leave(ws, platform, topic?)` | Remove from a specific topic, or all topics if omitted |
| `sync(ws, topic, platform)` | Send list without joining |
| `list(topic)` | Get current users |
| `count(topic)` | Count unique users |
| `metrics()` | Synchronous snapshot: `{ totalOnline, heartbeatLatencyMs, staleCleanedTotal }`. See [Metrics snapshot](#metrics-snapshot). |
| `clear()` | Reset all presence state |
| `destroy()` | Stop heartbeat and subscriber |
| `hooks` | `{ subscribe, close }` -- ready-made WebSocket hooks. Destructure for one-line `hooks.ws.js` setup. |

#### Metrics snapshot

`presence.metrics()` returns a synchronous snapshot of in-memory state:

| Field | Description |
|---|---|
| `totalOnline` | Sum of unique-users-per-topic across all topics this instance is locally tracking. The same user in two topics counts twice; per-topic counts sum cleanly. |
| `heartbeatLatencyMs` | Duration of the most recent heartbeat tick in milliseconds. Useful as a rough Redis-health indicator -- a tick that suddenly takes longer than usual is likely waiting on a slow Redis. |
| `staleCleanedTotal` | Cumulative count of stale fields removed by the heartbeat-driven cleanup script since this instance started. A non-zero rate means crashed sibling instances' presence is being cleaned up; a zero rate is the healthy steady state. |

The same numbers are exposed as Prometheus when a `metrics` registry is attached: `presence_total_online{topic="..."}` (gauge), `presence_heartbeat_latency_ms` (gauge), `presence_stale_cleaned_total` (counter, already shipped pre-0.5.0).

#### Keyspace cleanup mode

By default a sync-only observer (a connection that called `presence.sync()` to watch a room without joining it) only learns about leaves when the tracking instance broadcasts a `leave` event. If the tracking instance crashes, the broadcast never fires and the observer's UI shows stale data until the page is reloaded.

`keyspaceNotifications: true` closes that gap by `psubscribe`-ing to `__keyevent@*__:expired`. When the presence hash key for a topic expires (which happens once no instance is heartbeating the topic anymore -- typically because the only tracker crashed), this instance emits an empty `list` event on `__presence:<topic>` so local subscribers can refresh their UI to "no one here."

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

(or any flagset that includes both `K`/`E` and `x` -- e.g. `Ex`, `KEA`, etc.) If the `psubscribe` call fails because keyspace events are off, the failure is logged once and the rest of the tracker keeps working without the keyspace branch.

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

Note: the API signature is `createGroup(client, name, options)` instead of `createGroup(name, options)` -- the Redis client is the first argument.

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

Hash entries have a TTL so stale cursors from crashed instances get cleaned up automatically.

#### Setup

```js
// src/lib/server/cursors.js
import { redis } from './redis.js';
import { createCursor } from 'svelte-adapter-uws-extensions/redis/cursor';

export const cursors = createCursor(redis, {
  throttle: 50,
  select: (userData) => ({ id: userData.id, name: userData.name, color: userData.color }),
  ttl: 30
});
```

#### Usage

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
}
```

#### Options

| Option | Default | Description |
|---|---|---|
| `throttle` | `50` | Minimum ms between broadcasts per user per topic |
| `select` | strips `__`-prefixed keys | Extract user data to broadcast alongside position |
| `ttl` | `30` | Per-entry TTL in seconds (auto-refreshed on each broadcast). Stale entries from crashed instances are filtered out individually, even if other instances are still active on the same topic. |

#### API

| Method | Description |
|---|---|
| `update(ws, topic, data, platform)` | Broadcast cursor position (throttled per user per topic) |
| `remove(ws, platform, topic?)` | Remove from a specific topic, or all topics if omitted |
| `list(topic)` | Get current positions across all instances |
| `clear()` | Reset all local and Redis state |
| `destroy()` | Stop the Redis subscriber and clear timers |

---

**Postgres extensions**

## Replay buffer (Postgres)

Same API as the Redis replay buffer, but backed by a Postgres table. Best suited for durable audit trails or history that needs to survive longer than Redis TTLs. Sequence numbers are generated atomically via a dedicated `_seq` table using `INSERT ... ON CONFLICT DO UPDATE`, so concurrent publishes from multiple instances produce strictly ordered sequences with no duplicates or gaps.

Buffer trimming runs after each publish by deleting rows with `seq <= currentSeq - maxSize`. If the trim query fails, the publish still succeeds -- the periodic background cleanup (configurable via `cleanupInterval`) catches any excess rows later.

Same gap detection behavior as the Redis replay buffer: if the client's last-seen sequence is older than the oldest buffered row, or the buffer is empty but the sequence counter has advanced, a `truncated` event fires before replay. The standalone `gap(topic, lastSeenSeq)` probe is also available with the same `{ truncated, missingFrom }` shape; the gap query uses the `(topic, seq)` index for an O(log n) seek rather than scanning the buffer.

The aggregate-vs-broadcast guidance from the [Redis replay section](#aggregate-vs-broadcast-topics) applies equally here -- one topic per aggregate keeps the buffer size budget meaningful and gap detection actionable.

#### Setup

```js
// src/lib/server/replay.js
import { pg } from './pg.js';
import { createReplay } from 'svelte-adapter-uws-extensions/postgres/replay';

export const replay = createReplay(pg, {
  table: 'ws_replay',
  size: 1000,
  ttl: 86400,       // 24 hours
  autoMigrate: true  // auto-create table
});
```

#### Schema

The table is created automatically on first use (if `autoMigrate` is true):

```sql
CREATE TABLE IF NOT EXISTS ws_replay (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  seq BIGINT NOT NULL,
  event TEXT NOT NULL,
  data JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ws_replay_topic_seq ON ws_replay (topic, seq);

CREATE TABLE IF NOT EXISTS ws_replay_seq (
  topic TEXT PRIMARY KEY,
  seq BIGINT NOT NULL DEFAULT 0
);
```

#### Options

| Option | Default | Description |
|---|---|---|
| `table` | `'ws_replay'` | Table name |
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

Listens on a Postgres channel for notifications and forwards them to `platform.publish()`. You provide the trigger on your table -- this module handles the listening side.

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

The client side needs no changes -- the core `crud('messages')` store already handles `created`, `updated`, and `deleted` events.

#### Options

| Option | Default | Description |
|---|---|---|
| `channel` | *required* | Postgres LISTEN channel name |
| `parse` | JSON with `{ topic, event, data }` | Parse notification payload into a publish call. Return null to skip. |
| `autoReconnect` | `true` | Reconnect on connection loss |
| `reconnectInterval` | `3000` | ms between reconnect attempts |
| `multiListener` | `'all'` | `'all'`: every replica opens its own LISTEN (current default). `'advisory'`: leader-elected via `pg_try_advisory_lock`. See [Single-listener mode](#single-listener-mode). |
| `lockId` | -- | Advisory lock id. Required when `multiListener: 'advisory'`. |
| `pollInterval` | `5000` | ms between leader-election polls (advisory mode only). |

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

**Requires a cross-instance pub/sub bus.** In `'all'` mode the bridge passes `relay: false` because every replica's local LISTEN already delivers the notification. In `'advisory'` mode only the leader has LISTEN active, so the leader publishes *with* relay -- the bus fans out to non-leader replicas via Redis. Without a bus the leader's local clients receive notifications but other replicas' clients don't.

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

**Cross-backend**

## Idempotency store

Caches the result of an effectful operation under a stable key so retries within `ttl` return the original outcome rather than re-executing. Use it for HTTP/RPC retries, webhook redeliveries, and any handler where the caller may legitimately repeat a request that must execute at most once -- charge-customer, send-email, create-order.

The store exposes three states via `acquire(key)`:

- **acquired** -- you own the slot. Run the work, then call `commit(result)` on success or `abort()` on failure.
- **pending** -- another caller acquired the slot and has not committed yet. Decide locally whether to return a 409, retry later, or wait.
- **result** -- a previous run committed. The cached value is returned.

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
  table: 'ws_idempotency',
  ttl: 48 * 3600,
  acquireTtl: 60,
  autoMigrate: true
});
```

The Postgres backend periodically deletes expired rows (configurable via `cleanupInterval`, default 60s, 0 to disable). Stale pending rows clear on the next sweep without manual intervention.

The Postgres table is created automatically on first use:

```sql
CREATE TABLE IF NOT EXISTS ws_idempotency (
  key        TEXT        PRIMARY KEY,
  status     TEXT        NOT NULL,
  result     JSONB,
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ws_idempotency_expires_at ON ws_idempotency (expires_at);
```

#### Usage

```js
// Wrap an effectful handler.  The caller passes a stable key per logical
// operation; identical retries return the cached result.
export async function placeOrder(input, ctx) {
  const key = `order:${ctx.user.id}:${input.clientOrderId}`;

  const slot = await idempotency.acquire(key);
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
| `table` (Postgres) | `'ws_idempotency'` | Table name |
| `ttl` | `172800` (48h) | Result cache lifetime in seconds |
| `acquireTtl` | `60` | Pending-slot lifetime in seconds (anti-deadlock) |
| `autoMigrate` (Postgres) | `true` | Auto-create the table on first use |
| `cleanupInterval` (Postgres) | `60000` | Periodic expired-row cleanup interval in ms (0 to disable) |
| `breaker` | -- | Circuit breaker; bypassed when broken |
| `metrics` | -- | Prometheus registry; emits `idempotency_*_total` counters |

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

## Task runner

Wraps an effectful operation in a state machine that survives process crashes and naturally fans across cluster instances. Use it for background work that absolutely must finish exactly once: charging a customer, sending a transactional email, posting to a webhook, kicking off a long-running pipeline.

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

tasks.register('charge-customer', async ({ input, idempotencyKey, signal }) => {
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
  pay: async ({ request, locals }) => {
    const { amount } = Object.fromEntries(await request.formData());
    const result = await tasks.run('charge-customer', {
      input: { amount, customerId: locals.user.stripeCustomerId },
      idempotencyKey: `charge-${locals.user.id}-${request.headers.get('idempotency-key')}`
    });
    return { success: true, paymentIntentId: result.id };
  }
};
```

#### Schema

The table is created automatically on first use (if `autoMigrate` is true):

```sql
CREATE TABLE IF NOT EXISTS ws_tasks (
  task_id          UUID         PRIMARY KEY,
  name             TEXT         NOT NULL,
  input            JSONB,
  idempotency_key  TEXT,
  status           TEXT         NOT NULL,  -- 'running' | 'committed' | 'failed'
  result           JSONB,
  error            JSONB,
  fence            UUID         NOT NULL,
  fence_expires_at TIMESTAMPTZ  NOT NULL,
  attempts         INT          NOT NULL DEFAULT 1,
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ws_tasks_running_fence
    ON ws_tasks (fence_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_ws_tasks_terminal_updated
    ON ws_tasks (updated_at) WHERE status IN ('committed', 'failed');
```

#### Options

| Option | Default | Description |
|---|---|---|
| `table` | `'ws_tasks'` | Table name |
| `idempotency` | -- | An idempotency store ([above](#idempotency-store)). When provided, results are cached per `idempotencyKey`. Strongly recommended. |
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
| `autoMigrate` | `true` | Auto-create the table on first use |
| `breaker` | -- | Circuit breaker; bypassed when broken |
| `metrics` | -- | Prometheus registry; emits `tasks_*_total` counters |

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

Default is no retry on handler-thrown errors -- safe for non-idempotent tasks. Stuck recovery (fence-expired-while-running) is always on; it is not the same thing as retry-on-failure.

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

The `signal` fires when the heartbeat detects another worker has reclaimed the row (your fence_expires_at passed and the recovery sweep took over). Pass it to `fetch`, Stripe, anything that supports cancellation -- the handler should bail gracefully when it fires rather than racing the new owner.

#### Errors

`run()` throws three error shapes:

- `UnknownTaskError` -- no handler registered for that name in this process. Recovery does not throw on unknown names because the handler may live on a different deployment.
- `TaskInFlightError` -- the idempotency store reports the slot as pending (another caller is mid-flight for the same key). Caller may surface a 409 to the upstream HTTP request or retry after a backoff.
- The handler's own thrown error, after retries are exhausted. Errors are serialised to `{name, message, stack, code, cause}` for the failed-row record and reconstructed as a plain `Error` if a sibling caller reads the row.

#### Async path: `enqueue` + `await`

`run()` blocks the calling process until the handler finishes. Two more verbs let you decouple submission from completion:

- **`enqueue(name, opts)`** -- fire-and-forget. Inserts the row with `status='pending'` and returns the `taskId` immediately. A dispatch sweep on any live instance picks up the row and runs the handler in the background.
- **`await(taskId, opts?)`** -- block until the task reaches a terminal status. Returns the committed result, throws the stored error, or rejects with a timeout if the task is still pending/running past `awaitTimeout`.

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

By default the handler runs in the current process. For CPU-bound work that would otherwise block the event loop -- image resize, hashing, large JSON parse, anything that genuinely consumes a CPU core for long enough to matter -- you can opt in to running the handler in a worker thread.

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

When `worker` is set, the `handler` argument to `register` must be `null` or omitted -- the handler argument exists in the worker file, not at the registration site. Pool defaults: `size: 1`, `idleTimeout: 30000` ms (set to `0` to keep workers warm forever). Workers spawn lazily on first run; idle workers past `idleTimeout` are terminated.

The `signal` in the handler context fires when the runner detects a fence loss, exactly as for in-process handlers. The runner forwards an abort message to the worker; the worker translates it into a local `AbortController.abort()` so the handler can bail.

When *not* to use this:

- I/O-bound tasks (HTTP, database, Stripe). Workers add startup cost (~10ms cold, ~50MB memory) and connection-pool duplication; the event loop already handles I/O concurrency natively.
- Anything that needs to share in-memory state with the main process. Workers have separate memory.

When this *is* the right tool: a single handler that synchronously consumes the event loop for tens of milliseconds or more, where blocking the cluster instance's other work is unacceptable.

#### Redis fence provider (force-takeover detection)

Pass an optional `fence` provider to add a second source of truth for "is this attempt's fence still alive". The Postgres row remains the canonical record of task state; the provider mirrors the fence value to an external store with a short TTL refreshed by heartbeat. On every heartbeat tick the runner consults both sources -- either reporting "lost" aborts the handler.

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

Exposes extension metrics in Prometheus text exposition format. No external dependencies. Zero overhead when not enabled -- every metric call uses optional chaining on a nullish reference, so V8 short-circuits on a single pointer check.

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

#### Metrics reference

**Pub/sub bus**

| Metric | Type | Description |
|---|---|---|
| `pubsub_messages_relayed_total` | counter | Messages relayed to Redis |
| `pubsub_messages_received_total` | counter | Messages received from Redis |
| `pubsub_echo_suppressed_total` | counter | Messages dropped by echo suppression |
| `pubsub_relay_batch_size` | histogram | Relay batch size per flush |
| `pubsub_degraded_total` | counter | Auto-emitted `degraded` events |
| `pubsub_recovered_total` | counter | Auto-emitted `recovered` events |

**Sharded pub/sub bus**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `sharded_pubsub_messages_relayed_total` | counter | `topic` | Messages SPUBLISHed |
| `sharded_pubsub_messages_received_total` | counter | `topic` | Messages received via SSUBSCRIBE |
| `sharded_pubsub_echo_suppressed_total` | counter | | Sharded messages dropped by echo suppression |
| `sharded_pubsub_ssubscribes_total` | counter | | SSUBSCRIBE calls (first follower per channel) |
| `sharded_pubsub_sunsubscribes_total` | counter | | SUNSUBSCRIBE calls (last follower out) |

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

**Redis Functions**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `redis_function_loads_total` | counter | `library` | `FUNCTION LOAD` calls |
| `redis_function_calls_total` | counter | `library`, `function` | `FCALL` calls |
| `redis_function_errors_total` | counter | `library`, `function` | `FCALL` calls that threw |

---

**Reliability**

## Failure handling

Every Redis and Postgres extension accepts an optional `breaker` option -- a shared [circuit breaker](#circuit-breaker) that tracks backend health across all extensions wired to it. When the breaker trips, each extension degrades differently depending on whether the operation is critical or best-effort:

| Extension | Awaited operations (join, consume, publish) | Fire-and-forget operations |
|---|---|---|
| **Pub/sub bus** | `wrap().publish()` queues to local platform only; relay to Redis is skipped silently | Microtask relay flush is skipped entirely |
| **Presence** | `join()` / `leave()` throw `CircuitBrokenError` | Heartbeat refresh and stale cleanup are skipped |
| **Replay buffer** | `publish()` / `replay()` / `seq()` throw `CircuitBrokenError` | -- |
| **Rate limiting** | `consume()` throws `CircuitBrokenError` (fail-closed -- requests are blocked, not allowed through) | -- |
| **Broadcast groups** | `join()` / `leave()` throw `CircuitBrokenError` | Heartbeat refresh is skipped |
| **Cursor** | -- | Hash writes and cross-instance relay are skipped; local throttle continues |
| **LISTEN/NOTIFY** | `activate()` throws; auto-reconnect retries on its own interval | -- |

The breaker is a three-state machine: **healthy** (all requests pass through) -> **broken** after N consecutive failures (all requests fail fast via `CircuitBrokenError`) -> **probing** after a timeout (one request is allowed through to test recovery) -> back to **healthy** on success. See [Circuit breaker](#circuit-breaker) for configuration.

#### Notifying clients of degradation

When Redis pub/sub fails, live streams on other replicas stop receiving updates. Connected clients continue showing stale data with no indication that the stream is degraded. The pub/sub bus emits this directly: when the shared breaker leaves the healthy state, a `degraded` event fires on the bus's `systemChannel` (default `'__realtime'`); when it returns to healthy, a `recovered` event fires.

```js
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

On the client side, subscribe to the `__realtime` topic and show a banner when the `degraded` event fires. On `recovered`, dismiss the banner and refetch stale data. The event payload is `{ at: <epoch ms> }` so a client can show "lost connection 12s ago".

Both the topic name and the auto-emission are configurable:

| Option | Default | Description |
|---|---|---|
| `systemChannel` | `'__realtime'` | Topic used for `degraded` / `recovered` events. Set to `null` or `false` to disable auto-emission. |
| `onDegraded` | -- | Server-side handler invoked once on the healthy -> non-healthy transition |
| `onRecovered` | -- | Server-side handler invoked once on the non-healthy -> healthy transition |

Auto-emission is local-only -- Redis is what's degraded, so the event reaches local clients via the underlying platform without attempting a relay. Each instance reports its own breaker state to its own clients. If you need different semantics (cross-instance forwarding, custom payload, filtering by failure type), use `breaker.subscribe(handler)` to register your own listener and emit through whichever channel you prefer.

---

## Circuit breaker

Prevents thundering herd when a backend goes down. When Redis or Postgres becomes unreachable, every extension that uses the breaker fails fast instead of queueing up timeouts, and fire-and-forget operations (heartbeats, relay flushes, cursor broadcasts) are skipped entirely.

Three states:
- **healthy** -- everything works, requests go through
- **broken** -- too many failures, requests fail fast via `CircuitBrokenError`
- **probing** -- one request is allowed through to test if the backend is back

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
    // Backend is down -- degrade gracefully
    platform.publish('chat', 'msg', data); // local-only delivery
  }
}
```

---

## Admission control

Pressure-aware companion to the [circuit breaker](#circuit-breaker). Where the breaker answers "is the backend up?", admission control answers "are we OK to take more work right now?" -- using the adapter's `platform.pressure` signal (memory, publish rate, subscriber ratio) to gate non-critical work before it ever reaches a backend.

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

Each class is independently configured. The adapter has already collapsed concurrent signals (memory, publish rate, subscribers) into a single most-urgent `reason` -- this controller just maps the resolved reason to a per-class accept/reject decision.

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
| `metrics` | -- | Prometheus metrics registry. |

#### API

| Method | Description |
|---|---|
| `shouldAccept(className, platform)` | Returns `true` to admit, `false` to shed. Throws on unknown class name (typo defense) or missing `platform.pressure`. |

`shouldAccept` reads `platform.pressure` via a property access -- no I/O, safe to call on every request hot path. The reason-precedence math (memory > publish rate > subscribers > none) lives in the adapter; this method only checks the resolved `reason` against the configured rule.

#### Composition with the breaker

Admission control and the circuit breaker check independent signals. Use them together:

```js
export async function POST({ platform, request }) {
  // Local pressure check first -- cheaper, no Redis call.
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

---

## Redis Functions

`createFunctionLibrary` (`svelte-adapter-uws-extensions/redis/functions`) is a thin wrapper over Redis 7+ `FUNCTION LOAD` / `FCALL`. Versioned, hot-reloadable server-side scripts: ship a new library version and `load()` swaps it in atomically without an app redeploy.

The library code is plain Lua and must start with `#!lua name=<libname>` -- the wrapper parses the name from the shebang. Inside the library, declare functions via `redis.register_function(...)`. Function names are global on the Redis server (not namespaced by library), which is why `call(funcName, ...)` keys on function name only.

```js
import { createFunctionLibrary } from 'svelte-adapter-uws-extensions/redis/functions';

const lib = createFunctionLibrary(redis, `#!lua name=ws-presence
redis.register_function('cleanup', function(keys, args)
  -- args[1] = now (ms), args[2] = ttl (ms)
  local now = tonumber(args[1])
  local ttl = tonumber(args[2])
  local removed = 0
  -- ... iterate hash fields, HDEL stale ...
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
| `metrics` | -- | Prometheus metrics registry. |
| `breaker` | -- | Circuit breaker instance. |

#### API

| Method / Property | Description |
|---|---|
| `lib.name` | Library name parsed from the shebang |
| `lib.load()` | `FUNCTION LOAD REPLACE`. Runs `INFO server` on first call and throws on Redis < 7. Idempotent. |
| `lib.call(funcName, { keys?, args? })` | `FCALL` -- returns the function's return value |
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

```bash
npm test
```

Tests use in-memory mocks for Redis and Postgres, no running services needed.

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
| `mockPlatform()` | Platform API | `publish()`, `send()`, `batch()`, `topic()` -- records all calls in `.published` and `.sent` |
| `mockWs(userData?)` | uWS WebSocket | `subscribe()`, `unsubscribe()`, `getUserData()`, `getBufferedAmount()`, `close()` |
| `mockPgClient()` | `createPgClient()` | SQL parsing for replay buffer operations, sequence counters |

The circuit breaker (`createCircuitBreaker()`) is pure logic with no I/O -- use it directly in tests, no mock needed.

---

## Related projects

- [svelte-adapter-uws](https://github.com/lanteanio/svelte-adapter-uws) -- The core adapter this package extends. Single-process WebSocket pub/sub, presence, replay, and more for SvelteKit on uWebSockets.js.
- [svelte-realtime](https://github.com/lanteanio/svelte-realtime) -- Opinionated full-stack starter built on the adapter. Auth, database, real-time CRUD, and deployment config out of the box.
- [svelte-realtime-demo](https://github.com/lanteanio/svelte-realtime-demo) -- Live demo of svelte-realtime. [Try it here.](https://svelte-realtime-demo.lantean.io/)

---

## License

MIT
