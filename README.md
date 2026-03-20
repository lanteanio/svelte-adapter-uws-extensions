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
- [Replay buffer (Redis)](#replay-buffer-redis)
- [Presence](#presence)
- [Rate limiting](#rate-limiting)
- [Broadcast groups](#broadcast-groups)
- [Cursor](#cursor)

**Postgres extensions**
- [Replay buffer (Postgres)](#replay-buffer-postgres)
- [LISTEN/NOTIFY bridge](#listennotify-bridge)

**Observability**
- [Prometheus metrics](#prometheus-metrics)

**Reliability**
- [Circuit breaker](#circuit-breaker)

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

Distributes `platform.publish()` calls across multiple server instances via Redis pub/sub. Each instance publishes locally AND to Redis. Incoming Redis messages are forwarded to the local platform with echo suppression (messages from the same instance are ignored).

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

#### API

| Method | Description |
|---|---|
| `bus.wrap(platform)` | Returns a new Platform whose `publish()` sends to Redis + local |
| `bus.activate(platform)` | Start the Redis subscriber (idempotent) |
| `bus.deactivate()` | Stop the subscriber |

---

## Replay buffer (Redis)

Same API as the core `createReplay` plugin, but backed by Redis sorted sets. Messages survive restarts and are shared across instances.

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
| `size` | `1000` | Max messages per topic |
| `ttl` | `0` | Key expiry in seconds (0 = never) |

#### API

All methods are async (they hit Redis). The API otherwise matches the core plugin exactly:

| Method | Description |
|---|---|
| `publish(platform, topic, event, data)` | Store + broadcast |
| `seq(topic)` | Current sequence number |
| `since(topic, seq)` | Messages after a sequence |
| `replay(ws, topic, sinceSeq, platform)` | Send missed messages to one client |
| `clear()` | Delete all replay data |
| `clearTopic(topic)` | Delete replay data for one topic |

---

## Presence

Same API as the core `createPresence` plugin, but backed by Redis hashes. Presence state is shared across instances with cross-instance join/leave notifications via Redis pub/sub.

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

#### API

| Method | Description |
|---|---|
| `join(ws, topic, platform)` | Add connection to presence |
| `leave(ws, platform, topic?)` | Remove from a specific topic, or all topics if omitted |
| `sync(ws, topic, platform)` | Send list without joining |
| `list(topic)` | Get current users |
| `count(topic)` | Count unique users |
| `clear()` | Reset all presence state |
| `destroy()` | Stop heartbeat and subscriber |
| `hooks` | `{ subscribe, close }` -- ready-made WebSocket hooks. Destructure for one-line `hooks.ws.js` setup. |

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

Same API as the Redis replay buffer, but backed by a Postgres table. Best suited for durable audit trails or history that needs to survive longer than Redis TTLs. Sequence numbers are generated atomically via a dedicated `_seq` table, so they are safe across multiple server instances.

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

#### API

| Method | Description |
|---|---|
| `activate(platform)` | Start listening (idempotent) |
| `deactivate()` | Stop listening and release the connection |

#### Limitations

- Payload is limited to 8KB by Postgres. For large rows, send the row ID in the notification and let the client fetch the full row.
- Only fires from triggers. Changes made outside your app (manual SQL, migrations) are invisible unless you add triggers for those tables too.
- This is not logical replication. It is simpler, works on every Postgres provider, and needs no extensions or superuser access.

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

**Presence**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `presence_joins_total` | counter | `topic` | Join events |
| `presence_leaves_total` | counter | `topic` | Leave events |
| `presence_heartbeats_total` | counter | | Heartbeat refresh cycles |
| `presence_stale_cleaned_total` | counter | | Stale entries removed by cleanup |

**Replay buffer (Redis and Postgres)**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `replay_publishes_total` | counter | `topic` | Messages published |
| `replay_messages_replayed_total` | counter | `topic` | Messages replayed to clients |
| `replay_truncations_total` | counter | `topic` | Truncation events detected |

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

---

**Reliability**

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

---

## Related projects

- [svelte-adapter-uws](https://github.com/lanteanio/svelte-adapter-uws) -- The core adapter this package extends. Single-process WebSocket pub/sub, presence, replay, and more for SvelteKit on uWebSockets.js.
- [svelte-realtime](https://github.com/lanteanio/svelte-realtime) -- Opinionated full-stack starter built on the adapter. Auth, database, real-time CRUD, and deployment config out of the box.
- [svelte-realtime-demo](https://github.com/lanteanio/svelte-realtime-demo) -- Live demo of svelte-realtime. [Try it here.](https://svelte-realtime-demo.lantean.io/)

---

## License

MIT
