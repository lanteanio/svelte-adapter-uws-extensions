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

**Operations**
- [Graceful shutdown](#graceful-shutdown)
- [Testing](#testing)

**Help**
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
| `redis.duplicate()` | New connection with same config (for subscribers) |
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
| `select` | identity | Extract public fields from userData |
| `heartbeat` | `30000` | TTL refresh interval in ms |
| `ttl` | `90` | Hash entry expiry in seconds |

#### API

| Method | Description |
|---|---|
| `join(ws, topic, platform)` | Add connection to presence |
| `leave(ws, platform)` | Remove from all topics |
| `sync(ws, topic, platform)` | Send list without joining |
| `list(topic)` | Get current users |
| `count(topic)` | Count unique users |
| `clear()` | Reset all presence state |
| `destroy()` | Stop heartbeat and subscriber |

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
| `consume(ws, cost?)` | Attempt to consume tokens |
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
| `maxMembers` | `Infinity` | Maximum members allowed |
| `meta` | `{}` | Initial group metadata |
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
| `select` | identity | Extract user data to broadcast alongside position |
| `ttl` | `30` | Redis hash entry TTL in seconds (auto-refreshed on each broadcast) |

#### API

| Method | Description |
|---|---|
| `update(ws, topic, data, platform)` | Broadcast cursor position (throttled per user per topic) |
| `remove(ws, platform)` | Remove from all topics and broadcast removal |
| `list(topic)` | Get current positions across all instances |
| `clear()` | Reset all local and Redis state |
| `destroy()` | Stop the Redis subscriber and clear timers |

---

**Postgres extensions**

## Replay buffer (Postgres)

Same API as the Redis replay buffer, but backed by a Postgres table. Best suited for durable audit trails or history that needs to survive longer than Redis TTLs.

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

## License

MIT
