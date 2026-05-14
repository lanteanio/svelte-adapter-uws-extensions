# Migration guide: svelte-adapter-uws-extensions 0.4.x to 0.5.x

This guide is organized by **tier**. Most apps only need to read the first two sections.

- **[Critical](#critical-read-first)** - security-class behavior changes; audit required.
- **[Required source changes](#required-source-changes)** - won't run cleanly without these.
- **[Notable defaults and behaviors](#notable-defaults-and-behaviors)** - probably fine, but you may notice.
- **[Cosmetic](#cosmetic)** - type-only, internal refactors, niche edge cases.

`svelte-adapter-uws-extensions` 0.5 raises its peerDep on `svelte-adapter-uws` to `^0.5.0`. See the adapter's MIGRATION.md for breaking changes on that side.

If you have a small app and want the 5-minute version, see the [docs site upgrade quickstart](https://svelte-realtime.dev/docs/upgrade-quickstart).

---

## Critical (read first)

These close real security bugs in idiomatic 0.4 code paths.

### Replay backends consult `platform.checkSubscribe` before reading history

**What changed.** Every `replay()` implementation (Redis sorted-set, Redis streams, Postgres) now calls `platform.checkSubscribe(ws, topic)` first. On denial, it sends a single `{event:'denied', data:{code:<denial>, reqId}}` frame on `__replay:{topic}` and returns without reading the buffer. Pre-fix, an attacker could send a crafted `lastSeenSeqs` map and read history for a topic they could not subscribe to live.

**How to migrate.** Client-side replay stores must handle the new `denied` event in addition to existing `truncated` / `end` events. Treat it similarly to `truncated`:

```js
function onReplayFrame(event, data) {
  if (event === 'denied') {
    return;
  }
}
```

Older adapters lacking `checkSubscribe` degrade gracefully to the previous behavior.

### Tasks `idempotencyKey` is now namespaced by task name, with a 256-char cap

**In-flight cache entries become invisible after deploy.**

**What changed.** The cache key passed to `idempotency.acquire(...)` for `tasks.run` and `tasks.enqueue` is now `'task:' + name + ':' + idempotencyKey` (was the bare `idempotencyKey`). Pre-fix, two tasks sharing one client-supplied key shared one cache slot, which let a privileged task's cached result leak to a public task. Both methods also reject `idempotencyKey` longer than 256 characters; the underlying stores cap `acquire(key)` at 1024 chars as defense in depth.

**How to migrate.** No source change at call sites. In-flight cached entries from before the upgrade become invisible after deploy because the namespaced key does not match the old un-namespaced key. For Redis stores the old keys TTL out; for Postgres the cleanup interval handles them. Audit any code that supplies long `idempotencyKey` strings (request bodies, opaque tokens) and shorten them if they exceed 256 chars.

### Inbound bus envelopes are validated before republish

**What changed.** Every bus subscriber (`createPubSubBus`, `createShardedBus`, `createNotifyBridge`, `createCursor`) now gates inbound envelopes on raw byte size, topic shape, event shape, and a `__`-prefix denylist before republishing. Default `maxEnvelopeBytes` is 1 MB; default `allowSystemTopics` is `false` (secure-by-default - the wire-level `__`-subscribe gate alone does not cover bus-republish). The bus's own configured `systemChannel` (default `__realtime`) remains in an explicit allowlist so degraded / recovered events still flow.

**How to migrate.** No code change required for default deployments. If your application publishes envelopes larger than 1 MB on the bus, raise `maxEnvelopeBytes` explicitly:

```js
createPubSubBus(redis, { maxEnvelopeBytes: 4 * 1024 * 1024 });
createShardedBus(redis, { maxEnvelopeBytes: 4 * 1024 * 1024 });
createNotifyBridge(pg, { maxEnvelopeBytes: 4 * 1024 * 1024 });
createCursor(redis, { maxEnvelopeBytes: 4 * 1024 * 1024 });
```

If your app legitimately bus-relays user-defined `__`-prefixed topics (uncommon), opt back in:

```js
createPubSubBus(redis, { allowSystemTopics: true });
```

Topics longer than 256 chars or containing control characters are also rejected; review any code that constructs topic names dynamically.

---

## Required source changes

These won't run cleanly until you make the change.

### Runtime: Node.js 22+ required (was Node 20+)

**What changed.** `package.json#engines.node` moved from `>=20.0.0` to `>=22.0.0`. Tracks the adapter's bump, which in turn tracks `uWebSockets.js` v20.67.0 dropping Node 20 support upstream.

**How to migrate.** See `svelte-adapter-uws/MIGRATION.md` for the full uWS-side rationale. No extensions-specific action beyond bumping your runtime to Node 22+.

### Peer dependency on `svelte-adapter-uws` raised to `^0.5.0`

**What changed.** The peerDep range moved from `>=0.4.0` (0.4.x) to `^0.5.0`. `bus.wrap()` now binds adapter members (`maxPayloadLength`, `bufferedAmount`, `subscribe`, `unsubscribe`, `checkSubscribe`, `sendCoalesced`, `request`, `requestId`, `pressure`, `onPressure`, `onPublishRate`, `publishBatched`) unconditionally; an older adapter without these members crashes at wrap-construction time.

**How to migrate.** Upgrade the adapter first, then this package:

```sh
npm install svelte-adapter-uws@latest svelte-adapter-uws-extensions@latest
```

See the adapter's MIGRATION.md for adapter-side breaking changes.

### Postgres table prefix changed from `ws_` to `svti_`

**What changed.** All Postgres factories (`createReplay`, `createIdempotencyStore`, `createJobQueue`, `createTaskRunner`) now default to `svti_*` table names: `svti_replay`, `svti_replay_seq`, `svti_idempotency`, `svti_jobs`, `svti_tasks`. Primary key columns follow the `[tablename]_id` rule (`svti_replay_id`, `svti_jobs_id`, `svti_tasks_id`, `svti_idempotency_key`). The replay timestamp column was renamed `created_date` to `created_at`. JS-level field names on row objects are preserved via SQL `AS` aliases (`job.id`, `task.id`, `task.idempotency_key` are unchanged).

**How to migrate.** Pick one of:

Option A (zero-downtime, no SQL): override the `table` option on each module to keep the old names.

```js
createReplay(pg, { table: 'ws_replay' });
createIdempotencyStore(pg, { table: 'ws_idempotency' });
createJobQueue(pg, { table: 'ws_jobs' });
createTaskRunner(pg, { table: 'ws_tasks' });
```

Option B (rename in place):

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

### Presence `hooks` now includes `unsubscribe`

**What changed.** `presence.hooks` exports a `unsubscribe` hook in addition to `subscribe` and `close`. Required for correct single-topic leave when a client unsubscribes from a topic without disconnecting.

**How to migrate.** Update destructuring:

```js
// before
export const { subscribe, close } = presence.hooks;

// after
export const { subscribe, unsubscribe, close } = presence.hooks;
```

### Sharded pub/sub bus and Redis Functions wrapper require Redis 7+

**What changed.** `createShardedBus` (uses SPUBLISH/SSUBSCRIBE) and `createFunctionLibrary` (uses Redis Functions) run `INFO server` on activate and throw on Redis < 7. No EVALSHA or PUBLISH fallback is provided.

**How to migrate.** On Redis 6 or older Valkey, use `createPubSubBus` instead of `createShardedBus` and use `redis.eval` directly instead of `createFunctionLibrary`. Upgrade Redis to 7+ to use the new modules.

### Pub/sub bus `systemChannel` delivery requires `bus.hooks.open`

**What changed.** `svelte-adapter-uws@0.5.0-next.21` closed the wire-level path that put connections into `__`-prefixed subscriber sets. Pre-next.21, the `svelte-realtime` client store wire-subscribed to `__realtime` and the server-side subscriber set was populated as a side effect; the pub/sub bus's auto-emitted `degraded` / `recovered` events reached every connected client through that membership. After the wire-gate change, nothing populated the subscriber set on the server, so the bus published into an empty set and clients never saw the events - any `{#if $health === 'degraded'}` banner was silently dead. `bus.hooks.open` (new in this release) puts every connection into `systemChannel`'s subscriber set via the platform-trust path (`platform.subscribe`), which intentionally bypasses the wire-level gate.

**How to migrate.** If you use `createPubSubBus` with a `breaker` and follow the README's degradation-banner pattern, swap your open hook from:

```js
// before
export function open(ws, { platform }) {
  bus.activate(platform);
}
```

to:

```js
// after
export const { open } = bus.hooks;
```

or, if your open hook does additional work:

```js
export async function open(ws, ctx) {
  await bus.hooks.open(ws, ctx);
  // ... other open-hook work
}
```

`bus.activate(platform)` still works for the subscriber-half; the difference is that `bus.hooks.open` ALSO subscribes the connection to `systemChannel`. If you set `systemChannel: null` or `false`, no migration is required - `bus.hooks.open` still activates the subscriber and skips the per-WS subscribe. Bump the `svelte-adapter-uws` peer minimum to `>=0.5.0-next.23` to also clear the cosmetic `[ws] subscribe denied topic=__realtime` console warn on every page mount; the `bus.hooks.open` fix itself works against any 0.5 adapter.

### Cursor delivery requires `tracker.attach(ws, topic, platform)`

**What changed.** Same regression class as the pubsub `systemChannel` issue above. `svelte-adapter-uws@0.5.0-next.21` closed the wire-level path that put connections into `__`-prefixed subscriber sets; pre-next.21, the client store's wire subscribe to `__cursor:{topic}` populated the subscriber set as a side effect. After next.21, the wire frame is denied; after next.23, the client does not send it. The cursor extension publishes to `__cursor:{topic}` from `update` / the bulk-flush tick / `remove`, but nothing else subscribed connections to that channel server-side, so every cursor frame fanned out into an empty subscriber set and cross-tab / cross-user cursors were silently dropped. `tracker.attach(ws, topic, platform)` (new in this release) owns membership via the platform-trust path (`platform.subscribe`), which intentionally bypasses the wire-level gate, and folds the snapshot send into the same call.

**How to migrate.** Call `cursors.attach(ws, topic, platform)` from your "join room" RPC (the same place you call `presence.join`), and `cursors.detach` from your "leave room" RPC if the user stays connected:

```js
// before - publishes went into an empty subscriber set on adapter next.21+
export async function joinBoard(ws, { topic, platform }) {
  await presence.join(ws, topic, platform);
}

// after
export async function joinBoard(ws, { topic, platform }) {
  await presence.join(ws, topic, platform);
  await cursors.attach(ws, topic, platform);
}

export function leaveBoard(ws, { topic, platform }) {
  presence.leave(ws, topic, platform);
  cursors.detach(ws, topic, platform);
}
```

No `close`-hook change required: uWS releases all per-`ws` subscriptions on disconnect, so `cursors.remove(ws, platform)` in your `close` hook still handles cleanup. The legacy `cursors.hooks.subscribe` slot is now a no-op (the adapter's wire gate prevents it from ever firing); it stays exported for source-compat but new code should not rely on it.

### Presence wire shape on `__presence:{topic}` migrated to `presence_state` / `presence_diff`

**Only impacts apps with hand-rolled WebSocket clients consuming the wire directly.**

**What changed.** The `redis/presence` channel previously emitted `list` / `join` / `leave` / `updated` / `heartbeat` events. It now emits:

- `presence_state` (sent once on subscribe to a single connection): payload is `{[userKey]: data}` - a flat snapshot.
- `presence_diff` (broadcast to topic subscribers, microtask-batched): payload is `{joins: {[key]: data}, leaves: {[key]: data}}`. Same-tick joins+leaves on the same key collapse to the latest op; updates appear as a `joins` entry with the new data.
- `heartbeat` is unchanged.

The public JS API on `createPresence` is unchanged (`join`, `leave`, `sync`, `list`, `count`, `metrics`, `clear`, `destroy`, `hooks` keep their signatures). The cross-instance Redis pub/sub envelope on `presence:events:{topic}` is also unchanged. The `keyspaceNotifications: true` mode now emits an empty `presence_state` event (was an empty `list` event) on hash expiry.

**How to migrate.** Apps with a custom WebSocket client decoding presence frames must swap their decoder from the four legacy event names to `presence_state` / `presence_diff`:

```js
// before
case 'list':    setAll(payload); break;
case 'join':    add(payload.key, payload.data); break;
case 'leave':   remove(payload.key); break;
case 'updated': set(payload.key, payload.data); break;

// after
case 'presence_state':
  state.clear();
  for (const k of Object.keys(payload)) state.set(k, payload[k]);
  break;
case 'presence_diff':
  for (const k of Object.keys(payload.joins)) state.set(k, payload.joins[k]);
  for (const k of Object.keys(payload.leaves)) state.delete(k);
  break;
```

Apps using `svelte-realtime` get the new shape automatically. Apps using only the adapter's bundled in-memory `createPresence` plugin are unaffected; that plugin already used the new shape.

---

## Notable defaults and behaviors

These change observable runtime behavior. Most apps are unaffected; a few will notice.

### Postgres factories reject reserved-namespace table names

**What changed.** The `idempotency`, `jobs`, `replay`, and `tasks` factories now reject `table` values whose lowercase form starts with `pg_` or `information_schema` with an explicit "reserved Postgres schema" error. Pre-fix, these names passed the identifier-shape regex and could clash with Postgres internals.

**How to migrate.** Rename any custom `table` value that matches `pg_*` or `information_schema*` (extremely uncommon in practice). Standard prefixes are unaffected.

### Redis `ConnectionError` no longer leaks the URL password

**What changed.** When `new Redis(url, ...)` throws (DNS failure, malformed URL, bad TLS), the resulting `ConnectionError` message previously included the raw connection string with embedded password. The redis client factory now redacts the password segment to `***` via the new `shared/sensitive.js#redactConnectionUrl(url)` helper before interpolating into the error.

**How to migrate.** No source change. Error-tracker hooks and log pipelines that searched for the raw connection string must update their queries; the redacted form is `redis://:***@host:port`. Other URL forms (no userinfo, no password, query string with `@`, path-segment colons) pass through untouched.

### `replay()` end event data changed from `null` to `{ reqId }`

**What changed.** The end marker on `__replay:{topic}` now sends `{ reqId: undefined }` (or `{ reqId: 'some-id' }` when a correlation ID is passed) instead of `null`. The signature also accepts an optional `reqId` parameter: `replay(ws, topic, sinceSeq, platform, reqId?)`. A new `truncated` event is sent before replay messages when the buffer has been trimmed past the client's `sinceSeq`. (This change shipped in 0.4.0 but is repeated here because client-side decoders are commonly out of date.)

**How to migrate.** Update any client-side end-marker check from `data === null` to an object check. Handle or ignore the new `truncated` event in your replay store.

### `replay.publish()` storage failures throw `ReplayStorageError`

**What changed.** Storage failures in `publish()` (and `publishIdempotent` on the stream backend) now throw `ReplayStorageError` with the original error preserved on `.cause`. Pre-fix the underlying error propagated raw. Affects `redis/replay`, `redis/replay-stream`, and `postgres/replay`.

**How to migrate.** Any consumer that catches a specific underlying class (ioredis errors, `CircuitBrokenError`) must either catch `ReplayStorageError` and inspect `.cause`:

```js
try {
  await replay.publish(topic, event, data);
} catch (err) {
  if (err instanceof ReplayStorageError) {
    const root = err.cause;
  }
  throw err;
}
```

Or set `localFanoutOnStorageFailure: true` on the factory to opt into a best-effort `platform.publish` fallback (pure live frames; durability is sacrificed):

```js
createReplay(redis, { localFanoutOnStorageFailure: true });
```

`publishIdempotent` always throws `ReplayStorageError` on storage failure even when this option is set, since silent fanout would break the exactly-once contract.

### `createCursor` defaults flipped to a 60Hz world-state tick

**What changed.** `throttle` defaults to `16` (was `50`) and `topicThrottle` defaults to `16` (was `0`). Out of the box the broadcast path is now a 60Hz world-state tick: each topic emits at most one bulk frame per 16ms window carrying the latest position for every cursor that moved.

**How to migrate.** No action for apps that want the new default. To restore previous behavior:

```js
// previous "every update broadcasts immediately" behavior
createCursor(redis, { topicThrottle: 0 });

// previous 50ms per-cursor floor
createCursor(redis, { throttle: 50 });
```

For high-density rooms (>200 active movers) raise `topicThrottle` to 33 (30Hz).

### Pub/sub bus auto-emits `degraded` / `recovered` system events

**What changed.** When `createPubSubBus` shares a circuit breaker with the rest of the extensions, the bus subscribes to the breaker and auto-emits `degraded` / `recovered` events on a configurable system topic (default `'__realtime'`). Replaces the manually wired `breaker.onStateChange` to `distributed.publish('__system', ...)` pattern that the README previously documented.

**How to migrate.** Remove any manual wiring of breaker state to a `__system` topic if you previously followed the README pattern. To disable auto-emission, set `systemChannel: null` or `false`. To use a different topic, set `systemChannel: 'my-status-topic'`. The `onDegraded` / `onRecovered` callbacks remain available for server-side reactions regardless. **Delivery to clients also requires wiring `bus.hooks.open` in your `hooks.ws.js`** - see [Pub/sub bus `systemChannel` delivery requires `bus.hooks.open`](#pubsub-bus-systemchannel-delivery-requires-bushooksopen) under Required source changes.

### Default `select` strips `__`-prefixed and sensitive keys on presence and cursor

**What changed.** The default `select` on `createPresence` and `createCursor` was the identity function in earlier 0.3.x. Since 0.4.0 it strips keys starting with `__` (e.g. `__subscriptions`, `remoteAddress`) and keys matching `/token|secret|password|auth|session|cookie|jwt|credential/i`. Repeated here because it remains the most common upgrade trip-hazard. Presence and cursor also warn once (per-process) if userData contains keys matching the sensitive-key pattern even when not selected. Notify bridge warns when payload approaches Postgres ~8000 byte NOTIFY limit.

**How to migrate.** If you relied on any of these fields in presence or cursor data, pass an explicit `select` function:

```js
createPresence(redis, { select: (ud) => ({ name: ud.name, role: ud.role, token: ud.token }) });
```

Treat the warnings as actionable: either rename the offending field, exclude it from upstream user data, or pass a custom `select` that intentionally retains it. Warnings do not throw or alter behavior.

### New parse-error counters

**What changed.** The Redis pub/sub bus and sharded pub/sub bus previously swallowed malformed envelopes silently on receive. They now bump a `pubsub_parse_errors_total` / `sharded_pubsub_parse_errors_total` counter so a stream of bad messages becomes observable in Prometheus. Not a code break per se, but alert thresholds that assumed zero parse errors must be updated.

**How to migrate.** Add the new counters to your dashboards next to the existing `notify_parse_errors_total` on `createNotifyBridge`. No source change.

---

## Cosmetic

Type-only changes, internal refactors, niche edge cases. No action required for most apps.

### Lua scripts reject non-numeric ARGV with `redis.error_reply`

**What changed.** Defensive `tonumber(x) ~= nil` checks were added at the top of every Lua script (`shared/scripts.js` CLEANUP/COUNT/COUNT_DEDUP/LIST, `redis/groups.js` JOIN, `redis/presence.js` JOIN/LEAVE, `redis/ratelimit.js` CONSUME/BAN, `redis/replay.js` PUBLISH, `redis/replay-stream.js` IDMP_PUBLISH/PUBLISH). Pre-fix, bypassing the JS validation layer (direct `redis.eval`, poisoned hash entries) would crash the script.

**How to migrate.** No source change for code that uses the public JS APIs. Code that calls `redis.eval` against extension scripts directly must pass numeric ARGV strings (`'1700000000'`, not `'now'`). Poisoned hash entries with non-numeric `ts` fields are now treated as stale rather than crashing iteration.

### `idempotency.acquire(key)` parameter renamed to `idempotencyKey`

**What changed.** The first argument of `idempotency.acquire(...)` and `idempotency.purge(...)` is now named `idempotencyKey` instead of `key`, matching the existing `idempotencyKey` option on the task runner.

**How to migrate.** Function-argument rename, transparent at call sites that pass positionally. Code that destructures the parameter name from a wrapper signature should update accordingly.

### `redis/lock` Lua scripts moved to `shared/lease-scripts.js`

**What changed.** The lock module's `HEARTBEAT_SCRIPT` and `RELEASE_SCRIPT` are now imported from `shared/lease-scripts.js` under generic names (`LEASE_RENEW_SCRIPT`, `LEASE_RELEASE_SCRIPT`) so `redis/leader` can reuse them. Pure refactor, no behavior change.

**How to migrate.** No action required for callers using the public lock API. Internal-tooling code that imported the script constants by name from `redis/lock` must update its import path and rename:

```js
// before
import { HEARTBEAT_SCRIPT, RELEASE_SCRIPT } from 'svelte-adapter-uws-extensions/redis/lock';

// after
import { LEASE_RENEW_SCRIPT, LEASE_RELEASE_SCRIPT } from 'svelte-adapter-uws-extensions/shared/lease-scripts';
```

### `JOIN_SCRIPT` in groups returns `[status, ...liveMembers]`

**What changed.** Internal change since 0.4.0, but affects anyone calling the Lua script directly. The wire contract on the public groups API is unchanged.

**How to migrate.** If you do not call `JOIN_SCRIPT` directly via `redis.eval`, no action. Direct callers must read the array form.

---

## After upgrading

Run your test suite. Pay particular attention to:

- Custom WebSocket clients decoding `__presence:{topic}` and `__replay:{topic}` frames.
- Code that catches storage errors on `replay.publish()`.
- Postgres deployments that relied on `ws_*` table names.
- Redis deployments with versioned rate limiter keys (no action needed; old `ratelimit:*` keys expire on their own).
- Bus subscribers receiving envelopes larger than 1 MB.
- Tasks paths that pass long `idempotencyKey` strings (now capped at 256 chars).

Report regressions against the changelog entry the issue maps to.
