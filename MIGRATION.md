# Migration guide: svelte-adapter-uws-extensions

[Package overview](./README.md) | [Release history](./CHANGELOG.md)

- **[0.5.x to 0.6.x](#05x-to-06x)** - current prerelease migration.
- **[0.4.x to 0.5.x](#04x-to-05x)** - previous release.

---

# 0.5.x to 0.6.x

Ordered by **when the change reaches you**, because that is what decides how
much of it you can discover in staging. Some changes fail at install, import or
construction, so a misconfigured deployment stops at boot rather than at the
first request. PostgreSQL stores need schema and data preflight before deployment;
lease and identity changes also require the drain windows described below.
Other changes surface on calls that used to succeed.

Every item lists the **symptom** you will actually see and the **action**.

## Prerequisites and upgrade order

Use Node.js 22 or later, the adapter's matching native addon, and the exact
candidate tuple in the [README compatibility table](./README.md#version-compatibility).
The adapter's manifest is the tuple authority; a peer range alone is not an
integration-tested tuple. PostgreSQL 16 is the tested database baseline. Use the
Redis/Valkey backend tier you actually deploy when rehearsing the upgrade.

Save the current lockfile, application configuration, database backup and Redis
persistence snapshot before changing versions. Stop application producers and
drain active task/job handlers before the idempotency transition below. Upgrade
the adapter first, extensions second, and svelte-realtime last, following each
package's migration guide. Pin the selected artifacts in one lockfile and test
that application build; do not combine independently chosen prereleases.

The local [packed schema rehearsal](#executable-rehearsal) covers this package's
additive PostgreSQL transition. It does not certify application authorization,
Redis data conversion, native addon loading, or a mixed-version fleet.

### PostgreSQL schema and migration preflight

Automatic initialization now uses a versioned migration ledger named
`svti_schema_migrations` in the store's schema and validates the complete
catalog. Back up the ledger with its stores. Use a direct connection or a
session-pooled DSN for both explicit `migrate()` and the default
`autoMigrate: true`: session advisory locks and timeout settings cannot be
preserved by transaction pooling. Custom clients must provide `connect()`
returning a dedicated checkout with `query()` and `release(true)`.

Before upgrading existing tables, reconcile duplicate replay `(topic, seq)`
identities, unsupported task/idempotency statuses, negative attempts and
inconsistent job claim timestamps. Migration rejects these rows without
removing them. It also rejects extra CHECK/UNIQUE/foreign-key/exclusion
restrictions, unexpected required or computed columns and changed defaults.
Review operator-added constraints before rollout; nullable columns without
defaults and ordinary nonunique indexes remain permitted.

For large tables, run `migrate()` before traffic using an owner connection and
explicit `statementTimeoutMs` / `lockTimeoutMs` suited to the rehearsed DDL.
Automatic migration uses 30-second statement and 5-second lock limits; changing
pool timeouts does not change these migration limits. Start runtime stores with
`autoMigrate: false` and await every `ready()` gate. This also separates DDL
privileges from runtime access and permits transaction pooling for ordinary
queries, subject to the client's connection settings. Notification bridges still
need their own persistent session.

A failed automatic migration is latched for ordinary store operations. After
repairing the cause, explicitly retry `ready()` or recreate the store; requests
and retention ticks do not repeatedly rebuild an invalid index. Successful
readiness remains cached. See [PostgreSQL support](docs/postgres-support.md#schema-transitions-and-readiness)
for the catalog contract, migration limits and role setup.

## 1. Fails at startup

These fail at install, at import, or from a factory, so they fail on the
deploy rather than in production traffic. Fix them before you ship.

### The adapter peer floor is `svelte-adapter-uws >= 0.6.0-next.95`

**Symptom.** `npm ERR! ERESOLVE unable to resolve dependency tree` (or a peer
warning under `--legacy-peer-deps`) naming `svelte-adapter-uws`, at install
time. On an older adapter that slipped past the peer check, `import` of
`svelte-adapter-uws-extensions/admission` fails with
`ERR_PACKAGE_PATH_NOT_EXPORTED` for `svelte-adapter-uws/observability`.

**Why.** Several releases raised the floor. `0.6.0-next.87` implements the
observer-lane mode `{ requireGrant: true }` that the presence and cursor
snapshot lanes pass to `checkSubscribe`; an older adapter silently ignores
the option and re-opens the pure-grant roster exposure. `0.6.0-next.91` ships
`svelte-adapter-uws/observability`, and admission control reads the adapter's
pressure-reason table from that entry point. `0.6.0-next.95` supplies the
sequence authority contract for explicit `null` and `bigint` choices, keeping
local and relayed publications consistent.

**Action.** Bump `svelte-adapter-uws` to `0.6.0-next.95` or later in the same
upgrade. The ecosystem packages move together (see the version table in the
README).

### `capabilityCookie` refuses a weak secret

**Symptom.** `capability-cookie: secret must be at least 16 characters` or
`... has too few distinct characters to be generated key material; it looks
like a placeholder`, thrown from `capabilityCookie(...)` at module load.

**Why.** The cookie's entire security property is that a client cannot forge the
signature. An observer holding both the message and the tag can brute-force a
short key offline at memory speed, and only non-emptiness was checked before -
so `'dev'` was accepted.

**Action.** Generate a real secret and put it in the environment:

```
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

The same strength checks apply to `previousSecret`: accepting a forged old-key
cookie would bypass the new signing key. Rotate between strong keys using
`previousSecret`; when replacing a weak key, omit it and require clients to
obtain a new cookie. Both keys need at least 16 characters, at least eight
distinct characters, and must not be recognized placeholders.

### `capabilityCookie` refuses a malformed `cookieName` / `path` / `sameSite`

**Symptom.** `capability-cookie: cookieName must be an RFC 6265 token`, or
`capability-cookie: path must start with "/" and contain only printable ASCII
except whitespace, ";", "," and '"'`, thrown at construction.

**Why.** All three are serialized verbatim into the `Set-Cookie` header. A name
like `my cookie` produced a header the browser silently dropped, so the
capability check degraded to permanently-absent with nothing logged anywhere. A
`path` carrying NUL or a non-ASCII byte is worse: the header layer throws
`invalid header value`, turning every issued cookie into a per-request 500.

**Action.** Use an RFC 6265 token for `cookieName` (letters, digits and
``!#$%&'*+-.^_`|~``), and percent-encode anything in `path` outside printable
ASCII. `sameSite` is now matched case-insensitively, so `'lax'` and `'none'`
keep working.

### Every bus module refuses an invalid `maxEnvelopeBytes`

**Symptom.** `<module>: maxEnvelopeBytes must be a positive integer (bytes), got
<value>`, thrown from `createConnectionRegistry` / `createGroup` /
`createCursor` / `createCrdtCluster` / `createPubSubBus` / `createShardedBus` /
`createSmoothCluster` / `createTopicBroadcast` / `createNotifyBridge`.

**Why.** `0`, `-1`, `NaN` and `'65536'` all fell through to the 1MB default, so
a deployment that believed it had set a 64KB bound ran with 1MB and nothing
reported the difference. `NaN` is the sharp one: it *is* a number, and every
`bytes > NaN` comparison is false, which disables the cap it configures.

**Action.** Pass an integer, or drop the option to take the default. If you are
reading the value from an environment variable, `Number(process.env.X)` yields
`NaN` for an unset one - use a fallback.

### `redis/ratelimit` no longer exports its in-process floor helpers

**Symptom.** `SyntaxError: The requested module
'svelte-adapter-uws-extensions/redis/ratelimit' does not provide an export
named 'createWindowFloor'` (or `createSlidingFloor`, `createGcraFloor`,
`createLocalFloorBucket`) when the importing module loads.

**Why.** These were implementation helpers of the rate limiter's storage-outage
floor. They were never declared in the type definitions or documented, and the
type gate now fails any runtime export its declaration omits.

**Action.** Configure the floor through the limiter instead:
`createRateLimit(client, { ..., localFloorOnStorageFailure: true })` (or
`{ points, interval }` for a tighter per-instance budget).

## 2. Needs an operational step

### Ordered alarm generations

Alarm stores now require `nextGeneration(topic)`, generation-aware
`set(topic, at, meta, generation)` and `delete(topic, generation)`, plus
`get(topic)`. Stop old topic-only writers before deploying the matching realtime
runtime. PostgreSQL migration version 3 retains allocator and commit watermarks
in the companion `_generations` table and assigns generation `1` to legacy rows.
Redis retains authority in three hashes alongside schedule and metadata keys.
Restore authority together with alarms and the PostgreSQL migration ledger,
including counters for deleted or pending topics. See [the generation contract
and rollout guide](docs/alarm-generations.md).

### Jobs and tasks tables gain trace columns; tasks gain erasure identity

Only for tables created or migrated outside the package, with
`autoMigrate: false`. With the default `autoMigrate: true` the columns are
added on first use.

**Symptom.** `column "traceparent" does not exist` from `jobs.claim`, task
`list`, and the task runner's dispatch and recovery sweeps, which report it as
`postgres.tasks.sweep.failed` once per failure streak; from `jobs.enqueue`
and task `run` / `enqueue` too whenever the call carries a trace context.

**Why.** Jobs and tasks persist the W3C trace context of the call that
created them, in two nullable columns. Tasks also persist `user_id` for erasure;
without it, task inserts fail even when no trace context is supplied.

**Action.** Run `migrate(client, { store: 'jobs' })` and
`migrate(client, { store: 'tasks' })` with the deployment role before disabling
automatic migration. Pass `table` for custom names. This applies the complete
versioned schema, including constraints and indexes; see
[schema transitions and readiness](docs/postgres-support.md#schema-transitions-and-readiness).
The historical nullable-column additions are:

```sql
ALTER TABLE svti_jobs  ADD COLUMN IF NOT EXISTS traceparent TEXT;
ALTER TABLE svti_jobs  ADD COLUMN IF NOT EXISTS tracestate  TEXT;
ALTER TABLE svti_tasks ADD COLUMN IF NOT EXISTS traceparent TEXT;
ALTER TABLE svti_tasks ADD COLUMN IF NOT EXISTS tracestate  TEXT;
ALTER TABLE svti_tasks ADD COLUMN IF NOT EXISTS user_id     TEXT;
CREATE INDEX IF NOT EXISTS idx_svti_tasks_user ON svti_tasks (user_id);
```

These nullable additions preserve existing rows and can be applied repeatedly.
Old column-only readers continue to work; this is not a whole-fleet compatibility
guarantee. Existing task rows have null `user_id`: backfill from trusted
application identity if they must participate in user erasure. The package
cannot infer that identity from arbitrary historical input.

### Redis idempotency key derivation changed

**Symptom.** None at boot. After the upgrade, idempotency entries written by
0.5 instances are not found by 0.6 instances, so a request that was already
committed executes a second time.

**Why.** `acquire` filtered a non-string tenant while `purgeUser` coerced it, so
a numeric tenant indexed under one key and erased under another and the erasure
silently missed. Both sides now coerce identically, and NUL is rejected in
either segment (the composite key is NUL-delimited, so `('a\0b','c')` and
`('a','b\0c')` collided and one identity's purge deleted another's results).

**Action.** Pick one:

- **Drain.** Stop all producers of idempotent work, finish active handlers, and
  verify that both pending entries and retained committed results have expired
  under their actual TTLs before deploying. Waiting only for handler completion
  is insufficient. Use this path when duplicate execution is unacceptable.
- **Accept one re-execution.** Entries written before the upgrade fall back to a
  cache miss and run once more. Fine when your handlers are themselves
  idempotent against the downstream.

Do **not** deploy 0.6 alongside 0.5 instances sharing one Redis for longer than
your idempotency TTL: during that window the two derive different keys for the
same identity.

### Other PostgreSQL stores and readiness

Replay adds a user identity and sequence epoch; idempotency adds erasure identity
and owner tokens. Alarms and dead letters add PostgreSQL stores. Review the
[per-store migration matrix](./docs/postgres-support.md#schema-transitions-and-readiness)
and deployment migrations before disabling automatic migration. Every store's
`ready()` with `autoMigrate: false` verifies its schema and runtime privileges
read-only; a present migration ledger also requires SELECT access and known
versions/checksums. Await this gate before serving traffic.

## 3. Fails on a call that used to succeed

### Redis idempotency reserves its erasure-index namespace

Redis `acquire()` and `purge()` now reject caller keys beginning with the exact
prefix `byuser:`. Normal cache/index layouts remain unchanged. Stop callers using
that reserved prefix before upgrading, and drain pending work before changing
application keys: a renamed key can repeat an effect. Previously overwritten or
deleted reverse indices require paused-writer inspection and reconciliation;
the upgrade never automatically deletes retained data or repairs historical
erasure mappings. See [key rules and existing data](./docs/idempotency-keys.md).

### Redis sessions reserve their erasure-index namespace

All token operations reject tokens beginning with the exact prefix `byuser:`
before Redis access. Lifecycle cookie loads apply `onLoadError` to these invalid
tokens; the default rejects the upgrade. Server-minted tokens are unaffected.
Stop issuing reserved tokens before upgrading. A legacy reverse index containing
a reserved token now makes `purgeUser` reject before deleting any sessions. With
writers paused, reconcile those tokens and affected index hashes against trusted
application records; the upgrade does not reconstruct missing ownership mappings
or automatically delete colliding data. See [session token rules](./docs/session-tokens.md).

### Redis clients bound waits and disconnected buffering

Owned clients default to 5-second command, connection and socket timeouts,
one retry per request, and at most 4096 offline queued commands per connection.
Once the main connection first becomes ready, offline queuing is disabled unless
explicitly enabled in driver options; subscriber duplicates keep bounded buffering. Review long-running commands and outage behavior before rollout; see
[Redis outage behavior](./docs/redis-outages.md) for overrides and ambiguous
write outcomes. A timeout after a write was sent does not prove it was rejected.

### Worker admission and fence confidence are bounded

Worker pools reserve jobs and input bytes before durable claims. Saturated foreground worker
calls reject before inserting a task; enqueue work that should wait durably.
Queued workers expire after 30 seconds. Handlers must honor their abort signal:
a lost-fence handler cannot commit a returned success even if it ignores abort.
Review heartbeat intervals against backend latency and lease lifetime; see
[worker admission and fence renewal](./docs/task-workers.md) for exact limits.

### PostgreSQL notification startup is awaited

Await bridge activation in application initialization. An owned PostgreSQL client
now stops its bridges and standalone sessions before closing its pool; await
shutdown too. LISTEN and advisory ownership need a dedicated session-compatible
connection, not transaction pooling. Budget these sessions separately from the
query pool; see [PostgreSQL support](./docs/postgres-support.md).

### Owned PostgreSQL pools bound admission and waits

**Symptom.** A burst beyond 100 unsettled wrapper queries/checkouts rejects
with `ConnectionError.code === 'PG_CAPACITY_EXCEEDED'`. A checkout wait can
expire after 10 seconds, and a query read or server statement after 30 seconds.

**Why.** Driver pool size limits connections, but its queue and wait duration
were unlimited by default. Owned pools now bound both admission and native
connection/query/statement waits.

**Action.** Size `maxInFlight` for deliberate fan-out and override
`options.connectionTimeoutMillis`, `options.query_timeout` and
`options.statement_timeout` for long-running operations. Each accepts `0`
to opt out. Supplied pools retain their settings and default to unbounded
wrapper admission; configure the original pool and opt into `maxInFlight`
explicitly. See [pool operation bounds](./docs/postgres-support.md#pool-operation-bounds)
for cancellation, capacity accounting, metrics and direct-pool bypasses.

### Scope arguments that cannot be scoped are refused

**Symptom.** `redis ratelimit: clear tenant id must not contain NUL or glob
metacharacters (* ? [ ] \)`, the same from `compositeRateLimit.clear`, `redis
presence: purgeUser user id must not contain NUL or glob metacharacters`, or
`forget-store: tenant id must be ...`.

**Why.** Each of these interpolates its argument into a `SCAN MATCH` glob.
`clear('*')` and `clear('')` wiped **every** tenant's buckets through a
nominally tenant-scoped call.

**Action.** Call `clear()` with no argument for the deliberate global sweep.
`createForgetStore` tenant ids are `null` or up to 64 characters of
`[a-zA-Z0-9_.:-]` - domains and namespaced ids are fine, `/` is not, and a
non-string throws rather than being coerced.

### Postgres boundary inputs are bounded

**Symptom.** `postgres tasks: input is N bytes, past the 262144-byte payload
cap`, `postgres replay: data exceeds maxDataBytes`, `postgres jobs: batchSize
must be at most 1000`, or a job id rejected as not a digit string.

**Why.** `jobs.claim` accepted any batch size while every way to finish the
batch refused above 1000, so an oversized claim could only expire and be
redelivered - re-running each job's side effects every cycle.

**Action.** The task cap is now configurable: pass `maxPayloadBytes` to
`createTaskRunner`. Read the note on it in the README first - an oversized
**result** is terminal and not retried, and by then your handler has already run
and its side effects have landed. A task that returns a report or an export sits
under the 256KB default on an ordinary run and passes it on a large one, so
either raise the bound to cover the largest result the task can produce, or
return a reference (an object-store key, a row id) and keep the bytes out of the
row. The minimum is 80 bytes, enough to retain a valid terminal error shape.
`bigserial` ids beyond 2^53 - which pg returns as strings - are accepted.

### Registry and groups throw on an oversized outbound envelope

**Symptom.** `registry: outbound "send" envelope exceeds maxEnvelopeBytes` or
`groups: "<event>" envelope exceeds maxEnvelopeBytes`, from a call that
previously resolved.

**Why.** Publishing past the bound resolved as though delivered while every peer
dropped the frame on receipt - a silent split-brain only the sender could
detect.

**Action.** `await` these calls, or attach a `.catch`. An unhandled rejection
terminates the worker under Node's default, and the registry's own examples were
previously written unawaited. **Presence is deliberately the exception**: it
warns and drops rather than throwing, because its envelopes originate on the
connection-lifecycle path where a throw surfaces as a failed connection. That
does mean an oversized presence envelope is reported only in the log.

### `cursor.attach()` throws on a refused topic

**Symptom.** `SubscribeDeniedError` with `err.code === 'SUBSCRIBE_DENIED'`.

**Why.** It previously subscribed the socket and only then discovered the
denial, leaving a refused client subscribed to the room's cursor channel and
able to write to it.

**Action.** Catch `err.code === 'SUBSCRIBE_DENIED'` in the join-room RPC that
calls `attach`.

### `devAssert` throws outside production

**Symptom.** `Error: extensions devAssert: <message>` from a `devAssert` call
imported from `svelte-adapter-uws-extensions/assert`, in development or a test
run, right after the `severity=error` line it already printed.

**Why.** The adapter's `devAssert` throws in every mode but production, so
the same check behaved differently depending on which package it was imported
from.

**Action.** Fix the violated condition, or catch the error where a violation is
expected; `err.context` carries the context passed to the call, with internal
keys stripped. Production (`NODE_ENV=production`) is unaffected: `devAssert`
stays a no-op there.

### Notification startup and dedicated session shutdown

**Symptom.** Awaited advisory-mode notification activation now rejects a real
connection or LISTEN failure; a healthy follower still resolves. Owned
PostgreSQL factory shutdown also closes its standalone clients and notification
bridges, so those sessions no longer survive the pool that created them.

**Action.** Await notification activation in the adapter's init hook and handle
startup failure before serving traffic. Keep standalone sessions under the
owning factory's lifetime. With an external pool, explicitly await bridge
deactivation and standalone client closure before closing that pool; wrapper
end remains a no-op. Use direct or session-pooled PostgreSQL connections for
LISTEN/advisory locks. See [session budgets and shutdown](docs/postgres-support.md#notification-sessions-and-shutdown).

## 4. Silent behaviour changes to audit

### Metric identifiers collapse to bounded classes

**Symptom.** Dashboards and alerts selecting a raw topic, group, tenant or lock
key stop matching new series. `createMetrics().mapTopic` and the lock/leader
`mapKey` defaults now return `all` instead of the identifier. Built-in topic,
group and rate-limit `tenant_id` labels use that mapping, including a custom
registry without `mapTopic`.

**Action.** Update selectors to the aggregate label, or supply an explicit
mapper that returns a fixed set of non-identifying classes with a safe fallback
for every other value. Lock and leader use their own `mapKey` option. An explicit
identity mapper restores raw labels, but also restores disclosure and unbounded
cardinality; use it only as a deliberate operator decision.

`presence_total_online` sums per-topic online user counts across mapped topics
and attached trackers, so the same user on two topics counts twice.
`ws_topic_publish_rate` and `ws_topic_publish_bytes` sum sampled topics within
each mapped class. Update dashboards that assumed a series represented one
raw topic. Purging payloads does not erase previously scraped series; apply
your monitoring backend's retention or deletion policy to historical labels.
See [cardinality control](./README.md#cardinality-control) for a bounded mapper.

### Malformed relay envelopes are parse errors, not assertion violations

An inbound pub/sub or sharded envelope without a string `instanceId` printed an
`invariant.violated` line per envelope under `pubsub.envelope.shape` /
`sharded-bus.envelope.shape` and counted in
`extensions_assertion_violations_total`. It is now dropped without a line and
counted in `pubsub_parse_errors_total` / `sharded_pubsub_parse_errors_total`,
like every other malformed envelope. Move an alert on those two assertion
categories to the parse-error counters.

### A command Redis refuses inside a batch now reaches the breaker

The pub/sub and sharded relay flushes, cursor snapshot writes and removals
(every cursor close, and cursor `purgeUser`), the connection-registry
heartbeat and presence `purgeUser` report a batch with a refused command
(READONLY after a failover, OOM, NOPERM) to the breaker as a failure, where they
counted it as a success. During a failover window a breaker shared with other
modules can therefore open, and their awaited calls throw `CircuitBrokenError`
until it probes healthy again. The relayed-message counters now count only what
went out. Size `failureThreshold` for this, or give the busiest modules (the
relay buses, cursor) their own breaker.

### Admission rules recognize all seven adapter pressure reasons

**Symptom.** An admission class intended to shed under any pressure keeps
accepting while the adapter reports `PSI`, `CPU_QUOTA`, or `CAPACITY`.

**Why.** Those host and protection signals joined the adapter pressure enum
after the original four-value admission validator was written. The extensions
runtime and type now derive from the exported frozen `PRESSURE_REASONS` tuple.

**Action.** Replace hand-maintained "all pressure" arrays with
`PRESSURE_REASONS.filter((reason) => reason !== 'NONE')`. Explicit policies may
instead add only the new reasons they intend to shed. Registration now accepts
all seven values and still rejects unknown strings.

### Redis rate-limit proxy diagnostics omit the keyed IP

The one-shot proxy-collapse warning no longer includes the private or loopback
address that triggered it. Collectors should route the stable
`redis.ratelimit.proxy-collapse-risk` event and its
`addressClass=private-or-loopback` attribute instead of extracting an IP
from English text. Rate-limit verdicts and bucket keys are unchanged.

### Thrown error messages end with a stable ID and reference route

**What changed.** Errors thrown through the error registry - the connection
registry's `request()` and `requestSession()` errors for an invalid target or
event, an offline target, the pending cap and a cross-instance timeout, the
invalid-target errors of `send()` and `sendCoalesced()`, and the circuit
breaker's `CircuitBrokenError` - end with ` [EXTENSIONS-ERR-...] See:
docs/errors.md#...`. The text before the suffix is unchanged. Other registry
errors, including the other `send()`, `sendCoalesced()` and `sendTo()`
validation errors, carry no suffix.

**How to migrate.** Code that compares `err.message` for equality breaks
silently. Match on `err.message.startsWith(...)`, on the stable ID, or on the
error class and `code` instead.

### Diagnostics print the adapter's canonical grammar

**What changed.** Assertion lines and every operational failure this package
reports now start with
`[lantean/diagnostic source=svelte-adapter-uws-extensions component=... event=... severity=...]`
followed by a fixed message and a one-line JSON record, the same grammar the
adapter prints. Values the old prose interpolated - error text, request ids,
counts, bounds - are fields of that record's `attributes`; raw topics and user
ids are not logged, nor is the text of an error a custom NOTIFY parser throws,
since it can quote the payload. Cursor cleanup and group heartbeat failures are
reported once per failure streak rather than on every tick. The former
`[extensions/assert]`, `[extensions/fatal]`, `[extensions/devAssert]` heads and
every per-module prose prefix are gone: `[redis]`, `[redis replay]`,
`[redis stream replay]`, `[postgres replay]`, `[postgres tasks]`,
`[postgres/notify]`, `[prometheus]`, `[pubsub]`, `[sharded-bus]`,
`[registry]`, `[redis/registry]`, `[publish-rate]`, `[redis/presence]`,
`[redis/smooth]`, and the untagged `redis ratelimit:`, `redis composite
ratelimit:`, `presence:`, `replay resume:`, `capability-cookie:`,
`postgres:`, `groups ...`, `cursor ...`, `presence ...` and `sharded bus
...` lines. `devAssert` reports at `severity=error` under
`event=invariant.violated` with `category=development.assertion`. Operational
failures reach the process-wide sink installed through
`svelte-adapter-uws/observability`; assertion lines, as in the adapter, print
to the console directly and do not reach it. Every failure event is indexed
in `docs/errors.md`.

**How to migrate.** Route on the `event=` key (or on the parsed record's
`event` field) instead of on the old prefixes; the error reference lists every
event. `parseDiagnostic()` from `svelte-adapter-uws/observability` parses the
new lines as canonical records and still recognizes the old prefix families
during the 0.6 window, so a collector that already uses it needs no change.
A `See: https://svti.me/...` shortlink a line used to print is in its error
reference entry. Connection errors on the Redis and Postgres connections this
package creates are reported the same way (`redis.client.failed`,
`redis.*.subscriber.failed`, `postgres.notify.connection.failed`) instead of
ioredis's `[ioredis] Unhandled error event` line. Three one-shot configuration
advisories keep their prose under a new tag: the presence private-key-field
warning and the sensitive-`userData`-key warning now start with
`[extensions] `, and the cursor `hooks.message` wiring warning with
`[redis/cursor] `. Each names its own remedy and parses as a legacy record.
The diagnostic pipeline's own last-resort lines, printed when a record cannot
be rendered or delivered, start with `[extensions] ` and end with their
stable ID.

### Redis presence/cursor defaults carry only the identity

**Symptom.** A zero-config presence roster entry is just `{ id }` (the
configured `key` field) and a zero-config cursor catalog user is just
`{ id }`: `name`, `avatarUrl`, `role` and every other field the upgrade
hook returned are gone. If `presence.key` names a credential-shaped or
transport field such as `sessionId` or `email`, the tracker warns and uses one
`__conn:N` entry per connection instead of multi-tab dedup.

**Why.** Whatever the upgrade hook returned reached every topic peer and the
Redis presence/cursor hashes, minus a denylist of names. A denylist cannot know
which fields of an app's user record are private, so the default now copies the
identity only, as the adapter's in-memory plugins do. A presence key is also a
roster property/hash-field name, so a private key is never copied either.

**Action.** Pass an explicit allowlist containing only what the UI needs, for
example `select: (ud) => ({ id: ud.id, name: ud.name })`. An explicit select is
used as-is: it receives the connection's userData unchanged, `remoteAddress`
included, and its result is no longer passed through `stripInternal`, so return
only what peers should see. Presence alone drops top-level `__`-prefixed,
`constructor` and `prototype` names from it. Keep `presence.key` on a
non-secret selected identifier.

### Redis presence updates from clients need `clientUpdateFields`

**Symptom.** Typing indicators, selections or other state a client sends as a
`presence-update` frame stop reaching peers. A server-side `presence.update()`
that sets `id`, `role`, the key field, or a credential, personal-data or
transport name such as `email` drops that field, with a one-time warning per
identity name outside production. `presence.list()` entries gain the durable
`update()` fields.

**Why.** A client frame could write any field name, `id` and `role` included.
It was stored in Redis, relayed to every instance and rendered by every peer,
so a client could change the identity peers saw for it or publish private
data under its own entry. The in-memory plugin refuses the same names.

**Action.** List the fields clients may write:
`createPresence(redis, { clientUpdateFields: ['typing', 'selection'] })`.
That list is then also the whole rule for server calls, and every name on it
is one clients can write. A server `update()` that set `role` during a
session no longer does: `select` is read once per connection, at join, so a
server-owned value that changes later goes under a name of its own that is not
reserved, such as `isHost`, written with `update()` while `clientUpdateFields`
is unset. Code that read only the identity from `list()` should pick the
fields it needs.

### Authorization now fails closed

`presence.sync`, `cursor.snapshot` / `cursor.attach` and the replay resume gate
previously skipped authorization entirely when the platform had no
`checkSubscribe` - the gate disappeared rather than denying. They now deny. The
adapter peer floor has provided `checkSubscribe` for many releases, so only a
**custom platform** is affected; the fix there is to implement it.

### `presence.hooks.subscribe` returns a denial reason

It now resolves to the platform's denial reason (a string) rather than
`undefined` when the topic is refused. If you wrap the hook, **return** its
result - `return presence.hooks.subscribe(ws, topic, ctx)` - or the socket stays
subscribed to a topic the platform refused. `cursor.hooks.subscribe` is the
same. Both are typed `Promise<string | undefined>` now.

### Replay and dead-letter erasure uses the tenant topic namespace

Redis and Postgres replay buffers and dead-letter queues now restrict deletion
to topics beginning with `@t/<tenantId>/`. A null tenant matches only topics
outside `@t/`; it does not mean all tenants. Verify that stored wire topics
use the intended namespace before relying on tenant-specific erasure.

This is not a blanket guarantee for every store: task rows have no tenant
column and task `purgeUser` ignores its tenant argument. Use a tenant-qualified
user identity there and audit the application's erasure fan-out. Redis
forget-tombstone behavior and stores whose identity is not topic-derived must
also be checked against their own contract.

### `createClusterClock` prefixes `leaderKey`

`leaderKey` is now relative to the client key prefix, so two apps sharing one
Redis stop colliding on the well-known `clock:leader-offset` key. Pass the bare
name. During a rolling upgrade, `legacyLeaderKeyFallback: true` reads the
unprefixed key - opt-in, because the unprefixed name is the shared one the
prefixing exists to escape. Left off, a follower ahead of its leader degrades to
`consistent()` for one key lifetime.

### Persisted task errors omit `cause`

Handlers routinely attach config-bearing causes (a connection error carries its
DSN) and the stored row is re-served to dashboards by `await()` / `list()`. Set
`serializeErrorCause: true` on `createTaskRunner` to restore the old shape. The
same applies to the error handed to `onStateChange`, so a listener doing
`e.error.cause?.retryable` now reads `undefined` unless you opt in.

### `deadLetter.summary().byTopic` is null-prototype

Property reads are unchanged, but `byTopic.hasOwnProperty(...)`,
`byTopic.toString()` and `byTopic instanceof Object` no longer work. Use
`Object.hasOwn(byTopic, t)` and `Object.keys(byTopic)`. A stored topic named
`__proto__` previously hit the prototype setter instead of counting as data.

## 5. Test-only

### `mockRedisClient` SCAN matches the real matcher

`MATCH` now runs a port of Redis's own `stringmatchlen()` rather than a
translation into a JavaScript regular expression. A purge-scope or
tenant-isolation test that passed against the old matching **may now fail** -
which is the point, since it was passing against semantics production does not
have. Three cases changed direction: a reversed range (`[c-a]`) used to throw
`Range out of order`, an unescaped leading `]` was read as a literal member
where Redis closes the class, and `[a-]` is the range `a`..`]` with the
endpoints swapped rather than the two members `a` and `-`.

`mockRedisClient` also gained `xdel`, so the streams replay backend's
right-to-erasure purge now actually erases against the double instead of
swallowing a missing command and reporting success.

### The sim swarm's fault-enablement knob is renamed `faultMode`

On `runRedisSimSwarm` and `runPgSimSwarm`, three options and one summary
counter change name. The old names are given here so a migrating caller can
find them: <!-- vocabulary-allow: a rename note must name the superseded option so migrating callers can find it -->

| Was | Now |
|---|---|
| `buggify` | `faultMode` | <!-- vocabulary-allow: rename table, superseded option name -->
| `buggifyProbability` | `faultProbability` | <!-- vocabulary-allow: rename table, superseded option name -->
| `buggified` (per-run flag and summary counter) | `faulted` | <!-- vocabulary-allow: rename table, superseded option name -->

## 6. New, no action required

### The lease Lua scripts are public at `redis/lease-scripts`

**What changed.** The compare-and-renew and compare-and-release scripts shared by `createDistributedLock`, `createLeader`, the smooth-cluster relay and the CRDT relay are exported as `LEASE_RENEW_SCRIPT` and `LEASE_RELEASE_SCRIPT` from `svelte-adapter-uws-extensions/redis/lease-scripts`. That subpath is the only supported import; the implementation file under `shared/` is not in the export map and never was.

**How to migrate.** No action for callers of the lock, leader, or smooth-cluster APIs. Tooling that copied the script text out of `redis/lock` to call `redis.eval` itself can import the constants instead:

```js
import { LEASE_RENEW_SCRIPT, LEASE_RELEASE_SCRIPT } from 'svelte-adapter-uws-extensions/redis/lease-scripts';
```

## Executable rehearsal

From this checkout with dependencies installed and Docker running:

```sh
npm exec -- vitest run test/migration-imports.test.js
npm exec -- vitest run --config vitest.config.integration.postgres.js test/integration/postgres/migration-rehearsal.test.js
```

The PostgreSQL test packs the current checkout, imports only that extracted
package, and supplies the installed dependency tree. It starts an isolated real
PostgreSQL service through the integration harness. It seeds the jobs/tasks
schema from stable 0.5.10, tests automatic migration and deployment migrations,
then verifies old rows survive, new trace context round-trips, and a legacy
column-only query still reads the same data. Removing a trace-column migration
must fail the corresponding rehearsal. It does not run an entire old binary or
prove all old/new wire combinations interoperable.

For concurrent local suites, set `INTEGRATION_COMPOSE_PROJECT` and
`INTEGRATION_POSTGRES_HOST_PORT` to unused values. The harness removes only its
named stack. Run the remaining backend integration lanes used by your application
before production rollout.

## Rollback and fleet compatibility

Keep the pre-upgrade lockfile and artifact digests until the deployment passes
its application canary. Roll back dependants in reverse order: realtime,
extensions, then adapter. Stop producers and drain handlers first; restoring an
old process while a new handler still owns work can duplicate side effects.

The added nullable trace columns can remain when rolling back. Do not drop them
while any 0.6 instance is running. This additive change does not make every
storage format interchangeable: the Redis idempotency key transition still
requires the same TTL drain in either direction. A snapshot restore can discard
post-snapshot writes; reconcile those with downstream side effects before use.

Do not assume rolling wire compatibility merely because sockets connect.
Selected presence identity is now carried separately from durable fields;
update all receivers for its full identity guarantee. Confirm relayed compression,
sequence authority, authorization denials and reconnect/replay behavior on the
exact deployment tuple. Preserve prefixes, table names and tenant topic mapping
unless you separately migrate their data.

## After upgrading to 0.6

Run your test suite, then check:

- Anything wrapping `presence.hooks.subscribe` or `cursor.hooks.subscribe`
  returns the hook's result.
- Task handlers whose result size scales with input - the cap is terminal on
  that path.
- `registry.send` / `sendCoalesced` / `group.publish` call sites are awaited.
- Purge-scope and tenant-isolation tests, against the corrected SCAN matcher.
- A custom platform implements `checkSubscribe`.
- Jobs and tasks tables you migrate yourself have `traceparent` and
  `tracestate`; tasks also have `user_id` and its lookup index.
- Log routing and alerting keyed on the old prose prefixes route on `event=`.

---

## Duration metric migration

Generic histogram defaults now describe seconds with fractional bounds from
`0.001` through `1`. Supply explicit buckets for count, byte or millisecond
observations, or set `defaultBuckets` to the former
`[1, 5, 10, 25, 50, 100, 250, 500, 1000]` for application histograms that need it.
Built-in Redis relay durations use explicit bounds through 10 seconds and
PostgreSQL operations through 60 seconds, independently of that default.

Replace `lock_acquire_wait_ms` queries with `lock_acquire_wait_seconds` and
convert millisecond alert thresholds to seconds by dividing by 1000. The new
metric's `_sum` already reports seconds; `_count` is unchanged.
Historical bucket counts cannot be converted to a new layout. Use matching
bounds across producers before comparing histogram quantiles during a rollout.

# 0.4.x to 0.5.x

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

### `createPresence` requires Redis 7.4+ (was 7.0+)

**What changed.** `createPresence` was rewritten to use Redis 7.4+'s per-field hash TTL primitive (`HEXPIRE` family) so a single LEAVE_SCRIPT call is now O(1) Redis-blocked Lua time rather than O(M) where M is the topic's total presence-field count. Mass-disconnect of N users on instance restart is now O(N) rather than O(N x M); the previous quadratic behavior was reachable at realistic auctions/chat scale (10k users x 10 instances) and could block Redis for seconds during a server restart. The new code probes `INFO server` on first use and throws on older Redis with the message `redis presence: requires Redis 7.4+ for per-field TTL (HEXPIRE); got X.Y.Z`.

**Storage layout.** The pre-fix per-topic hash was `presence:{topic}` with fields keyed by `{instanceId}|{userKey}`. The new layout splits storage:

- `presence:topic:{topic}` HASH, field=`userKey`, value=JSON `{data, ts}`. One field per unique user on the topic. Backs `list()` / `count()`.
- `presence:user:{topic}:{userKey}` HASH, field=`instanceId`, value=`ts`. One field per instance currently presenting this user. Backs the leave HLEN check.

Both hashes have per-field TTLs via `HPEXPIRE`. The pre-fix whole-key EXPIRE is gone; keys implicitly disappear when their last field expires.

**How to migrate.**

1. **Upgrade Redis to 7.4 or newer.** Every major managed Redis (AWS ElastiCache, Upstash, Redis Cloud, MemoryDB) supports it. Self-hosters bump the image tag (`redis:7.4-alpine` is the canonical pin).

2. **Rolling deploy.** Old (0.4) and new (0.5) instances coexist by writing to disjoint key namespaces. Old keys at `presence:{topic}` are ignored by the new code and expire naturally as the old instances drain. During the rollout window, `list()` / `count()` from new instances miss old-instance presence; convergence completes within one TTL of full rollout. If your TTL is the default 90 seconds, plan a ~2-minute rollout completion before relying on accurate presence counts.

3. **`metrics().staleCleanedTotal` is now always 0.** Per-field staleness is enforced atomically by Redis HPEXPIRE rather than by an application-side cleanup script, so there is nothing to count. The field stays in the return shape for backward-compatibility. The corresponding Prometheus counter `presence_stale_cleaned_total` is no longer registered.

4. **`keyspaceNotifications: true` scope is narrower.** Pre-fix, the option subscribed to whole-key `__keyevent@*__:expired` for `presence:{topic}` keys and emitted an empty `state` when one fired. Same shape in 0.5 (the per-topic hash whole-key expiry only fires when every field has expired, i.e. no live instances). Per-field expiry (a single crashed instance) does NOT trigger this notification; users disappear from `list()` / `count()` results lazily. If you need explicit per-field-expiry events, subscribe to `__keyevent@*__:hexpired` separately (separate Redis CONFIG flag).

5. **Falling back to single-instance.** If you can't upgrade Redis, switch to the in-memory `createPresence` plugin from `svelte-adapter-uws/plugins/presence`. Public API is the same; you lose cross-instance presence which is the whole point of the Redis variant, but for single-instance deployments it's a clean drop-in.

If your app uses `createPresence` and you cannot upgrade Redis immediately, pin `svelte-adapter-uws-extensions` to the last 0.5.x release before this change.

### Replay backends consult `platform.checkSubscribe` before reading history

**What changed.** Every `replay()` implementation (Redis sorted-set, Redis streams, Postgres) now calls `platform.checkSubscribe(ws, topic)` first. On denial, it sends a single `{event:'denied', data:{code:<denial>, reqId}}` frame on `__replay:{topic}` and returns without reading the buffer. Pre-fix, an attacker could send a crafted `lastSeenSeqs` map and read history for a topic they could not subscribe to live.

**How to migrate.** Track denial separately from truncation: authorization failed, so automatic retries cannot make the cached content current. Preserve the denial code and request ID for support, mark the affected content stale, stop replay-dependent writes, and expose a visible, politely announced notice with an ordinary keyboard-accessible recovery link. Use your application's actual account/access route and localized copy.

Mount the notice before replay starts so later text updates are announced:

<!-- replay-denial-markup -->
```html
<p id="replay-notice" role="status" aria-live="polite" aria-atomic="true"></p>
<p id="replay-action" hidden><a href="/account">Review sign-in and access</a></p>
<section id="replay-content" data-stale="false" aria-describedby="replay-notice">Cached content</section>
```

<!-- replay-denial-consumer -->
```js
// One state/controller per topic. Set reqId when starting that topic's replay.
const replayState = { status: 'loading', reqId: 'resume-1', code: null, stale: true, canWrite: false };
const notice = document.querySelector('#replay-notice');
const action = document.querySelector('#replay-action');
const content = document.querySelector('#replay-content');
content.dataset.stale = 'true';

function onReplayFrame(event, data) {
  // An older replay response must not overwrite the current request's state.
  if (data.reqId !== replayState.reqId || event !== 'denied') return;
  Object.assign(replayState, { status: 'denied', code: data.code, reqId: data.reqId, stale: true, canWrite: false });
  content.dataset.stale = 'true';
  notice.textContent = 'Updates are unavailable because access was denied. Displayed content may be out of date. Review your sign-in and access before trying again.';
  action.hidden = false;
}
```

Bind replay-dependent editing controls to `canWrite`. Keep the notice and stale marker until a newly authorized replay or snapshot has actually completed; an old `end` frame or a restored WebSocket connection alone does not prove that content is current. Do not move focus when the notice appears. The link remains available in the normal tab order.

Custom platforms without `checkSubscribe` fail closed and receive a denial; implement the documented authorization hook before enabling replay.

### Tasks `idempotencyKey` is now namespaced by task name, with a 256-char cap

**In-flight cache entries become invisible after deploy.**

**What changed.** The cache key passed to `idempotency.acquire(...)` for `tasks.run` is now `'task:' + name + ':' + idempotencyKey` (was the bare `idempotencyKey`). Pre-fix, two tasks sharing one client-supplied key shared one cache slot, which let a privileged task's cached result leak to a public task. Both `tasks.run` and `tasks.enqueue` reject `idempotencyKey` longer than 256 characters; the underlying stores cap `acquire(key)` at 1024 chars as defense in depth.

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

### Presence wire shape on `__presence:{topic}` migrated to `state` / `diff`

**Only impacts apps with hand-rolled WebSocket clients consuming the wire directly.**

**What changed.** The `redis/presence` channel previously emitted `list` / `join` / `leave` / `updated` / `heartbeat` events. It now emits:

- `state` (sent once on subscribe to a single connection): payload is `{[userKey]: data}` - a flat snapshot.
- `diff` (broadcast to topic subscribers, microtask-batched): payload is `{joins: {[key]: data}, leaves: {[key]: data}}`. Same-tick joins+leaves on the same key collapse to the latest op; updates appear as a `joins` entry with the new data.
- `heartbeat` is unchanged.

The public JS API on `createPresence` is unchanged (`join`, `leave`, `sync`, `list`, `count`, `metrics`, `clear`, `destroy`, `hooks` keep their signatures). The cross-instance Redis pub/sub envelope on `presence:events:{topic}` is also unchanged. The `keyspaceNotifications: true` mode now emits an empty `state` event (was an empty `list` event) on hash expiry.

**How to migrate.** Apps with a custom WebSocket client decoding presence frames must swap their decoder from the four legacy event names to `state` / `diff`:

```js
// before
case 'list':    setAll(payload); break;
case 'join':    add(payload.key, payload.data); break;
case 'leave':   remove(payload.key); break;
case 'updated': set(payload.key, payload.data); break;

// after
case 'state':
  state.clear();
  for (const k of Object.keys(payload)) state.set(k, payload[k]);
  break;
case 'diff':
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
