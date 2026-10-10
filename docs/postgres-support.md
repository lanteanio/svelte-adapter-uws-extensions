# PostgreSQL support

For replacing credentials, draining pools and preserving emergency access, see
[Database credential rotation](./credential-rotation.md).

The supported server floor is PostgreSQL 16. The repository integration lane runs `postgres:16-alpine`; that is the verified baseline, not a claim that every newer server or managed provider has been qualified. Use a current minor release and consult the [PostgreSQL support policy](https://www.postgresql.org/support/versioning/). Qualify newer majors and provider-specific behavior with the same affected integration suites before deployment.

| Import suffix | Server floor | Additional requirement |
| --- | --- | --- |
| `postgres` | 16 | Ordinary PostgreSQL connection/pool access. |
| `postgres/replay` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/dead-letter` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/alarm-store` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/idempotency` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/tasks` | 16 | Table/index access and `SKIP LOCKED`; apply the documented nullable columns before running with automatic migration disabled. |
| `postgres/jobs` | 16 | Table/index access and `SKIP LOCKED`; apply the documented nullable columns before running with automatic migration disabled. No queue extension is required. |
| `postgres/notify` | 16 | A dedicated session for `LISTEN`; transaction-pooling proxies cannot preserve session subscriptions. |

All suffixes are under `svelte-adapter-uws-extensions/`. The old job-queue claim of PostgreSQL 9.5 support was incorrect: although 9.5 has `SKIP LOCKED`, its `ALTER TABLE` grammar lacks `ADD COLUMN IF NOT EXISTS`, which initialization uses. Compare the official [9.5 grammar](https://www.postgresql.org/docs/9.5/sql-altertable.html) and [9.6 grammar](https://www.postgresql.org/docs/9.6/sql-altertable.html). Legacy 9.x support is not provided.

## Read-only preflight

Before enabling stores, inspect the actual destination database. From this repository with dependencies installed and `DATABASE_URL` configured, run:

```sh
npm run preflight:postgres
```

The command issues only `SHOW server_version_num`, refuses a missing/malformed identity or a version below 16, and closes its single connection. Connection and query waits are bounded to five seconds. It prints the numeric server version and the tested baseline, never the connection URL. The standalone Postgres integration setup performs the same check before any store tests initialize tables.

For an application using the installed package, the equivalent read-only check uses the existing client API:

```js
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';

const database = createPgClient({
  connectionString: process.env.DATABASE_URL,
  autoShutdown: false,
  options: { max: 1, connectionTimeoutMillis: 5000, query_timeout: 5000, statement_timeout: 5000 }
});
try {
  const { rows } = await database.query('SHOW server_version_num');
  const version = rows[0]?.server_version_num;
  if (typeof version !== 'string' || !/^\d{5,6}$/.test(version) || Number(version) < 160000) {
    throw new Error('PostgreSQL 16 or newer is required before store initialization');
  }
} finally {
  await database.end();
}
```

This verifies the server floor, not schema readiness, migration privileges, pool sizing, backup/restore, or proxy behavior. Apply the schema described by each store before using `autoMigrate: false`; version acceptance cannot replace those operational checks.

## Pool operation bounds

`createPgClient({ connectionString })` owns its pool and applies these defaults:

| Setting | Default | Scope |
| --- | --- | --- |
| `options.connectionTimeoutMillis` | `10000` | Maximum wait for pool checkout or a new connection. |
| `options.query_timeout` | `30000` | Driver query read deadline after checkout. |
| `options.statement_timeout` | `30000` | PostgreSQL execution deadline per statement. |
| `maxInFlight` | `100` | Total unsettled wrapper `query()` and `connect()` operations. |

Each setting accepts `0` to disable its bound. Timeout values must be integers from `0` through `2147483647` milliseconds; `maxInFlight` must be a non-negative safe integer. Choose overrides for long migrations or deliberate long-running statements; finite deadlines are a behavior change from the driver's unlimited defaults. Pool checkout and query execution are separate phases, so a query can consume both budgets. Configure timeout opt-outs on the factory's `options`; node-postgres does not treat a per-query `query_timeout: 0` as an override of a positive pool default.

Admission happens before entering the driver queue. Excess calls reject with a `ConnectionError` whose `code` is `PG_CAPACITY_EXCEEDED`. A query holds admission through checkout and query settlement. `connect()` holds admission only until checkout settles; the caller then owns the lease and must release it. The driver's pool `max` still bounds checked-out connections. Direct calls through `client.pool`, acquired-client queries, and standalone `createClient()` sessions bypass wrapper admission. Owned timeout defaults also apply to standalone sessions.

A supplied `pool` retains its original settings and lifecycle. Its default `maxInFlight` is `0`; opt into admission with `createPgClient({ pool, maxInFlight: 100 })`. Configure `connectionTimeoutMillis`, `query_timeout` and `statement_timeout` on the original `pg.Pool` before passing it in. `options` does not reconfigure that pool, and `end()` remains a no-op. If a connection string is also supplied for `createClient()`, its standalone configuration uses the supplied `options` without adding owned-pool defaults.

### Caller cancellation and deadlines

`query(textOrConfig, values, { signal })` and `connect({ signal })` accept an `AbortSignal`. `AbortSignal.timeout(ms)` provides a caller deadline; pass `undefined` as `values` when a query has no positional values. A pre-aborted signal rejects before driver admission. Abort rejects with the signal's original reason and removes its listener.

Cancellation stops the caller's wait. It does not cancel SQL, remove a queued checkout from node-postgres, roll back a transaction, or free admission early. A cancelled query can still execute and commit. Before retrying a write, reconcile its durable result or use an idempotency key. An abandoned checkout is returned exactly once when the driver eventually acquires it. A successfully returned lease remains the caller's responsibility even if the signal later aborts.

The wrapper reserves admission until the actual driver operation settles, including after caller cancellation. A custom external pool that never settles can therefore consume its configured capacity permanently; new operations fail fast at the bound. Keep native pool deadlines enabled to terminate real driver waits. Circuit-breaker wrappers observe timeout or cancellation rejections and can open while abandoned work is still accounted for. Redis clients have their own command and teardown deadline settings; see [Redis connection options](../README.md#redis-client).

### Pool metrics

Pass the existing Prometheus registry as `metrics`. Labels use only the fixed operation names `query` and `connect`, result classes, and rejection reasons; SQL, connection strings and caller identifiers are excluded.

| Metric | Meaning |
| --- | --- |
| `postgres_operations_in_flight{operation}` | Actual unsettled wrapper work, including cancelled caller waits. |
| `postgres_pool_waiters` | Driver checkout queue depth at scrape time; shared pools are counted once per registry. |
| `postgres_pool_connections{state}` | Native connections in fixed `active` and `idle` states, including direct pool checkouts. |
| `postgres_pool_capacity` | Sum of configured pool connection limits. |
| `postgres_operation_duration_seconds{operation,result}` | Underlying settlement latency in seconds; result is `ok`, `error` or `timeout`. |
| `postgres_operation_timeouts_total{operation}` | Recognized driver checkout, query-read and server statement timeout failures. |
| `postgres_admission_rejected_total{operation,reason}` | Capacity rejection or caller cancellation; reason is `capacity` or `aborted`. |

Duration and timeout metrics follow actual settlement, so an aborted caller can leave the in-flight gauge positive without producing a duration observation yet. Driver queue depth also includes direct use of the underlying pool. Metrics are optional; the uninstrumented path does not read a clock or scrape pool state.

Pool gauges count each native pool once per registry, omit ended or collected pools, and exclude unavailable fields on custom pool implementations. A checked-out client remains active after the wrapper's connect operation has settled; use these gauges for pool utilization rather than the wrapper in-flight count. Pool names and connection strings are never labels.

## Notification sessions and shutdown

`createNotifyBridge()` needs a persistent PostgreSQL session. Use a direct database DSN or a PgBouncer pool configured in **session** mode. Transaction and statement pooling cannot preserve `LISTEN` subscriptions or session-scoped advisory locks. A transaction-pooled DSN may still be suitable for ordinary application queries, but it must not be reused for this bridge. Pass a separate direct/session DSN alongside an external query pool when the two routes differ.

Each active bridge consumes one standalone database session outside `pool.max`. In both `multiListener: 'all'` and `'advisory'`, N replicas with B bridges consume N x B additional sessions. Advisory election reduces active LISTEN consumers to one per lock, while every follower retains a session to attempt the lock. Budget pooled connections, these dedicated sessions, administrative access, migrations and other services against the server/proxy connection limits. Reconnects wait for the old session to close before creating its replacement.

Initialize in the adapter's awaited `init({ platform })` hook, before WebSocket traffic is served. Await the Redis bus first when using advisory mode, then await `bridge.activate(bus.wrap(platform))`. Startup resolves once the dedicated session is established and either LISTEN succeeds or another advisory session owns the lock. A connection or setup failure rejects in both modes; a healthy follower is not a startup error. Concurrent activation calls share work. Notifications sent before LISTEN is established or during a disconnection are not durable and cannot be replayed by this bridge.

Owned `createPgClient({ connectionString })` instances stop registered bridges, cancel their retry/election timers, close every tracked `createClient()` session, then close the pool. `await pg.end()` waits for all of this, and repeated calls share the same shutdown. The existing `sveltekit:shutdown` registration invokes the same cleanup unless `autoShutdown: false` is set. Call `await bridge.deactivate()` to stop a bridge independently. Shutdown interrupts startup waits; a late connect or LISTEN result cannot reactivate the bridge. New standalone sessions and bridge activation are refused after owned shutdown begins.

An externally supplied `pool` remains caller-owned: wrapper `end()` is still a no-op, including for its standalone sessions. In your application shutdown hook, await each `bridge.deactivate()` and manually created standalone client's `end()` before awaiting the external `pool.end()`. Do not create a new standalone client only to close it. Pool checkout leases still require the caller's `release()` before a pool can finish closing.

Reconnect and election operations never overlap within one bridge. Failed connections use a jittered delay from `reconnectInterval` up to twice that baseline; `0` requests immediate retries. Both intervals must fit the native timer maximum of `2147483647` ms, and jitter is capped at that maximum. Healthy advisory followers poll at `pollInterval`; a leader checks liveness without repeatedly acquiring its already-held advisory lock. A retry waiting on its jitter timer is not bypassed by election polling. `autoReconnect: false` stops automatic retries after loss; an explicit awaited `activate()` can retry. Keep the owned connection/query deadlines enabled, or provide equivalent finite standalone options when using an external pool, so backend waits have a deadline.

## Schema transitions and readiness

The [schema transition manifest](./postgres-schema.v1.json) describes this
package's 0.6 candidate relative to stable 0.5.10. It ships with the package;
retain it with the exact tarball digest and application lockfile. It is a
release description, **not an applied database migration ledger**. The stores
have no shared schema-version row, checksum ledger or all-store transaction.
The minimum server is PostgreSQL 16 for every row below.

| Store | Change from 0.5.10 | Initialization with `autoMigrate: true` | Startup gate |
| --- | --- | --- | --- |
| Replay | Adds event `user_id`, identity and retention indexes, and sequence `epoch` with default 0. | First store operation. | No `ready()`; apply/verify schema before opening traffic. |
| Idempotency | Adds `user_id`, `tenant_id`, `owner_token` and tenant/user index. | Starts at construction. | Await `store.ready()`. |
| Jobs | Adds nullable `traceparent` and `tracestate`. | First store operation. | No `ready()`; apply/verify schema before starting producers/workers. |
| Tasks | Adds nullable trace columns, `user_id` and user index. | Starts at construction. | Await `runner.ready()` before producers and handlers. |
| Dead letter | New PostgreSQL store in this release series. | First store operation. | No `ready()`; provision/verify its complete schema. |
| Alarms | New PostgreSQL store in this release series. | Starts at construction. | Await `store.ready()` before scheduling. |

`autoMigrate: false` disables DDL; existing `ready()` methods resolve immediately
and do not inspect columns, types, indexes or privileges. The read-only server
preflight above checks only the server version. Neither result certifies schema
readiness. Constructors and ordinary reads can perform DDL when automatic
migration is enabled, so do not use an arbitrary store call as a read-only probe.
Notification activation verifies its own dedicated session and LISTEN/election,
not any durable store's schema or missed-event reconciliation.

For a controlled rollout, resolve the configured table names and `search_path`,
compare column types/defaults/nullability and valid indexes with the target
schema, and verify the actual runtime role's operation privileges. Use explicit
column-only `SELECT ... LIMIT 0` queries plus PostgreSQL catalogs under a
read-only transaction; do not invoke migrations merely to inspect production.
The built-in drift guard checks selected column names, not their types or index
shape. Own the DDL in one deployment step and await its completion before
admitting application traffic with `autoMigrate: false`.

### Existing replay and idempotency tables

Alongside the [jobs/tasks migration](../MIGRATION.md#jobs-and-tasks-tables-gain-trace-columns-tasks-gain-erasure-identity),
apply these additions when managing the schema yourself. Names below are the
defaults; substitute each configured table name, including replay's `_seq`
companion. Fresh dead-letter and alarm stores need their complete README schemas,
not just an `ALTER TABLE` fragment.

```sql
ALTER TABLE svti_replay ADD COLUMN IF NOT EXISTS user_id TEXT;
CREATE INDEX IF NOT EXISTS idx_svti_replay_user ON svti_replay (user_id);
CREATE INDEX IF NOT EXISTS idx_svti_replay_created_at ON svti_replay (created_at);
ALTER TABLE svti_replay_seq ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE svti_idempotency ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE svti_idempotency ADD COLUMN IF NOT EXISTS tenant_id TEXT;
ALTER TABLE svti_idempotency ADD COLUMN IF NOT EXISTS owner_token TEXT;
CREATE INDEX IF NOT EXISTS idx_svti_idempotency_user ON svti_idempotency (tenant_id, user_id);
```

These additions preserve old values; they do not infer ownership. Historical
null identity columns need trusted application backfill or expiry before erasure
can cover those records. Do not invent owner tokens for an in-flight old writer.
Stop old idempotency producers, drain their pending ownership windows, and
reconcile retained results before the new token-aware writers take over.

### Mixed versions, downgrade and restore

Additive SQL is not proof of mixed-writer safety. The rehearsal verifies old
jobs/tasks rows and column-only readers, plus preserved replay sequence values
and idempotency results. It does not certify concurrent old/new worker binaries.
Old idempotency writers do not honor the new owner token; old replay reset/resume
code does not honor epochs. Keep one compatible writer cohort for those stores.
There is no destructive down migration: leave added columns and indexes in place,
stop new producers, drain active work, then revert dependants in the order in the
[migration guide](../MIGRATION.md#rollback-and-fleet-compatibility).

Restore replay events together with their sequence/epoch table. Restoring only
one can reuse sequence numbers or claim history that is absent. Restore task
rows with their fence, attempt, status, input/result and idempotency mappings
from a coordinated application recovery point. A database restore cannot undo
external side effects: reconcile payment/email/job outcomes before retrying
work. New alarm and dead-letter tables have no old PostgreSQL implementation;
preserve them and define application reconciliation instead of deleting data
as a downgrade shortcut. Backup freshness, RPO and RTO require a measured host
restore drill; this package's disposable tests do not establish them.

### Failed-migration tabletop

1. Rehearse against an isolated restored copy and the exact candidate tarball.
   Pause producers and retention workers; save the source snapshot and schema.
2. Inject a missing-column or denied-DDL failure. Await the available `ready()`
   gates and keep application readiness false on rejection. For a lazy store,
   run the deployment's explicit schema verifier before enabling it.
3. Inventory committed DDL: automatic initialization issues separate statements,
   so a later failure can leave earlier additions in place. Do not assume an
   all-or-nothing migration or drop a data-bearing table because a drift error
   mentions a conflicting object. Correct the schema/privileges deliberately.
4. Reapply idempotent additions, construct fresh store instances, and re-run the
   startup gates. A rejected one-shot `ready()` promise on an old instance stays
   rejected. Validate old records and an isolated new operation before admitting
   traffic; inspect role, index and storage errors rather than overriding them.
5. If validation fails, stop the candidate and follow the downgrade boundaries
   above. Retain the error, artifact/lockfile digests, applied statements and
   recovery decision as incident evidence; exclude credentials and payload data.

The live `test/integration/postgres/migration-readiness.test.js` regression covers
old replay/idempotency schemas and the failed-schema/disabled-migration boundary.
The [packed rehearsal](../MIGRATION.md#executable-rehearsal) covers both automatic
and documented manual jobs/tasks upgrades. Run both on the actual PostgreSQL
version before deployment; neither substitutes for a host restore drill.
