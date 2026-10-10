# PostgreSQL support

For replacing credentials, draining pools and preserving emergency access, see
[Database credential rotation](./credential-rotation.md).

For workload plans, statement I/O and maintenance measurements, see
[PostgreSQL query and maintenance evidence](./postgres-plans.md).

The supported server floor is PostgreSQL 16. The repository integration lane runs `postgres:16-alpine`; that is the verified baseline, not a claim that every newer server or managed provider has been qualified. Use a current minor release and consult the [PostgreSQL support policy](https://www.postgresql.org/support/versioning/). Qualify newer majors and provider-specific behavior with the same affected integration suites before deployment.

| Import suffix | Server floor | Additional requirement |
| --- | --- | --- |
| `postgres` | 16 | Ordinary PostgreSQL connection/pool access. |
| `postgres/replay` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/dead-letter` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/alarm-store` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/idempotency` | 16 | Table/index access; schema creation privileges when automatic initialization is enabled. |
| `postgres/tasks` | 16 | Table/index access and `SKIP LOCKED`; run the complete versioned migration before disabling automatic migration. |
| `postgres/jobs` | 16 | Table/index access and `SKIP LOCKED`; run the complete versioned migration before disabling automatic migration. No queue extension is required. |
| `postgres/notify` | 16 | A dedicated session for `LISTEN`; transaction-pooling proxies cannot preserve session subscriptions. |

All suffixes are under `svelte-adapter-uws-extensions/`. PostgreSQL versions below 16 are not supported.

## Read-only preflight

Before enabling stores, inspect the actual destination database. From this repository with dependencies installed and `DATABASE_URL` configured, run:

```sh
npm run preflight:postgres
```

The command issues only `SHOW server_version_num`, refuses a missing/malformed identity or a version below 16, and closes its single connection. Connection and query waits are bounded to five seconds. It prints the numeric server version and the tested baseline, never the connection URL. The standalone Postgres integration setup performs the same check before any store tests initialize tables.

For an application using the installed package, use the bounded read-only client check:

```js
import { createPgClient } from 'svelte-adapter-uws-extensions/postgres';

const database = createPgClient({
  connectionString: process.env.DATABASE_URL,
  autoShutdown: false,
  options: { max: 1, connectionTimeoutMillis: 5000, query_timeout: 5000, statement_timeout: 5000 }
});
try {
  const { serverVersionNum } = await database.check();
  console.log(serverVersionNum);
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

Admission happens before entering the driver queue. Excess calls reject with a `ConnectionError` whose `code` is `PG_CAPACITY_EXCEEDED`. Store breakers leave dependency health unchanged for these local rejections and release a held half-open probe; they still count actual driver failures. A query holds admission through checkout and query settlement. `connect()` holds admission only until checkout settles; the caller then owns the lease and must release it. The driver's pool `max` still bounds checked-out connections. Direct calls through `client.pool`, acquired-client queries, and standalone `createClient()` sessions bypass wrapper admission. Owned timeout defaults also apply to standalone sessions.

A supplied `pool` retains its original settings and lifecycle. Its default `maxInFlight` is `0`; opt into admission with `createPgClient({ pool, maxInFlight: 100 })`. Configure `connectionTimeoutMillis`, `query_timeout` and `statement_timeout` on the original `pg.Pool` before passing it in. `options` does not reconfigure that pool, and `end()` remains a no-op. If a connection string is also supplied for `createClient()`, its standalone configuration uses the supplied `options` without adding owned-pool defaults.

### PgBouncer startup parameters

The owned client's default `statement_timeout` is sent as a PostgreSQL startup parameter. PgBouncer rejects unsupported startup parameters in both session and transaction modes, including this one in its default configuration. A plain owned factory can therefore fail with `08P01: unsupported startup parameter: statement_timeout`. See [PgBouncer startup parameter handling](https://www.pgbouncer.org/config.html#ignore_startup_parameters).

For a proxy route, configure a finite execution deadline on the actual backend login role, reconnect existing backend sessions, and omit the factory startup parameter:

```sql
ALTER ROLE application_runtime SET statement_timeout = '30s';
```

```js
const runtime = createPgClient({
  connectionString: process.env.DATABASE_PROXY_URL,
  options: { statement_timeout: 0 }
});
const { rows } = await runtime.query('SHOW statement_timeout');
// Verify the expected server deadline before admitting application traffic.
```

Here `statement_timeout: 0` omits that startup parameter; it does not clear a role/database default. The factory still supplies its 10-second connection deadline, 30-second query-read deadline and 100-operation admission bound. PostgreSQL applies [role defaults at login](https://www.postgresql.org/docs/16/sql-alterrole.html); if PgBouncer forces a backend user, configure that role. Verify `SHOW statement_timeout` through every application route and test a server-cancelled statement. Merely adding `statement_timeout` to PgBouncer's `ignore_startup_parameters` discards it without installing an execution limit; a client read timeout alone does not establish server cancellation.

Use a separate direct/session route for deployment migrations and runtime `autoMigrate: false` through transaction pooling. Notification bridges still require their own direct/session route. A session-mode proxy also needs the startup-parameter adjustment for `createClient()`; supply the corresponding standalone `options` when wrapping an external pool.

Run `npm run test:integration:pgbouncer` for the isolated native proxy regression. It verifies rejection under the default proxy configuration, runtime store readiness/writes with the documented configuration, inherited role deadlines, server cancellation and standalone clients in both modes, plus notification startup, delivery and owned shutdown through the session route. Host ports and the Compose project can be overridden with `INTEGRATION_POSTGRES_HOST_PORT`, `INTEGRATION_PGBOUNCER_SESSION_PORT`, `INTEGRATION_PGBOUNCER_TRANSACTION_PORT` and `INTEGRATION_COMPOSE_PROJECT`.

### Caller cancellation and deadlines

`query(textOrConfig, values, { signal })` and `connect({ signal })` accept an `AbortSignal`. `AbortSignal.timeout(ms)` provides a caller deadline; pass `undefined` as `values` when a query has no positional values. A pre-aborted signal rejects before driver admission. Abort rejects with the signal's original reason and removes its listener.

Cancellation stops the caller's wait. It does not cancel SQL, remove a queued checkout from node-postgres, roll back a transaction, or free admission early. A cancelled query can still execute and commit. Before retrying a write, reconcile its durable result or use an idempotency key. An abandoned checkout is returned exactly once when the driver eventually acquires it. A successfully returned lease remains the caller's responsibility even if the signal later aborts.

The wrapper reserves admission until the actual driver operation settles, including after caller cancellation. A custom external pool that never settles can therefore consume its configured capacity permanently; new operations fail fast at the bound. Keep native pool deadlines enabled to terminate real driver waits. Circuit-breaker wrappers observe timeout or cancellation rejections and can open while abandoned work is still accounted for. Redis clients have their own command and teardown deadline settings; see [Redis connection options](../README.md#redis-client).

### Pool metrics

Pass the existing Prometheus registry as `metrics`. Labels use only the fixed operation names `query` and `connect`, result classes, and rejection reasons; SQL, connection strings and caller identifiers are excluded.

| Metric | Meaning |
| --- | --- |
| `postgres_operations_in_flight{operation}` | Actual unsettled wrapper work, including cancelled caller waits. |
| `postgres_pool_waiters` | Driver checkout queue depth at scrape time; shared pools are counted once per returned gauge. |
| `postgres_pool_connections{state}` | Native connections in fixed `active` and `idle` states, including direct pool checkouts. |
| `postgres_pool_capacity` | Sum of configured pool connection limits. |
| `postgres_operation_duration_seconds{operation,result}` | Underlying settlement latency in seconds; result is `ok`, `error` or `timeout`. |
| `postgres_operation_timeouts_total{operation}` | Recognized driver checkout, query-read and server statement timeout failures. |
| `postgres_admission_rejected_total{operation,reason}` | Capacity rejection or caller cancellation; reason is `capacity` or `aborted`. |

Duration and timeout metrics follow actual settlement, so an aborted caller can leave the in-flight gauge positive without producing a duration observation yet. Driver queue depth also includes direct use of the underlying pool. Metrics are optional; the uninstrumented path does not read a clock or scrape pool state.

Pool gauges count each native pool once per returned gauge, including registry facades such as `{ ...metrics }` and facades that share only some instruments. They omit ended or collected pools, clean up collected registration handles without requiring a scrape, and exclude unavailable fields on custom pool implementations. A checked-out client remains active after the wrapper's connect operation has settled; use these gauges for pool utilization rather than the wrapper in-flight count. Pool names and connection strings are never labels.

Operation durations declare explicit seconds buckets `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]`, independently of registry defaults. The upper bounds cover the default checkout plus query deadlines; durations above 60 seconds remain in `+Inf`. A custom registry bucket cap must allow at least 14 bounds for this instrument.

## Notification sessions and shutdown

`createNotifyBridge()` needs a persistent PostgreSQL session. Use a direct database DSN or a PgBouncer pool configured in **session** mode. Transaction and statement pooling cannot preserve `LISTEN` subscriptions or session-scoped advisory locks. A transaction-pooled DSN may still be suitable for ordinary application queries after separate deployment migrations and with runtime stores using `autoMigrate: false`, but it must not be reused for this bridge. Pass a separate direct/session DSN alongside an external query pool when the two routes differ.

Each active bridge consumes one standalone database session outside `pool.max`. In both `multiListener: 'all'` and `'advisory'`, N replicas with B bridges consume N x B additional sessions. Advisory election reduces active LISTEN consumers to one per lock, while every follower retains a session to attempt the lock. Budget pooled connections, these dedicated sessions, administrative access, migrations and other services against the server/proxy connection limits. Reconnects wait for the old session to close before creating its replacement.

Initialize in the adapter's awaited `init({ platform })` hook, before WebSocket traffic is served. Await the Redis bus first when using advisory mode, then await `bridge.activate(bus.wrap(platform))`. Startup resolves once the dedicated session is established and either LISTEN succeeds or another advisory session owns the lock. A connection or setup failure rejects in both modes, including a socket error emitted before the connect, lock or LISTEN promise settles; a healthy follower is not a startup error. Concurrent activation calls share work. Notifications sent before LISTEN is established or during a disconnection are not durable and cannot be replayed by this bridge.

Owned `createPgClient({ connectionString })` instances stop registered bridges, cancel their retry/election timers, close every tracked `createClient()` session, then close the pool. `await pg.end()` waits for all of this, and repeated calls share the same shutdown. The existing `sveltekit:shutdown` registration invokes the same cleanup unless `autoShutdown: false` is set. Call `await bridge.deactivate()` to stop a bridge independently. Shutdown interrupts startup waits; a late connect or LISTEN result cannot reactivate the bridge. New standalone sessions and bridge activation are refused after owned shutdown begins.

An externally supplied `pool` remains caller-owned: wrapper `end()` is still a no-op, including for its standalone sessions. In your application shutdown hook, await each `bridge.deactivate()` and manually created standalone client's `end()` before awaiting the external `pool.end()`. Do not create a new standalone client only to close it. Pool checkout leases still require the caller's `release()` before a pool can finish closing.

Reconnect and election operations never overlap within one bridge. Failed connections use a jittered delay from `reconnectInterval` up to twice that baseline; `0` requests immediate retries. Both intervals must fit the native timer maximum of `2147483647` ms, and jitter is capped at that maximum. Healthy advisory followers poll at `pollInterval`; a leader checks liveness without repeatedly acquiring its already-held advisory lock. A retry waiting on its jitter timer is not bypassed by election polling. `autoReconnect: false` stops automatic retries after loss; an explicit awaited `activate()` can retry. Keep the owned connection/query deadlines enabled, or provide equivalent finite standalone options when using an external pool, so backend waits have a deadline.

## Schema transitions and readiness

The [schema transition manifest](./postgres-schema.v1.json) describes this
package's 0.6 candidate relative to stable 0.5.10. Retain it with the exact
tarball digest and application lockfile. Applied migrations are recorded in
`svti_schema_migrations` in the configured schema. PostgreSQL 16 is the minimum
server for every store.

| Store | Change from 0.5.10 | Initialization with `autoMigrate: true` | Startup gate |
| --- | --- | --- | --- |
| Replay | Adds event `user_id`, unique identity and retention indexes, and sequence `epoch` with default 0. | First store operation or `ready()`. | Await `store.ready()`. |
| Idempotency | Adds `user_id`, `tenant_id`, `owner_token` and tenant/user index. | Starts at construction. | Await `store.ready()`. |
| Jobs | Adds nullable trace columns, nonnegative attempts and consistent claim timestamps. | First store operation or `ready()`. | Await `queue.ready()`. |
| Tasks | Adds nullable trace columns, `user_id` and user index. | Starts at construction. | Await `runner.ready()` before producers and handlers. |
| Dead letter | New PostgreSQL store in this release series. | First store operation or `ready()`. | Await `store.ready()`. |
| Alarms | New PostgreSQL store in this release series. | Starts at construction. | Await `store.ready()` before scheduling. |

Every durable store exposes `ready()` and `status()`. With automatic migration
enabled, readiness migrates and validates the schema. With `autoMigrate: false`,
readiness issues only catalog reads: required columns, types, nullability,
defaults, primary keys, valid indexes, CHECK expressions, operation privileges
and serial-sequence privileges must match. When a ledger exists, readiness also
verifies its selected table's versions and checksums; grant runtime SELECT on
the ledger. Compatible manually provisioned schemas may have no ledger.
Readiness rejects unknown required columns, CHECK constraints, unique indexes,
foreign keys and exclusion constraints because they can reject valid store writes.
This conservative policy also rejects required extra columns with automatic defaults.
Extra columns must be nullable ordinary columns without defaults, identity or
generated expressions; ordinary nonunique indexes are allowed. Migration
checkpoints permit already installed constraints from this package's later known
versions, so complete manual provisioning and interrupted upgrades remain adoptable.
Ordinary operations with automatic migration disabled
retain their existing behavior; explicitly await readiness before traffic.

`status()` returns `{ state, error }`, where state is `idle`, `checking`, `ready`
or `failed`. Concurrent readiness calls share work. Failed automatic initialization is latched for ordinary operations, including retention ticks; they reject the stored error without repeating DDL. After repairing the cause, explicitly retry `ready()` or recreate the store.
Successful readiness is cached for that store instance. Use
`validateSchema(client, { store, table })` for a fresh read-only validation, with
a ten-second bound per catalog operation. This is a point-in-time check, not a
continuous availability guarantee.
It checks the declared schema and direct privileges, not deployment-specific
trigger behavior or row-level security policies.

Use `client.check({ timeoutMs: 5000 })` for a bounded server preflight returning
`{ serverVersionNum }`. `postgresReadiness(client, stores)` checks the server,
then awaits every store and reports all store failures in an `AggregateError`.
Notification bridges retain their separate awaited `activate()` gate for their
dedicated session and LISTEN/election.

### Separate deployment and runtime roles

Run migrations with an owner/deployment role before starting runtime stores:

```js
import { createPgClient, migrate, postgresReadiness } from 'svelte-adapter-uws-extensions/postgres';
import { createJobQueue } from 'svelte-adapter-uws-extensions/postgres/jobs';

const deployment = createPgClient({ connectionString: process.env.DATABASE_MIGRATION_URL });
try {
  await migrate(deployment, { store: 'jobs', table: 'svti_jobs' });
} finally {
  await deployment.end();
}
const runtime = createPgClient({ connectionString: process.env.DATABASE_URL });
const jobs = createJobQueue(runtime, { autoMigrate: false });
await postgresReadiness(runtime, [jobs]);
```

`store` is one of `replay`, `idempotency`, `jobs`, `tasks`, `dead-letter` or
`alarm-store`; `table` defaults to the documented store table. Keep the same
table names and `search_path` in deployment and runtime. Grant the runtime role
schema USAGE and required table DML, plus USAGE or UPDATE on serial sequences;
grant SELECT on an existing migration ledger, without DDL ownership. Use a direct/session-pooled
connection for migrations because migration coordination uses a session lock.
This requirement also applies to default `autoMigrate: true` initialization,
which uses the same migration path and session timeout settings. Custom clients
must expose `connect()` with a dedicated `query()` / `release(true)` checkout;
a query-only client is insufficient for automatic initialization.

### Runtime grants

The deployment role owns the schema, store tables, indexes and migration ledger.
Use a distinct login for runtime, grant schema `USAGE` without `CREATE`, and
start stores with `autoMigrate: false`. Readiness requires these privileges:

| Relation (default name) | Runtime table privileges |
| --- | --- |
| Replay events `svti_replay` and counters `svti_replay_seq` | SELECT, INSERT, UPDATE, DELETE |
| Jobs `svti_jobs` | SELECT, INSERT, UPDATE, DELETE |
| Tasks `svti_tasks` | SELECT, INSERT, UPDATE, DELETE |
| Idempotency `svti_idempotency` | SELECT, INSERT, UPDATE, DELETE |
| Dead letters `svti_dead_letter` | SELECT, INSERT, UPDATE, DELETE |
| Alarm schedules `svti_alarms` | SELECT, INSERT, UPDATE, DELETE |
| Alarm authority `svti_alarms_generations` | SELECT, INSERT, UPDATE |
| Migration ledger `svti_schema_migrations` | SELECT |

Grant `USAGE` (or `UPDATE`) on the owned serial sequences for replay, jobs and
dead letters. Substitute configured names and grant only the stores in use.
Replay and dead-letter cleanup select rows with `FOR UPDATE SKIP LOCKED`, so
`UPDATE` is required even though their public event operations insert and delete
rather than updating stored payloads. Test readiness and maintenance through the
actual runtime login: testing with the owner or superuser can conceal missing
grants. Trigger behavior and row-level policies still need application tests.

### Versioned migration behavior

`postgresMigrations({ store, table })` returns the versioned SQL and SHA-256
checksums. Version 1 establishes the existing baseline and additive columns;
version 2 adds constraints and replay uniqueness. Alarm-store version 3 adds
[ordered generation authority](./alarm-generations.md); other stores remain at
version 2. Idempotency states are
`pending`/`committed`; task states are `pending`/`running`/`committed`/`failed`.
Jobs and tasks require nonnegative attempts. Job claim timestamps must both be
null or both present with expiry at or after claim time. Replay's unique
`(topic, seq)` index replaces its redundant nonunique index. Invalid existing
rows or duplicate identities require deliberate operator reconciliation;
migrations never delete or rewrite them to make validation succeed.

The ledger columns are `table_name`, `store`, `version`, `checksum` and
`applied_at`, with primary key `(table_name, version)`. A replay ledger entry
also covers its `_seq` companion; an alarm entry covers `_generations`.
Migrations are serialized per database/schema
with bounded advisory-lock polling. Each version is recorded only after its
schema validates; checksum drift, gaps, future versions and missing tables
after an applied migration fail closed. Restore the ledger with its stores.
Validation uses schema names and catalogs, without persisting relation OIDs.

DDL uses separate autocommit statements, allowing `CREATE INDEX CONCURRENTLY`.
CHECK constraints are added `NOT VALID` before validation. Interrupted work can
be retried; an invalid concurrent index is rebuilt only when its name, owning
table and shape match the expected index. Conflicting objects are refused.
Earlier successful statements can remain after a later failure; migration is
not an all-or-nothing transaction.

Every migration uses a dedicated checkout, with a ten-second checkout deadline,
a default five-second lock deadline and a default thirty-second statement
deadline. Override `lockTimeoutMs` and `statementTimeoutMs` with positive finite
integer millisecond values up to `2147482647`; migration bounds cannot be disabled.
Driver query deadlines allow one additional second for server errors. The
checkout is destroyed on success or failure so session locks and settings
cannot leak into runtime queries. Busy tables can make a bounded migration fail;
rehearse DDL on a restored copy and schedule a suitable deployment window.
Large tables should use explicit deployment migrations with measured bounds,
then runtime `autoMigrate: false`. Automatic initialization uses the migration
defaults above even if the pool's query or statement timeout is longer.

### Existing replay and idempotency tables

Alongside the [jobs/tasks migration](../MIGRATION.md#jobs-and-tasks-tables-gain-trace-columns-tasks-gain-erasure-identity),
these additions show the historical nullable-column transition. Use `migrate()`
for the complete schema, constraints and indexes. Names below are the
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
2. Inject a missing-column or denied-DDL failure. Await every store's `ready()`
   gate and keep application readiness false on rejection.
3. Inventory committed DDL: automatic initialization issues separate statements,
   so a later failure can leave earlier additions in place. Do not assume an
   all-or-nothing migration or drop a data-bearing table because a drift error
   mentions a conflicting object. Correct the schema/privileges deliberately.
4. Correct the cause, retry `migrate()` and re-run the startup gates. Failed
   `ready()` calls are retryable. Validate old records and a new operation before admitting
   traffic; inspect role, index and storage errors rather than overriding them.
5. If validation fails, stop the candidate and follow the downgrade boundaries
   above. Retain the error, artifact/lockfile digests, applied statements and
   recovery decision as incident evidence; exclude credentials and payload data.

The live `test/integration/postgres/migration-readiness.test.js` regression covers
old replay/idempotency schemas and the failed-schema/disabled-migration boundary.
The [packed rehearsal](../MIGRATION.md#executable-rehearsal) covers both automatic
and documented manual jobs/tasks upgrades. Run both on the actual PostgreSQL
version before deployment; neither substitutes for a host restore drill.

For backup scope, coupled restore groups and the read-only verification command,
see [Storage recovery](./recovery.md).
