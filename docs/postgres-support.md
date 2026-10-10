# PostgreSQL support

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
| `postgres_operation_duration_seconds{operation,result}` | Underlying settlement latency in seconds; result is `ok`, `error` or `timeout`. |
| `postgres_operation_timeouts_total{operation}` | Recognized driver checkout, query-read and server statement timeout failures. |
| `postgres_admission_rejected_total{operation,reason}` | Capacity rejection or caller cancellation; reason is `capacity` or `aborted`. |

Duration and timeout metrics follow actual settlement, so an aborted caller can leave the in-flight gauge positive without producing a duration observation yet. Driver queue depth also includes direct use of the underlying pool. Metrics are optional; the uninstrumented path does not read a clock or scrape pool state.
