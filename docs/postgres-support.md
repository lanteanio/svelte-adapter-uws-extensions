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
