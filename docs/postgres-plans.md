# PostgreSQL query and maintenance evidence

[Package overview](../README.md) | [Public entry points](public-entries.md)

Run the repository collector against an isolated PostgreSQL 16 or newer database
on the intended deployment class. It creates a private schema, initializes the
real stores, seeds deterministic skew and update churn, exercises their queries,
and removes its schema on success or failure. Existing application tables are
outside its search path. Connection URL session options are replaced by the
collector's private search path and bounded deadlines. Authentication and TLS
settings remain those of the supplied URL. It needs permission to create and drop its own schema.
Do not run it inside an application process: maintenance collection temporarily
uses the package's deterministic timer seam.

## Collect an artifact

Configure the isolated server with shared_preload_libraries=pg_stat_statements
and have its administrator run CREATE EXTENSION pg_stat_statements in the public
schema. The collector reads that extension; it never installs it or resets shared
statistics. The repository's disposable PostgreSQL integration service preloads
it and the native test installs it in the test database.

Set DATABASE_URL through the host's secret environment, then run from the repository:

~~~text
node scripts/collect-postgres-plans.js
~~~

PG_PLAN_ROWS selects 1,000 through 250,000 rows per data-bearing store; the default
is 20,000. The sequence table has one row per fixture topic. PG_PLAN_OUTPUT selects
a new JSON artifact path; the default is source/postgres-plans-<timestamp>.json.
Existing artifacts are never overwritten. PG_PLAN_REQUIRE_STATS=0 explicitly
permits collection without statement statistics and records that absence.

Each statement has a five-second server deadline and a six-second driver deadline.
Collection checks a sixty-second budget between operations, EXPLAIN statements
and vacuum calls. An already submitted statement may use its remaining deadline;
schema teardown runs afterward. Increase cardinality within the documented range
only on a host that can complete this bounded collection. A failed run creates no
success artifact. The CLI omits raw database errors and connection information.

The native test is executable with the repository integration configuration:

~~~text
npm exec vitest run -- --config vitest.config.integration.postgres.js test/integration/postgres/plan-evidence.test.js --no-cache
~~~

## Read the evidence

The artifact includes package identity and lockfile, collector and source digests,
server identity, relevant planner
and memory settings, fixture shape, and these observations:

- Actual EXPLAIN ANALYZE plans with row estimates, actual rows, loops, buffer I/O,
  temporary blocks and WAL. Structural output omits predicates and bound values;
  planning and execution times remain separate numeric observations.
- pg_stat_statements for those exact queries against the newly created relations.
  Full signed 64-bit query identifiers are preserved while selecting records.
  Source SQL fingerprints join records to plans; unrelated server statements and
  raw SQL are excluded. Counters include the fixture's operation executions, not
  a production traffic interval, and multiple operation labels can share one query.
- Table row/dead-tuple estimates before and after explicit VACUUM ANALYZE, vacuum
  duration, relation sizes and transaction-ID age. Estimates can already be zero
  after page pruning or lag completed vacuum work while backend statistics are flushed;
  relation size is not a measurement of reclaimable bloat.

Replay has a 90% hot topic; jobs, tasks and dead letters have a 95% hot name/topic.
Tasks include pending, running, committed and failed rows. Expired idempotency
entries, due alarms, old terminal tasks and replay retention exercise maintenance.
Every third job and every running task is updated. Relative age/distribution is
repeatable; absolute timestamps and generated ownership tokens are fresh.

Captured statements come from real publish/read/claim/extend/complete/fail,
task execution/retry/recovery, idempotency, alarm, dead-letter and retention paths.
The fixture is reseeded before EXPLAIN so previous cleanup does not shrink later
plans. Each EXPLAIN runs in a rolled-back transaction and re-plans captured SQL;
it does not reuse a connection's warmed prepared-plan cache. Point mutations whose
captured fresh ID no longer exists still measure lookup planning, rather than a
successful update's WAL or execution cost. Plans and aggregate statement counters
must be interpreted together.

Compare the same operation and SQL digest on the same server settings and fixture
cardinality. Investigate large estimate errors, unexpected scans on selective
lookups, disk spills, rising buffer reads, or maintenance duration. A sequential
scan on a tiny table or a whole-table aggregate can be the correct plan; the
regression test pins index use only for selective replay, idempotency and alarm
lookups. It also checks owned-schema cleanup, unchanged preexisting tables and
explicit behavior when statement statistics are unavailable.

## Qualify the deployment

This artifact is a bounded synthetic workload. It does not establish production
throughput, lock contention under concurrent workers, autovacuum pace, storage
bloat, or wraparound safety. Use an isolated restored clone with the deployment's
actual cardinality, tenant skew, payload sizes, indexes, role and server settings
for the host's acceptance run. Keep application writers isolated from production.

Replay the expected sustained and peak traffic on that clone, retain normalized
plans for the same SQL fingerprints, and collect pg_stat_statements deltas over
the measured interval without resetting another workload's counters. Warm the
application's named prepared statements and compare their generic/custom plan
behavior after repeated executions. Record
concurrency, queue age, tail latency, lock waits, temporary I/O, maintenance rows
per pass and backlog convergence. Compare dead tuples, relation growth, vacuum
completion and transaction-ID age over multiple retention cycles. Apply the
host's latency, storage and transaction-age thresholds before admitting traffic.

Keep the candidate tarball/lockfile digest, collector artifact and host workload
record together. [Retention ownership and budgets](./postgres-retention.md) and
[PostgreSQL support and schema readiness](./postgres-support.md) describe the
runtime guarantees that these measurements exercise.
