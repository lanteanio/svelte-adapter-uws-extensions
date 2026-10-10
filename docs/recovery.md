# Storage recovery

[Package overview](../README.md) | [Public entry points](public-entries.md)

Use the [persistence manifest](./persistence.v1.json) from the same installed
package as the application. Its schema version describes the inventory format;
the enclosing package version identifies the implementation. The verifier reports
both the package version and the manifest's SHA-256 digest. The
[PostgreSQL schema guide](./postgres-support.md#schema-transitions-and-readiness)
describes upgrade and initialization behavior.

## Restore scope

| State | Restore boundary | Before traffic resumes |
| --- | --- | --- |
| PostgreSQL migration ledger | svti_schema_migrations in each store schema, together with all covered tables | Check version/checksum compatibility through store readiness. |
| PostgreSQL replay | Event rows, sequence/epoch rows, serial sequence and indexes | Confirm retained events never exceed their topic sequence. |
| Redis replay | Sorted-set or stream buffer, sequence, epoch and producer idempotency hashes for each topic | Preserve epochs even when buffers have expired. An absent legacy epoch means zero. |
| Jobs and tasks | Complete rows, serial sequences where used, ownership/fence state, and any application result cache | Stop old workers and reconcile effects that completed outside the database. |
| Idempotency and sessions | Results/session records, expiry and erasure indexes | Reapply erasures and revocations newer than the backup; reconcile pending owners. |
| Alarms | Schedule, metadata and all retained generation authority; PostgreSQL also needs its migration ledger | Preserve watermarks for deleted and pending topics; reconcile executed callbacks before enabling due work. |
| Dead letters | Records, ordered index, ID authority and erasure tombstones | Reapply later erasures before redelivery. |
| Presence, routing, cursors and leases | Rebuild from current authenticated connections and processes | Isolate old owners and reconnect clients. Restoring bytes does not restore a socket. |
| Groups, smoothing and admission controls | Apply the manifest's family-specific reconciliation policy | Preserve closure decisions and expiry; establish current ownership and policy. |

A database snapshot is not a transaction with Redis, a webhook endpoint or an
application document store. Establish an application recovery barrier when those
systems jointly own an outcome. Redis CRDT persistence leases do not contain the
document; include the application's snapshot callback destination separately.
Redis Pub/Sub notifications are transient and are not a recovery archive.

## Create an artifact and rehearse it

For PostgreSQL, take a whole-database custom archive with the matching server
tools, then restore into an empty isolated database. Keep roles and other
cluster-global configuration in the host's separate configuration backup.
A custom archive preserves schema and sequence state; selecting only individual
table data can break the restore groups above. See the official
[pg_dump documentation](https://www.postgresql.org/docs/16/app-pgdump.html).

Keep the migration ledger with its store tables. An applied ledger whose covered
table is missing fails startup instead of recreating empty storage. Selective
restores must preserve compatible ledger rows; a schema installed without a ledger
can be checked with validateSchema or adopted through explicit migrate. The recovery
verifier checks ledger structure and row count; store readiness checks its versions
and checksums before serving traffic.

The following commands assume the operator configured PostgreSQL connection
environment variables for the source, then switched them to an empty recovery
database before the second command:

~~~text
pg_dump --format=custom --file=application.dump
pg_restore --exit-on-error --dbname=recovery_database application.dump
~~~

Review the restore log and ownership/privilege configuration before admitting
traffic. The [pg_restore reference](https://www.postgresql.org/docs/16/app-pgrestore.html)
describes archive selection and restore options.

For Redis, use the deployment's supported RDB/AOF or managed backup procedure
and include every Cluster primary in the coordinated restore. Preserve absolute
expiry deadlines. Coordinate writers across masters for coupled state; independently
copied snapshots do not establish a global application cut. Restore into isolated
servers with the same required features before connecting application workers.
See [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).

Use the [artifact record template](./recovery-artifact.v1.json). Retain the package tarball and lockfile, persistence manifest, private scope
configuration, backend versions, artifact digests, backup cut/time, restore logs,
verification report and application replay/erasure checks together. Backups
contain payloads and authentication material even though verifier output does not.
Apply the host's encryption, access and retention policy to the artifacts.

The repository's native recovery tests create a real PostgreSQL custom archive
and a bounded Redis DUMP/RESTORE fixture with absolute expiries. They restore into
isolated destinations and deliberately remove replay authority and alarm metadata.
Local artifacts and sanitized reports are written under source/recovery-drill.
The Redis fixture is a controlled stopped-writer test, not a live-cluster backup
implementation.

## Run the read-only verifier

Save a private JSON scope file describing the stores actually deployed. Omit a
backend that is not used. PostgreSQL names may be schema-qualified; all components
must be lowercase SQL identifiers. Replay requires both event and sequence tables;
alarms require both `alarms` and `alarm-generations` tables.
Redis keyPrefix is the client prefix. The optional prefixes map replaces each
family's store-level prefix, including empty strings.

~~~json
{
  "schemaVersion": 1,
  "timeoutMs": 60000,
  "postgres": {
    "stores": {
      "schema-migrations": "public.svti_schema_migrations",
      "replay": "public.svti_replay",
      "replay-sequence": "public.svti_replay_seq",
      "tasks": "public.svti_tasks",
      "idempotency": "public.svti_idempotency"
    }
  },
  "redis": {
    "keyPrefix": "application:",
    "families": ["replay", "alarms", "sessions"],
    "prefixes": { "sessions": "auth:" },
    "minimumKeys": { "replay": 2, "alarms": 5 },
    "maxKeys": 10000,
    "maxScanCalls": 10000,
    "maxMembers": 10000,
    "cluster": false
  }
}
~~~

Set RECOVERY_POSTGRES_URL and/or RECOVERY_REDIS_URL through the host's secret
environment, then run the installed tool:

~~~text
node node_modules/svelte-adapter-uws-extensions/docs/tools/recovery-verify.mjs recovery-scope.json
~~~

Use a PostgreSQL role with SELECT on the selected tables and sequences, and a
Redis ACL permitting the listed read commands. The PostgreSQL connection also
enforces default_transaction_read_only and uses a READ ONLY transaction.
The tool never constructs stores, migrates schema, refreshes TTLs, claims work
or repairs data. Its connections have finite command and connection deadlines.
Redis Cluster mode starts from the configured seed and scans every discovered
primary; advertised endpoints must be reachable.

PostgreSQL checks table presence, column types, valid index leading columns,
serial counters, replay sequence authority and alarm generation authority using
bigint-safe comparisons.
It takes one repeatable-read snapshot. It does not certify every constraint,
partial-index predicate, row payload or external side effect.

Redis checks selected family key types, optional minimum key counts, replay
sequence authority, exact membership pairing for alarms and dead letters, and
canonical alarm generation watermarks and current membership. Retained generation
hashes must not expire, even after all schedules are deleted. Legacy alarms without
generation authority fail this package version's verification; use the matching
pre-upgrade manifest when restoring an older artifact.
It reads SCAN, TYPE, PTTL, GET, ZREVRANGE, XREVRANGE, HLEN, ZCARD, HKEYS, HMGET and
ZRANGE; Cluster mode also reads CLUSTER SLOTS. Driver handshake commands are
required too. It may read individual stored replay entries to obtain sequence
metadata, but never includes them in the report.

Run against isolated restored state with writers stopped. Redis SCAN is not a
snapshot. A topology change or disappearing key makes verification incomplete;
stable topology alone cannot prove that writers were stopped. Per-field expiry
is preserved by the backend's backup format, but the verifier's expiry summary
counts whole-key TTLs only. Do not use overlapping prefixes for durable sessions
and connection registry records.

## Interpret the report

Exit 0 means every selected check completed and passed. Exit 1 means at least
one failed or could not complete. complete: false and a bounded reason such as
key_limit_reached, member_limit_reached, topology_changed or backend_read_failed
never certify recovery. Raise a limit deliberately, resolve the reported
condition, and rerun against the same isolated restore.

The key cap applies to distinct keys scanned under the client prefix, including
unselected families. SCAN COUNT is a hint, so a single backend response can exceed
that count; retained key metadata is capped. Member checks are capped separately.
Unselected and application-owned keys are outside the checks. Missing expected
records cannot be inferred from an empty backup; set minimumKeys from the
backup inventory and compare PostgreSQL row counts with its recorded baseline.

Reports contain fixed family/check names, counts, completion state, package
identity and manifest digest. They omit URLs, raw backend errors, configured
table names, key names, topics and payloads. The report is evidence about the
selected storage invariants, not a certificate of complete application recovery.

Measure RPO from the newest restored committed application checkpoint relative
to the incident cut. Measure RTO through restored storage, reconciliation,
authenticated client reconnect and successful application work. Record both in
the host drill; repository fixture timings are not host recovery objectives.
