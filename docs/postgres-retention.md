# PostgreSQL retention cleanup

Replay, idempotency and task stores run bounded background maintenance after
schema initialization. `cleanupInterval` is the maximum jitter delay in
milliseconds between settled passes, including the first pass. Each delay is
chosen independently between 1 ms and that maximum; `0` disables the timer.
Use an integer no larger than 2147483647. Call the store's `destroy()` when its
owner shuts down. Destroy prevents later passes; an active pass stops between bounded maintenance
operations. An already-started operation (deadline setup and its statement) may
finish before its checkout closes.

Each pass obtains one pool checkout and a transaction-scoped advisory lock for
the resolved table OID. Replicas that cannot acquire ownership skip that pass.
The lock lasts through COMMIT and cannot leak onto a returned pooled connection.
Different tables have independent database locks. While a checkout is pending,
other maintenance on the same client skips admission; an overdue checkout stays
owned until its underlying acquisition settles and is then released unused.

The fixed limits are 250 rows per DELETE, 1000 rows per pass, and at most 128 work
queries. Checkout has a 1-second caller deadline. After acquisition, maintenance
has a 900 ms work budget with 100 ms reserved for commit. Transaction-local
statement deadlines shrink with the remaining budget; lock waits are capped at
100 ms. A failed or uncertain checkout is destroyed so the database rolls back
its transaction and releases ownership. Initialization DDL happens before these
maintenance limits and follows the client's own query deadlines; provision the
schema separately when startup needs a deployment-controlled migration window.

Deletes select indexed candidate rows with `FOR UPDATE SKIP LOCKED`. Idempotency
uses `expires_at`; task cleanup uses the partial terminal-row `updated_at` index
and never deletes pending or running tasks. Replay TTL selection uses
`created_at`. Replay size cleanup walks at most 32 sequence-table topics per
pass and resumes from an indexed topic cursor. The cursor advances even when a
busy topic consumes the row budget, so other topics still progress. TTL and size
cleanup alternate priority. Larger backlogs drain over several passes; cleanup
is not a hard physical-storage cap or an exact expiry alarm. Replay publish-time
trimming and logical read-expiry checks keep their existing behavior.

With automatic migration disabled, add the replay TTL index to the configured
replay table before enabling maintenance (the default name is shown):

```sql
CREATE INDEX IF NOT EXISTS idx_svti_replay_created_at ON svti_replay (created_at);
```

Pass the same metrics registry through each store's existing `metrics` option:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `postgres_cleanup_runs_total` | counter | `store`, `result` | Passes grouped by `complete`, `budget`, `contended`, `waiting`, `stopped` or `error`. |
| `postgres_cleanup_rows_total` | counter | `store` | Deleted rows after acknowledged COMMIT. |
| `postgres_cleanup_duration_seconds` | histogram | `store` | Whole maintenance tick duration, including initialization and checkout. |

`store` is one of `replay`, `idempotency` or `tasks`; table names and payloads are
never metric labels. A lost COMMIT reply can leave an unknown outcome, so the row
counter is an acknowledged-deletion count. Investigate repeated errors, sustained
budget exhaustion and growing table size together. Contention alone is expected
when replicas share a table; `waiting` means this client already has a pending
maintenance checkout. Neither waiting nor stopped passes reset breaker health. Query `pg_stat_user_tables`, index validity, database
logs and replication lag before reducing cleanup intervals; tuning the timer does
not repair missing indexes or insufficient database capacity.
