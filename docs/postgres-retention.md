# PostgreSQL retention cleanup

[Package overview](../README.md) | [Public entry points](public-entries.md)

Replay, idempotency, task and dead-letter stores run bounded background maintenance after
schema initialization. `cleanupInterval` is the maximum jitter delay in
milliseconds between settled passes, including the first pass. Each delay is
chosen independently between 1 ms and that maximum; `0` disables the timer.
When a pass exhausts its row, query or time budget, the next timer uses at most
100 ms of jitter instead of the normal interval. This continues until a pass
finishes, errors or loses ownership, allowing a backlog to drain at the database's
measured maintenance capacity. Use an integer no larger than 2147483647. Call the store's `destroy()` when its
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

Dead-letter writes use the same bounded maintenance as idle passes. Concurrent
writes share local cleanup ownership. Writes arriving during a leading pass
queue at most one trailing bounded pass, without extending the original write's
wait or adding recurring timers. `destroy()` cancels a queued trailing pass.
TTL selection uses `failed_at`; size cleanup retains the newest `max`
ids. The two policies alternate priority. `get`, `count`, `list` and `summary`
exclude expired rows using database time, including when physical cleanup has
not finished. The idle timer runs even when TTL is disabled so an oversized
queue can drain after writes stop. An acknowledged INSERT keeps its returned
id when cleanup fails, times out or loses ownership to another replica.
With `cleanupInterval: 0`, residual backlog needs another write to trigger
cleanup, including writes that arrive during the trailing pass. `max` is a
retention target, not a strict post-write physical row count.

Bounded transactions add round trips to isolated dead-letter captures. A local
PostgreSQL 16 Docker benchmark on 2026-10-10 used a 100-record target, a one-hour
TTL and five alternating samples of 250 captures. Median elapsed time per
sequential capture was 2.40 ms with unbounded deletes and 6.50 ms with bounded
transactions. At eight concurrent captures, elapsed time per capture improved
from 0.429 ms to 0.368 ms through coalescing; median sample p95 latency improved
from 3.69 ms to 2.12 ms. This trades 4.1 ms of sequential capture latency for
bounded housekeeping on the failed-delivery path; it is not a latency-neutral
change or a production capacity estimate.

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

`store` is one of `replay`, `idempotency`, `tasks` or `dead-letter`; table names and payloads are
never metric labels. A lost COMMIT reply can leave an unknown outcome, so the row
counter is an acknowledged-deletion count. Investigate repeated errors, sustained
budget exhaustion and growing table size together. Contention alone is expected
when replicas share a table; `waiting` means this client already has a pending
maintenance checkout. Neither waiting nor stopped passes reset breaker health. Query `pg_stat_user_tables`, index validity, database
logs and replication lag before reducing cleanup intervals; tuning the timer does
not repair missing indexes or insufficient database capacity.

The cleanup duration histogram uses seconds with buckets at 0.001, 0.005, 0.01,
0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30 and 60, plus infinity. Duration
includes schema readiness and checkout, so slow initialization remains visible
above the normal one-second maintenance budget.
