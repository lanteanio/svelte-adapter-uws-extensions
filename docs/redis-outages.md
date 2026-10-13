# Redis outage policy

[Package overview](../README.md) | [Public entry points](public-entries.md)

For replacing credentials and subscriber connections with a tested rollback path,
see [Database credential rotation](./credential-rotation.md).

`createRedisClient()` bounds command waits by default while allowing calls made
immediately after construction to wait for initial connection setup.

| Setting | Default | Effect |
|---|---|---|
| `maxRetriesPerRequest` | `1` | Reject pending work after one reconnect retry. |
| `commandTimeout` | `5000` ms | Reject a command whose reply has not arrived. |
| `connectTimeout` | `5000` ms | Bound a TCP connection attempt. |
| `socketTimeout` | `5000` ms | Close a socket that stops returning expected data. |
| `autoResendUnfulfilledCommands` | `false` | Reject sent commands on connection loss instead of replaying uncertain writes. |
| Offline command admission | At most `4096` per connection | Reject excess admission with `ConnectionError`. |
| Main connection after first ready | No offline buffering | New commands fail promptly while disconnected. |

The queue cap also applies during initial setup and to subscriber duplicates.
A rejected command that is still in the offline queue is removed; if it belongs
to a pipeline, every still-unsent member is rejected together. Startup writes
whose deadlines expire are not sent after recovery. When a sent command times
out, the client fails all work pending on that connection and retires its socket
before reconnecting. This preserves reply ordering and prevents rejected writes
or blocking WAIT commands from being resent. A timeout cannot prove that Redis
did not execute a command already sent;
use idempotency or fencing for effects whose repetition would be unsafe.

A connection reset, including a socket deadline, rejects unanswered sent commands
with `ConnectionError` before reconnect. This also applies when `commandTimeout`
exceeds `socketTimeout`: the socket deadline wins without resending a write.
Prefer `commandTimeout <= socketTimeout` so normal deadline failures carry the
command's `TimeoutError`. Explicit `autoResendUnfulfilledCommands: true` restores
ioredis replay behavior and can repeat writes whose reply was lost.

Failures reject the calling promise through the existing ioredis errors,
extensions `TimeoutError` for shared command deadlines, or `ConnectionError` for
queue-cap admission. Deadline errors expose `code: 'OPERATION_TIMEOUT'`,
`service: 'redis'` and the configured deadline as `ms`. This gives a configured breaker and
`localFloorOnStorageFailure` rate limiter a failure to handle. The floor remains
an explicit application choice; its allowance is per process. Store durable work
in the task or outbox store instead of using an offline connection queue as a
reliable delivery mechanism.

`client.duplicate()` is intended for subscriber connections. It keeps bounded
offline buffering across reconnects, with the same finite deadlines. The bundled
subscriber owner restores membership after reconnect. Raw `client.redis.duplicate()`
inherits the underlying connection's current options; use the wrapper's duplicate
method for this subscriber policy.

Existing `options` and duplicate overrides select a workload-specific policy:

```js
const commandClient = createRedisClient({
  options: { commandTimeout: 2000, socketTimeout: 2000 }
});

// Dedicated blocking consumer: explicitly accept waiting across reconnects.
const blocking = commandClient.duplicate({
  enableOfflineQueue: true,
  maxRetriesPerRequest: null,
  commandTimeout: undefined,
  socketTimeout: undefined
});
```

Explicit `options.enableOfflineQueue: true` also keeps bounded buffering on the
main connection after its first ready event. Explicit `false` disables buffering
from construction, so callers must wait for `ready` before sending normal commands.
The infinite retry and disabled deadline settings above are opt-ins for dedicated
blocking workloads; do not share that connection with request-path rate limits,
locks, or writes that need a bounded response. Replay `replicationTimeoutMs`
sets the Redis WAIT timeout, while command and socket deadlines still apply.
Setting it to `0` disables only the Redis-side WAIT timeout, not client deadlines.
Choose explicit connection deadlines longer than the required replication wait.
Externally constructed Redis or Cluster clients retain their own policies.

Independent multi-slot work shares a limit of eight in-flight pipelines or routed
commands per client, with at most four assigned to one known Cluster owner.
Pipelines contain at most 256 commands. Ready work for other owners can use free
slots while one owner stalls; each failed batch returns error tuples alongside
successful results. Cluster work groups known keys by their current owner allocation and retries only explicit MOVED/ASK
refusals; transport failures do not replay ambiguous writes. Missing topology
uses bounded individual routing.

Presence and registry TTL heartbeats, cursor cleanup sweeps, and periodic
cursor snapshot writes run at most one refresh per Cluster owner at a time (one
in total on a standalone server). Owners that answer keep refreshing on every
tick while another owner stalls. A tick that finds an owner still busy leaves
it only the newest work, which runs once that owner's refresh settles, with
current membership or the latest pending cursor values; that waiting work is
skipped while the circuit breaker is not healthy. A cursor topic is written
again only after its previous snapshot write answered, so a write still on its
way to a node the slot has left never lands after a newer one.

Heartbeats and snapshot writes report one success or failure to the circuit
breaker per tick, from the owners that tick reached, and one per waiting
refresh when it settles. A tick that reaches no owner reports neither and does
not use up a breaker probe.

Externally constructed Cluster clients retain their caller-selected deadlines;
configure finite command and socket deadlines on their node connections.
Capacity stays charged until the underlying operation settles. Presence
heartbeat latency includes Redis completion; while a refresh from an earlier
tick is still outstanding, it reports the age of the oldest one, so a stalled
owner shows as a growing value. Keep the heartbeat-to-TTL margin large enough
for the complete refresh at your deployment's cardinality.

For backup scope, coupled restore groups and the read-only verification command,
see [Storage recovery](./recovery.md).
