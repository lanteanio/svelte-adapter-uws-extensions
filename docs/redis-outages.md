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
| Offline command admission | At most `4096` per connection | Reject excess admission with `ConnectionError`. |
| Main connection after first ready | No offline buffering | New commands fail promptly while disconnected. |

The queue cap also applies during initial setup and to subscriber duplicates.
A rejected command that is still in the offline queue is removed, so a timed-out
startup write is not sent when Redis eventually becomes ready. Sent commands
remain in their protocol queue until their replies arrive or the connection
closes. A timeout cannot prove that Redis did not execute a command already sent;
use idempotency or fencing for effects whose repetition would be unsafe.

Failures reject the calling promise through the existing ioredis errors,
extensions `TimeoutError` for shared command deadlines, or `ConnectionError` for
queue-cap admission. This gives a configured breaker and
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
locks, or writes that need a bounded response.

Independent multi-slot work shares a limit of eight in-flight pipelines or routed
commands per client, with at most four assigned to one known Cluster owner.
Pipelines contain at most 256 commands. Ready work for other owners can use free
slots while one owner stalls; each failed batch returns error tuples alongside
successful results. Cluster work groups known keys by their current owner allocation and retries only explicit MOVED/ASK
refusals; transport failures do not replay ambiguous writes. Missing topology
uses bounded individual routing. Presence and registry TTL heartbeats, cursor
cleanup sweeps, and periodic cursor snapshot writes each run one refresh at a time, coalescing missed ticks into one
rerun with current membership or the latest pending cursor values. Externally
constructed Cluster clients retain their caller-selected deadlines; configure
finite command and socket deadlines on their node connections. Capacity stays
charged until the underlying operation settles.
Presence heartbeat latency includes Redis completion. Keep the heartbeat-to-TTL
margin large enough for the complete refresh at your deployment's cardinality.

For backup scope, coupled restore groups and the read-only verification command,
see [Storage recovery](./recovery.md).
