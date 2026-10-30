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
with `ConnectionError` before reconnect. A drop during the startup handshake
rejects only the sent commands that are not part of the handshake: ioredis
writes commands valid while Redis loads, such as `PUBLISH` or `SUBSCRIBE`, and
application commands named like handshake commands (`INFO`, `SELECT`, `CLIENT`,
`AUTH`) before ready, so they may have run. Other commands issued during
startup were never sent and stay in the offline queue until the next
connection is ready. The handshake commands ioredis issues itself (`AUTH`,
`SELECT`, `CLIENT SETNAME`, `CLIENT SETINFO LIB-NAME` and the `INFO` ready
check) are retried by ioredis. Its `CLIENT SETINFO LIB-VER` is sent only once
ioredis has read its package version, possibly after application commands,
so it is rejected with them; ioredis ignores that outcome. This rejection
applies even with `autoResendUnfulfilledCommands: true`, because ioredis does
not resend commands from a connection that never reached ready. The reset rule
also applies when `commandTimeout`
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
current membership or the latest pending cursor values; waiting heartbeat and
snapshot work is skipped while the circuit breaker is not healthy (cleanup
sweeps do not consult the breaker). A cursor topic is written again only after
its previous snapshot write answered or failed, so a write still on its way to
a node the slot has left never lands after a newer one.

Heartbeats and snapshot writes report one success or failure to the circuit
breaker per tick, from the owners that tick reached, and one per waiting
refresh that runs, when it settles. Each presence and registry heartbeat tick
and each snapshot flush takes breaker admission, so a half-open breaker is
probed by that work itself; a tick that reaches no owner reports neither and
returns the probe it was admitted with.

Externally constructed Cluster clients retain their caller-selected deadlines;
configure finite command and socket deadlines on their node connections.
Capacity stays charged until the underlying operation settles. Presence
heartbeat latency includes Redis completion; while a refresh from an earlier
tick is still outstanding, it reports the age of the oldest one, so a stalled
owner shows as a growing value. Keep the heartbeat-to-TTL margin large enough
for the complete refresh at your deployment's cardinality.

For backup scope, coupled restore groups and the read-only verification command,
see [Storage recovery](./recovery.md).

## Refetch after lost pub/sub delivery

The pub/sub bus queues nothing during an outage, so a recovery does not replay
what was lost. On a bus with a `breaker`, a `recovered` event on
`systemChannel` carries `recovery: { refetch: true }` when messages may have
been lost, and the client reloads the affected state (svelte-realtime puts it
on its `degradation` store).

- An instance asks its own clients when its subscriber missed messages: the
  Redis connection dropped and the channel had to be restored, activation
  failed before it succeeded, or another instance reported dropped relays. The
  event goes out once the breaker is healthy and the channel is back, without a
  `degraded` before it when the breaker never opened.
- An instance whose relays were dropped (refused by the open breaker, failed
  in Redis, or shed by a full relay queue) cannot reach the other instances'
  clients itself. It relays one `recovered` envelope on `systemChannel` when
  its breaker recovers, or, when the breaker never opened, after the next
  relay that succeeds. The envelope carries the request and a timestamp,
  nothing about the sender's outage. An instance with a breaker answers it with
  a `recovered` event of its own once its own breaker is healthy; an instance
  without one delivers the envelope unchanged. A `recovered` envelope an
  application relays itself, without `recovery.refetch`, is delivered as is.

In an asymmetric partition, where one instance loses Redis and the others keep
it, the clients on both sides are therefore asked to refetch.

A `recovered` event that asks for a refetch waits one second, and every report
that arrives meanwhile merges into it and extends the wait by another second.
After an outage every instance saw, each instance relays a request; a client
gets one refetch event, not one per instance. The wait never exceeds ten
seconds, and two refetch events are at least ten seconds apart: an instance
that keeps dropping relays, for example while its relay queue sheds under
overload, makes the clients of the other instances reload at most once every
ten seconds, and no client stays stale longer than that after a loss was
reported. When the breaker's own `recovered` event asks for a refetch, that
event waits one second but not for the spacing; one that does not goes out at
once, and a refetch event follows if requests arrive. A breaker that opens
again during the wait drops the event, and its next recovery carries the
request.

Every bus with a breaker on one platform and topic shares one announcer, so
the pub/sub bus and the sharded bus, or two pub/sub buses, never publish
competing events. It listens to all of their breakers: one `degraded` when the
first leaves healthy, one `recovered` when the last is healthy again, and none
while any is open. Their refetch reports merge into the same events. The
degradation policy of the first bus attached with one (in activation order)
shapes every event; a different policy passed to a bus attached later applies
only once the earlier bus is deactivated. When the bus whose breaker kept the
clients degraded is deactivated and the remaining breakers are healthy, the
announcer publishes `recovered`.

Every `recovered` event goes through the degradation policy: its `recovery`
fields and `recoveryJitterMs` apply, and an event outside a breaker transition
passes the policy `{ from: 'healthy', to: 'healthy' }`. A policy whose
`recovery` envelope sets `refetch` keeps its value; otherwise `refetch: true`
is added to it. `pubsub_recovered_total` counts every `recovered` event,
refetch-only ones included. The bookkeeping is two flags per bus, set on the
failure paths; a successful relay flush checks one of them.

The sharded bus does the same when it is given a `breaker`, on the fixed topic
`__realtime` (the pub/sub bus's default `systemChannel`): it announces its
breaker with the shared announcer, takes no degradation policy of its own, and
follows the shard channel of `__realtime` (`channelPrefix` plus
`shardKey('__realtime')`), where the instances exchange their requests. The
Redis user needs that channel; a refused subscription is reported once as
`EXTENSIONS-ERR-SHARDED-HEALTH-CHANNEL` and retried. The sharded bus does not
subscribe connections to `__realtime`: put them there from your `open` hook
with `platform.subscribe(ws, '__realtime')`, or with the pub/sub bus's
`hooks.open` when you run both. The PostgreSQL notification bridge publishes no
`degraded` or `recovered` events.
