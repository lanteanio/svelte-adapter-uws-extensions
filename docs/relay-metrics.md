# Redis relay metrics

Both buses expose these additional metrics when their existing `metrics` option
is enabled. Replace `<bus>` with `pubsub` or `sharded_pubsub`.

| Metric | Type | Labels | Unit |
|---|---|---|---|
| `<bus>_relay_attempts_total` | Counter | `outcome`: `success`, `error`, `breaker_open` | Relay envelopes |
| `<bus>_messages_local_only_total` | Counter | `reason`: `backend_error`, `breaker_open`, `envelope_oversized`, `queue_full`, `deactivated` | Logical messages |
| `<bus>_relay_duration_seconds` | Histogram | None | Seconds per Redis flush |
| `<bus>_relay_queue_envelopes` | Gauge | `state`: `queued`, `in_flight` | Retained envelopes |
| `<bus>_relay_queue_bytes` | Gauge | `state`: `queued`, `in_flight` | Encoded UTF-8 bytes |
| `<bus>_relay_oldest_age_seconds` | Gauge | None | Oldest retained envelope age |

An envelope can carry several logical messages. One wire batch of ten messages
therefore contributes one successful attempt when Redis accepts it. If Redis
refuses that envelope, it contributes one error attempt and ten messages without
a confirmed relay. Per-command pipeline errors count only the failed envelopes;
a rejected whole pipeline leaves every envelope in that flush unconfirmed.
Breaker refusals count attempted envelopes but produce no backend duration.

`messages_local_only_total` counts messages already accepted by the local publish
path whose Redis relay was not confirmed. It does not count socket recipients or
prove that every remote recipient missed the message: a lost reply can follow a
successful Redis publish. `backend_error` includes that ambiguous case as well as
explicit backend refusals. `deactivated` records queued relays discarded when the
bus closes. An oversized ordinary publish retains its local delivery and counts
`envelope_oversized`; an oversized `publishBatched` rejects before delivery and
increments the separate `*_batch_rejected_total` instead. Explicit `relay: false`
messages are intentional local delivery and do not increment these gap counters.

Duration starts immediately before Redis submission and ends at settlement. It
excludes the coalescing wait and reports seconds, including rejected flushes.
The histogram uses the registry's configured buckets. These metrics never label
payloads, topics, channel names, error messages or user identifiers. With metrics
disabled, relay duration does not read the clock.

## Bounded admission and draining

Each bus retains at most 8,192 relay envelopes and 8 MiB of their encoded UTF-8
payloads, counting both queued and submitted envelopes until Redis settles them.
This bounds payload retention; it is not an exact JavaScript heap budget. A new
envelope that would exceed either limit is withheld from Redis after its local
delivery, including a locally accepted `publishBatched` call. Its logical
messages count as `queue_full`. The existing burst diagnostic reports this loss
once per bus even without metrics. Queue pressure does not fail the backend
breaker. There is no retry or replay of refused envelopes: reconcile from durable
application state when missed events matter.

One FIFO chunk runs at a time, preserving admission order for each channel.
Chunks contain at most 256 envelopes and normally at most 1 MiB; conservative
UTF-8 accounting may produce smaller chunks. One envelope
larger than 1 MiB uses a chunk alone if it fits both the configured envelope cap
and the queue's byte budget. Every subsequent chunk yields through the timer
queue. A stalled submission holds its capacity until its actual outcome, so new
ticks cannot accumulate unbounded in-flight batches. Owned Redis clients have
finite command deadlines; external clients must provide their own deadlines.
Standard Cluster publishing borrows one ready master connection across chunks so
cross-node gossip cannot reorder a healthy stream. It selects another ready
master only when the previous one is unavailable or removed. Node connections
retain the caller's `redisOptions` deadlines; no new connection is created and no
connection options are changed. A node failure or lost reply remains an ambiguous
delivery and ordering boundary; submitted envelopes are never retried by the bus.
There is no latest-value coalescing for ordinary ordered event streams.

`deactivate()` discards unsent entries; it cannot retract an already submitted
publish. Queue gauges sum live buses sharing a metrics registry and bus prefix.
Oldest age includes in-flight entries. Gauges use no topic or channel labels and
are collected at scrape time using exact encoded lengths; uninstrumented
admission does not read the clock. Normal admission charges a safe UTF-8 upper
bound without scanning the string, then resolves exact retained lengths before
refusing on bytes.
