# Redis relay metrics

Both buses expose these additional metrics when their existing `metrics` option
is enabled. Replace `<bus>` with `pubsub` or `sharded_pubsub`.

| Metric | Type | Labels | Unit |
|---|---|---|---|
| `<bus>_relay_attempts_total` | Counter | `outcome`: `success`, `error`, `breaker_open` | Relay envelopes |
| `<bus>_messages_local_only_total` | Counter | `reason`: `backend_error`, `breaker_open`, `envelope_oversized`, `deactivated` | Logical messages |
| `<bus>_relay_duration_seconds` | Histogram | None | Seconds per Redis flush |

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
